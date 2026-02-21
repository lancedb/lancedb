// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! DataFusion ExecutionPlan for inserting data into remote LanceDB tables.

use std::any::Any;
use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_ipc::CompressionType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::StreamExt;
use http::header::CONTENT_TYPE;

use crate::remote::client::{HttpSend, RestfulLanceDbClient, Sender};
use crate::remote::table::RemoteTable;
use crate::remote::ARROW_STREAM_CONTENT_TYPE;
use crate::table::datafusion::insert::COUNT_SCHEMA;
use crate::table::AddResult;
use crate::Error;

/// ExecutionPlan for inserting data into a remote LanceDB table.
///
/// This plan:
/// 1. Requires single partition (no parallel remote inserts yet)
/// 2. Streams data as Arrow IPC to `/v1/table/{id}/insert/` endpoint
/// 3. Stores AddResult for retrieval after execution
#[derive(Debug)]
pub struct RemoteInsertExec<S: HttpSend = Sender> {
    table_name: String,
    identifier: String,
    client: RestfulLanceDbClient<S>,
    input: Arc<dyn ExecutionPlan>,
    overwrite: bool,
    properties: PlanProperties,
    add_result: Arc<Mutex<Option<AddResult>>>,
}

impl<S: HttpSend + 'static> RemoteInsertExec<S> {
    /// Create a new RemoteInsertExec.
    pub fn new(
        table_name: String,
        identifier: String,
        client: RestfulLanceDbClient<S>,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
    ) -> Self {
        let schema = COUNT_SCHEMA.clone();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion_physical_plan::Partitioning::UnknownPartitioning(1),
            datafusion_physical_plan::execution_plan::EmissionType::Final,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            table_name,
            identifier,
            client,
            input,
            overwrite,
            properties,
            add_result: Arc::new(Mutex::new(None)),
        }
    }

    /// Get the add result after execution.
    // TODO: this will be used when we wire this up to Table::add().
    #[allow(dead_code)]
    pub fn add_result(&self) -> Option<AddResult> {
        self.add_result.lock().unwrap().clone()
    }

    /// Stream the input into an HTTP body as an Arrow IPC stream, capturing any
    /// stream errors into the provided channel. Errors from the input plan
    /// (e.g. NaN rejection) would otherwise be swallowed inside the HTTP body
    /// upload; by stashing them in the channel we can surface them with their
    /// original message after the request completes.
    fn stream_as_http_body(
        data: SendableRecordBatchStream,
        error_tx: tokio::sync::oneshot::Sender<DataFusionError>,
    ) -> DataFusionResult<reqwest::Body> {
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            Vec::new(),
            &data.schema(),
            options,
        )?;

        let stream = futures::stream::try_unfold(
            (data, writer, Some(error_tx), false),
            move |(mut data, mut writer, error_tx, finished)| async move {
                if finished {
                    return Ok(None);
                }
                match data.next().await {
                    Some(Ok(batch)) => {
                        writer
                            .write(&batch)
                            .map_err(|e| std::io::Error::other(e.to_string()))?;
                        let buffer = std::mem::take(writer.get_mut());
                        Ok(Some((buffer, (data, writer, error_tx, false))))
                    }
                    Some(Err(e)) => {
                        // Send the original error through the channel before
                        // returning a generic error to reqwest.
                        if let Some(tx) = error_tx {
                            let _ = tx.send(e);
                        }
                        Err(std::io::Error::other(
                            "input stream error (see error channel)",
                        ))
                    }
                    None => {
                        writer
                            .finish()
                            .map_err(|e| std::io::Error::other(e.to_string()))?;
                        let buffer = std::mem::take(writer.get_mut());
                        if buffer.is_empty() {
                            Ok(None)
                        } else {
                            Ok(Some((buffer, (data, writer, None, true))))
                        }
                    }
                }
            },
        );

        Ok(reqwest::Body::wrap_stream(stream))
    }
}

impl<S: HttpSend + 'static> DisplayAs for RemoteInsertExec<S> {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "RemoteInsertExec: table={}, overwrite={}",
                    self.table_name, self.overwrite
                )
            }
            DisplayFormatType::TreeRender => {
                write!(f, "RemoteInsertExec")
            }
        }
    }
}

impl<S: HttpSend + 'static> ExecutionPlan for RemoteInsertExec<S> {
    fn name(&self) -> &str {
        Self::static_name()
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        // Until we have a separate commit endpoint, we need to do all inserts in a single partition
        vec![datafusion_physical_plan::Distribution::SinglePartition]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "RemoteInsertExec requires exactly one child".to_string(),
            ));
        }
        Ok(Arc::new(Self::new(
            self.table_name.clone(),
            self.identifier.clone(),
            self.client.clone(),
            children[0].clone(),
            self.overwrite,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(
                "RemoteInsertExec only supports single partition execution".to_string(),
            ));
        }

        let input_stream = self.input.execute(0, context)?;
        let client = self.client.clone();
        let identifier = self.identifier.clone();
        let overwrite = self.overwrite;
        let add_result = self.add_result.clone();
        let table_name = self.table_name.clone();

        let stream = futures::stream::once(async move {
            let mut request = client
                .post(&format!("/v1/table/{}/insert/", identifier))
                .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);

            if overwrite {
                request = request.query(&[("mode", "overwrite")]);
            }

            let (error_tx, mut error_rx) = tokio::sync::oneshot::channel();
            let body = Self::stream_as_http_body(input_stream, error_tx)?;
            let request = request.body(body);

            let result: DataFusionResult<(String, _)> = async {
                let (request_id, response) = client
                    .send(request)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let response = RemoteTable::<Sender>::handle_table_not_found(
                    &table_name,
                    response,
                    &request_id,
                )
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

                let response = client
                    .check_response(&request_id, response)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                Ok((request_id, response))
            }
            .await;

            // If the request failed due to an input stream error, surface the
            // original error (e.g. NaN rejection) instead of the HTTP error.
            if let Ok(stream_err) = error_rx.try_recv() {
                return Err(stream_err);
            }

            let (request_id, response) = result?;

            let body_text = response.text().await.map_err(|e| {
                DataFusionError::External(Box::new(Error::Http {
                    source: Box::new(e),
                    request_id: request_id.clone(),
                    status_code: None,
                }))
            })?;

            let parsed_result = if body_text.trim().is_empty() {
                // Backward compatible with old servers
                AddResult { version: 0 }
            } else {
                serde_json::from_str(&body_text).map_err(|e| {
                    DataFusionError::External(Box::new(Error::Http {
                        source: format!("Failed to parse add response: {}", e).into(),
                        request_id: request_id.clone(),
                        status_code: None,
                    }))
                })?
            };

            {
                let mut res_lock = add_result.lock().map_err(|_| {
                    DataFusionError::Execution("Failed to acquire lock for add_result".to_string())
                })?;
                *res_lock = Some(parsed_result);
            }

            // Return a single batch with count 0 (actual count is tracked in add_result)
            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
            let batch = RecordBatch::try_new(COUNT_SCHEMA.clone(), vec![count_array])?;
            Ok::<_, DataFusionError>(batch)
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            COUNT_SCHEMA.clone(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::record_batch;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::MemTable;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use crate::remote::ARROW_STREAM_CONTENT_TYPE;
    use crate::table::datafusion::BaseTableAdapter;
    use crate::Table;

    fn schema_json() -> &'static str {
        r#"{"fields": [{"name": "id", "type": {"type": "int32"}, "nullable": true}]}"#
    }

    #[tokio::test]
    async fn test_remote_insert_exec_execute_empty() {
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_clone = request_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                // Return schema for BaseTableAdapter::try_new
                return http::Response::builder()
                    .status(200)
                    .body(format!(r#"{{"version": 1, "schema": {}}}"#, schema_json()))
                    .unwrap();
            }

            if path == "/v1/table/my_table/insert/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    ARROW_STREAM_CONTENT_TYPE
                );
                request_count_clone.fetch_add(1, Ordering::SeqCst);

                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        });

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));

        // Create empty MemTable (no batches)
        let source_table = MemTable::try_new(schema, vec![vec![]]).unwrap();

        let ctx = SessionContext::new();

        // Register the remote table as insert target
        let provider = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        ctx.register_table("my_table", Arc::new(provider)).unwrap();

        // Register empty source
        ctx.register_table("empty_source", Arc::new(source_table))
            .unwrap();

        // Execute the INSERT
        ctx.sql("INSERT INTO my_table SELECT * FROM empty_source")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // Verify: should have made exactly one HTTP request even with empty input
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn test_remote_insert_exec_multi_partition() {
        let request_count = Arc::new(AtomicUsize::new(0));
        let request_count_clone = request_count.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            let path = request.url().path();

            if path == "/v1/table/my_table/describe/" {
                // Return schema for BaseTableAdapter::try_new
                return http::Response::builder()
                    .status(200)
                    .body(format!(r#"{{"version": 1, "schema": {}}}"#, schema_json()))
                    .unwrap();
            }

            if path == "/v1/table/my_table/insert/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    ARROW_STREAM_CONTENT_TYPE
                );
                request_count_clone.fetch_add(1, Ordering::SeqCst);

                return http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#.to_string())
                    .unwrap();
            }

            panic!("Unexpected request path: {}", path);
        });

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));

        // Create MemTable with multiple partitions and multiple batches
        let source_table = MemTable::try_new(
            schema,
            vec![
                // Partition 0
                vec![
                    record_batch!(("id", Int32, [1, 2])).unwrap(),
                    record_batch!(("id", Int32, [3, 4])).unwrap(),
                ],
                // Partition 1
                vec![record_batch!(("id", Int32, [5, 6, 7])).unwrap()],
                // Partition 2
                vec![record_batch!(("id", Int32, [8])).unwrap()],
            ],
        )
        .unwrap();

        let ctx = SessionContext::new();

        // Register the remote table as insert target
        let provider = BaseTableAdapter::try_new(table.base_table().clone())
            .await
            .unwrap();
        ctx.register_table("my_table", Arc::new(provider)).unwrap();

        // Register multi-partition source
        ctx.register_table("multi_partition_source", Arc::new(source_table))
            .unwrap();

        // Get the physical plan and verify it includes a repartition to 1
        let df = ctx
            .sql("INSERT INTO my_table SELECT * FROM multi_partition_source")
            .await
            .unwrap();
        let plan = df.clone().create_physical_plan().await.unwrap();
        let plan_str = datafusion::physical_plan::displayable(plan.as_ref())
            .indent(true)
            .to_string();

        // The plan should include a CoalescePartitionsExec to merge partitions
        assert!(
            plan_str.contains("CoalescePartitionsExec"),
            "Expected CoalescePartitionsExec in plan:\n{}",
            plan_str
        );

        // Execute the INSERT
        df.collect().await.unwrap();

        // Verify: should have made exactly one HTTP request despite multiple input partitions
        assert_eq!(request_count.load(Ordering::SeqCst), 1);
    }
}
