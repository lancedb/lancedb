// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! DataFusion ExecutionPlan for inserting data into remote LanceDB tables.

use std::sync::{Arc, Mutex};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_ipc::CompressionType;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties, PlanProperties,
};
use futures::{SinkExt, StreamExt};
use http::header::CONTENT_TYPE;
use lance::io::exec::utils::InstrumentedRecordBatchStreamAdapter;

use crate::Error;
use crate::remote::ARROW_STREAM_CONTENT_TYPE;
use crate::remote::client::{HttpSend, RestfulLanceDbClient, Sender};
use crate::remote::table::RemoteTable;
use crate::table::AddResult;
use crate::table::datafusion::insert::COUNT_SCHEMA;
use crate::table::write_progress::WriteProgressTracker;

/// ExecutionPlan for inserting data into a remote LanceDB table.
///
/// Streams data as Arrow IPC to `/v1/table/{id}/insert/` endpoint.
///
/// When `upload_id` is set, inserts are staged as part of a multipart write
/// session and the plan supports multiple partitions for parallel uploads.
/// Without `upload_id`, the plan requires a single partition and commits
/// immediately.
#[derive(Debug)]
pub struct RemoteInsertExec<S: HttpSend = Sender> {
    table_name: String,
    identifier: String,
    client: RestfulLanceDbClient<S>,
    input: Arc<dyn ExecutionPlan>,
    overwrite: bool,
    properties: Arc<PlanProperties>,
    add_result: Arc<Mutex<Option<AddResult>>>,
    metrics: ExecutionPlanMetricsSet,
    upload_id: Option<String>,
    tracker: Option<Arc<WriteProgressTracker>>,
    /// Branch to write to via `?branch=`. `None` targets the main branch.
    branch: Option<String>,
    /// For multipart writes, split each partition into parts of at most this
    /// many bytes, each uploaded as a separate request. `None` sends the whole
    /// partition as a single request.
    max_bytes_per_request: Option<usize>,
}

impl<S: HttpSend + 'static> RemoteInsertExec<S> {
    /// Create a new single-partition RemoteInsertExec.
    pub fn new(
        table_name: String,
        identifier: String,
        client: RestfulLanceDbClient<S>,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
        tracker: Option<Arc<WriteProgressTracker>>,
        branch: Option<String>,
    ) -> Self {
        Self::new_inner(
            table_name, identifier, client, input, overwrite, None, tracker, branch, None,
        )
    }

    /// Create a multi-partition RemoteInsertExec for use with multipart writes.
    ///
    /// Each partition's insert is staged under the given `upload_id` without
    /// committing. The caller is responsible for calling the complete (or abort)
    /// endpoint after all partitions finish.
    #[allow(clippy::too_many_arguments)]
    pub fn new_multipart(
        table_name: String,
        identifier: String,
        client: RestfulLanceDbClient<S>,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
        upload_id: String,
        tracker: Option<Arc<WriteProgressTracker>>,
        branch: Option<String>,
        max_bytes_per_request: Option<usize>,
    ) -> Self {
        Self::new_inner(
            table_name,
            identifier,
            client,
            input,
            overwrite,
            Some(upload_id),
            tracker,
            branch,
            max_bytes_per_request,
        )
    }

    #[allow(clippy::too_many_arguments)]
    fn new_inner(
        table_name: String,
        identifier: String,
        client: RestfulLanceDbClient<S>,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
        upload_id: Option<String>,
        tracker: Option<Arc<WriteProgressTracker>>,
        branch: Option<String>,
        max_bytes_per_request: Option<usize>,
    ) -> Self {
        let num_partitions = if upload_id.is_some() {
            input.output_partitioning().partition_count()
        } else {
            1
        };
        let schema = COUNT_SCHEMA.clone();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema),
            datafusion_physical_plan::Partitioning::UnknownPartitioning(num_partitions),
            datafusion_physical_plan::execution_plan::EmissionType::Final,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );

        Self {
            table_name,
            identifier,
            client,
            input,
            overwrite,
            properties: Arc::new(properties),
            add_result: Arc::new(Mutex::new(None)),
            metrics: ExecutionPlanMetricsSet::new(),
            upload_id,
            tracker,
            branch,
            max_bytes_per_request,
        }
    }

    /// Get the add result after execution.
    // TODO: this will be used when we wire this up to Table::add().
    #[allow(dead_code)]
    pub fn add_result(&self) -> Option<AddResult> {
        self.add_result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Stream the input into an HTTP body as an Arrow IPC stream, capturing any
    /// stream errors into the provided channel. Errors from the input plan
    /// (e.g. NaN rejection) would otherwise be swallowed inside the HTTP body
    /// upload; by stashing them in the channel we can surface them with their
    /// original message after the request completes.
    fn stream_as_http_body(
        data: SendableRecordBatchStream,
        error_tx: tokio::sync::oneshot::Sender<DataFusionError>,
        tracker: Option<Arc<WriteProgressTracker>>,
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
            move |(mut data, mut writer, error_tx, finished)| {
                let tracker = tracker.clone();
                async move {
                    if finished {
                        return Ok(None);
                    }
                    match data.next().await {
                        Some(Ok(batch)) => {
                            writer
                                .write(&batch)
                                .map_err(|e| std::io::Error::other(e.to_string()))?;
                            let buffer = std::mem::take(writer.get_mut());
                            if let Some(ref t) = tracker {
                                t.record_bytes(buffer.len());
                            }
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
                                if let Some(ref t) = tracker {
                                    t.record_bytes(buffer.len());
                                }
                                Ok(Some((buffer, (data, writer, None, true))))
                            }
                        }
                    }
                }
            },
        );

        Ok(reqwest::Body::wrap_stream(stream))
    }

    /// Upload a partition as one or more multipart parts, each at most
    /// `max_bytes` (Arrow IPC, compressed) bytes.
    ///
    /// Each part is a separate `/insert?upload_id=...&upload_part_id=...` request
    /// whose body is still streamed through a bounded channel, so peak memory
    /// stays at a couple of batches regardless of `max_bytes`. The server stages
    /// every part under the shared `upload_id` and merges them atomically when
    /// the caller completes the multipart write. An empty partition stages
    /// nothing: the multipart write always has at least one non-empty partition
    /// to commit.
    #[allow(clippy::too_many_arguments)]
    async fn send_multipart_chunked(
        client: &RestfulLanceDbClient<S>,
        identifier: &str,
        table_name: &str,
        upload_id: &str,
        branch: Option<&str>,
        overwrite: bool,
        max_bytes: usize,
        mut input: SendableRecordBatchStream,
        tracker: Option<Arc<WriteProgressTracker>>,
    ) -> DataFusionResult<()> {
        let schema = input.schema();

        // A part always starts from a batch we already hold: the first batch of
        // the partition, or the look-ahead batch from the previous part. This
        // keeps empty partitions from staging a part and stops a size cut that
        // lands exactly on the end of input from emitting a trailing empty part.
        let mut first = match input.next().await {
            Some(batch) => batch?,
            None => return Ok(()),
        };

        loop {
            let (part_bytes, input_ended) = Self::send_one_part(
                client, identifier, table_name, upload_id, branch, overwrite, &schema, max_bytes,
                first, &mut input,
            )
            .await?;

            if let Some(ref t) = tracker {
                t.record_bytes(part_bytes);
            }

            if input_ended {
                break;
            }

            first = match input.next().await {
                Some(batch) => batch?,
                None => break,
            };
        }

        Ok(())
    }

    /// Build the `/insert` request for a single multipart part.
    fn build_part_request(
        client: &RestfulLanceDbClient<S>,
        identifier: &str,
        upload_id: &str,
        part_id: &str,
        branch: Option<&str>,
        overwrite: bool,
        body: reqwest::Body,
    ) -> reqwest::RequestBuilder {
        let mut request = client
            .post(&format!("/v1/table/{}/insert/", identifier))
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
            .query(&[("upload_id", upload_id)])
            .query(&[("upload_part_id", part_id)]);
        if overwrite {
            request = request.query(&[("mode", "overwrite")]);
        }
        if let Some(b) = branch {
            request = request.query(&[("branch", b)]);
        }
        request.body(body)
    }

    /// Send a single part's request and drain the response, mapping HTTP and
    /// table-not-found errors into `DataFusionError`.
    async fn send_part_request(
        client: &RestfulLanceDbClient<S>,
        table_name: &str,
        request: reqwest::RequestBuilder,
    ) -> DataFusionResult<()> {
        let (request_id, response) = client
            .send(request)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let response =
            RemoteTable::<Sender>::handle_table_not_found(table_name, response, &request_id)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
        let response = client
            .check_response(&request_id, response)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;
        response.bytes().await.map_err(|e| {
            DataFusionError::External(Box::new(Error::Http {
                source: Box::new(e),
                request_id: request_id.clone(),
                status_code: None,
            }))
        })?;
        Ok(())
    }

    /// Stream one part, starting from `first` and pulling from `input` until the
    /// part reaches `max_bytes` or the input ends. The body is streamed through
    /// a bounded channel concurrently with the request, so peak memory stays at
    /// a couple of batches. Returns the compressed bytes written and whether the
    /// input was exhausted while filling this part.
    #[allow(clippy::too_many_arguments)]
    async fn send_one_part(
        client: &RestfulLanceDbClient<S>,
        identifier: &str,
        table_name: &str,
        upload_id: &str,
        branch: Option<&str>,
        overwrite: bool,
        schema: &arrow_schema::SchemaRef,
        max_bytes: usize,
        first: RecordBatch,
        input: &mut SendableRecordBatchStream,
    ) -> DataFusionResult<(usize, bool)> {
        let (mut chunk_tx, chunk_rx) =
            futures::channel::mpsc::channel::<Result<Vec<u8>, std::io::Error>>(2);
        let body = reqwest::Body::wrap_stream(chunk_rx);

        let part_id = uuid::Uuid::new_v4().to_string();
        let request = Self::build_part_request(
            client, identifier, upload_id, &part_id, branch, overwrite, body,
        );

        let producer = async move {
            let options = arrow_ipc::writer::IpcWriteOptions::default()
                .try_with_compression(Some(CompressionType::LZ4_FRAME))
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let mut writer =
                arrow_ipc::writer::StreamWriter::try_new_with_options(Vec::new(), schema, options)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let mut part_bytes: usize = 0;
            let mut input_ended = false;
            let mut pending = Some(first);
            loop {
                let batch = match pending.take() {
                    Some(batch) => batch,
                    None => match input.next().await {
                        Some(Ok(batch)) => batch,
                        Some(Err(e)) => {
                            // Abort the body so the server does not treat the
                            // truncated stream as a successful write; the
                            // original error is surfaced to the caller.
                            let _ = chunk_tx
                                .send(Err(std::io::Error::other("input stream error")))
                                .await;
                            return Err(e);
                        }
                        None => {
                            input_ended = true;
                            break;
                        }
                    },
                };
                writer
                    .write(&batch)
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;
                let chunk = std::mem::take(writer.get_mut());
                part_bytes += chunk.len();
                if chunk_tx.send(Ok(chunk)).await.is_err() {
                    // The request finished or failed; stop producing.
                    break;
                }
                if part_bytes >= max_bytes {
                    break;
                }
            }

            writer
                .finish()
                .map_err(|e| DataFusionError::External(Box::new(e)))?;
            let tail = std::mem::take(writer.get_mut());
            if !tail.is_empty() {
                let _ = chunk_tx.send(Ok(tail)).await;
            }
            Ok::<(usize, bool), DataFusionError>((part_bytes, input_ended))
        };

        let send = Self::send_part_request(client, table_name, request);

        let (producer_result, send_result) = futures::join!(producer, send);
        // Prefer the producer error (e.g. NaN rejection) over any HTTP error it
        // induced.
        let (part_bytes, input_ended) = producer_result?;
        send_result?;

        Ok((part_bytes, input_ended))
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

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![false]
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        if self.upload_id.is_some() {
            vec![datafusion_physical_plan::Distribution::UnspecifiedDistribution]
        } else {
            vec![datafusion_physical_plan::Distribution::SinglePartition]
        }
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
        Ok(Arc::new(Self::new_inner(
            self.table_name.clone(),
            self.identifier.clone(),
            self.client.clone(),
            children[0].clone(),
            self.overwrite,
            self.upload_id.clone(),
            self.tracker.clone(),
            self.branch.clone(),
            self.max_bytes_per_request,
        )))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if self.upload_id.is_none() && partition != 0 {
            return Err(DataFusionError::Internal(
                "RemoteInsertExec only supports single partition execution without upload_id"
                    .to_string(),
            ));
        }

        let input_stream = self.input.execute(partition, context)?;
        let input_schema = input_stream.schema();
        let input_stream: SendableRecordBatchStream =
            Box::pin(InstrumentedRecordBatchStreamAdapter::new(
                input_schema,
                input_stream,
                partition,
                &self.metrics,
            ));
        let client = self.client.clone();
        let identifier = self.identifier.clone();
        let overwrite = self.overwrite;
        let add_result = self.add_result.clone();
        let table_name = self.table_name.clone();
        let upload_id = self.upload_id.clone();
        let tracker = self.tracker.clone();
        let branch = self.branch.clone();
        let max_bytes_per_request = self.max_bytes_per_request;

        let stream = futures::stream::once(async move {
            // Multipart writes with a byte budget split the partition into
            // several bounded, still-streamed requests so no single request
            // stays open long enough to hit the client read timeout.
            if let (Some(upload_id), Some(max_bytes)) =
                (upload_id.as_deref(), max_bytes_per_request)
            {
                Self::send_multipart_chunked(
                    &client,
                    &identifier,
                    &table_name,
                    upload_id,
                    branch.as_deref(),
                    overwrite,
                    max_bytes,
                    input_stream,
                    tracker,
                )
                .await?;
                let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
                return Ok::<RecordBatch, DataFusionError>(RecordBatch::try_new(
                    COUNT_SCHEMA.clone(),
                    vec![count_array],
                )?);
            }

            let mut request = client
                .post(&format!("/v1/table/{}/insert/", identifier))
                .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);

            if overwrite {
                request = request.query(&[("mode", "overwrite")]);
            }
            if let Some(ref uid) = upload_id {
                request = request.query(&[("upload_id", uid.as_str())]);
            }
            if let Some(ref b) = branch {
                request = request.query(&[("branch", b.as_str())]);
            }

            let (error_tx, mut error_rx) = tokio::sync::oneshot::channel();
            let body = Self::stream_as_http_body(input_stream, error_tx, tracker)?;
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

            // For multipart writes, the staging response is not the final
            // version. Only parse AddResult for non-multipart inserts.
            if upload_id.is_none() {
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

                let mut res_lock = add_result.lock().map_err(|_| {
                    DataFusionError::Execution("Failed to acquire lock for add_result".to_string())
                })?;
                *res_lock = Some(parsed_result);
            } else {
                // We don't use the body in this case, but we should still consume it.
                let _ = response.bytes().await.map_err(|e| {
                    DataFusionError::External(Box::new(Error::Http {
                        source: Box::new(e),
                        request_id: request_id.clone(),
                        status_code: None,
                    }))
                })?;
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::record_batch;
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use datafusion::prelude::SessionContext;
    use datafusion_catalog::MemTable;
    use datafusion_execution::TaskContext;
    use datafusion_physical_plan::ExecutionPlan;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::RemoteInsertExec;
    use crate::Table;
    use crate::remote::ARROW_STREAM_CONTENT_TYPE;
    use crate::table::datafusion::BaseTableAdapter;

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

    /// Build a single-partition input plan from the given batches.
    async fn input_plan_from_batches(
        schema: Arc<ArrowSchema>,
        batches: Vec<arrow_array::RecordBatch>,
    ) -> Arc<dyn ExecutionPlan> {
        use datafusion_catalog::TableProvider;
        let mem = MemTable::try_new(schema, vec![batches]).unwrap();
        let ctx = SessionContext::new();
        mem.scan(&ctx.state(), None, &[], None).await.unwrap()
    }

    fn counting_insert_client(
        counter: Arc<AtomicUsize>,
    ) -> crate::remote::client::RestfulLanceDbClient<crate::remote::client::test_utils::MockSender>
    {
        crate::remote::client::test_utils::client_with_handler(move |request| {
            let path = request.url().path();
            assert_eq!(path, "/v1/table/my_table/insert/");
            let query = request.url().query().unwrap_or("");
            assert!(query.contains("upload_id=upload-1"), "query: {query}");
            assert!(query.contains("upload_part_id="), "query: {query}");
            counter.fetch_add(1, Ordering::SeqCst);
            http::Response::builder()
                .status(200)
                .body(String::new())
                .unwrap()
        })
    }

    #[tokio::test]
    async fn test_multipart_chunked_splits_into_parts() {
        use futures::StreamExt;

        let insert_count = Arc::new(AtomicUsize::new(0));
        let client = counting_insert_client(insert_count.clone());

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let batches = vec![
            record_batch!(("id", Int32, [1, 2])).unwrap(),
            record_batch!(("id", Int32, [3, 4])).unwrap(),
            record_batch!(("id", Int32, [5, 6])).unwrap(),
        ];
        let input = input_plan_from_batches(schema, batches).await;

        // A 1-byte budget forces every batch into its own part.
        let exec = RemoteInsertExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            None,
            None,
            Some(1),
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        assert_eq!(insert_count.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn test_multipart_single_part_when_under_budget() {
        use futures::StreamExt;

        let insert_count = Arc::new(AtomicUsize::new(0));
        let client = counting_insert_client(insert_count.clone());

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::Int32,
            true,
        )]));
        let batches = vec![
            record_batch!(("id", Int32, [1, 2])).unwrap(),
            record_batch!(("id", Int32, [3, 4])).unwrap(),
            record_batch!(("id", Int32, [5, 6])).unwrap(),
        ];
        let input = input_plan_from_batches(schema, batches).await;

        // A large budget keeps the whole partition in a single part.
        let exec = RemoteInsertExec::new_multipart(
            "my_table".to_string(),
            "my_table".to_string(),
            client,
            input,
            false,
            "upload-1".to_string(),
            None,
            None,
            Some(64 * 1024 * 1024),
        );

        let mut stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        while stream.next().await.transpose().unwrap().is_some() {}

        assert_eq!(insert_count.load(Ordering::SeqCst), 1);
    }
}
