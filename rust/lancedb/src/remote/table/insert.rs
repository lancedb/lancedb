// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{
    any::Any,
    sync::{Arc, Mutex},
};

use arrow_array::{ArrayRef, RecordBatch, UInt64Array};
use arrow_ipc::CompressionType;
use arrow_schema::{ArrowError, DataType, Field, Schema as ArrowSchema, SchemaRef};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion_common::{DataFusionError, Result as DataFusionResult};
use datafusion_execution::{SendableRecordBatchStream, TaskContext};
use datafusion_physical_plan::{
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan,
    ExecutionPlanProperties, Partitioning, PlanProperties,
};
use futures::{stream, StreamExt};
use http::header::CONTENT_TYPE;
use serde::{Deserialize, Serialize};

use super::RemoteTable;
use crate::{
    remote::{
        client::{HttpSend, RestfulLanceDbClient, Sender},
        db::ServerVersion,
        ARROW_STREAM_CONTENT_TYPE,
    },
    table::{AddResult, WriteProgressState},
    Error,
};

fn make_count_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

#[derive(Debug, Deserialize)]
struct UncommittedInsertResponse {
    transaction: String,
}

#[derive(Debug, Serialize)]
struct CommitRequest {
    transactions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct CommitResponse {
    version: u64,
}

pub struct RemoteInsertExec<S: HttpSend = Sender> {
    table_name: String,
    identifier: String,
    client: RestfulLanceDbClient<S>,
    input: Arc<dyn ExecutionPlan>,
    overwrite: bool,
    parallel_insert: bool,
    properties: PlanProperties,
    add_result: Arc<Mutex<Option<AddResult>>>,
    transactions: Arc<Mutex<Vec<String>>>,
    progress: Option<Arc<WriteProgressState>>,
}

impl<S: HttpSend> std::fmt::Debug for RemoteInsertExec<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteInsertExec")
            .field("table_name", &self.table_name)
            .field("identifier", &self.identifier)
            .field("overwrite", &self.overwrite)
            .field("parallel_insert", &self.parallel_insert)
            .finish()
    }
}

impl<S: HttpSend> DisplayAs for RemoteInsertExec<S> {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", Self::static_name())
    }
}

impl<S: HttpSend> RemoteInsertExec<S> {
    pub fn new(
        table_name: String,
        identifier: String,
        client: RestfulLanceDbClient<S>,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
        server_version: &ServerVersion,
        progress: Option<Arc<WriteProgressState>>,
    ) -> Self {
        let parallel_insert = server_version.support_parallel_insert()
            && input.output_partitioning().partition_count() > 1;
        Self::new_inner(
            table_name,
            identifier,
            client,
            input,
            overwrite,
            parallel_insert,
            progress,
        )
    }

    fn new_inner(
        table_name: String,
        identifier: String,
        client: RestfulLanceDbClient<S>,
        input: Arc<dyn ExecutionPlan>,
        overwrite: bool,
        parallel_insert: bool,
        progress: Option<Arc<WriteProgressState>>,
    ) -> Self {
        let num_partitions = if parallel_insert {
            input.output_partitioning().partition_count()
        } else {
            1
        };
        let output_schema = make_count_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(num_partitions),
            datafusion_physical_plan::execution_plan::EmissionType::Final,
            datafusion_physical_plan::execution_plan::Boundedness::Bounded,
        );
        Self {
            table_name,
            identifier,
            client,
            input,
            overwrite,
            parallel_insert,
            properties,
            add_result: Arc::new(Mutex::new(None)),
            transactions: Arc::new(Mutex::new(Vec::new())),
            progress,
        }
    }

    pub fn add_result(&self) -> Option<AddResult> {
        match self.add_result.lock() {
            Err(_) => None,
            Ok(res) => res.clone(),
        }
    }

    fn stream_as_body(
        data: SendableRecordBatchStream,
        progress: Option<Arc<WriteProgressState>>,
    ) -> DataFusionResult<reqwest::Body> {
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            Vec::new(),
            &data.schema(),
            options,
        )?;

        let stream = futures::stream::try_unfold((data, writer), move |(mut data, mut writer)| {
            let progress = progress.clone();
            async move {
                match data.next().await {
                    Some(Ok(batch)) => {
                        let num_rows = batch.num_rows();
                        writer.write(&batch)?;
                        let buffer = std::mem::take(writer.get_mut());
                        if let Some(ref progress) = progress {
                            progress.report(num_rows, buffer.len());
                        }
                        Ok(Some((buffer, (data, writer))))
                    }
                    Some(Err(e)) => Err(e),
                    None => {
                        if let Err(ArrowError::IpcError(_msg)) = writer.finish() {
                            // Will error if already closed.
                            return Ok(None);
                        };
                        let buffer = std::mem::take(writer.get_mut());
                        Ok(Some((buffer, (data, writer))))
                    }
                }
            }
        });

        Ok(reqwest::Body::wrap_stream(stream))
    }
}

impl<S: HttpSend> ExecutionPlan for RemoteInsertExec<S> {
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
            self.parallel_insert,
            self.progress.clone(),
        )))
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        if self.parallel_insert {
            vec![datafusion_physical_plan::Distribution::UnspecifiedDistribution]
        } else {
            vec![datafusion_physical_plan::Distribution::SinglePartition]
        }
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // Input partitioning decides the number of output files, which we want
        // to control carefully with a custom optimizer rule.
        vec![false]
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;

        let output_schema = make_count_schema();
        let add_result_mutex = self.add_result.clone();
        let client = self.client.clone();
        let identifier = self.identifier.clone();
        let table_name = self.table_name.clone();
        let overwrite = self.overwrite;
        let parallel_insert = self.parallel_insert;
        let num_partitions = self.input.output_partitioning().partition_count();
        let transactions = self.transactions.clone();

        let progress = self.progress.clone();

        let fut = async move {
            let mut request = client
                .post(&format!("/v1/table/{}/insert/", identifier))
                .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);

            if overwrite {
                request = request.query(&[("mode", "overwrite")]);
            }

            if parallel_insert {
                request = request.query(&[("uncommitted", "true")]);
            }

            let body = Self::stream_as_body(input_stream, progress)?;
            let request = request.body(body);

            let (request_id, response) = client
                .send(request)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Check for table not found
            let response =
                RemoteTable::<Sender>::handle_table_not_found(&table_name, response, &request_id)
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

            // Check for other HTTP errors
            let response = client
                .check_response(&request_id, response)
                .await
                .map_err(|e| DataFusionError::External(Box::new(e)))?;

            let body_text = response.text().await.map_err(|e| {
                DataFusionError::External(Box::new(Error::Http {
                    source: Box::new(e),
                    request_id: request_id.clone(),
                    status_code: None,
                }))
            })?;

            if parallel_insert {
                // Parse the uncommitted insert response
                let uncommitted_response: UncommittedInsertResponse =
                    serde_json::from_str(&body_text).map_err(|e| {
                        DataFusionError::External(Box::new(Error::Http {
                            source: format!("Failed to parse uncommitted insert response: {}", e)
                                .into(),
                            request_id: request_id.clone(),
                            status_code: None,
                        }))
                    })?;

                // Collect the transaction; if we're the last partition, commit.
                let should_commit = {
                    let mut txns = transactions.lock().map_err(|_| {
                        DataFusionError::Execution(
                            "Failed to acquire lock for transactions".to_string(),
                        )
                    })?;
                    txns.push(uncommitted_response.transaction);
                    txns.len() == num_partitions
                };

                if should_commit {
                    let commit_txns = {
                        let txns = transactions.lock().map_err(|_| {
                            DataFusionError::Execution(
                                "Failed to acquire lock for transactions".to_string(),
                            )
                        })?;
                        txns.clone()
                    };

                    let commit_body = serde_json::to_vec(&CommitRequest {
                        transactions: commit_txns,
                    })
                    .map_err(|e| {
                        DataFusionError::External(Box::new(Error::Runtime {
                            message: format!("Failed to serialize commit request: {}", e),
                        }))
                    })?;

                    let commit_request = client
                        .post(&format!("/v1/table/{}/commit/", identifier))
                        .header(CONTENT_TYPE, "application/json")
                        .body(commit_body);

                    let (commit_request_id, commit_response) = client
                        .send(commit_request)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let commit_response = RemoteTable::<Sender>::handle_table_not_found(
                        &table_name,
                        commit_response,
                        &commit_request_id,
                    )
                    .await
                    .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let commit_response = client
                        .check_response(&commit_request_id, commit_response)
                        .await
                        .map_err(|e| DataFusionError::External(Box::new(e)))?;

                    let commit_body_text = commit_response.text().await.map_err(|e| {
                        DataFusionError::External(Box::new(Error::Http {
                            source: Box::new(e),
                            request_id: commit_request_id.clone(),
                            status_code: None,
                        }))
                    })?;

                    let commit_result: CommitResponse = serde_json::from_str(&commit_body_text)
                        .map_err(|e| {
                            DataFusionError::External(Box::new(Error::Http {
                                source: format!("Failed to parse commit response: {}", e).into(),
                                request_id: commit_request_id.clone(),
                                status_code: None,
                            }))
                        })?;

                    let mut res_lock = add_result_mutex.lock().map_err(|_| {
                        DataFusionError::Execution(
                            "Failed to acquire lock for add_result".to_string(),
                        )
                    })?;
                    *res_lock = Some(AddResult {
                        version: commit_result.version,
                    });
                }
            } else {
                // Legacy single-partition path
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

                let mut res_lock = add_result_mutex.lock().map_err(|_| {
                    DataFusionError::Execution("Failed to acquire lock for add_result".to_string())
                })?;
                *res_lock = Some(parsed_result);
            }

            // Return a single batch with count 0 (actual count is tracked in add_result)
            let count_array: ArrayRef = Arc::new(UInt64Array::from(vec![0u64]));
            let batch = RecordBatch::try_new(make_count_schema(), vec![count_array])?;

            Ok::<_, DataFusionError>(batch)
        };

        let stream = stream::once(fut).boxed();
        let stream = RecordBatchStreamAdapter::new(output_schema, stream);

        Ok(Box::pin(stream))
    }
}
