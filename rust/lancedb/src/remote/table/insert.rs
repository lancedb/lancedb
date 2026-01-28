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
    stream::RecordBatchStreamAdapter, DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning,
    PlanProperties,
};
use futures::{stream, StreamExt};
use http::header::CONTENT_TYPE;

use super::RemoteTable;
use crate::{
    remote::{
        client::{HttpSend, RestfulLanceDbClient, Sender},
        ARROW_STREAM_CONTENT_TYPE,
    },
    table::AddResult,
    Error,
};

fn make_count_schema() -> SchemaRef {
    Arc::new(ArrowSchema::new(vec![Field::new(
        "count",
        DataType::UInt64,
        false,
    )]))
}

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
    ) -> Self {
        let output_schema = make_count_schema();
        let properties = PlanProperties::new(
            EquivalenceProperties::new(output_schema),
            Partitioning::UnknownPartitioning(1),
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

    pub fn add_result(&self) -> Option<AddResult> {
        match self.add_result.lock() {
            Err(_) => None,
            Ok(res) => res.clone(),
        }
    }

    fn stream_as_body(data: SendableRecordBatchStream) -> DataFusionResult<reqwest::Body> {
        let options = arrow_ipc::writer::IpcWriteOptions::default()
            .try_with_compression(Some(CompressionType::LZ4_FRAME))?;
        let writer = arrow_ipc::writer::StreamWriter::try_new_with_options(
            Vec::new(),
            &data.schema(),
            options,
        )?;

        let stream = futures::stream::try_unfold((data, writer), move |(mut data, mut writer)| {
            async move {
                match data.next().await {
                    Some(Ok(batch)) => {
                        writer.write(&batch)?;
                        let buffer = std::mem::take(writer.get_mut());
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
        Ok(Arc::new(Self::new(
            self.table_name.clone(),
            self.identifier.clone(),
            self.client.clone(),
            children[0].clone(),
            self.overwrite,
        )))
    }

    fn required_input_distribution(&self) -> Vec<datafusion_physical_plan::Distribution> {
        // Until we have a separate commit endpoint, we need to do all inserts in a single partition
        vec![datafusion_physical_plan::Distribution::SinglePartition]
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
        // Make a streaming request to remote server to insert data
        let input_stream = self.input.execute(partition, context)?;

        let output_schema = make_count_schema();
        let add_result_mutex = self.add_result.clone();
        let client = self.client.clone();
        let identifier = self.identifier.clone();
        let table_name = self.table_name.clone();
        let overwrite = self.overwrite;

        let fut = async move {
            let mut request = client
                .post(&format!("/v1/table/{}/insert/", identifier))
                .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);

            if overwrite {
                request = request.query(&[("mode", "overwrite")]);
            }

            let body = Self::stream_as_body(input_stream)?;
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

            // Store the add result
            {
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
