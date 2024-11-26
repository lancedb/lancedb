use std::io::Cursor;
use std::sync::{Arc, Mutex};

use crate::index::Index;
use crate::index::IndexStatistics;
use crate::query::Select;
use crate::table::AddDataMode;
use crate::utils::{supported_btree_data_type, supported_vector_data_type};
use crate::{Error, Table};
use arrow_array::RecordBatchReader;
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::TryStreamExt;
use http::header::CONTENT_TYPE;
use http::StatusCode;
use lance::arrow::json::JsonSchema;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::{ColumnAlteration, NewColumnTransform, Version};
use lance_datafusion::exec::OneShotExec;
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::{
    connection::NoData,
    error::Result,
    index::{IndexBuilder, IndexConfig},
    query::{Query, QueryExecutionOptions, VectorQuery},
    table::{
        merge::MergeInsertBuilder, AddDataBuilder, NativeTable, OptimizeAction, OptimizeStats,
        TableDefinition, TableInternal, UpdateBuilder,
    },
};

use super::client::RequestResultExt;
use super::client::{HttpSend, RestfulLanceDbClient, Sender};
use super::{ARROW_STREAM_CONTENT_TYPE, JSON_CONTENT_TYPE};

#[derive(Debug)]
pub struct RemoteTable<S: HttpSend = Sender> {
    #[allow(dead_code)]
    client: RestfulLanceDbClient<S>,
    name: String,

    version: RwLock<Option<u64>>,
}

impl<S: HttpSend> RemoteTable<S> {
    pub fn new(client: RestfulLanceDbClient<S>, name: String) -> Self {
        Self {
            client,
            name,
            version: RwLock::new(None),
        }
    }

    async fn describe(&self) -> Result<TableDescription> {
        let version = self.current_version().await;
        self.describe_version(version).await
    }

    async fn describe_version(&self, version: Option<u64>) -> Result<TableDescription> {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/describe/", self.name));

        let body = serde_json::json!({ "version": version });
        request = request.json(&body);

        let (request_id, response) = self.client.send(request, true).await?;

        let response = self.check_table_response(&request_id, response).await?;

        match response.text().await {
            Ok(body) => serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse table description: {}", e).into(),
                request_id,
                status_code: None,
            }),
            Err(err) => {
                let status_code = err.status();
                Err(Error::Http {
                    source: Box::new(err),
                    request_id,
                    status_code,
                })
            }
        }
    }

    fn reader_as_body(data: Box<dyn RecordBatchReader + Send>) -> Result<reqwest::Body> {
        // TODO: Once Phalanx supports compression, we should use it here.
        let mut writer = arrow_ipc::writer::StreamWriter::try_new(Vec::new(), &data.schema())?;

        //  Mutex is just here to make it sync. We shouldn't have any contention.
        let mut data = Mutex::new(data);
        let body_iter = std::iter::from_fn(move || match data.get_mut().unwrap().next() {
            Some(Ok(batch)) => {
                writer.write(&batch).ok()?;
                let buffer = std::mem::take(writer.get_mut());
                Some(Ok(buffer))
            }
            Some(Err(e)) => Some(Err(e)),
            None => {
                writer.finish().ok()?;
                let buffer = std::mem::take(writer.get_mut());
                Some(Ok(buffer))
            }
        });
        let body_stream = futures::stream::iter(body_iter);
        Ok(reqwest::Body::wrap_stream(body_stream))
    }

    async fn check_table_response(
        &self,
        request_id: &str,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: self.name.clone(),
            });
        }

        self.client.check_response(request_id, response).await
    }

    async fn read_arrow_stream(
        &self,
        request_id: &str,
        response: reqwest::Response,
    ) -> Result<SendableRecordBatchStream> {
        let response = self.check_table_response(request_id, response).await?;

        // There isn't a way to actually stream this data yet. I have an upstream issue:
        // https://github.com/apache/arrow-rs/issues/6420
        let body = response.bytes().await.err_to_http(request_id.into())?;
        let reader = FileReader::try_new(Cursor::new(body), None)?;
        let schema = reader.schema();
        let stream = futures::stream::iter(reader).map_err(DataFusionError::from);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn apply_query_params(body: &mut serde_json::Value, params: &Query) -> Result<()> {
        if params.offset.is_some() {
            return Err(Error::NotSupported {
                message: "Offset is not yet supported in LanceDB Cloud".into(),
            });
        }

        if let Some(limit) = params.limit {
            body["k"] = serde_json::Value::Number(serde_json::Number::from(limit));
        }

        if let Some(filter) = &params.filter {
            body["filter"] = serde_json::Value::String(filter.clone());
        }

        match &params.select {
            Select::All => {}
            Select::Columns(columns) => {
                body["columns"] = serde_json::Value::Array(
                    columns
                        .iter()
                        .map(|s| serde_json::Value::String(s.clone()))
                        .collect(),
                );
            }
            Select::Dynamic(pairs) => {
                body["columns"] = serde_json::Value::Array(
                    pairs
                        .iter()
                        .map(|(name, expr)| serde_json::json!([name, expr]))
                        .collect(),
                );
            }
        }

        if params.fast_search {
            body["fast_search"] = serde_json::Value::Bool(true);
        }

        if params.with_row_id {
            body["with_row_id"] = serde_json::Value::Bool(true);
        }

        if let Some(full_text_search) = &params.full_text_search {
            if full_text_search.wand_factor.is_some() {
                return Err(Error::NotSupported {
                    message: "Wand factor is not yet supported in LanceDB Cloud".into(),
                });
            }
            body["full_text_query"] = serde_json::json!({
                "columns": full_text_search.columns,
                "query": full_text_search.query,
            })
        }

        Ok(())
    }

    fn apply_vector_query_params(
        mut body: serde_json::Value,
        query: &VectorQuery,
    ) -> Result<Vec<serde_json::Value>> {
        Self::apply_query_params(&mut body, &query.base)?;

        // Apply general parameters, before we dispatch based on number of query vectors.
        body["prefilter"] = query.base.prefilter.into();
        body["distance_type"] = serde_json::json!(query.distance_type.unwrap_or_default());
        body["nprobes"] = query.nprobes.into();
        body["ef"] = query.ef.into();
        body["refine_factor"] = query.refine_factor.into();
        if let Some(vector_column) = query.column.as_ref() {
            body["vector_column"] = serde_json::Value::String(vector_column.clone());
        }
        if !query.use_index {
            body["bypass_vector_index"] = serde_json::Value::Bool(true);
        }

        fn vector_to_json(vector: &arrow_array::ArrayRef) -> Result<serde_json::Value> {
            match vector.data_type() {
                DataType::Float32 => {
                    let array = vector
                        .as_any()
                        .downcast_ref::<arrow_array::Float32Array>()
                        .unwrap();
                    Ok(serde_json::Value::Array(
                        array
                            .values()
                            .iter()
                            .map(|v| {
                                serde_json::Value::Number(
                                    serde_json::Number::from_f64(*v as f64).unwrap(),
                                )
                            })
                            .collect(),
                    ))
                }
                _ => Err(Error::InvalidInput {
                    message: "VectorQuery vector must be of type Float32".into(),
                }),
            }
        }

        match query.query_vector.len() {
            0 => {
                // Server takes empty vector, not null or undefined.
                body["vector"] = serde_json::Value::Array(Vec::new());
                Ok(vec![body])
            }
            1 => {
                body["vector"] = vector_to_json(&query.query_vector[0])?;
                Ok(vec![body])
            }
            _ => {
                let mut bodies = Vec::with_capacity(query.query_vector.len());
                for vector in &query.query_vector {
                    let mut body = body.clone();
                    body["vector"] = vector_to_json(vector)?;
                    bodies.push(body);
                }
                Ok(bodies)
            }
        }
    }

    async fn check_mutable(&self) -> Result<()> {
        let read_guard = self.version.read().await;
        match *read_guard {
            None => Ok(()),
            Some(version) => Err(Error::NotSupported {
                message: format!(
                    "Cannot mutate table reference fixed at version {}. Call checkout_latest() to get a mutable table reference.",
                    version
                )
            })
        }
    }

    async fn current_version(&self) -> Option<u64> {
        let read_guard = self.version.read().await;
        *read_guard
    }
}

#[derive(Deserialize)]
struct TableDescription {
    version: u64,
    schema: JsonSchema,
}

impl<S: HttpSend> std::fmt::Display for RemoteTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteTable({})", self.name)
    }
}

#[cfg(all(test, feature = "remote"))]
mod test_utils {
    use super::*;
    use crate::remote::client::test_utils::client_with_handler;
    use crate::remote::client::test_utils::MockSender;

    impl RemoteTable<MockSender> {
        pub fn new_mock<F, T>(name: String, handler: F) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            let client = client_with_handler(handler);
            Self {
                client,
                name,
                version: RwLock::new(None),
            }
        }
    }
}

#[async_trait]
impl<S: HttpSend> TableInternal for RemoteTable<S> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn as_native(&self) -> Option<&NativeTable> {
        None
    }
    fn name(&self) -> &str {
        &self.name
    }
    async fn version(&self) -> Result<u64> {
        self.describe().await.map(|desc| desc.version)
    }
    async fn checkout(&self, version: u64) -> Result<()> {
        // check that the version exists
        self.describe_version(Some(version))
            .await
            .map_err(|e| match e {
                // try to map the error to a more user-friendly error telling them
                // specifically that the version does not exist
                Error::TableNotFound { name } => Error::TableNotFound {
                    name: format!("{} (version: {})", name, version),
                },
                e => e,
            })?;

        let mut write_guard = self.version.write().await;
        *write_guard = Some(version);
        Ok(())
    }
    async fn checkout_latest(&self) -> Result<()> {
        let mut write_guard = self.version.write().await;
        *write_guard = None;
        Ok(())
    }
    async fn restore(&self) -> Result<()> {
        self.check_mutable().await?;
        Err(Error::NotSupported {
            message: "restore is not supported on LanceDB cloud.".into(),
        })
    }

    async fn list_versions(&self) -> Result<Vec<Version>> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/version/list/", self.name));
        let (request_id, response) = self.client.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;

        #[derive(Deserialize)]
        struct ListVersionsResponse {
            versions: Vec<Version>,
        }

        let body = response.text().await.err_to_http(request_id.clone())?;
        let body: ListVersionsResponse =
            serde_json::from_str(&body).map_err(|err| Error::Http {
                source: format!(
                    "Failed to parse list_versions response: {}, body: {}",
                    err, body
                )
                .into(),
                request_id,
                status_code: None,
            })?;

        Ok(body.versions)
    }

    async fn schema(&self) -> Result<SchemaRef> {
        let schema = self.describe().await?.schema;
        Ok(Arc::new(schema.try_into()?))
    }
    async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/count_rows/", self.name));

        let version = self.current_version().await;

        if let Some(filter) = filter {
            request = request.json(&serde_json::json!({ "predicate": filter, "version": version }));
        } else {
            let body = serde_json::json!({ "version": version });
            request = request.json(&body);
        }

        let (request_id, response) = self.client.send(request, true).await?;

        let response = self.check_table_response(&request_id, response).await?;

        let body = response.text().await.err_to_http(request_id.clone())?;

        serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse row count: {}", e).into(),
            request_id,
            status_code: None,
        })
    }
    async fn add(
        &self,
        add: AddDataBuilder<NoData>,
        data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        self.check_mutable().await?;
        let body = Self::reader_as_body(data)?;
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/insert/", self.name))
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
            .body(body);

        match add.mode {
            AddDataMode::Append => {}
            AddDataMode::Overwrite => {
                request = request.query(&[("mode", "overwrite")]);
            }
        }

        let (request_id, response) = self.client.send(request, false).await?;

        self.check_table_response(&request_id, response).await?;

        Ok(())
    }

    async fn create_plan(
        &self,
        query: &VectorQuery,
        _options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let request = self.client.post(&format!("/v1/table/{}/query/", self.name));

        let version = self.current_version().await;
        let body = serde_json::json!({ "version": version });
        let bodies = Self::apply_vector_query_params(body, query)?;

        let mut futures = Vec::with_capacity(bodies.len());
        for body in bodies {
            let request = request.try_clone().unwrap().json(&body);
            let future = async move {
                let (request_id, response) = self.client.send(request, true).await?;
                self.read_arrow_stream(&request_id, response).await
            };
            futures.push(future);
        }
        let streams = futures::future::try_join_all(futures).await?;
        if streams.len() == 1 {
            let stream = streams.into_iter().next().unwrap();
            Ok(Arc::new(OneShotExec::new(stream)))
        } else {
            let stream_execs = streams
                .into_iter()
                .map(|stream| Arc::new(OneShotExec::new(stream)) as Arc<dyn ExecutionPlan>)
                .collect();
            Table::multi_vector_plan(stream_execs)
        }
    }

    async fn plain_query(
        &self,
        query: &Query,
        _options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/query/", self.name))
            .header(CONTENT_TYPE, JSON_CONTENT_TYPE);

        let version = self.current_version().await;
        let mut body = serde_json::json!({ "version": version });
        Self::apply_query_params(&mut body, query)?;
        // Empty vector can be passed if no vector search is performed.
        body["vector"] = serde_json::Value::Array(Vec::new());

        let request = request.json(&body);

        let (request_id, response) = self.client.send(request, true).await?;

        let stream = self.read_arrow_stream(&request_id, response).await?;

        Ok(DatasetRecordBatchStream::new(stream))
    }
    async fn update(&self, update: UpdateBuilder) -> Result<u64> {
        self.check_mutable().await?;
        let request = self
            .client
            .post(&format!("/v1/table/{}/update/", self.name));

        let mut updates = Vec::new();
        for (column, expression) in update.columns {
            updates.push(vec![column, expression]);
        }

        let request = request.json(&serde_json::json!({
            "updates": updates,
            "predicate": update.filter,
        }));

        let (request_id, response) = self.client.send(request, false).await?;

        self.check_table_response(&request_id, response).await?;

        Ok(0) // TODO: support returning number of modified rows once supported in SaaS.
    }
    async fn delete(&self, predicate: &str) -> Result<()> {
        self.check_mutable().await?;
        let body = serde_json::json!({ "predicate": predicate });
        let request = self
            .client
            .post(&format!("/v1/table/{}/delete/", self.name))
            .json(&body);
        let (request_id, response) = self.client.send(request, false).await?;
        self.check_table_response(&request_id, response).await?;
        Ok(())
    }

    async fn create_index(&self, mut index: IndexBuilder) -> Result<()> {
        self.check_mutable().await?;
        let request = self
            .client
            .post(&format!("/v1/table/{}/create_index/", self.name));

        let column = match index.columns.len() {
            0 => {
                return Err(Error::InvalidInput {
                    message: "No columns specified".into(),
                })
            }
            1 => index.columns.pop().unwrap(),
            _ => {
                return Err(Error::NotSupported {
                    message: "Indices over multiple columns not yet supported".into(),
                })
            }
        };
        let mut body = serde_json::json!({
            "column": column
        });

        let (index_type, distance_type) = match index.index {
            // TODO: Should we pass the actual index parameters? SaaS does not
            // yet support them.
            Index::IvfPq(index) => ("IVF_PQ", Some(index.distance_type)),
            Index::IvfHnswSq(index) => ("IVF_HNSW_SQ", Some(index.distance_type)),
            Index::BTree(_) => ("BTREE", None),
            Index::Bitmap(_) => ("BITMAP", None),
            Index::LabelList(_) => ("LABEL_LIST", None),
            Index::FTS(_) => ("FTS", None),
            Index::Auto => {
                let schema = self.schema().await?;
                let field = schema
                    .field_with_name(&column)
                    .map_err(|_| Error::InvalidInput {
                        message: format!("Column {} not found in schema", column),
                    })?;
                if supported_vector_data_type(field.data_type()) {
                    ("IVF_PQ", None)
                } else if supported_btree_data_type(field.data_type()) {
                    ("BTREE", None)
                } else {
                    return Err(Error::NotSupported {
                        message: format!(
                            "there are no indices supported for the field `{}` with the data type {}",
                            field.name(),
                            field.data_type()
                        ),
                    });
                }
            }
            _ => {
                return Err(Error::NotSupported {
                    message: "Index type not supported".into(),
                })
            }
        };
        body["index_type"] = serde_json::Value::String(index_type.into());
        if let Some(distance_type) = distance_type {
            // Phalanx expects this to be lowercase right now.
            body["metric_type"] =
                serde_json::Value::String(distance_type.to_string().to_lowercase());
        }

        let request = request.json(&body);

        let (request_id, response) = self.client.send(request, false).await?;

        self.check_table_response(&request_id, response).await?;

        Ok(())
    }

    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        self.check_mutable().await?;
        let query = MergeInsertRequest::try_from(params)?;
        let body = Self::reader_as_body(new_data)?;
        let request = self
            .client
            .post(&format!("/v1/table/{}/merge_insert/", self.name))
            .query(&query)
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
            .body(body);

        let (request_id, response) = self.client.send(request, false).await?;

        self.check_table_response(&request_id, response).await?;

        Ok(())
    }
    async fn optimize(&self, _action: OptimizeAction) -> Result<OptimizeStats> {
        self.check_mutable().await?;
        Err(Error::NotSupported {
            message: "optimize is not supported on LanceDB cloud.".into(),
        })
    }
    async fn add_columns(
        &self,
        _transforms: NewColumnTransform,
        _read_columns: Option<Vec<String>>,
    ) -> Result<()> {
        self.check_mutable().await?;
        Err(Error::NotSupported {
            message: "add_columns is not yet supported.".into(),
        })
    }
    async fn alter_columns(&self, _alterations: &[ColumnAlteration]) -> Result<()> {
        self.check_mutable().await?;
        Err(Error::NotSupported {
            message: "alter_columns is not yet supported.".into(),
        })
    }
    async fn drop_columns(&self, _columns: &[&str]) -> Result<()> {
        self.check_mutable().await?;
        Err(Error::NotSupported {
            message: "drop_columns is not yet supported.".into(),
        })
    }

    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        // Make request to list the indices
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/index/list/", self.name));
        let version = self.current_version().await;
        let body = serde_json::json!({ "version": version });
        request = request.json(&body);

        let (request_id, response) = self.client.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;

        #[derive(Deserialize)]
        struct ListIndicesResponse {
            indexes: Vec<IndexConfigResponse>,
        }

        #[derive(Deserialize)]
        struct IndexConfigResponse {
            index_name: String,
            columns: Vec<String>,
        }

        let body = response.text().await.err_to_http(request_id.clone())?;
        let body: ListIndicesResponse = serde_json::from_str(&body).map_err(|err| Error::Http {
            source: format!(
                "Failed to parse list_indices response: {}, body: {}",
                err, body
            )
            .into(),
            request_id,
            status_code: None,
        })?;

        // Make request to get stats for each index, so we get the index type.
        // This is a bit inefficient, but it's the only way to get the index type.
        let mut futures = Vec::with_capacity(body.indexes.len());
        for index in body.indexes {
            let future = async move {
                match self.index_stats(&index.index_name).await {
                    Ok(Some(stats)) => Ok(Some(IndexConfig {
                        name: index.index_name,
                        index_type: stats.index_type,
                        columns: index.columns,
                    })),
                    Ok(None) => Ok(None), // The index must have been deleted since we listed it.
                    Err(e) => Err(e),
                }
            };
            futures.push(future);
        }
        let results = futures::future::try_join_all(futures).await?;
        let index_configs = results.into_iter().flatten().collect();

        Ok(index_configs)
    }

    async fn index_stats(&self, index_name: &str) -> Result<Option<IndexStatistics>> {
        let mut request = self.client.post(&format!(
            "/v1/table/{}/index/{}/stats/",
            self.name, index_name
        ));
        let version = self.current_version().await;
        let body = serde_json::json!({ "version": version });
        request = request.json(&body);

        let (request_id, response) = self.client.send(request, true).await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Ok(None);
        }

        let response = self.check_table_response(&request_id, response).await?;

        let body = response.text().await.err_to_http(request_id.clone())?;

        let stats = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse index statistics: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        Ok(Some(stats))
    }
    async fn table_definition(&self) -> Result<TableDefinition> {
        Err(Error::NotSupported {
            message: "table_definition is not supported on LanceDB cloud.".into(),
        })
    }
    fn dataset_uri(&self) -> &str {
        "NOT_SUPPORTED"
    }
}

#[derive(Serialize)]
struct MergeInsertRequest {
    on: String,
    when_matched_update_all: bool,
    when_matched_update_all_filt: Option<String>,
    when_not_matched_insert_all: bool,
    when_not_matched_by_source_delete: bool,
    when_not_matched_by_source_delete_filt: Option<String>,
}

impl TryFrom<MergeInsertBuilder> for MergeInsertRequest {
    type Error = Error;

    fn try_from(value: MergeInsertBuilder) -> Result<Self> {
        if value.on.is_empty() {
            return Err(Error::InvalidInput {
                message: "MergeInsertBuilder missing required 'on' field".into(),
            });
        } else if value.on.len() > 1 {
            return Err(Error::NotSupported {
                message: "MergeInsertBuilder only supports a single 'on' column".into(),
            });
        }
        let on = value.on[0].clone();

        Ok(Self {
            on,
            when_matched_update_all: value.when_matched_update_all,
            when_matched_update_all_filt: value.when_matched_update_all_filt,
            when_not_matched_insert_all: value.when_not_matched_insert_all,
            when_not_matched_by_source_delete: value.when_not_matched_by_source_delete,
            when_not_matched_by_source_delete_filt: value.when_not_matched_by_source_delete_filt,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, pin::Pin};

    use super::*;

    use arrow::{array::AsArray, compute::concat_batches, datatypes::Int32Type};
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use chrono::{DateTime, Utc};
    use futures::{future::BoxFuture, StreamExt, TryFutureExt};
    use lance_index::scalar::FullTextSearchQuery;
    use reqwest::Body;

    use crate::{
        index::{vector::IvfPqIndexBuilder, Index, IndexStatistics, IndexType},
        query::{ExecutableQuery, QueryBase},
        remote::ARROW_FILE_CONTENT_TYPE,
        DistanceType, Error, Table,
    };

    #[tokio::test]
    async fn test_not_found() {
        let table = Table::new_with_handler("my_table", |_| {
            http::Response::builder()
                .status(404)
                .body("table my_table not found")
                .unwrap()
        });

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let example_data = || {
            Box::new(RecordBatchIterator::new(
                [Ok(batch.clone())],
                batch.schema(),
            ))
        };

        // All endpoints should translate 404 to TableNotFound.
        let results: Vec<BoxFuture<'_, Result<()>>> = vec![
            Box::pin(table.version().map_ok(|_| ())),
            Box::pin(table.schema().map_ok(|_| ())),
            Box::pin(table.count_rows(None).map_ok(|_| ())),
            Box::pin(table.update().column("a", "a + 1").execute().map_ok(|_| ())),
            Box::pin(table.add(example_data()).execute().map_ok(|_| ())),
            Box::pin(table.merge_insert(&["test"]).execute(example_data())),
            Box::pin(table.delete("false")), // TODO: other endpoints.
        ];

        for result in results {
            let result = result.await;
            assert!(result.is_err());
            assert!(matches!(result, Err(Error::TableNotFound { name }) if name == "my_table"));
        }
    }

    #[tokio::test]
    async fn test_version() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

            http::Response::builder()
                .status(200)
                .body(r#"{"version": 42, "schema": { "fields": [] }}"#)
                .unwrap()
        });

        let version = table.version().await.unwrap();
        assert_eq!(version, 42);
    }

    #[tokio::test]
    async fn test_schema() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/describe/");

            http::Response::builder()
                .status(200)
                .body(
                    r#"{"version": 42, "schema": {"fields": [
                    {"name": "a", "type": { "type": "int32" }, "nullable": false},
                    {"name": "b", "type": { "type": "string" }, "nullable": true}
                ], "metadata": {"key": "value"}}}"#,
                )
                .unwrap()
        });

        let expected = Arc::new(
            Schema::new(vec![
                Field::new("a", DataType::Int32, false),
                Field::new("b", DataType::Utf8, true),
            ])
            .with_metadata([("key".into(), "value".into())].into()),
        );

        let schema = table.schema().await.unwrap();
        assert_eq!(schema, expected);
    }

    #[tokio::test]
    async fn test_count_rows() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/count_rows/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );
            assert_eq!(
                request.body().unwrap().as_bytes().unwrap(),
                br#"{"version":null}"#
            );

            http::Response::builder().status(200).body("42").unwrap()
        });

        let count = table.count_rows(None).await.unwrap();
        assert_eq!(count, 42);

        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/count_rows/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );
            assert_eq!(
                request.body().unwrap().as_bytes().unwrap(),
                br#"{"predicate":"a > 10","version":null}"#
            );

            http::Response::builder().status(200).body("42").unwrap()
        });

        let count = table.count_rows(Some("a > 10".into())).await.unwrap();
        assert_eq!(count, 42);
    }

    async fn collect_body(body: Body) -> Vec<u8> {
        use http_body::Body;
        let mut body = body;
        let mut data = Vec::new();
        let mut body_pin = Pin::new(&mut body);
        futures::stream::poll_fn(|cx| body_pin.as_mut().poll_frame(cx))
            .for_each(|frame| {
                data.extend_from_slice(frame.unwrap().data_ref().unwrap());
                futures::future::ready(())
            })
            .await;
        data
    }

    fn write_ipc_stream(data: &RecordBatch) -> Vec<u8> {
        let mut body = Vec::new();
        {
            let mut writer = arrow_ipc::writer::StreamWriter::try_new(&mut body, &data.schema())
                .expect("Failed to create writer");
            writer.write(data).expect("Failed to write data");
            writer.finish().expect("Failed to finish");
        }
        body
    }

    fn write_ipc_file(data: &RecordBatch) -> Vec<u8> {
        let mut body = Vec::new();
        {
            let mut writer = arrow_ipc::writer::FileWriter::try_new(&mut body, &data.schema())
                .expect("Failed to create writer");
            writer.write(data).expect("Failed to write data");
            writer.finish().expect("Failed to finish");
        }
        body
    }

    #[tokio::test]
    async fn test_add_append() {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let (sender, receiver) = std::sync::mpsc::channel();
        let table = Table::new_with_handler("my_table", move |mut request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/insert/");
            // If mode is specified, it should be "append". Append is default
            // so it's not required.
            assert!(request
                .url()
                .query_pairs()
                .filter(|(k, _)| k == "mode")
                .all(|(_, v)| v == "append"));

            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                ARROW_STREAM_CONTENT_TYPE
            );

            let mut body_out = reqwest::Body::from(Vec::new());
            std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
            sender.send(body_out).unwrap();

            http::Response::builder().status(200).body("").unwrap()
        });

        table
            .add(RecordBatchIterator::new([Ok(data.clone())], data.schema()))
            .execute()
            .await
            .unwrap();

        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let expected_body = write_ipc_stream(&data);
        assert_eq!(&body, &expected_body);
    }

    #[tokio::test]
    async fn test_add_overwrite() {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let (sender, receiver) = std::sync::mpsc::channel();
        let table = Table::new_with_handler("my_table", move |mut request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/insert/");
            assert_eq!(
                request
                    .url()
                    .query_pairs()
                    .find(|(k, _)| k == "mode")
                    .map(|kv| kv.1)
                    .as_deref(),
                Some("overwrite"),
                "Expected mode=overwrite"
            );

            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                ARROW_STREAM_CONTENT_TYPE
            );

            let mut body_out = reqwest::Body::from(Vec::new());
            std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
            sender.send(body_out).unwrap();

            http::Response::builder().status(200).body("").unwrap()
        });

        table
            .add(RecordBatchIterator::new([Ok(data.clone())], data.schema()))
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .unwrap();

        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let expected_body = write_ipc_stream(&data);
        assert_eq!(&body, &expected_body);
    }

    #[tokio::test]
    async fn test_update() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/update/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            if let Some(body) = request.body().unwrap().as_bytes() {
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let updates = value.get("updates").unwrap().as_array().unwrap();
                assert!(updates.len() == 2);

                let col_name = updates[0][0].as_str().unwrap();
                let expression = updates[0][1].as_str().unwrap();
                assert_eq!(col_name, "a");
                assert_eq!(expression, "a + 1");

                let col_name = updates[1][0].as_str().unwrap();
                let expression = updates[1][1].as_str().unwrap();
                assert_eq!(col_name, "b");
                assert_eq!(expression, "b - 1");

                let only_if = value.get("predicate").unwrap().as_str().unwrap();
                assert_eq!(only_if, "b > 10");
            }

            http::Response::builder().status(200).body("{}").unwrap()
        });

        table
            .update()
            .column("a", "a + 1")
            .column("b", "b - 1")
            .only_if("b > 10")
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_merge_insert() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let data = Box::new(RecordBatchIterator::new(
            [Ok(batch.clone())],
            batch.schema(),
        ));

        // Default parameters
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/merge_insert/");

            let params = request.url().query_pairs().collect::<HashMap<_, _>>();
            assert_eq!(params["on"], "some_col");
            assert_eq!(params["when_matched_update_all"], "false");
            assert_eq!(params["when_not_matched_insert_all"], "false");
            assert_eq!(params["when_not_matched_by_source_delete"], "false");
            assert!(!params.contains_key("when_matched_update_all_filt"));
            assert!(!params.contains_key("when_not_matched_by_source_delete_filt"));

            http::Response::builder().status(200).body("").unwrap()
        });

        table
            .merge_insert(&["some_col"])
            .execute(data)
            .await
            .unwrap();

        // All parameters specified
        let (sender, receiver) = std::sync::mpsc::channel();
        let table = Table::new_with_handler("my_table", move |mut request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/merge_insert/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                ARROW_STREAM_CONTENT_TYPE
            );

            let params = request.url().query_pairs().collect::<HashMap<_, _>>();
            assert_eq!(params["on"], "some_col");
            assert_eq!(params["when_matched_update_all"], "true");
            assert_eq!(params["when_not_matched_insert_all"], "false");
            assert_eq!(params["when_not_matched_by_source_delete"], "true");
            assert_eq!(params["when_matched_update_all_filt"], "a = 1");
            assert_eq!(params["when_not_matched_by_source_delete_filt"], "b = 2");

            let mut body_out = reqwest::Body::from(Vec::new());
            std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
            sender.send(body_out).unwrap();

            http::Response::builder().status(200).body("").unwrap()
        });
        let mut builder = table.merge_insert(&["some_col"]);
        builder
            .when_matched_update_all(Some("a = 1".into()))
            .when_not_matched_by_source_delete(Some("b = 2".into()));
        let data = Box::new(RecordBatchIterator::new(
            [Ok(batch.clone())],
            batch.schema(),
        ));
        builder.execute(data).await.unwrap();

        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let expected_body = write_ipc_stream(&batch);
        assert_eq!(&body, &expected_body);
    }

    #[tokio::test]
    async fn test_delete() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/delete/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let predicate = body.get("predicate").unwrap().as_str().unwrap();
            assert_eq!(predicate, "id in (1, 2, 3)");

            http::Response::builder().status(200).body("").unwrap()
        });

        table.delete("id in (1, 2, 3)").await.unwrap();
    }

    #[tokio::test]
    async fn test_query_vector_default_values() {
        let expected_data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let expected_data_ref = expected_data.clone();

        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let mut expected_body = serde_json::json!({
                "prefilter": true,
                "distance_type": "l2",
                "nprobes": 20,
                "k": 10,
                "ef": Option::<usize>::None,
                "refine_factor": null,
                "version": null,
            });
            // Pass vector separately to make sure it matches f32 precision.
            expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
            assert_eq!(body, expected_body);

            let response_body = write_ipc_file(&expected_data_ref);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let data = table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .execute()
            .await;
        let data = data.unwrap().collect::<Vec<_>>().await;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].as_ref().unwrap(), &expected_data);
    }

    #[tokio::test]
    async fn test_query_vector_all_params() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let mut expected_body = serde_json::json!({
                "vector_column": "my_vector",
                "prefilter": false,
                "k": 42,
                "distance_type": "cosine",
                "bypass_vector_index": true,
                "columns": ["a", "b"],
                "nprobes": 12,
                "ef": Option::<usize>::None,
                "refine_factor": 2,
                "version": null,
            });
            // Pass vector separately to make sure it matches f32 precision.
            expected_body["vector"] = vec![0.1f32, 0.2, 0.3].into();
            assert_eq!(body, expected_body);

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let response_body = write_ipc_file(&data);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let _ = table
            .query()
            .limit(42)
            .select(Select::columns(&["a", "b"]))
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .column("my_vector")
            .postfilter()
            .distance_type(crate::DistanceType::Cosine)
            .nprobes(12)
            .refine_factor(2)
            .bypass_vector_index()
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_query_fts() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let expected_body = serde_json::json!({
                "full_text_query": {
                    "columns": ["a", "b"],
                    "query": "hello world",
                },
                "k": 10,
                "vector": [],
                "with_row_id": true,
                "version": null
            });
            assert_eq!(body, expected_body);

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let response_body = write_ipc_file(&data);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let _ = table
            .query()
            .full_text_search(
                FullTextSearchQuery::new("hello world".into())
                    .columns(Some(vec!["a".into(), "b".into()])),
            )
            .with_row_id()
            .limit(10)
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_query_multiple_vectors() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );
            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let response_body = write_ipc_file(&data);
            http::Response::builder()
                .status(200)
                .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                .body(response_body)
                .unwrap()
        });

        let query = table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .add_query_vector(vec![0.4, 0.5, 0.6])
            .unwrap();
        let plan = query.explain_plan(true).await.unwrap();
        assert!(plan.contains("UnionExec"), "Plan: {}", plan);

        let results = query
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let results = concat_batches(&results[0].schema(), &results).unwrap();

        let query_index = results["query_index"].as_primitive::<Int32Type>();
        // We don't guarantee order.
        assert!(query_index.values().contains(&0));
        assert!(query_index.values().contains(&1));
    }

    #[tokio::test]
    async fn test_create_index() {
        let cases = [
            ("IVF_PQ", Some("l2"), Index::IvfPq(Default::default())),
            (
                "IVF_PQ",
                Some("cosine"),
                Index::IvfPq(IvfPqIndexBuilder::default().distance_type(DistanceType::Cosine)),
            ),
            (
                "IVF_HNSW_SQ",
                Some("l2"),
                Index::IvfHnswSq(Default::default()),
            ),
            // HNSW_PQ isn't yet supported on SaaS
            ("BTREE", None, Index::BTree(Default::default())),
            ("BITMAP", None, Index::Bitmap(Default::default())),
            ("LABEL_LIST", None, Index::LabelList(Default::default())),
            ("FTS", None, Index::FTS(Default::default())),
        ];

        for (index_type, distance_type, index) in cases {
            let table = Table::new_with_handler("my_table", move |request| {
                assert_eq!(request.method(), "POST");
                assert_eq!(request.url().path(), "/v1/table/my_table/create_index/");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );
                let body = request.body().unwrap().as_bytes().unwrap();
                let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                let mut expected_body = serde_json::json!({
                    "column": "a",
                    "index_type": index_type,
                });
                if let Some(distance_type) = distance_type {
                    expected_body["metric_type"] = distance_type.to_lowercase().into();
                }
                assert_eq!(body, expected_body);

                http::Response::builder().status(200).body("{}").unwrap()
            });

            table.create_index(&["a"], index).execute().await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_list_indices() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");

            let response_body = match request.url().path() {
                "/v1/table/my_table/index/list/" => {
                    serde_json::json!({
                        "indexes": [
                            {
                                "index_name": "vector_idx",
                                "index_uuid": "3fa85f64-5717-4562-b3fc-2c963f66afa6",
                                "columns": ["vector"],
                                "index_status": "done",
                            },
                            {
                                "index_name": "my_idx",
                                "index_uuid": "34255f64-5717-4562-b3fc-2c963f66afa6",
                                "columns": ["my_column"],
                                "index_status": "done",
                            },
                        ]
                    })
                }
                "/v1/table/my_table/index/vector_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": 0,
                        "index_type": "IVF_PQ",
                        "distance_type": "l2"
                    })
                }
                "/v1/table/my_table/index/my_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": 0,
                        "index_type": "LABEL_LIST"
                    })
                }
                path => panic!("Unexpected path: {}", path),
            };
            http::Response::builder()
                .status(200)
                .body(serde_json::to_string(&response_body).unwrap())
                .unwrap()
        });

        let indices = table.list_indices().await.unwrap();
        let expected = vec![
            IndexConfig {
                name: "vector_idx".into(),
                index_type: IndexType::IvfPq,
                columns: vec!["vector".into()],
            },
            IndexConfig {
                name: "my_idx".into(),
                index_type: IndexType::LabelList,
                columns: vec!["my_column".into()],
            },
        ];
        assert_eq!(indices, expected);
    }

    #[tokio::test]
    async fn test_list_versions() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/version/list/");

            let version1 = lance::dataset::Version {
                version: 1,
                timestamp: "2024-01-01T00:00:00Z".parse().unwrap(),
                metadata: Default::default(),
            };
            let version2 = lance::dataset::Version {
                version: 2,
                timestamp: "2024-02-01T00:00:00Z".parse().unwrap(),
                metadata: Default::default(),
            };
            let response_body = serde_json::json!({
                "versions": [
                    version1,
                    version2,
                ]
            });
            let response_body = serde_json::to_string(&response_body).unwrap();

            http::Response::builder()
                .status(200)
                .body(response_body)
                .unwrap()
        });

        let versions = table.list_versions().await.unwrap();
        assert_eq!(versions.len(), 2);
        assert_eq!(versions[0].version, 1);
        assert_eq!(
            versions[0].timestamp,
            "2024-01-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        assert_eq!(versions[1].version, 2);
        assert_eq!(
            versions[1].timestamp,
            "2024-02-01T00:00:00Z".parse::<DateTime<Utc>>().unwrap()
        );
        // assert_eq!(versions, expected);
    }

    #[tokio::test]
    async fn test_index_stats() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/stats/"
            );

            let response_body = serde_json::json!({
              "num_indexed_rows": 100000,
              "num_unindexed_rows": 0,
              "index_type": "IVF_PQ",
              "distance_type": "l2"
            });
            let response_body = serde_json::to_string(&response_body).unwrap();

            http::Response::builder()
                .status(200)
                .body(response_body)
                .unwrap()
        });
        let indices = table.index_stats("my_index").await.unwrap().unwrap();
        let expected = IndexStatistics {
            num_indexed_rows: 100000,
            num_unindexed_rows: 0,
            index_type: IndexType::IvfPq,
            distance_type: Some(DistanceType::L2),
            num_indices: None,
        };
        assert_eq!(indices, expected);

        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/stats/"
            );

            http::Response::builder().status(404).body("").unwrap()
        });
        let indices = table.index_stats("my_index").await.unwrap();
        assert!(indices.is_none());
    }

    #[tokio::test]
    async fn test_passes_version() {
        let table = Table::new_with_handler("my_table", |request| {
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let version = body
                .as_object()
                .unwrap()
                .get("version")
                .unwrap()
                .as_u64()
                .unwrap();
            assert_eq!(version, 42);

            let response_body = match request.url().path() {
                "/v1/table/my_table/describe/" => {
                    serde_json::json!({
                        "version": 42,
                        "schema": { "fields": [] }
                    })
                }
                "/v1/table/my_table/index/list/" => {
                    serde_json::json!({
                        "indexes": []
                    })
                }
                "/v1/table/my_table/index/my_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": 0,
                        "index_type": "IVF_PQ",
                        "distance_type": "l2"
                    })
                }
                "/v1/table/my_table/count_rows/" => {
                    serde_json::json!(1000)
                }
                "/v1/table/my_table/query/" => {
                    let expected_data = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                    )
                    .unwrap();
                    let expected_data_ref = expected_data.clone();
                    let response_body = write_ipc_file(&expected_data_ref);
                    return http::Response::builder()
                        .status(200)
                        .header(CONTENT_TYPE, ARROW_FILE_CONTENT_TYPE)
                        .body(response_body)
                        .unwrap();
                }

                path => panic!("Unexpected path: {}", path),
            };

            http::Response::builder()
                .status(200)
                .body(
                    serde_json::to_string(&response_body)
                        .unwrap()
                        .as_bytes()
                        .to_vec(),
                )
                .unwrap()
        });

        table.checkout(42).await.unwrap();

        // ensure that version is passed to the /describe endpoint
        let version = table.version().await.unwrap();
        assert_eq!(version, 42);

        // ensure it's passed to other read API calls
        table.list_indices().await.unwrap();
        table.index_stats("my_idx").await.unwrap();
        table.count_rows(None).await.unwrap();
        table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_fails_if_checkout_version_doesnt_exist() {
        let table = Table::new_with_handler("my_table", |request| {
            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let version = body
                .as_object()
                .unwrap()
                .get("version")
                .unwrap()
                .as_u64()
                .unwrap();
            if version != 42 {
                return http::Response::builder()
                    .status(404)
                    .body(format!("Table my_table (version: {}) not found", version))
                    .unwrap();
            }

            let response_body = match request.url().path() {
                "/v1/table/my_table/describe/" => {
                    serde_json::json!({
                        "version": 42,
                        "schema": { "fields": [] }
                    })
                }
                _ => panic!("Unexpected path"),
            };

            http::Response::builder()
                .status(200)
                .body(serde_json::to_string(&response_body).unwrap())
                .unwrap()
        });

        let res = table.checkout(43).await;
        println!("{:?}", res);
        assert!(
            matches!(res, Err(Error::TableNotFound { name }) if name == "my_table (version: 43)")
        );
    }

    #[tokio::test]
    async fn test_timetravel_immutable() {
        let table = Table::new_with_handler::<String>("my_table", |request| {
            let response_body = match request.url().path() {
                "/v1/table/my_table/describe/" => {
                    serde_json::json!({
                        "version": 42,
                        "schema": { "fields": [] }
                    })
                }
                _ => panic!("Should not have made a request: {:?}", request),
            };

            http::Response::builder()
                .status(200)
                .body(serde_json::to_string(&response_body).unwrap())
                .unwrap()
        });

        table.checkout(42).await.unwrap();

        // Ensure that all mutable operations fail.
        let res = table
            .update()
            .column("a", "a + 1")
            .column("b", "b - 1")
            .only_if("b > 10")
            .execute()
            .await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));

        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let data = Box::new(RecordBatchIterator::new(
            [Ok(batch.clone())],
            batch.schema(),
        ));
        let res = table.merge_insert(&["some_col"]).execute(data).await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));

        let res = table.delete("id in (1, 2, 3)").await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));

        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let res = table
            .add(RecordBatchIterator::new([Ok(data.clone())], data.schema()))
            .execute()
            .await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));

        let res = table
            .create_index(&["a"], Index::IvfPq(Default::default()))
            .execute()
            .await;
        assert!(matches!(res, Err(Error::NotSupported { .. })));
    }
}
