// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::index::Index;
use crate::index::IndexStatistics;
use crate::query::{QueryFilter, QueryRequest, Select, VectorQueryRequest};
use crate::table::AddColumnsResult;
use crate::table::AddResult;
use crate::table::AlterColumnsResult;
use crate::table::DeleteResult;
use crate::table::DropColumnsResult;
use crate::table::MergeResult;
use crate::table::Tags;
use crate::table::UpdateResult;
use crate::table::{AddDataMode, AnyQuery, Filter, TableStatistics};
use crate::utils::{supported_btree_data_type, supported_vector_data_type};
use crate::{DistanceType, Error, Table};
use arrow_array::{RecordBatch, RecordBatchIterator, RecordBatchReader};
use arrow_ipc::reader::FileReader;
use arrow_schema::{DataType, SchemaRef};
use async_trait::async_trait;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{ExecutionPlan, RecordBatchStream, SendableRecordBatchStream};
use futures::TryStreamExt;
use http::header::CONTENT_TYPE;
use http::{HeaderName, StatusCode};
use lance::arrow::json::{JsonDataType, JsonSchema};
use lance::dataset::refs::TagContents;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::{ColumnAlteration, NewColumnTransform, Version};
use lance_datafusion::exec::{execute_plan, OneShotExec};
use reqwest::{RequestBuilder, Response};
use serde::{Deserialize, Serialize};
use serde_json::Number;
use std::collections::HashMap;
use std::io::Cursor;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::RwLock;

use super::client::RequestResultExt;
use super::client::{HttpSend, RestfulLanceDbClient, Sender};
use super::db::ServerVersion;
use super::ARROW_STREAM_CONTENT_TYPE;
use crate::index::waiter::wait_for_index;
use crate::{
    connection::NoData,
    error::Result,
    index::{IndexBuilder, IndexConfig},
    query::QueryExecutionOptions,
    table::{
        merge::MergeInsertBuilder, AddDataBuilder, BaseTable, OptimizeAction, OptimizeStats,
        TableDefinition, UpdateBuilder,
    },
};

const REQUEST_TIMEOUT_HEADER: HeaderName = HeaderName::from_static("x-request-timeout-ms");
const METRIC_TYPE_KEY: &str = "metric_type";
const INDEX_TYPE_KEY: &str = "index_type";

pub struct RemoteTags<'a, S: HttpSend = Sender> {
    inner: &'a RemoteTable<S>,
}

#[async_trait]
impl<S: HttpSend + 'static> Tags for RemoteTags<'_, S> {
    async fn list(&self) -> Result<HashMap<String, TagContents>> {
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/list/", self.inner.identifier));
        let (request_id, response) = self.inner.send(request, true).await?;
        let response = self
            .inner
            .check_table_response(&request_id, response)
            .await?;

        match response.text().await {
            Ok(body) => {
                // Explicitly tell serde_json what type we want to deserialize into
                let tags_map: HashMap<String, TagContents> =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse tags list: {}", e).into(),
                        request_id,
                        status_code: None,
                    })?;

                Ok(tags_map)
            }
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

    async fn get_version(&self, tag: &str) -> Result<u64> {
        let request = self
            .inner
            .client
            .post(&format!(
                "/v1/table/{}/tags/version/",
                self.inner.identifier
            ))
            .json(&serde_json::json!({ "tag": tag }));

        let (request_id, response) = self.inner.send(request, true).await?;
        let response = self
            .inner
            .check_table_response(&request_id, response)
            .await?;

        match response.text().await {
            Ok(body) => {
                let value: serde_json::Value =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse tag version: {}", e).into(),
                        request_id: request_id.clone(),
                        status_code: None,
                    })?;

                value
                    .get("version")
                    .and_then(|v| v.as_u64())
                    .ok_or_else(|| Error::Http {
                        source: format!("Invalid tag version response: {}", body).into(),
                        request_id,
                        status_code: None,
                    })
            }
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

    async fn create(&mut self, tag: &str, version: u64) -> Result<()> {
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/create/", self.inner.identifier))
            .json(&serde_json::json!({
                "tag": tag,
                "version": version
            }));

        let (request_id, response) = self.inner.send(request, true).await?;
        self.inner
            .check_table_response(&request_id, response)
            .await?;
        Ok(())
    }

    async fn delete(&mut self, tag: &str) -> Result<()> {
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/delete/", self.inner.identifier))
            .json(&serde_json::json!({ "tag": tag }));

        let (request_id, response) = self.inner.send(request, true).await?;
        self.inner
            .check_table_response(&request_id, response)
            .await?;
        Ok(())
    }

    async fn update(&mut self, tag: &str, version: u64) -> Result<()> {
        let request = self
            .inner
            .client
            .post(&format!("/v1/table/{}/tags/update/", self.inner.identifier))
            .json(&serde_json::json!({
                "tag": tag,
                "version": version
            }));

        let (request_id, response) = self.inner.send(request, true).await?;
        self.inner
            .check_table_response(&request_id, response)
            .await?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct RemoteTable<S: HttpSend = Sender> {
    #[allow(dead_code)]
    client: RestfulLanceDbClient<S>,
    name: String,
    namespace: Vec<String>,
    identifier: String,
    server_version: ServerVersion,

    version: RwLock<Option<u64>>,
}

impl<S: HttpSend> RemoteTable<S> {
    pub fn new(
        client: RestfulLanceDbClient<S>,
        name: String,
        namespace: Vec<String>,
        identifier: String,
        server_version: ServerVersion,
    ) -> Self {
        Self {
            client,
            name,
            namespace,
            identifier,
            server_version,
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
            .post(&format!("/v1/table/{}/describe/", self.identifier));

        let body = serde_json::json!({ "version": version });
        request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;

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

    /// Buffer the reader into memory
    async fn buffer_reader<R: RecordBatchReader + ?Sized>(
        reader: &mut R,
    ) -> Result<(SchemaRef, Vec<RecordBatch>)> {
        let schema = reader.schema();
        let mut batches = Vec::new();
        for batch in reader {
            batches.push(batch?);
        }
        Ok((schema, batches))
    }

    /// Create a new RecordBatchReader from buffered data
    fn make_reader(schema: SchemaRef, batches: Vec<RecordBatch>) -> impl RecordBatchReader {
        let iter = batches.into_iter().map(Ok);
        RecordBatchIterator::new(iter, schema)
    }

    async fn send(&self, req: RequestBuilder, with_retry: bool) -> Result<(String, Response)> {
        let res = if with_retry {
            self.client.send_with_retry(req, None, true).await?
        } else {
            self.client.send(req).await?
        };
        Ok(res)
    }

    /// Send the request with streaming body.
    /// This will use retries if with_retry is set and the number of configured retries is > 0.
    /// If retries are enabled, the stream will be buffered into memory.
    async fn send_streaming(
        &self,
        req: RequestBuilder,
        mut data: Box<dyn RecordBatchReader + Send>,
        with_retry: bool,
    ) -> Result<(String, Response)> {
        if !with_retry || self.client.retry_config.retries == 0 {
            let body = Self::reader_as_body(data)?;
            return self.client.send(req.body(body)).await;
        }

        // to support retries, buffer into memory and clone the batches on each retry
        let (schema, batches) = Self::buffer_reader(&mut *data).await?;
        let make_body = Box::new(move || {
            let reader = Self::make_reader(schema.clone(), batches.clone());
            Self::reader_as_body(Box::new(reader))
        });
        let res = self
            .client
            .send_with_retry(req, Some(make_body), false)
            .await?;

        Ok(res)
    }

    async fn check_table_response(
        &self,
        request_id: &str,
        response: reqwest::Response,
    ) -> Result<reqwest::Response> {
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: self.identifier.clone(),
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

    fn apply_query_params(
        &self,
        body: &mut serde_json::Value,
        params: &QueryRequest,
    ) -> Result<()> {
        body["prefilter"] = params.prefilter.into();
        if let Some(offset) = params.offset {
            body["offset"] = serde_json::Value::Number(serde_json::Number::from(offset));
        }

        // Server requires k.
        // use isize::MAX as usize to avoid overflow: https://github.com/lancedb/lancedb/issues/2211
        let limit = params.limit.unwrap_or(isize::MAX as usize);
        body["k"] = serde_json::Value::Number(serde_json::Number::from(limit));

        if let Some(filter) = &params.filter {
            if let QueryFilter::Sql(filter) = filter {
                body["filter"] = serde_json::Value::String(filter.clone());
            } else {
                return Err(Error::NotSupported {
                    message: "querying a remote table with a non-sql filter".to_string(),
                });
            }
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

            if self.server_version.support_structural_fts() {
                body["full_text_query"] = serde_json::json!({
                    "query": full_text_search.query.clone(),
                });
            } else {
                body["full_text_query"] = serde_json::json!({
                    "columns": full_text_search.columns().into_iter().collect::<Vec<_>>(),
                    "query": full_text_search.query.query(),
                })
            }
        }

        Ok(())
    }

    fn apply_vector_query_params(
        &self,
        mut body: serde_json::Value,
        query: &VectorQueryRequest,
    ) -> Result<Vec<serde_json::Value>> {
        self.apply_query_params(&mut body, &query.base)?;

        // Apply general parameters, before we dispatch based on number of query vectors.
        body["distance_type"] = serde_json::json!(query.distance_type.unwrap_or_default());
        // In 0.23.1 we migrated from `nprobes` to `minimum_nprobes` and `maximum_nprobes`.
        // Old client / new server: since minimum_nprobes is missing, fallback to nprobes
        // New client / old server: old server will only see nprobes, make sure to set both
        //                          nprobes and minimum_nprobes
        // New client / new server: since minimum_nprobes is present, server can ignore nprobes
        body["nprobes"] = query.minimum_nprobes.into();
        body["minimum_nprobes"] = query.minimum_nprobes.into();
        if let Some(maximum_nprobes) = query.maximum_nprobes {
            body["maximum_nprobes"] = maximum_nprobes.into();
        } else {
            body["maximum_nprobes"] = serde_json::Value::Number(Number::from_u128(0).unwrap())
        }
        body["lower_bound"] = query.lower_bound.into();
        body["upper_bound"] = query.upper_bound.into();
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

        let bodies = match query.query_vector.len() {
            0 => {
                // Server takes empty vector, not null or undefined.
                body["vector"] = serde_json::Value::Array(Vec::new());
                vec![body]
            }
            1 => {
                body["vector"] = vector_to_json(&query.query_vector[0])?;
                vec![body]
            }
            _ => {
                if self.server_version.support_multivector() {
                    let vectors = query
                        .query_vector
                        .iter()
                        .map(vector_to_json)
                        .collect::<Result<Vec<_>>>()?;
                    body["vector"] = serde_json::Value::Array(vectors);
                    vec![body]
                } else {
                    // Server does not support multiple vectors in a single query.
                    // We need to send multiple requests.
                    let mut bodies = Vec::with_capacity(query.query_vector.len());
                    for vector in &query.query_vector {
                        let mut body = body.clone();
                        body["vector"] = vector_to_json(vector)?;
                        bodies.push(body);
                    }
                    bodies
                }
            }
        };

        Ok(bodies)
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

    async fn execute_query(
        &self,
        query: &AnyQuery,
        options: &QueryExecutionOptions,
    ) -> Result<Vec<Pin<Box<dyn RecordBatchStream + Send>>>> {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/query/", self.identifier));

        if let Some(timeout) = options.timeout {
            // Also send to server, so it can abort the query if it takes too long.
            // (If it doesn't fit into u64, it's not worth sending anyways.)
            if let Ok(timeout_ms) = u64::try_from(timeout.as_millis()) {
                request = request.header(REQUEST_TIMEOUT_HEADER, timeout_ms);
            }
        }

        let query_bodies = self.prepare_query_bodies(query).await?;
        let requests: Vec<reqwest::RequestBuilder> = query_bodies
            .into_iter()
            .map(|body| request.try_clone().unwrap().json(&body))
            .collect();

        let futures = requests.into_iter().map(|req| async move {
            let (request_id, response) = self.send(req, true).await?;
            self.read_arrow_stream(&request_id, response).await
        });
        let streams = futures::future::try_join_all(futures);

        if let Some(timeout) = options.timeout {
            let timeout_future = tokio::time::sleep(timeout);
            tokio::pin!(timeout_future);
            tokio::pin!(streams);
            tokio::select! {
                _ = &mut timeout_future => {
                    Err(Error::Other {
                        message: format!("Query timeout after {} ms", timeout.as_millis()),
                        source: None,
                    })
                }
                result = &mut streams => {
                    Ok(result?)
                }
            }
        } else {
            Ok(streams.await?)
        }
    }

    async fn prepare_query_bodies(&self, query: &AnyQuery) -> Result<Vec<serde_json::Value>> {
        let version = self.current_version().await;
        let base_body = serde_json::json!({ "version": version });

        match query {
            AnyQuery::Query(query) => {
                let mut body = base_body.clone();
                self.apply_query_params(&mut body, query)?;
                // Empty vector can be passed if no vector search is performed.
                body["vector"] = serde_json::Value::Array(Vec::new());
                Ok(vec![body])
            }
            AnyQuery::VectorQuery(query) => self.apply_vector_query_params(base_body, query),
        }
    }
}

#[derive(Deserialize)]
struct TableDescription {
    version: u64,
    schema: JsonSchema,
}

impl<S: HttpSend> std::fmt::Display for RemoteTable<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteTable({})", self.identifier)
    }
}

#[cfg(all(test, feature = "remote"))]
mod test_utils {
    use super::*;
    use crate::remote::client::test_utils::client_with_handler;
    use crate::remote::client::test_utils::MockSender;

    impl RemoteTable<MockSender> {
        pub fn new_mock<F, T>(name: String, handler: F, version: Option<semver::Version>) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            let client = client_with_handler(handler);
            Self {
                client,
                name: name.clone(),
                namespace: vec![],
                identifier: name,
                server_version: version.map(ServerVersion).unwrap_or_default(),
                version: RwLock::new(None),
            }
        }
    }
}

#[async_trait]
impl<S: HttpSend> BaseTable for RemoteTable<S> {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        &self.name
    }

    fn namespace(&self) -> &[String] {
        &self.namespace
    }

    fn id(&self) -> &str {
        &self.identifier
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
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/restore/", self.identifier));
        let version = self.current_version().await;
        let body = serde_json::json!({ "version": version });
        request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;
        self.check_table_response(&request_id, response).await?;
        self.checkout_latest().await?;
        Ok(())
    }

    async fn list_versions(&self) -> Result<Vec<Version>> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/version/list/", self.identifier));
        let (request_id, response) = self.send(request, true).await?;
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
    async fn count_rows(&self, filter: Option<Filter>) -> Result<usize> {
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/count_rows/", self.identifier));

        let version = self.current_version().await;

        if let Some(filter) = filter {
            let Filter::Sql(filter) = filter else {
                return Err(Error::NotSupported {
                    message: "querying a remote table with a datafusion filter".to_string(),
                });
            };
            request = request.json(&serde_json::json!({ "predicate": filter, "version": version }));
        } else {
            let body = serde_json::json!({ "version": version });
            request = request.json(&body);
        }

        let (request_id, response) = self.send(request, true).await?;

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
    ) -> Result<AddResult> {
        self.check_mutable().await?;
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/insert/", self.identifier))
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);

        match add.mode {
            AddDataMode::Append => {}
            AddDataMode::Overwrite => {
                request = request.query(&[("mode", "overwrite")]);
            }
        }

        let (request_id, response) = self.send_streaming(request, data, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(AddResult { version: 0 });
        }

        let add_response: AddResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse add response: {}", e).into(),
            request_id,
            status_code: None,
        })?;
        Ok(add_response)
    }

    async fn create_plan(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let streams = self.execute_query(query, &options).await?;
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

    async fn query(
        &self,
        query: &AnyQuery,
        options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        let streams = self.execute_query(query, &options).await?;

        if streams.len() == 1 {
            Ok(DatasetRecordBatchStream::new(
                streams.into_iter().next().unwrap(),
            ))
        } else {
            let stream_execs = streams
                .into_iter()
                .map(|stream| Arc::new(OneShotExec::new(stream)) as Arc<dyn ExecutionPlan>)
                .collect();
            let plan = Table::multi_vector_plan(stream_execs)?;

            Ok(DatasetRecordBatchStream::new(execute_plan(
                plan,
                Default::default(),
            )?))
        }
    }

    async fn explain_plan(&self, query: &AnyQuery, verbose: bool) -> Result<String> {
        let base_request = self
            .client
            .post(&format!("/v1/table/{}/explain_plan/", self.identifier));

        let query_bodies = self.prepare_query_bodies(query).await?;
        let requests: Vec<reqwest::RequestBuilder> = query_bodies
            .into_iter()
            .map(|query_body| {
                let explain_request = serde_json::json!({
                    "verbose": verbose,
                    "query": query_body
                });

                base_request.try_clone().unwrap().json(&explain_request)
            })
            .collect::<Vec<_>>();

        let futures = requests.into_iter().map(|req| async move {
            let (request_id, response) = self.send(req, true).await?;
            let response = self.check_table_response(&request_id, response).await?;
            let body = response.text().await.err_to_http(request_id.clone())?;

            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse explain plan: {}", e).into(),
                request_id,
                status_code: None,
            })
        });

        let plan_texts = futures::future::try_join_all(futures).await?;
        let final_plan = if plan_texts.len() > 1 {
            plan_texts
                .into_iter()
                .enumerate()
                .map(|(i, plan)| format!("--- Plan #{} ---\n{}", i + 1, plan))
                .collect::<Vec<_>>()
                .join("\n\n")
        } else {
            plan_texts.into_iter().next().unwrap_or_default()
        };

        Ok(final_plan)
    }

    async fn analyze_plan(
        &self,
        query: &AnyQuery,
        _options: QueryExecutionOptions,
    ) -> Result<String> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/analyze_plan/", self.identifier));

        let query_bodies = self.prepare_query_bodies(query).await?;
        let requests: Vec<reqwest::RequestBuilder> = query_bodies
            .into_iter()
            .map(|body| request.try_clone().unwrap().json(&body))
            .collect();

        let futures = requests.into_iter().map(|req| async move {
            let (request_id, response) = self.send(req, true).await?;
            let response = self.check_table_response(&request_id, response).await?;
            let body = response.text().await.err_to_http(request_id.clone())?;

            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to execute analyze plan: {}", e).into(),
                request_id,
                status_code: None,
            })
        });

        let analyze_result_texts = futures::future::try_join_all(futures).await?;
        let final_analyze = if analyze_result_texts.len() > 1 {
            analyze_result_texts
                .into_iter()
                .enumerate()
                .map(|(i, plan)| format!("--- Query #{} ---\n{}", i + 1, plan))
                .collect::<Vec<_>>()
                .join("\n\n")
        } else {
            analyze_result_texts.into_iter().next().unwrap_or_default()
        };

        Ok(final_analyze)
    }

    async fn update(&self, update: UpdateBuilder) -> Result<UpdateResult> {
        self.check_mutable().await?;
        let request = self
            .client
            .post(&format!("/v1/table/{}/update/", self.identifier));

        let mut updates = Vec::new();
        for (column, expression) in update.columns {
            updates.push(vec![column, expression]);
        }

        let request = request.json(&serde_json::json!({
            "updates": updates,
            "predicate": update.filter,
        }));

        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(UpdateResult {
                rows_updated: 0,
                version: 0,
            });
        }

        let update_response: UpdateResult =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse update response: {}", e).into(),
                request_id,
                status_code: None,
            })?;

        Ok(update_response)
    }

    async fn delete(&self, predicate: &str) -> Result<DeleteResult> {
        self.check_mutable().await?;
        let body = serde_json::json!({ "predicate": predicate });
        let request = self
            .client
            .post(&format!("/v1/table/{}/delete/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;
        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(DeleteResult { version: 0 });
        }
        let delete_response: DeleteResult =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse delete response: {}", e).into(),
                request_id,
                status_code: None,
            })?;
        Ok(delete_response)
    }

    async fn create_index(&self, mut index: IndexBuilder) -> Result<()> {
        self.check_mutable().await?;
        let request = self
            .client
            .post(&format!("/v1/table/{}/create_index/", self.identifier));

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

        // Add name parameter if provided (for backwards compatibility, only include if Some)
        if let Some(ref name) = index.name {
            body["name"] = serde_json::Value::String(name.clone());
        }

        // Warn if train=false is specified since it's not meaningful
        if !index.train {
            log::warn!(
                "train=false has no effect remote tables. The index will be created empty and automatically populated in the background."
            );
        }

        match index.index {
            // TODO: Should we pass the actual index parameters? SaaS does not
            // yet support them.
            Index::IvfFlat(index) => {
                body[INDEX_TYPE_KEY] = serde_json::Value::String("IVF_FLAT".to_string());
                body[METRIC_TYPE_KEY] =
                    serde_json::Value::String(index.distance_type.to_string().to_lowercase());
                if let Some(num_partitions) = index.num_partitions {
                    body["num_partitions"] = serde_json::Value::Number(num_partitions.into());
                }
            }
            Index::IvfPq(index) => {
                body[INDEX_TYPE_KEY] = serde_json::Value::String("IVF_PQ".to_string());
                body[METRIC_TYPE_KEY] =
                    serde_json::Value::String(index.distance_type.to_string().to_lowercase());
                if let Some(num_partitions) = index.num_partitions {
                    body["num_partitions"] = serde_json::Value::Number(num_partitions.into());
                }
                if let Some(num_bits) = index.num_bits {
                    body["num_bits"] = serde_json::Value::Number(num_bits.into());
                }
            }
            Index::IvfHnswSq(index) => {
                body[INDEX_TYPE_KEY] = serde_json::Value::String("IVF_HNSW_SQ".to_string());
                body[METRIC_TYPE_KEY] =
                    serde_json::Value::String(index.distance_type.to_string().to_lowercase());
                if let Some(num_partitions) = index.num_partitions {
                    body["num_partitions"] = serde_json::Value::Number(num_partitions.into());
                }
            }
            Index::BTree(_) => {
                body[INDEX_TYPE_KEY] = serde_json::Value::String("BTREE".to_string());
            }
            Index::Bitmap(_) => {
                body[INDEX_TYPE_KEY] = serde_json::Value::String("BITMAP".to_string());
            }
            Index::LabelList(_) => {
                body[INDEX_TYPE_KEY] = serde_json::Value::String("LABEL_LIST".to_string());
            }
            Index::FTS(fts) => {
                body[INDEX_TYPE_KEY] = serde_json::Value::String("FTS".to_string());
                let params = serde_json::to_value(&fts).map_err(|e| Error::InvalidInput {
                    message: format!("failed to serialize FTS index params {:?}", e),
                })?;
                for (key, value) in params.as_object().unwrap() {
                    body[key] = value.clone();
                }
            }
            Index::Auto => {
                let schema = self.schema().await?;
                let field = schema
                    .field_with_name(&column)
                    .map_err(|_| Error::InvalidInput {
                        message: format!("Column {} not found in schema", column),
                    })?;
                if supported_vector_data_type(field.data_type()) {
                    body[INDEX_TYPE_KEY] = serde_json::Value::String("IVF_PQ".to_string());
                    body[METRIC_TYPE_KEY] =
                        serde_json::Value::String(DistanceType::L2.to_string().to_lowercase());
                } else if supported_btree_data_type(field.data_type()) {
                    body[INDEX_TYPE_KEY] = serde_json::Value::String("BTREE".to_string());
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

        let request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;

        self.check_table_response(&request_id, response).await?;

        if let Some(wait_timeout) = index.wait_timeout {
            let index_name = index.name.unwrap_or_else(|| format!("{}_idx", column));
            self.wait_for_index(&[&index_name], wait_timeout).await?;
        }

        Ok(())
    }

    /// Poll until the columns are fully indexed. Will return Error::Timeout if the columns
    /// are not fully indexed within the timeout.
    async fn wait_for_index(&self, index_names: &[&str], timeout: Duration) -> Result<()> {
        wait_for_index(self, index_names, timeout).await
    }

    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<MergeResult> {
        self.check_mutable().await?;

        let timeout = params.timeout;

        let query = MergeInsertRequest::try_from(params)?;
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/merge_insert/", self.identifier))
            .query(&query)
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);

        if let Some(timeout) = timeout {
            // (If it doesn't fit into u64, it's not worth sending anyways.)
            if let Ok(timeout_ms) = u64::try_from(timeout.as_millis()) {
                request = request.header(REQUEST_TIMEOUT_HEADER, timeout_ms);
            }
        }

        let (request_id, response) = self.send_streaming(request, new_data, true).await?;

        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(MergeResult {
                version: 0,
                num_deleted_rows: 0,
                num_inserted_rows: 0,
                num_updated_rows: 0,
            });
        }

        let merge_insert_response: MergeResult =
            serde_json::from_str(&body).map_err(|e| Error::Http {
                source: format!("Failed to parse merge_insert response: {}", e).into(),
                request_id,
                status_code: None,
            })?;

        Ok(merge_insert_response)
    }

    async fn tags(&self) -> Result<Box<dyn Tags + '_>> {
        Ok(Box::new(RemoteTags { inner: self }))
    }
    async fn checkout_tag(&self, tag: &str) -> Result<()> {
        let tags = self.tags().await?;
        let version = tags.get_version(tag).await?;
        let mut write_guard = self.version.write().await;
        *write_guard = Some(version);
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
        transforms: NewColumnTransform,
        _read_columns: Option<Vec<String>>,
    ) -> Result<AddColumnsResult> {
        self.check_mutable().await?;
        match transforms {
            NewColumnTransform::SqlExpressions(expressions) => {
                let body = expressions
                    .into_iter()
                    .map(|(name, expression)| {
                        serde_json::json!({
                            "name": name,
                            "expression": expression,
                        })
                    })
                    .collect::<Vec<_>>();
                let body = serde_json::json!({ "new_columns": body });
                let request = self
                    .client
                    .post(&format!("/v1/table/{}/add_columns/", self.identifier))
                    .json(&body);
                let (request_id, response) = self.send(request, true).await?;
                let response = self.check_table_response(&request_id, response).await?;
                let body = response.text().await.err_to_http(request_id.clone())?;

                if body.trim().is_empty() {
                    // Backward compatible with old servers
                    return Ok(AddColumnsResult { version: 0 });
                }

                let result: AddColumnsResult =
                    serde_json::from_str(&body).map_err(|e| Error::Http {
                        source: format!("Failed to parse add_columns response: {}", e).into(),
                        request_id,
                        status_code: None,
                    })?;

                Ok(result)
            }
            _ => {
                return Err(Error::NotSupported {
                    message: "Only SQL expressions are supported for adding columns".into(),
                });
            }
        }
    }

    async fn alter_columns(&self, alterations: &[ColumnAlteration]) -> Result<AlterColumnsResult> {
        self.check_mutable().await?;
        let body = alterations
            .iter()
            .map(|alteration| {
                let mut value = serde_json::json!({
                    "path": alteration.path,
                });
                if let Some(rename) = &alteration.rename {
                    value["rename"] = serde_json::Value::String(rename.clone());
                }
                if let Some(data_type) = &alteration.data_type {
                    let json_data_type = JsonDataType::try_from(data_type).unwrap();
                    let json_data_type = serde_json::to_value(&json_data_type).unwrap();
                    value["data_type"] = json_data_type;
                }
                if let Some(nullable) = &alteration.nullable {
                    value["nullable"] = serde_json::Value::Bool(*nullable);
                }
                value
            })
            .collect::<Vec<_>>();
        let body = serde_json::json!({ "alterations": body });
        let request = self
            .client
            .post(&format!("/v1/table/{}/alter_columns/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(AlterColumnsResult { version: 0 });
        }

        let result: AlterColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse alter_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        Ok(result)
    }

    async fn drop_columns(&self, columns: &[&str]) -> Result<DropColumnsResult> {
        self.check_mutable().await?;
        let body = serde_json::json!({ "columns": columns });
        let request = self
            .client
            .post(&format!("/v1/table/{}/drop_columns/", self.identifier))
            .json(&body);
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        if body.trim().is_empty() {
            // Backward compatible with old servers
            return Ok(DropColumnsResult { version: 0 });
        }

        let result: DropColumnsResult = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse drop_columns response: {}", e).into(),
            request_id,
            status_code: None,
        })?;

        Ok(result)
    }

    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        // Make request to list the indices
        let mut request = self
            .client
            .post(&format!("/v1/table/{}/index/list/", self.identifier));
        let version = self.current_version().await;
        let body = serde_json::json!({ "version": version });
        request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;
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
            self.identifier, index_name
        ));
        let version = self.current_version().await;
        let body = serde_json::json!({ "version": version });
        request = request.json(&body);

        let (request_id, response) = self.send(request, true).await?;

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

    async fn drop_index(&self, index_name: &str) -> Result<()> {
        let request = self.client.post(&format!(
            "/v1/table/{}/index/{}/drop/",
            self.identifier, index_name
        ));
        let (request_id, response) = self.send(request, true).await?;
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::IndexNotFound {
                name: index_name.to_string(),
            });
        };
        self.client.check_response(&request_id, response).await?;
        Ok(())
    }

    async fn prewarm_index(&self, _index_name: &str) -> Result<()> {
        Err(Error::NotSupported {
            message: "prewarm_index is not yet supported on LanceDB cloud.".into(),
        })
    }

    async fn table_definition(&self) -> Result<TableDefinition> {
        Err(Error::NotSupported {
            message: "table_definition is not supported on LanceDB cloud.".into(),
        })
    }
    fn dataset_uri(&self) -> &str {
        "NOT_SUPPORTED"
    }

    async fn stats(&self) -> Result<TableStatistics> {
        let request = self
            .client
            .post(&format!("/v1/table/{}/stats/", self.identifier));
        let (request_id, response) = self.send(request, true).await?;
        let response = self.check_table_response(&request_id, response).await?;
        let body = response.text().await.err_to_http(request_id.clone())?;

        let stats = serde_json::from_str(&body).map_err(|e| Error::Http {
            source: format!("Failed to parse table statistics: {}", e).into(),
            request_id,
            status_code: None,
        })?;
        Ok(stats)
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
    use lance_index::scalar::inverted::query::MatchQuery;
    use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams};
    use reqwest::Body;
    use rstest::rstest;
    use serde_json::json;

    use crate::index::vector::{IvfFlatIndexBuilder, IvfHnswSqIndexBuilder};
    use crate::remote::db::DEFAULT_SERVER_VERSION;
    use crate::remote::JSON_CONTENT_TYPE;
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
            Box::pin(
                table
                    .merge_insert(&["test"])
                    .execute(example_data())
                    .map_ok(|_| ()),
            ),
            Box::pin(table.delete("false").map_ok(|_| ())),
            Box::pin(
                table
                    .add_columns(
                        NewColumnTransform::SqlExpressions(vec![("x".into(), "y".into())]),
                        None,
                    )
                    .map_ok(|_| ()),
            ),
            Box::pin(async {
                let alterations = vec![ColumnAlteration::new("x".into()).rename("y".into())];
                table.alter_columns(&alterations).await.map(|_| ())
            }),
            Box::pin(table.drop_columns(&["a"]).map_ok(|_| ())),
            // TODO: other endpoints.
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

    #[rstest]
    #[case("", 0)]
    #[case("{}", 0)]
    #[case(r#"{"request_id": "test-request-id"}"#, 0)]
    #[case(r#"{"version": 43}"#, 43)]
    #[tokio::test]
    async fn test_add_append(#[case] response_body: &str, #[case] expected_version: u64) {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        // Clone response_body to give it 'static lifetime for the closure
        let response_body = response_body.to_string();

        let (sender, receiver) = std::sync::mpsc::channel();
        let table = Table::new_with_handler("my_table", move |mut request| {
            if request.url().path() == "/v1/table/my_table/insert/" {
                assert_eq!(request.method(), "POST");
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
                http::Response::builder()
                    .status(200)
                    .body(response_body.clone())
                    .unwrap()
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });
        let result = table
            .add(RecordBatchIterator::new([Ok(data.clone())], data.schema()))
            .execute()
            .await
            .unwrap();

        // Check version matches expected value
        assert_eq!(result.version, expected_version);

        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let expected_body = write_ipc_stream(&data);
        assert_eq!(&body, &expected_body);
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_add_overwrite(#[case] old_server: bool) {
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

            if old_server {
                http::Response::builder().status(200).body("").unwrap()
            } else {
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 43}"#)
                    .unwrap()
            }
        });

        let result = table
            .add(RecordBatchIterator::new([Ok(data.clone())], data.schema()))
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });

        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let expected_body = write_ipc_stream(&data);
        assert_eq!(&body, &expected_body);
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_update(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/update/" {
                assert_eq!(request.method(), "POST");
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

                if old_server {
                    http::Response::builder().status(200).body("").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"rows_updated": 5, "version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table
            .update()
            .column("a", "a + 1")
            .column("b", "b - 1")
            .only_if("b > 10")
            .execute()
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });
        assert_eq!(result.rows_updated, if old_server { 0 } else { 5 });
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_alter_columns(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/alter_columns/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                let body = request.body().unwrap().as_bytes().unwrap();
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let alterations = value.get("alterations").unwrap().as_array().unwrap();
                assert!(alterations.len() == 2);

                let path = alterations[0]["path"].as_str().unwrap();
                let data_type = alterations[0]["data_type"]["type"].as_str().unwrap();
                assert_eq!(path, "b.c");
                assert_eq!(data_type, "int32");

                let path = alterations[1]["path"].as_str().unwrap();
                let nullable = alterations[1]["nullable"].as_bool().unwrap();
                let rename = alterations[1]["rename"].as_str().unwrap();
                assert_eq!(path, "x");
                assert!(nullable);
                assert_eq!(rename, "y");

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table
            .alter_columns(&[
                ColumnAlteration::new("b.c".into()).cast_to(DataType::Int32),
                ColumnAlteration::new("x".into())
                    .rename("y".into())
                    .set_nullable(true),
            ])
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_merge_insert(#[case] old_server: bool) {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let data = Box::new(RecordBatchIterator::new(
            [Ok(batch.clone())],
            batch.schema(),
        ));

        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/merge_insert/" {
                assert_eq!(request.method(), "POST");

                let params = request.url().query_pairs().collect::<HashMap<_, _>>();
                assert_eq!(params["on"], "some_col");
                assert_eq!(params["when_matched_update_all"], "false");
                assert_eq!(params["when_not_matched_insert_all"], "false");
                assert_eq!(params["when_not_matched_by_source_delete"], "false");
                assert!(!params.contains_key("when_matched_update_all_filt"));
                assert!(!params.contains_key("when_not_matched_by_source_delete_filt"));

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43, "num_deleted_rows": 0, "num_inserted_rows": 3, "num_updated_rows": 0}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table
            .merge_insert(&["some_col"])
            .execute(data)
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });
        if !old_server {
            assert_eq!(result.num_deleted_rows, 0);
            assert_eq!(result.num_inserted_rows, 3);
            assert_eq!(result.num_updated_rows, 0);
        }
    }

    #[tokio::test]
    async fn test_merge_insert_retries_on_409() {
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

            http::Response::builder().status(409).body("").unwrap()
        });

        let e = table
            .merge_insert(&["some_col"])
            .execute(data)
            .await
            .unwrap_err();
        assert!(e.to_string().contains("Hit retry limit"));
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_delete(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/delete/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                let body = request.body().unwrap().as_bytes().unwrap();
                let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                let predicate = body.get("predicate").unwrap().as_str().unwrap();
                assert_eq!(predicate, "id in (1, 2, 3)");

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table.delete("id in (1, 2, 3)").await.unwrap();
        assert_eq!(result.version, if old_server { 0 } else { 43 });
    }

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_drop_columns(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/drop_columns/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                let body = request.body().unwrap().as_bytes().unwrap();
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let columns = value.get("columns").unwrap().as_array().unwrap();
                assert!(columns.len() == 2);

                let col1 = columns[0].as_str().unwrap();
                let col2 = columns[1].as_str().unwrap();
                assert_eq!(col1, "a");
                assert_eq!(col2, "b");

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table.drop_columns(&["a", "b"]).await.unwrap();
        assert_eq!(result.version, if old_server { 0 } else { 43 });
    }

    #[tokio::test]
    async fn test_query_plain() {
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
            let expected_body = serde_json::json!({
                "k": isize::MAX as usize,
                "prefilter": true,
                "vector": [], // Empty vector means no vector query.
                "version": null,
            });
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
            .execute()
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].as_ref().unwrap(), &expected_data);
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
                "minimum_nprobes": 20,
                "maximum_nprobes": 20,
                "lower_bound": Option::<f32>::None,
                "upper_bound": Option::<f32>::None,
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
    async fn test_query_fts_default_values() {
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
            let expected_body = serde_json::json!({
                "full_text_query": {
                    "columns": [],
                    "query": "test",
                },
                "prefilter": true,
                "version": null,
                "k": 10,
                "vector": [],
            });
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
            .full_text_search(FullTextSearchQuery::new("test".to_owned()))
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
                "offset": 10,
                "distance_type": "cosine",
                "bypass_vector_index": true,
                "columns": ["a", "b"],
                "nprobes": 12,
                "minimum_nprobes": 12,
                "maximum_nprobes": 12,
                "lower_bound": Option::<f32>::None,
                "upper_bound": Option::<f32>::None,
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
            .offset(10)
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
                "prefilter": true,
                "version": null
            });
            let expected_body_2 = serde_json::json!({
                "full_text_query": {
                    "columns": ["b","a"],
                    "query": "hello world",
                },
                "k": 10,
                "vector": [],
                "with_row_id": true,
                "prefilter": true,
                "version": null
            });
            assert!(body == expected_body || body == expected_body_2);

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
                    .with_columns(&["a".into(), "b".into()])
                    .unwrap(),
            )
            .with_row_id()
            .limit(10)
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_query_structured_fts() {
        let table =
            Table::new_with_handler_version("my_table", semver::Version::new(0, 3, 0), |request| {
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
                        "query": {
                            "match": {
                                "terms": "hello world",
                                "column": "a",
                                "boost": 1.0,
                                "fuzziness": 0,
                                "max_expansions": 50,
                                "operator": "Or",
                                "prefix_length": 0,
                            },
                        }
                    },
                    "k": 10,
                    "vector": [],
                    "with_row_id": true,
                    "prefilter": true,
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
            .full_text_search(FullTextSearchQuery::new_query(
                MatchQuery::new("hello world".to_owned())
                    .with_column(Some("a".to_owned()))
                    .into(),
            ))
            .with_row_id()
            .limit(10)
            .execute()
            .await
            .unwrap();
    }

    #[rstest]
    #[case(DEFAULT_SERVER_VERSION.clone())]
    #[case(semver::Version::new(0, 2, 0))]
    #[tokio::test]
    async fn test_batch_queries(#[case] version: semver::Version) {
        let table = Table::new_with_handler_version("my_table", version.clone(), move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/v1/table/my_table/query/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );
            let body: serde_json::Value =
                serde_json::from_slice(request.body().unwrap().as_bytes().unwrap()).unwrap();
            let query_vectors = body["vector"].as_array().unwrap();
            let version = ServerVersion(version.clone());
            let data = if version.support_multivector() {
                assert_eq!(query_vectors.len(), 2);
                assert_eq!(query_vectors[0].as_array().unwrap().len(), 3);
                assert_eq!(query_vectors[1].as_array().unwrap().len(), 3);
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("a", DataType::Int32, false),
                        Field::new("query_index", DataType::Int32, false),
                    ])),
                    vec![
                        Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6])),
                        Arc::new(Int32Array::from(vec![0, 0, 0, 1, 1, 1])),
                    ],
                )
                .unwrap()
            } else {
                // it's single flat vector, so here the length is dim
                assert_eq!(query_vectors.len(), 3);
                RecordBatch::try_new(
                    Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                    vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                )
                .unwrap()
            };

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
            (
                "IVF_FLAT",
                json!({
                    "metric_type": "hamming",
                }),
                Index::IvfFlat(IvfFlatIndexBuilder::default().distance_type(DistanceType::Hamming)),
            ),
            (
                "IVF_FLAT",
                json!({
                    "metric_type": "hamming",
                    "num_partitions": 128,
                }),
                Index::IvfFlat(
                    IvfFlatIndexBuilder::default()
                        .distance_type(DistanceType::Hamming)
                        .num_partitions(128),
                ),
            ),
            (
                "IVF_PQ",
                json!({
                    "metric_type": "l2",
                }),
                Index::IvfPq(Default::default()),
            ),
            (
                "IVF_PQ",
                json!({
                    "metric_type": "cosine",
                    "num_partitions": 128,
                    "num_bits": 4,
                }),
                Index::IvfPq(
                    IvfPqIndexBuilder::default()
                        .distance_type(DistanceType::Cosine)
                        .num_partitions(128)
                        .num_bits(4),
                ),
            ),
            (
                "IVF_HNSW_SQ",
                json!({
                    "metric_type": "l2",
                }),
                Index::IvfHnswSq(Default::default()),
            ),
            (
                "IVF_HNSW_SQ",
                json!({
                    "metric_type": "l2",
                    "num_partitions": 128,
                }),
                Index::IvfHnswSq(
                    IvfHnswSqIndexBuilder::default()
                        .distance_type(DistanceType::L2)
                        .num_partitions(128),
                ),
            ),
            // HNSW_PQ isn't yet supported on SaaS
            ("BTREE", json!({}), Index::BTree(Default::default())),
            ("BITMAP", json!({}), Index::Bitmap(Default::default())),
            (
                "LABEL_LIST",
                json!({}),
                Index::LabelList(Default::default()),
            ),
            (
                "FTS",
                serde_json::to_value(InvertedIndexParams::default()).unwrap(),
                Index::FTS(Default::default()),
            ),
        ];

        for (index_type, expected_body, index) in cases {
            let table = Table::new_with_handler("my_table", move |request| {
                assert_eq!(request.method(), "POST");
                assert_eq!(request.url().path(), "/v1/table/my_table/create_index/");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );
                let body = request.body().unwrap().as_bytes().unwrap();
                let body: serde_json::Value = serde_json::from_slice(body).unwrap();
                let mut expected_body = expected_body.clone();
                expected_body["column"] = "a".into();
                expected_body[INDEX_TYPE_KEY] = index_type.into();

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
            loss: None,
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

    #[rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_add_columns(#[case] old_server: bool) {
        let table = Table::new_with_handler("my_table", move |request| {
            if request.url().path() == "/v1/table/my_table/add_columns/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    JSON_CONTENT_TYPE
                );

                let body = request.body().unwrap().as_bytes().unwrap();
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let new_columns = value.get("new_columns").unwrap().as_array().unwrap();
                assert!(new_columns.len() == 2);

                let col_name = new_columns[0]["name"].as_str().unwrap();
                let expression = new_columns[0]["expression"].as_str().unwrap();
                assert_eq!(col_name, "b");
                assert_eq!(expression, "a + 1");

                let col_name = new_columns[1]["name"].as_str().unwrap();
                let expression = new_columns[1]["expression"].as_str().unwrap();
                assert_eq!(col_name, "x");
                assert_eq!(expression, "cast(NULL as int32)");

                if old_server {
                    http::Response::builder().status(200).body("{}").unwrap()
                } else {
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 43}"#)
                        .unwrap()
                }
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![
                    ("b".into(), "a + 1".into()),
                    ("x".into(), "cast(NULL as int32)".into()),
                ]),
                None,
            )
            .await
            .unwrap();

        assert_eq!(result.version, if old_server { 0 } else { 43 });
    }

    #[tokio::test]
    async fn test_drop_index() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/drop/"
            );
            http::Response::builder().status(200).body("{}").unwrap()
        });
        table.drop_index("my_index").await.unwrap();
    }

    #[tokio::test]
    async fn test_drop_index_not_exists() {
        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/my_table/index/my_index/drop/"
            );
            http::Response::builder().status(404).body("{}").unwrap()
        });

        // Assert that the error is IndexNotFound
        let e = table.drop_index("my_index").await.unwrap_err();
        assert!(matches!(e, Error::IndexNotFound { .. }));
    }

    #[tokio::test]
    async fn test_wait_for_index() {
        let table = _make_table_with_indices(0);
        table
            .wait_for_index(&["vector_idx", "my_idx"], Duration::from_secs(1))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_wait_for_index_timeout() {
        let table = _make_table_with_indices(100);
        let e = table
            .wait_for_index(&["vector_idx", "my_idx"], Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "Timeout error: timed out waiting for indices: [\"vector_idx\", \"my_idx\"] after 1s"
        );
    }

    #[tokio::test]
    async fn test_wait_for_index_timeout_never_created() {
        let table = _make_table_with_indices(0);
        let e = table
            .wait_for_index(&["doesnt_exist_idx"], Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(
            e.to_string(),
            "Timeout error: timed out waiting for indices: [\"doesnt_exist_idx\"] after 1s"
        );
    }

    fn _make_table_with_indices(unindexed_rows: usize) -> Table {
        let table = Table::new_with_handler("my_table", move |request| {
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
                        "num_unindexed_rows": unindexed_rows,
                        "index_type": "IVF_PQ",
                        "distance_type": "l2"
                    })
                }
                "/v1/table/my_table/index/my_idx/stats/" => {
                    serde_json::json!({
                        "num_indexed_rows": 100000,
                        "num_unindexed_rows": unindexed_rows,
                        "index_type": "LABEL_LIST"
                    })
                }
                _path => {
                    serde_json::json!(None::<String>)
                }
            };
            let body = serde_json::to_string(&response_body).unwrap();
            let status = if body == "null" { 404 } else { 200 };
            http::Response::builder().status(status).body(body).unwrap()
        });
        table
    }

    #[tokio::test]
    async fn test_table_with_namespace_identifier() {
        // Test that a table created with namespace uses the correct identifier in API calls
        let table = Table::new_with_handler("ns1$ns2$table1", |request| {
            assert_eq!(request.method(), "POST");
            // All API calls should use the full identifier in the path
            assert_eq!(request.url().path(), "/v1/table/ns1$ns2$table1/describe/");

            http::Response::builder()
                .status(200)
                .body(r#"{"version": 1, "schema": { "fields": [] }}"#)
                .unwrap()
        });

        // The name() method should return just the base name, not the full identifier
        assert_eq!(table.name(), "ns1$ns2$table1");

        // API operations should work correctly
        let version = table.version().await.unwrap();
        assert_eq!(version, 1);
    }

    #[tokio::test]
    async fn test_query_with_namespace() {
        let table = Table::new_with_handler("analytics$events", |request| {
            match request.url().path() {
                "/v1/table/analytics$events/query/" => {
                    assert_eq!(request.method(), "POST");

                    // Return empty arrow stream
                    let data = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)])),
                        vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
                    )
                    .unwrap();
                    let body = write_ipc_file(&data);

                    http::Response::builder()
                        .status(200)
                        .header("Content-Type", ARROW_FILE_CONTENT_TYPE)
                        .body(body)
                        .unwrap()
                }
                _ => {
                    panic!("Unexpected path: {}", request.url().path());
                }
            }
        });

        let results = table.query().execute().await.unwrap();
        let batches = results.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 3);
    }

    #[tokio::test]
    async fn test_add_data_with_namespace() {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let (sender, receiver) = std::sync::mpsc::channel();
        let table = Table::new_with_handler("prod$metrics", move |mut request| {
            if request.url().path() == "/v1/table/prod$metrics/insert/" {
                assert_eq!(request.method(), "POST");
                assert_eq!(
                    request.headers().get("Content-Type").unwrap(),
                    ARROW_STREAM_CONTENT_TYPE
                );
                let mut body_out = reqwest::Body::from(Vec::new());
                std::mem::swap(request.body_mut().as_mut().unwrap(), &mut body_out);
                sender.send(body_out).unwrap();
                http::Response::builder()
                    .status(200)
                    .body(r#"{"version": 2}"#)
                    .unwrap()
            } else {
                panic!("Unexpected request path: {}", request.url().path());
            }
        });

        let result = table
            .add(RecordBatchIterator::new([Ok(data.clone())], data.schema()))
            .execute()
            .await
            .unwrap();

        assert_eq!(result.version, 2);

        let body = receiver.recv().unwrap();
        let body = collect_body(body).await;
        let expected_body = write_ipc_stream(&data);
        assert_eq!(&body, &expected_body);
    }

    #[tokio::test]
    async fn test_create_index_with_namespace() {
        let table = Table::new_with_handler("dev$users", |request| {
            match request.url().path() {
                "/v1/table/dev$users/create_index/" => {
                    assert_eq!(request.method(), "POST");
                    assert_eq!(
                        request.headers().get("Content-Type").unwrap(),
                        JSON_CONTENT_TYPE
                    );

                    // Verify the request body contains the column name
                    if let Some(body) = request.body().unwrap().as_bytes() {
                        let body = std::str::from_utf8(body).unwrap();
                        let value: serde_json::Value = serde_json::from_str(body).unwrap();
                        assert_eq!(value["column"], "embedding");
                        assert_eq!(value["index_type"], "IVF_PQ");
                    }

                    http::Response::builder().status(200).body("").unwrap()
                }
                "/v1/table/dev$users/describe/" => {
                    // Needed for schema check in Auto index type
                    http::Response::builder()
                        .status(200)
                        .body(r#"{"version": 1, "schema": {"fields": [{"name": "embedding", "type": {"type": "list", "item": {"type": "float32"}}, "nullable": false}]}}"#)
                        .unwrap()
                }
                _ => {
                    panic!("Unexpected path: {}", request.url().path());
                }
            }
        });

        table
            .create_index(&["embedding"], Index::IvfPq(IvfPqIndexBuilder::default()))
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_columns_with_namespace() {
        let table = Table::new_with_handler("test$schema_ops", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(
                request.url().path(),
                "/v1/table/test$schema_ops/drop_columns/"
            );
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            if let Some(body) = request.body().unwrap().as_bytes() {
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let columns = value["columns"].as_array().unwrap();
                assert_eq!(columns.len(), 2);
                assert_eq!(columns[0], "old_col1");
                assert_eq!(columns[1], "old_col2");
            }

            http::Response::builder()
                .status(200)
                .body(r#"{"version": 5}"#)
                .unwrap()
        });

        let result = table.drop_columns(&["old_col1", "old_col2"]).await.unwrap();
        assert_eq!(result.version, 5);
    }
}
