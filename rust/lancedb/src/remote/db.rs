// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::RecordBatchIterator;
use async_trait::async_trait;
use http::StatusCode;
use lance_io::object_store::StorageOptions;
use moka::future::Cache;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use tokio::task::spawn_blocking;

use crate::database::{
    CreateTableData, CreateTableMode, CreateTableRequest, Database, OpenTableRequest,
    TableNamesRequest,
};
use crate::error::Result;
use crate::table::BaseTable;
use crate::Error;

use super::client::{ClientConfig, HttpSend, RequestResultExt, RestfulLanceDbClient, Sender};
use super::table::RemoteTable;
use super::util::{batches_to_ipc_bytes, parse_server_version};
use super::ARROW_STREAM_CONTENT_TYPE;

// the versions of the server that we support
// for any new feature that we need to change the SDK behavior, we should bump the server version,
// and add a feature flag as method of `ServerVersion` here.
pub const DEFAULT_SERVER_VERSION: semver::Version = semver::Version::new(0, 1, 0);
#[derive(Debug, Clone)]
pub struct ServerVersion(pub semver::Version);

impl Default for ServerVersion {
    fn default() -> Self {
        Self(DEFAULT_SERVER_VERSION.clone())
    }
}

impl ServerVersion {
    pub fn parse(version: &str) -> Result<Self> {
        let version = Self(
            semver::Version::parse(version).map_err(|e| Error::InvalidInput {
                message: e.to_string(),
            })?,
        );
        Ok(version)
    }

    pub fn support_multivector(&self) -> bool {
        self.0 >= semver::Version::new(0, 2, 0)
    }
}

#[derive(Deserialize)]
struct ListTablesResponse {
    tables: Vec<String>,
}

#[derive(Debug)]
pub struct RemoteDatabase<S: HttpSend = Sender> {
    client: RestfulLanceDbClient<S>,
    table_cache: Cache<String, Arc<RemoteTable<S>>>,
}

impl RemoteDatabase {
    pub fn try_new(
        uri: &str,
        api_key: &str,
        region: &str,
        host_override: Option<String>,
        client_config: ClientConfig,
        options: RemoteOptions,
    ) -> Result<Self> {
        let client = RestfulLanceDbClient::try_new(
            uri,
            api_key,
            region,
            host_override,
            client_config,
            &options,
        )?;

        let table_cache = Cache::builder()
            .time_to_live(std::time::Duration::from_secs(300))
            .max_capacity(10_000)
            .build();

        Ok(Self {
            client,
            table_cache,
        })
    }
}

#[cfg(all(test, feature = "remote"))]
mod test_utils {
    use super::*;
    use crate::remote::client::test_utils::client_with_handler;
    use crate::remote::client::test_utils::MockSender;

    impl RemoteDatabase<MockSender> {
        pub fn new_mock<F, T>(handler: F) -> Self
        where
            F: Fn(reqwest::Request) -> http::Response<T> + Send + Sync + 'static,
            T: Into<reqwest::Body>,
        {
            let client = client_with_handler(handler);
            Self {
                client,
                table_cache: Cache::new(0),
            }
        }
    }
}

impl<S: HttpSend> std::fmt::Display for RemoteDatabase<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "RemoteDatabase(host={})", self.client.host())
    }
}

impl From<&CreateTableMode> for &'static str {
    fn from(val: &CreateTableMode) -> Self {
        match val {
            CreateTableMode::Create => "create",
            CreateTableMode::Overwrite => "overwrite",
            CreateTableMode::ExistOk(_) => "exist_ok",
        }
    }
}

#[async_trait]
impl<S: HttpSend> Database for RemoteDatabase<S> {
    async fn table_names(&self, request: TableNamesRequest) -> Result<Vec<String>> {
        let mut req = self.client.get("/v1/table/");
        if let Some(limit) = request.limit {
            req = req.query(&[("limit", limit)]);
        }
        if let Some(start_after) = request.start_after {
            req = req.query(&[("page_token", start_after)]);
        }
        let (request_id, rsp) = self.client.send(req, true).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let version = parse_server_version(&request_id, &rsp)?;
        let tables = rsp
            .json::<ListTablesResponse>()
            .await
            .err_to_http(request_id)?
            .tables;
        for table in &tables {
            let remote_table = Arc::new(RemoteTable::new(
                self.client.clone(),
                table.clone(),
                version.clone(),
            ));
            self.table_cache.insert(table.clone(), remote_table).await;
        }
        Ok(tables)
    }

    async fn create_table(&self, request: CreateTableRequest) -> Result<Arc<dyn BaseTable>> {
        let data = match request.data {
            CreateTableData::Data(data) => data,
            CreateTableData::StreamingData(_) => {
                return Err(Error::NotSupported {
                    message: "Creating a remote table from a streaming source".to_string(),
                })
            }
            CreateTableData::Empty(table_definition) => {
                let schema = table_definition.schema.clone();
                Box::new(RecordBatchIterator::new(vec![], schema))
            }
        };

        // TODO: https://github.com/lancedb/lancedb/issues/1026
        // We should accept data from an async source.  In the meantime, spawn this as blocking
        // to make sure we don't block the tokio runtime if the source is slow.
        let data_buffer = spawn_blocking(move || batches_to_ipc_bytes(data))
            .await
            .unwrap()?;

        let req = self
            .client
            .post(&format!("/v1/table/{}/create/", request.name))
            .query(&[("mode", Into::<&str>::into(&request.mode))])
            .body(data_buffer)
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);

        let (request_id, rsp) = self.client.send(req, false).await?;

        if rsp.status() == StatusCode::BAD_REQUEST {
            let body = rsp.text().await.err_to_http(request_id.clone())?;
            if body.contains("already exists") {
                return match request.mode {
                    CreateTableMode::Create => {
                        Err(crate::Error::TableAlreadyExists { name: request.name })
                    }
                    CreateTableMode::ExistOk(callback) => {
                        let req = OpenTableRequest {
                            name: request.name.clone(),
                            index_cache_size: None,
                            lance_read_params: None,
                        };
                        let req = (callback)(req);
                        self.open_table(req).await
                    }

                    // This should not happen, as we explicitly set the mode to overwrite and the server
                    // shouldn't return an error if the table already exists.
                    //
                    // However if the server is an older version that doesn't support the mode parameter,
                    // then we'll get the 400 response.
                    CreateTableMode::Overwrite => Err(crate::Error::Http {
                        source: format!(
                            "unexpected response from server for create mode overwrite: {}",
                            body
                        )
                        .into(),
                        request_id,
                        status_code: Some(StatusCode::BAD_REQUEST),
                    }),
                };
            } else {
                return Err(crate::Error::InvalidInput { message: body });
            }
        }
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let version = parse_server_version(&request_id, &rsp)?;
        let table = Arc::new(RemoteTable::new(
            self.client.clone(),
            request.name.clone(),
            version,
        ));
        self.table_cache
            .insert(request.name.clone(), table.clone())
            .await;

        Ok(table)
    }

    async fn open_table(&self, request: OpenTableRequest) -> Result<Arc<dyn BaseTable>> {
        // We describe the table to confirm it exists before moving on.
        if let Some(table) = self.table_cache.get(&request.name).await {
            Ok(table.clone())
        } else {
            let req = self
                .client
                .post(&format!("/v1/table/{}/describe/", request.name));
            let (request_id, rsp) = self.client.send(req, true).await?;
            if rsp.status() == StatusCode::NOT_FOUND {
                return Err(crate::Error::TableNotFound { name: request.name });
            }
            let rsp = self.client.check_response(&request_id, rsp).await?;
            let version = parse_server_version(&request_id, &rsp)?;
            let table = Arc::new(RemoteTable::new(
                self.client.clone(),
                request.name.clone(),
                version,
            ));
            self.table_cache.insert(request.name, table.clone()).await;
            Ok(table)
        }
    }

    async fn rename_table(&self, current_name: &str, new_name: &str) -> Result<()> {
        let req = self
            .client
            .post(&format!("/v1/table/{}/rename/", current_name));
        let req = req.json(&serde_json::json!({ "new_table_name": new_name }));
        let (request_id, resp) = self.client.send(req, false).await?;
        self.client.check_response(&request_id, resp).await?;
        let table = self.table_cache.remove(current_name).await;
        if let Some(table) = table {
            self.table_cache.insert(new_name.into(), table).await;
        }
        Ok(())
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        let req = self.client.post(&format!("/v1/table/{}/drop/", name));
        let (request_id, resp) = self.client.send(req, true).await?;
        self.client.check_response(&request_id, resp).await?;
        self.table_cache.remove(name).await;
        Ok(())
    }

    async fn drop_all_tables(&self) -> Result<()> {
        Err(crate::Error::NotSupported {
            message: "Dropping databases is not supported in the remote API".to_string(),
        })
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// RemoteOptions contains a subset of StorageOptions that are compatible with Remote LanceDB connections
#[derive(Clone, Debug, Default)]
pub struct RemoteOptions(pub HashMap<String, String>);

impl RemoteOptions {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self(options)
    }
}

impl From<StorageOptions> for RemoteOptions {
    fn from(options: StorageOptions) -> Self {
        let supported_opts = vec!["account_name", "azure_storage_account_name"];
        let mut filtered = HashMap::new();
        for opt in supported_opts {
            if let Some(v) = options.0.get(opt) {
                filtered.insert(opt.to_string(), v.to_string());
            }
        }
        Self::new(filtered)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};

    use crate::connection::ConnectBuilder;
    use crate::{
        database::CreateTableMode,
        remote::{ARROW_STREAM_CONTENT_TYPE, JSON_CONTENT_TYPE},
        Connection, Error,
    };

    #[tokio::test]
    async fn test_retries() {
        // We'll record the request_id here, to check it matches the one in the error.
        let seen_request_id = Arc::new(OnceLock::new());
        let seen_request_id_ref = seen_request_id.clone();
        let conn = Connection::new_with_handler(move |request| {
            // Request id should be the same on each retry.
            let request_id = request.headers()["x-request-id"]
                .to_str()
                .unwrap()
                .to_string();
            let seen_id = seen_request_id_ref.get_or_init(|| request_id.clone());
            assert_eq!(&request_id, seen_id);

            http::Response::builder()
                .status(500)
                .body("internal server error")
                .unwrap()
        });
        let result = conn.table_names().execute().await;
        if let Err(Error::Retry {
            request_id,
            request_failures,
            max_request_failures,
            source,
            ..
        }) = result
        {
            let expected_id = seen_request_id.get().unwrap();
            assert_eq!(&request_id, expected_id);
            assert_eq!(request_failures, max_request_failures);
            assert!(
                source.to_string().contains("internal server error"),
                "source: {:?}",
                source
            );
        } else {
            panic!("unexpected result: {:?}", result);
        };
    }

    #[tokio::test]
    async fn test_table_names() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/table/");
            assert_eq!(request.url().query(), None);

            http::Response::builder()
                .status(200)
                .body(r#"{"tables": ["table1", "table2"]}"#)
                .unwrap()
        });
        let names = conn.table_names().execute().await.unwrap();
        assert_eq!(names, vec!["table1", "table2"]);
    }

    #[tokio::test]
    async fn test_table_names_pagination() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::GET);
            assert_eq!(request.url().path(), "/v1/table/");
            assert!(request.url().query().unwrap().contains("limit=2"));
            assert!(request.url().query().unwrap().contains("page_token=table2"));

            http::Response::builder()
                .status(200)
                .body(r#"{"tables": ["table3", "table4"], "page_token": "token"}"#)
                .unwrap()
        });
        let names = conn
            .table_names()
            .start_after("table2")
            .limit(2)
            .execute()
            .await
            .unwrap();
        assert_eq!(names, vec!["table3", "table4"]);
    }

    #[tokio::test]
    async fn test_open_table() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/describe/");
            assert_eq!(request.url().query(), None);

            http::Response::builder()
                .status(200)
                .body(r#"{"table": "table1"}"#)
                .unwrap()
        });
        let table = conn.open_table("table1").execute().await.unwrap();
        assert_eq!(table.name(), "table1");

        // Storage options should be ignored.
        let table = conn
            .open_table("table1")
            .storage_option("key", "value")
            .execute()
            .await
            .unwrap();
        assert_eq!(table.name(), "table1");
    }

    #[tokio::test]
    async fn test_open_table_not_found() {
        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(404)
                .body("table not found")
                .unwrap()
        });
        let result = conn.open_table("table1").execute().await;
        assert!(result.is_err());
        assert!(matches!(result, Err(crate::Error::TableNotFound { .. })));
    }

    #[tokio::test]
    async fn test_create_table() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/create/");
            assert_eq!(
                request
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .unwrap(),
                ARROW_STREAM_CONTENT_TYPE.as_bytes()
            );

            http::Response::builder().status(200).body("").unwrap()
        });
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let table = conn.create_table("table1", reader).execute().await.unwrap();
        assert_eq!(table.name(), "table1");
    }

    #[tokio::test]
    async fn test_create_table_already_exists() {
        let conn = Connection::new_with_handler(|_| {
            http::Response::builder()
                .status(400)
                .body("table table1 already exists")
                .unwrap()
        });
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let result = conn.create_table("table1", reader).execute().await;
        assert!(result.is_err());
        assert!(
            matches!(result, Err(crate::Error::TableAlreadyExists { name }) if name == "table1")
        );
    }

    #[tokio::test]
    async fn test_create_table_modes() {
        let test_cases = [
            (None, "mode=create"),
            (Some(CreateTableMode::Create), "mode=create"),
            (Some(CreateTableMode::Overwrite), "mode=overwrite"),
            (
                Some(CreateTableMode::ExistOk(Box::new(|b| b))),
                "mode=exist_ok",
            ),
        ];

        for (mode, expected_query_string) in test_cases {
            let conn = Connection::new_with_handler(move |request| {
                assert_eq!(request.method(), &reqwest::Method::POST);
                assert_eq!(request.url().path(), "/v1/table/table1/create/");
                assert_eq!(request.url().query(), Some(expected_query_string));

                http::Response::builder().status(200).body("").unwrap()
            });

            let data = RecordBatch::try_new(
                Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
                vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
            )
            .unwrap();
            let reader = RecordBatchIterator::new([Ok(data.clone())], data.schema());
            let mut builder = conn.create_table("table1", reader);
            if let Some(mode) = mode {
                builder = builder.mode(mode);
            }
            builder.execute().await.unwrap();
        }

        // check that the open table callback is called with exist_ok
        let conn = Connection::new_with_handler(|request| match request.url().path() {
            "/v1/table/table1/create/" => http::Response::builder()
                .status(400)
                .body("Table table1 already exists")
                .unwrap(),
            "/v1/table/table1/describe/" => http::Response::builder().status(200).body("").unwrap(),
            _ => {
                panic!("unexpected path: {:?}", request.url().path());
            }
        });
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let called: Arc<OnceLock<bool>> = Arc::new(OnceLock::new());
        let reader = RecordBatchIterator::new([Ok(data.clone())], data.schema());
        let called_in_cb = called.clone();
        conn.create_table("table1", reader)
            .mode(CreateTableMode::ExistOk(Box::new(move |b| {
                called_in_cb.clone().set(true).unwrap();
                b
            })))
            .execute()
            .await
            .unwrap();

        let called = *called.get().unwrap_or(&false);
        assert!(called);
    }

    #[tokio::test]
    async fn test_create_table_empty() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/create/");
            assert_eq!(
                request
                    .headers()
                    .get(reqwest::header::CONTENT_TYPE)
                    .unwrap(),
                ARROW_STREAM_CONTENT_TYPE.as_bytes()
            );

            http::Response::builder().status(200).body("").unwrap()
        });
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        conn.create_empty_table("table1", schema)
            .execute()
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn test_drop_table() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/drop/");
            assert_eq!(request.url().query(), None);
            assert!(request.body().is_none());

            http::Response::builder().status(200).body("").unwrap()
        });
        conn.drop_table("table1").await.unwrap();
        // NOTE: the API will return 200 even if the table does not exist. So we shouldn't expect 404.
    }

    #[tokio::test]
    async fn test_rename_table() {
        let conn = Connection::new_with_handler(|request| {
            assert_eq!(request.method(), &reqwest::Method::POST);
            assert_eq!(request.url().path(), "/v1/table/table1/rename/");
            assert_eq!(
                request.headers().get("Content-Type").unwrap(),
                JSON_CONTENT_TYPE
            );

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            assert_eq!(body["new_table_name"], "table2");

            http::Response::builder().status(200).body("").unwrap()
        });
        conn.rename_table("table1", "table2").await.unwrap();
    }

    #[tokio::test]
    async fn test_connect_remote_options() {
        let db_uri = "db://my-container/my-prefix";
        let _ = ConnectBuilder::new(db_uri)
            .region("us-east-1")
            .api_key("my-api-key")
            .storage_options(vec![("azure_storage_account_name", "my-storage-account")])
            .execute()
            .await
            .unwrap();
    }
}
