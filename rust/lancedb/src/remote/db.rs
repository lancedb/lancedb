// Copyright 2024 LanceDB Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_array::RecordBatchReader;
use async_trait::async_trait;
use http::StatusCode;
use moka::future::Cache;
use reqwest::header::CONTENT_TYPE;
use serde::Deserialize;
use tokio::task::spawn_blocking;

use crate::connection::{
    ConnectionInternal, CreateTableBuilder, NoData, OpenTableBuilder, TableNamesBuilder,
};
use crate::embeddings::EmbeddingRegistry;
use crate::error::Result;
use crate::Table;

use super::client::{ClientConfig, HttpSend, RequestResultExt, RestfulLanceDbClient, Sender};
use super::table::RemoteTable;
use super::util::batches_to_ipc_bytes;
use super::ARROW_STREAM_CONTENT_TYPE;

#[derive(Deserialize)]
struct ListTablesResponse {
    tables: Vec<String>,
}

#[derive(Debug)]
pub struct RemoteDatabase<S: HttpSend = Sender> {
    client: RestfulLanceDbClient<S>,
    table_cache: Cache<String, ()>,
}

impl RemoteDatabase {
    pub fn try_new(
        uri: &str,
        api_key: &str,
        region: &str,
        host_override: Option<String>,
        client_config: ClientConfig,
    ) -> Result<Self> {
        let client =
            RestfulLanceDbClient::try_new(uri, api_key, region, host_override, client_config)?;

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

#[async_trait]
impl<S: HttpSend> ConnectionInternal for RemoteDatabase<S> {
    async fn table_names(&self, options: TableNamesBuilder) -> Result<Vec<String>> {
        let mut req = self.client.get("/v1/table/");
        if let Some(limit) = options.limit {
            req = req.query(&[("limit", limit)]);
        }
        if let Some(start_after) = options.start_after {
            req = req.query(&[("page_token", start_after)]);
        }
        let (request_id, rsp) = self.client.send(req, true).await?;
        let rsp = self.client.check_response(&request_id, rsp).await?;
        let tables = rsp
            .json::<ListTablesResponse>()
            .await
            .err_to_http(request_id)?
            .tables;
        for table in &tables {
            self.table_cache.insert(table.clone(), ()).await;
        }
        Ok(tables)
    }

    async fn do_create_table(
        &self,
        options: CreateTableBuilder<false, NoData>,
        data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<Table> {
        // TODO: https://github.com/lancedb/lancedb/issues/1026
        // We should accept data from an async source.  In the meantime, spawn this as blocking
        // to make sure we don't block the tokio runtime if the source is slow.
        let data_buffer = spawn_blocking(move || batches_to_ipc_bytes(data))
            .await
            .unwrap()?;

        let req = self
            .client
            .post(&format!("/v1/table/{}/create/", options.name))
            .body(data_buffer)
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE);
        let (request_id, rsp) = self.client.send(req, false).await?;

        if rsp.status() == StatusCode::BAD_REQUEST {
            let body = rsp.text().await.err_to_http(request_id.clone())?;
            if body.contains("already exists") {
                return Err(crate::Error::TableAlreadyExists { name: options.name });
            } else {
                return Err(crate::Error::InvalidInput { message: body });
            }
        }

        self.client.check_response(&request_id, rsp).await?;

        self.table_cache.insert(options.name.clone(), ()).await;

        Ok(Table::new(Arc::new(RemoteTable::new(
            self.client.clone(),
            options.name,
        ))))
    }

    async fn do_open_table(&self, options: OpenTableBuilder) -> Result<Table> {
        // We describe the table to confirm it exists before moving on.
        if self.table_cache.get(&options.name).is_none() {
            let req = self
                .client
                .get(&format!("/v1/table/{}/describe/", options.name));
            let (request_id, resp) = self.client.send(req, true).await?;
            if resp.status() == StatusCode::NOT_FOUND {
                return Err(crate::Error::TableNotFound { name: options.name });
            }
            self.client.check_response(&request_id, resp).await?;
        }

        Ok(Table::new(Arc::new(RemoteTable::new(
            self.client.clone(),
            options.name,
        ))))
    }

    async fn rename_table(&self, current_name: &str, new_name: &str) -> Result<()> {
        let req = self
            .client
            .post(&format!("/v1/table/{}/rename/", current_name));
        let req = req.json(&serde_json::json!({ "new_table_name": new_name }));
        let (request_id, resp) = self.client.send(req, false).await?;
        self.client.check_response(&request_id, resp).await?;
        self.table_cache.remove(current_name).await;
        self.table_cache.insert(new_name.into(), ()).await;
        Ok(())
    }

    async fn drop_table(&self, name: &str) -> Result<()> {
        let req = self.client.post(&format!("/v1/table/{}/drop/", name));
        let (request_id, resp) = self.client.send(req, true).await?;
        self.client.check_response(&request_id, resp).await?;
        self.table_cache.remove(name).await;
        Ok(())
    }

    async fn drop_db(&self) -> Result<()> {
        Err(crate::Error::NotSupported {
            message: "Dropping databases is not supported in the remote API".to_string(),
        })
    }

    fn embedding_registry(&self) -> &dyn EmbeddingRegistry {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, OnceLock};

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};

    use crate::{
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
            assert_eq!(request.method(), &reqwest::Method::GET);
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
}
