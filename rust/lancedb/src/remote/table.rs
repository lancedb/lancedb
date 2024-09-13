use std::sync::Arc;

use crate::table::dataset::DatasetReadGuard;
use crate::Error;
use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use datafusion_physical_plan::ExecutionPlan;
use http::StatusCode;
use lance::arrow::json::JsonSchema;
use lance::dataset::scanner::{DatasetRecordBatchStream, Scanner};
use lance::dataset::{ColumnAlteration, NewColumnTransform};
use serde::Deserialize;

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

use super::client::{HttpSend, RestfulLanceDbClient, Sender};

#[derive(Debug)]
pub struct RemoteTable<S: HttpSend = Sender> {
    #[allow(dead_code)]
    client: RestfulLanceDbClient<S>,
    name: String,
}

impl<S: HttpSend> RemoteTable<S> {
    pub fn new(client: RestfulLanceDbClient<S>, name: String) -> Self {
        Self { client, name }
    }

    async fn describe(&self) -> Result<TableDescription> {
        let request = self.client.post(&format!("/table/{}/describe/", self.name));
        let response = self.client.send(request).await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: self.name.clone(),
            });
        }

        let response = self.client.check_response(response).await?;

        let body = response.text().await?;

        serde_json::from_str(&body).map_err(|e| Error::Http {
            message: format!("Failed to parse table description: {}", e),
        })
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
            Self { client, name }
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
    async fn checkout(&self, _version: u64) -> Result<()> {
        Err(Error::NotSupported {
            message: "checkout is not supported on LanceDB cloud.".into(),
        })
    }
    async fn checkout_latest(&self) -> Result<()> {
        Err(Error::NotSupported {
            message: "checkout is not supported on LanceDB cloud.".into(),
        })
    }
    async fn restore(&self) -> Result<()> {
        Err(Error::NotSupported {
            message: "restore is not supported on LanceDB cloud.".into(),
        })
    }
    async fn schema(&self) -> Result<SchemaRef> {
        let schema = self.describe().await?.schema;
        Ok(Arc::new(schema.try_into()?))
    }
    async fn count_rows(&self, filter: Option<String>) -> Result<usize> {
        let mut request = self
            .client
            .post(&format!("/table/{}/count_rows/", self.name));

        if let Some(filter) = filter {
            request = request.json(&serde_json::json!({ "filter": filter }));
        } else {
            request = request.json(&serde_json::json!({}));
        }

        let response = self.client.send(request).await?;

        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: self.name.clone(),
            });
        }

        let response = self.client.check_response(response).await?;

        let body = response.text().await?;

        serde_json::from_str(&body).map_err(|e| Error::Http {
            message: format!("Failed to parse row count: {}", e),
        })
    }
    async fn add(
        &self,
        _add: AddDataBuilder<NoData>,
        _data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        todo!()
    }
    async fn build_plan(
        &self,
        _ds_ref: &DatasetReadGuard,
        _query: &VectorQuery,
        _options: Option<QueryExecutionOptions>,
    ) -> Result<Scanner> {
        todo!()
    }
    async fn create_plan(
        &self,
        _query: &VectorQuery,
        _options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        unimplemented!()
    }
    async fn explain_plan(&self, _query: &VectorQuery, _verbose: bool) -> Result<String> {
        todo!()
    }
    async fn plain_query(
        &self,
        _query: &Query,
        _options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        todo!()
    }
    async fn update(&self, _update: UpdateBuilder) -> Result<()> {
        todo!()
    }
    async fn delete(&self, _predicate: &str) -> Result<()> {
        todo!()
    }
    async fn create_index(&self, _index: IndexBuilder) -> Result<()> {
        todo!()
    }
    async fn merge_insert(
        &self,
        _params: MergeInsertBuilder,
        _new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        todo!()
    }
    async fn optimize(&self, _action: OptimizeAction) -> Result<OptimizeStats> {
        todo!()
    }
    async fn add_columns(
        &self,
        _transforms: NewColumnTransform,
        _read_columns: Option<Vec<String>>,
    ) -> Result<()> {
        todo!()
    }
    async fn alter_columns(&self, _alterations: &[ColumnAlteration]) -> Result<()> {
        todo!()
    }
    async fn drop_columns(&self, _columns: &[&str]) -> Result<()> {
        todo!()
    }
    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        todo!()
    }
    async fn table_definition(&self) -> Result<TableDefinition> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use futures::{future::BoxFuture, TryFutureExt};

    use crate::{remote::ARROW_STREAM_CONTENT_TYPE, Error, Table};

    #[tokio::test]
    async fn test_not_found() {
        let table = Table::new_with_handler("my_table", |_| {
            http::Response::builder()
                .status(404)
                .body("table my_table not found")
                .unwrap()
        });

        // All endpoints should translate 404 to TableNotFound.
        let results: Vec<BoxFuture<'_, Result<()>>> = vec![
            Box::pin(table.version().map_ok(|_| ())),
            Box::pin(table.schema().map_ok(|_| ())),
            Box::pin(table.count_rows(None).map_ok(|_| ())),
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
            assert_eq!(request.url().path(), "/table/my_table/describe/");

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
            assert_eq!(request.url().path(), "/table/my_table/describe/");

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
            assert_eq!(request.url().path(), "/table/my_table/count_rows/");
            assert_eq!(request.body().unwrap().as_bytes().unwrap(), br#"{}"#);

            http::Response::builder().status(200).body("42").unwrap()
        });

        let count = table.count_rows(None).await.unwrap();
        assert_eq!(count, 42);

        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/table/my_table/count_rows/");
            assert_eq!(
                request.body().unwrap().as_bytes().unwrap(),
                br#"{"filter":"a > 10"}"#
            );

            http::Response::builder().status(200).body("42").unwrap()
        });

        let count = table.count_rows(Some("a > 10".into())).await.unwrap();
        assert_eq!(count, 42);
    }

    #[tokio::test]
    async fn test_add_append() {
        let data = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)])),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();

        let data_ref = data.clone();
        let table = Table::new_with_handler("my_table", move |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/table/my_table/insert/");
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

            let mut expected_body = Vec::new();
            {
                let mut writer = arrow_ipc::writer::StreamWriter::try_new(
                    &mut expected_body,
                    &data_ref.schema(),
                )
                .unwrap();
                writer.write(&data_ref).unwrap();
                writer.finish().unwrap();
            }

            assert_eq!(request.body().unwrap().as_bytes().unwrap(), &expected_body);

            http::Response::builder().status(200).body("").unwrap()
        });

        table
            .add(RecordBatchIterator::new([Ok(data.clone())], data.schema()))
            .execute()
            .await
            .unwrap();
    }
}
