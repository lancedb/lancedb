use std::sync::{Arc, Mutex};

use crate::table::AddDataMode;
use crate::Error;
use arrow_array::RecordBatchReader;
use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;
use async_trait::async_trait;
use bytes::Buf;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use datafusion_physical_plan::{ExecutionPlan, SendableRecordBatchStream};
use futures::TryStreamExt;
use http::header::CONTENT_TYPE;
use http::StatusCode;
use lance::arrow::json::JsonSchema;
use lance::dataset::scanner::DatasetRecordBatchStream;
use lance::dataset::{ColumnAlteration, NewColumnTransform};
use lance_datafusion::exec::OneShotExec;
use serde::{Deserialize, Serialize};

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
use super::ARROW_STREAM_CONTENT_TYPE;

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

        let response = self.check_table_response(response).await?;

        let body = response.text().await?;

        serde_json::from_str(&body).map_err(|e| Error::Http {
            message: format!("Failed to parse table description: {}", e),
        })
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

    async fn check_table_response(&self, response: reqwest::Response) -> Result<reqwest::Response> {
        if response.status() == StatusCode::NOT_FOUND {
            return Err(Error::TableNotFound {
                name: self.name.clone(),
            });
        }

        self.client.check_response(response).await
    }

    async fn read_arrow_stream(
        &self,
        body: reqwest::Response,
    ) -> Result<SendableRecordBatchStream> {
        // There isn't a way to actually stream this data yet. I have an upstream issue:
        // https://github.com/apache/arrow-rs/issues/6420
        let body = body.bytes().await?;
        let reader = StreamReader::try_new(body.reader(), None)?;
        let schema = reader.schema();
        let stream = futures::stream::iter(reader).map_err(DataFusionError::from);
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
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

        let response = self.check_table_response(response).await?;

        let body = response.text().await?;

        serde_json::from_str(&body).map_err(|e| Error::Http {
            message: format!("Failed to parse row count: {}", e),
        })
    }
    async fn add(
        &self,
        add: AddDataBuilder<NoData>,
        data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        let body = Self::reader_as_body(data)?;
        let mut request = self
            .client
            .post(&format!("/table/{}/insert/", self.name))
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
            .body(body);

        match add.mode {
            AddDataMode::Append => {}
            AddDataMode::Overwrite => {
                request = request.query(&[("mode", "overwrite")]);
            }
        }

        let response = self.client.send(request).await?;

        self.check_table_response(response).await?;

        Ok(())
    }

    async fn create_plan(
        &self,
        _query: &VectorQuery,
        _options: QueryExecutionOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let request = self.client.post(&format!("/table/{}/query/", self.name));

        todo!("build request from query");

        let response = self.client.send(request).await?;

        let stream = self.read_arrow_stream(response).await?;

        Ok(Arc::new(OneShotExec::new(stream)))
    }

    async fn plain_query(
        &self,
        _query: &Query,
        _options: QueryExecutionOptions,
    ) -> Result<DatasetRecordBatchStream> {
        Err(Error::NotSupported {
            message: "plain_query is not yet supported on LanceDB cloud.".into(),
        })
    }
    async fn update(&self, update: UpdateBuilder) -> Result<u64> {
        let request = self.client.post(&format!("/table/{}/update/", self.name));

        let mut updates = Vec::new();
        for (column, expression) in update.columns {
            updates.push(column);
            updates.push(expression);
        }

        let request = request.json(&serde_json::json!({
            "updates": updates,
            "only_if": update.filter,
        }));

        let response = self.client.send(request).await?;

        let response = self.check_table_response(response).await?;

        let body = response.text().await?;

        serde_json::from_str(&body).map_err(|e| Error::Http {
            message: format!(
                "Failed to parse updated rows result from response {}: {}",
                body, e
            ),
        })
    }
    async fn delete(&self, predicate: &str) -> Result<()> {
        let body = serde_json::json!({ "predicate": predicate });
        let request = self
            .client
            .post(&format!("/table/{}/delete/", self.name))
            .json(&body);
        let response = self.client.send(request).await?;
        self.check_table_response(response).await?;
        Ok(())
    }
    async fn create_index(&self, _index: IndexBuilder) -> Result<()> {
        Err(Error::NotSupported {
            message: "create_index is not yet supported on LanceDB cloud.".into(),
        })
    }
    async fn merge_insert(
        &self,
        params: MergeInsertBuilder,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        let query = MergeInsertRequest::try_from(params)?;
        let body = Self::reader_as_body(new_data)?;
        let request = self
            .client
            .post(&format!("/table/{}/merge_insert/", self.name))
            .query(&query)
            .header(CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
            .body(body);

        let response = self.client.send(request).await?;

        self.check_table_response(response).await?;

        Ok(())
    }
    async fn optimize(&self, _action: OptimizeAction) -> Result<OptimizeStats> {
        Err(Error::NotSupported {
            message: "optimize is not supported on LanceDB cloud.".into(),
        })
    }
    async fn add_columns(
        &self,
        _transforms: NewColumnTransform,
        _read_columns: Option<Vec<String>>,
    ) -> Result<()> {
        Err(Error::NotSupported {
            message: "add_columns is not yet supported.".into(),
        })
    }
    async fn alter_columns(&self, _alterations: &[ColumnAlteration]) -> Result<()> {
        Err(Error::NotSupported {
            message: "alter_columns is not yet supported.".into(),
        })
    }
    async fn drop_columns(&self, _columns: &[&str]) -> Result<()> {
        Err(Error::NotSupported {
            message: "drop_columns is not yet supported.".into(),
        })
    }
    async fn list_indices(&self) -> Result<Vec<IndexConfig>> {
        Err(Error::NotSupported {
            message: "list_indices is not yet supported.".into(),
        })
    }
    async fn table_definition(&self) -> Result<TableDefinition> {
        Err(Error::NotSupported {
            message: "table_definition is not supported on LanceDB cloud.".into(),
        })
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

    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use futures::{future::BoxFuture, StreamExt, TryFutureExt};
    use reqwest::Body;

    use crate::{query::ExecutableQuery, Error, Table};

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
            assert_eq!(request.url().path(), "/table/my_table/insert/");
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
            assert_eq!(request.url().path(), "/table/my_table/update/");

            if let Some(body) = request.body().unwrap().as_bytes() {
                let body = std::str::from_utf8(body).unwrap();
                let value: serde_json::Value = serde_json::from_str(body).unwrap();
                let updates = value.get("updates").unwrap().as_array().unwrap();
                assert!(updates.len() == 2);

                let col_name = updates[0].as_str().unwrap();
                let expression = updates[1].as_str().unwrap();
                assert_eq!(col_name, "a");
                assert_eq!(expression, "a + 1");

                let only_if = value.get("only_if").unwrap().as_str().unwrap();
                assert_eq!(only_if, "b > 10");
            }

            http::Response::builder().status(200).body("1").unwrap()
        });

        table
            .update()
            .column("a", "a + 1")
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
            assert_eq!(request.url().path(), "/table/my_table/merge_insert/");

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
            assert_eq!(request.url().path(), "/table/my_table/merge_insert/");
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
            assert_eq!(request.url().path(), "/table/my_table/delete/");

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

        let table = Table::new_with_handler("my_table", |request| {
            assert_eq!(request.method(), "POST");
            assert_eq!(request.url().path(), "/table/my_table/delete/");

            let body = request.body().unwrap().as_bytes().unwrap();
            let body: serde_json::Value = serde_json::from_slice(body).unwrap();
            let expected_body = serde_json::json!({
                "vector": [0.1, 0.2, 0.3],
                "vector_column": "my_vector",
                "prefilter": false,
                "k": 42,
                "distance_type": "cosine",
                "bypass_vector_index": false,
                "columns": ["a", "b"],
                "nprobes": 12,
                "refine_factor": 2,
            });
            assert_eq!(body, expected_body);

            // TODO: test the body output
            http::Response::builder().status(200).body("").unwrap()
        });

        let data = table
            .query()
            .nearest_to(vec![0.1, 0.2, 0.3])
            .unwrap()
            .execute()
            .await;
    }

    #[tokio::test]
    async fn test_query_fts() {
        todo!()
    }
}
