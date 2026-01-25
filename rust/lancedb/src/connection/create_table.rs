// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use lance_io::object_store::StorageOptionsProvider;

use crate::{
    connection::{merge_storage_options, set_storage_options_provider},
    data::scannable::Scannable,
    database::{CreateTableMode, CreateTableRequest, Database},
    embeddings::{EmbeddingDefinition, EmbeddingFunction, EmbeddingRegistry},
    table::WriteOptions,
    Error, Result, Table,
};

pub struct CreateTableBuilder {
    parent: Arc<dyn Database>,
    embeddings: Vec<(EmbeddingDefinition, Arc<dyn EmbeddingFunction>)>,
    embedding_registry: Arc<dyn EmbeddingRegistry>,
    request: CreateTableRequest,
}

impl CreateTableBuilder {
    pub(super) fn new(
        parent: Arc<dyn Database>,
        embedding_registry: Arc<dyn EmbeddingRegistry>,
        name: String,
        data: Box<dyn Scannable>,
    ) -> Self {
        Self {
            parent,
            embeddings: Vec::new(),
            embedding_registry,
            request: CreateTableRequest::new(name, data),
        }
    }

    /// Set the mode for creating the table
    ///
    /// This controls what happens if a table with the given name already exists
    pub fn mode(mut self, mode: CreateTableMode) -> Self {
        self.request.mode = mode;
        self
    }

    /// Apply the given write options when writing the initial data
    pub fn write_options(mut self, write_options: WriteOptions) -> Self {
        self.request.write_options = write_options;
        self
    }

    /// Set an option for the storage layer.
    ///
    /// Options already set on the connection will be inherited by the table,
    /// but can be overridden here.
    ///
    /// See available options at <https://lancedb.com/docs/storage/>
    pub fn storage_option(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        let store_params = self
            .request
            .write_options
            .lance_write_params
            .get_or_insert(Default::default())
            .store_params
            .get_or_insert(Default::default());
        merge_storage_options(store_params, [(key.into(), value.into())]);
        self
    }

    /// Set multiple options for the storage layer.
    ///
    /// Options already set on the connection will be inherited by the table,
    /// but can be overridden here.
    ///
    /// See available options at <https://lancedb.com/docs/storage/>
    pub fn storage_options(
        mut self,
        pairs: impl IntoIterator<Item = (impl Into<String>, impl Into<String>)>,
    ) -> Self {
        let store_params = self
            .request
            .write_options
            .lance_write_params
            .get_or_insert(Default::default())
            .store_params
            .get_or_insert(Default::default());
        let updates = pairs
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()));
        merge_storage_options(store_params, updates);
        self
    }

    /// Add an embedding definition to the table.
    ///
    /// The `embedding_name` must match the name of an embedding function that
    /// was previously registered with the connection's [`EmbeddingRegistry`].
    pub fn add_embedding(mut self, definition: EmbeddingDefinition) -> Result<Self> {
        // Early verification of the embedding name
        let embedding_func = self
            .embedding_registry
            .get(&definition.embedding_name)
            .ok_or_else(|| Error::EmbeddingFunctionNotFound {
                name: definition.embedding_name.clone(),
                reason: "No embedding function found in the connection's embedding_registry"
                    .to_string(),
            })?;

        self.embeddings.push((definition, embedding_func));
        Ok(self)
    }

    /// Set the namespace for the table
    pub fn namespace(mut self, namespace: Vec<String>) -> Self {
        self.request.namespace = namespace;
        self
    }

    /// Set a custom location for the table.
    ///
    /// If not set, the database will derive a location from its URI and the table name.
    /// This is useful when integrating with namespace systems that manage table locations.
    pub fn location(mut self, location: impl Into<String>) -> Self {
        self.request.location = Some(location.into());
        self
    }

    /// Set a storage options provider for automatic credential refresh.
    ///
    /// This allows tables to automatically refresh cloud storage credentials
    /// when they expire, enabling long-running operations on remote storage.
    pub fn storage_options_provider(mut self, provider: Arc<dyn StorageOptionsProvider>) -> Self {
        let store_params = self
            .request
            .write_options
            .lance_write_params
            .get_or_insert(Default::default())
            .store_params
            .get_or_insert(Default::default());
        set_storage_options_provider(store_params, provider);
        self
    }

    /// Execute the create table operation
    pub async fn execute(self) -> Result<Table> {
        let embedding_registry = self.embedding_registry.clone();
        let parent = self.parent.clone();
        Ok(Table::new_with_embedding_registry(
            parent.create_table(self.request).await?,
            parent,
            embedding_registry,
        ))
    }
}

#[cfg(test)]
mod tests {
    use arrow_array::{record_batch, RecordBatchIterator};
    use arrow_schema::{ArrowError, DataType, Field, Schema};
    use lance_file::version::LanceFileVersion;
    use tempfile::tempdir;

    use crate::{
        arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
        connect,
        database::listing::{ListingDatabaseOptions, NewTableConfig},
    };

    use super::*;

    #[tokio::test]
    async fn create_empty_table() {
        let db = connect("memory://").execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        db.create_empty_table("name", schema.clone())
            .execute()
            .await
            .unwrap();
        let table = db.open_table("name").execute().await.unwrap();
        assert_eq!(table.schema().await.unwrap(), schema);
        assert_eq!(table.count_rows(None).await.unwrap(), 0);
    }

    async fn test_create_table_with_data<T>(data: T)
    where
        T: Scannable + 'static,
    {
        let db = connect("memory://").execute().await.unwrap();
        let schema = data.schema();
        db.create_table("data_table", data).execute().await.unwrap();
        let table = db.open_table("data_table").execute().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 3);
        assert_eq!(table.schema().await.unwrap(), schema);
    }

    #[tokio::test]
    async fn create_table_with_batch() {
        let batch = record_batch!(("id", Int64, [1, 2, 3])).unwrap();
        test_create_table_with_data(batch).await;
    }

    #[tokio::test]
    async fn test_create_table_with_vec_batch() {
        let data = vec![
            record_batch!(("id", Int64, [1, 2])).unwrap(),
            record_batch!(("id", Int64, [3])).unwrap(),
        ];
        test_create_table_with_data(data).await;
    }

    #[tokio::test]
    async fn test_create_table_with_record_batch_reader() {
        let data = vec![
            record_batch!(("id", Int64, [1, 2])).unwrap(),
            record_batch!(("id", Int64, [3])).unwrap(),
        ];
        let schema = data[0].schema();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
            RecordBatchIterator::new(data.into_iter().map(Ok), schema.clone()),
        );
        test_create_table_with_data(reader).await;
    }

    #[tokio::test]
    async fn test_create_table_with_stream() {
        let data = vec![
            record_batch!(("id", Int64, [1, 2])).unwrap(),
            record_batch!(("id", Int64, [3])).unwrap(),
        ];
        let schema = data[0].schema();
        let stream = futures::stream::iter(data.into_iter().map(Ok));
        let stream: SendableRecordBatchStream =
            Box::pin(SimpleRecordBatchStream { schema, stream });
        test_create_table_with_data(stream).await;
    }

    #[derive(Debug)]
    struct MyError;

    impl std::fmt::Display for MyError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "MyError occurred")
        }
    }

    impl std::error::Error for MyError {}

    #[tokio::test]
    async fn test_create_preserves_reader_error() {
        let first_batch = record_batch!(("id", Int64, [1, 2])).unwrap();
        let schema = first_batch.schema();
        let iterator = vec![
            Ok(first_batch),
            Err(ArrowError::ExternalError(Box::new(MyError))),
        ];
        let reader = Box::new(RecordBatchIterator::new(
            iterator.into_iter(),
            schema.clone(),
        )) as Box<dyn arrow_array::RecordBatchReader + Send>;

        let db = connect("memory://").execute().await.unwrap();
        let result = db.create_table("failing_table", reader).execute().await;

        assert!(matches!(result, Err(Error::External { source})
            if source.downcast_ref::<MyError>().is_some()
        ));
    }

    #[tokio::test]
    async fn test_create_preserves_stream_error() {
        let first_batch = record_batch!(("id", Int64, [1, 2])).unwrap();
        let schema = first_batch.schema();
        let iterator = vec![
            Ok(first_batch),
            Err(Error::External {
                source: Box::new(MyError),
            }),
        ];
        let stream = futures::stream::iter(iterator);
        let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
            schema: schema.clone(),
            stream,
        });

        let db = connect("memory://").execute().await.unwrap();
        let result = db
            .create_table("failing_stream_table", stream)
            .execute()
            .await;

        assert!(matches!(result, Err(Error::External { source})
            if source.downcast_ref::<MyError>().is_some()
        ));
    }

    #[tokio::test]
    async fn test_create_table_with_storage_options() {
        let batch = record_batch!(("id", Int64, [1, 2, 3])).unwrap();
        let db = connect("memory://").execute().await.unwrap();

        let table = db
            .create_table("options_table", batch)
            .storage_option("timeout", "30s")
            .storage_options([("retry_count", "3")])
            .execute()
            .await
            .unwrap();

        let final_options = table.storage_options().await.unwrap();
        assert_eq!(final_options.get("timeout"), Some(&"30s".to_string()));
        assert_eq!(final_options.get("retry_count"), Some(&"3".to_string()));
    }

    #[tokio::test]
    async fn test_create_table_unregistered_embedding() {
        let db = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("text", Utf8, ["hello", "world"])).unwrap();

        // Try to add an embedding that doesn't exist in the registry
        let result = db
            .create_table("embed_table", batch)
            .add_embedding(EmbeddingDefinition::new(
                "text",
                "nonexistent_embedding_function",
                None::<&str>,
            ));

        match result {
            Err(Error::EmbeddingFunctionNotFound { name, .. }) => {
                assert_eq!(name, "nonexistent_embedding_function");
            }
            Err(other) => panic!("Expected EmbeddingFunctionNotFound error, got: {:?}", other),
            Ok(_) => panic!("Expected error, but got Ok"),
        }
    }

    #[tokio::test]
    async fn test_create_table_already_exists() {
        let tmp_dir = tempdir().unwrap();
        let uri = tmp_dir.path().to_str().unwrap();
        let db = connect(uri).execute().await.unwrap();
        let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
        db.create_empty_table("test", schema.clone())
            .execute()
            .await
            .unwrap();
        db.create_empty_table("test", schema)
            .mode(CreateTableMode::exist_ok(|mut req| {
                req.index_cache_size = Some(16);
                req
            }))
            .execute()
            .await
            .unwrap();
        let other_schema = Arc::new(Schema::new(vec![Field::new("y", DataType::Int32, false)]));
        assert!(db
            .create_empty_table("test", other_schema.clone())
            .execute()
            .await
            .is_err()); // TODO: assert what this error is
        let overwritten = db
            .create_empty_table("test", other_schema.clone())
            .mode(CreateTableMode::Overwrite)
            .execute()
            .await
            .unwrap();
        assert_eq!(other_schema, overwritten.schema().await.unwrap());
    }

    #[tokio::test]
    #[rstest::rstest]
    #[case(LanceFileVersion::Legacy)]
    #[case(LanceFileVersion::Stable)]
    async fn test_create_table_with_storage_version(
        #[case] data_storage_version: LanceFileVersion,
    ) {
        let db = connect("memory://")
            .database_options(&ListingDatabaseOptions {
                new_table_config: NewTableConfig {
                    data_storage_version: Some(data_storage_version),
                    ..Default::default()
                },
                ..Default::default()
            })
            .execute()
            .await
            .unwrap();

        let batch = record_batch!(("id", Int64, [1, 2, 3])).unwrap();
        let table = db
            .create_table("legacy_table", batch)
            .execute()
            .await
            .unwrap();

        let native_table = table.as_native().unwrap();
        let storage_format = native_table
            .manifest()
            .await
            .unwrap()
            .data_storage_format
            .lance_file_version()
            .unwrap();
        assert_eq!(storage_format, data_storage_version);
    }
}
