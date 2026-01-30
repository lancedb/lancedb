// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::data::scannable::Scannable;
use crate::embeddings::EmbeddingRegistry;
use crate::Result;

use super::{BaseTable, WriteOptions};

#[derive(Debug, Clone, Default)]
pub enum AddDataMode {
    /// Rows will be appended to the table (the default)
    #[default]
    Append,
    /// The existing table will be overwritten with the new data
    Overwrite,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct AddResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
}

/// A builder for configuring a [`crate::table::Table::add`] operation
pub struct AddDataBuilder {
    pub(crate) parent: Arc<dyn BaseTable>,
    pub(crate) data: Box<dyn Scannable>,
    pub(crate) mode: AddDataMode,
    pub(crate) write_options: WriteOptions,
    pub(crate) embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
}

impl std::fmt::Debug for AddDataBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AddDataBuilder")
            .field("parent", &self.parent)
            .field("mode", &self.mode)
            .field("write_options", &self.write_options)
            .finish()
    }
}

impl AddDataBuilder {
    pub(crate) fn new(
        parent: Arc<dyn BaseTable>,
        data: Box<dyn Scannable>,
        embedding_registry: Option<Arc<dyn EmbeddingRegistry>>,
    ) -> Self {
        Self {
            parent,
            data,
            mode: AddDataMode::Append,
            write_options: WriteOptions::default(),
            embedding_registry,
        }
    }

    pub fn mode(mut self, mode: AddDataMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn write_options(mut self, options: WriteOptions) -> Self {
        self.write_options = options;
        self
    }

    pub async fn execute(self) -> Result<AddResult> {
        self.parent.clone().add(self).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{record_batch, RecordBatch, RecordBatchIterator};
    use arrow_schema::{ArrowError, DataType, Field, Schema};
    use futures::TryStreamExt;
    use lance::dataset::{WriteMode, WriteParams};

    use crate::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
    use crate::connect;
    use crate::data::scannable::Scannable;
    use crate::embeddings::{
        EmbeddingDefinition, EmbeddingFunction, EmbeddingRegistry, MemoryRegistry,
    };
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::table::{ColumnDefinition, ColumnKind, Table, TableDefinition, WriteOptions};
    use crate::test_utils::embeddings::MockEmbed;
    use crate::Error;

    use super::AddDataMode;

    async fn create_test_table() -> Table {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("id", Int64, [1, 2, 3])).unwrap();
        conn.create_table("test", batch).execute().await.unwrap()
    }

    async fn test_add_with_data<T>(data: T)
    where
        T: Scannable + 'static,
    {
        let table = create_test_table().await;
        let schema = data.schema();
        table.add(data).execute().await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 5); // 3 initial + 2 added
        assert_eq!(table.schema().await.unwrap(), schema);
    }

    #[tokio::test]
    async fn test_add_with_batch() {
        let batch = record_batch!(("id", Int64, [4, 5])).unwrap();
        test_add_with_data(batch).await;
    }

    #[tokio::test]
    async fn test_add_with_vec_batch() {
        let data = vec![
            record_batch!(("id", Int64, [4])).unwrap(),
            record_batch!(("id", Int64, [5])).unwrap(),
        ];
        test_add_with_data(data).await;
    }

    #[tokio::test]
    async fn test_add_with_record_batch_reader() {
        let data = vec![
            record_batch!(("id", Int64, [4])).unwrap(),
            record_batch!(("id", Int64, [5])).unwrap(),
        ];
        let schema = data[0].schema();
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
            RecordBatchIterator::new(data.into_iter().map(Ok), schema.clone()),
        );
        test_add_with_data(reader).await;
    }

    #[tokio::test]
    async fn test_add_with_stream() {
        let data = vec![
            record_batch!(("id", Int64, [4])).unwrap(),
            record_batch!(("id", Int64, [5])).unwrap(),
        ];
        let schema = data[0].schema();
        let stream = futures::stream::iter(data.into_iter().map(Ok));
        let stream: SendableRecordBatchStream =
            Box::pin(SimpleRecordBatchStream { schema, stream });
        test_add_with_data(stream).await;
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
    async fn test_add_preserves_reader_error() {
        let table = create_test_table().await;
        let first_batch = record_batch!(("id", Int64, [4])).unwrap();
        let schema = first_batch.schema();
        let iterator = vec![
            Ok(first_batch),
            Err(ArrowError::ExternalError(Box::new(MyError))),
        ];
        let reader = Box::new(RecordBatchIterator::new(
            iterator.into_iter(),
            schema.clone(),
        )) as Box<dyn arrow_array::RecordBatchReader + Send>;

        let result = table.add(reader).execute().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add_preserves_stream_error() {
        let table = create_test_table().await;
        let first_batch = record_batch!(("id", Int64, [4])).unwrap();
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

        let result = table.add(stream).execute().await;

        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_add() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("i", Int32, [0, 1, 2])).unwrap();
        let table = conn
            .create_table("test", batch.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 3);

        let new_batch = record_batch!(("i", Int32, [3])).unwrap();
        table.add(new_batch).execute().await.unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 4);
        assert_eq!(table.schema().await.unwrap(), batch.schema());
    }

    #[tokio::test]
    async fn test_add_overwrite() {
        let conn = connect("memory://").execute().await.unwrap();

        let batch = record_batch!(("i", Int32, [0, 1, 2])).unwrap();
        let table = conn
            .create_table("test", batch.clone())
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), batch.num_rows());

        let new_batch = record_batch!(("x", Float32, [0.0, 1.0])).unwrap();
        let res = table
            .add(new_batch.clone())
            .mode(AddDataMode::Overwrite)
            .execute()
            .await
            .unwrap();
        assert_eq!(res.version, table.version().await.unwrap());
        assert_eq!(table.count_rows(None).await.unwrap(), new_batch.num_rows());
        assert_eq!(table.schema().await.unwrap(), new_batch.schema());

        // Can overwrite using underlying WriteParams (which
        // take precedence over AddDataMode)
        let param: WriteParams = WriteParams {
            mode: WriteMode::Overwrite,
            ..Default::default()
        };

        table
            .add(new_batch.clone())
            .write_options(WriteOptions {
                lance_write_params: Some(param),
            })
            .mode(AddDataMode::Append)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), new_batch.num_rows());
    }

    #[tokio::test]
    async fn test_add_with_embeddings() {
        let registry = Arc::new(MemoryRegistry::new());
        let mock_embedding: Arc<dyn EmbeddingFunction> = Arc::new(MockEmbed::new("mock", 4));
        registry.register("mock", mock_embedding).unwrap();

        let conn = connect("memory://")
            .embedding_registry(registry)
            .execute()
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "text_embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]));

        // Add embedding metadata to the schema
        let embedding_def = EmbeddingDefinition::new("text", "mock", Some("text_embedding"));
        let table_def = TableDefinition::new(
            schema.clone(),
            vec![
                ColumnDefinition {
                    kind: ColumnKind::Physical,
                },
                ColumnDefinition {
                    kind: ColumnKind::Embedding(embedding_def),
                },
            ],
        );
        let rich_schema = table_def.into_rich_schema();

        let table = conn
            .create_empty_table("embed_test", rich_schema)
            .execute()
            .await
            .unwrap();

        // Now add new data WITHOUT the embedding column - it should be computed automatically
        let new_batch = record_batch!(("text", Utf8, ["hello", "world"])).unwrap();
        table.add(new_batch).execute().await.unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 2);

        // Query to verify the embeddings were computed for the new rows
        let results: Vec<RecordBatch> = table
            .query()
            .select(Select::columns(&["text", "text_embedding"]))
            .execute()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();

        let total_rows: usize = results.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Check that all rows have embedding values (not null)
        for batch in &results {
            let embedding_col = batch.column(1);
            assert_eq!(embedding_col.null_count(), 0);
        }
    }
}
