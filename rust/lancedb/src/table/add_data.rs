// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow_schema::{DataType, Fields, Schema};
use lance::dataset::WriteMode;
use serde::{Deserialize, Serialize};

use crate::data::scannable::scannable_with_embeddings;
use crate::data::scannable::Scannable;
use crate::embeddings::EmbeddingRegistry;
use crate::table::datafusion::cast::cast_to_table_schema;
use crate::table::datafusion::reject_nan::reject_nan_vectors;
use crate::table::datafusion::scannable_exec::ScannableExec;
use crate::{Error, Result};

use super::{BaseTable, TableDefinition, WriteOptions};

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

#[derive(Debug, Default, Clone, Copy)]
pub enum NaNVectorBehavior {
    /// Reject any vectors containing NaN values (the default)
    #[default]
    Error,
    /// Allow NaN values to be added, but they will not be indexed for search
    Keep,
}

/// A builder for configuring a [`crate::table::Table::add`] operation
pub struct AddDataBuilder {
    pub(crate) parent: Arc<dyn BaseTable>,
    pub(crate) data: Box<dyn Scannable>,
    pub(crate) mode: AddDataMode,
    pub(crate) write_options: WriteOptions,
    pub(crate) on_nan_vectors: NaNVectorBehavior,
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
            on_nan_vectors: NaNVectorBehavior::default(),
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

    /// Configure how to handle NaN values in vector columns.
    ///
    /// By default, any vectors containing NaN values will be rejected with an
    /// error, since NaNs cannot be indexed for search. Setting this to `Keep`
    /// will allow NaN values to be added to the table, but they will not be
    /// indexed and will not be searchable.
    pub fn on_nan_vectors(mut self, behavior: NaNVectorBehavior) -> Self {
        self.on_nan_vectors = behavior;
        self
    }

    pub async fn execute(self) -> Result<AddResult> {
        self.parent.clone().add(self).await
    }

    /// Build a DataFusion execution plan that applies embeddings, casts data to
    /// the table schema, and optionally rejects NaN vectors.
    ///
    /// Returns the plan along with whether the input is rescannable (for retry
    /// decisions) and whether this is an overwrite operation.
    pub(crate) fn into_plan(
        mut self,
        table_schema: &Schema,
        table_def: &TableDefinition,
    ) -> Result<PreprocessingOutput> {
        let overwrite = self
            .write_options
            .lance_write_params
            .as_ref()
            .is_some_and(|p| matches!(p.mode, WriteMode::Overwrite))
            || matches!(self.mode, AddDataMode::Overwrite);

        if !overwrite {
            validate_schema(&self.data.schema(), table_schema)?;
        }

        self.data =
            scannable_with_embeddings(self.data, table_def, self.embedding_registry.as_ref())?;

        let rescannable = self.data.rescannable();
        let plan: Arc<dyn datafusion_physical_plan::ExecutionPlan> =
            Arc::new(ScannableExec::new(self.data));
        // Skip casting when overwriting — the input schema replaces the table schema.
        let plan = if overwrite {
            plan
        } else {
            cast_to_table_schema(plan, table_schema)?
        };
        let plan = match self.on_nan_vectors {
            NaNVectorBehavior::Error => reject_nan_vectors(plan)?,
            NaNVectorBehavior::Keep => plan,
        };

        Ok(PreprocessingOutput {
            plan,
            overwrite,
            rescannable,
            write_options: self.write_options,
            mode: self.mode,
        })
    }
}

pub struct PreprocessingOutput {
    pub plan: Arc<dyn datafusion_physical_plan::ExecutionPlan>,
    pub overwrite: bool,
    pub rescannable: bool,
    pub write_options: WriteOptions,
    pub mode: AddDataMode,
}

/// Check that the input schema is valid for insert.
///
/// Fields can be in different orders, so match by name.
///
/// If a column exists in input but not in table, error (no extra columns allowed).
///
/// If a column exists in table but not in input, that is okay - it may be filled with nulls.
///
/// If the types are not exactly the same, we will attempt to cast later - so that is also okay at this stage.
///
/// If the nullability is different, that is also okay - we can relax nullability when casting.
fn validate_schema(input: &Schema, table: &Schema) -> Result<()> {
    validate_fields(input.fields(), table.fields())
}

fn validate_fields(input: &Fields, table: &Fields) -> Result<()> {
    for field in input {
        match table.iter().find(|f| f.name() == field.name()) {
            None => {
                return Err(Error::InvalidInput {
                    message: format!("field '{}' does not exist in table schema", field.name()),
                });
            }
            Some(table_field) => {
                if let (DataType::Struct(in_children), DataType::Struct(tbl_children)) =
                    (field.data_type(), table_field.data_type())
                {
                    validate_fields(in_children, tbl_children)?;
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::datatypes::Float64Type;
    use arrow_array::{
        record_batch, FixedSizeListArray, Float32Array, Int32Array, LargeStringArray, ListArray,
        RecordBatch, RecordBatchIterator,
    };
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
    use crate::table::add_data::NaNVectorBehavior;
    use crate::table::{ColumnDefinition, ColumnKind, Table, TableDefinition, WriteOptions};
    use crate::test_utils::embeddings::MockEmbed;
    use crate::test_utils::TestCustomError;
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
        let inner = futures::stream::iter(data.into_iter().map(Ok));
        let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
            schema,
            stream: inner,
        });
        test_add_with_data(stream).await;
    }

    fn assert_preserves_external_error(err: &Error) {
        assert!(
            matches!(err, Error::External { source } if source.downcast_ref::<TestCustomError>().is_some()),
            "Expected Error::External, got: {err:?}"
        );
        // The original TestCustomError message should be preserved through the
        // error chain, even if the error gets wrapped multiple times by
        // lance's insert pipeline.
        assert!(
            err.to_string().contains("TestCustomError occurred"),
            "Expected original error message to be preserved, got: {err}"
        );
    }

    #[tokio::test]
    async fn test_add_preserves_reader_error() {
        let table = create_test_table().await;
        let first_batch = record_batch!(("id", Int64, [4])).unwrap();
        let schema = first_batch.schema();
        let iterator = vec![
            Ok(first_batch),
            Err(ArrowError::ExternalError(Box::new(TestCustomError))),
        ];
        let reader: Box<dyn arrow_array::RecordBatchReader + Send> = Box::new(
            RecordBatchIterator::new(iterator.into_iter(), schema.clone()),
        );

        let result = table.add(reader).execute().await;

        assert_preserves_external_error(&result.unwrap_err());
    }

    #[tokio::test]
    async fn test_add_preserves_stream_error() {
        let table = create_test_table().await;
        let first_batch = record_batch!(("id", Int64, [4])).unwrap();
        let schema = first_batch.schema();
        let iterator = vec![
            Ok(first_batch),
            Err(Error::External {
                source: Box::new(TestCustomError),
            }),
        ];
        let stream = futures::stream::iter(iterator);
        let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
            schema: schema.clone(),
            stream,
        });

        let result = table.add(stream).execute().await;

        assert_preserves_external_error(&result.unwrap_err());
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

    #[tokio::test]
    async fn test_add_casts_to_table_schema() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
            Field::new(
                "embedding",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]));

        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false), // Upcast integer
            Field::new("text", DataType::LargeUtf8, false), // Re-encode string
            // Cast list of float64 to fixed-size list of float32
            // (This will only work if list size is correct. See next test.
            Field::new(
                "embedding",
                DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
                false,
            ),
        ]));

        let db = connect("memory://").execute().await.unwrap();
        let table = db
            .create_empty_table("cast_test", table_schema.clone())
            .execute()
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            input_schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(LargeStringArray::from(vec!["hello", "world"])),
                Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                    Some(vec![0.1, 0.2, 0.3, 0.4].into_iter().map(Some)),
                    Some(vec![0.5, 0.6, 0.7, 0.8].into_iter().map(Some)),
                ])),
            ],
        )
        .unwrap();
        table.add(batch).execute().await.unwrap();

        let row_count = table.count_rows(None).await.unwrap();
        assert_eq!(row_count, 2);
    }

    #[tokio::test]
    async fn test_add_rejects_bad_vector_dimensions() {
        let table_schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        )]));

        let input_schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float64, true))),
            false,
        )]));

        let db = connect("memory://").execute().await.unwrap();
        let table = db
            .create_empty_table("cast_test", table_schema.clone())
            .execute()
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            input_schema,
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
                    Some(vec![0.1, 0.2, 0.3, 0.4].into_iter().map(Some)),
                    Some(vec![0.5, 0.6, 0.8].into_iter().map(Some)),
                ]),
            )],
        )
        .unwrap();
        let res = table.add(batch).execute().await;

        // TODO: to recover the error, we will need fix upstream in Lance.
        // assert!(
        //     matches!(res, Err(Error::Arrow { source: ArrowError::CastError(_) })),
        //     "Expected schema mismatch error due to wrong vector dimensions, but got: {res:?}"
        // );
        assert!(
            res.is_err(),
            "Expected error due to wrong vector dimensions, but got success"
        );
    }

    #[tokio::test]
    async fn test_add_rejects_nan_vectors() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            false,
        )]));

        let db = connect("memory://").execute().await.unwrap();
        let table = db
            .create_empty_table("nan_test", schema.clone())
            .execute()
            .await
            .unwrap();

        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(
                FixedSizeListArray::try_new(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    4,
                    Arc::new(Float32Array::from(vec![0.1, 0.2, f32::NAN, 0.4])),
                    None,
                )
                .unwrap(),
            )],
        )
        .unwrap();
        let res = table.add(batch.clone()).execute().await;
        let err = res.unwrap_err();
        assert!(
            err.to_string().contains("NaN"),
            "Expected error mentioning NaN values, but got: {err:?}"
        );

        table
            .add(batch)
            .on_nan_vectors(NaNVectorBehavior::Keep)
            .execute()
            .await
            .unwrap();

        let row_count = table.count_rows(None).await.unwrap();
        assert_eq!(row_count, 1);
    }

    #[tokio::test]
    async fn test_add_subschema() {
        let data = record_batch!(("id", Int64, [4, 5]), ("text", Utf8, ["foo", "bar"])).unwrap();
        let db = connect("memory://").execute().await.unwrap();
        let table = db
            .create_table("test", data.clone())
            .execute()
            .await
            .unwrap();

        let new_data = record_batch!(("id", Int64, [6, 7])).unwrap();
        table.add(new_data).execute().await.unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 4);
        assert_eq!(
            table
                .count_rows(Some("id IS NOT NULL".to_string()))
                .await
                .unwrap(),
            4
        );
        assert_eq!(
            table
                .count_rows(Some("text IS NOT NULL".to_string()))
                .await
                .unwrap(),
            2
        );

        // We can still cast
        let new_data = record_batch!(("text", LargeUtf8, ["baz", "qux"])).unwrap();
        table.add(new_data).execute().await.unwrap();

        assert_eq!(table.count_rows(None).await.unwrap(), 6);
        assert_eq!(
            table
                .count_rows(Some("id IS NOT NULL".to_string()))
                .await
                .unwrap(),
            4
        );
        assert_eq!(
            table
                .count_rows(Some("text IS NOT NULL".to_string()))
                .await
                .unwrap(),
            4
        );

        // Extra columns mean an error
        let new_data =
            record_batch!(("id", Int64, [8, 9]), ("extra", Utf8, ["extra1", "extra2"])).unwrap();
        let res = table.add(new_data).execute().await;
        assert!(
            res.is_err(),
            "Expected error due to extra column, but got: {res:?}"
        );

        // Insert with a subset of struct sub-fields
        let struct_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "metadata",
                DataType::Struct(
                    vec![
                        Field::new("a", DataType::Int64, true),
                        Field::new("b", DataType::Utf8, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]));
        let db2 = connect("memory://").execute().await.unwrap();
        let table2 = db2
            .create_empty_table("struct_test", struct_schema)
            .execute()
            .await
            .unwrap();

        // Insert with only the "a" sub-field of the struct
        let sub_struct_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "metadata",
                DataType::Struct(vec![Field::new("a", DataType::Int64, true)].into()),
                true,
            ),
        ]));
        let struct_batch = RecordBatch::try_new(
            sub_struct_schema,
            vec![
                Arc::new(arrow_array::Int64Array::from(vec![1, 2])),
                Arc::new(arrow_array::StructArray::from(vec![(
                    Arc::new(Field::new("a", DataType::Int64, true)),
                    Arc::new(arrow_array::Int64Array::from(vec![10, 20]))
                        as Arc<dyn arrow_array::Array>,
                )])),
            ],
        )
        .unwrap();
        table2.add(struct_batch).execute().await.unwrap();
        assert_eq!(table2.count_rows(None).await.unwrap(), 2);
    }
}
