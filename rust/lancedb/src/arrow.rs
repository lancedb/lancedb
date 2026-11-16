// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{pin::Pin, sync::Arc};

// Re-export the arrow crates we depend on so downstream consumers can build
// `RecordBatch`/arrays/builders against the exact same arrow line lancedb was
// compiled against, instead of declaring their own (potentially mismatched)
// direct arrow dependencies. See https://github.com/lancedb/lancedb/issues/3575.
pub use arrow;
pub use arrow_array;
pub use arrow_buffer;
pub use arrow_cast;
pub use arrow_data;
pub use arrow_ipc;
pub use arrow_ord;
pub use arrow_schema;
pub use arrow_select;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt, TryStreamExt};
use lance_datagen::{BatchCount, BatchGeneratorBuilder, RowCount};

#[cfg(feature = "polars")]
use {crate::polars_arrow_convertors, polars::frame::ArrowChunk, polars::prelude::DataFrame};

use crate::{Error, error::Result};

/// An iterator of batches that also has a schema
pub trait RecordBatchReader: Iterator<Item = Result<arrow_array::RecordBatch>> {
    /// Returns the schema of this `RecordBatchReader`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// reader should have the same schema as returned from this method.
    fn schema(&self) -> Arc<arrow_schema::Schema>;
}

/// A simple RecordBatchReader formed from the two parts (iterator + schema)
pub struct SimpleRecordBatchReader<I: Iterator<Item = Result<arrow_array::RecordBatch>>> {
    pub schema: Arc<arrow_schema::Schema>,
    pub batches: I,
}

impl<I: Iterator<Item = Result<arrow_array::RecordBatch>>> Iterator for SimpleRecordBatchReader<I> {
    type Item = Result<arrow_array::RecordBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next()
    }
}

impl<I: Iterator<Item = Result<arrow_array::RecordBatch>>> RecordBatchReader
    for SimpleRecordBatchReader<I>
{
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.schema.clone()
    }
}

/// A stream of batches that also has a schema
pub trait RecordBatchStream: Stream<Item = Result<arrow_array::RecordBatch>> {
    /// Returns the schema of this `RecordBatchStream`.
    ///
    /// Implementation of this trait should guarantee that all `RecordBatch`'s returned by this
    /// stream should have the same schema as returned from this method.
    fn schema(&self) -> Arc<arrow_schema::Schema>;
}

/// A boxed RecordBatchStream that is also Send
pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

impl<I: lance::io::RecordBatchStream + 'static> From<I> for SendableRecordBatchStream {
    fn from(stream: I) -> Self {
        let schema = stream.schema();
        let mapped_stream = Box::pin(stream.map(|r| r.map_err(Into::into)));
        Box::pin(SimpleRecordBatchStream {
            schema,
            stream: mapped_stream,
        })
    }
}

pub trait SendableRecordBatchStreamExt {
    fn into_df_stream(self) -> datafusion_physical_plan::SendableRecordBatchStream;
}

impl SendableRecordBatchStreamExt for SendableRecordBatchStream {
    fn into_df_stream(self) -> datafusion_physical_plan::SendableRecordBatchStream {
        let schema = self.schema();
        Box::pin(RecordBatchStreamAdapter::new(
            schema,
            self.map_err(|ldb_err| DataFusionError::External(ldb_err.into())),
        ))
    }
}

/// A simple RecordBatchStream formed from the two parts (stream + schema)
#[pin_project::pin_project]
pub struct SimpleRecordBatchStream<S: Stream<Item = Result<arrow_array::RecordBatch>>> {
    pub schema: Arc<arrow_schema::Schema>,
    #[pin]
    pub stream: S,
}

impl<S: Stream<Item = Result<arrow_array::RecordBatch>>> Stream for SimpleRecordBatchStream<S> {
    type Item = Result<arrow_array::RecordBatch>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        this.stream.poll_next(cx)
    }
}

impl<S: Stream<Item = Result<arrow_array::RecordBatch>>> RecordBatchStream
    for SimpleRecordBatchStream<S>
{
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.schema.clone()
    }
}

/// A trait for converting incoming data to Arrow
///
pub type BoxedRecordBatchReader = Box<dyn arrow_array::RecordBatchReader + Send>;

impl<S: Stream<Item = Result<arrow_array::RecordBatch>>> SimpleRecordBatchStream<S> {
    pub fn new(stream: S, schema: Arc<arrow_schema::Schema>) -> Self {
        Self { schema, stream }
    }
}

pub trait LanceDbDatagenExt {
    fn into_ldb_stream(
        self,
        batch_size: RowCount,
        num_batches: BatchCount,
    ) -> SendableRecordBatchStream;
}

impl LanceDbDatagenExt for BatchGeneratorBuilder {
    fn into_ldb_stream(
        self,
        batch_size: RowCount,
        num_batches: BatchCount,
    ) -> SendableRecordBatchStream {
        let (stream, schema) = self.into_reader_stream(batch_size, num_batches);
        let stream = stream.map_err(|err| Error::Arrow { source: err });
        Box::pin(SimpleRecordBatchStream::new(stream, schema))
    }
}

#[cfg(feature = "polars")]
/// An iterator of record batches formed from a Polars DataFrame.
pub struct PolarsDataFrameRecordBatchReader {
    chunks: std::vec::IntoIter<ArrowChunk>,
    arrow_schema: Arc<arrow_schema::Schema>,
}

#[cfg(feature = "polars")]
impl PolarsDataFrameRecordBatchReader {
    /// Creates a new `PolarsDataFrameRecordBatchReader` from a given Polars DataFrame.
    /// If the input dataframe does not have aligned chunks, this function undergoes
    /// the costly operation of reallocating each series as a single contiguous chunk.
    pub fn new(mut df: DataFrame) -> Result<Self> {
        df.align_chunks();
        let arrow_schema =
            polars_arrow_convertors::convert_polars_df_schema_to_arrow_rb_schema(df.schema())?;
        Ok(Self {
            chunks: df
                .iter_chunks(polars_arrow_convertors::POLARS_ARROW_FLAVOR)
                .collect::<Vec<ArrowChunk>>()
                .into_iter(),
            arrow_schema,
        })
    }
}

#[cfg(feature = "polars")]
impl Iterator for PolarsDataFrameRecordBatchReader {
    type Item = std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next().map(|chunk| {
            let columns: std::result::Result<Vec<arrow_array::ArrayRef>, arrow_schema::ArrowError> =
                chunk
                    .into_arrays()
                    .into_iter()
                    .zip(self.arrow_schema.fields.iter())
                    .map(|(polars_array, arrow_field)| {
                        polars_arrow_convertors::convert_polars_arrow_array_to_arrow_rs_array(
                            polars_array,
                            arrow_field.data_type().clone(),
                        )
                    })
                    .collect();
            arrow_array::RecordBatch::try_new(self.arrow_schema.clone(), columns?)
        })
    }
}

#[cfg(feature = "polars")]
impl arrow_array::RecordBatchReader for PolarsDataFrameRecordBatchReader {
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.arrow_schema.clone()
    }
}

/// A trait for converting the result of a LanceDB query into a Polars DataFrame with aligned
/// chunks. The resulting Polars DataFrame will have aligned chunks, but the series's
/// chunks are not guaranteed to be contiguous.
#[cfg(feature = "polars")]
pub trait IntoPolars {
    fn into_polars(self) -> impl std::future::Future<Output = Result<DataFrame>> + Send;
}

#[cfg(feature = "polars")]
impl IntoPolars for SendableRecordBatchStream {
    async fn into_polars(mut self) -> Result<DataFrame> {
        let polars_schema =
            polars_arrow_convertors::convert_arrow_rb_schema_to_polars_df_schema(&self.schema())?;
        let mut acc_df: DataFrame = DataFrame::from(&polars_schema);
        while let Some(record_batch) = self.next().await {
            let new_df = polars_arrow_convertors::convert_arrow_rb_to_polars_df(
                &record_batch?,
                &polars_schema,
            )?;
            acc_df = acc_df.vstack(&new_df)?;
        }
        Ok(acc_df)
    }
}

#[cfg(feature = "serde")]
fn struct_tracing_options() -> serde_arrow::schema::TracingOptions {
    // serde_arrow defaults to 64-bit offsets (LargeUtf8, LargeList, LargeBinary).
    // Use the 32-bit types that the rest of LanceDB and pyarrow produce.
    serde_arrow::schema::TracingOptions::default()
        .sequence_as_large_list(false)
        .strings_as_large_utf8(false)
        .bytes_as_large_binary(false)
}

/// Convert a slice of serde-serializable records into a [`RecordBatch`].
///
/// The Arrow schema is derived from the type `T`, so it works for an empty
/// slice too. `Vec<T>` fields become `List` columns. Adding the batch to an
/// existing table casts them to the table's `FixedSizeList` vector columns,
/// but a table created from this batch has no vector column that can be
/// searched. Use [`from_structs_with_schema`] to create tables with vectors.
///
/// ```
/// use lancedb::arrow::from_structs;
/// use serde::{Deserialize, Serialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Doc {
///     id: i64,
///     text: String,
///     vector: Vec<f32>,
/// }
///
/// # async fn add(table: &lancedb::Table) -> lancedb::Result<()> {
/// let docs = vec![Doc { id: 1, text: "hello".into(), vector: vec![0.1, 0.2] }];
/// table.add(from_structs(&docs)?).execute().await?;
/// # Ok(())
/// # }
/// ```
///
/// [`RecordBatch`]: arrow_array::RecordBatch
#[cfg(feature = "serde")]
pub fn from_structs<T>(records: &[T]) -> Result<arrow_array::RecordBatch>
where
    T: serde::Serialize + serde::de::DeserializeOwned,
{
    use serde_arrow::schema::SchemaLike;

    let fields = Vec::<arrow_schema::FieldRef>::from_type::<T>(struct_tracing_options())?;
    Ok(serde_arrow::to_record_batch(&fields, &records)?)
}

/// Convert a slice of serde-serializable records into a [`RecordBatch`] with
/// the given schema.
///
/// Struct fields are matched to columns by name. Use this to control the Arrow
/// types, for example to declare a vector column as a `FixedSizeList` so a
/// table created from the batch can be searched.
///
/// ```
/// use std::sync::Arc;
///
/// use lancedb::arrow::arrow_schema::{DataType, Field, Schema};
/// use lancedb::arrow::from_structs_with_schema;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Doc {
///     id: i64,
///     vector: Vec<f32>,
/// }
///
/// # async fn create(db: &lancedb::Connection) -> lancedb::Result<()> {
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int64, false),
///     Field::new(
///         "vector",
///         DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
///         false,
///     ),
/// ]));
/// let docs = vec![Doc { id: 1, vector: vec![0.1, 0.2] }];
/// db.create_table("docs", from_structs_with_schema(&docs, schema)?)
///     .execute()
///     .await?;
/// # Ok(())
/// # }
/// ```
///
/// [`RecordBatch`]: arrow_array::RecordBatch
#[cfg(feature = "serde")]
pub fn from_structs_with_schema<T>(
    records: &[T],
    schema: arrow_schema::SchemaRef,
) -> Result<arrow_array::RecordBatch>
where
    T: serde::Serialize,
{
    let batch = serde_arrow::to_record_batch(schema.fields(), &records)?;
    // serde_arrow only takes the fields, so put the schema metadata back.
    Ok(arrow_array::RecordBatch::try_new(
        schema,
        batch.columns().to_vec(),
    )?)
}

/// A trait for collecting the result of a LanceDB query into a `Vec` of
/// serde-deserializable records.
///
/// Columns are matched to struct fields by name. Columns the struct does not
/// declare, such as `_distance` from a vector search, are ignored.
///
/// ```
/// use lancedb::arrow::IntoStructs;
/// use lancedb::query::{ExecutableQuery, QueryBase};
/// use serde::Deserialize;
///
/// #[derive(Deserialize)]
/// struct Hit {
///     id: i64,
///     _distance: f32,
/// }
///
/// # async fn search(table: &lancedb::Table) -> lancedb::Result<()> {
/// let hits: Vec<Hit> = table
///     .query()
///     .nearest_to(&[0.1f32, 0.2])?
///     .execute()
///     .await?
///     .into_structs()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[cfg(feature = "serde")]
pub trait IntoStructs {
    fn into_structs<T>(self) -> impl std::future::Future<Output = Result<Vec<T>>> + Send
    where
        T: serde::de::DeserializeOwned + Send;
}

#[cfg(feature = "serde")]
impl IntoStructs for SendableRecordBatchStream {
    async fn into_structs<T>(mut self) -> Result<Vec<T>>
    where
        T: serde::de::DeserializeOwned + Send,
    {
        let mut records = Vec::new();
        while let Some(batch) = self.next().await {
            records.extend(serde_arrow::from_record_batch::<Vec<T>>(&batch?)?);
        }
        Ok(records)
    }
}

#[cfg(all(test, feature = "serde"))]
mod serde_tests {
    use std::sync::Arc;

    use arrow_schema::{DataType, Field, Schema, SchemaRef};
    use serde::{Deserialize, Serialize};

    use super::{IntoStructs, from_structs, from_structs_with_schema};
    use crate::query::{ExecutableQuery, QueryBase};

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct Location {
        x: f64,
        y: f64,
    }

    #[derive(Serialize, Deserialize, Debug, Clone, PartialEq)]
    struct Record {
        id: i64,
        caption: String,
        tag: Option<String>,
        location: Location,
        vector: Vec<f32>,
    }

    fn records() -> Vec<Record> {
        (0..4)
            .map(|i| Record {
                id: i,
                caption: format!("caption {i}"),
                tag: (i % 2 == 0).then(|| format!("tag {i}")),
                location: Location {
                    x: i as f64,
                    y: -(i as f64),
                },
                vector: vec![i as f32; 4],
            })
            .collect()
    }

    fn vector_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("caption", DataType::Utf8, false),
            Field::new("tag", DataType::Utf8, true),
            Field::new(
                "location",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Float64, false),
                        Field::new("y", DataType::Float64, false),
                    ]
                    .into(),
                ),
                false,
            ),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                false,
            ),
        ]))
    }

    #[test]
    fn test_from_structs_schema() {
        let batch = from_structs(&records()).unwrap();
        assert_eq!(batch.num_rows(), 4);
        let schema = batch.schema();
        assert_eq!(
            schema.field_with_name("caption").unwrap().data_type(),
            &DataType::Utf8
        );
        assert!(schema.field_with_name("tag").unwrap().is_nullable());
        assert!(matches!(
            schema.field_with_name("vector").unwrap().data_type(),
            DataType::List(_)
        ));
    }

    #[test]
    fn test_from_structs_empty() {
        let batch = from_structs::<Record>(&[]).unwrap();
        assert_eq!(batch.num_rows(), 0);
        assert_eq!(batch.num_columns(), 5);
    }

    #[test]
    fn test_from_structs_with_schema_keeps_metadata() {
        let schema = Arc::new(
            vector_schema()
                .as_ref()
                .clone()
                .with_metadata([("key".to_string(), "value".to_string())].into()),
        );
        let batch = from_structs_with_schema(&records(), schema.clone()).unwrap();
        assert_eq!(batch.schema(), schema);
    }

    #[test]
    fn test_from_structs_with_schema_wrong_dimension() {
        let mut bad = records();
        bad[1].vector = vec![1.0];
        let err = from_structs_with_schema(&bad, vector_schema()).unwrap_err();
        assert!(err.to_string().contains("FixedSizeList"), "{err}");
    }

    #[tokio::test]
    async fn test_add_and_read_back() {
        let db = crate::connect("memory://").execute().await.unwrap();
        let table = db
            .create_empty_table("t", vector_schema())
            .execute()
            .await
            .unwrap();

        table
            .add(from_structs(&records()).unwrap())
            .execute()
            .await
            .unwrap();

        let mut rows: Vec<Record> = table
            .query()
            .execute()
            .await
            .unwrap()
            .into_structs()
            .await
            .unwrap();
        rows.sort_by_key(|r| r.id);
        assert_eq!(rows, records());
    }

    #[tokio::test]
    async fn test_create_table_and_vector_search() {
        #[derive(Deserialize)]
        struct Hit {
            id: i64,
            _distance: f32,
        }

        let db = crate::connect("memory://").execute().await.unwrap();
        let table = db
            .create_table(
                "t",
                from_structs_with_schema(&records(), vector_schema()).unwrap(),
            )
            .execute()
            .await
            .unwrap();

        let hits: Vec<Hit> = table
            .query()
            .nearest_to(&[2.0f32; 4])
            .unwrap()
            .limit(1)
            .execute()
            .await
            .unwrap()
            .into_structs()
            .await
            .unwrap();
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].id, 2);
        assert_eq!(hits[0]._distance, 0.0);
    }

    #[tokio::test]
    async fn test_into_structs_missing_column() {
        #[derive(Deserialize, Debug)]
        #[allow(dead_code)]
        struct Missing {
            id: i64,
            not_a_column: String,
        }

        let db = crate::connect("memory://").execute().await.unwrap();
        let table = db
            .create_table(
                "t",
                from_structs_with_schema(&records(), vector_schema()).unwrap(),
            )
            .execute()
            .await
            .unwrap();

        let result = table
            .query()
            .execute()
            .await
            .unwrap()
            .into_structs::<Missing>()
            .await;
        assert!(result.is_err());
    }
}

#[cfg(all(test, feature = "polars"))]
mod tests {
    use super::SendableRecordBatchStream;
    use crate::arrow::{IntoPolars, PolarsDataFrameRecordBatchReader, SimpleRecordBatchStream};
    use polars::prelude::{DataFrame, NamedFrom, Series};

    fn get_record_batch_reader_from_polars() -> Box<dyn arrow_array::RecordBatchReader + Send> {
        let mut string_series = Series::new("string", &["ab"]);
        let mut int_series = Series::new("int", &[1]);
        let mut float_series = Series::new("float", &[1.0]);
        let df1 = DataFrame::new(vec![string_series, int_series, float_series]).unwrap();

        string_series = Series::new("string", &["bc"]);
        int_series = Series::new("int", &[2]);
        float_series = Series::new("float", &[2.0]);
        let df2 = DataFrame::new(vec![string_series, int_series, float_series]).unwrap();

        Box::new(PolarsDataFrameRecordBatchReader::new(df1.vstack(&df2).unwrap()).unwrap())
    }

    #[test]
    fn from_polars_to_arrow() {
        let record_batch_reader = get_record_batch_reader_from_polars();
        let schema = record_batch_reader.schema();

        // Test schema conversion
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| (field.name().as_str(), field.data_type()))
                .collect::<Vec<_>>(),
            vec![
                ("string", &arrow_schema::DataType::LargeUtf8),
                ("int", &arrow_schema::DataType::Int32),
                ("float", &arrow_schema::DataType::Float64)
            ]
        );
        let record_batches: Vec<arrow_array::RecordBatch> =
            record_batch_reader.map(|result| result.unwrap()).collect();
        assert_eq!(record_batches.len(), 2);
        assert_eq!(schema, record_batches[0].schema());
        assert_eq!(record_batches[0].schema(), record_batches[1].schema());

        // Test number of rows
        assert_eq!(record_batches[0].num_rows(), 1);
        assert_eq!(record_batches[1].num_rows(), 1);
    }

    #[tokio::test]
    async fn from_arrow_to_polars() {
        let record_batch_reader = get_record_batch_reader_from_polars();
        let schema = record_batch_reader.schema();
        let stream: SendableRecordBatchStream = Box::pin(SimpleRecordBatchStream {
            schema: schema.clone(),
            stream: futures::stream::iter(
                record_batch_reader
                    .into_iter()
                    .map(|r| r.map_err(Into::into)),
            ),
        });
        let df = stream.into_polars().await.unwrap();

        // Test number of chunks and rows
        assert_eq!(df.n_chunks(), 2);
        assert_eq!(df.height(), 2);

        // Test schema conversion
        assert_eq!(
            df.schema()
                .into_iter()
                .map(|(name, datatype)| (name.to_string(), datatype))
                .collect::<Vec<_>>(),
            vec![
                ("string".to_string(), polars::prelude::DataType::String),
                ("int".to_owned(), polars::prelude::DataType::Int32),
                ("float".to_owned(), polars::prelude::DataType::Float64)
            ]
        );
    }
}
