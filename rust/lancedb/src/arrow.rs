// Copyright 2023 Lance Developers.
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

use std::{pin::Pin, sync::Arc};

pub use arrow_array;
pub use arrow_schema;
use futures::{Stream, StreamExt};
use polars::frame::ArrowChunk;

#[cfg(feature = "polars")]
use {
    futures::TryStreamExt,
    polars::datatypes,
    polars::prelude::{DataFrame, Field, Schema, Series},
    polars_arrow::array,
};

use crate::error::Result;

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
/// Integrations should implement this trait to allow data to be
/// imported directly from the integration.  For example, implementing
/// this trait for `Vec<Vec<...>>` would allow the `Vec` to be directly
/// used in methods like [`crate::connection::Connection::create_table`]
/// or [`crate::table::Table::add`]
pub trait IntoArrow {
    /// Convert the data into an Arrow array
    fn into_arrow(self) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>>;
}

impl<T: arrow_array::RecordBatchReader + Send + 'static> IntoArrow for T {
    fn into_arrow(self) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>> {
        Ok(Box::new(self))
    }
}

#[cfg(feature = "polars")]
/// An iterator of record batches formed from a Polars DataFrame. The iterator
/// panics if the DataFrame's chunks are not aligned. Consider calling
/// [`polars::prelude::DataFrame::should_rechunk`] to determine whether the DataFrame
/// should be rechunked and calling [`polars::prelude::DataFrame::align_chunks`]
/// if the DataFrame needs to be rechunked before using the iterator.
pub struct PolarsDataFrameRecordBatchReader {
    chunks: Vec<ArrowChunk>,
    arrow_schema: Arc<arrow_schema::Schema>,
    index: usize,
}

#[cfg(feature = "polars")]
impl PolarsDataFrameRecordBatchReader {
    /// Creates a new `PolarsDataFrameRecordBatchReader` from a given Polars DataFrame.
    pub fn new(df: DataFrame) -> Self {
        let fields: Vec<arrow_schema::Field> = df
            .schema()
            .into_iter()
            .map(|(name, dtype)| {
                arrow_schema::Field::new(
                    name,
                    arrow_schema::DataType::from(dtype.to_arrow(false)),
                    true,
                )
            })
            .collect();
        // Use pl_flavor = false to use LargeBinary and LargeUtf8 Arrow types instead of
        // BinaryView and Utf8View types because polars-arrow to arrow-rs conversion
        // is not yet implemented for BinaryView and Utf8View.
        // See: https://lists.apache.org/thread/w88tpz76ox8h3rxkjl4so6rg3f1rv7wt for the
        // differences in the types.
        PolarsDataFrameRecordBatchReader {
            chunks: df.iter_chunks(false).collect(),
            arrow_schema: Arc::new(arrow_schema::Schema::new(fields)),
            index: 0,
        }
    }
}

#[cfg(feature = "polars")]
impl Iterator for PolarsDataFrameRecordBatchReader {
    type Item = std::result::Result<arrow_array::RecordBatch, arrow_schema::ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index < self.chunks.len() {
            let columns: Vec<arrow_array::ArrayRef> = self.chunks[self.index]
                .arrays()
                .iter()
                .map(|polars_array| arrow_array::ArrayRef::from(&**polars_array))
                .collect();
            self.index += 1;
            Some(arrow_array::RecordBatch::try_new(
                self.arrow_schema.clone(),
                columns,
            ))
        } else {
            None
        }
    }
}

#[cfg(feature = "polars")]
impl arrow_array::RecordBatchReader for PolarsDataFrameRecordBatchReader {
    fn schema(&self) -> Arc<arrow_schema::Schema> {
        self.arrow_schema.clone()
    }

    fn next_batch(
        &mut self,
    ) -> std::prelude::v1::Result<Option<arrow_array::RecordBatch>, arrow_schema::ArrowError> {
        self.next().transpose()
    }
}

/// A trait for converting the result of a LanceDB query into a Polars DataFrame with aligned
/// chunks. The resulting Polars DataFrame will have aligned chunks, but the series's
/// chunks are not guaranteed to be contiguous.
#[cfg(feature = "polars")]
pub trait ToPolars {
    fn to_polars(self) -> impl std::future::Future<Output = Result<DataFrame>> + Send;
}

#[cfg(feature = "polars")]
impl ToPolars for SendableRecordBatchStream {
    async fn to_polars(self) -> Result<DataFrame> {
        let arrow_schema = self.schema();
        let polars_schema = convert_arrow_schema_to_polars_schema(&arrow_schema);
        let mut acc_df: DataFrame = DataFrame::from(&polars_schema);
        let record_batches = self.try_collect::<Vec<_>>().await?;
        for record_batch in record_batches {
            let new_df = convert_record_batch_to_polars_df(&record_batch, &polars_schema)?;
            acc_df = acc_df.vstack(&new_df)?;
        }
        return Ok(acc_df);
    }
}

#[cfg(feature = "polars")]
fn convert_arrow_schema_to_polars_schema(arrow_schema: &arrow_schema::Schema) -> Schema {
    Schema::from_iter(arrow_schema.fields().iter().map(|field| {
        Field::new(
            field.name(),
            datatypes::DataType::from(&datatypes::ArrowDataType::from(field.data_type().clone())),
        )
    }))
}

#[cfg(feature = "polars")]
fn convert_record_batch_to_polars_df(
    record_batch: &arrow::record_batch::RecordBatch,
    polars_schema: &Schema,
) -> Result<DataFrame> {
    let mut columns: Vec<Series> = Vec::with_capacity(record_batch.num_columns());

    for (i, column) in record_batch.columns().iter().enumerate() {
        let polars_array = Box::<dyn array::Array>::from(&**column);
        columns.push(Series::from_arrow(
            polars_schema.try_get_at_index(i)?.0,
            polars_array,
        )?);
    }

    Ok(DataFrame::from_iter(columns))
}

#[cfg(all(test, feature = "polars"))]
mod tests {
    use super::SendableRecordBatchStream;
    use crate::arrow::{
        IntoArrow, PolarsDataFrameRecordBatchReader, SimpleRecordBatchStream, ToPolars,
    };
    use polars::df;

    fn get_record_batch_reader_from_polars() -> Box<dyn arrow_array::RecordBatchReader + Send> {
        let df1 = df!("string" => &["ab"],
             "int" => &[1],
             "float" => &[1.0])
        .unwrap();
        let df2 = df!("string" => &["bc"],
             "int" => &[2],
             "float" => &[2.0])
        .unwrap();

        PolarsDataFrameRecordBatchReader::new(df1.vstack(&df2).unwrap())
            .into_arrow()
            .unwrap()
    }

    #[test]
    fn from_polars_to_arrow_non_empty() {
        let record_batch_reader = get_record_batch_reader_from_polars();
        let schema = record_batch_reader.schema();

        // Test schema conversion
        assert_eq!(
            schema
                .fields
                .iter()
                .map(|field| ((field.name().as_str(), field.data_type())))
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
    async fn from_arrow_to_polars_non_empty() {
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
        let df = stream.to_polars().await.unwrap();

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
