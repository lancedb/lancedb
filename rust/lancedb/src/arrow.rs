// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{pin::Pin, sync::Arc};

pub use arrow_schema;
use datafusion_common::DataFusionError;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{Stream, StreamExt, TryStreamExt};

#[cfg(feature = "polars")]
use {crate::polars_arrow_convertors, polars::frame::ArrowChunk, polars::prelude::DataFrame};

use crate::{error::Result, Error};

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
/// Integrations should implement this trait to allow data to be
/// imported directly from the integration.  For example, implementing
/// this trait for `Vec<Vec<...>>` would allow the `Vec` to be directly
/// used in methods like [`crate::connection::Connection::create_table`]
/// or [`crate::table::Table::add`]
pub trait IntoArrow {
    /// Convert the data into an iterator of Arrow batches
    fn into_arrow(self) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>>;
}

pub type BoxedRecordBatchReader = Box<dyn arrow_array::RecordBatchReader + Send>;

impl<T: arrow_array::RecordBatchReader + Send + 'static> IntoArrow for T {
    fn into_arrow(self) -> Result<Box<dyn arrow_array::RecordBatchReader + Send>> {
        Ok(Box::new(self))
    }
}

/// A trait for converting incoming data to Arrow asynchronously
///
/// Serves the same purpose as [`IntoArrow`], but for asynchronous data.
///
/// Note: Arrow has no async equivalent to RecordBatchReader and so
pub trait IntoArrowStream {
    /// Convert the data into a stream of Arrow batches
    fn into_arrow(self) -> Result<SendableRecordBatchStream>;
}

impl<S: Stream<Item = Result<arrow_array::RecordBatch>>> SimpleRecordBatchStream<S> {
    pub fn new(stream: S, schema: Arc<arrow_schema::Schema>) -> Self {
        Self { schema, stream }
    }
}

impl IntoArrowStream for SendableRecordBatchStream {
    fn into_arrow(self) -> Result<SendableRecordBatchStream> {
        Ok(self)
    }
}

impl IntoArrowStream for datafusion_physical_plan::SendableRecordBatchStream {
    fn into_arrow(self) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let stream = self.map_err(|df_err| Error::Runtime {
            message: df_err.to_string(),
        });
        Ok(Box::pin(SimpleRecordBatchStream::new(stream, schema)))
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
    /// the costly operation of reallocating each series as a single contigous chunk.
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

#[cfg(all(test, feature = "polars"))]
mod tests {
    use super::SendableRecordBatchStream;
    use crate::arrow::{
        IntoArrow, IntoPolars, PolarsDataFrameRecordBatchReader, SimpleRecordBatchStream,
    };
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

        PolarsDataFrameRecordBatchReader::new(df1.vstack(&df2).unwrap())
            .unwrap()
            .into_arrow()
            .unwrap()
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
