// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Data source abstraction for LanceDB.
//!
//! This module provides a [`SourceData`] trait that allows input data sources to express
//! capabilities (row count, rescannability) so the insert pipeline can make
//! better decisions about write parallelism and retry strategies.
//!
//! # Example
//!
//! ```
//! use lancedb::data_source::SourceData;
//! use arrow_array::RecordBatch;
//!
//! # use lancedb::Table;
//! # async fn example(table: &Table, batch: RecordBatch) -> Result<(), Box<dyn std::error::Error>> {
//! // RecordBatch implements SourceData with exact row count
//! assert_eq!(batch.num_rows(), SourceData::num_rows(&batch).unwrap());
//!
//! // Use with add_from_source()
//! table.add_from_source(Box::new(batch)).execute().await?;
//! # Ok(())
//! # }
//! ```

use std::sync::Arc;

use arrow_array::{RecordBatch, RecordBatchReader};
use arrow_schema::SchemaRef;
use datafusion_common::exec_datafusion_err;
use datafusion_execution::SendableRecordBatchStream;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream::once;

use crate::Result;

/// A data source that can provide data to LanceDB.
///
/// This trait allows different data types to be used as input for insert operations,
/// while providing optional metadata hints for better performance.
///
/// # Implementing SourceData
///
/// The trait has default implementations for common types:
/// - [`RecordBatch`] - exact row count, rescannable
/// - [`Vec<RecordBatch>`] - exact row count (sum), rescannable
/// - [`Box<dyn RecordBatchReader + Send>`] - no row count, not rescannable
///
/// # Example
///
/// ```
/// use lancedb::data_source::SourceData;
/// use arrow_array::{RecordBatch, Int32Array};
/// use arrow_schema::{Schema, Field, DataType};
/// use std::sync::Arc;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
/// ]));
/// let batch = RecordBatch::try_new(
///     schema,
///     vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
/// ).unwrap();
///
/// // RecordBatch provides exact row count
/// assert_eq!(batch.num_rows(), 3);
/// assert_eq!(SourceData::num_rows(&batch), Some(3));
/// assert!(SourceData::rescannable(&batch));
/// ```
pub trait SourceData: Send {
    /// Returns the schema of the data.
    fn schema(&self) -> SchemaRef;

    /// Read data as a stream of record batches.
    ///
    /// This consumes the data source. The returned stream produces batches
    /// matching the schema from [`Self::schema()`].
    fn read(self: Box<Self>) -> Result<SendableRecordBatchStream>;

    /// Optional hint about the number of rows.
    ///
    /// When available, this allows the pipeline to estimate total data size
    /// and choose appropriate partitioning.
    fn num_rows(&self) -> Option<usize> {
        None
    }

    /// Whether the source can be re-read from the beginning.
    ///
    /// `true` for in-memory data (Tables, DataFrames) and disk-based sources (Datasets).
    /// `false` for streaming sources (DuckDB results, network streams).
    ///
    /// When true, the pipeline can retry failed writes by rescanning.
    fn rescannable(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for dyn SourceData {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceData")
            .field("schema", &self.schema())
            .field("num_rows", &self.num_rows())
            .field("rescannable", &self.rescannable())
            .finish()
    }
}

// Implementation for RecordBatch - exact count, rescannable
impl SourceData for RecordBatch {
    fn schema(&self) -> SchemaRef {
        RecordBatch::schema(self)
    }

    fn read(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            schema,
            once(async move { Ok(*self) }),
        )))
    }

    fn num_rows(&self) -> Option<usize> {
        Some(RecordBatch::num_rows(self))
    }

    fn rescannable(&self) -> bool {
        true
    }
}

// Implementation for Vec<RecordBatch> - sum of counts, rescannable
impl SourceData for Vec<RecordBatch> {
    fn schema(&self) -> SchemaRef {
        if self.is_empty() {
            Arc::new(arrow_schema::Schema::empty())
        } else {
            self[0].schema()
        }
    }

    fn read(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        let schema = SourceData::schema(self.as_ref());
        let batches = *self;
        let stream = futures::stream::iter(batches.into_iter().map(Ok));
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn num_rows(&self) -> Option<usize> {
        Some(self.iter().map(|b| b.num_rows()).sum())
    }

    fn rescannable(&self) -> bool {
        true
    }
}

// Implementation for Box<dyn RecordBatchReader + Send> - no count, not rescannable
impl SourceData for Box<dyn RecordBatchReader + Send> {
    fn schema(&self) -> SchemaRef {
        RecordBatchReader::schema(self.as_ref())
    }

    fn read(self: Box<Self>) -> Result<SendableRecordBatchStream> {
        let inner: Box<dyn RecordBatchReader + Send> = *self;
        let schema = RecordBatchReader::schema(inner.as_ref());
        // Create a lazy stream that pulls batches on demand
        let stream = futures::stream::unfold(inner, |mut reader| async move {
            match reader.next() {
                Some(Ok(batch)) => Some((Ok(batch), reader)),
                Some(Err(e)) => Some((Err(exec_datafusion_err!("Arrow error: {}", e)), reader)),
                None => None,
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn num_rows(&self) -> Option<usize> {
        None
    }

    fn rescannable(&self) -> bool {
        false
    }
}

/// A wrapper that adds a row count hint to a RecordBatchReader.
///
/// This allows callers to provide row count hints for better partitioning
/// when the underlying reader doesn't provide this information natively.
///
/// Note: Streams cannot be rescanned, so this always returns `false` for
/// [`SourceData::rescannable()`].
///
/// # Example
///
/// ```
/// use lancedb::data_source::{SourceData, ReaderWithMetadata};
/// use arrow_array::{RecordBatch, RecordBatchIterator};
/// use arrow_schema::{Schema, Field, DataType};
/// use std::sync::Arc;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
/// ]));
/// let reader = RecordBatchIterator::new(vec![].into_iter(), schema.clone());
///
/// // Wrap reader with known row count for better partitioning
/// let source = ReaderWithMetadata::new(Box::new(reader), schema)
///     .with_num_rows(10000);
///
/// assert_eq!(source.num_rows(), Some(10000));
/// assert!(!source.rescannable()); // Streams cannot be rescanned
/// ```
pub struct ReaderWithMetadata {
    inner: Option<Box<dyn RecordBatchReader + Send>>,
    schema: SchemaRef,
    num_rows: Option<usize>,
}

impl std::fmt::Debug for ReaderWithMetadata {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReaderWithMetadata")
            .field("schema", &self.schema)
            .field("num_rows", &self.num_rows)
            .field("inner", &self.inner.is_some())
            .finish()
    }
}

impl ReaderWithMetadata {
    /// Create a new wrapper from a RecordBatchReader.
    ///
    /// The schema must be provided separately since we need it before consuming the reader.
    pub fn new(reader: Box<dyn RecordBatchReader + Send>, schema: SchemaRef) -> Self {
        Self {
            inner: Some(reader),
            schema,
            num_rows: None,
        }
    }

    /// Set a row count hint for this source.
    pub fn with_num_rows(mut self, num_rows: usize) -> Self {
        self.num_rows = Some(num_rows);
        self
    }
}

impl SourceData for ReaderWithMetadata {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn read(mut self: Box<Self>) -> Result<SendableRecordBatchStream> {
        let reader = self.inner.take().ok_or_else(|| crate::Error::Runtime {
            message: "ReaderWithMetadata already consumed".to_string(),
        })?;
        let schema = self.schema.clone();
        // Create a lazy stream that pulls batches on demand
        let stream = futures::stream::unfold(reader, |mut reader| async move {
            match reader.next() {
                Some(Ok(batch)) => Some((Ok(batch), reader)),
                Some(Err(e)) => Some((Err(exec_datafusion_err!("Arrow error: {}", e)), reader)),
                None => None,
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn num_rows(&self) -> Option<usize> {
        self.num_rows
    }

    fn rescannable(&self) -> bool {
        false // Streams cannot be rescanned
    }
}

// --- ExecutionPlan adapter for SourceData ---

use std::any::Any;
use std::sync::Mutex;

use datafusion::physical_expr::EquivalenceProperties;
use datafusion_common::stats::Precision;
use datafusion_common::{DataFusionError, Result as DFResult, Statistics};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion_physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};

/// An ExecutionPlan that wraps a SourceData, allowing it to be used in DataFusion pipelines.
///
/// This plan has a single partition and streams data lazily without collecting into memory.
pub struct SourceDataExec {
    /// The wrapped source data. Option allows taking ownership in execute().
    source: Mutex<Option<Box<dyn SourceData>>>,
    schema: SchemaRef,
    properties: PlanProperties,
    num_rows: Option<usize>,
}

impl std::fmt::Debug for SourceDataExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SourceDataExec")
            .field("schema", &self.schema)
            .field("num_rows", &self.num_rows)
            .finish()
    }
}

impl SourceDataExec {
    /// Create a new SourceDataExec from a SourceData.
    pub fn new(source: Box<dyn SourceData>) -> Self {
        let schema = source.schema();
        let num_rows = source.num_rows();

        // Single partition, incremental emission (streaming), bounded
        let properties = PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );

        Self {
            source: Mutex::new(Some(source)),
            schema,
            properties,
            num_rows,
        }
    }
}

impl DisplayAs for SourceDataExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SourceDataExec: rows={:?}",
            self.num_rows.map(|n| n.to_string()).unwrap_or("?".into())
        )
    }
}

impl ExecutionPlan for SourceDataExec {
    fn name(&self) -> &str {
        "SourceDataExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(DataFusionError::Internal(
                "SourceDataExec does not have children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        if partition != 0 {
            return Err(DataFusionError::Internal(format!(
                "SourceDataExec only supports partition 0, got {}",
                partition
            )));
        }

        // Take the source out of the mutex (can only be executed once)
        let source = self.source.lock().unwrap().take().ok_or_else(|| {
            DataFusionError::Internal("SourceDataExec can only be executed once".to_string())
        })?;

        source
            .read()
            .map_err(|e| DataFusionError::External(Box::new(e)))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        Ok(Statistics {
            num_rows: self
                .num_rows
                .map(Precision::Exact)
                .unwrap_or(Precision::Absent),
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use futures::StreamExt;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn test_batch(values: Vec<i32>, schema: SchemaRef) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[test]
    fn test_record_batch_source_data() {
        let schema = test_schema();
        let batch = test_batch(vec![1, 2, 3], schema.clone());

        assert_eq!(SourceData::schema(&batch), schema);
        assert_eq!(batch.num_rows(), 3);
        assert!(batch.rescannable());
    }

    #[test]
    fn test_vec_record_batch_source_data() {
        let schema = test_schema();
        let batch1 = test_batch(vec![1, 2, 3], schema.clone());
        let batch2 = test_batch(vec![4, 5], schema.clone());
        let batches = vec![batch1, batch2];

        assert_eq!(SourceData::schema(&batches), schema);
        assert_eq!(batches.num_rows(), Some(5));
        assert!(batches.rescannable());
    }

    #[test]
    fn test_vec_empty_source_data() {
        let batches: Vec<RecordBatch> = vec![];

        assert_eq!(SourceData::schema(&batches).fields().len(), 0);
        assert_eq!(batches.num_rows(), Some(0));
    }

    #[test]
    fn test_record_batch_reader_source_data() {
        let schema = test_schema();
        let batch = test_batch(vec![1, 2, 3], schema.clone());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let boxed: Box<dyn RecordBatchReader + Send> = Box::new(reader);

        assert_eq!(SourceData::schema(&boxed), schema);
        assert_eq!(boxed.num_rows(), None);
        assert!(!boxed.rescannable());
    }

    #[test]
    fn test_reader_with_metadata() {
        let schema = test_schema();
        let batch = test_batch(vec![1, 2, 3], schema.clone());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let source = ReaderWithMetadata::new(Box::new(reader), schema.clone()).with_num_rows(100);

        assert_eq!(SourceData::schema(&source), schema);
        assert_eq!(source.num_rows(), Some(100));
        assert!(!source.rescannable()); // Streams cannot be rescanned
    }

    #[tokio::test]
    async fn test_record_batch_read() {
        let schema = test_schema();
        let batch = test_batch(vec![1, 2, 3], schema.clone());

        let boxed: Box<dyn SourceData> = Box::new(batch);
        let mut stream = boxed.read().unwrap();

        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema(), schema);
    }

    #[tokio::test]
    async fn test_vec_record_batch_read() {
        let schema = test_schema();
        let batch1 = test_batch(vec![1, 2, 3], schema.clone());
        let batch2 = test_batch(vec![4, 5], schema.clone());

        let boxed: Box<dyn SourceData> = Box::new(vec![batch1, batch2]);
        let mut stream = boxed.read().unwrap();

        let result1 = stream.next().await.unwrap().unwrap();
        assert_eq!(result1.num_rows(), 3);

        let result2 = stream.next().await.unwrap().unwrap();
        assert_eq!(result2.num_rows(), 2);

        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_reader_with_metadata_read() {
        let schema = test_schema();
        let batch = test_batch(vec![1, 2, 3], schema.clone());
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone());

        let source = ReaderWithMetadata::new(Box::new(reader), schema.clone()).with_num_rows(100);

        let boxed: Box<dyn SourceData> = Box::new(source);
        let mut stream = boxed.read().unwrap();

        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema(), schema);
    }
}
