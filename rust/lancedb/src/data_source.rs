// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Data source abstraction for LanceDB.
//!
//! This module provides a [`DataSource`] trait that allows input data sources to express
//! capabilities (row count, parallelism, rescannability) so the insert pipeline can make
//! better decisions about write parallelism and retry strategies.
//!
//! # Example
//!
//! ```
//! use lancedb::data_source::{DataSource, DataSourceMetadata, RowCountHint};
//! use arrow_schema::SchemaRef;
//!
//! # use lancedb::Table;
//! # async fn example(table: &Table, source: Box<dyn DataSource>) -> Result<(), Box<dyn std::error::Error>> {
//! // Query metadata to make informed decisions
//! let metadata = source.metadata();
//! if let Some(hint) = &metadata.row_count {
//!     println!("Row count: ~{}", hint.value());
//! }
//! println!("Rescannable: {}", metadata.rescannable);
//! println!("Num streams: {}", metadata.num_streams);
//!
//! // Use with add_from_source()
//! table.add_from_source(source).execute().await?;
//! # Ok(())
//! # }
//! ```

use std::fmt;
use std::sync::Arc;

use arrow_array::RecordBatchReader;
use arrow_schema::SchemaRef;
use datafusion_physical_plan::ExecutionPlan;

use crate::arrow::IntoArrow;
use crate::Result;

/// Confidence level for row count estimates.
///
/// Different data sources can provide row counts with varying levels of confidence.
/// This enum allows sources to express how reliable their row count is, which helps
/// the insert pipeline make better decisions about partitioning.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RowCountHint {
    /// Exact row count, typically from in-memory data like PyArrow Table or DataFrame.
    ///
    /// Example: `pa.Table.num_rows`, `df.len()`
    Exact(usize),

    /// Computed row count from metadata (cheap but may involve I/O).
    ///
    /// Example: `pa.dataset.Dataset.count_rows()` - reads metadata files but not data.
    Computed(usize),

    /// User-provided hint, may be approximate.
    ///
    /// Useful when the user has domain knowledge about the expected size.
    UserHint(usize),

    /// Upper bound on the number of rows.
    ///
    /// Useful when exact count is expensive but maximum is known.
    UpperBound(usize),
}

impl RowCountHint {
    /// Get the numeric value of the hint.
    pub fn value(&self) -> usize {
        match *self {
            Self::Exact(n) | Self::Computed(n) | Self::UserHint(n) | Self::UpperBound(n) => n,
        }
    }

    /// Returns true if this is an exact count.
    pub fn is_exact(&self) -> bool {
        matches!(self, Self::Exact(_))
    }

    /// Returns true if this hint is reliable enough for partition planning.
    ///
    /// Exact and Computed counts are reliable. UserHint and UpperBound are less so.
    pub fn is_reliable(&self) -> bool {
        matches!(self, Self::Exact(_) | Self::Computed(_))
    }
}

/// Metadata about a data source's capabilities.
///
/// This metadata helps the insert pipeline make better decisions about:
/// - **Write parallelism**: How many output files to create
/// - **Parallel ingestion**: Whether the source can provide multiple streams
/// - **Retry strategy**: Whether the source can be rescanned on failure
#[derive(Debug, Clone, Default)]
pub struct DataSourceMetadata {
    /// Hint about the number of rows in the source.
    ///
    /// When available, this allows the pipeline to estimate total data size
    /// and choose appropriate partitioning.
    pub row_count: Option<RowCountHint>,

    /// Whether the source can be rescanned from the beginning.
    ///
    /// `true` for in-memory data (Tables, DataFrames) and disk-based sources (Datasets).
    /// `false` for streaming sources (DuckDB results, network streams).
    ///
    /// When true, the pipeline can retry failed writes by rescanning.
    pub rescannable: bool,

    /// Number of parallel streams the source can provide.
    ///
    /// Sources like PyArrow Datasets can provide one stream per fragment,
    /// enabling parallel ingestion. In-memory sources typically provide 1 stream.
    ///
    /// Default: 1
    pub num_streams: usize,
}

impl DataSourceMetadata {
    /// Create metadata for a simple single-stream, non-rescannable source.
    pub fn streaming() -> Self {
        Self {
            row_count: None,
            rescannable: false,
            num_streams: 1,
        }
    }

    /// Create metadata for an in-memory source with exact row count.
    pub fn in_memory(row_count: usize) -> Self {
        Self {
            row_count: Some(RowCountHint::Exact(row_count)),
            rescannable: true,
            num_streams: 1,
        }
    }

    /// Create metadata for a multi-stream source (e.g., Dataset with fragments).
    pub fn multi_stream(num_streams: usize) -> Self {
        Self {
            row_count: None,
            rescannable: true,
            num_streams,
        }
    }

    /// Set the row count hint.
    pub fn with_row_count(mut self, hint: RowCountHint) -> Self {
        self.row_count = Some(hint);
        self
    }

    /// Set whether the source is rescannable.
    pub fn with_rescannable(mut self, rescannable: bool) -> Self {
        self.rescannable = rescannable;
        self
    }

    /// Set the number of streams.
    pub fn with_num_streams(mut self, num_streams: usize) -> Self {
        self.num_streams = num_streams;
        self
    }
}

/// A data source that can provide data to LanceDB with metadata about its capabilities.
///
/// This trait extends beyond [`IntoArrow`] by providing:
/// - Schema information upfront
/// - Metadata about row counts, parallelism, and rescannability
/// - Conversion to DataFusion execution plans for parallel processing
///
/// # Implementing DataSource
///
/// For simple sources, use [`IntoArrowAdapter`] to wrap an existing [`IntoArrow`] implementation.
/// For sources that can provide multiple parallel streams, use [`MultiStreamDataSource`].
///
/// # Example
///
/// ```
/// use lancedb::data_source::{DataSource, DataSourceMetadata, RowCountHint, IntoArrowAdapter};
/// use arrow_array::{RecordBatch, RecordBatchIterator};
/// use arrow_schema::{Schema, Field, DataType};
/// use std::sync::Arc;
///
/// // Wrap an existing RecordBatchIterator with metadata
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
/// ]));
/// let batches: Vec<RecordBatch> = vec![]; // Your data here
/// let reader = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());
///
/// let source = IntoArrowAdapter::new(Box::new(reader), schema)
///     .with_row_count(RowCountHint::Exact(1000));
/// ```
pub trait DataSource: Send + fmt::Debug {
    /// Returns the schema of the data.
    fn schema(&self) -> SchemaRef;

    /// Returns metadata about this data source.
    ///
    /// The default implementation returns minimal metadata (single stream, not rescannable).
    fn metadata(&self) -> DataSourceMetadata {
        DataSourceMetadata::default()
    }

    /// Convert this data source into a DataFusion execution plan.
    ///
    /// The returned plan should produce batches matching the schema from [`Self::schema()`].
    /// For multi-stream sources, the plan may have multiple partitions.
    fn into_execution_plan(self: Box<Self>) -> Result<Arc<dyn ExecutionPlan>>;
}

/// Wraps any [`IntoArrow`] as a [`DataSource`] with optional metadata hints.
///
/// This adapter provides backward compatibility with existing code that uses `IntoArrow`,
/// while allowing callers to optionally provide metadata hints for better performance.
///
/// # Example
///
/// ```
/// use lancedb::data_source::{IntoArrowAdapter, RowCountHint};
/// use arrow_array::{RecordBatch, RecordBatchIterator};
/// use arrow_schema::{Schema, Field, DataType};
/// use std::sync::Arc;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
/// ]));
/// let reader = RecordBatchIterator::new(vec![].into_iter(), schema.clone());
///
/// // Basic usage
/// let source = IntoArrowAdapter::new(Box::new(reader), schema.clone());
///
/// // With row count hint for better partitioning
/// let reader2 = RecordBatchIterator::new(vec![].into_iter(), schema.clone());
/// let source_with_hint = IntoArrowAdapter::new(Box::new(reader2), schema)
///     .with_row_count(RowCountHint::Exact(10000));
/// ```
pub struct IntoArrowAdapter {
    inner: Option<Box<dyn RecordBatchReader + Send>>,
    schema: SchemaRef,
    metadata: DataSourceMetadata,
}

impl fmt::Debug for IntoArrowAdapter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("IntoArrowAdapter")
            .field("schema", &self.schema)
            .field("metadata", &self.metadata)
            .field("inner", &self.inner.is_some())
            .finish()
    }
}

impl IntoArrowAdapter {
    /// Create a new adapter from an IntoArrow source.
    ///
    /// The schema must be provided separately since we need it before consuming the reader.
    pub fn new(data: Box<dyn RecordBatchReader + Send>, schema: SchemaRef) -> Self {
        Self {
            inner: Some(data),
            schema,
            metadata: DataSourceMetadata::default(),
        }
    }

    /// Create a new adapter from any IntoArrow implementation.
    ///
    /// This consumes the data to get the reader and schema.
    pub fn from_into_arrow<T: IntoArrow>(data: T) -> Result<Self> {
        let reader = data.into_arrow()?;
        let schema = reader.schema();
        Ok(Self::new(reader, schema))
    }

    /// Set a row count hint for this source.
    pub fn with_row_count(mut self, hint: RowCountHint) -> Self {
        self.metadata.row_count = Some(hint);
        self
    }

    /// Mark this source as rescannable.
    pub fn with_rescannable(mut self, rescannable: bool) -> Self {
        self.metadata.rescannable = rescannable;
        self
    }

    /// Set the full metadata for this source.
    pub fn with_metadata(mut self, metadata: DataSourceMetadata) -> Self {
        self.metadata = metadata;
        self
    }
}

impl DataSource for IntoArrowAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn metadata(&self) -> DataSourceMetadata {
        self.metadata.clone()
    }

    fn into_execution_plan(mut self: Box<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let reader = self.inner.take().ok_or_else(|| crate::Error::Runtime {
            message: "IntoArrowAdapter already consumed".to_string(),
        })?;
        Ok(crate::table::datafusion::insert::reader_to_execution_plan(
            reader,
        ))
    }
}

/// A factory that creates a RecordBatchReader for a single stream.
///
/// This is used by [`MultiStreamDataSource`] to defer reader creation until execution time.
pub type StreamFactory =
    Box<dyn FnOnce() -> Result<Box<dyn RecordBatchReader + Send>> + Send + Sync>;

/// A data source that provides multiple parallel streams.
///
/// This is useful for sources like PyArrow Datasets that can provide one reader per fragment,
/// enabling parallel ingestion. Each stream factory is called at execution time to create
/// a reader for that partition.
///
/// # Example
///
/// ```
/// use lancedb::data_source::{MultiStreamDataSource, DataSourceMetadata, RowCountHint};
/// use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
/// use arrow_schema::{Schema, Field, DataType};
/// use std::sync::Arc;
///
/// let schema = Arc::new(Schema::new(vec![
///     Field::new("id", DataType::Int32, false),
/// ]));
///
/// // Create a multi-stream source with 3 partitions
/// let mut source = MultiStreamDataSource::new(schema.clone());
///
/// for i in 0..3 {
///     let schema_clone = schema.clone();
///     source = source.add_stream(Box::new(move || {
///         let batch = RecordBatch::try_new(
///             schema_clone.clone(),
///             vec![Arc::new(Int32Array::from(vec![i * 100, i * 100 + 1]))],
///         ).unwrap();
///         let reader = RecordBatchIterator::new(
///             vec![Ok(batch)],
///             schema_clone,
///         );
///         Ok(Box::new(reader) as Box<dyn arrow_array::RecordBatchReader + Send>)
///     }));
/// }
///
/// // Optionally add row count hint
/// let source = source.with_row_count(RowCountHint::Computed(6));
/// ```
pub struct MultiStreamDataSource {
    schema: SchemaRef,
    stream_factories: Vec<StreamFactory>,
    metadata: DataSourceMetadata,
}

impl fmt::Debug for MultiStreamDataSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MultiStreamDataSource")
            .field("schema", &self.schema)
            .field("num_streams", &self.stream_factories.len())
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl MultiStreamDataSource {
    /// Create a new multi-stream data source with the given schema.
    pub fn new(schema: SchemaRef) -> Self {
        Self {
            schema,
            stream_factories: Vec::new(),
            metadata: DataSourceMetadata {
                row_count: None,
                rescannable: true,
                num_streams: 0,
            },
        }
    }

    /// Add a stream factory that will create a reader for one partition.
    ///
    /// The factory is called at execution time to create the reader.
    pub fn add_stream(mut self, factory: StreamFactory) -> Self {
        self.stream_factories.push(factory);
        self.metadata.num_streams = self.stream_factories.len();
        self
    }

    /// Set a row count hint.
    pub fn with_row_count(mut self, hint: RowCountHint) -> Self {
        self.metadata.row_count = Some(hint);
        self
    }

    /// Set whether this source is rescannable.
    pub fn with_rescannable(mut self, rescannable: bool) -> Self {
        self.metadata.rescannable = rescannable;
        self
    }

    /// Set the full metadata for this source.
    pub fn with_metadata(mut self, metadata: DataSourceMetadata) -> Self {
        // Preserve num_streams from the actual stream count
        let num_streams = self.stream_factories.len();
        self.metadata = metadata.with_num_streams(num_streams);
        self
    }
}

impl DataSource for MultiStreamDataSource {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn metadata(&self) -> DataSourceMetadata {
        self.metadata.clone()
    }

    fn into_execution_plan(self: Box<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        use parking_lot::RwLock;
        use std::sync::Mutex;

        use datafusion_physical_plan::memory::{LazyBatchGenerator, LazyMemoryExec};

        // Create a generator for each stream factory
        let generators: Vec<Arc<RwLock<dyn LazyBatchGenerator>>> = self
            .stream_factories
            .into_iter()
            .map(|factory| {
                let generator = StreamFactoryGenerator {
                    factory: Mutex::new(Some(factory)),
                    reader: Mutex::new(None),
                    schema: self.schema.clone(),
                };
                Arc::new(RwLock::new(generator)) as Arc<RwLock<dyn LazyBatchGenerator>>
            })
            .collect();

        if generators.is_empty() {
            return Err(crate::Error::InvalidInput {
                message: "MultiStreamDataSource has no streams".to_string(),
            });
        }

        Ok(Arc::new(
            LazyMemoryExec::try_new(self.schema.clone(), generators).map_err(|e| {
                crate::Error::Runtime {
                    message: format!("Failed to create execution plan: {}", e),
                }
            })?,
        ))
    }
}

/// A lazy batch generator that creates a reader from a factory on first use.
struct StreamFactoryGenerator {
    factory: std::sync::Mutex<Option<StreamFactory>>,
    reader: std::sync::Mutex<Option<Box<dyn RecordBatchReader + Send>>>,
    schema: SchemaRef,
}

impl fmt::Debug for StreamFactoryGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StreamFactoryGenerator")
            .field("schema", &self.schema)
            .finish()
    }
}

impl fmt::Display for StreamFactoryGenerator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "StreamFactoryGenerator")
    }
}

impl datafusion_physical_plan::memory::LazyBatchGenerator for StreamFactoryGenerator {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn generate_next_batch(
        &mut self,
    ) -> datafusion_common::Result<Option<arrow_array::RecordBatch>> {
        // Initialize reader from factory if needed
        let mut reader_guard = self.reader.lock().unwrap();
        if reader_guard.is_none() {
            let mut factory_guard = self.factory.lock().unwrap();
            if let Some(factory) = factory_guard.take() {
                let reader = factory()
                    .map_err(|e| datafusion_common::DataFusionError::External(Box::new(e)))?;
                *reader_guard = Some(reader);
            } else {
                return Ok(None);
            }
        }

        // Get next batch from reader
        if let Some(reader) = reader_guard.as_mut() {
            match reader.next() {
                Some(Ok(batch)) => Ok(Some(batch)),
                Some(Err(e)) => Err(datafusion_common::DataFusionError::ArrowError(
                    Box::new(e),
                    None,
                )),
                None => Ok(None),
            }
        } else {
            Ok(None)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion_physical_plan::ExecutionPlanProperties;

    fn test_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]))
    }

    fn test_batch(values: Vec<i32>, schema: SchemaRef) -> RecordBatch {
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(values))]).unwrap()
    }

    #[test]
    fn test_row_count_hint() {
        assert_eq!(RowCountHint::Exact(100).value(), 100);
        assert_eq!(RowCountHint::Computed(200).value(), 200);
        assert_eq!(RowCountHint::UserHint(300).value(), 300);
        assert_eq!(RowCountHint::UpperBound(400).value(), 400);

        assert!(RowCountHint::Exact(100).is_exact());
        assert!(!RowCountHint::Computed(200).is_exact());
        assert!(!RowCountHint::UserHint(300).is_exact());
        assert!(!RowCountHint::UpperBound(400).is_exact());

        assert!(RowCountHint::Exact(100).is_reliable());
        assert!(RowCountHint::Computed(200).is_reliable());
        assert!(!RowCountHint::UserHint(300).is_reliable());
        assert!(!RowCountHint::UpperBound(400).is_reliable());
    }

    #[test]
    fn test_data_source_metadata() {
        let streaming = DataSourceMetadata::streaming();
        assert!(streaming.row_count.is_none());
        assert!(!streaming.rescannable);
        assert_eq!(streaming.num_streams, 1);

        let in_memory = DataSourceMetadata::in_memory(1000);
        assert_eq!(in_memory.row_count.unwrap().value(), 1000);
        assert!(in_memory.rescannable);
        assert_eq!(in_memory.num_streams, 1);

        let multi_stream = DataSourceMetadata::multi_stream(4);
        assert!(multi_stream.row_count.is_none());
        assert!(multi_stream.rescannable);
        assert_eq!(multi_stream.num_streams, 4);
    }

    #[test]
    fn test_into_arrow_adapter() {
        let schema = test_schema();
        let batch = test_batch(vec![1, 2, 3], schema.clone());
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let adapter = IntoArrowAdapter::new(Box::new(reader), schema.clone())
            .with_row_count(RowCountHint::Exact(3))
            .with_rescannable(true);

        assert_eq!(adapter.schema(), schema);
        let metadata = adapter.metadata();
        assert_eq!(metadata.row_count.unwrap().value(), 3);
        assert!(metadata.rescannable);
    }

    #[test]
    fn test_multi_stream_data_source() {
        let schema = test_schema();

        let mut source = MultiStreamDataSource::new(schema.clone());

        for i in 0..3 {
            let schema_clone = schema.clone();
            source = source.add_stream(Box::new(move || {
                let batch = RecordBatch::try_new(
                    schema_clone.clone(),
                    vec![Arc::new(Int32Array::from(vec![i * 10, i * 10 + 1]))],
                )
                .unwrap();
                let reader = RecordBatchIterator::new(vec![Ok(batch)], schema_clone);
                Ok(Box::new(reader) as Box<dyn RecordBatchReader + Send>)
            }));
        }

        let source = source.with_row_count(RowCountHint::Computed(6));

        assert_eq!(source.schema(), schema);
        let metadata = source.metadata();
        assert_eq!(metadata.row_count.unwrap().value(), 6);
        assert_eq!(metadata.num_streams, 3);
        assert!(metadata.rescannable);
    }

    #[tokio::test]
    async fn test_into_arrow_adapter_execution() {
        use datafusion_execution::TaskContext;
        use futures::StreamExt;

        let schema = test_schema();
        let batch = test_batch(vec![1, 2, 3], schema.clone());
        let reader = RecordBatchIterator::new(vec![Ok(batch.clone())], schema.clone());

        let adapter = Box::new(IntoArrowAdapter::new(Box::new(reader), schema.clone()));
        let plan = adapter.into_execution_plan().unwrap();

        let ctx = Arc::new(TaskContext::default());
        let mut stream = plan.execute(0, ctx).unwrap();

        let result = stream.next().await.unwrap().unwrap();
        assert_eq!(result.num_rows(), 3);
        assert_eq!(result.schema(), schema);
    }

    #[tokio::test]
    async fn test_multi_stream_execution() {
        use datafusion_execution::TaskContext;
        use futures::StreamExt;

        let schema = test_schema();

        let mut source = MultiStreamDataSource::new(schema.clone());

        for i in 0..2 {
            let schema_clone = schema.clone();
            source = source.add_stream(Box::new(move || {
                let batch = RecordBatch::try_new(
                    schema_clone.clone(),
                    vec![Arc::new(Int32Array::from(vec![i * 10, i * 10 + 1]))],
                )
                .unwrap();
                let reader = RecordBatchIterator::new(vec![Ok(batch)], schema_clone);
                Ok(Box::new(reader) as Box<dyn RecordBatchReader + Send>)
            }));
        }

        let plan = Box::new(source).into_execution_plan().unwrap();

        // Should have 2 partitions
        assert_eq!(plan.output_partitioning().partition_count(), 2);

        let ctx = Arc::new(TaskContext::default());

        // Execute partition 0
        let mut stream0 = plan.execute(0, ctx.clone()).unwrap();
        let batch0 = stream0.next().await.unwrap().unwrap();
        assert_eq!(batch0.num_rows(), 2);

        // Execute partition 1
        let mut stream1 = plan.execute(1, ctx).unwrap();
        let batch1 = stream1.next().await.unwrap().unwrap();
        assert_eq!(batch1.num_rows(), 2);
    }
}
