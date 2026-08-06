// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Vector Index
//!

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;

use arrow_array::{ArrayRef, Float32Array, RecordBatch, UInt32Array};
use arrow_schema::Field;
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use futures::stream;
use ivf::storage::IvfModel;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, ROW_ID_FIELD, Result};
use lance_io::traits::Reader;
use lance_linalg::distance::DistanceType;
use quantizer::{QuantizationType, Quantizer};
use std::sync::LazyLock;
use v3::subindex::SubIndexType;

pub mod bq;
pub mod distributed;
pub mod flat;
pub mod graph;
pub mod hnsw;
pub mod ivf;
pub mod kmeans;
pub mod pq;
pub mod quantizer;
pub mod residual;
pub mod shared;
pub mod sq;
pub mod storage;
pub mod transform;
pub mod utils;
pub mod v3;

use super::pb;
use crate::metrics::MetricsCollector;
use crate::{Index, prefilter::PreFilter};

// TODO: Make these crate private once the migration from lance to lance-index is done.
pub const DIST_COL: &str = "_distance";
pub const DISTANCE_TYPE_KEY: &str = "distance_type";
pub const INDEX_UUID_COLUMN: &str = "__index_uuid";
pub const PART_ID_COLUMN: &str = "__ivf_part_id";
pub const DIST_Q_C_COLUMN: &str = "__dist_q_c";
// dist from vector to centroid
pub const CENTROID_DIST_COLUMN: &str = "__centroid_dist";
pub const PQ_CODE_COLUMN: &str = "__pq_code";
pub const SQ_CODE_COLUMN: &str = "__sq_code";
pub const LOSS_METADATA_KEY: &str = "_loss";

pub type PreparedPartitionSearchHandle = Box<dyn Any + Send>;

/// Controls when a multi-partition search should stop producing more partition results.
pub trait PartitionSearchControl: Send + Sync {
    fn should_stop(&self) -> bool;

    fn record_batch(&self, _batch: &RecordBatch) {}
}

pub static VECTOR_RESULT_SCHEMA: LazyLock<arrow_schema::SchemaRef> = LazyLock::new(|| {
    arrow_schema::SchemaRef::new(arrow_schema::Schema::new(vec![
        Field::new(DIST_COL, arrow_schema::DataType::Float32, true),
        ROW_ID_FIELD.clone(),
    ]))
});

pub static PART_ID_FIELD: LazyLock<arrow_schema::Field> = LazyLock::new(|| {
    arrow_schema::Field::new(PART_ID_COLUMN, arrow_schema::DataType::UInt32, true)
});

pub static CENTROID_DIST_FIELD: LazyLock<arrow_schema::Field> = LazyLock::new(|| {
    arrow_schema::Field::new(CENTROID_DIST_COLUMN, arrow_schema::DataType::Float32, true)
});

pub const DEFAULT_QUERY_PARALLELISM: i32 = 0;

/// Controls the speed / accuracy tradeoff for approximate vector search.
///
/// This currently affects RQ-quantized vector indexes (such as IVF_RQ) and
/// prefiltered search on HNSW sub-indexes, where `Fast` enables the ACORN
/// traversal. Other index types ignore this setting.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ApproxMode {
    /// Prefer lower query latency, which can reduce recall.
    Fast,

    /// Use the default balance between query latency and recall.
    #[default]
    Normal,

    /// Prefer higher recall, which can increase query latency.
    Accurate,
}

/// Query parameters for the vector indices

#[derive(Debug, Clone)]
pub struct Query {
    /// The column to be searched.
    pub column: String,

    /// The vector to be searched.
    pub key: ArrayRef,

    /// Top k results to return.
    pub k: usize,

    /// The lower bound (inclusive) of the distance to be searched.
    pub lower_bound: Option<f32>,

    /// The upper bound (exclusive) of the distance to be searched.
    pub upper_bound: Option<f32>,

    /// The minimum number of probes to load and search.  More partitions
    /// will only be loaded if we have not found k results, or the algorithm
    /// determines more partitions are needed to satisfy recall requirements.
    ///
    /// The planner will always search at least this many partitions. Defaults to 1.
    pub minimum_nprobes: usize,

    /// The maximum number of probes to load and search.  If not set then
    /// ALL partitions will be searched, if needed, to satisfy k results.
    pub maximum_nprobes: Option<usize>,

    /// The number of candidates to reserve while searching.
    /// this is an optional parameter for HNSW related index types.
    pub ef: Option<usize>,

    /// If presented, apply a refine step.
    /// TODO: should we support fraction / float number here?
    pub refine_factor: Option<u32>,

    /// Distance metric type. If None, uses the index's metric (if available)
    /// or the default for the data type.
    pub metric_type: Option<DistanceType>,

    /// Whether to use an ANN index if available
    pub use_index: bool,

    /// Maximum partition-search concurrency for a single vector query.
    ///
    /// The default is 0.
    /// Value 0 selects the automatic policy; today this resolves to 1 for the
    /// sequential fast path unless an index implementation overrides it.
    /// Value -1 uses the CPU pool size.
    /// Value 1 uses the single-worker sequential partition search path.
    /// Values >= 2 use the partition-parallel path and are clamped to the CPU
    /// pool size by the execution layer.
    pub query_parallelism: i32,

    /// the distance between the query and the centroid
    /// this is only used for IVF index with Rabit quantization
    pub dist_q_c: f32,

    /// Controls the speed / accuracy tradeoff for approximate vector search.
    ///
    /// This currently only affects RQ-quantized vector indexes, such as IVF_RQ.
    /// Other index types ignore this setting.
    pub approx_mode: ApproxMode,
}

impl From<pb::VectorMetricType> for DistanceType {
    fn from(proto: pb::VectorMetricType) -> Self {
        match proto {
            pb::VectorMetricType::L2 => Self::L2,
            pb::VectorMetricType::Cosine => Self::Cosine,
            pb::VectorMetricType::Dot => Self::Dot,
            pb::VectorMetricType::Hamming => Self::Hamming,
        }
    }
}

impl From<DistanceType> for pb::VectorMetricType {
    fn from(mt: DistanceType) -> Self {
        match mt {
            DistanceType::L2 => Self::L2,
            DistanceType::Cosine => Self::Cosine,
            DistanceType::Dot => Self::Dot,
            DistanceType::Hamming => Self::Hamming,
        }
    }
}

/// Vector Index for (Approximate) Nearest Neighbor (ANN) Search.
///
/// Vector indices are often built as a chain of indices.  For example, IVF -> PQ
/// or IVF -> HNSW -> SQ.
///
/// We use one trait for both the top-level and the sub-indices.  Typically the top-level
/// search is a partition-aware search and all sub-indices are whole-index searches.
#[async_trait]
#[allow(clippy::redundant_pub_crate)]
pub trait VectorIndex: Send + Sync + std::fmt::Debug + Index {
    /// Search entire index for k nearest neighbors.
    ///
    /// It returns a [RecordBatch] with Schema of:
    ///
    /// ```
    /// use arrow_schema::{Schema, Field, DataType};
    ///
    /// Schema::new(vec![
    ///   Field::new("_rowid", DataType::UInt64, true),
    ///   Field::new("_distance", DataType::Float32, true),
    /// ]);
    /// ```
    ///
    /// The `pre_filter` argument is used to filter out row ids that we know are
    /// not relevant to the query. For example, it removes deleted rows or rows that
    /// do not match a user-provided filter.
    async fn search(
        &self,
        query: &Query,
        pre_filter: Arc<dyn PreFilter>,
        metrics: &dyn MetricsCollector,
    ) -> Result<RecordBatch>;

    /// Find partitions that may contain nearest neighbors.
    ///
    /// If maximum_nprobes is set then this method will return the partitions
    /// that are most likely to contain the nearest neighbors (e.g. the closest
    /// partitions to the query vector).
    ///
    /// Return the partition ids and the distances between the query and the centroids,
    /// the results should be in sorted order from closest to farthest.
    fn find_partitions(&self, query: &Query) -> Result<(UInt32Array, Float32Array)>;

    /// Get the total number of partitions in the index.
    fn total_partitions(&self) -> usize;

    /// Search a single partition for nearest neighbors.
    ///
    /// This method should return the same results as [`VectorIndex::search`] method except
    /// that it will only search a single partition.
    async fn search_in_partition(
        &self,
        partition_id: usize,
        query: &Query,
        pre_filter: Arc<dyn PreFilter>,
        metrics: &dyn MetricsCollector,
    ) -> Result<RecordBatch>;

    /// Asynchronously prepare a single-partition search so the CPU-heavy portion
    /// can be executed separately.
    async fn prepare_partition_search(
        &self,
        _partition_id: usize,
        _query: &Query,
        _pre_filter: Arc<dyn PreFilter>,
        _metrics: &dyn MetricsCollector,
    ) -> Result<PreparedPartitionSearchHandle> {
        unimplemented!("prepared partition search is not supported for this index")
    }

    /// Execute the synchronous portion of a previously prepared partition search.
    fn search_prepared_partition(
        &self,
        _prepared: PreparedPartitionSearchHandle,
        _metrics: &dyn MetricsCollector,
    ) -> Result<RecordBatch> {
        unimplemented!("prepared partition search is not supported for this index")
    }

    /// Return true if the index supports splitting partition search into async
    /// prepare and sync execute phases.
    fn supports_prepared_partition_search(&self) -> bool {
        false
    }

    /// Choose partition search concurrency for `query_parallelism = 0`.
    ///
    /// The default keeps the single-worker sequential path. Index
    /// implementations can override this when their sub-index search work does
    /// not benefit from the sequential fast path.
    fn auto_query_parallelism(&self, _cpu_pool_size: usize) -> usize {
        1
    }

    /// Search a range of partitions and return a stream of per-partition result batches.
    ///
    /// The default implementation searches each partition sequentially with
    /// [`VectorIndex::search_in_partition`]. Implementations can override this
    /// to use a more efficient execution strategy.
    #[allow(clippy::too_many_arguments)]
    async fn search_partitions(
        self: Arc<Self>,
        query: Query,
        partitions: Arc<UInt32Array>,
        q_c_dists: Arc<Float32Array>,
        start_idx: usize,
        end_idx: usize,
        pre_filter: Arc<dyn PreFilter>,
        control: Option<Arc<dyn PartitionSearchControl>>,
        metrics: Arc<dyn MetricsCollector>,
    ) -> Result<SendableRecordBatchStream>
    where
        Self: 'static,
    {
        if partitions.len() != q_c_dists.len() {
            return Err(Error::invalid_input(format!(
                "partition count {} does not match centroid distance count {}",
                partitions.len(),
                q_c_dists.len()
            )));
        }
        if start_idx > end_idx || end_idx > partitions.len() {
            return Err(Error::invalid_input(format!(
                "invalid partition search range [{start_idx}, {end_idx}) for {} partitions",
                partitions.len()
            )));
        }

        let stream = stream::try_unfold(start_idx, move |idx| {
            let index = self.clone();
            let partitions = partitions.clone();
            let q_c_dists = q_c_dists.clone();
            let query = query.clone();
            let pre_filter = pre_filter.clone();
            let control = control.clone();
            let metrics = metrics.clone();
            async move {
                if idx >= end_idx
                    || control
                        .as_ref()
                        .is_some_and(|control| control.should_stop())
                {
                    return Ok(None);
                }
                let part_id = partitions.value(idx);
                let mut query = query;
                query.dist_q_c = q_c_dists.value(idx);
                index
                    .search_in_partition(part_id as usize, &query, pre_filter, metrics.as_ref())
                    .await
                    .map(|batch| {
                        if let Some(control) = control.as_ref() {
                            control.record_batch(&batch);
                        }
                        Some((batch, idx + 1))
                    })
                    .map_err(Into::into)
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            VECTOR_RESULT_SCHEMA.clone(),
            stream,
        )))
    }

    /// If the index is loadable by IVF, so it can be a sub-index that
    /// is loaded on demand by IVF.
    fn is_loadable(&self) -> bool;

    /// Use residual vector to search.
    fn use_residual(&self) -> bool;

    // async fn append(&self, batches: Vec<RecordBatch>) -> Result<()>;
    // async fn merge(&self, indices: Vec<Arc<dyn VectorIndex>>) -> Result<()>;

    /// Load the index from the reader on-demand.
    async fn load(
        &self,
        reader: Arc<dyn Reader>,
        offset: usize,
        length: usize,
    ) -> Result<Box<dyn VectorIndex>>;

    /// Load the partition from the reader on-demand.
    async fn load_partition(
        &self,
        reader: Arc<dyn Reader>,
        offset: usize,
        length: usize,
        _partition_id: usize,
    ) -> Result<Box<dyn VectorIndex>> {
        self.load(reader, offset, length).await
    }

    // for IVF only
    async fn partition_reader(
        &self,
        _partition_id: usize,
        _with_vector: bool,
        _metrics: &dyn MetricsCollector,
    ) -> Result<SendableRecordBatchStream> {
        unimplemented!("only for IVF")
    }

    // for SubIndex only
    async fn to_batch_stream(&self, with_vector: bool) -> Result<SendableRecordBatchStream>;

    fn num_rows(&self) -> u64;

    /// Return the IDs of rows in the index.
    fn row_ids(&self) -> Box<dyn Iterator<Item = &'_ u64> + '_>;

    /// Remap the index according to mapping
    ///
    /// Each item in mapping describes an old row id -> new row id
    /// pair.  If old row id -> None then that row id has been
    /// deleted and can be removed from the index.
    ///
    /// If an old row id is not in the mapping then it should be
    /// left alone.
    async fn remap(&mut self, mapping: &RowAddrRemap) -> Result<()>;

    /// The metric type of this vector index.
    fn metric_type(&self) -> DistanceType;

    fn ivf_model(&self) -> &IvfModel;
    fn quantizer(&self) -> Quantizer;
    fn partition_size(&self, part_id: usize) -> usize;

    /// the index type of this vector index.
    fn sub_index_type(&self) -> (SubIndexType, QuantizationType);

    /// The cumulative I/O performed while opening this index (file footers, IVF
    /// centroids, quantization metadata).  This is a one-time cost; it is
    /// reported once, on the query that actually opens the index, and is `None`
    /// for index implementations that do not track it.
    fn open_io_stats(&self) -> Option<lance_io::scheduler::ScanStats> {
        None
    }
}

// it can be an IVF index or a partition of IVF index
pub trait VectorIndexCacheEntry: Debug + Send + Sync + DeepSizeOf {
    fn as_any(&self) -> &dyn Any;
}
