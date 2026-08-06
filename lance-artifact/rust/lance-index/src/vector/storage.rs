// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Vector Storage, holding (quantized) vectors and providing distance calculation.

use crate::vector::quantizer::QuantizerStorage;
use arrow::compute::concat_batches;
use arrow_array::{ArrayRef, RecordBatch};
use arrow_schema::SchemaRef;
use futures::prelude::stream::TryStreamExt;
use lance_arrow::RecordBatchExt;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, ROW_ID, Result};
use lance_encoding::decoder::FilterExpression;
use lance_file::reader::FileReader;
use lance_io::ReadBatchParams;
use lance_io::scheduler::IoStats;
use lance_linalg::distance::DistanceType;
use prost::Message;
use std::{
    any::Any,
    borrow::Cow,
    collections::BinaryHeap,
    mem::size_of,
    ops::{Deref, DerefMut},
    sync::Arc,
};

use crossbeam_queue::ArrayQueue;

use crate::frag_reuse::FragReuseIndex;
use crate::{
    pb,
    vector::{
        ivf::storage::{IVF_METADATA_KEY, IvfModel},
        quantizer::Quantization,
    },
};

use super::graph::OrderedFloat;
use super::graph::OrderedNode;
use super::quantizer::{Quantizer, QuantizerMetadata};
use super::{ApproxMode, DISTANCE_TYPE_KEY};

/// <section class="warning">
///  Internal API
///
///  API stability is not guaranteed
/// </section>
pub trait DistCalculator {
    fn distance(&self, id: u32) -> f32;

    // return the distances of all rows
    // k_hint is a hint that can be used for optimization
    fn distance_all(&self, k_hint: usize) -> Vec<f32>;

    // Write the distances of all rows into caller-owned scratch buffers.
    fn distance_all_with_scratch(
        &self,
        k_hint: usize,
        dists: &mut Vec<f32>,
        _u16_scratch: &mut Vec<u16>,
        _u8_scratch: &mut Vec<u8>,
        _u32_scratch: &mut Vec<u32>,
    ) {
        *dists = self.distance_all(k_hint);
    }

    fn prefetch(&self, _id: u32) {}

    #[allow(clippy::too_many_arguments)]
    fn accumulate_topk_with_scratch(
        &self,
        k: usize,
        lower_bound: Option<f32>,
        upper_bound: Option<f32>,
        row_id: impl Fn(u32) -> u64,
        res: &mut BinaryHeap<OrderedNode<u64>>,
        dists: &mut Vec<f32>,
        u16_scratch: &mut Vec<u16>,
        u8_scratch: &mut Vec<u8>,
        u32_scratch: &mut Vec<u32>,
    ) {
        if k == 0 {
            return;
        }

        self.distance_all_with_scratch(k, dists, u16_scratch, u8_scratch, u32_scratch);
        let lower_bound = lower_bound.unwrap_or(f32::MIN).into();
        let upper_bound = upper_bound.unwrap_or(f32::MAX).into();
        let mut max_dist = res.peek().map(|node| node.dist);

        for (id, dist) in dists.iter().copied().enumerate() {
            let dist = OrderedFloat(dist);
            if dist < lower_bound || dist >= upper_bound {
                continue;
            }
            if res.len() < k {
                res.push(OrderedNode::new(row_id(id as u32), dist));
                if res.len() == k {
                    max_dist = res.peek().map(|node| node.dist);
                }
            } else if max_dist.is_some_and(|max_dist| max_dist > dist) {
                res.pop();
                res.push(OrderedNode::new(row_id(id as u32), dist));
                max_dist = res.peek().map(|node| node.dist);
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn accumulate_filtered_topk_with_scratch(
        &self,
        k: usize,
        lower_bound: Option<f32>,
        upper_bound: Option<f32>,
        row_ids: impl Iterator<Item = (u32, u64)>,
        accept_row: impl Fn(u64) -> bool,
        res: &mut BinaryHeap<OrderedNode<u64>>,
        _dists: &mut Vec<f32>,
        _u16_scratch: &mut Vec<u16>,
        _u8_scratch: &mut Vec<u8>,
        _u32_scratch: &mut Vec<u32>,
    ) {
        if k == 0 {
            return;
        }

        let lower_bound = lower_bound.unwrap_or(f32::MIN).into();
        let upper_bound = upper_bound.unwrap_or(f32::MAX).into();
        let mut max_dist = res.peek().map(|node| node.dist);

        for (id, row_id) in row_ids {
            if !accept_row(row_id) {
                continue;
            }
            let dist = OrderedFloat(self.distance(id));
            if dist < lower_bound || dist >= upper_bound {
                continue;
            }
            if res.len() < k {
                res.push(OrderedNode::new(row_id, dist));
                if res.len() == k {
                    max_dist = res.peek().map(|node| node.dist);
                }
            } else if max_dist.is_some_and(|max_dist| max_dist > dist) {
                res.pop();
                res.push(OrderedNode::new(row_id, dist));
                max_dist = res.peek().map(|node| node.dist);
            }
        }
    }
}

pub const STORAGE_METADATA_KEY: &str = "storage_metadata";

#[derive(Debug)]
pub struct QueryScratch {
    pub distances: Vec<f32>,
    pub query_f32: Vec<f32>,
    pub u16: Vec<u16>,
    pub u8: Vec<u8>,
    pub u32: Vec<u32>,
}

impl QueryScratch {
    pub const fn new() -> Self {
        Self {
            distances: Vec::new(),
            query_f32: Vec::new(),
            u16: Vec::new(),
            u8: Vec::new(),
            u32: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: QueryScratchCapacity) -> Self {
        Self {
            distances: vec![0.0; capacity.distances],
            query_f32: vec![0.0; capacity.query_f32],
            u16: vec![0; capacity.u16],
            u8: vec![0; capacity.u8],
            u32: vec![0; capacity.u32],
        }
    }
}

impl Default for QueryScratch {
    fn default() -> Self {
        Self::new()
    }
}

impl DeepSizeOf for QueryScratch {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        self.distances.capacity() * size_of::<f32>()
            + self.query_f32.capacity() * size_of::<f32>()
            + self.u16.capacity() * size_of::<u16>()
            + self.u8.capacity() * size_of::<u8>()
            + self.u32.capacity() * size_of::<u32>()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct QueryScratchCapacity {
    pub distances: usize,
    pub query_f32: usize,
    pub u16: usize,
    pub u8: usize,
    pub u32: usize,
}

impl QueryScratchCapacity {
    pub const fn new(distances: usize, query_f32: usize, u16: usize, u8: usize) -> Self {
        Self::new_with_u32(distances, query_f32, u16, u8, 0)
    }

    pub const fn new_with_u32(
        distances: usize,
        query_f32: usize,
        u16: usize,
        u8: usize,
        u32: usize,
    ) -> Self {
        Self {
            distances,
            query_f32,
            u16,
            u8,
            u32,
        }
    }

    fn deep_size_bytes(&self) -> usize {
        self.distances * size_of::<f32>()
            + self.query_f32 * size_of::<f32>()
            + self.u16 * size_of::<u16>()
            + self.u8 * size_of::<u8>()
            + self.u32 * size_of::<u32>()
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DistanceCalculatorOptions {
    pub approx_mode: ApproxMode,
}

#[derive(Debug)]
pub struct RabitRawQueryContext {
    pub code_dim: usize,
    pub ex_bits: u8,
    pub rotated_query: Vec<f32>,
    pub dist_table: Vec<f32>,
    /// The rotated query zero-padded to a 64-dim multiple for the ex-dot
    /// kernels; empty when `code_dim` is already aligned (the kernels then
    /// read `rotated_query` directly).
    pub ex_query: Vec<f32>,
    pub sum_q: f32,
}

#[derive(Clone, Copy)]
pub enum QueryResidual<'a> {
    Centroid(&'a dyn arrow_array::Array),
    RabitRawQuery {
        rotated_centroid: Option<&'a [f32]>,
        query: Option<&'a RabitRawQueryContext>,
    },
}

#[derive(Debug)]
pub struct QueryScratchPool {
    scratches: ArrayQueue<QueryScratch>,
    scratch_capacity: QueryScratchCapacity,
}

impl QueryScratchPool {
    pub fn new(size: usize) -> Self {
        Self::with_capacity(size, QueryScratchCapacity::default())
    }

    pub fn with_capacity(size: usize, capacity: QueryScratchCapacity) -> Self {
        let size = size.max(1);
        let scratches = ArrayQueue::new(size);
        for _ in 0..size {
            scratches
                .push(QueryScratch::with_capacity(capacity))
                .expect("query scratch pool should have spare capacity during initialization");
        }
        Self {
            scratches,
            scratch_capacity: capacity,
        }
    }

    pub fn scratch(&self) -> QueryScratchGuard<'_> {
        let (scratch, pooled) = if let Some(scratch) = self.scratches.pop() {
            (scratch, true)
        } else {
            (QueryScratch::with_capacity(self.scratch_capacity), false)
        };
        QueryScratchGuard {
            pool: self,
            scratch: Some(scratch),
            pooled,
        }
    }

    pub fn with_scratch<T>(&self, f: impl FnOnce(&mut QueryScratch) -> T) -> T {
        let mut scratch = self.scratch();
        f(&mut scratch)
    }
}

pub struct QueryScratchGuard<'a> {
    pool: &'a QueryScratchPool,
    scratch: Option<QueryScratch>,
    pooled: bool,
}

impl Deref for QueryScratchGuard<'_> {
    type Target = QueryScratch;

    fn deref(&self) -> &Self::Target {
        self.scratch
            .as_ref()
            .expect("query scratch guard should hold scratch")
    }
}

impl DerefMut for QueryScratchGuard<'_> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.scratch
            .as_mut()
            .expect("query scratch guard should hold scratch")
    }
}

impl Drop for QueryScratchGuard<'_> {
    fn drop(&mut self) {
        if !self.pooled {
            return;
        }
        if let Some(scratch) = self.scratch.take() {
            match self.pool.scratches.push(scratch) {
                Ok(()) => {}
                Err(_) => unreachable!("query scratch pool should not exceed its capacity"),
            }
        }
    }
}

impl DeepSizeOf for QueryScratchPool {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        let mut total = self.scratches.capacity() * size_of::<QueryScratch>();
        let mut scratches = Vec::new();
        while let Some(scratch) = self.scratches.pop() {
            total += scratch.deep_size_of_children(context);
            scratches.push(scratch);
        }
        let checked_out = self.scratches.capacity().saturating_sub(scratches.len());
        total += checked_out * self.scratch_capacity.deep_size_bytes();
        for scratch in scratches {
            let _ = self.scratches.push(scratch);
        }
        total
    }
}

/// Vector Storage is the abstraction to store the vectors.
///
/// It can be in-memory or on-disk, raw vector or quantized vectors.
///
/// It abstracts away the logic to compute the distance between vectors.
///
/// TODO: should we rename this to "VectorDistance"?;
///
/// <section class="warning">
///  Internal API
///
///  API stability is not guaranteed
/// </section>
pub trait VectorStore: Send + Sync + Sized + Clone {
    type DistanceCalculator<'a>: DistCalculator
    where
        Self: 'a;

    fn as_any(&self) -> &dyn Any;

    fn schema(&self) -> &SchemaRef;

    fn to_batches(&self) -> Result<impl Iterator<Item = RecordBatch> + Send>;

    fn len(&self) -> usize;

    /// Returns true if this graph is empty.
    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Return [DistanceType].
    fn distance_type(&self) -> DistanceType;

    /// Get the lance ROW ID from one vector.
    fn row_id(&self, id: u32) -> u64;

    fn row_ids(&self) -> impl Iterator<Item = &u64>;

    /// Append Raw [RecordBatch] into the Storage.
    /// The storage implement will perform quantization if necessary.
    fn append_batch(&self, batch: RecordBatch, vector_column: &str) -> Result<Self>;

    /// Create a [DistCalculator] to compute the distance between the query.
    ///
    /// Using dist calculator can be more efficient as it can pre-compute some
    /// values.
    fn dist_calculator(&self, query: ArrayRef, dist_q_c: f32) -> Self::DistanceCalculator<'_>;

    /// Create a [DistCalculator], reusing caller-owned scratch for query-time
    /// precomputed state when the storage supports it.
    fn dist_calculator_with_scratch<'a>(
        &'a self,
        query: ArrayRef,
        dist_q_c: f32,
        _residual: Option<QueryResidual<'a>>,
        _f32_scratch: &'a mut Vec<f32>,
        _options: DistanceCalculatorOptions,
    ) -> Self::DistanceCalculator<'a> {
        self.dist_calculator(query, dist_q_c)
    }

    fn dist_calculator_from_id(&self, id: u32) -> Self::DistanceCalculator<'_>;

    fn dist_between(&self, u: u32, v: u32) -> f32 {
        let dist_cal_u = self.dist_calculator_from_id(u);
        dist_cal_u.distance(v)
    }

    fn prefers_candidate(&self, candidate: &OrderedNode, selected: &[OrderedNode]) -> bool {
        let dist_cal_candidate = self.dist_calculator_from_id(candidate.id);
        selected
            .iter()
            .all(|other| candidate.dist < OrderedFloat(dist_cal_candidate.distance(other.id)))
    }
}

pub struct StorageBuilder<Q: Quantization> {
    vector_column: String,
    distance_type: DistanceType,
    quantizer: Q,

    frag_reuse_index: Option<Arc<FragReuseIndex>>,
}

impl<Q: Quantization> StorageBuilder<Q> {
    pub fn new(
        vector_column: String,
        distance_type: DistanceType,
        quantizer: Q,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self> {
        Ok(Self {
            vector_column,
            distance_type,
            quantizer,
            frag_reuse_index,
        })
    }

    pub fn build(&self, batches: Vec<RecordBatch>) -> Result<Q::Storage> {
        let mut batch = concat_batches(batches[0].schema_ref(), batches.iter())?;

        if batch.column_by_name(self.quantizer.column()).is_none() {
            let vectors = batch
                .column_by_name(&self.vector_column)
                .ok_or(Error::index(format!(
                    "Vector column {} not found in batch",
                    self.vector_column
                )))?;
            let codes = self.quantizer.quantize(vectors)?;
            batch = batch.drop_column(&self.vector_column)?.try_with_column(
                arrow_schema::Field::new(self.quantizer.column(), codes.data_type().clone(), true),
                codes,
            )?;
        }

        debug_assert!(batch.column_by_name(ROW_ID).is_some());
        debug_assert!(batch.column_by_name(self.quantizer.column()).is_some());

        Q::Storage::try_from_batch(
            batch,
            &self.quantizer.metadata(None),
            self.distance_type,
            self.frag_reuse_index.clone(),
        )
    }
}

/// Loader to load partitioned PQ storage from disk.
#[derive(Debug)]
pub struct IvfQuantizationStorage<Q: Quantization> {
    reader: FileReader,

    distance_type: DistanceType,
    metadata: Q::Metadata,

    ivf: IvfModel,
    frag_reuse_index: Option<Arc<FragReuseIndex>>,
}

impl<Q: Quantization> DeepSizeOf for IvfQuantizationStorage<Q> {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.metadata.deep_size_of_children(context) + self.ivf.deep_size_of_children(context)
    }
}

impl<Q: Quantization> IvfQuantizationStorage<Q> {
    /// Open a Loader.
    ///
    ///
    pub async fn try_new(
        reader: FileReader,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self> {
        let schema = reader.schema();

        let distance_type = DistanceType::try_from(
            schema
                .metadata
                .get(DISTANCE_TYPE_KEY)
                .ok_or(Error::index(format!("{} not found", DISTANCE_TYPE_KEY)))?
                .as_str(),
        )?;

        let ivf_pos = schema
            .metadata
            .get(IVF_METADATA_KEY)
            .ok_or(Error::index(format!("{} not found", IVF_METADATA_KEY)))?
            .parse()
            .map_err(|e| Error::index(format!("Failed to decode IVF metadata: {}", e)))?;
        let ivf_bytes = reader.read_global_buffer(ivf_pos).await?;
        let ivf = IvfModel::try_from(pb::Ivf::decode(ivf_bytes)?)?;

        let mut metadata: Vec<String> = serde_json::from_str(
            schema
                .metadata
                .get(STORAGE_METADATA_KEY)
                .ok_or(Error::index(format!("{} not found", STORAGE_METADATA_KEY)))?
                .as_str(),
        )?;
        debug_assert_eq!(metadata.len(), 1);
        // for now the metadata is the same for all partitions, so we just store one
        let metadata = metadata
            .pop()
            .ok_or(Error::index("metadata is empty".to_string()))?;
        let mut metadata: Q::Metadata = serde_json::from_str(&metadata)?;
        // we store large metadata (e.g. PQ codebook) in global buffer,
        // and the schema metadata just contains a pointer to the buffer
        if let Some(pos) = metadata.buffer_index() {
            let bytes = reader.read_global_buffer(pos).await?;
            metadata.parse_buffer(bytes)?;
        }

        Ok(Self {
            reader,
            distance_type,
            metadata,
            ivf,
            frag_reuse_index,
        })
    }

    /// Construct from pre-parsed metadata, skipping global buffer reads.
    /// Used when reconstructing from a disk cache.
    pub fn from_cached(
        reader: FileReader,
        ivf: IvfModel,
        metadata: Q::Metadata,
        distance_type: DistanceType,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Self {
        Self {
            reader,
            distance_type,
            metadata,
            ivf,
            frag_reuse_index,
        }
    }

    pub fn reader(&self) -> &FileReader {
        &self.reader
    }

    pub fn ivf(&self) -> &IvfModel {
        &self.ivf
    }

    pub fn num_rows(&self) -> u64 {
        self.reader.num_rows()
    }

    pub fn partition_size(&self, part_id: usize) -> usize {
        self.ivf.partition_size(part_id)
    }

    pub fn quantizer(&self) -> Result<Quantizer> {
        let metadata = self.metadata();
        Q::from_metadata(metadata, self.distance_type)
    }

    pub fn metadata(&self) -> &Q::Metadata {
        &self.metadata
    }

    pub fn distance_type(&self) -> DistanceType {
        self.distance_type
    }

    pub fn schema(&self) -> SchemaRef {
        Arc::new(self.reader.schema().as_ref().into())
    }

    /// Get the number of partitions in the storage.
    pub fn num_partitions(&self) -> usize {
        self.ivf.num_partitions()
    }

    /// Load a partition's quantization storage, optionally measuring the exact
    /// I/O it performs into `io_stats`.
    ///
    /// When `io_stats` is `Some`, the partition is read through a reader whose
    /// scheduler also records into the sink (a cheap clone that shares all
    /// cached metadata, so no file is re-opened).  When `None`, the normal
    /// uninstrumented reader is used.
    pub async fn load_partition(
        &self,
        part_id: usize,
        io_stats: Option<IoStats>,
    ) -> Result<Q::Storage> {
        let range = self.ivf.row_range(part_id);
        let batch = if range.is_empty() {
            let schema = self.reader.schema();
            let arrow_schema = arrow_schema::Schema::from(schema.as_ref());
            RecordBatch::new_empty(Arc::new(arrow_schema))
        } else {
            let reader = match &io_stats {
                Some(io_stats) => Cow::Owned(self.reader.with_io_stats(io_stats.recorder())),
                None => Cow::Borrowed(&self.reader),
            };
            let batches = reader
                .read_stream(
                    ReadBatchParams::Range(range),
                    u32::MAX,
                    1,
                    FilterExpression::no_filter(),
                )
                .await?
                .try_collect::<Vec<_>>()
                .await?;
            let schema = Arc::new(self.reader.schema().as_ref().into());
            concat_batches(&schema, batches.iter())?
        };
        Q::Storage::try_from_batch(
            batch,
            self.metadata(),
            self.distance_type,
            self.frag_reuse_index.clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::{QueryScratchCapacity, QueryScratchPool};
    use lance_core::deepsize::DeepSizeOf;

    #[test]
    fn test_query_scratch_pool_reuses_buffers() {
        let pool = QueryScratchPool::new(1);
        let first_ptrs = pool.with_scratch(|scratch| {
            scratch.query_f32.clear();
            scratch.query_f32.resize(16, 1.0);
            scratch.distances.clear();
            scratch.distances.resize(8, 2.0);
            scratch.u16.clear();
            scratch.u16.resize(4, 3);
            scratch.u8.clear();
            scratch.u8.resize(2, 4);
            scratch.u32.clear();
            scratch.u32.resize(3, 5);
            (
                scratch.query_f32.as_ptr(),
                scratch.distances.as_ptr(),
                scratch.u16.as_ptr(),
                scratch.u8.as_ptr(),
                scratch.u32.as_ptr(),
            )
        });

        let second_ptrs = pool.with_scratch(|scratch| {
            assert_eq!(scratch.query_f32.len(), 16);
            assert!(scratch.query_f32.iter().all(|value| *value == 1.0));
            assert_eq!(scratch.distances.len(), 8);
            assert!(scratch.distances.iter().all(|value| *value == 2.0));
            assert_eq!(scratch.u16.len(), 4);
            assert!(scratch.u16.iter().all(|value| *value == 3));
            assert_eq!(scratch.u8.len(), 2);
            assert!(scratch.u8.iter().all(|value| *value == 4));
            assert_eq!(scratch.u32.len(), 3);
            assert!(scratch.u32.iter().all(|value| *value == 5));
            (
                scratch.query_f32.as_ptr(),
                scratch.distances.as_ptr(),
                scratch.u16.as_ptr(),
                scratch.u8.as_ptr(),
                scratch.u32.as_ptr(),
            )
        });

        assert_eq!(first_ptrs, second_ptrs);
    }

    #[test]
    fn test_query_scratch_pool_is_pool_owned() {
        let first_pool = QueryScratchPool::new(1);
        let second_pool = QueryScratchPool::new(1);

        let first_ptr = first_pool.with_scratch(|scratch| {
            scratch.query_f32.resize(16, 1.0);
            scratch.query_f32.as_ptr()
        });
        let second_ptr = second_pool.with_scratch(|scratch| {
            scratch.query_f32.resize(16, 1.0);
            scratch.query_f32.as_ptr()
        });

        assert_ne!(first_ptr, second_ptr);
    }

    #[test]
    fn test_query_scratch_pool_uses_temporary_scratch_when_empty() {
        let pool =
            QueryScratchPool::with_capacity(1, QueryScratchCapacity::new_with_u32(8, 16, 4, 2, 3));
        let pooled = pool.scratch();
        assert!(pooled.pooled);

        let temporary = pool.scratch();
        assert!(!temporary.pooled);
        assert_eq!(temporary.distances.len(), 8);
        assert_eq!(temporary.query_f32.len(), 16);
        assert_eq!(temporary.u16.len(), 4);
        assert_eq!(temporary.u8.len(), 2);
        assert_eq!(temporary.u32.len(), 3);
    }

    #[test]
    fn test_query_scratch_pool_deep_size_includes_buffer_capacity() {
        let empty_size = QueryScratchPool::new(1).deep_size_of();
        let pool =
            QueryScratchPool::with_capacity(1, QueryScratchCapacity::new_with_u32(8, 16, 4, 2, 3));

        assert!(pool.deep_size_of() > empty_size);

        let idle_size = pool.deep_size_of();
        let _checked_out = pool.scratch();

        assert_eq!(pool.deep_size_of(), idle_size);
    }

    #[test]
    fn test_query_scratch_pool_initializes_buffer_capacity() {
        let pool =
            QueryScratchPool::with_capacity(1, QueryScratchCapacity::new_with_u32(8, 16, 4, 2, 3));

        pool.with_scratch(|scratch| {
            assert_eq!(scratch.distances.len(), 8);
            assert_eq!(scratch.distances.capacity(), 8);
            assert_eq!(scratch.query_f32.len(), 16);
            assert_eq!(scratch.query_f32.capacity(), 16);
            assert_eq!(scratch.u16.len(), 4);
            assert_eq!(scratch.u16.capacity(), 4);
            assert_eq!(scratch.u8.len(), 2);
            assert_eq!(scratch.u8.capacity(), 2);
            assert_eq!(scratch.u32.len(), 3);
            assert_eq!(scratch.u32.capacity(), 3);
        });
    }
}
