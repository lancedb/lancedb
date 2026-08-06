// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! IVF - Inverted File index.

use super::{
    LogicalIvfView, derive_hnsw_params,
    pq::{PQIndex, build_pq_model},
    utils::{filter_finite_training_data, maybe_sample_training_data},
};
use super::{
    builder::{IvfIndexBuilder, index_type_string},
    utils::PartitionLoadLock,
};
use crate::dataset::index::dataset_format_version;
use crate::index::DatasetIndexExt;
use crate::index::DatasetIndexInternalExt;
use crate::index::vector::open_index_file;
use crate::index::vector::utils::{get_vector_dim, get_vector_type};
use crate::{
    dataset::Dataset,
    index::{
        INDEX_FILE_NAME, pb,
        prefilter::PreFilter,
        vector::ivf::io::{write_pq_partition_payload, write_pq_partitions},
    },
};
use crate::{dataset::builder::DatasetBuilder, index::vector::IndexFileVersion};
use arrow::array::ArrayData;
use arrow::compute::concat_batches;
use arrow::datatypes::UInt8Type;
use arrow_arith::numeric::sub;
use arrow_array::Float32Array;
use arrow_array::{
    Array, ArrayRef, FixedSizeListArray, PrimitiveArray, RecordBatch, UInt32Array,
    cast::AsArray,
    types::{ArrowPrimitiveType, Float16Type, Float32Type, Float64Type},
};
use arrow_buffer::MutableBuffer;
use arrow_schema::{DataType, Schema};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use futures::TryFutureExt;
use futures::{
    Stream, TryStreamExt,
    stream::{self, StreamExt},
};
use io::write_hnsw_quantization_index_partitions;
use lance_arrow::*;
use lance_core::deepsize::DeepSizeOf;
use lance_core::utils::row_addr_remap::RowAddrRemap;
use lance_core::{
    Error, ROW_ID_FIELD, Result,
    cache::{CacheKeySchema, KeyBuilder, LanceCache, UnsizedCacheKey, WeakLanceCache},
    traits::DatasetTakeRows,
    utils::parse::parse_env_as_bool,
    utils::tracing::{IO_TYPE_LOAD_VECTOR_PART, TRACE_IO_EVENTS},
};
use lance_encoding::decoder::FilterExpression;
use lance_file::{
    format::MAGIC,
    reader::{FileReader as V2Reader, FileReaderOptions as V2ReaderOptions},
    versions::v1::writer::{FileWriter as V1FileWriter, FileWriterOptions as V1FileWriterOptions},
    writer::{FileWriter as V2Writer, FileWriterOptions as V2WriterOptions},
};
use lance_index::metrics::MetricsCollector;
use lance_index::metrics::NoOpMetricsCollector;
use lance_index::vector::DISTANCE_TYPE_KEY;
use lance_index::vector::bq::builder::RabitQuantizer;
use lance_index::vector::flat::index::{FlatBinQuantizer, FlatIndex, FlatMetadata, FlatQuantizer};
use lance_index::vector::flat::storage::{FLAT_COLUMN, FlatBinStorage, FlatFloatStorage};
use lance_index::vector::hnsw::HnswMetadata;
use lance_index::vector::hnsw::builder::HNSW_METADATA_KEY;
use lance_index::vector::ivf::storage::IVF_METADATA_KEY;
use lance_index::vector::ivf::storage::IvfModel;
use lance_index::vector::kmeans::{KMeans, KMeansParams};
use lance_index::vector::pq::storage::{
    PQ_METADATA_KEY, ProductQuantizationMetadata, ProductQuantizationStorage, transpose,
};
use lance_index::vector::quantizer::QuantizationType;
use lance_index::vector::storage::STORAGE_METADATA_KEY;
use lance_index::vector::v3::shuffler::create_ivf_shuffler;
use lance_index::vector::v3::subindex::{IvfSubIndex, SubIndexType};
use lance_index::{
    INDEX_AUXILIARY_FILE_NAME, INDEX_METADATA_SCHEMA_KEY, Index, IndexMetadata, IndexType,
    MAX_PARTITION_SIZE_FACTOR, MIN_PARTITION_SIZE_PERCENT,
    optimize::OptimizeOptions,
    vector::{
        Query, VectorIndex,
        hnsw::{HNSW, HNSWIndex, builder::HnswBuildParams},
        ivf::{
            IvfBuildParams, builder::load_precomputed_partitions, shuffler::shuffle_dataset,
            storage::IVF_PARTITION_KEY,
        },
        pq::{PQBuildParams, ProductQuantizer},
        quantizer::{Quantization, QuantizationMetadata, Quantizer, QuantizerStorage},
        sq::{
            ScalarQuantizer,
            storage::{SQ_METADATA_KEY, ScalarQuantizationMetadata, ScalarQuantizationStorage},
        },
    },
};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::utils::CachedFileSize;
use lance_io::{
    ReadBatchParams,
    local::to_local_path,
    object_store::ObjectStore,
    stream::RecordBatchStream,
    traits::{Reader, WriteExt, Writer},
};
use lance_linalg::distance::{DistanceType, Dot, L2, MetricType};
use lance_linalg::{distance::Normalize, kernels::normalize_fsl_owned};
use lance_table::format::{IndexFile, IndexMetadata as TableIndexMetadata};
use log::{info, warn};
use object_store::path::Path;
use prost::Message;
use rand::rngs::SmallRng;
use rand::{Rng, SeedableRng};
use roaring::RoaringBitmap;
use serde::Serialize;
use serde_json::json;
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    ops::Range,
    sync::Arc,
};
use tokio::sync::mpsc;
use tracing::instrument;
use uuid::Uuid;

pub mod builder;
pub mod io;
mod partition_serde;
pub mod v2;

// Cache wrapper for vector index trait objects
// Cache key for IVF partitions in the legacy IVF index
#[derive(Debug, Clone)]
pub struct LegacyIVFPartitionKey {
    pub partition_id: usize,
}

impl LegacyIVFPartitionKey {
    pub fn new(partition_id: usize) -> Self {
        Self { partition_id }
    }
}

impl UnsizedCacheKey for LegacyIVFPartitionKey {
    type ValueType = dyn VectorIndex;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("ivf-{}", self.partition_id).into()
    }

    fn type_name() -> &'static str {
        "LegacyIVFPartition"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.index.legacy-ivf-partition-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u64(self.partition_id as u64);
    }
}

/// IVF Index.
/// WARNING: Internal API with no stability guarantees.
pub struct IVFIndex {
    uuid: Uuid,

    /// Ivf model
    pub ivf: IvfModel,

    reader: Arc<dyn Reader>,

    /// Index in each partition.
    sub_index: Arc<dyn VectorIndex>,

    partition_locks: PartitionLoadLock,

    pub metric_type: MetricType,

    index_cache: WeakLanceCache,
}

impl DeepSizeOf for IVFIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        // `Uuid` is a fixed 16-byte struct with no heap children, so contributes 0.
        self.reader.deep_size_of_children(context) + self.sub_index.deep_size_of_children(context)
    }
}

impl IVFIndex {
    /// Create a new IVF index.
    pub(crate) fn try_new(
        uuid: Uuid,
        ivf: IvfModel,
        reader: Arc<dyn Reader>,
        sub_index: Arc<dyn VectorIndex>,
        metric_type: MetricType,
        index_cache: LanceCache,
    ) -> Result<Self> {
        if !sub_index.is_loadable() {
            return Err(Error::index(format!(
                "IVF sub index must be loadable, got: {:?}",
                sub_index
            )));
        }

        let num_partitions = ivf.num_partitions();
        Ok(Self {
            uuid,
            ivf,
            reader,
            sub_index,
            metric_type,
            partition_locks: PartitionLoadLock::new(num_partitions),
            index_cache: WeakLanceCache::from(&index_cache),
        })
    }

    /// Load one partition of the IVF sub-index.
    ///
    /// Internal API with no stability guarantees.
    ///
    /// Parameters
    /// ----------
    ///  - partition_id: partition ID.
    #[instrument(level = "debug", skip(self, metrics))]
    pub async fn load_partition(
        &self,
        partition_id: usize,
        write_cache: bool,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<dyn VectorIndex>> {
        let cache_key = LegacyIVFPartitionKey::new(partition_id);
        let part_index = if let Some(part_idx) =
            self.index_cache.get_unsized_with_key(&cache_key).await
        {
            part_idx
        } else {
            metrics.record_part_load();
            tracing::info!(target: TRACE_IO_EVENTS, r#type=IO_TYPE_LOAD_VECTOR_PART, index_type="ivf", part_id=cache_key.key().as_ref());

            let mtx = self.partition_locks.get_partition_mutex(partition_id);
            let _guard = mtx.lock().await;
            // check the cache again, as the partition may have been loaded by another
            // thread that held the lock on loading the partition
            if let Some(part_idx) = self.index_cache.get_unsized_with_key(&cache_key).await {
                part_idx
            } else {
                if partition_id >= self.ivf.num_partitions() {
                    return Err(Error::index(format!(
                        "partition id {} is out of range of {} partitions",
                        partition_id,
                        self.ivf.num_partitions()
                    )));
                }

                let range = self.ivf.row_range(partition_id);
                let idx = self
                    .sub_index
                    .load_partition(
                        self.reader.clone(),
                        range.start,
                        range.end - range.start,
                        partition_id,
                    )
                    .await?;
                let idx: Arc<dyn VectorIndex> = idx.into();
                if write_cache {
                    self.index_cache
                        .insert_unsized_with_key(&cache_key, idx.clone())
                        .await;
                }
                idx
            }
        };
        Ok(part_index)
    }

    /// preprocess the query vector given the partition id.
    ///
    /// Internal API with no stability guarantees.
    pub fn preprocess_query(&self, partition_id: usize, query: &Query) -> Result<Query> {
        if self.sub_index.use_residual() {
            let partition_centroids = self.ivf.centroids.as_ref().unwrap().value(partition_id);
            let residual_key = sub(&query.key, &partition_centroids)?;
            let mut part_query = query.clone();
            part_query.key = residual_key;
            Ok(part_query)
        } else {
            Ok(query.clone())
        }
    }
}

impl std::fmt::Debug for IVFIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Ivf({}) -> {:?}", self.metric_type, self.sub_index)
    }
}

#[derive(Clone, Copy, Debug)]
struct SegmentRebalanceCandidate {
    segment_id: Uuid,
    score: usize,
    created_at_ms: i64,
}

fn candidate_is_better(
    candidate: SegmentRebalanceCandidate,
    current_best: Option<SegmentRebalanceCandidate>,
) -> bool {
    match current_best {
        None => true,
        Some(current_best) => {
            candidate.score > current_best.score
                || (candidate.score == current_best.score
                    && (candidate.created_at_ms, candidate.segment_id.as_bytes())
                        < (
                            current_best.created_at_ms,
                            current_best.segment_id.as_bytes(),
                        ))
        }
    }
}

pub(crate) fn index_type_for_segmented_optimize(index: &dyn VectorIndex) -> Result<IndexType> {
    let (sub_index_type, quantization_type) = index.sub_index_type();
    IndexType::try_from(index_type_string(sub_index_type, quantization_type).as_str())
}

pub(crate) fn select_segment_for_single_rebalance(
    logical_index: &LogicalIvfView<'_>,
) -> Result<Option<Uuid>> {
    let mut best_split = None;
    let mut best_join = None;

    for (metadata, index) in logical_index.segments() {
        let index_type = index_type_for_segmented_optimize(index.as_ref())?;
        let split_threshold = MAX_PARTITION_SIZE_FACTOR * index_type.target_partition_size();
        let join_threshold = MIN_PARTITION_SIZE_PERCENT * index_type.target_partition_size() / 100;
        let num_partitions = index.ivf_model().num_partitions();
        if num_partitions == 0 {
            continue;
        }

        let mut split_partition_count = 0usize;
        let mut join_partition_count = 0usize;
        for partition_id in 0..num_partitions {
            let partition_size = index.partition_size(partition_id);
            if partition_size > split_threshold {
                split_partition_count += 1;
            }
            if num_partitions > 1 && partition_size < join_threshold {
                join_partition_count += 1;
            }
        }

        let created_at_ms = metadata
            .created_at
            .map(|dt| dt.timestamp_millis())
            .unwrap_or(i64::MIN);

        let split_candidate = (split_partition_count > 0).then_some(SegmentRebalanceCandidate {
            segment_id: metadata.uuid,
            score: split_partition_count,
            created_at_ms,
        });
        if let Some(candidate) = split_candidate
            && candidate_is_better(candidate, best_split)
        {
            best_split = Some(candidate);
        }

        let join_candidate = (join_partition_count > 0).then_some(SegmentRebalanceCandidate {
            segment_id: metadata.uuid,
            score: join_partition_count,
            created_at_ms,
        });
        if let Some(candidate) = join_candidate
            && candidate_is_better(candidate, best_join)
        {
            best_join = Some(candidate);
        }
    }

    let selected = best_split.or(best_join);
    Ok(selected.map(|candidate| candidate.segment_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum VectorSegmentCompatibility {
    SharedModel,
    QueryCompatibleModelsDiffer,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum VectorModelMismatch {
    StorageFormat,
    IvfCentroids,
    QuantizerMetadata,
}

fn vector_index_dimension(index: &dyn VectorIndex) -> usize {
    let ivf_dimension = index.ivf_model().dimension();
    if ivf_dimension != 0 {
        return ivf_dimension;
    }

    match index.quantizer() {
        Quantizer::Flat(quantizer) => quantizer.metadata(None).dim,
        Quantizer::FlatBin(quantizer) => quantizer.metadata(None).dim,
        Quantizer::Product(quantizer) => quantizer.dimension,
        Quantizer::Scalar(quantizer) => quantizer.metadata(None).dim,
        Quantizer::Rabit(quantizer) => quantizer.metadata(None).rotated_dim(),
    }
}

fn validate_vector_query_compatibility(
    indices: &[Arc<dyn VectorIndex>],
    operation: &str,
) -> Result<()> {
    let Some(first) = indices.first() else {
        return Ok(());
    };

    let first_metric = first.metric_type();
    let first_dimension = vector_index_dimension(first.as_ref());
    let first_index_type = first.sub_index_type();
    let first_quantizer = first.quantizer();
    let first_quantizer_type = first_quantizer.quantization_type();

    for (idx, index) in indices.iter().enumerate().skip(1) {
        if index.metric_type() != first_metric {
            return Err(Error::index(format!(
                "{operation}: vector index segment {idx} has metric {:?}, expected {:?}",
                index.metric_type(),
                first_metric
            )));
        }
        let dimension = vector_index_dimension(index.as_ref());
        if dimension != first_dimension {
            return Err(Error::index(format!(
                "{operation}: vector index segment {idx} has dimension {dimension}, expected {first_dimension}"
            )));
        }
        let index_type = index.sub_index_type();
        if std::mem::discriminant(&index_type.0) != std::mem::discriminant(&first_index_type.0)
            || index_type.1 != first_index_type.1
        {
            return Err(Error::index(format!(
                "{operation}: vector index segment {idx} has type {:?}, expected {:?}",
                index_type, first_index_type
            )));
        }

        let quantizer = index.quantizer();
        if quantizer.quantization_type() != first_quantizer_type {
            return Err(Error::index(format!(
                "{operation}: vector index segment {idx} has quantizer {:?}, expected {:?}",
                quantizer.quantization_type(),
                first_quantizer_type
            )));
        }
    }

    Ok(())
}

fn vector_model_mismatch(indices: &[Arc<dyn VectorIndex>]) -> Option<VectorModelMismatch> {
    let first = indices.first()?;
    let first_centroids = first.ivf_model().centroids_array();
    let first_quantizer = first.quantizer();

    for index in indices.iter().skip(1) {
        if first.as_any().type_id() != index.as_any().type_id() {
            return Some(VectorModelMismatch::StorageFormat);
        }
        match (first_centroids, index.ivf_model().centroids_array()) {
            (Some(expected), Some(actual)) if expected.to_data() != actual.to_data() => {
                return Some(VectorModelMismatch::IvfCentroids);
            }
            (Some(_), None) | (None, Some(_)) => {
                return Some(VectorModelMismatch::IvfCentroids);
            }
            _ => {}
        }

        if !shared_quantizer_model(&first_quantizer, &index.quantizer()) {
            return Some(VectorModelMismatch::QuantizerMetadata);
        }
    }

    None
}

pub(crate) fn vector_segment_compatibility(
    logical_index: &LogicalIvfView<'_>,
    operation: &str,
) -> Result<VectorSegmentCompatibility> {
    let indices = logical_index.indices().cloned().collect::<Vec<_>>();
    validate_vector_query_compatibility(&indices, operation)?;
    Ok(if vector_model_mismatch(&indices).is_none() {
        VectorSegmentCompatibility::SharedModel
    } else {
        VectorSegmentCompatibility::QueryCompatibleModelsDiffer
    })
}

fn validate_shared_vector_model(indices: &[Arc<dyn VectorIndex>], operation: &str) -> Result<()> {
    validate_vector_query_compatibility(indices, operation)?;
    match vector_model_mismatch(indices) {
        Some(VectorModelMismatch::StorageFormat) => Err(Error::index(format!(
            "{operation}: vector index segments do not share a storage format"
        ))),
        Some(VectorModelMismatch::IvfCentroids) => Err(Error::index(format!(
            "{operation}: vector index segments do not share IVF centroids"
        ))),
        Some(VectorModelMismatch::QuantizerMetadata) => Err(Error::index(format!(
            "{operation}: vector index segments do not share quantizer metadata"
        ))),
        None => Ok(()),
    }
}

fn shared_quantizer_model(left: &Quantizer, right: &Quantizer) -> bool {
    match (left, right) {
        (Quantizer::Flat(left), Quantizer::Flat(right)) => {
            left.metadata(None).dim == right.metadata(None).dim
        }
        (Quantizer::FlatBin(left), Quantizer::FlatBin(right)) => {
            left.metadata(None).dim == right.metadata(None).dim
        }
        (Quantizer::Product(left), Quantizer::Product(right)) => {
            left.num_sub_vectors == right.num_sub_vectors
                && left.num_bits == right.num_bits
                && left.dimension == right.dimension
                && left.distance_type == right.distance_type
                && left.codebook.to_data() == right.codebook.to_data()
        }
        (Quantizer::Scalar(left), Quantizer::Scalar(right)) => {
            left.metadata(None) == right.metadata(None)
        }
        (Quantizer::Rabit(left), Quantizer::Rabit(right)) => {
            let left = left.metadata(None);
            let right = right.metadata(None);
            left.rotation_type == right.rotation_type
                && left.code_dim == right.code_dim
                && left.num_bits == right.num_bits
                && left.packed == right.packed
                && left.query_estimator == right.query_estimator
                && left.fast_rotation_signs == right.fast_rotation_signs
                && match (&left.rotate_mat, &right.rotate_mat) {
                    (Some(left), Some(right)) => left.to_data() == right.to_data(),
                    (None, None) => true,
                    _ => false,
                }
        }
        _ => false,
    }
}

// TODO: move to `lance-index` crate.
///
/// Returns (new_uuid, num_indices_merged, files)
pub(crate) async fn optimize_vector_indices(
    dataset: Dataset,
    unindexed: Option<impl RecordBatchStream + Unpin + 'static>,
    vector_column: &str,
    logical_index: &LogicalIvfView<'_>,
    options: &OptimizeOptions,
) -> Result<(Uuid, usize, Vec<IndexFile>)> {
    let existing_indices = logical_index.indices().cloned().collect::<Vec<_>>();
    // Sanity check the indices
    if existing_indices.is_empty() {
        return Err(Error::index(
            "optimizing vector index: no existing index found".to_string(),
        ));
    }
    validate_shared_vector_model(&existing_indices, "optimizing vector index")?;

    // try cast to v1 IVFIndex,
    // fallback to v2 IVFIndex if it's not v1 IVFIndex
    if !existing_indices[0].as_any().is::<IVFIndex>() {
        return optimize_vector_indices_v2(
            &dataset,
            unindexed,
            vector_column,
            &existing_indices,
            options,
        )
        .await;
    }

    let new_uuid = Uuid::new_v4();
    let object_store = dataset.object_store.as_ref();
    let index_file = dataset
        .indices_dir()
        .join(new_uuid.to_string())
        .join(INDEX_FILE_NAME);
    let writer = object_store.create(&index_file).await?;

    let first_idx = existing_indices[0]
        .as_any()
        .downcast_ref::<IVFIndex>()
        .ok_or(Error::index(
            "optimizing vector index: the first index isn't IVF".to_string(),
        ))?;

    let (merged, files) =
        if let Some(pq_index) = first_idx.sub_index.as_any().downcast_ref::<PQIndex>() {
            let (merged, file) = optimize_ivf_pq_indices(
                first_idx,
                pq_index,
                vector_column,
                unindexed,
                &existing_indices,
                options,
                writer,
                dataset.version().version,
            )
            .await?;
            (merged, vec![file])
        } else if let Some(hnsw_sq) = first_idx
            .sub_index
            .as_any()
            .downcast_ref::<HNSWIndex<ScalarQuantizer>>()
        {
            let aux_file = dataset
                .indices_dir()
                .join(new_uuid.to_string())
                .join(INDEX_AUXILIARY_FILE_NAME);
            let aux_writer = object_store.create(&aux_file).await?;
            optimize_ivf_hnsw_indices(
                Arc::new(dataset),
                first_idx,
                hnsw_sq,
                vector_column,
                unindexed,
                &existing_indices,
                options,
                writer,
                aux_writer,
            )
            .await?
        } else {
            return Err(Error::index(
                "optimizing vector index: the sub index isn't PQ or HNSW".to_string(),
            ));
        };

    // never change the index version,
    // because we won't update the legacy vector index format
    Ok((new_uuid, merged, files))
}

pub(crate) async fn optimize_vector_indices_v2(
    dataset: &Dataset,
    unindexed: Option<impl RecordBatchStream + Unpin + 'static>,
    vector_column: &str,
    existing_indices: &[Arc<dyn VectorIndex>],
    options: &OptimizeOptions,
) -> Result<(Uuid, usize, Vec<IndexFile>)> {
    // Sanity check the indices
    if existing_indices.is_empty() {
        return Err(Error::index(
            "optimizing vector index: no existing index found".to_string(),
        ));
    }
    let existing_indices = existing_indices.to_vec();

    let new_uuid = Uuid::new_v4();
    let index_dir = dataset.indices_dir().join(new_uuid.to_string());
    let ivf_model = existing_indices[0].ivf_model();
    let quantizer = existing_indices[0].quantizer();
    let distance_type = existing_indices[0].metric_type();
    let num_partitions = ivf_model.num_partitions();
    let index_type = existing_indices[0].sub_index_type();
    let frag_reuse_index = dataset.open_frag_reuse_index(&NoOpMetricsCollector).await?;

    let format_version = dataset_format_version(dataset);

    let temp_dir = lance_core::utils::tempfile::TempStdDir::default();
    let temp_dir_path = Path::from_filesystem_path(&temp_dir)?;
    let shuffler = create_ivf_shuffler(temp_dir_path, num_partitions, format_version, None);

    let (_, element_type) = get_vector_type(dataset.schema(), vector_column)?;
    let summary = match index_type {
        // IVF_FLAT
        (SubIndexType::Flat, QuantizationType::Flat) => {
            if element_type == DataType::UInt8 {
                IvfIndexBuilder::<FlatIndex, FlatBinQuantizer>::new_incremental(
                    dataset.clone(),
                    vector_column.to_owned(),
                    index_dir,
                    distance_type,
                    shuffler,
                    (),
                    frag_reuse_index,
                    options.clone(),
                )?
                .with_ivf(ivf_model.clone())
                .with_quantizer(quantizer.try_into()?)
                .with_existing_indices(existing_indices.clone())
                .with_progress(options.progress.clone())
                .shuffle_data_input(unindexed)
                .build()
                .await?
            } else {
                IvfIndexBuilder::<FlatIndex, FlatQuantizer>::new_incremental(
                    dataset.clone(),
                    vector_column.to_owned(),
                    index_dir,
                    distance_type,
                    shuffler,
                    (),
                    frag_reuse_index,
                    options.clone(),
                )?
                .with_ivf(ivf_model.clone())
                .with_quantizer(quantizer.try_into()?)
                .with_existing_indices(existing_indices.clone())
                .with_progress(options.progress.clone())
                .shuffle_data_input(unindexed)
                .build()
                .await?
            }
        }
        // IVF_FLAT (binary vectors)
        (SubIndexType::Flat, QuantizationType::FlatBin) => {
            IvfIndexBuilder::<FlatIndex, FlatBinQuantizer>::new_incremental(
                dataset.clone(),
                vector_column.to_owned(),
                index_dir,
                distance_type,
                shuffler,
                (),
                frag_reuse_index,
                options.clone(),
            )?
            .with_ivf(ivf_model.clone())
            .with_quantizer(quantizer.try_into()?)
            .with_existing_indices(existing_indices.clone())
            .with_progress(options.progress.clone())
            .shuffle_data_input(unindexed)
            .build()
            .await?
        }
        // IVF_PQ
        (SubIndexType::Flat, QuantizationType::Product) => {
            IvfIndexBuilder::<FlatIndex, ProductQuantizer>::new_incremental(
                dataset.clone(),
                vector_column.to_owned(),
                index_dir,
                distance_type,
                shuffler,
                (),
                frag_reuse_index,
                options.clone(),
            )?
            .with_ivf(ivf_model.clone())
            .with_quantizer(quantizer.try_into()?)
            .with_existing_indices(existing_indices.clone())
            .with_progress(options.progress.clone())
            .shuffle_data_input(unindexed)
            .build()
            .await?
        }
        // IVF_SQ
        (SubIndexType::Flat, QuantizationType::Scalar) => {
            IvfIndexBuilder::<FlatIndex, ScalarQuantizer>::new_incremental(
                dataset.clone(),
                vector_column.to_owned(),
                index_dir,
                distance_type,
                shuffler,
                (),
                frag_reuse_index,
                options.clone(),
            )?
            .with_ivf(ivf_model.clone())
            .with_quantizer(quantizer.try_into()?)
            .with_existing_indices(existing_indices.clone())
            .with_progress(options.progress.clone())
            .shuffle_data_input(unindexed)
            .build()
            .await?
        }
        (SubIndexType::Flat, QuantizationType::Rabit) => {
            IvfIndexBuilder::<FlatIndex, RabitQuantizer>::new_incremental(
                dataset.clone(),
                vector_column.to_owned(),
                index_dir,
                distance_type,
                shuffler,
                (),
                frag_reuse_index,
                options.clone(),
            )?
            .with_ivf(ivf_model.clone())
            .with_quantizer(quantizer.try_into()?)
            .with_existing_indices(existing_indices.clone())
            .with_progress(options.progress.clone())
            .shuffle_data_input(unindexed)
            .build()
            .await?
        }
        // IVF_HNSW_FLAT
        (SubIndexType::Hnsw, QuantizationType::Flat) => {
            if element_type == DataType::UInt8 {
                IvfIndexBuilder::<HNSW, FlatBinQuantizer>::new_incremental(
                    dataset.clone(),
                    vector_column.to_owned(),
                    index_dir,
                    distance_type,
                    shuffler,
                    derive_hnsw_params(existing_indices[0].as_ref()),
                    frag_reuse_index,
                    options.clone(),
                )?
                .with_ivf(ivf_model.clone())
                .with_quantizer(quantizer.try_into()?)
                .with_existing_indices(existing_indices.clone())
                .with_progress(options.progress.clone())
                .shuffle_data_input(unindexed)
                .build()
                .await?
            } else {
                IvfIndexBuilder::<HNSW, FlatQuantizer>::new_incremental(
                    dataset.clone(),
                    vector_column.to_owned(),
                    index_dir,
                    distance_type,
                    shuffler,
                    derive_hnsw_params(existing_indices[0].as_ref()),
                    frag_reuse_index,
                    options.clone(),
                )?
                .with_ivf(ivf_model.clone())
                .with_quantizer(quantizer.try_into()?)
                .with_existing_indices(existing_indices.clone())
                .with_progress(options.progress.clone())
                .shuffle_data_input(unindexed)
                .build()
                .await?
            }
        }
        // IVF_HNSW_SQ
        (SubIndexType::Hnsw, QuantizationType::Scalar) => {
            IvfIndexBuilder::<HNSW, ScalarQuantizer>::new_incremental(
                dataset.clone(),
                vector_column.to_owned(),
                index_dir,
                distance_type,
                shuffler,
                derive_hnsw_params(existing_indices[0].as_ref()),
                frag_reuse_index,
                options.clone(),
            )?
            .with_ivf(ivf_model.clone())
            .with_quantizer(quantizer.try_into()?)
            .with_existing_indices(existing_indices.clone())
            .with_progress(options.progress.clone())
            .shuffle_data_input(unindexed)
            .build()
            .await?
        }
        // IVF_HNSW_PQ
        (SubIndexType::Hnsw, QuantizationType::Product) => {
            IvfIndexBuilder::<HNSW, ProductQuantizer>::new_incremental(
                dataset.clone(),
                vector_column.to_owned(),
                index_dir,
                distance_type,
                shuffler,
                derive_hnsw_params(existing_indices[0].as_ref()),
                frag_reuse_index,
                options.clone(),
            )?
            .with_ivf(ivf_model.clone())
            .with_quantizer(quantizer.try_into()?)
            .with_existing_indices(existing_indices.clone())
            .with_progress(options.progress.clone())
            .shuffle_data_input(unindexed)
            .build()
            .await?
        }
        (sub_index_type, quantization_type) => {
            unimplemented!(
                "unsupported index type: {}, {}",
                sub_index_type,
                quantization_type
            )
        }
    };

    Ok((new_uuid, summary.indices_merged, summary.files))
}

#[allow(clippy::too_many_arguments)]
async fn optimize_ivf_pq_indices(
    first_idx: &IVFIndex,
    pq_index: &PQIndex,
    vector_column: &str,
    unindexed: Option<impl RecordBatchStream + Unpin + 'static>,
    existing_indices: &[Arc<dyn VectorIndex>],
    options: &OptimizeOptions,
    mut writer: Box<dyn Writer>,
    dataset_version: u64,
) -> Result<(usize, IndexFile)> {
    let metric_type = first_idx.metric_type;
    let dim = first_idx.ivf.dimension();

    // TODO: merge `lance::vector::ivf::IVF` and `lance-index::vector::ivf::Ivf`` implementations.
    let ivf = lance_index::vector::ivf::IvfTransformer::with_pq(
        first_idx.ivf.centroids.clone().unwrap(),
        metric_type,
        vector_column,
        pq_index.pq.clone(),
        None,
    );

    // Shuffled un-indexed data with partition.
    let shuffled = match unindexed {
        Some(unindexed) => Some(
            shuffle_dataset(
                unindexed,
                ivf.into(),
                None,
                first_idx.ivf.num_partitions() as u32,
                10000,
                2,
                None,
            )
            .await?,
        ),
        None => None,
    };

    let mut ivf_mut = IvfModel::new(first_idx.ivf.centroids.clone().unwrap(), first_idx.ivf.loss);

    let start_pos = existing_indices
        .len()
        .saturating_sub(options.num_indices_to_merge.unwrap_or(1));

    let indices_to_merge = existing_indices[start_pos..]
        .iter()
        .map(|idx| {
            idx.as_any().downcast_ref::<IVFIndex>().ok_or(Error::index(
                "optimizing vector index: it is not a IVF index".to_string(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    write_pq_partitions(
        writer.as_mut(),
        &mut ivf_mut,
        shuffled,
        Some(&indices_to_merge),
    )
    .await?;
    let metadata = IvfPQIndexMetadata {
        name: format!("_{}_idx", vector_column),
        column: vector_column.to_string(),
        dimension: dim as u32,
        dataset_version,
        metric_type,
        ivf: ivf_mut,
        pq: pq_index.pq.clone(),
        transforms: vec![],
    };

    let metadata = pb::Index::try_from(&metadata)?;
    let pos = writer.write_protobuf(&metadata).await?;
    // TODO: for now the IVF_PQ index file format hasn't been updated, so keep the old version,
    // change it to latest version value after refactoring the IVF_PQ
    writer.write_magics(pos, 0, 1, MAGIC).await?;
    let size_bytes = writer.tell().await? as u64;
    Writer::shutdown(writer.as_mut()).await?;

    Ok((
        existing_indices.len() - start_pos,
        IndexFile {
            path: INDEX_FILE_NAME.to_string(),
            size_bytes,
        },
    ))
}

#[allow(clippy::too_many_arguments)]
async fn optimize_ivf_hnsw_indices<Q: Quantization>(
    dataset: Arc<dyn DatasetTakeRows>,
    first_idx: &IVFIndex,
    hnsw_index: &HNSWIndex<Q>,
    vector_column: &str,
    unindexed: Option<impl RecordBatchStream + Unpin + 'static>,
    existing_indices: &[Arc<dyn VectorIndex>],
    options: &OptimizeOptions,
    writer: Box<dyn Writer>,
    aux_writer: Box<dyn Writer>,
) -> Result<(usize, Vec<IndexFile>)> {
    let distance_type = first_idx.metric_type;
    let quantizer = hnsw_index.quantizer().clone();
    let ivf = lance_index::vector::ivf::new_ivf_transformer_with_quantizer(
        first_idx.ivf.centroids.clone().unwrap(),
        distance_type,
        vector_column,
        quantizer.clone(),
        None,
    )?;

    // Shuffled un-indexed data with partition.
    let unindexed_data = match unindexed {
        Some(unindexed) => Some(
            shuffle_dataset(
                unindexed,
                Arc::new(ivf),
                None,
                first_idx.ivf.num_partitions() as u32,
                10000,
                2,
                None,
            )
            .await?,
        ),
        None => None,
    };

    let mut ivf_mut = IvfModel::new(first_idx.ivf.centroids.clone().unwrap(), first_idx.ivf.loss);

    let num_to_merge = options.num_indices_to_merge.unwrap_or(1);
    let start_pos = if num_to_merge > existing_indices.len() {
        0
    } else {
        existing_indices.len() - num_to_merge
    };

    let indices_to_merge = existing_indices[start_pos..]
        .iter()
        .map(|idx| {
            idx.as_any().downcast_ref::<IVFIndex>().ok_or(Error::index(
                "optimizing vector index: it is not a IVF index".to_string(),
            ))
        })
        .collect::<Result<Vec<_>>>()?;

    // Prepare the HNSW writer
    let schema = lance_core::datatypes::Schema::try_from(HNSW::schema().as_ref())?;
    let mut writer =
        V1FileWriter::with_object_writer(writer, schema, &V1FileWriterOptions::default())?;
    writer.add_metadata(
        INDEX_METADATA_SCHEMA_KEY,
        json!(IndexMetadata {
            index_type: format!("IVF_HNSW_{}", quantizer.quantization_type()),
            distance_type: distance_type.to_string(),
        })
        .to_string()
        .as_str(),
    );

    // Prepare the quantization storage writer
    let schema = Schema::new(vec![
        ROW_ID_FIELD.clone(),
        arrow_schema::Field::new(
            quantizer.column(),
            DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new("item", DataType::UInt8, true)),
                quantizer.code_dim() as i32,
            ),
            false,
        ),
    ]);
    let schema = lance_core::datatypes::Schema::try_from(&schema)?;
    let mut aux_writer =
        V1FileWriter::with_object_writer(aux_writer, schema, &V1FileWriterOptions::default())?;
    aux_writer.add_metadata(
        INDEX_METADATA_SCHEMA_KEY,
        json!(IndexMetadata {
            index_type: quantizer.quantization_type().to_string(),
            distance_type: distance_type.to_string(),
        })
        .to_string()
        .as_str(),
    );

    // Write the metadata of quantizer
    let quantization_metadata = match &quantizer {
        Quantizer::Product(pq) => {
            let codebook_tensor = pb::Tensor::try_from(&pq.codebook)?;
            let codebook_pos = aux_writer.tell().await?;
            aux_writer
                .object_writer
                .write_protobuf(&codebook_tensor)
                .await?;

            Some(QuantizationMetadata {
                codebook_position: Some(codebook_pos),
                ..Default::default()
            })
        }
        _ => None,
    };

    aux_writer.add_metadata(
        quantizer.metadata_key(),
        quantizer
            .metadata(quantization_metadata)?
            .to_string()
            .as_str(),
    );

    let hnsw_params = &hnsw_index.metadata().params;
    let (hnsw_metadata, aux_ivf) = write_hnsw_quantization_index_partitions(
        dataset,
        vector_column,
        distance_type,
        hnsw_params,
        &mut writer,
        Some(&mut aux_writer),
        &mut ivf_mut,
        quantizer,
        unindexed_data,
        Some(&indices_to_merge),
    )
    .await?;

    // Add the metadata of HNSW partitions
    let hnsw_metadata_json = json!(hnsw_metadata);
    writer.add_metadata(IVF_PARTITION_KEY, &hnsw_metadata_json.to_string());

    ivf_mut.write(&mut writer).await?;
    // `finish` writes the footer and returns the authoritative on-disk size.
    let index_size = writer.finish().await?.size_bytes;

    // Write the aux file
    aux_ivf.write(&mut aux_writer).await?;
    let aux_size = aux_writer.finish().await?.size_bytes;

    Ok((
        existing_indices.len() - start_pos,
        vec![
            IndexFile {
                path: INDEX_FILE_NAME.to_string(),
                size_bytes: index_size,
            },
            IndexFile {
                path: INDEX_AUXILIARY_FILE_NAME.to_string(),
                size_bytes: aux_size,
            },
        ],
    ))
}

#[derive(Serialize)]
pub struct IvfIndexPartitionStatistics {
    size: u32,
}

#[derive(Serialize)]
pub struct IvfIndexStatistics {
    index_type: String,
    uuid: String,
    uri: String,
    metric_type: String,
    num_partitions: usize,
    sub_index: serde_json::Value,
    partitions: Vec<IvfIndexPartitionStatistics>,
    #[serde(skip_serializing_if = "Option::is_none")]
    centroids: Option<Vec<Vec<f32>>>,
    loss: Option<f64>,
    index_file_version: IndexFileVersion,
}

/// Environment variable controlling whether vector index statistics include
/// the centroid vectors. When unset, centroids are still included for
/// backward compatibility, but a one-time warning is logged. Set to a truthy
/// value (e.g. `1`, `true`) to keep the current behavior without the warning,
/// or any other value (e.g. `0`, `false`) to omit centroids from stats.
pub const LANCE_INCLUDE_VECTOR_CENTROIDS_ENV: &str = "LANCE_INCLUDE_VECTOR_CENTROIDS";

/// Read the centroids for inclusion in index stats, honoring
/// `LANCE_INCLUDE_VECTOR_CENTROIDS`.
///
/// - If the env var is set to a truthy value (per `parse_env_as_bool`),
///   returns the converted centroids.
/// - If the env var is set to any other value, returns `Ok(None)` without
///   reading the centroids.
/// - If unset, returns the converted centroids and logs a one-time
///   deprecation warning that the default will change in a future release.
pub(crate) fn maybe_centroids_for_stats(
    centroids: &FixedSizeListArray,
) -> Result<Option<Vec<Vec<f32>>>> {
    use std::sync::Once;
    static WARN_ONCE: Once = Once::new();

    if std::env::var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV).is_err() {
        WARN_ONCE.call_once(|| {
            warn!(
                "Vector index statistics currently include centroids, which can use \
                 significant memory for large indexes. In a future release, centroids \
                 will be excluded from statistics by default. Set {}=true to preserve \
                 the current behavior (and silence this warning), or {}=false to opt \
                 in to the new behavior now.",
                LANCE_INCLUDE_VECTOR_CENTROIDS_ENV, LANCE_INCLUDE_VECTOR_CENTROIDS_ENV
            );
        });
    }
    if !parse_env_as_bool(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV, true) {
        return Ok(None);
    }
    Ok(Some(centroids_to_vectors(centroids)?))
}

fn centroids_to_vectors(centroids: &FixedSizeListArray) -> Result<Vec<Vec<f32>>> {
    centroids
        .iter()
        .map(|v| {
            if let Some(row) = v {
                match row.data_type() {
                    DataType::Float16 => Ok(row
                        .as_primitive::<Float16Type>()
                        .values()
                        .iter()
                        .map(|v| v.to_f32())
                        .collect::<Vec<_>>()),
                    DataType::Float32 => Ok(row.as_primitive::<Float32Type>().values().to_vec()),
                    DataType::Float64 => Ok(row
                        .as_primitive::<Float64Type>()
                        .values()
                        .iter()
                        .map(|v| *v as f32)
                        .collect::<Vec<_>>()),
                    DataType::UInt8 => Ok(row
                        .as_primitive::<UInt8Type>()
                        .values()
                        .iter()
                        .map(|v| *v as f32)
                        .collect::<Vec<_>>()),
                    _ => Err(Error::index(format!(
                        "IVF centroids must be FixedSizeList of floating number, got: {}",
                        row.data_type()
                    ))),
                }
            } else {
                Err(Error::index("Invalid centroid".to_string()))
            }
        })
        .collect()
}

#[async_trait]
impl Index for IVFIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    fn index_type(&self) -> IndexType {
        if self.sub_index.as_any().downcast_ref::<PQIndex>().is_some() {
            IndexType::IvfPq
        } else if self
            .sub_index
            .as_any()
            .downcast_ref::<HNSWIndex<ScalarQuantizer>>()
            .is_some()
        {
            IndexType::IvfHnswSq
        } else if self
            .sub_index
            .as_any()
            .downcast_ref::<HNSWIndex<ProductQuantizer>>()
            .is_some()
        {
            IndexType::IvfHnswPq
        } else {
            IndexType::Vector
        }
    }

    async fn prewarm(&self) -> Result<()> {
        futures::stream::iter(0..self.ivf.num_partitions())
            .map(Ok)
            .try_for_each_concurrent(Some(self.reader.io_parallelism()), |part_id| {
                self.load_partition(part_id, true, &NoOpMetricsCollector)
                    .map_ok(|_| ())
            })
            .await
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        let partitions_statistics = (0..self.ivf.num_partitions())
            .map(|part_id| IvfIndexPartitionStatistics {
                size: self.ivf.partition_size(part_id) as u32,
            })
            .collect::<Vec<_>>();

        let centroid_vecs = maybe_centroids_for_stats(self.ivf.centroids.as_ref().unwrap())?;

        Ok(serde_json::to_value(IvfIndexStatistics {
            index_type: self.index_type().to_string(),
            uuid: self.uuid.to_string(),
            uri: to_local_path(self.reader.path()),
            metric_type: self.metric_type.to_string(),
            num_partitions: self.ivf.num_partitions(),
            sub_index: self.sub_index.statistics()?,
            partitions: partitions_statistics,
            centroids: centroid_vecs,
            loss: self.ivf.loss(),
            index_file_version: IndexFileVersion::Legacy,
        })?)
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        let mut frag_ids = RoaringBitmap::default();
        let part_ids = 0..self.ivf.num_partitions();
        for part_id in part_ids {
            let part = self
                .load_partition(part_id, false, &NoOpMetricsCollector)
                .await?;
            frag_ids |= part.calculate_included_frags().await?;
        }
        Ok(frag_ids)
    }
}

#[async_trait]
impl VectorIndex for IVFIndex {
    #[instrument(level = "debug", skip_all, name = "IVFIndex::search")]
    async fn search(
        &self,
        _query: &Query,
        _pre_filter: Arc<dyn PreFilter>,
        _metrics: &dyn MetricsCollector,
    ) -> Result<RecordBatch> {
        unimplemented!(
            "IVFIndex not currently used as sub-index and top-level indices do partition-aware search"
        )
    }

    /// find the IVF partitions ids given the query vector.
    ///
    /// Internal API with no stability guarantees.
    ///
    /// Assumes the query vector is normalized if the metric type is cosine.
    fn find_partitions(&self, query: &Query) -> Result<(UInt32Array, Float32Array)> {
        let mt = if self.metric_type == MetricType::Cosine {
            MetricType::L2
        } else {
            self.metric_type
        };

        let max_nprobes = query.maximum_nprobes.unwrap_or(self.ivf.num_partitions());

        self.ivf.find_partitions(&query.key, max_nprobes, mt)
    }

    async fn search_in_partition(
        &self,
        partition_id: usize,
        query: &Query,
        pre_filter: Arc<dyn PreFilter>,
        metrics: &dyn MetricsCollector,
    ) -> Result<RecordBatch> {
        let part_index = self.load_partition(partition_id, true, metrics).await?;

        let query = self.preprocess_query(partition_id, query)?;
        let batch = part_index.search(&query, pre_filter, metrics).await?;
        Ok(batch)
    }

    fn total_partitions(&self) -> usize {
        self.ivf.num_partitions()
    }

    fn is_loadable(&self) -> bool {
        false
    }

    fn use_residual(&self) -> bool {
        false
    }

    async fn load(
        &self,
        _reader: Arc<dyn Reader>,
        _offset: usize,
        _length: usize,
    ) -> Result<Box<dyn VectorIndex>> {
        Err(Error::index("Flat index does not support load".to_string()))
    }

    async fn partition_reader(
        &self,
        partition_id: usize,
        with_vector: bool,
        metrics: &dyn MetricsCollector,
    ) -> Result<SendableRecordBatchStream> {
        let partition = self.load_partition(partition_id, false, metrics).await?;
        partition.to_batch_stream(with_vector).await
    }

    async fn to_batch_stream(&self, _with_vector: bool) -> Result<SendableRecordBatchStream> {
        unimplemented!("this method is for only sub index")
    }

    fn num_rows(&self) -> u64 {
        self.ivf.num_rows()
    }

    fn row_ids(&self) -> Box<dyn Iterator<Item = &u64>> {
        todo!("this method is for only IVF_HNSW_* index");
    }

    async fn remap(&mut self, _mapping: &RowAddrRemap) -> Result<()> {
        // This will be needed if we want to clean up IVF to allow more than just
        // one layer (e.g. IVF -> IVF -> PQ).  We need to pass on the call to
        // remap to the lower layers.

        // Currently, remapping for IVF is implemented in remap_index_file which
        // mirrors some of the other IVF routines like build_ivf_pq_index
        Err(Error::index(
            "Remapping IVF in this way not supported".to_string(),
        ))
    }

    fn ivf_model(&self) -> &IvfModel {
        &self.ivf
    }

    fn quantizer(&self) -> Quantizer {
        unimplemented!("only for v2 IVFIndex")
    }

    fn partition_size(&self, part_id: usize) -> usize {
        self.ivf.partition_size(part_id)
    }

    /// the index type of this vector index.
    fn sub_index_type(&self) -> (SubIndexType, QuantizationType) {
        unimplemented!("only for v2 IVFIndex")
    }

    fn metric_type(&self) -> MetricType {
        self.metric_type
    }
}

/// Ivf PQ index metadata.
///
/// It contains the on-disk data for a IVF PQ index.
#[derive(Debug)]
pub struct IvfPQIndexMetadata {
    /// Index name
    name: String,

    /// The column to build the index for.
    column: String,

    /// Vector dimension.
    dimension: u32,

    /// The version of dataset where this index was built.
    dataset_version: u64,

    /// Metric to compute distance
    pub(crate) metric_type: MetricType,

    /// IVF model
    pub(crate) ivf: IvfModel,

    /// Product Quantizer
    pub(crate) pq: ProductQuantizer,

    /// Transforms to be applied before search.
    transforms: Vec<pb::Transform>,
}

impl IvfPQIndexMetadata {
    /// Create a new IvfPQIndexMetadata object
    pub fn new(
        name: String,
        column: String,
        dataset_version: u64,
        metric_type: MetricType,
        ivf: IvfModel,
        pq: ProductQuantizer,
        transforms: Vec<pb::Transform>,
    ) -> Self {
        let dimension = ivf.dimension() as u32;
        Self {
            name,
            column,
            dimension,
            dataset_version,
            metric_type,
            ivf,
            pq,
            transforms,
        }
    }
}

/// Convert a IvfPQIndex to protobuf payload
impl TryFrom<&IvfPQIndexMetadata> for pb::Index {
    type Error = Error;

    fn try_from(idx: &IvfPQIndexMetadata) -> Result<Self> {
        let mut stages: Vec<pb::VectorIndexStage> = idx
            .transforms
            .iter()
            .map(|tf| {
                Ok(pb::VectorIndexStage {
                    stage: Some(pb::vector_index_stage::Stage::Transform(tf.clone())),
                })
            })
            .collect::<Result<Vec<_>>>()?;

        stages.extend_from_slice(&[
            pb::VectorIndexStage {
                stage: Some(pb::vector_index_stage::Stage::Ivf(pb::Ivf::try_from(
                    &idx.ivf,
                )?)),
            },
            pb::VectorIndexStage {
                stage: Some(pb::vector_index_stage::Stage::Pq(pb::Pq::try_from(
                    &idx.pq,
                )?)),
            },
        ]);

        Ok(Self {
            name: idx.name.clone(),
            columns: vec![idx.column.clone()],
            dataset_version: idx.dataset_version,
            index_type: pb::IndexType::Vector.into(),
            implementation: Some(pb::index::Implementation::VectorIndex(pb::VectorIndex {
                spec_version: 1,
                dimension: idx.dimension,
                stages,
                metric_type: match idx.metric_type {
                    MetricType::L2 => pb::VectorMetricType::L2.into(),
                    MetricType::Cosine => pb::VectorMetricType::Cosine.into(),
                    MetricType::Dot => pb::VectorMetricType::Dot.into(),
                    MetricType::Hamming => pb::VectorMetricType::Hamming.into(),
                },
            })),
        })
    }
}

fn sanity_check_ivf_params(ivf: &IvfBuildParams) -> Result<()> {
    if ivf.precomputed_partitions_file.is_some() && ivf.centroids.is_none() {
        return Err(Error::index(
            "precomputed_partitions_file requires centroids to be set".to_string(),
        ));
    }

    if ivf.precomputed_shuffle_buffers.is_some() && ivf.centroids.is_none() {
        return Err(Error::index(
            "precomputed_shuffle_buffers requires centroids to be set".to_string(),
        ));
    }

    if ivf.precomputed_shuffle_buffers.is_some() && ivf.precomputed_partitions_file.is_some() {
        return Err(Error::index(
            "precomputed_shuffle_buffers and precomputed_partitions_file are mutually exclusive"
                .to_string(),
        ));
    }

    Ok(())
}

fn sanity_check_params(ivf: &IvfBuildParams, pq: &PQBuildParams) -> Result<()> {
    sanity_check_ivf_params(ivf)?;
    if ivf.precomputed_shuffle_buffers.is_some() && pq.codebook.is_none() {
        return Err(Error::index(
            "precomputed_shuffle_buffers requires codebooks to be set".to_string(),
        ));
    }

    Ok(())
}

/// Build IVF model from the dataset.
///
/// Parameters
/// ----------
/// - *dataset*: Dataset instance
/// - *column*: vector column.
/// - *dim*: vector dimension.
/// - *metric_type*: distance metric type.
/// - *params*: IVF build parameters.
///
/// Returns
/// -------
/// - IVF model.
///
/// Visibility: pub(super) for testing
#[instrument(level = "debug", skip_all, name = "build_ivf_model")]
pub async fn build_ivf_model(
    dataset: &Dataset,
    column: &str,
    dim: usize,
    metric_type: MetricType,
    params: &IvfBuildParams,
    fragment_ids: Option<&[u32]>,
    progress: std::sync::Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<IvfModel> {
    let num_partitions = params.num_partitions.unwrap();
    let centroids = params.centroids.clone();
    if let (Some(centroids), false) = (centroids.as_deref(), params.retrain) {
        info!("Pre-computed IVF centroids is provided, skip IVF training");
        if centroids.values().len() != num_partitions * dim {
            return Err(Error::index(format!(
                "IVF centroids length mismatch: {} != {}",
                centroids.len(),
                num_partitions * dim,
            )));
        }
        return Ok(IvfModel::new(centroids.clone(), None));
    }
    let sample_size_hint = num_partitions * params.sample_rate;

    if let Some(streaming_sample_rate) = params.streaming_sample_rate {
        if streaming_sample_rate == 0 {
            return Err(Error::invalid_input(
                "streaming_sample_rate must be greater than 0".to_string(),
            ));
        }
        if let Some(streaming_coreset_rate) = params.streaming_coreset_rate {
            if streaming_coreset_rate == 0 {
                return Err(Error::invalid_input(
                    "streaming_coreset_rate must be greater than 0".to_string(),
                ));
            }
            if streaming_coreset_rate > params.sample_rate {
                return Err(Error::invalid_input(format!(
                    "streaming_coreset_rate ({streaming_coreset_rate}) must be less than or equal to sample_rate ({})",
                    params.sample_rate
                )));
            }
        }
        if streaming_sample_rate < params.sample_rate {
            info!(
                "Start streaming IVF training. Total sample size: {}, per-step sample size: {}",
                sample_size_hint,
                num_partitions * streaming_sample_rate
            );
            let start = std::time::Instant::now();
            let ivf = train_streaming_ivf_model(
                dataset,
                column,
                dim,
                metric_type,
                params,
                fragment_ids,
                progress,
            )
            .await?;
            info!(
                "Trained streaming IVF model in {:02} seconds",
                start.elapsed().as_secs_f32()
            );
            return Ok(ivf);
        }
    }

    let start = std::time::Instant::now();
    info!(
        "Loading training data for IVF. Sample size: {}",
        sample_size_hint
    );
    let training_data =
        maybe_sample_training_data(dataset, column, sample_size_hint, fragment_ids).await?;
    info!(
        "Finished loading training data in {:02} seconds",
        start.elapsed().as_secs_f32()
    );
    if params.sample_rate >= 1024 && training_data.value_type() == DataType::Float16 {
        warn!(
            "Large sample_rate ({} >= 1024) for float16 vectors is possible to result in all zeros cluster centroid",
            params.sample_rate
        );
    }

    // If metric type is cosine, normalize the training data, and after this point,
    // treat the metric type as L2.
    let (training_data, mt) = if metric_type == MetricType::Cosine {
        let training_data = normalize_fsl_owned(training_data)?;
        (training_data, MetricType::L2)
    } else {
        (training_data, metric_type)
    };

    // we filtered out nulls when sampling, but we still need to filter out NaNs and INFs here
    let training_data = filter_finite_training_data(training_data)?;

    info!("Start to train IVF model");
    let start = std::time::Instant::now();
    let ivf = train_ivf_model(centroids, &training_data, mt, params, progress).await?;
    info!(
        "Trained IVF model in {:02} seconds",
        start.elapsed().as_secs_f32()
    );
    Ok(ivf)
}

async fn build_ivf_model_and_pq(
    dataset: &Dataset,
    column: &str,
    metric_type: MetricType,
    ivf_params: &IvfBuildParams,
    pq_params: &PQBuildParams,
    progress: std::sync::Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<(IvfModel, ProductQuantizer)> {
    sanity_check_params(ivf_params, pq_params)?;

    // `num_partitions` should be set before building the IVF model,
    // we use 32 as the default to avoid panicking, 32 is the default value
    // before we make `num_partitions` optional.
    let num_partitions = ivf_params.num_partitions.unwrap_or(32);
    info!(
        "Building vector index: IVF{},PQ{}, metric={}",
        num_partitions, pq_params.num_sub_vectors, metric_type,
    );

    // sanity check
    get_vector_type(dataset.schema(), column)?;
    let dim = get_vector_dim(dataset.schema(), column)?;

    let ivf_model = build_ivf_model(
        dataset,
        column,
        dim,
        metric_type,
        ivf_params,
        None,
        progress,
    )
    .await?;

    let ivf_residual = if matches!(metric_type, MetricType::Cosine | MetricType::L2) {
        Some(&ivf_model)
    } else {
        None
    };

    let pq = build_pq_model(dataset, column, dim, metric_type, pq_params, ivf_residual).await?;

    Ok((ivf_model, pq))
}

async fn scan_index_field_stream(
    dataset: &Dataset,
    column: &str,
) -> Result<impl RecordBatchStream + Unpin + 'static> {
    let mut scanner = dataset.scan();
    scanner.project(&[column])?;
    scanner.with_row_id();
    scanner.try_into_stream().await
}

pub async fn load_precomputed_partitions_if_available(
    ivf_params: &IvfBuildParams,
) -> Result<Option<HashMap<u64, u32>>> {
    match &ivf_params.precomputed_partitions_file {
        Some(file) => {
            info!("Loading precomputed partitions from file: {}", file);
            let mut builder = DatasetBuilder::from_uri(file);
            if let Some(storage_options) = &ivf_params.storage_options {
                builder = builder.with_storage_options(storage_options.clone());
            }
            let ds = builder.load().await?;
            let stream = ds.scan().try_into_stream().await?;
            Ok(Some(
                load_precomputed_partitions(stream, ds.count_rows(None).await?).await?,
            ))
        }
        None => Ok(None),
    }
}

#[allow(clippy::too_many_arguments)]
pub async fn build_ivf_pq_index(
    dataset: &Dataset,
    column: &str,
    index_name: &str,
    uuid: Uuid,
    metric_type: MetricType,
    ivf_params: &IvfBuildParams,
    pq_params: &PQBuildParams,
    progress: std::sync::Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<Vec<IndexFile>> {
    let (ivf_model, pq) = build_ivf_model_and_pq(
        dataset,
        column,
        metric_type,
        ivf_params,
        pq_params,
        progress,
    )
    .await?;
    let stream = scan_index_field_stream(dataset, column).await?;
    let precomputed_partitions = load_precomputed_partitions_if_available(ivf_params).await?;

    let file = write_ivf_pq_file(
        dataset.object_store.as_ref(),
        dataset.indices_dir(),
        column,
        index_name,
        uuid,
        dataset.version().version,
        ivf_model,
        pq,
        metric_type,
        stream,
        precomputed_partitions,
        ivf_params.shuffle_partition_batches,
        ivf_params.shuffle_partition_concurrency,
        ivf_params.precomputed_shuffle_buffers.clone(),
    )
    .await?;
    Ok(vec![file])
}

#[allow(clippy::too_many_arguments)]
pub async fn build_ivf_hnsw_pq_index(
    dataset: &Dataset,
    column: &str,
    index_name: &str,
    uuid: Uuid,
    metric_type: MetricType,
    ivf_params: &IvfBuildParams,
    hnsw_params: &HnswBuildParams,
    pq_params: &PQBuildParams,
) -> Result<()> {
    let (ivf_model, pq) = build_ivf_model_and_pq(
        dataset,
        column,
        metric_type,
        ivf_params,
        pq_params,
        lance_index::progress::noop_progress(),
    )
    .await?;
    let stream = scan_index_field_stream(dataset, column).await?;
    let precomputed_partitions = load_precomputed_partitions_if_available(ivf_params).await?;

    write_ivf_hnsw_file(
        dataset,
        column,
        index_name,
        uuid,
        ivf_model,
        Quantizer::Product(pq),
        metric_type,
        hnsw_params,
        stream,
        precomputed_partitions,
        ivf_params.shuffle_partition_batches,
        ivf_params.shuffle_partition_concurrency,
        ivf_params.precomputed_shuffle_buffers.clone(),
    )
    .await
}

struct RemapPageTask {
    offset: usize,
    length: u32,
    page: Option<Box<dyn VectorIndex>>,
}

impl RemapPageTask {
    fn new(offset: usize, length: u32) -> Self {
        Self {
            offset,
            length,
            page: None,
        }
    }
}

impl RemapPageTask {
    async fn load_and_remap(
        mut self,
        reader: Arc<dyn Reader>,
        index: &IVFIndex,
        mapping: &RowAddrRemap,
    ) -> Result<Self> {
        let mut page = index
            .sub_index
            .load(reader, self.offset, self.length as usize)
            .await?;
        page.remap(mapping).await?;
        self.page = Some(page);
        Ok(self)
    }

    async fn write(self, writer: &mut dyn Writer, ivf: &mut IvfModel) -> Result<()> {
        let page = self.page.as_ref().expect("Load was not called");
        let page: &PQIndex = page
            .as_any()
            .downcast_ref()
            .expect("Generic index writing not supported yet");
        ivf.offsets.push(writer.tell().await?);
        ivf.lengths
            .push(page.row_ids.as_ref().unwrap().len() as u32);
        let original_pq = transpose(
            page.code.as_ref().unwrap(),
            page.pq.code_dim(),
            page.row_ids.as_ref().unwrap().len(),
        );
        write_pq_partition_payload(
            writer,
            &[&original_pq],
            &[page.row_ids.as_ref().unwrap().as_ref()],
        )
        .await?;
        Ok(())
    }
}

fn generate_remap_tasks(offsets: &[usize], lengths: &[u32]) -> Result<Vec<RemapPageTask>> {
    let mut tasks: Vec<RemapPageTask> = Vec::with_capacity(offsets.len() * 2 + 1);

    for (offset, length) in offsets.iter().zip(lengths.iter()) {
        tasks.push(RemapPageTask::new(*offset, *length));
    }

    Ok(tasks)
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn remap_index_file_v3(
    dataset: &Dataset,
    new_uuid: &Uuid,
    index: Arc<dyn VectorIndex>,
    mapping: &RowAddrRemap,
    column: String,
) -> Result<Vec<IndexFile>> {
    let dataset = dataset.clone();
    let index_dir = dataset.indices_dir().join(new_uuid.to_string());
    let (_, element_type) = get_vector_type(dataset.schema(), &column)?;
    match index.sub_index_type() {
        (SubIndexType::Flat, QuantizationType::Flat) => match element_type {
            DataType::Float16 | DataType::Float32 | DataType::Float64 => {
                IvfIndexBuilder::<FlatIndex, FlatQuantizer>::new_remapper(
                    dataset, column, index_dir, index,
                )?
                .remap(mapping)
                .await
            }
            DataType::UInt8 => {
                IvfIndexBuilder::<FlatIndex, FlatBinQuantizer>::new_remapper(
                    dataset, column, index_dir, index,
                )?
                .remap(mapping)
                .await
            }
            _ => Err(Error::index(format!(
                "the field type {} is not supported for FLAT index",
                element_type
            ))),
        },
        (SubIndexType::Flat, QuantizationType::Product) => {
            IvfIndexBuilder::<FlatIndex, ProductQuantizer>::new_remapper(
                dataset, column, index_dir, index,
            )?
            .remap(mapping)
            .await
        }
        (SubIndexType::Flat, QuantizationType::Scalar) => {
            IvfIndexBuilder::<FlatIndex, ScalarQuantizer>::new_remapper(
                dataset, column, index_dir, index,
            )?
            .remap(mapping)
            .await
        }
        (SubIndexType::Flat, QuantizationType::FlatBin) => {
            IvfIndexBuilder::<FlatIndex, FlatBinQuantizer>::new_remapper(
                dataset, column, index_dir, index,
            )?
            .remap(mapping)
            .await
        }
        (SubIndexType::Flat, QuantizationType::Rabit) => {
            IvfIndexBuilder::<FlatIndex, RabitQuantizer>::new_remapper(
                dataset, column, index_dir, index,
            )?
            .remap(mapping)
            .await
        }
        (SubIndexType::Hnsw, QuantizationType::Flat) => {
            IvfIndexBuilder::<HNSW, FlatQuantizer>::new_remapper(dataset, column, index_dir, index)?
                .remap(mapping)
                .await
        }
        (SubIndexType::Hnsw, QuantizationType::FlatBin) => {
            IvfIndexBuilder::<HNSW, FlatBinQuantizer>::new_remapper(
                dataset, column, index_dir, index,
            )?
            .remap(mapping)
            .await
        }
        (SubIndexType::Hnsw, QuantizationType::Product) => {
            IvfIndexBuilder::<HNSW, ProductQuantizer>::new_remapper(
                dataset, column, index_dir, index,
            )?
            .remap(mapping)
            .await
        }

        (SubIndexType::Hnsw, QuantizationType::Scalar) => {
            IvfIndexBuilder::<HNSW, ScalarQuantizer>::new_remapper(
                dataset, column, index_dir, index,
            )?
            .remap(mapping)
            .await
        }
        (SubIndexType::Hnsw, QuantizationType::Rabit) => {
            IvfIndexBuilder::<HNSW, RabitQuantizer>::new_remapper(
                dataset, column, index_dir, index,
            )?
            .remap(mapping)
            .await
        }
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) async fn remap_index_file(
    dataset: &Dataset,
    old_uuid: &Uuid,
    new_uuid: &Uuid,
    old_version: u64,
    index: &IVFIndex,
    mapping: &RowAddrRemap,
    name: String,
    column: String,
    transforms: Vec<pb::Transform>,
) -> Result<IndexFile> {
    let object_store = dataset.object_store.as_ref();
    let old_path = dataset
        .indices_dir()
        .join(old_uuid.to_string())
        .join(INDEX_FILE_NAME);
    let new_path = dataset
        .indices_dir()
        .join(new_uuid.to_string())
        .join(INDEX_FILE_NAME);

    let file_sizes = dataset
        .load_index(old_uuid)
        .await?
        .map(|index| index.file_size_map())
        .unwrap_or_default();
    let reader: Arc<dyn Reader> =
        open_index_file(object_store, &old_path, INDEX_FILE_NAME, &file_sizes)
            .await?
            .into();
    let mut writer = object_store.create(&new_path).await?;

    let tasks = generate_remap_tasks(&index.ivf.offsets, &index.ivf.lengths)?;

    let mut task_stream = stream::iter(tasks)
        .map(|task| task.load_and_remap(reader.clone(), index, mapping))
        .buffered(object_store.io_parallelism());

    let mut ivf = IvfModel {
        centroids: index.ivf.centroids.clone(),
        offsets: Vec::with_capacity(index.ivf.offsets.len()),
        lengths: Vec::with_capacity(index.ivf.lengths.len()),
        loss: index.ivf.loss,
    };
    while let Some(write_task) = task_stream.try_next().await? {
        write_task.write(writer.as_mut(), &mut ivf).await?;
    }

    let pq_sub_index = index
        .sub_index
        .as_any()
        .downcast_ref::<PQIndex>()
        .ok_or_else(|| Error::not_supported_source("Remapping a non-pq sub-index".into()))?;

    let metadata = IvfPQIndexMetadata {
        name,
        column,
        dimension: index.ivf.dimension() as u32,
        dataset_version: old_version,
        ivf,
        metric_type: index.metric_type,
        pq: pq_sub_index.pq.clone(),
        transforms,
    };

    let metadata = pb::Index::try_from(&metadata)?;
    let pos = writer.write_protobuf(&metadata).await?;
    // TODO: for now the IVF_PQ index file format hasn't been updated, so keep the old version,
    // change it to latest version value after refactoring the IVF_PQ
    writer.write_magics(pos, 0, 1, MAGIC).await?;
    let size_bytes = writer.tell().await? as u64;
    Writer::shutdown(writer.as_mut()).await?;

    Ok(IndexFile {
        path: INDEX_FILE_NAME.to_string(),
        size_bytes,
    })
}

/// Write the index to the index file.
///
#[allow(clippy::too_many_arguments)]
async fn write_ivf_pq_file(
    object_store: &ObjectStore,
    index_dir: Path,
    column: &str,
    index_name: &str,
    uuid: Uuid,
    dataset_version: u64,
    mut ivf: IvfModel,
    pq: ProductQuantizer,
    metric_type: MetricType,
    stream: impl RecordBatchStream + Unpin + 'static,
    precomputed_partitions: Option<HashMap<u64, u32>>,
    shuffle_partition_batches: usize,
    shuffle_partition_concurrency: usize,
    precomputed_shuffle_buffers: Option<(Path, Vec<String>)>,
) -> Result<IndexFile> {
    let path = index_dir
        .clone()
        .join(uuid.to_string())
        .join(INDEX_FILE_NAME);
    let mut writer = object_store.create(&path).await?;

    let start = std::time::Instant::now();
    let num_partitions = ivf.num_partitions() as u32;
    builder::build_partitions(
        writer.as_mut(),
        stream,
        column,
        &mut ivf,
        pq.clone(),
        metric_type,
        0..num_partitions,
        precomputed_partitions,
        shuffle_partition_batches,
        shuffle_partition_concurrency,
        precomputed_shuffle_buffers,
    )
    .await?;
    info!("Built IVF partitions: {}s", start.elapsed().as_secs_f32());

    let metadata = IvfPQIndexMetadata {
        name: index_name.to_string(),
        column: column.to_string(),
        dimension: pq.dimension as u32,
        dataset_version,
        metric_type,
        ivf,
        pq,
        transforms: vec![],
    };

    let metadata = pb::Index::try_from(&metadata)?;
    let pos = writer.write_protobuf(&metadata).await?;
    // TODO: for now the IVF_PQ index file format hasn't been updated, so keep the old version,
    // change it to latest version value after refactoring the IVF_PQ
    writer.write_magics(pos, 0, 1, MAGIC).await?;
    let size_bytes = writer.tell().await? as u64;
    Writer::shutdown(writer.as_mut()).await?;

    Ok(IndexFile {
        path: INDEX_FILE_NAME.to_string(),
        size_bytes,
    })
}

pub async fn write_ivf_pq_file_from_existing_index(
    dataset: &Dataset,
    column: &str,
    index_name: &str,
    index_id: Uuid,
    mut ivf: IvfModel,
    pq: ProductQuantizer,
    streams: Vec<impl Stream<Item = Result<RecordBatch>>>,
) -> Result<()> {
    let obj_store = dataset.object_store.as_ref();
    let path = dataset
        .indices_dir()
        .join(index_id.to_string())
        .join("index.idx");
    let mut writer = obj_store.create(&path).await?;
    write_pq_partitions(writer.as_mut(), &mut ivf, Some(streams), None).await?;

    let metadata = IvfPQIndexMetadata::new(
        index_name.to_string(),
        column.to_string(),
        dataset.version().version,
        pq.distance_type,
        ivf,
        pq,
        vec![],
    );

    let metadata = pb::Index::try_from(&metadata)?;
    let pos = writer.write_protobuf(&metadata).await?;
    writer.write_magics(pos, 0, 1, MAGIC).await?;
    Writer::shutdown(writer.as_mut()).await?;

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn write_ivf_hnsw_file(
    dataset: &Dataset,
    column: &str,
    _index_name: &str,
    uuid: Uuid,
    mut ivf: IvfModel,
    quantizer: Quantizer,
    distance_type: DistanceType,
    hnsw_params: &HnswBuildParams,
    stream: impl RecordBatchStream + Unpin + 'static,
    precomputed_partitions: Option<HashMap<u64, u32>>,
    shuffle_partition_batches: usize,
    shuffle_partition_concurrency: usize,
    precomputed_shuffle_buffers: Option<(Path, Vec<String>)>,
) -> Result<()> {
    let object_store = dataset.object_store.as_ref();
    let path = dataset
        .indices_dir()
        .join(uuid.to_string())
        .join(INDEX_FILE_NAME);
    let writer = object_store.create(&path).await?;

    let schema = lance_core::datatypes::Schema::try_from(HNSW::schema().as_ref())?;
    let mut writer =
        V1FileWriter::with_object_writer(writer, schema, &V1FileWriterOptions::default())?;
    writer.add_metadata(
        INDEX_METADATA_SCHEMA_KEY,
        json!(IndexMetadata {
            index_type: format!("IVF_HNSW_{}", quantizer.quantization_type()),
            distance_type: distance_type.to_string(),
        })
        .to_string()
        .as_str(),
    );

    let aux_path = dataset
        .indices_dir()
        .join(uuid.to_string())
        .join(INDEX_AUXILIARY_FILE_NAME);
    let aux_writer = object_store.create(&aux_path).await?;
    let schema = Schema::new(vec![
        ROW_ID_FIELD.clone(),
        arrow_schema::Field::new(
            quantizer.column(),
            DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new("item", DataType::UInt8, true)),
                quantizer.code_dim() as i32,
            ),
            false,
        ),
    ]);
    let schema = lance_core::datatypes::Schema::try_from(&schema)?;
    let mut aux_writer =
        V1FileWriter::with_object_writer(aux_writer, schema, &V1FileWriterOptions::default())?;
    aux_writer.add_metadata(
        INDEX_METADATA_SCHEMA_KEY,
        json!(IndexMetadata {
            index_type: quantizer.quantization_type().to_string(),
            distance_type: distance_type.to_string(),
        })
        .to_string()
        .as_str(),
    );

    // For PQ, we need to store the codebook
    let quantization_metadata = match &quantizer {
        Quantizer::Product(pq) => {
            let codebook_tensor = pb::Tensor::try_from(&pq.codebook)?;
            let codebook_pos = aux_writer.tell().await?;
            aux_writer
                .object_writer
                .write_protobuf(&codebook_tensor)
                .await?;

            Some(QuantizationMetadata {
                codebook_position: Some(codebook_pos),
                ..Default::default()
            })
        }
        _ => None,
    };

    aux_writer.add_metadata(
        quantizer.metadata_key(),
        quantizer
            .metadata(quantization_metadata)?
            .to_string()
            .as_str(),
    );

    let start = std::time::Instant::now();
    let num_partitions = ivf.num_partitions() as u32;

    let (hnsw_metadata, aux_ivf) = builder::build_hnsw_partitions(
        Arc::new(dataset.clone()),
        &mut writer,
        Some(&mut aux_writer),
        stream,
        column,
        &mut ivf,
        quantizer,
        distance_type,
        hnsw_params,
        0..num_partitions,
        precomputed_partitions,
        shuffle_partition_batches,
        shuffle_partition_concurrency,
        precomputed_shuffle_buffers,
    )
    .await?;
    info!("Built IVF partitions: {}s", start.elapsed().as_secs_f32());

    // Add the metadata of HNSW partitions
    let hnsw_metadata_json = json!(hnsw_metadata);
    writer.add_metadata(IVF_PARTITION_KEY, &hnsw_metadata_json.to_string());

    ivf.write(&mut writer).await?;
    writer.finish().await?;

    // Write the aux file
    aux_ivf.write(&mut aux_writer).await?;
    aux_writer.finish().await?;
    Ok(())
}

/// Merge one caller-defined group of source segments into a single segment.
pub(crate) async fn merge_segments(
    object_store: &ObjectStore,
    indices_dir: &Path,
    segments: Vec<TableIndexMetadata>,
) -> Result<TableIndexMetadata> {
    merge_segments_with_progress(
        object_store,
        indices_dir,
        segments,
        lance_index::progress::noop_progress(),
    )
    .await
}

/// Merge one caller-defined group of source segments into a single segment and
/// report progress through the provided callback.
pub(crate) async fn merge_segments_with_progress(
    object_store: &ObjectStore,
    indices_dir: &Path,
    segments: Vec<TableIndexMetadata>,
    progress: Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<TableIndexMetadata> {
    if segments.is_empty() {
        return Err(Error::index("No segment metadata was provided".to_string()));
    }
    if segments.len() == 1 {
        return Ok(segments.into_iter().next().unwrap());
    }

    let mut merged_segment = segments[0].clone();
    let mut fragment_bitmap = RoaringBitmap::new();
    for segment in &segments {
        let source_fragment_bitmap = segment.fragment_bitmap.as_ref().ok_or_else(|| {
            Error::index(format!(
                "Segment '{}' is missing fragment coverage",
                segment.uuid
            ))
        })?;
        fragment_bitmap |= source_fragment_bitmap.clone();
    }

    let index_version = infer_source_index_version(&segments)?;
    let segment_uuid = Uuid::new_v4();
    let final_dir = indices_dir.clone().join(segment_uuid.to_string());
    let files = merge_segments_to_dir(
        object_store,
        indices_dir,
        &final_dir,
        &segments,
        None,
        progress,
    )
    .await?;

    merged_segment = TableIndexMetadata {
        uuid: segment_uuid,
        fragment_bitmap: Some(fragment_bitmap),
        index_details: Some(Arc::new(crate::index::vector_index_details_default())),
        index_version,
        created_at: Some(chrono::Utc::now()),
        base_id: None,
        files: Some(files),
        ..merged_segment
    };
    Ok(merged_segment)
}

/// Merge the selected input segments into `final_dir`.
///
/// The caller defines the source segment group explicitly. This helper reads
/// those input segments directly from `indices/<segment_uuid>/` and writes the
/// merged auxiliary/index files into `final_dir`.
async fn merge_segments_to_dir(
    object_store: &ObjectStore,
    indices_dir: &Path,
    final_dir: &Path,
    segments: &[TableIndexMetadata],
    _requested_index_type: Option<IndexType>,
    progress: Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<Vec<IndexFile>> {
    reset_final_segment_dir(object_store, final_dir).await?;

    debug_assert!(
        segments.len() > 1,
        "merge helper should only be used for multi-source groups"
    );

    let aux_paths = segments
        .iter()
        .map(|segment| {
            indices_dir
                .clone()
                .join(segment.uuid.to_string())
                .join(INDEX_AUXILIARY_FILE_NAME)
        })
        .collect::<Vec<_>>();
    let source_index_paths = segments
        .iter()
        .map(|segment| {
            indices_dir
                .clone()
                .join(segment.uuid.to_string())
                .join(INDEX_FILE_NAME)
        })
        .collect::<Vec<_>>();

    let auxiliary_file =
        lance_index::vector::distributed::index_merger::merge_partial_vector_auxiliary_files(
            object_store,
            &aux_paths,
            final_dir,
            progress.clone(),
        )
        .await?;
    let index_file = write_root_vector_index_from_auxiliary(
        object_store,
        final_dir,
        None,
        &source_index_paths,
        progress.clone(),
    )
    .await?;

    Ok(vec![auxiliary_file, index_file])
}

fn infer_source_index_version(group: &[TableIndexMetadata]) -> Result<i32> {
    debug_assert!(!group.is_empty());
    let first = group[0].index_version;
    if group.iter().any(|segment| segment.index_version != first) {
        return Err(Error::index(
            "Distributed vector segments must all have the same index version".to_string(),
        ));
    }
    Ok(first)
}

/// Best-effort reset of one target directory before rewriting it.
async fn reset_final_segment_dir(object_store: &ObjectStore, final_dir: &Path) -> Result<()> {
    match object_store.remove_dir_all(final_dir.clone()).await {
        Ok(()) => {}
        Err(Error::NotFound { .. }) => {}
        Err(err) => return Err(err),
    }
    Ok(())
}

async fn write_root_vector_index_from_auxiliary(
    object_store: &ObjectStore,
    index_dir: &Path,
    requested_index_type: Option<IndexType>,
    centroid_source_index_paths: &[Path],
    progress: Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<IndexFile> {
    let aux_path = index_dir.clone().join(INDEX_AUXILIARY_FILE_NAME);
    let scheduler = ScanScheduler::new(
        Arc::new(object_store.clone()),
        SchedulerConfig::max_bandwidth(object_store),
    );
    let fh = scheduler
        .open_file(&aux_path, &CachedFileSize::unknown())
        .await?;
    let aux_reader = V2Reader::try_open(
        fh,
        None,
        Arc::default(),
        &LanceCache::no_cache(),
        V2ReaderOptions::default(),
    )
    .await?;

    let meta = aux_reader.metadata();
    // Inherit file format version from the unified auxiliary (which inherited it from shards)
    let format_version = meta.version();
    let ivf_buf_idx: u32 = meta
        .file_schema
        .metadata
        .get(IVF_METADATA_KEY)
        .ok_or_else(|| Error::index("IVF meta missing in unified auxiliary".to_string()))?
        .parse()
        .map_err(|_| Error::index("IVF index parse error".to_string()))?;

    let raw_ivf_bytes = aux_reader.read_global_buffer(ivf_buf_idx).await?;
    let mut pb_ivf: lance_index::pb::Ivf = Message::decode(raw_ivf_bytes.clone())?;

    // If the unified IVF metadata does not contain centroids, try to source them
    // from one of the shard index files that fed this merge.
    if pb_ivf.centroids_tensor.is_none() {
        for partial_index_path in centroid_source_index_paths {
            if !object_store.exists(partial_index_path).await? {
                continue;
            }
            let fh = scheduler
                .open_file(partial_index_path, &CachedFileSize::unknown())
                .await?;
            let partial_reader = V2Reader::try_open(
                fh,
                None,
                Arc::default(),
                &LanceCache::no_cache(),
                V2ReaderOptions::default(),
            )
            .await?;
            let partial_meta = partial_reader.metadata();
            if let Some(ivf_idx_str) = partial_meta.file_schema.metadata.get(IVF_METADATA_KEY)
                && let Ok(ivf_idx) = ivf_idx_str.parse::<u32>()
            {
                let partial_ivf_bytes = partial_reader.read_global_buffer(ivf_idx).await?;
                let partial_pb_ivf: lance_index::pb::Ivf = Message::decode(partial_ivf_bytes)?;
                if partial_pb_ivf.centroids_tensor.is_some() {
                    pb_ivf.centroids_tensor = partial_pb_ivf.centroids_tensor;
                    break;
                }
            }
        }
    }

    let ivf_model: IvfModel = IvfModel::try_from(pb_ivf.clone())?;
    let nlist = ivf_model.num_partitions();
    let ivf_bytes = pb_ivf.encode_to_vec().into();

    // Determine index metadata JSON from auxiliary or requested index type.
    let mut idx_meta: IndexMetadata =
        if let Some(idx_json) = meta.file_schema.metadata.get(INDEX_METADATA_SCHEMA_KEY) {
            serde_json::from_str(idx_json)?
        } else {
            let dt = meta
                .file_schema
                .metadata
                .get(DISTANCE_TYPE_KEY)
                .cloned()
                .unwrap_or_else(|| "l2".to_string());
            let index_type = requested_index_type.ok_or_else(|| {
                Error::index(
                    "Index type must be provided when auxiliary metadata is missing index metadata"
                        .to_string(),
                )
            })?;
            IndexMetadata {
                index_type: index_type.to_string(),
                distance_type: dt,
            }
        };
    if let Some(source_hnsw_index_metadata) =
        read_hnsw_index_metadata_from_sources(object_store, &scheduler, centroid_source_index_paths)
            .await?
    {
        if idx_meta.index_type.starts_with("IVF_HNSW")
            && !index_metadata_eq(&idx_meta, &source_hnsw_index_metadata)
        {
            return Err(Error::invalid_input(format!(
                "HNSW index metadata mismatch while merging index segments: expected {:?}, got {:?}",
                idx_meta, source_hnsw_index_metadata
            )));
        }
        idx_meta = source_hnsw_index_metadata;
    }

    // Write root index.idx via V2 writer so downstream opens through v2 path.
    let index_path = index_dir.clone().join(INDEX_FILE_NAME);
    let obj_writer = object_store.create(&index_path).await?;
    progress
        .stage_start("write_root_index", Some(1), "files")
        .await?;

    // Schema for HNSW sub-index: include neighbors/dist fields; empty batch is fine.
    let arrow_schema = HNSW::schema();
    let schema = lance_core::datatypes::Schema::try_from(arrow_schema.as_ref())?;
    let mut v2_writer = lance_file::versions::create_writer(
        format_version,
        obj_writer,
        schema,
        V2WriterOptions::default(),
    )?;

    // For HNSW variants, attach per-partition metadata list; for FLAT-based
    // variants, attach minimal placeholder metadata.
    let is_hnsw = idx_meta.index_type.starts_with("IVF_HNSW");
    let is_flat_based = matches!(
        idx_meta.index_type.as_str(),
        "IVF_FLAT" | "IVF_PQ" | "IVF_SQ" | "IVF_RQ"
    );

    if is_hnsw {
        let hnsw_params = read_hnsw_build_params_from_sources(
            object_store,
            &scheduler,
            centroid_source_index_paths,
        )
        .await?;
        write_hnsw_root_index_from_auxiliary(
            &mut v2_writer,
            &aux_reader,
            &ivf_model,
            &hnsw_params,
            &idx_meta,
            progress.clone(),
        )
        .await?;
    } else {
        // Attach precise index metadata (type + distance).
        let index_meta_json = serde_json::to_string(&idx_meta)?;
        v2_writer.add_schema_metadata(INDEX_METADATA_SCHEMA_KEY, &index_meta_json);

        // Add IVF protobuf as a global buffer and reference via IVF_METADATA_KEY.
        let pos = v2_writer.add_global_buffer(ivf_bytes).await?;
        v2_writer.add_schema_metadata(IVF_METADATA_KEY, pos.to_string());

        if is_flat_based {
            let meta_vec: Vec<String> = (0..nlist).map(|_| "{}".to_string()).collect();
            let meta_vec_json = serde_json::to_string(&meta_vec)?;
            v2_writer.add_schema_metadata("lance:flat", meta_vec_json);
        }

        let empty_batch = RecordBatch::new_empty(arrow_schema);
        v2_writer.write_batch(&empty_batch).await?;
    }

    let summary = v2_writer.finish().await?;
    progress.stage_progress("write_root_index", 1).await?;
    progress.stage_complete("write_root_index").await?;

    Ok(IndexFile {
        path: INDEX_FILE_NAME.to_string(),
        size_bytes: summary.size_bytes,
    })
}

async fn read_hnsw_index_metadata_from_sources(
    object_store: &ObjectStore,
    scheduler: &Arc<ScanScheduler>,
    source_index_paths: &[Path],
) -> Result<Option<IndexMetadata>> {
    let mut index_metadata: Option<IndexMetadata> = None;

    for source_index_path in source_index_paths {
        if !object_store.exists(source_index_path).await? {
            continue;
        }

        let fh = scheduler
            .open_file(source_index_path, &CachedFileSize::unknown())
            .await?;
        let reader = V2Reader::try_open(
            fh,
            None,
            Arc::default(),
            &LanceCache::no_cache(),
            V2ReaderOptions::default(),
        )
        .await?;
        let Some(metadata_json) = reader
            .metadata()
            .file_schema
            .metadata
            .get(INDEX_METADATA_SCHEMA_KEY)
        else {
            continue;
        };
        let metadata: IndexMetadata = serde_json::from_str(metadata_json)?;
        if !metadata.index_type.starts_with("IVF_HNSW") {
            continue;
        }

        if let Some(index_metadata) = index_metadata.as_ref() {
            if !index_metadata_eq(index_metadata, &metadata) {
                return Err(Error::invalid_input(format!(
                    "HNSW index metadata mismatch while merging index segments: \
                     expected {:?}, got {:?} in {}",
                    index_metadata, metadata, source_index_path
                )));
            }
        } else {
            index_metadata = Some(metadata);
        }
    }

    Ok(index_metadata)
}

fn index_metadata_eq(left: &IndexMetadata, right: &IndexMetadata) -> bool {
    left.index_type == right.index_type && left.distance_type == right.distance_type
}

async fn read_hnsw_build_params_from_sources(
    object_store: &ObjectStore,
    scheduler: &Arc<ScanScheduler>,
    source_index_paths: &[Path],
) -> Result<HnswBuildParams> {
    let mut build_params: Option<HnswBuildParams> = None;

    for source_index_path in source_index_paths {
        if !object_store.exists(source_index_path).await? {
            continue;
        }

        let fh = scheduler
            .open_file(source_index_path, &CachedFileSize::unknown())
            .await?;
        let reader = V2Reader::try_open(
            fh,
            None,
            Arc::default(),
            &LanceCache::no_cache(),
            V2ReaderOptions::default(),
        )
        .await?;
        let Some(metadata_json) = reader
            .metadata()
            .file_schema
            .metadata
            .get(HNSW_METADATA_KEY)
        else {
            continue;
        };
        let partition_metadata: Vec<String> = serde_json::from_str(metadata_json)?;
        for metadata in partition_metadata {
            if metadata.is_empty() {
                continue;
            }
            let metadata: HnswMetadata = serde_json::from_str(&metadata)?;
            if let Some(build_params) = build_params.as_ref() {
                if !hnsw_build_params_eq(build_params, &metadata.params) {
                    return Err(Error::invalid_input(format!(
                        "HNSW build parameters mismatch while merging index segments: \
                         expected {:?}, got {:?} in {}",
                        build_params, metadata.params, source_index_path
                    )));
                }
            } else {
                build_params = Some(metadata.params);
            }
        }
    }

    Ok(build_params.unwrap_or_default())
}

fn hnsw_build_params_eq(left: &HnswBuildParams, right: &HnswBuildParams) -> bool {
    left.max_level == right.max_level
        && left.m == right.m
        && left.ef_construction == right.ef_construction
        && left.prefetch_distance == right.prefetch_distance
}

async fn write_hnsw_root_index_from_auxiliary(
    writer: &mut V2Writer,
    aux_reader: &V2Reader,
    aux_ivf: &IvfModel,
    hnsw_params: &HnswBuildParams,
    index_metadata: &IndexMetadata,
    progress: Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<()> {
    let mut index_ivf = if let Some(centroids) = aux_ivf.centroids.clone() {
        IvfModel::new(centroids, aux_ivf.loss)
    } else {
        IvfModel::empty()
    };
    let distance_type = DistanceType::try_from(index_metadata.distance_type.as_str())?;
    let mut partition_index_metadata = Vec::with_capacity(aux_ivf.num_partitions());

    progress
        .stage_start(
            "rebuild_hnsw_graph",
            Some(aux_ivf.num_partitions() as u64),
            "partitions",
        )
        .await?;

    for partition_id in 0..aux_ivf.num_partitions() {
        let row_range = aux_ivf.row_range(partition_id);
        if row_range.is_empty() {
            index_ivf.add_partition(0);
            partition_index_metadata.push(String::new());
            progress
                .stage_progress("rebuild_hnsw_graph", partition_id as u64 + 1)
                .await?;
            continue;
        }

        let batch = read_v2_partition_batch(aux_reader, row_range).await?;
        let hnsw = build_hnsw_from_storage_batch(
            &index_metadata.index_type,
            batch,
            aux_reader,
            distance_type,
            hnsw_params,
        )
        .await?;
        let index_batch = hnsw.to_batch()?;

        writer.write_batch(&index_batch).await?;
        index_ivf.add_partition(index_batch.num_rows() as u32);
        partition_index_metadata.push(serde_json::to_string(&hnsw.metadata())?);
        progress
            .stage_progress("rebuild_hnsw_graph", partition_id as u64 + 1)
            .await?;
    }

    progress.stage_complete("rebuild_hnsw_graph").await?;

    write_hnsw_index_metadata(writer, &index_ivf, distance_type, index_metadata).await?;
    writer.add_schema_metadata(
        HNSW_METADATA_KEY,
        serde_json::to_string(&partition_index_metadata)?,
    );

    Ok(())
}

async fn read_v2_partition_batch(reader: &V2Reader, range: Range<usize>) -> Result<RecordBatch> {
    let schema = Arc::new(reader.schema().as_ref().into());
    let stream = reader
        .read_stream(
            ReadBatchParams::Range(range),
            u32::MAX,
            4,
            FilterExpression::no_filter(),
        )
        .await?;
    let batches = stream.try_collect::<Vec<_>>().await?;
    if batches.is_empty() {
        Ok(RecordBatch::new_empty(schema))
    } else {
        Ok(concat_batches(&schema, batches.iter())?)
    }
}

async fn build_hnsw_from_storage_batch(
    index_type: &str,
    batch: RecordBatch,
    aux_reader: &V2Reader,
    distance_type: DistanceType,
    hnsw_params: &HnswBuildParams,
) -> Result<HNSW> {
    match index_type {
        "IVF_HNSW_FLAT" => {
            let metadata = read_storage_metadata::<FlatMetadata>(aux_reader, "")?;
            let vector_type = batch
                .column_by_name(FLAT_COLUMN)
                .ok_or_else(|| {
                    Error::index(format!(
                        "{FLAT_COLUMN} column missing from HNSW_FLAT storage"
                    ))
                })?
                .as_fixed_size_list()
                .value_type();
            if vector_type == DataType::UInt8 && distance_type == DistanceType::Hamming {
                let storage =
                    FlatBinStorage::try_from_batch(batch, &metadata, distance_type, None)?;
                HNSW::index_vectors(&storage, hnsw_params.clone())
            } else {
                let storage =
                    FlatFloatStorage::try_from_batch(batch, &metadata, distance_type, None)?;
                HNSW::index_vectors(&storage, hnsw_params.clone())
            }
        }
        "IVF_HNSW_PQ" => {
            let metadata = read_pq_storage_metadata(aux_reader).await?;
            let storage =
                ProductQuantizationStorage::try_from_batch(batch, &metadata, distance_type, None)?;
            HNSW::index_vectors(&storage, hnsw_params.clone())
        }
        "IVF_HNSW_SQ" => {
            let metadata =
                read_storage_metadata::<ScalarQuantizationMetadata>(aux_reader, SQ_METADATA_KEY)?;
            let storage =
                ScalarQuantizationStorage::try_from_batch(batch, &metadata, distance_type, None)?;
            HNSW::index_vectors(&storage, hnsw_params.clone())
        }
        other => Err(Error::index(format!(
            "Cannot rebuild HNSW graph for unsupported index type {other}"
        ))),
    }
}

async fn write_hnsw_index_metadata(
    writer: &mut V2Writer,
    ivf: &IvfModel,
    distance_type: DistanceType,
    index_metadata: &IndexMetadata,
) -> Result<()> {
    let pb_ivf: lance_index::pb::Ivf = ivf.try_into()?;
    let pos = writer
        .add_global_buffer(pb_ivf.encode_to_vec().into())
        .await?;
    writer.add_schema_metadata(IVF_METADATA_KEY, pos.to_string());
    writer.add_schema_metadata(
        INDEX_METADATA_SCHEMA_KEY,
        serde_json::to_string(&IndexMetadata {
            index_type: index_metadata.index_type.clone(),
            distance_type: distance_type.to_string(),
        })?,
    );
    Ok(())
}

async fn read_pq_storage_metadata(reader: &V2Reader) -> Result<ProductQuantizationMetadata> {
    let mut metadata =
        read_storage_metadata::<ProductQuantizationMetadata>(reader, PQ_METADATA_KEY)?;
    if metadata.codebook.is_none() {
        let tensor_bytes = reader
            .read_global_buffer(metadata.codebook_position as u32)
            .await?;
        let codebook_tensor: lance_index::pb::Tensor = Message::decode(tensor_bytes)?;
        metadata.codebook = Some(FixedSizeListArray::try_from(&codebook_tensor)?);
    }
    Ok(metadata)
}

fn read_storage_metadata<T>(reader: &V2Reader, storage_metadata_key: &str) -> Result<T>
where
    T: serde::de::DeserializeOwned,
{
    if !storage_metadata_key.is_empty()
        && let Some(metadata) = reader
            .metadata()
            .file_schema
            .metadata
            .get(storage_metadata_key)
    {
        return Ok(serde_json::from_str(metadata)?);
    }

    let storage_metadata = reader
        .metadata()
        .file_schema
        .metadata
        .get(STORAGE_METADATA_KEY)
        .ok_or_else(|| Error::index(format!("{STORAGE_METADATA_KEY} missing from storage file")))?;
    let metadata_entries: Vec<String> = serde_json::from_str(storage_metadata)?;
    let metadata = metadata_entries.first().ok_or_else(|| {
        Error::index(format!(
            "{STORAGE_METADATA_KEY} did not contain any storage metadata entries"
        ))
    })?;
    Ok(serde_json::from_str(metadata)?)
}

async fn do_train_ivf_model<T: ArrowPrimitiveType>(
    centroids: Option<Arc<FixedSizeListArray>>,
    data: &PrimitiveArray<T>,
    dimension: usize,
    metric_type: MetricType,
    params: &IvfBuildParams,
    progress: std::sync::Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<IvfModel>
where
    <T as ArrowPrimitiveType>::Native: Dot + L2 + Normalize,
    PrimitiveArray<T>: From<Vec<T::Native>>,
{
    const REDOS: usize = 1;
    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<u64>();
    let progress_worker = {
        let progress = progress.clone();
        tokio::spawn(async move {
            while let Some(iter) = progress_rx.recv().await {
                if let Err(e) = progress.stage_progress("train_ivf", iter).await {
                    warn!("Progress callback error during train_ivf: {e}");
                }
            }
        })
    };

    let on_progress: Arc<dyn Fn(u32, u32) + Send + Sync> = {
        let progress_tx = progress_tx.clone();
        let cumulative_iters = std::sync::atomic::AtomicU64::new(0);
        Arc::new(move |_iter: u32, _max_iters: u32| {
            // Track cumulative iterations across all kmeans runs in this stage
            // (flat and hierarchical both invoke the callback per-iteration).
            let total = cumulative_iters.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            // Non-blocking send from sync kmeans loop into async progress worker.
            let _ = progress_tx.send(total);
        })
    };
    let kmeans_params = KMeansParams::new(centroids, params.max_iters as u32, REDOS, metric_type)
        .with_balance_factor(1.0)
        .with_on_progress(on_progress);
    let kmeans = lance_index::vector::kmeans::train_kmeans::<T>(
        data,
        kmeans_params,
        dimension,
        params.num_partitions.unwrap_or(32),
        params.sample_rate,
    );
    drop(progress_tx);
    if let Err(e) = progress_worker.await {
        warn!("Progress worker join error during train_ivf: {e}");
    }
    let kmeans = kmeans?;
    let training_data = FixedSizeListArray::try_new_from_values(
        Arc::new(data.clone()) as ArrayRef,
        dimension as i32,
    )?;
    let loss = kmeans.compute_loss(&training_data)?;
    Ok(IvfModel::new(
        FixedSizeListArray::try_new_from_values(kmeans.centroids, dimension as i32)?,
        Some(loss),
    ))
}

async fn sample_ivf_training_chunk(
    dataset: &Dataset,
    column: &str,
    sample_size_hint: usize,
    metric_type: MetricType,
    fragment_ids: Option<&[u32]>,
) -> Result<(FixedSizeListArray, MetricType)> {
    let training_data =
        maybe_sample_training_data(dataset, column, sample_size_hint, fragment_ids).await?;
    let (training_data, mt) = if metric_type == MetricType::Cosine {
        let training_data = normalize_fsl_owned(training_data)?;
        (training_data, MetricType::L2)
    } else {
        (training_data, metric_type)
    };
    Ok((filter_finite_training_data(training_data)?, mt))
}

#[derive(Debug, Clone)]
struct FixedIvfTrainingRanges {
    ranges: Vec<Range<u64>>,
    num_rows: usize,
}

impl FixedIvfTrainingRanges {
    fn new(ranges: Vec<Range<u64>>) -> Self {
        let num_rows = ranges.iter().map(range_len).sum();
        Self { ranges, num_rows }
    }

    fn num_rows(&self) -> usize {
        self.num_rows
    }

    fn chunk(&self, row_offset: usize, row_count: usize) -> Vec<Range<u64>> {
        if row_count == 0 || row_offset >= self.num_rows {
            return Vec::new();
        }

        let mut remaining_skip = row_offset;
        let mut remaining_take = row_count.min(self.num_rows - row_offset);
        let mut chunk = Vec::new();
        for range in &self.ranges {
            let range_len = range_len(range);
            if remaining_skip >= range_len {
                remaining_skip -= range_len;
                continue;
            }

            let start = range.start + remaining_skip as u64;
            let available = range_len - remaining_skip;
            let take = available.min(remaining_take);
            chunk.push(start..start + take as u64);
            remaining_take -= take;
            remaining_skip = 0;
            if remaining_take == 0 {
                break;
            }
        }
        chunk
    }
}

fn range_len(range: &Range<u64>) -> usize {
    (range.end - range.start) as usize
}

const DEFAULT_STREAMING_IVF_TAKE_RANGE_ROWS: usize = 8192;
const DEFAULT_STREAMING_IVF_PREFETCH_DEPTH: usize = 1;
const DEFAULT_STREAMING_IVF_PROGRESS_INTERVAL: u64 = 128;
const STREAMING_IVF_PREFETCH_DEPTH_ENV: &str = "LANCE_STREAMING_IVF_PREFETCH_DEPTH";
const STREAMING_IVF_TAKE_RANGE_ROWS_ENV: &str = "LANCE_STREAMING_IVF_TAKE_RANGE_ROWS";
const STREAMING_IVF_PROGRESS_INTERVAL_ENV: &str = "LANCE_STREAMING_IVF_PROGRESS_INTERVAL";

fn streaming_ivf_prefetch_depth() -> usize {
    std::env::var(STREAMING_IVF_PREFETCH_DEPTH_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|depth| *depth > 0)
        .unwrap_or(DEFAULT_STREAMING_IVF_PREFETCH_DEPTH)
}

fn streaming_ivf_take_range_rows() -> usize {
    std::env::var(STREAMING_IVF_TAKE_RANGE_ROWS_ENV)
        .ok()
        .and_then(|value| value.parse::<usize>().ok())
        .filter(|rows| *rows > 0)
        .unwrap_or(DEFAULT_STREAMING_IVF_TAKE_RANGE_ROWS)
}

fn streaming_ivf_progress_interval() -> u64 {
    std::env::var(STREAMING_IVF_PROGRESS_INTERVAL_ENV)
        .ok()
        .and_then(|value| value.parse::<u64>().ok())
        .filter(|interval| *interval > 0)
        .unwrap_or(DEFAULT_STREAMING_IVF_PROGRESS_INTERVAL)
}

fn should_report_streaming_ivf_progress(total: u64, interval: u64) -> bool {
    total == 1 || total.is_multiple_of(interval.max(1))
}

fn split_ranges_by_row_count(ranges: &[Range<u64>], max_rows: usize) -> Vec<Range<u64>> {
    let max_rows = max_rows.max(1) as u64;
    let mut split = Vec::new();
    for range in ranges {
        let mut start = range.start;
        while start < range.end {
            let end = (start + max_rows).min(range.end);
            split.push(start..end);
            start = end;
        }
    }
    split
}

fn generate_fixed_training_ranges(
    num_rows: usize,
    sample_size: usize,
    block_size: usize,
    byte_width: usize,
) -> FixedIvfTrainingRanges {
    let sample_size = num_rows.min(sample_size);
    if sample_size == 0 {
        return FixedIvfTrainingRanges::new(Vec::new());
    }
    if sample_size >= num_rows {
        return FixedIvfTrainingRanges::new(vec![0..num_rows as u64]);
    }

    let rows_per_range = 1.max(block_size / byte_width);
    let num_bins = num_rows.div_ceil(rows_per_range);
    let mut rng = SmallRng::seed_from_u64(0x1a6c_e5eed);

    let bins = if sample_size * 5 >= num_rows {
        let mut bins = (0..num_bins).collect::<Vec<_>>();
        for i in 0..num_bins {
            let j = rng.random_range(i..num_bins);
            bins.swap(i, j);
        }
        bins
    } else {
        let mut bins = Vec::with_capacity(sample_size.div_ceil(rows_per_range).saturating_add(1));
        let mut seen = HashSet::with_capacity(bins.capacity());
        while bins.len() * rows_per_range < sample_size {
            let bin = rng.random_range(0..num_bins);
            if seen.insert(bin) {
                bins.push(bin);
            }
        }
        bins
    };

    let mut remaining = sample_size;
    let mut ranges = Vec::new();
    for bin in bins {
        if remaining == 0 {
            break;
        }
        let bin_start = bin * rows_per_range;
        let bin_end = ((bin + 1) * rows_per_range).min(num_rows);
        let bin_len = bin_end - bin_start;
        if bin_len == 0 {
            continue;
        }

        let take = bin_len.min(remaining);
        let offset = if take < bin_len {
            rng.random_range(0..=bin_len - take)
        } else {
            0
        };
        let start = bin_start + offset;
        ranges.push(start as u64..(start + take) as u64);
        remaining -= take;
    }

    ranges.sort_unstable_by_key(|range| range.start);
    let mut merged: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.is_empty() {
            continue;
        }
        if let Some(last) = merged.last_mut()
            && last.end >= range.start
        {
            last.end = last.end.max(range.end);
            continue;
        }
        merged.push(range);
    }
    FixedIvfTrainingRanges::new(merged)
}

fn default_streaming_coreset_rate(total_sample_rate: usize, streaming_sample_rate: usize) -> usize {
    total_sample_rate.min(streaming_sample_rate).min(64)
}

fn streaming_coreset_rate(
    total_sample_rate: usize,
    streaming_sample_rate: usize,
    configured_coreset_rate: Option<usize>,
) -> usize {
    configured_coreset_rate
        .unwrap_or_else(|| default_streaming_coreset_rate(total_sample_rate, streaming_sample_rate))
        .min(total_sample_rate)
        .max(1)
}

fn streaming_local_coreset_k(
    num_partitions: usize,
    step_sample_size: usize,
    coreset_rate: usize,
    total_steps: usize,
    decoupled_coreset_budget: bool,
) -> usize {
    if !decoupled_coreset_budget {
        return num_partitions.min(step_sample_size);
    }
    num_partitions
        .saturating_mul(coreset_rate)
        .div_ceil(total_steps.max(1))
        .max(num_partitions)
        .min(step_sample_size)
}

fn get_top_level_vector_column(batch: &RecordBatch, column: &str) -> Result<ArrayRef> {
    batch.column_by_name(column).cloned().ok_or_else(|| {
        Error::index(format!(
            "Fixed streaming IVF sampling only supports top-level vector column '{}'",
            column
        ))
    })
}

fn append_fsl_values(
    values_buf: &mut MutableBuffer,
    total_rows: &mut usize,
    array: &ArrayRef,
    byte_width: usize,
) -> Result<()> {
    let fsl = array.as_fixed_size_list();
    let values = fsl.values();
    let values_data = values.to_data();
    let elem_size = byte_width / fsl.value_length() as usize;
    let offset_bytes = values_data.offset() * elem_size;
    let total_bytes = fsl.len() * byte_width;
    let buf = &values_data.buffers()[0].as_slice()[offset_bytes..offset_bytes + total_bytes];
    values_buf.extend_from_slice(buf);
    *total_rows += fsl.len();
    Ok(())
}

fn fsl_values_to_fixed_array(
    vector_type: &DataType,
    values_buf: MutableBuffer,
    rows: usize,
) -> Result<FixedSizeListArray> {
    let DataType::FixedSizeList(field, dimension) = vector_type else {
        return Err(Error::invalid_input(format!(
            "expected FixedSizeList vector type, got {}",
            vector_type
        )));
    };
    let value_len = rows * *dimension as usize;
    let values = arrow_array::make_array(
        ArrayData::builder(field.data_type().clone())
            .len(value_len)
            .add_buffer(values_buf.into())
            .build()?,
    );
    Ok(FixedSizeListArray::try_new(
        field.clone(),
        *dimension,
        values,
        None,
    )?)
}

struct FixedIvfTrainingSampler<'a> {
    dataset: &'a Dataset,
    column: &'a str,
    vector_type: DataType,
    projection: Arc<lance_core::datatypes::Schema>,
    byte_width: usize,
}

impl<'a> FixedIvfTrainingSampler<'a> {
    fn try_new(dataset: &'a Dataset, column: &'a str) -> Result<Option<Self>> {
        let vector_field = dataset.schema().field(column).ok_or(Error::index(format!(
            "Sample training data: column {} does not exist in schema",
            column
        )))?;
        if vector_field.nullable
            || !matches!(vector_field.data_type(), DataType::FixedSizeList(_, _))
        {
            return Ok(None);
        }
        Ok(Some(Self {
            dataset,
            column,
            vector_type: vector_field.data_type(),
            projection: Arc::new(dataset.schema().project(&[column])?),
            byte_width: vector_field
                .data_type()
                .byte_width_opt()
                .unwrap_or(4 * 1024),
        }))
    }

    async fn sample_ranges(
        &self,
        ranges: &[Range<u64>],
        metric_type: MetricType,
    ) -> Result<(FixedSizeListArray, MetricType)> {
        let rows = ranges.iter().map(range_len).sum::<usize>();
        let mut values_buf = MutableBuffer::with_capacity(rows * self.byte_width);
        let mut total_rows = 0;

        let read_ranges = split_ranges_by_row_count(ranges, streaming_ivf_take_range_rows());
        let range_stream = stream::iter(read_ranges.into_iter().map(Ok));
        let batch_readahead = streaming_ivf_prefetch_depth();
        let mut batch_stream = self.dataset.take_scan(
            Box::pin(range_stream),
            self.projection.clone(),
            batch_readahead,
        );
        while let Some(batch) = batch_stream.try_next().await? {
            let array = get_top_level_vector_column(&batch, self.column)?;
            append_fsl_values(&mut values_buf, &mut total_rows, &array, self.byte_width)?;
        }

        let training_data = fsl_values_to_fixed_array(&self.vector_type, values_buf, total_rows)?;
        let (training_data, mt) = if metric_type == MetricType::Cosine {
            let training_data = normalize_fsl_owned(training_data)?;
            (training_data, MetricType::L2)
        } else {
            (training_data, metric_type)
        };
        Ok((filter_finite_training_data(training_data)?, mt))
    }
}

type KMeansProgressCallback = Arc<dyn Fn(u32, u32) + Send + Sync>;

struct KMeansStepOptions {
    dimension: usize,
    metric_type: MetricType,
    num_partitions: usize,
    sample_rate: usize,
    max_iters: usize,
    on_progress: KMeansProgressCallback,
}

fn train_ivf_kmeans_step<T: ArrowPrimitiveType>(
    centroids: Option<Arc<FixedSizeListArray>>,
    data: &PrimitiveArray<T>,
    options: &KMeansStepOptions,
) -> Result<KMeans>
where
    <T as ArrowPrimitiveType>::Native: Dot + L2 + Normalize,
    PrimitiveArray<T>: From<Vec<T::Native>>,
{
    let has_centroids = centroids.is_some();
    let mut kmeans_params =
        KMeansParams::new(centroids, options.max_iters as u32, 1, options.metric_type)
            .with_balance_factor(1.0)
            .with_on_progress(options.on_progress.clone());
    if has_centroids {
        // Incremental refinement already has the full centroid set.  The
        // hierarchical trainer bootstraps a smaller tree and is only suitable
        // for the initial training pass.
        kmeans_params = kmeans_params.with_hierarchical_k(1);
    }
    lance_index::vector::kmeans::train_kmeans::<T>(
        data,
        kmeans_params,
        options.dimension,
        options.num_partitions,
        options.sample_rate,
    )
}

fn train_ivf_kmeans_step_arrow_array_no_loss(
    centroids: Option<Arc<FixedSizeListArray>>,
    data: &FixedSizeListArray,
    metric_type: MetricType,
    num_partitions: usize,
    sample_rate: usize,
    max_iters: usize,
    on_progress: Arc<dyn Fn(u32, u32) + Send + Sync>,
) -> Result<KMeans> {
    let dimension = data.value_length() as usize;
    let values = data.values();
    let step_options = KMeansStepOptions {
        dimension,
        metric_type,
        num_partitions,
        sample_rate,
        max_iters,
        on_progress,
    };
    let kmeans = match (values.data_type(), metric_type) {
        (DataType::Float16, _) => train_ivf_kmeans_step::<Float16Type>(
            centroids,
            values.as_primitive::<Float16Type>(),
            &step_options,
        )?,
        (DataType::Float32, _) => train_ivf_kmeans_step::<Float32Type>(
            centroids,
            values.as_primitive::<Float32Type>(),
            &step_options,
        )?,
        (DataType::Float64, _) => train_ivf_kmeans_step::<Float64Type>(
            centroids,
            values.as_primitive::<Float64Type>(),
            &step_options,
        )?,
        (DataType::Int8, DistanceType::L2)
        | (DataType::Int8, DistanceType::Dot)
        | (DataType::Int8, DistanceType::Cosine) => {
            let data = data.convert_to_floating_point()?;
            train_ivf_kmeans_step::<Float32Type>(
                centroids,
                data.values().as_primitive::<Float32Type>(),
                &step_options,
            )?
        }
        (DataType::UInt8, DistanceType::Hamming) => train_ivf_kmeans_step::<UInt8Type>(
            centroids,
            values.as_primitive::<UInt8Type>(),
            &step_options,
        )?,
        _ => Err(Error::index(format!(
            "KMeans: can not train data type {} with distance type: {}",
            values.data_type(),
            metric_type
        )))?,
    };
    Ok(kmeans)
}

fn accumulate_refine_assignments(
    data: &FixedSizeListArray,
    centroids: &FixedSizeListArray,
    cluster_sums: &mut [f32],
    cluster_weights: &mut [f64],
) -> Result<f64> {
    let dimension = data.value_length() as usize;
    let kmeans = KMeans::with_centroids(
        centroids.values().clone(),
        dimension,
        DistanceType::L2,
        f64::MAX,
    );
    let (membership, distances) = kmeans.compute_membership_and_distances(data)?;
    let data_values = data.values().as_primitive::<Float32Type>().values();
    let mut loss = 0.0;

    for row_idx in 0..data.len() {
        let (Some(cluster_id), Some(distance)) = (membership[row_idx], distances[row_idx]) else {
            continue;
        };
        let cluster_id = cluster_id as usize;
        cluster_weights[cluster_id] += 1.0;
        loss += distance as f64;
        let vector = &data_values[row_idx * dimension..(row_idx + 1) * dimension];
        let sum = &mut cluster_sums[cluster_id * dimension..(cluster_id + 1) * dimension];
        for (sum, value) in sum.iter_mut().zip(vector) {
            *sum += *value;
        }
    }

    Ok(loss)
}

fn update_refined_centroids(
    centroids: &FixedSizeListArray,
    cluster_sums: &[f32],
    cluster_weights: &[f64],
) -> Result<FixedSizeListArray> {
    let dimension = centroids.value_length() as usize;
    let mut next = centroids
        .values()
        .as_primitive::<Float32Type>()
        .values()
        .to_vec();
    for cluster_id in 0..centroids.len() {
        let weight = cluster_weights[cluster_id];
        if weight <= 0.0 {
            continue;
        }
        let centroid = &mut next[cluster_id * dimension..(cluster_id + 1) * dimension];
        let sum = &cluster_sums[cluster_id * dimension..(cluster_id + 1) * dimension];
        for (value, sum) in centroid.iter_mut().zip(sum) {
            *value = *sum / weight as f32;
        }
    }
    f32_fsl_from_values(next, dimension)
}

async fn refine_streaming_f32_kmeans_with_sampler(
    sampler: &FixedIvfTrainingSampler<'_>,
    metric_type: MetricType,
    streaming_sample_size: usize,
    sample_ranges: &FixedIvfTrainingRanges,
    initial_centroids: &FixedSizeListArray,
    passes: usize,
    on_progress: Arc<dyn Fn(u32, u32) + Send + Sync>,
) -> Result<FixedSizeListArray> {
    let dimension = initial_centroids.value_length() as usize;
    let mut centroids = initial_centroids.clone();
    for pass in 1..=passes {
        let mut cluster_sums = vec![0.0_f32; centroids.len() * dimension];
        let mut cluster_weights = vec![0.0_f64; centroids.len()];
        let mut loss = 0.0;
        let mut row_offset = 0;
        while row_offset < sample_ranges.num_rows() {
            let ranges = sample_ranges.chunk(row_offset, streaming_sample_size.max(1));
            row_offset += ranges.iter().map(range_len).sum::<usize>();
            let (training_data, mt) = sampler.sample_ranges(&ranges, metric_type).await?;
            let training_data = if training_data.value_type() == DataType::Float32 {
                training_data
            } else {
                training_data.convert_to_floating_point()?
            };
            if mt != DistanceType::L2 {
                return Err(Error::invalid_input(format!(
                    "streaming IVF refinement currently supports L2/Cosine training, got {}",
                    metric_type
                )));
            }
            loss += accumulate_refine_assignments(
                &training_data,
                &centroids,
                &mut cluster_sums,
                &mut cluster_weights,
            )?;
        }
        centroids = update_refined_centroids(&centroids, &cluster_sums, &cluster_weights)?;
        on_progress(pass as u32, passes as u32);
        info!(
            "Streaming IVF raw-vector refinement pass {} / {} assigned {} vectors; pre-update loss={}",
            pass,
            passes,
            cluster_weights.iter().sum::<f64>() as usize,
            loss
        );
    }
    Ok(centroids)
}

#[allow(clippy::too_many_arguments)]
async fn refine_streaming_f32_kmeans_with_resampling(
    dataset: &Dataset,
    column: &str,
    metric_type: MetricType,
    total_sample_rate: usize,
    streaming_sample_rate: usize,
    num_partitions: usize,
    initial_centroids: &FixedSizeListArray,
    fragment_ids: Option<&[u32]>,
    passes: usize,
    on_progress: Arc<dyn Fn(u32, u32) + Send + Sync>,
) -> Result<FixedSizeListArray> {
    let dimension = initial_centroids.value_length() as usize;
    let mut centroids = initial_centroids.clone();
    for pass in 1..=passes {
        let mut cluster_sums = vec![0.0_f32; centroids.len() * dimension];
        let mut cluster_weights = vec![0.0_f64; centroids.len()];
        let mut remaining_sample_rate = total_sample_rate;
        let mut loss = 0.0;
        while remaining_sample_rate > 0 {
            let step_sample_rate = remaining_sample_rate.min(streaming_sample_rate);
            let step_sample_size = num_partitions * step_sample_rate;
            let (training_data, mt) = sample_ivf_training_chunk(
                dataset,
                column,
                step_sample_size,
                metric_type,
                fragment_ids,
            )
            .await?;
            let training_data = if training_data.value_type() == DataType::Float32 {
                training_data
            } else {
                training_data.convert_to_floating_point()?
            };
            if mt != DistanceType::L2 {
                return Err(Error::invalid_input(format!(
                    "streaming IVF refinement currently supports L2/Cosine training, got {}",
                    metric_type
                )));
            }
            loss += accumulate_refine_assignments(
                &training_data,
                &centroids,
                &mut cluster_sums,
                &mut cluster_weights,
            )?;
            remaining_sample_rate -= step_sample_rate;
        }
        centroids = update_refined_centroids(&centroids, &cluster_sums, &cluster_weights)?;
        on_progress(pass as u32, passes as u32);
        info!(
            "Streaming IVF resampled raw-vector refinement pass {} / {} assigned {} vectors; pre-update loss={}",
            pass,
            passes,
            cluster_weights.iter().sum::<f64>() as usize,
            loss
        );
    }
    Ok(centroids)
}

fn f32_fsl_from_values(values: Vec<f32>, dimension: usize) -> Result<FixedSizeListArray> {
    Ok(FixedSizeListArray::try_new_from_values(
        Float32Array::from(values),
        dimension as i32,
    )?)
}

struct WeightedCoreset {
    values: Vec<f32>,
    weights: Vec<f64>,
    losses: Vec<f64>,
}

impl WeightedCoreset {
    fn new(dimension: usize, capacity: usize) -> Self {
        Self {
            values: Vec::with_capacity(capacity * dimension),
            weights: Vec::with_capacity(capacity),
            losses: Vec::with_capacity(capacity),
        }
    }

    fn len(&self) -> usize {
        self.weights.len()
    }

    fn push(&mut self, centroid: &[f32], weight: f64, loss: f64) {
        if weight <= 0.0 {
            return;
        }
        self.values.extend_from_slice(centroid);
        self.weights.push(weight);
        self.losses.push(loss);
    }

    fn append(&mut self, other: Self) {
        self.values.extend(other.values);
        self.weights.extend(other.weights);
        self.losses.extend(other.losses);
    }

    fn into_fsl_parts(self, dimension: usize) -> Result<(FixedSizeListArray, Vec<f64>, Vec<f64>)> {
        Ok((
            f32_fsl_from_values(self.values, dimension)?,
            self.weights,
            self.losses,
        ))
    }

    fn reduce_to_budget(&mut self, dimension: usize, budget: usize) {
        if self.len() <= budget {
            return;
        }
        let total_weight = self.weights.iter().sum::<f64>();
        if total_weight <= 0.0 {
            *self = Self::new(dimension, budget);
            return;
        }

        let mut weighted_sums = vec![0.0_f64; dimension];
        let mut weighted_square_sums = vec![0.0_f64; dimension];
        for (row_idx, vector) in self.values.chunks_exact(dimension).enumerate() {
            let weight = self.weights[row_idx];
            for dim in 0..dimension {
                let value = vector[dim] as f64;
                weighted_sums[dim] += weight * value;
                weighted_square_sums[dim] += weight * value * value;
            }
        }
        let split_dim = (0..dimension)
            .max_by(|left, right| {
                let left_mean = weighted_sums[*left] / total_weight;
                let right_mean = weighted_sums[*right] / total_weight;
                let left_var = weighted_square_sums[*left] / total_weight - left_mean * left_mean;
                let right_var =
                    weighted_square_sums[*right] / total_weight - right_mean * right_mean;
                left_var
                    .partial_cmp(&right_var)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .unwrap_or(0);

        let mut indices = (0..self.len()).collect::<Vec<_>>();
        indices.sort_unstable_by(|left, right| {
            self.values[left * dimension + split_dim]
                .partial_cmp(&self.values[right * dimension + split_dim])
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.cmp(right))
        });

        let mut reduced = Self::new(dimension, budget);
        for group_idx in 0..budget {
            let group_start = group_idx * indices.len() / budget;
            let group_end = (group_idx + 1) * indices.len() / budget;
            if group_start == group_end {
                continue;
            }
            let mut weight_sum = 0.0;
            let centroid_start = reduced.values.len();
            reduced.values.resize(centroid_start + dimension, 0.0);
            {
                let centroid = &mut reduced.values[centroid_start..centroid_start + dimension];
                for &idx in &indices[group_start..group_end] {
                    let weight = self.weights[idx];
                    weight_sum += weight;
                    let vector = &self.values[idx * dimension..(idx + 1) * dimension];
                    for (sum, value) in centroid.iter_mut().zip(vector) {
                        *sum += *value * weight as f32;
                    }
                }
            }
            if weight_sum <= 0.0 {
                reduced.values.truncate(centroid_start);
                continue;
            }
            {
                let centroid = &mut reduced.values[centroid_start..centroid_start + dimension];
                for value in centroid {
                    *value /= weight_sum as f32;
                }
            }

            let mut loss = 0.0;
            let centroid = &reduced.values[centroid_start..centroid_start + dimension];
            for &idx in &indices[group_start..group_end] {
                let vector = &self.values[idx * dimension..(idx + 1) * dimension];
                let dist = vector
                    .iter()
                    .zip(centroid)
                    .map(|(left, right)| {
                        let diff = left - right;
                        diff * diff
                    })
                    .sum::<f32>() as f64;
                loss += self.losses[idx] + self.weights[idx] * dist;
            }
            reduced.weights.push(weight_sum);
            reduced.losses.push(loss);
        }
        *self = reduced;
    }
}

struct WeightedKMeansResult {
    centroids: Vec<f32>,
    membership: Vec<Option<u32>>,
    cluster_weights: Vec<f64>,
    cluster_losses: Vec<f64>,
    loss: f64,
}

fn initialize_weighted_centroids(
    data_values: &[f32],
    dimension: usize,
    k: usize,
    n: usize,
    weights: &[f64],
) -> Vec<f32> {
    let mut rng = SmallRng::seed_from_u64(0x1f17_5eed);
    let mut centroids = Vec::with_capacity(k * dimension);
    let mut selected = vec![false; n];
    let total_weight = weights.iter().copied().sum::<f64>();
    let first = if total_weight > 0.0 {
        let mut threshold = rng.random::<f64>() * total_weight;
        let mut row_idx = 0;
        for (idx, weight) in weights.iter().enumerate() {
            threshold -= *weight;
            if threshold <= 0.0 {
                row_idx = idx;
                break;
            }
        }
        row_idx
    } else {
        0
    };
    selected[first] = true;
    centroids.extend_from_slice(&data_values[first * dimension..(first + 1) * dimension]);

    let mut min_distances = vec![f64::MAX; n];
    while centroids.len() / dimension < k {
        let last_centroid = &centroids[centroids.len() - dimension..centroids.len()];
        for row_idx in 0..n {
            if selected[row_idx] {
                min_distances[row_idx] = 0.0;
                continue;
            }
            let vector = &data_values[row_idx * dimension..(row_idx + 1) * dimension];
            let distance = vector
                .iter()
                .zip(last_centroid)
                .map(|(left, right)| {
                    let diff = left - right;
                    diff * diff
                })
                .sum::<f32>() as f64;
            min_distances[row_idx] = min_distances[row_idx].min(distance);
        }

        let weighted_distance_sum = min_distances
            .iter()
            .zip(weights)
            .map(|(distance, weight)| distance * weight)
            .sum::<f64>();
        let next = if weighted_distance_sum > 0.0 {
            let mut threshold = rng.random::<f64>() * weighted_distance_sum;
            let mut row_idx = None;
            for idx in 0..n {
                if selected[idx] {
                    continue;
                }
                threshold -= min_distances[idx] * weights[idx];
                if threshold <= 0.0 {
                    row_idx = Some(idx);
                    break;
                }
            }
            row_idx
        } else {
            None
        }
        .or_else(|| (0..n).find(|idx| !selected[*idx]));

        let Some(next) = next else {
            break;
        };
        selected[next] = true;
        centroids.extend_from_slice(&data_values[next * dimension..(next + 1) * dimension]);
    }

    while centroids.len() / dimension < k {
        let row_idx = (centroids.len() / dimension) * n / k;
        centroids.extend_from_slice(&data_values[row_idx * dimension..(row_idx + 1) * dimension]);
    }
    centroids
}

fn assign_weighted_f32_points(
    data: &FixedSizeListArray,
    weights: &[f64],
    base_losses: &[f64],
    centroid_values: &[f32],
    metric_type: MetricType,
) -> Result<WeightedKMeansResult> {
    let dimension = data.value_length() as usize;
    let k = centroid_values.len() / dimension;
    let centroids = Arc::new(Float32Array::from(centroid_values.to_vec())) as ArrayRef;
    let kmeans = KMeans::with_centroids(centroids, dimension, metric_type, f64::MAX);
    let (membership, distances) = kmeans.compute_membership_and_distances(data)?;
    let data_values = data.values().as_primitive::<Float32Type>().values();
    let mut centroid_sums = vec![0.0_f32; k * dimension];
    let mut cluster_weights = vec![0.0; k];
    let mut cluster_losses = vec![0.0; k];

    for row_idx in 0..data.len() {
        let Some(cluster_id) = membership[row_idx] else {
            continue;
        };
        let Some(distance) = distances[row_idx] else {
            continue;
        };
        let cluster_id = cluster_id as usize;
        let weight = weights[row_idx];
        cluster_weights[cluster_id] += weight;
        cluster_losses[cluster_id] += base_losses[row_idx] + weight * distance as f64;
        let vector = &data_values[row_idx * dimension..(row_idx + 1) * dimension];
        let centroid_sum = &mut centroid_sums[cluster_id * dimension..(cluster_id + 1) * dimension];
        for (sum, value) in centroid_sum.iter_mut().zip(vector) {
            *sum += *value * weight as f32;
        }
    }

    let mut next_centroids = vec![0.0_f32; k * dimension];
    for cluster_id in 0..k {
        let next_centroid =
            &mut next_centroids[cluster_id * dimension..(cluster_id + 1) * dimension];
        if cluster_weights[cluster_id] > 0.0 {
            let centroid_sum = &centroid_sums[cluster_id * dimension..(cluster_id + 1) * dimension];
            for (value, sum) in next_centroid.iter_mut().zip(centroid_sum) {
                *value = *sum / cluster_weights[cluster_id] as f32;
            }
        } else {
            next_centroid.copy_from_slice(
                &centroid_values[cluster_id * dimension..(cluster_id + 1) * dimension],
            );
        }
    }

    let loss = cluster_losses.iter().sum();
    Ok(WeightedKMeansResult {
        centroids: next_centroids,
        membership,
        cluster_weights,
        cluster_losses,
        loss,
    })
}

fn train_weighted_f32_kmeans(
    data: &FixedSizeListArray,
    weights: &[f64],
    base_losses: &[f64],
    k: usize,
    metric_type: MetricType,
    max_iters: usize,
    on_progress: Arc<dyn Fn(u32, u32) + Send + Sync>,
) -> Result<WeightedKMeansResult> {
    if data.len() < k {
        return Err(Error::invalid_input(format!(
            "weighted kmeans requires at least {k} coreset rows, got {}",
            data.len()
        )));
    }
    if weights.len() != data.len() || base_losses.len() != data.len() {
        return Err(Error::invalid_input(format!(
            "weighted kmeans input lengths do not match: data={}, weights={}, losses={}",
            data.len(),
            weights.len(),
            base_losses.len()
        )));
    }

    let dimension = data.value_length() as usize;
    let data_values = data.values().as_primitive::<Float32Type>().values();
    let mut centroids =
        initialize_weighted_centroids(data_values, dimension, k, data.len(), weights);
    let mut previous_loss = f64::MAX;
    let max_iters = max_iters.max(1);
    for iter in 1..=max_iters {
        on_progress(iter as u32, max_iters as u32);
        let mut result =
            assign_weighted_f32_points(data, weights, base_losses, &centroids, metric_type)?;
        let converged = (previous_loss - result.loss).abs() < 1e-4 * result.loss.max(1.0);
        previous_loss = result.loss;
        if converged || iter == max_iters {
            return Ok(result);
        }
        centroids = std::mem::take(&mut result.centroids);
    }
    unreachable!("weighted kmeans runs at least one iteration")
}

fn refine_weighted_f32_kmeans(
    data: &FixedSizeListArray,
    weights: &[f64],
    base_losses: &[f64],
    initial_centroids: &FixedSizeListArray,
    metric_type: MetricType,
    max_iters: usize,
    on_progress: Arc<dyn Fn(u32, u32) + Send + Sync>,
) -> Result<WeightedKMeansResult> {
    let mut centroids = initial_centroids
        .values()
        .as_primitive::<Float32Type>()
        .values()
        .to_vec();
    let mut previous_loss = f64::MAX;
    let max_iters = max_iters.max(1);
    for iter in 1..=max_iters {
        on_progress(iter as u32, max_iters as u32);
        let mut result =
            assign_weighted_f32_points(data, weights, base_losses, &centroids, metric_type)?;
        let converged = (previous_loss - result.loss).abs() < 1e-4 * result.loss.max(1.0);
        previous_loss = result.loss;
        if converged || iter == max_iters {
            return Ok(result);
        }
        centroids = std::mem::take(&mut result.centroids);
    }
    unreachable!("weighted kmeans refinement runs at least one iteration")
}

fn append_local_coreset(
    coreset: &mut WeightedCoreset,
    data: &FixedSizeListArray,
    metric_type: MetricType,
    local_k: usize,
    max_iters: usize,
    on_progress: Arc<dyn Fn(u32, u32) + Send + Sync>,
) -> Result<()> {
    let dimension = data.value_length() as usize;
    let sample_rate = data.len().div_ceil(local_k).max(1);
    let kmeans = train_ivf_kmeans_step_arrow_array_no_loss(
        None,
        data,
        metric_type,
        local_k,
        sample_rate,
        max_iters,
        on_progress,
    )?;
    let centroids = FixedSizeListArray::try_new_from_values(kmeans.centroids, dimension as i32)?;
    let kmeans =
        KMeans::with_centroids(centroids.values().clone(), dimension, metric_type, f64::MAX);
    let (membership, distances) = kmeans.compute_membership_and_distances(data)?;
    let mut weights = vec![0.0; centroids.len()];
    let mut losses = vec![0.0; centroids.len()];
    for (member, distance) in membership.into_iter().zip(distances) {
        let (Some(member), Some(distance)) = (member, distance) else {
            continue;
        };
        weights[member as usize] += 1.0;
        losses[member as usize] += distance as f64;
    }

    let centroid_values = centroids.values().as_primitive::<Float32Type>().values();
    for centroid_idx in 0..centroids.len() {
        coreset.push(
            &centroid_values[centroid_idx * dimension..(centroid_idx + 1) * dimension],
            weights[centroid_idx],
            losses[centroid_idx],
        );
    }
    Ok(())
}

#[derive(Clone, Debug)]
struct WeightedCluster {
    id: usize,
    indices: Vec<usize>,
    centroid: Vec<f32>,
    weight: f64,
    loss: f64,
    finalized: bool,
}

impl Eq for WeightedCluster {}

impl PartialEq for WeightedCluster {
    fn eq(&self, other: &Self) -> bool {
        self.loss == other.loss && self.weight == other.weight
    }
}

impl Ord for WeightedCluster {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self.finalized, other.finalized) {
            (false, true) => std::cmp::Ordering::Greater,
            (true, false) => std::cmp::Ordering::Less,
            _ => self
                .loss
                .partial_cmp(&other.loss)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| {
                    self.weight
                        .partial_cmp(&other.weight)
                        .unwrap_or(std::cmp::Ordering::Equal)
                }),
        }
    }
}

impl PartialOrd for WeightedCluster {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

struct WeightedHierarchicalKMeansParams {
    dimension: usize,
    target_k: usize,
    metric_type: MetricType,
    max_iters: usize,
    on_progress: Arc<dyn Fn(u32, u32) + Send + Sync>,
}

fn weighted_subset(
    data_values: &[f32],
    weights: &[f64],
    losses: &[f64],
    indices: &[usize],
    dimension: usize,
) -> Result<(FixedSizeListArray, Vec<f64>, Vec<f64>)> {
    let mut values = Vec::with_capacity(indices.len() * dimension);
    let mut subset_weights = Vec::with_capacity(indices.len());
    let mut subset_losses = Vec::with_capacity(indices.len());
    for &idx in indices {
        values.extend_from_slice(&data_values[idx * dimension..(idx + 1) * dimension]);
        subset_weights.push(weights[idx]);
        subset_losses.push(losses[idx]);
    }
    Ok((
        f32_fsl_from_values(values, dimension)?,
        subset_weights,
        subset_losses,
    ))
}

fn train_weighted_hierarchical_f32_kmeans(
    data: &FixedSizeListArray,
    weights: &[f64],
    losses: &[f64],
    params: &WeightedHierarchicalKMeansParams,
) -> Result<FixedSizeListArray> {
    if data.len() == 0 {
        return Err(Error::index("empty weighted coreset"));
    }
    if weights.len() != data.len() || losses.len() != data.len() {
        return Err(Error::invalid_input(format!(
            "weighted hierarchical kmeans input lengths do not match: data={}, weights={}, losses={}",
            data.len(),
            weights.len(),
            losses.len()
        )));
    }

    let dimension = params.dimension;
    let target_k = params.target_k;
    let metric_type = params.metric_type;
    let max_iters = params.max_iters;
    let initial_k = 16_usize.min(target_k).min(data.len()).max(1);
    let initial = train_weighted_f32_kmeans(
        data,
        weights,
        losses,
        initial_k,
        metric_type,
        max_iters,
        params.on_progress.clone(),
    )?;

    let centroids = initial.centroids;
    let mut heap = std::collections::BinaryHeap::new();
    let mut next_cluster_id = 0;
    for cluster_id in 0..initial_k {
        let mut indices = Vec::new();
        for (row_idx, member) in initial.membership.iter().enumerate() {
            if member.is_some_and(|member| member as usize == cluster_id) {
                indices.push(row_idx);
            }
        }
        if !indices.is_empty() {
            heap.push(WeightedCluster {
                id: next_cluster_id,
                indices,
                centroid: centroids[cluster_id * dimension..(cluster_id + 1) * dimension].to_vec(),
                weight: initial.cluster_weights[cluster_id],
                loss: initial.cluster_losses[cluster_id],
                finalized: false,
            });
            next_cluster_id += 1;
        }
    }

    let data_values = data.values().as_primitive::<Float32Type>().values();
    while heap.len() < target_k {
        let mut cluster = heap
            .pop()
            .ok_or_else(|| Error::index("No weighted cluster can be further split"))?;
        if cluster.finalized || cluster.indices.len() <= 1 {
            cluster.finalized = true;
            heap.push(cluster);
            break;
        }

        let remaining_k = target_k - heap.len();
        let cluster_k = if cluster.indices.len() <= 16 {
            2.min(remaining_k).min(cluster.indices.len())
        } else {
            (cluster.indices.len() / 16).min(remaining_k).clamp(2, 16)
        };
        let (sub_data, sub_weights, sub_losses) =
            weighted_subset(data_values, weights, losses, &cluster.indices, dimension)?;
        let split = train_weighted_f32_kmeans(
            &sub_data,
            &sub_weights,
            &sub_losses,
            cluster_k,
            metric_type,
            max_iters.min(20),
            params.on_progress.clone(),
        )?;

        let mut assignments = vec![Vec::new(); cluster_k];
        let mut first_member = None;
        let mut all_same = true;
        for (local_idx, member) in split.membership.iter().enumerate() {
            let Some(member) = member else {
                continue;
            };
            if first_member.is_some_and(|first| first != *member) {
                all_same = false;
            } else if first_member.is_none() {
                first_member = Some(*member);
            }
            assignments[*member as usize].push(cluster.indices[local_idx]);
        }
        if all_same {
            cluster.finalized = true;
            heap.push(cluster);
            continue;
        }

        for (child_id, child_indices) in assignments.into_iter().enumerate() {
            if child_indices.is_empty() {
                continue;
            }
            heap.push(WeightedCluster {
                id: next_cluster_id,
                indices: child_indices,
                centroid: split.centroids[child_id * dimension..(child_id + 1) * dimension]
                    .to_vec(),
                weight: split.cluster_weights[child_id],
                loss: split.cluster_losses[child_id],
                finalized: false,
            });
            next_cluster_id += 1;
        }
    }

    let mut clusters = heap.into_vec();
    clusters.sort_by_key(|cluster| cluster.id);
    while clusters.len() < target_k {
        let duplicate = clusters
            .iter()
            .max_by(|left, right| {
                left.weight
                    .partial_cmp(&right.weight)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .cloned()
            .ok_or_else(|| Error::index("No weighted clusters were trained"))?;
        clusters.push(WeightedCluster {
            id: next_cluster_id,
            ..duplicate
        });
        next_cluster_id += 1;
    }
    clusters.truncate(target_k);

    let mut values = Vec::with_capacity(target_k * dimension);
    for cluster in clusters {
        values.extend_from_slice(&cluster.centroid);
    }
    f32_fsl_from_values(values, dimension)
}

async fn train_streaming_coreset_ivf_model(
    dataset: &Dataset,
    column: &str,
    dimension: usize,
    metric_type: MetricType,
    params: &IvfBuildParams,
    fragment_ids: Option<&[u32]>,
    progress: std::sync::Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<IvfModel> {
    let num_partitions = params.num_partitions.unwrap_or(32);
    let streaming_sample_rate = params.streaming_sample_rate.unwrap();
    let total_sample_rate = params.sample_rate;
    let mut remaining_sample_rate = total_sample_rate;
    let mut max_training_vectors = 0;
    let mut total_training_vectors = 0;
    let fixed_sampler = if fragment_ids.is_none() {
        FixedIvfTrainingSampler::try_new(dataset, column)?
    } else {
        None
    };
    let fixed_sample_ranges = if let Some(sampler) = &fixed_sampler {
        let num_rows = dataset.count_rows(None).await?;
        let sample_size = num_rows.min(num_partitions * total_sample_rate);
        Some(generate_fixed_training_ranges(
            num_rows,
            sample_size,
            dataset.object_store.as_ref().block_size(),
            sampler.byte_width,
        ))
    } else {
        None
    };
    let mut sample_offset = 0;

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<u64>();
    let progress_worker = {
        let progress = progress.clone();
        tokio::spawn(async move {
            while let Some(iter) = progress_rx.recv().await {
                if let Err(e) = progress.stage_progress("train_ivf", iter).await {
                    warn!("Progress callback error during train_ivf: {e}");
                }
            }
        })
    };

    let on_progress: Arc<dyn Fn(u32, u32) + Send + Sync> = {
        let progress_tx = progress_tx.clone();
        let cumulative_iters = std::sync::atomic::AtomicU64::new(0);
        let progress_interval = streaming_ivf_progress_interval();
        Arc::new(move |_iter: u32, _max_iters: u32| {
            let total = cumulative_iters.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            if should_report_streaming_ivf_progress(total, progress_interval) {
                let _ = progress_tx.send(total);
            }
        })
    };

    let coreset_rate = streaming_coreset_rate(
        total_sample_rate,
        streaming_sample_rate,
        params.streaming_coreset_rate,
    );
    let coreset_budget = num_partitions
        .saturating_mul(coreset_rate)
        .max(num_partitions);
    let total_steps = total_sample_rate.div_ceil(streaming_sample_rate);
    let decoupled_coreset_budget = params.streaming_coreset_rate.is_some();
    let mut coreset = WeightedCoreset::new(dimension, coreset_budget.min(num_partitions * 16));
    let mut step = 0;
    while remaining_sample_rate > 0 {
        let step_sample_rate = remaining_sample_rate.min(streaming_sample_rate);
        let step_sample_size = num_partitions * step_sample_rate;
        step += 1;
        info!(
            "Streaming coreset IVF training: step {}, sample_rate={}, sample_size={}",
            step, step_sample_rate, step_sample_size
        );

        let (training_data, mt) = if let (Some(sample_ranges), Some(sampler)) =
            (&fixed_sample_ranges, &fixed_sampler)
        {
            let ranges = sample_ranges.chunk(sample_offset, step_sample_size);
            sample_offset += ranges.iter().map(range_len).sum::<usize>();
            sampler.sample_ranges(&ranges, metric_type).await?
        } else {
            sample_ivf_training_chunk(dataset, column, step_sample_size, metric_type, fragment_ids)
                .await?
        };
        let training_data = if training_data.value_type() == DataType::Float32 {
            training_data
        } else {
            training_data.convert_to_floating_point()?
        };
        if mt != DistanceType::L2 {
            return Err(Error::invalid_input(format!(
                "streaming coreset IVF currently supports L2/Cosine training, got {}",
                metric_type
            )));
        }
        if training_data.len() < num_partitions {
            return Err(Error::index(format!(
                "Not enough training vectors for streaming coreset IVF. Requires at least {} rows but sampled {} rows",
                num_partitions,
                training_data.len()
            )));
        }

        max_training_vectors = max_training_vectors.max(training_data.len());
        total_training_vectors += training_data.len();
        let local_k = streaming_local_coreset_k(
            num_partitions,
            training_data.len(),
            coreset_rate,
            total_steps,
            decoupled_coreset_budget,
        );
        let mut chunk_coreset = WeightedCoreset::new(dimension, local_k);
        append_local_coreset(
            &mut chunk_coreset,
            &training_data,
            mt,
            local_k,
            params.max_iters,
            on_progress.clone(),
        )?;
        coreset.append(chunk_coreset);
        coreset.reduce_to_budget(dimension, coreset_budget);
        info!(
            "Streaming coreset IVF step {} compressed {} vectors into {} weighted centroids",
            step,
            total_training_vectors,
            coreset.len()
        );
        remaining_sample_rate -= step_sample_rate;
    }

    let coreset_len = coreset.len();
    let (coreset_data, coreset_weights, coreset_losses) = coreset.into_fsl_parts(dimension)?;
    // Scope `weighted_hierarchical_params` so the `on_progress` clone it holds
    // (which owns a clone of `progress_tx`) is dropped as soon as training
    // returns. Otherwise it would outlive the `progress_worker.await` below,
    // keeping a channel sender alive so `progress_rx.recv()` never returns
    // `None` and the progress worker — and thus this function — hangs forever.
    let mut centroids = {
        let weighted_hierarchical_params = WeightedHierarchicalKMeansParams {
            dimension,
            target_k: num_partitions,
            metric_type: DistanceType::L2,
            max_iters: params.max_iters,
            on_progress: on_progress.clone(),
        };
        train_weighted_hierarchical_f32_kmeans(
            &coreset_data,
            &coreset_weights,
            &coreset_losses,
            &weighted_hierarchical_params,
        )?
    };
    let refine_iters = 3;
    if refine_iters > 0 {
        let refined = refine_weighted_f32_kmeans(
            &coreset_data,
            &coreset_weights,
            &coreset_losses,
            &centroids,
            DistanceType::L2,
            refine_iters,
            on_progress.clone(),
        )?;
        centroids = f32_fsl_from_values(refined.centroids, dimension)?;
    }
    if params.streaming_refine_passes > 0 {
        info!(
            "Running {} streaming raw-vector refinement pass(es)",
            params.streaming_refine_passes
        );
        centroids =
            if let (Some(sample_ranges), Some(sampler)) = (&fixed_sample_ranges, &fixed_sampler) {
                refine_streaming_f32_kmeans_with_sampler(
                    sampler,
                    metric_type,
                    num_partitions * streaming_sample_rate,
                    sample_ranges,
                    &centroids,
                    params.streaming_refine_passes,
                    on_progress.clone(),
                )
                .await?
            } else {
                refine_streaming_f32_kmeans_with_resampling(
                    dataset,
                    column,
                    metric_type,
                    total_sample_rate,
                    streaming_sample_rate,
                    num_partitions,
                    &centroids,
                    fragment_ids,
                    params.streaming_refine_passes,
                    on_progress.clone(),
                )
                .await?
            };
    }

    drop(progress_tx);
    drop(on_progress);
    if let Err(e) = progress_worker.await {
        warn!("Progress worker join error during train_ivf: {e}");
    }

    info!(
        "Streaming coreset IVF sampled {} vectors total; max in-memory training vectors per step: {}; coreset vectors: {}",
        total_training_vectors, max_training_vectors, coreset_len
    );

    Ok(IvfModel::new(centroids, None))
}

async fn train_streaming_ivf_model(
    dataset: &Dataset,
    column: &str,
    dimension: usize,
    metric_type: MetricType,
    params: &IvfBuildParams,
    fragment_ids: Option<&[u32]>,
    progress: std::sync::Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<IvfModel> {
    let num_partitions = params.num_partitions.unwrap_or(32);
    if num_partitions > 256 {
        return train_streaming_coreset_ivf_model(
            dataset,
            column,
            dimension,
            metric_type,
            params,
            fragment_ids,
            progress,
        )
        .await;
    }
    let streaming_sample_rate = params.streaming_sample_rate.unwrap();
    let total_sample_rate = params.sample_rate;
    let mut remaining_sample_rate = total_sample_rate;
    let mut centroids = params.centroids.clone();
    let mut max_training_vectors = 0;
    let mut total_training_vectors = 0;

    let (progress_tx, mut progress_rx) = mpsc::unbounded_channel::<u64>();
    let progress_worker = {
        let progress = progress.clone();
        tokio::spawn(async move {
            while let Some(iter) = progress_rx.recv().await {
                if let Err(e) = progress.stage_progress("train_ivf", iter).await {
                    warn!("Progress callback error during train_ivf: {e}");
                }
            }
        })
    };

    let on_progress: Arc<dyn Fn(u32, u32) + Send + Sync> = {
        let progress_tx = progress_tx.clone();
        let cumulative_iters = std::sync::atomic::AtomicU64::new(0);
        Arc::new(move |_iter: u32, _max_iters: u32| {
            let total = cumulative_iters.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
            let _ = progress_tx.send(total);
        })
    };

    let mut step = 0;
    while remaining_sample_rate > 0 {
        let step_sample_rate = remaining_sample_rate.min(streaming_sample_rate);
        let step_sample_size = num_partitions * step_sample_rate;
        step += 1;
        info!(
            "Streaming IVF training: step {}, sample_rate={}, sample_size={}",
            step, step_sample_rate, step_sample_size
        );

        let (training_data, mt) =
            sample_ivf_training_chunk(dataset, column, step_sample_size, metric_type, fragment_ids)
                .await?;
        if training_data.len() < num_partitions {
            return Err(Error::index(format!(
                "Not enough training vectors for streaming IVF. Requires at least {} rows but sampled {} rows",
                num_partitions,
                training_data.len()
            )));
        }

        max_training_vectors = max_training_vectors.max(training_data.len());
        total_training_vectors += training_data.len();
        if params.sample_rate >= 1024 && training_data.value_type() == DataType::Float16 {
            warn!(
                "Large sample_rate ({} >= 1024) for float16 vectors is possible to result in all zeros cluster centroid",
                params.sample_rate
            );
        }

        let kmeans = train_ivf_kmeans_step_arrow_array_no_loss(
            centroids.clone(),
            &training_data,
            mt,
            num_partitions,
            step_sample_rate,
            params.max_iters,
            on_progress.clone(),
        )?;
        let trained_centroids = Arc::new(FixedSizeListArray::try_new_from_values(
            kmeans.centroids,
            dimension as i32,
        )?);
        centroids = Some(trained_centroids);

        remaining_sample_rate -= step_sample_rate;
    }

    drop(progress_tx);
    drop(on_progress);
    if let Err(e) = progress_worker.await {
        warn!("Progress worker join error during train_ivf: {e}");
    }

    info!(
        "Streaming IVF training sampled {} vectors total; max in-memory training vectors per step: {}",
        total_training_vectors, max_training_vectors
    );

    let centroids = centroids.ok_or_else(|| Error::index("No IVF centroids trained"))?;
    Ok(IvfModel::new((*centroids).clone(), None))
}

/// Train IVF partitions using kmeans.
async fn train_ivf_model(
    centroids: Option<Arc<FixedSizeListArray>>,
    data: &FixedSizeListArray,
    distance_type: DistanceType,
    params: &IvfBuildParams,
    progress: std::sync::Arc<dyn lance_index::progress::IndexBuildProgress>,
) -> Result<IvfModel> {
    assert!(
        distance_type != DistanceType::Cosine,
        "Cosine metric should be done by normalized L2 distance",
    );
    let values = data.values();
    let dim = data.value_length() as usize;
    match (values.data_type(), distance_type) {
        (DataType::Float16, _) => {
            do_train_ivf_model::<Float16Type>(
                centroids,
                values.as_primitive::<Float16Type>(),
                dim,
                distance_type,
                params,
                progress.clone(),
            )
            .await
        }
        (DataType::Float32, _) => {
            do_train_ivf_model::<Float32Type>(
                centroids,
                values.as_primitive::<Float32Type>(),
                dim,
                distance_type,
                params,
                progress.clone(),
            )
            .await
        }
        (DataType::Float64, _) => {
            do_train_ivf_model::<Float64Type>(
                centroids,
                values.as_primitive::<Float64Type>(),
                dim,
                distance_type,
                params,
                progress.clone(),
            )
            .await
        }
        (DataType::Int8, DistanceType::L2)
        | (DataType::Int8, DistanceType::Dot)
        | (DataType::Int8, DistanceType::Cosine) => {
            do_train_ivf_model::<Float32Type>(
                centroids,
                data.convert_to_floating_point()?
                    .values()
                    .as_primitive::<Float32Type>(),
                dim,
                distance_type,
                params,
                progress.clone(),
            )
            .await
        }
        (DataType::UInt8, DistanceType::Hamming) => {
            do_train_ivf_model::<UInt8Type>(
                centroids,
                values.as_primitive::<UInt8Type>(),
                dim,
                distance_type,
                params,
                progress.clone(),
            )
            .await
        }
        _ => Err(Error::index(format!(
            "Unsupported data type {} with distance type {}",
            values.data_type(),
            distance_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::collections::HashSet;
    use std::iter::repeat_n;
    use std::ops::Range;

    use arrow_array::types::UInt64Type;
    use arrow_array::{
        FixedSizeListArray, Float16Array, Float32Array, RecordBatch, RecordBatchIterator,
        RecordBatchReader, UInt64Array, make_array,
    };
    use arrow_buffer::{BooleanBuffer, NullBuffer};
    use arrow_schema::{DataType, Field, Schema};
    use half::f16;
    use itertools::Itertools;
    use lance_core::ROW_ID;
    use lance_core::utils::address::RowAddress;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::{ArrayGeneratorExt, BatchCount, Dimension, RowCount, array, gen_batch};
    use lance_index::VECTOR_INDEX_VERSION;
    use lance_index::metrics::NoOpMetricsCollector;
    use lance_index::vector::sq::builder::SQBuildParams;
    use lance_linalg::distance::l2_distance_batch;
    use lance_testing::datagen::{
        generate_random_array, generate_random_array_with_range, generate_random_array_with_seed,
        generate_scaled_random_array, sample_without_replacement,
    };
    use rand::{rng, seq::SliceRandom};
    use rstest::rstest;

    use crate::dataset::{InsertBuilder, WriteMode, WriteParams};
    use crate::index::prefilter::DatasetPreFilter;
    use crate::index::vector::IndexFileVersion;
    use crate::index::vector_index_details_default;
    use crate::index::{DatasetIndexExt, DatasetIndexInternalExt, vector::VectorIndexParams};
    use crate::utils::test::copy_test_data_to_tmp;

    const DIM: usize = 32;

    #[test]
    fn test_shared_quantizer_model_compares_skipped_payloads() {
        let codebook = |offset| {
            let values = Float32Array::from_iter_values((0..32).map(|v| v as f32 + offset));
            FixedSizeListArray::try_new_from_values(values, 2).unwrap()
        };
        let pq1 = Quantizer::Product(ProductQuantizer::new(
            1,
            4,
            2,
            codebook(0.0),
            DistanceType::L2,
        ));
        let pq2 = Quantizer::Product(ProductQuantizer::new(
            1,
            4,
            2,
            codebook(100.0),
            DistanceType::L2,
        ));
        assert!(!shared_quantizer_model(&pq1, &pq2));

        let rq1 = Quantizer::Rabit(RabitQuantizer::new_with_rotation::<Float32Type>(
            1,
            8,
            lance_index::vector::bq::RQRotationType::Matrix,
        ));
        let rq2 = Quantizer::Rabit(RabitQuantizer::new_with_rotation::<Float32Type>(
            1,
            8,
            lance_index::vector::bq::RQRotationType::Matrix,
        ));
        assert!(!shared_quantizer_model(&rq1, &rq2));
    }

    async fn compute_test_ivf_loss(dataset: &Dataset, column: &str, ivf: &IvfModel) -> f64 {
        let centroids = ivf
            .centroids_array()
            .expect("test IVF model should include centroids");
        let mut scanner = dataset.scan();
        scanner.project(&[column]).unwrap();
        let batch = scanner.try_into_batch().await.unwrap();
        let data = batch
            .column_by_name(column)
            .expect("test vector column should exist")
            .as_fixed_size_list()
            .clone();
        let kmeans = KMeans::with_centroids(
            centroids.values().clone(),
            centroids.value_length() as usize,
            DistanceType::L2,
            f64::MAX,
        );
        kmeans.compute_loss(&data).unwrap()
    }

    // Verifies LANCE_INCLUDE_VECTOR_CENTROIDS env var is honored by
    // maybe_centroids_for_stats. The env var is process-global, so this test
    // is serialized against any other test that touches the same key.
    #[test]
    #[serial_test::serial(LANCE_INCLUDE_VECTOR_CENTROIDS)]
    fn test_maybe_centroids_for_stats_env_var() {
        let centroids = Float32Array::from(vec![1.0_f32, 2.0, 3.0, 4.0]);
        let centroids = FixedSizeListArray::try_new_from_values(centroids, 2).unwrap();
        let expected = Some(vec![vec![1.0, 2.0], vec![3.0, 4.0]]);

        // Save the original value so we can restore it afterwards.
        let original = std::env::var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV).ok();

        // Unset → centroids included (with one-time warning).
        unsafe {
            std::env::remove_var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV);
        }
        assert_eq!(maybe_centroids_for_stats(&centroids).unwrap(), expected);

        // Truthy values → centroids included.
        for truthy in ["1", "true", "TRUE", "on", "yes", "y"] {
            unsafe {
                std::env::set_var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV, truthy);
            }
            assert_eq!(
                maybe_centroids_for_stats(&centroids).unwrap(),
                expected,
                "expected centroids to be included for {truthy:?}",
            );
        }

        // Non-truthy values → centroids omitted.
        for falsy in ["0", "false", "FALSE", "no", "off"] {
            unsafe {
                std::env::set_var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV, falsy);
            }
            assert_eq!(
                maybe_centroids_for_stats(&centroids).unwrap(),
                None,
                "expected centroids to be omitted for {falsy:?}",
            );
        }

        // Restore original value.
        unsafe {
            match original {
                Some(v) => std::env::set_var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV, v),
                None => std::env::remove_var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV),
            }
        }
    }

    // Verifies that when centroids are omitted via the env var, the
    // serialized stats JSON does not contain the `centroids` field at all
    // (instead of an explicit null), since downstream code distinguishes
    // missing from null.
    #[test]
    #[serial_test::serial(LANCE_INCLUDE_VECTOR_CENTROIDS)]
    fn test_stats_centroids_omitted_when_disabled() {
        let original = std::env::var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV).ok();

        unsafe {
            std::env::set_var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV, "false");
        }
        let stats = IvfIndexStatistics {
            index_type: "IVF_PQ".to_string(),
            uuid: "uuid".to_string(),
            uri: "uri".to_string(),
            metric_type: "l2".to_string(),
            num_partitions: 0,
            sub_index: serde_json::Value::Null,
            partitions: vec![],
            centroids: None,
            loss: None,
            index_file_version: IndexFileVersion::V3,
        };
        let json = serde_json::to_value(&stats).unwrap();
        assert!(json.get("centroids").is_none());

        unsafe {
            match original {
                Some(v) => std::env::set_var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV, v),
                None => std::env::remove_var(LANCE_INCLUDE_VECTOR_CENTROIDS_ENV),
            }
        }
    }

    /// This goal of this function is to generate data that behaves in a very deterministic way so that
    /// we can evaluate the correctness of an IVF_PQ implementation.  Currently it is restricted to the
    /// L2 distance metric.
    ///
    /// First, we generate a set of centroids.  These are generated randomly but we ensure that is
    /// sufficient distance between each of the centroids.
    ///
    /// Then, we generate 256 vectors per centroid.  Each vector is generated by making a line by
    /// adding / subtracting [1,1,1...,1] (with the centroid in the middle)
    ///
    /// The trained result should have our generated centroids (without these centroids actually being
    /// a part of the input data) and the PQ codes for every data point should be identical and, given
    /// any three data points a, b, and c that are in the same centroid then the distance between a and
    /// b should be different than the distance between a and c.
    struct WellKnownIvfPqData {
        dim: u32,
        num_centroids: u32,
        centroids: Option<Float32Array>,
        vectors: Option<Float32Array>,
    }

    impl WellKnownIvfPqData {
        // Right now we are assuming 8-bit codes
        const VALS_PER_CODE: u32 = 256;
        const COLUMN: &'static str = "vector";

        fn new(dim: u32, num_centroids: u32) -> Self {
            Self {
                dim,
                num_centroids,
                centroids: None,
                vectors: None,
            }
        }

        fn generate_centroids(dim: u32, num_centroids: u32) -> Float32Array {
            const MAX_ATTEMPTS: u32 = 10;
            let distance_needed = (dim as f32).sqrt() * Self::VALS_PER_CODE as f32 * 2_f32;
            let mut attempts_remaining = MAX_ATTEMPTS;
            let num_values = dim * num_centroids;
            while attempts_remaining > 0 {
                // Use some biggish numbers to ensure we get the distance we want but make them positive
                // and not too big for easier debugging.
                let centroids: Float32Array =
                    generate_scaled_random_array(num_values as usize, 0_f32, 1000_f32);
                let mut broken = false;
                for (index, centroid) in centroids.values().chunks_exact(dim as usize).enumerate() {
                    let offset = (index + 1) * dim as usize;
                    let length = centroids.len() - offset;
                    if length == 0 {
                        // This will be true for the last item since we ignore comparison with self
                        continue;
                    }
                    let distances = l2_distance_batch(
                        centroid,
                        &centroids.values()[offset..offset + length],
                        dim as usize,
                    );
                    let min_distance = distances.min_by(|a, b| a.total_cmp(b)).unwrap();
                    // In theory we could just replace this one vector but, out of laziness, we just retry all of them
                    if min_distance < distance_needed {
                        broken = true;
                        break;
                    }
                }
                if !broken {
                    return centroids;
                }
                attempts_remaining -= 1;
            }
            panic!(
                "Unable to generate centroids with sufficient distance after {} attempts",
                MAX_ATTEMPTS
            );
        }

        fn get_centroids(&mut self) -> &Float32Array {
            let dim = self.dim;
            let num_centroids = self.num_centroids;
            self.centroids
                .get_or_insert_with(|| Self::generate_centroids(dim, num_centroids))
        }

        fn get_centroids_as_list_arr(&mut self) -> Arc<FixedSizeListArray> {
            Arc::new(
                FixedSizeListArray::try_new_from_values(
                    self.get_centroids().clone(),
                    self.dim as i32,
                )
                .unwrap(),
            )
        }

        fn generate_vectors(
            dim: u32,
            num_centroids: u32,
            centroids: &Float32Array,
        ) -> Float32Array {
            let dim = dim as usize;
            let mut vectors: Vec<f32> =
                vec![0_f32; Self::VALS_PER_CODE as usize * dim * num_centroids as usize];
            for (centroid, dst_batch) in centroids
                .values()
                .chunks_exact(dim)
                .zip(vectors.chunks_exact_mut(dim * Self::VALS_PER_CODE as usize))
            {
                for (offset, dst) in (-128..0).chain(1..129).zip(dst_batch.chunks_exact_mut(dim)) {
                    for (cent_val, dst_val) in centroid.iter().zip(dst) {
                        *dst_val = *cent_val + offset as f32;
                    }
                }
            }
            Float32Array::from(vectors)
        }

        fn get_vectors(&mut self) -> &Float32Array {
            let dim = self.dim;
            let num_centroids = self.num_centroids;
            let centroids = self.get_centroids().clone();
            self.vectors
                .get_or_insert_with(|| Self::generate_vectors(dim, num_centroids, &centroids))
        }

        fn get_vector(&mut self, idx: u32) -> Float32Array {
            let dim = self.dim as usize;
            let vectors = self.get_vectors();
            let start = idx as usize * dim;
            vectors.slice(start, dim)
        }

        fn generate_batches(&mut self) -> impl RecordBatchReader + Send + 'static {
            let dim = self.dim as usize;
            let vectors_array = self.get_vectors();

            let schema = Arc::new(Schema::new(vec![Field::new(
                Self::COLUMN,
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    dim as i32,
                ),
                true,
            )]));
            let array = Arc::new(
                FixedSizeListArray::try_new_from_values(vectors_array.clone(), dim as i32).unwrap(),
            );
            let batch = RecordBatch::try_new(schema.clone(), vec![array]).unwrap();
            RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema)
        }

        async fn generate_dataset(&mut self, test_uri: &str) -> Result<Dataset> {
            let batches = self.generate_batches();
            Dataset::write(batches, test_uri, None).await
        }

        async fn check_index<F: Fn(u64) -> Option<u64>>(
            &mut self,
            index: &IVFIndex,
            prefilter: Arc<dyn PreFilter>,
            ids_to_test: &[u64],
            row_id_map: F,
        ) {
            const ROWS_TO_TEST: u32 = 500;
            let num_vectors = ids_to_test.len() as u32 / self.dim;
            let num_tests = ROWS_TO_TEST.min(num_vectors);
            let row_ids_to_test = sample_without_replacement(ids_to_test, num_tests);
            for row_id in row_ids_to_test {
                let row = self.get_vector(row_id as u32);
                let query = Query {
                    column: Self::COLUMN.to_string(),
                    key: Arc::new(row),
                    k: 5,
                    lower_bound: None,
                    upper_bound: None,
                    minimum_nprobes: 1,
                    maximum_nprobes: None,
                    ef: None,
                    refine_factor: None,
                    metric_type: Some(MetricType::L2),
                    use_index: true,
                    query_parallelism: lance_index::vector::DEFAULT_QUERY_PARALLELISM,
                    dist_q_c: 0.0,
                    approx_mode: Default::default(),
                };
                let (partitions, _) = index.find_partitions(&query).unwrap();
                let nearest_partition_id = partitions.value(0) as usize;
                let search_result = index
                    .search_in_partition(
                        nearest_partition_id,
                        &query,
                        prefilter.clone(),
                        &NoOpMetricsCollector,
                    )
                    .await
                    .unwrap();

                let found_ids = search_result.column(1);
                let found_ids = found_ids.as_any().downcast_ref::<UInt64Array>().unwrap();
                let expected_id = row_id_map(row_id);

                match expected_id {
                    // Original id was deleted, results can be anything, just make sure they don't
                    // include the original id
                    None => assert!(!found_ids.iter().any(|f_id| f_id.unwrap() == row_id)),
                    // Original id remains or was remapped, make sure expected id in results
                    Some(expected_id) => {
                        assert!(found_ids.iter().any(|f_id| f_id.unwrap() == expected_id))
                    }
                };
                // The invalid row id should never show up in results
                assert!(
                    !found_ids
                        .iter()
                        .any(|f_id| f_id.unwrap() == RowAddress::TOMBSTONE_ROW)
                );
            }
        }
    }

    async fn generate_test_dataset(
        test_uri: &str,
        range: Range<f32>,
    ) -> (Dataset, Arc<FixedSizeListArray>) {
        let vectors = generate_random_array_with_range::<Float32Type>(1000 * DIM, range);
        let metadata: HashMap<String, String> = vec![("test".to_string(), "ivf_pq".to_string())]
            .into_iter()
            .collect();

        let schema = Arc::new(
            Schema::new(vec![Field::new(
                "vector",
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::Float32, true)),
                    DIM as i32,
                ),
                true,
            )])
            .with_metadata(metadata),
        );
        let array = Arc::new(FixedSizeListArray::try_new_from_values(vectors, DIM as i32).unwrap());
        let batch = RecordBatch::try_new(schema.clone(), vec![array.clone()]).unwrap();

        let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let dataset = Dataset::write(batches, test_uri, None).await.unwrap();
        (dataset, array)
    }

    #[tokio::test]
    async fn test_create_ivf_pq_with_centroids() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let (mut dataset, vector_array) = generate_test_dataset(test_uri, 0.0..1.0).await;

        let centroids = generate_random_array(2 * DIM);
        let ivf_centroids = FixedSizeListArray::try_new_from_values(centroids, DIM as i32).unwrap();
        let ivf_params = IvfBuildParams::try_with_centroids(2, Arc::new(ivf_centroids)).unwrap();

        let codebook = Arc::new(generate_random_array(256 * DIM));
        let pq_params = PQBuildParams::with_codebook(4, 8, codebook);

        let params = VectorIndexParams::with_ivf_pq_params(MetricType::L2, ivf_params, pq_params);

        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let sample_query = vector_array.value(10);
        let query = sample_query.as_primitive::<Float32Type>();
        let results = dataset
            .scan()
            .nearest("vector", query, 5)
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(1, results.len());
        assert_eq!(5, results[0].num_rows());
    }

    fn partition_ids(mut ids: Vec<u64>, num_parts: u32) -> Vec<Vec<u64>> {
        if num_parts > ids.len() as u32 {
            panic!("Not enough ids to break into {num_parts} parts");
        }
        let mut rng = rng();
        ids.shuffle(&mut rng);

        let values_per_part = ids.len() / num_parts as usize;
        let parts_with_one_extra = ids.len() % num_parts as usize;

        let mut parts = Vec::with_capacity(num_parts as usize);
        let mut offset = 0;
        for part_size in (0..num_parts).map(|part_idx| {
            if part_idx < parts_with_one_extra as u32 {
                values_per_part + 1
            } else {
                values_per_part
            }
        }) {
            parts.push(Vec::from_iter(
                ids[offset..(offset + part_size)].iter().copied(),
            ));
            offset += part_size;
        }

        parts
    }

    const BIG_OFFSET: u64 = 10000;

    fn build_mapping(
        row_ids_to_modify: &[u64],
        row_ids_to_remove: &[u64],
        max_id: u64,
    ) -> HashMap<u64, Option<u64>> {
        // Some big number we can add to row ids so they are remapped but don't intersect with anything
        if max_id > BIG_OFFSET {
            panic!("This logic will only work if the max row id is less than BIG_OFFSET");
        }
        row_ids_to_modify
            .iter()
            .copied()
            .map(|val| (val, Some(val + BIG_OFFSET)))
            .chain(row_ids_to_remove.iter().copied().map(|val| (val, None)))
            .collect()
    }

    #[tokio::test]
    async fn remap_ivf_pq_index() {
        // Use small numbers to keep runtime down
        const DIM: u32 = 8;
        const CENTROIDS: u32 = 2;
        const NUM_SUBVECTORS: u32 = 4;
        const NUM_BITS: u32 = 8;
        const INDEX_NAME: &str = "my_index";

        // In this test we create a sample dataset with reliable data, train an IVF PQ index
        // remap the rows, and then verify that we can still search the index and will get
        // back the remapped row ids.

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let mut test_data = WellKnownIvfPqData::new(DIM, CENTROIDS);

        let dataset = Arc::new(test_data.generate_dataset(test_uri).await.unwrap());
        let ivf_params = IvfBuildParams::try_with_centroids(
            CENTROIDS as usize,
            test_data.get_centroids_as_list_arr(),
        )
        .unwrap();
        let pq_params = PQBuildParams::new(NUM_SUBVECTORS as usize, NUM_BITS as usize);

        let uuid = Uuid::new_v4();

        build_ivf_pq_index(
            &dataset,
            WellKnownIvfPqData::COLUMN,
            INDEX_NAME,
            uuid,
            MetricType::L2,
            &ivf_params,
            &pq_params,
            lance_index::progress::noop_progress(),
        )
        .await
        .unwrap();

        // After building the index file, we need to register the index metadata
        // so that the dataset can find it when we try to open it
        let field = dataset.schema().field(WellKnownIvfPqData::COLUMN).unwrap();
        let index_meta = lance_table::format::IndexMetadata {
            uuid,
            dataset_version: dataset.version().version,
            fields: vec![field.id],
            name: INDEX_NAME.to_string(),
            fragment_bitmap: Some(dataset.fragment_bitmap.as_ref().clone()),
            index_details: Some(Arc::new(vector_index_details_default())),
            index_version: VECTOR_INDEX_VERSION as i32,
            created_at: Some(chrono::Utc::now()),
            base_id: None,
            files: None,
        };

        // We need to commit this index to the dataset so that it can be found
        use crate::dataset::transaction::{Operation, Transaction};
        let transaction = Transaction::new(
            dataset.version().version,
            Operation::CreateIndex {
                new_indices: vec![index_meta.clone()],
                removed_indices: vec![],
            },
            None,
        );

        // Apply the transaction to register the index
        // Since dataset is Arc<Dataset>, we need to create a mutable reference
        let mut dataset_mut = (*dataset).clone();
        dataset_mut
            .apply_commit(transaction, &Default::default(), &Default::default())
            .await
            .unwrap();

        let index = dataset_mut
            .open_vector_index(WellKnownIvfPqData::COLUMN, &uuid, &NoOpMetricsCollector)
            .await
            .unwrap();

        let ivf_index = index.as_any().downcast_ref::<IVFIndex>().unwrap();

        let index_meta = lance_table::format::IndexMetadata {
            uuid,
            dataset_version: 0,
            fields: Vec::new(),
            name: INDEX_NAME.to_string(),
            fragment_bitmap: None,
            index_details: Some(Arc::new(vector_index_details_default())),
            index_version: VECTOR_INDEX_VERSION as i32,
            created_at: None, // Test index, not setting timestamp
            base_id: None,
            files: None,
        };

        let prefilter = Arc::new(DatasetPreFilter::new(dataset.clone(), &[index_meta], None));

        let is_not_remapped = Some;
        let is_remapped = |row_id| Some(row_id + BIG_OFFSET);
        let is_removed = |_| None;
        let max_id = test_data.get_vectors().len() as u64 / test_data.dim as u64;
        let row_ids = Vec::from_iter(0..max_id);

        // Sanity check to make sure the index we built is behaving correctly.  Any
        // input row, when used as a query, should be found in the results list with
        // the same id
        test_data
            .check_index(ivf_index, prefilter.clone(), &row_ids, is_not_remapped)
            .await;

        // When remapping we change the id of 1/3 of the rows, we remove another 1/3,
        // and we keep 1/3 as they are
        let partitioned_row_ids = partition_ids(row_ids, 3);
        let row_ids_to_modify = &partitioned_row_ids[0];
        let row_ids_to_remove = &partitioned_row_ids[1];
        let row_ids_to_remain = &partitioned_row_ids[2];

        let mapping = build_mapping(row_ids_to_modify, row_ids_to_remove, max_id);

        let new_uuid = Uuid::new_v4();

        remap_index_file(
            &dataset_mut,
            &uuid,
            &new_uuid,
            dataset_mut.version().version,
            ivf_index,
            &RowAddrRemap::direct(mapping),
            INDEX_NAME.to_string(),
            WellKnownIvfPqData::COLUMN.to_string(),
            vec![],
        )
        .await
        .unwrap();

        // After remapping the index file, we need to register the new index metadata
        // so that the dataset can find it when we try to open it
        let field = dataset_mut
            .schema()
            .field(WellKnownIvfPqData::COLUMN)
            .unwrap();
        let new_index_meta = lance_table::format::IndexMetadata {
            uuid: new_uuid,
            dataset_version: dataset_mut.version().version,
            fields: vec![field.id],
            name: format!("{}_remapped", INDEX_NAME),
            fragment_bitmap: Some(dataset_mut.fragment_bitmap.as_ref().clone()),
            index_details: Some(Arc::new(vector_index_details_default())),
            index_version: VECTOR_INDEX_VERSION as i32,
            created_at: Some(chrono::Utc::now()),
            base_id: None,
            files: None,
        };

        // We need to commit this new index to the dataset so it can be found
        let transaction = Transaction::new(
            dataset_mut.version().version,
            Operation::CreateIndex {
                new_indices: vec![new_index_meta],
                removed_indices: vec![],
            },
            None,
        );

        // Apply the transaction to register the new index
        dataset_mut
            .apply_commit(transaction, &Default::default(), &Default::default())
            .await
            .unwrap();

        let remapped = dataset_mut
            .open_vector_index(WellKnownIvfPqData::COLUMN, &new_uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ivf_remapped = remapped.as_any().downcast_ref::<IVFIndex>().unwrap();

        // If the ids were remapped then make sure the new row id is in the results
        test_data
            .check_index(
                ivf_remapped,
                prefilter.clone(),
                row_ids_to_modify,
                is_remapped,
            )
            .await;
        // If the ids were removed then make sure the old row id isn't in the results
        test_data
            .check_index(
                ivf_remapped,
                prefilter.clone(),
                row_ids_to_remove,
                is_removed,
            )
            .await;
        // If the ids were not remapped then make sure they still return the old id
        test_data
            .check_index(
                ivf_remapped,
                prefilter.clone(),
                row_ids_to_remain,
                is_not_remapped,
            )
            .await;
    }

    struct TestPqParams {
        num_sub_vectors: usize,
        num_bits: usize,
    }

    impl TestPqParams {
        fn small() -> Self {
            Self {
                num_sub_vectors: 2,
                num_bits: 8,
            }
        }
    }

    // Clippy doesn't like that all start with Ivf but we might have some in the future
    // that _don't_ start with Ivf so I feel it is meaningful to keep the prefix
    #[allow(clippy::enum_variant_names)]
    enum TestIndexType {
        IvfPq { pq: TestPqParams },
        IvfHnswPq { pq: TestPqParams, num_edges: usize },
        IvfHnswSq { num_edges: usize },
        IvfFlat,
    }

    struct CreateIndexCase {
        metric_type: MetricType,
        num_partitions: usize,
        dimension: usize,
        index_type: TestIndexType,
    }

    // We test L2 and Dot, because L2 PQ uses residuals while Dot doesn't,
    // so they have slightly different code paths.
    #[tokio::test]
    #[rstest]
    #[case::ivf_pq_l2(CreateIndexCase {
        metric_type: MetricType::L2,
        num_partitions: 2,
        dimension: 16,
        index_type: TestIndexType::IvfPq { pq: TestPqParams::small() },
    })]
    #[case::ivf_pq_dot(CreateIndexCase {
        metric_type: MetricType::Dot,
        num_partitions: 2,
        dimension: 2000,
        index_type: TestIndexType::IvfPq { pq: TestPqParams::small() },
    })]
    #[case::ivf_flat(CreateIndexCase { num_partitions: 1, metric_type: MetricType::Dot, dimension: 16, index_type: TestIndexType::IvfFlat })]
    #[case::ivf_hnsw_pq(CreateIndexCase {
        num_partitions: 2,
        metric_type: MetricType::Dot,
        dimension: 16,
        index_type: TestIndexType::IvfHnswPq { pq: TestPqParams::small(), num_edges: 100 },
    })]
    #[case::ivf_hnsw_sq(CreateIndexCase {
        metric_type: MetricType::Dot,
        num_partitions: 2,
        dimension: 16,
        index_type: TestIndexType::IvfHnswSq { num_edges: 100 },
    })]
    async fn test_create_index_nulls(
        #[case] test_case: CreateIndexCase,
        #[values(IndexFileVersion::Legacy, IndexFileVersion::V3)] index_version: IndexFileVersion,
    ) {
        // Most vector search algorithms are approximate, so they may not return all results.
        // IvfFlat is exact under this test's parameters.
        let is_approximate = !matches!(&test_case.index_type, TestIndexType::IvfFlat);
        let mut index_params = match test_case.index_type {
            TestIndexType::IvfPq { pq } => VectorIndexParams::with_ivf_pq_params(
                test_case.metric_type,
                IvfBuildParams::new(test_case.num_partitions),
                PQBuildParams::new(pq.num_sub_vectors, pq.num_bits),
            ),
            TestIndexType::IvfHnswPq { pq, num_edges } => {
                VectorIndexParams::with_ivf_hnsw_pq_params(
                    test_case.metric_type,
                    IvfBuildParams::new(test_case.num_partitions),
                    HnswBuildParams::default().num_edges(num_edges),
                    PQBuildParams::new(pq.num_sub_vectors, pq.num_bits),
                )
            }
            TestIndexType::IvfFlat => {
                VectorIndexParams::ivf_flat(test_case.num_partitions, test_case.metric_type)
            }
            TestIndexType::IvfHnswSq { num_edges } => VectorIndexParams::with_ivf_hnsw_sq_params(
                test_case.metric_type,
                IvfBuildParams::new(test_case.num_partitions),
                HnswBuildParams::default().num_edges(num_edges),
                SQBuildParams::default(),
            ),
        };
        index_params.version(index_version);

        let nrows = 2_000;
        let data = gen_batch()
            .col(
                "vec",
                array::rand_vec::<Float32Type>(Dimension::from(test_case.dimension as u32)),
            )
            .into_batch_rows(RowCount::from(nrows))
            .unwrap();

        // Make every other row null
        let null_buffer = (0..nrows).map(|i| i % 2 == 0).collect::<BooleanBuffer>();
        let null_buffer = NullBuffer::new(null_buffer);
        let vectors = data["vec"]
            .clone()
            .to_data()
            .into_builder()
            .nulls(Some(null_buffer))
            .build()
            .unwrap();
        let vectors = make_array(vectors);
        let num_non_null = vectors.len() - vectors.logical_null_count();
        let data = RecordBatch::try_new(data.schema(), vec![vectors]).unwrap();

        let mut dataset = InsertBuilder::new("memory://")
            .execute(vec![data])
            .await
            .unwrap();

        // Create index
        dataset
            .create_index(&["vec"], IndexType::Vector, None, &index_params, false)
            .await
            .unwrap();

        let query = vec![0.0; test_case.dimension]
            .into_iter()
            .collect::<Float32Array>();
        let results = dataset
            .scan()
            .nearest("vec", &query, 2_000)
            .unwrap()
            .ef(100_000)
            .minimum_nprobes(2)
            .try_into_batch()
            .await
            .unwrap();
        // Use a relaxed assertion for approximate indexes.
        if is_approximate {
            let recall = results.num_rows() as f32 / num_non_null as f32;
            assert!(
                recall >= 0.99,
                "Recall {} below threshold {} ({}/{})",
                recall,
                0.99,
                results.num_rows(),
                num_non_null,
            );
        } else {
            assert_eq!(results.num_rows(), num_non_null);
        }
        assert_eq!(results["vec"].logical_null_count(), 0);
    }

    #[tokio::test]
    async fn test_index_lifecycle_nulls() {
        // Generate random data with nulls
        let nrows = 2_000;
        let dims = 32;
        let data = gen_batch()
            .col(
                "vec",
                array::rand_vec::<Float32Type>(Dimension::from(dims as u32)).with_random_nulls(0.5),
            )
            .into_batch_rows(RowCount::from(nrows))
            .unwrap();
        let num_non_null = data["vec"].len() - data["vec"].logical_null_count();

        let mut dataset = InsertBuilder::new("memory://")
            .execute(vec![data])
            .await
            .unwrap();

        // Create index
        let index_params = VectorIndexParams::with_ivf_pq_params(
            MetricType::L2,
            IvfBuildParams::new(2),
            PQBuildParams::new(2, 8),
        );
        dataset
            .create_index(&["vec"], IndexType::Vector, None, &index_params, false)
            .await
            .unwrap();

        // Check that the index is working
        async fn check_index(dataset: &Dataset, num_non_null: usize, dims: usize) {
            let query = vec![0.0; dims].into_iter().collect::<Float32Array>();
            let results = dataset
                .scan()
                .nearest("vec", &query, 2_000)
                .unwrap()
                .minimum_nprobes(2)
                .try_into_batch()
                .await
                .unwrap();
            assert_eq!(results.num_rows(), num_non_null);
        }
        check_index(&dataset, num_non_null, dims).await;

        // Append more data
        let data = gen_batch()
            .col(
                "vec",
                array::rand_vec::<Float32Type>(Dimension::from(dims as u32)).with_random_nulls(0.5),
            )
            .into_batch_rows(RowCount::from(500))
            .unwrap();
        let num_non_null = data["vec"].len() - data["vec"].logical_null_count() + num_non_null;
        let mut dataset = InsertBuilder::new(Arc::new(dataset))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![data])
            .await
            .unwrap();
        check_index(&dataset, num_non_null, dims).await;

        // Optimize the index
        dataset.optimize_indices(&Default::default()).await.unwrap();
        check_index(&dataset, num_non_null, dims).await;
    }

    #[tokio::test]
    async fn test_create_ivf_pq_cosine() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let (mut dataset, vector_array) = generate_test_dataset(test_uri, 0.0..1.0).await;

        let centroids = generate_random_array(2 * DIM);
        let ivf_centroids = FixedSizeListArray::try_new_from_values(centroids, DIM as i32).unwrap();
        let ivf_params = IvfBuildParams::try_with_centroids(2, Arc::new(ivf_centroids)).unwrap();

        let pq_params = PQBuildParams::new(4, 8);

        let params =
            VectorIndexParams::with_ivf_pq_params(MetricType::Cosine, ivf_params, pq_params);

        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let sample_query = vector_array.value(10);
        let query = sample_query.as_primitive::<Float32Type>();
        let results = dataset
            .scan()
            .nearest("vector", query, 5)
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(1, results.len());
        assert_eq!(5, results[0].num_rows());
        for batch in results.iter() {
            let dist = &batch["_distance"];
            dist.as_primitive::<Float32Type>()
                .values()
                .iter()
                .for_each(|v| {
                    assert!(
                        (0.0..2.0).contains(v),
                        "Expect cosine value in range [0.0, 2.0], got: {}",
                        v
                    )
                });
        }
    }

    #[tokio::test]
    async fn test_build_ivf_model_l2() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let (dataset, _) = generate_test_dataset(test_uri, 1000.0..1100.0).await;

        let ivf_params = IvfBuildParams::new(2);
        let ivf_model = build_ivf_model(
            &dataset,
            "vector",
            DIM,
            MetricType::L2,
            &ivf_params,
            None,
            lance_index::progress::noop_progress(),
        )
        .await
        .unwrap();
        assert_eq!(2, ivf_model.centroids.as_ref().unwrap().len());
        assert_eq!(32, ivf_model.centroids.as_ref().unwrap().value_length());
        assert_eq!(2, ivf_model.num_partitions());

        // All centroids values should be in the range [1000, 1100]
        ivf_model
            .centroids
            .unwrap()
            .values()
            .as_primitive::<Float32Type>()
            .values()
            .iter()
            .for_each(|v| {
                assert!((1000.0..1100.0).contains(v));
            });
    }

    #[tokio::test]
    async fn test_build_ivf_model_cosine() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let (dataset, _) = generate_test_dataset(test_uri, 1000.0..1100.0).await;

        let ivf_params = IvfBuildParams::new(2);
        let ivf_model = build_ivf_model(
            &dataset,
            "vector",
            DIM,
            MetricType::Cosine,
            &ivf_params,
            None,
            lance_index::progress::noop_progress(),
        )
        .await
        .unwrap();
        assert_eq!(2, ivf_model.centroids.as_ref().unwrap().len());
        assert_eq!(32, ivf_model.centroids.as_ref().unwrap().value_length());
        assert_eq!(2, ivf_model.num_partitions());

        // All centroids values should be in the range [1000, 1100]
        ivf_model
            .centroids
            .unwrap()
            .values()
            .as_primitive::<Float32Type>()
            .values()
            .iter()
            .for_each(|v| {
                assert!(
                    (-1.0..1.0).contains(v),
                    "Expect cosine value in range [-1.0, 1.0], got: {}",
                    v
                );
            });
    }

    #[tokio::test]
    async fn test_create_ivf_pq_dot() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let (mut dataset, vector_array) = generate_test_dataset(test_uri, 0.0..1.0).await;

        let centroids = generate_random_array(2 * DIM);
        let ivf_centroids = FixedSizeListArray::try_new_from_values(centroids, DIM as i32).unwrap();
        let ivf_params = IvfBuildParams::try_with_centroids(2, Arc::new(ivf_centroids)).unwrap();

        let codebook = Arc::new(generate_random_array(256 * DIM));
        let pq_params = PQBuildParams::with_codebook(4, 8, codebook);

        let params = VectorIndexParams::with_ivf_pq_params(MetricType::Dot, ivf_params, pq_params);

        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let sample_query = vector_array.value(10);
        let query = sample_query.as_primitive::<Float32Type>();
        let results = dataset
            .scan()
            .nearest("vector", query, 5)
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(1, results.len());
        assert_eq!(5, results[0].num_rows());

        for batch in results.iter() {
            let dist = &batch["_distance"];
            dist.as_primitive::<Float32Type>()
                .values()
                .iter()
                .for_each(|v| {
                    assert!(
                        (-2.0 * DIM as f32..0.0).contains(v),
                        "Expect dot product value in range [-2.0 * DIM, 0.0], got: {}",
                        v
                    )
                });
        }
    }

    #[tokio::test]
    async fn test_build_ivf_model_streaming_training() {
        let test_dir = TempStrDir::default();
        let uri = format!("{}/ds", test_dir.as_str());
        let reader = gen_batch()
            .col("id", array::step::<UInt64Type>())
            .col("vector", array::rand_vec::<Float32Type>(32.into()))
            .into_reader_rows(RowCount::from(512), BatchCount::from(2));
        let dataset = Dataset::write(reader, &uri, None).await.unwrap();

        let mut params = IvfBuildParams::new(8);
        params.sample_rate = 16;
        params.streaming_sample_rate = Some(4);
        params.streaming_refine_passes = 1;
        params.max_iters = 2;

        let ivf_model = build_ivf_model(
            &dataset,
            "vector",
            32,
            MetricType::L2,
            &params,
            None,
            lance_index::progress::noop_progress(),
        )
        .await
        .unwrap();

        assert_eq!(ivf_model.num_partitions(), 8);
        assert_eq!(ivf_model.dimension(), 32);
        assert!(ivf_model.loss().is_none());
        assert!(
            compute_test_ivf_loss(&dataset, "vector", &ivf_model)
                .await
                .is_finite()
        );
    }

    /// Regression test for a hang in the streaming *coreset* trainer
    /// (`train_streaming_coreset_ivf_model`, taken when `num_partitions > 256`).
    ///
    /// That function spawns a `progress_worker` task that loops on
    /// `progress_rx.recv()` and only terminates once every clone of the mpsc
    /// sender is dropped. The `on_progress` closure owns a sender clone, and
    /// `WeightedHierarchicalKMeansParams` used to retain an `on_progress` clone
    /// that outlived the `progress_worker.await` at the end of the function.
    /// With a live sender remaining, `recv()` never returned `None`, the worker
    /// never finished, and the trainer hung forever after all compute was done.
    ///
    /// The build is wrapped in a timeout so the regression fails fast rather
    /// than hanging the test process indefinitely.
    #[tokio::test(flavor = "multi_thread")]
    async fn test_streaming_coreset_ivf_training_terminates() {
        use lance_index::progress::IndexBuildProgress;
        use std::sync::atomic::{AtomicU64, Ordering};
        use std::time::Duration;

        #[derive(Debug, Default)]
        struct CountingProgress {
            progress_calls: AtomicU64,
        }

        #[async_trait::async_trait]
        impl IndexBuildProgress for CountingProgress {
            async fn stage_start(&self, _: &str, _: Option<u64>, _: &str) -> Result<()> {
                Ok(())
            }
            async fn stage_progress(&self, _: &str, _: u64) -> Result<()> {
                self.progress_calls.fetch_add(1, Ordering::Relaxed);
                Ok(())
            }
            async fn stage_complete(&self, _: &str) -> Result<()> {
                Ok(())
            }
        }

        const SMALL_DIM: usize = 8;

        let test_dir = TempStrDir::default();
        let uri = format!("{}/ds", test_dir.as_str());
        let reader = gen_batch()
            .col("id", array::step::<UInt64Type>())
            .col(
                "vector",
                array::rand_vec::<Float32Type>((SMALL_DIM as u32).into()),
            )
            .into_reader_rows(RowCount::from(2048), BatchCount::from(4));
        let dataset = Dataset::write(reader, &uri, None).await.unwrap();

        // > 256 partitions routes through `train_streaming_coreset_ivf_model`.
        let mut params = IvfBuildParams::new(257);
        params.sample_rate = 8;
        params.streaming_sample_rate = Some(4);
        params.streaming_refine_passes = 1;
        params.max_iters = 2;

        let progress = Arc::new(CountingProgress::default());

        let ivf_model = tokio::time::timeout(
            Duration::from_secs(120),
            build_ivf_model(
                &dataset,
                "vector",
                SMALL_DIM,
                MetricType::L2,
                &params,
                None,
                progress.clone(),
            ),
        )
        .await
        .expect(
            "streaming coreset IVF training hung: progress worker never terminated after training",
        )
        .unwrap();

        assert_eq!(ivf_model.num_partitions(), 257);
        assert_eq!(ivf_model.dimension(), SMALL_DIM);
        // The progress worker must have processed reports and then joined
        // cleanly (proven by `build_ivf_model` returning at all).
        assert!(
            progress.progress_calls.load(Ordering::Relaxed) > 0,
            "expected the progress worker to receive at least one report"
        );
    }

    #[test]
    fn test_fixed_training_ranges_are_sorted_and_bounded() {
        let ranges = generate_fixed_training_ranges(10_000, 1_234, 1_024, 16);
        assert_eq!(ranges.num_rows(), 1_234);
        assert!(ranges.ranges.iter().all(|range| {
            range.start < range.end && range.end <= 10_000 && range_len(range) <= 1_234
        }));
        assert!(
            ranges
                .ranges
                .windows(2)
                .all(|pair| pair[0].end < pair[1].start)
        );

        let all_rows = generate_fixed_training_ranges(128, 256, 1_024, 16);
        assert_eq!(all_rows.ranges, vec![0..128]);
        assert_eq!(all_rows.num_rows(), 128);
    }

    #[test]
    fn test_fixed_training_ranges_chunk_splits_ranges() {
        let ranges = FixedIvfTrainingRanges::new(vec![10..20, 30..45]);
        assert_eq!(ranges.num_rows(), 25);
        assert_eq!(ranges.chunk(0, 5), vec![10..15]);
        assert_eq!(ranges.chunk(5, 12), vec![15..20, 30..37]);
        assert_eq!(ranges.chunk(20, 10), vec![40..45]);
        assert!(ranges.chunk(25, 10).is_empty());
    }

    #[test]
    fn test_split_ranges_by_row_count() {
        assert_eq!(
            split_ranges_by_row_count(&[10..25, 30..33], 8),
            vec![10..18, 18..25, 30..33]
        );
        assert_eq!(
            split_ranges_by_row_count(&[5..8], 0),
            vec![5..6, 6..7, 7..8]
        );
        assert!(split_ranges_by_row_count(&[], 8).is_empty());
    }

    #[test]
    fn test_streaming_ivf_progress_throttle() {
        assert!(should_report_streaming_ivf_progress(1, 64));
        assert!(!should_report_streaming_ivf_progress(63, 64));
        assert!(should_report_streaming_ivf_progress(64, 64));
        assert!(should_report_streaming_ivf_progress(128, 64));
        assert!(should_report_streaming_ivf_progress(2, 0));
    }

    #[test]
    fn test_streaming_coreset_default_rate_is_bounded_by_stream_rate() {
        assert_eq!(default_streaming_coreset_rate(256, 1), 1);
        assert_eq!(default_streaming_coreset_rate(256, 16), 16);
        assert_eq!(default_streaming_coreset_rate(256, 128), 64);
        assert_eq!(default_streaming_coreset_rate(8, 128), 8);
        assert_eq!(streaming_coreset_rate(256, 128, Some(16)), 16);
        assert_eq!(
            streaming_local_coreset_k(1024, 1024 * 128, 16, 2, true),
            1024 * 8
        );
        assert_eq!(
            streaming_local_coreset_k(1024, 1024 * 128, 16, 2, false),
            1024
        );
    }

    #[test]
    fn test_weighted_coreset_reduction_groups_nearby_centroids() {
        let mut coreset = WeightedCoreset::new(1, 4);
        coreset.push(&[0.0], 1.0, 0.0);
        coreset.push(&[100.0], 1.0, 0.0);
        coreset.push(&[1.0], 1.0, 0.0);
        coreset.push(&[101.0], 1.0, 0.0);

        coreset.reduce_to_budget(1, 2);

        assert_eq!(coreset.len(), 2);
        assert!((coreset.values[0] - 0.5).abs() < 1e-6);
        assert!((coreset.values[1] - 100.5).abs() < 1e-6);
        assert_eq!(coreset.weights, vec![2.0, 2.0]);
        assert!((coreset.losses.iter().sum::<f64>() - 1.0).abs() < 1e-6);
    }

    #[test]
    fn test_weighted_kmeanspp_initialization_selects_distant_centroids() {
        let values = vec![0.0, 0.1, 100.0, 101.0];
        let weights = vec![1.0; 4];
        let centroids = initialize_weighted_centroids(&values, 1, 2, 4, &weights);

        assert_eq!(centroids.len(), 2);
        assert!(
            (centroids[0] - centroids[1]).abs() > 10.0,
            "weighted kmeans++ should seed distant coreset regions, got {:?}",
            centroids
        );
    }

    #[tokio::test]
    async fn test_create_ivf_pq_f16() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        const DIM: usize = 32;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float16, true)),
                DIM as i32,
            ),
            true,
        )]));

        let arr = generate_random_array_with_seed::<Float16Type>(1000 * DIM, [22; 32]);
        let fsl = FixedSizeListArray::try_new_from_values(arr, DIM as i32).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap();
        let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(batches, test_uri, None).await.unwrap();

        let params = VectorIndexParams::with_ivf_pq_params(
            MetricType::L2,
            IvfBuildParams::new(2),
            PQBuildParams::new(4, 8),
        );
        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let results = dataset
            .scan()
            .nearest(
                "vector",
                &Float32Array::from_iter_values(repeat_n(0.5, DIM)),
                5,
            )
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        let batch = &results[0];
        assert_eq!(
            batch.schema(),
            Arc::new(Schema::new(vec![
                Field::new(
                    "vector",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float16, true)),
                        DIM as i32,
                    ),
                    true,
                ),
                Field::new("_distance", DataType::Float32, true)
            ]))
        );
    }

    #[tokio::test]
    async fn test_create_ivf_pq_f16_with_codebook() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        const DIM: usize = 32;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float16, true)),
                DIM as i32,
            ),
            true,
        )]));

        let arr = generate_random_array_with_seed::<Float16Type>(1000 * DIM, [22; 32]);
        let fsl = FixedSizeListArray::try_new_from_values(arr, DIM as i32).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap();
        let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(batches, test_uri, None).await.unwrap();

        let codebook = Arc::new(generate_random_array_with_seed::<Float16Type>(
            256 * DIM,
            [22; 32],
        ));
        let params = VectorIndexParams::with_ivf_pq_params(
            MetricType::L2,
            IvfBuildParams::new(2),
            PQBuildParams::with_codebook(4, 8, codebook),
        );
        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let results = dataset
            .scan()
            .nearest(
                "vector",
                &Float32Array::from_iter_values(repeat_n(0.5, DIM)),
                5,
            )
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        let batch = &results[0];
        assert_eq!(
            batch.schema(),
            Arc::new(Schema::new(vec![
                Field::new(
                    "vector",
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::Float16, true)),
                        DIM as i32,
                    ),
                    true,
                ),
                Field::new("_distance", DataType::Float32, true)
            ]))
        );
    }

    #[tokio::test]
    async fn test_create_ivf_flat_f16() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        const DIM: usize = 32;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float16, true)),
                DIM as i32,
            ),
            true,
        )]));

        let arr = generate_random_array_with_seed::<Float16Type>(1000 * DIM, [22; 32]);
        let fsl = FixedSizeListArray::try_new_from_values(arr, DIM as i32).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap();
        let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(batches, test_uri, None).await.unwrap();

        let params = VectorIndexParams::ivf_flat(2, MetricType::L2);
        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let query = Float16Array::from_iter_values(repeat_n(f16::from_f32(0.5), DIM));
        let results = dataset
            .scan()
            .nearest("vector", &query, 5)
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        let schema = results[0].schema();
        let field = schema.field(0);
        let DataType::FixedSizeList(item, _) = field.data_type() else {
            panic!("vector column should remain fixed size list");
        };
        assert_eq!(item.data_type(), &DataType::Float16);
    }

    #[tokio::test]
    async fn test_create_ivf_hnsw_flat_f16() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        const DIM: usize = 32;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float16, true)),
                DIM as i32,
            ),
            true,
        )]));

        let arr = generate_random_array_with_seed::<Float16Type>(1000 * DIM, [22; 32]);
        let fsl = FixedSizeListArray::try_new_from_values(arr, DIM as i32).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap();
        let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(batches, test_uri, None).await.unwrap();

        let params = VectorIndexParams::ivf_hnsw(
            MetricType::L2,
            IvfBuildParams::new(2),
            HnswBuildParams::default(),
        );
        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let query = Float16Array::from_iter_values(repeat_n(f16::from_f32(0.5), DIM));
        let results = dataset
            .scan()
            .nearest("vector", &query, 5)
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 5);
        let schema = results[0].schema();
        let field = schema.field(0);
        let DataType::FixedSizeList(item, _) = field.data_type() else {
            panic!("vector column should remain fixed size list");
        };
        assert_eq!(item.data_type(), &DataType::Float16);
    }

    #[tokio::test]
    async fn test_create_ivf_pq_with_invalid_num_sub_vectors() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        const DIM: usize = 32;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        )]));

        let arr = generate_random_array_with_seed::<Float32Type>(1000 * DIM, [22; 32]);
        let fsl = FixedSizeListArray::try_new_from_values(arr, DIM as i32).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap();
        let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(batches, test_uri, None).await.unwrap();

        let params = VectorIndexParams::with_ivf_pq_params(
            MetricType::L2,
            IvfBuildParams::new(256),
            PQBuildParams::new(6, 8),
        );
        let res = dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await;
        match &res {
            Err(Error::InvalidInput { source, .. }) => {
                assert!(
                    source
                        .to_string()
                        .contains("num_sub_vectors must divide vector dimension"),
                    "{:?}",
                    res
                );
            }
            _ => panic!("Expected InvalidInput error: {:?}", res),
        }
    }

    fn ground_truth(
        fsl: &FixedSizeListArray,
        query: &[f32],
        k: usize,
        distance_type: DistanceType,
    ) -> Vec<(f32, u32)> {
        let dim = fsl.value_length() as usize;
        let mut dists = fsl
            .values()
            .as_primitive::<Float32Type>()
            .values()
            .chunks(dim)
            .enumerate()
            .map(|(i, vec)| {
                let dist = distance_type.func()(query, vec);
                (dist, i as u32)
            })
            .collect_vec();
        dists.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap());
        dists.truncate(k);
        dists
    }

    #[tokio::test]
    async fn test_create_ivf_hnsw_pq() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let nlist = 4;
        let (mut dataset, vector_array) = generate_test_dataset(test_uri, 0.0..1.0).await;

        let ivf_params = IvfBuildParams::new(nlist);
        let pq_params = PQBuildParams::default();
        let hnsw_params = HnswBuildParams::default();
        let params = VectorIndexParams::with_ivf_hnsw_pq_params(
            MetricType::L2,
            ivf_params,
            hnsw_params,
            pq_params,
        );

        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let query = vector_array.value(0);
        let query = query.as_primitive::<Float32Type>();
        let k = 100;
        let results = dataset
            .scan()
            .with_row_id()
            .nearest("vector", query, k)
            .unwrap()
            .minimum_nprobes(nlist)
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(1, results.len());
        assert_eq!(k, results[0].num_rows());

        let row_ids = results[0]
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap() as u32)
            .collect::<Vec<_>>();
        let dists = results[0]
            .column_by_name("_distance")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec();

        let results = dists.into_iter().zip(row_ids.into_iter()).collect_vec();
        let gt = ground_truth(&vector_array, query.values(), k, DistanceType::L2);

        let results_set = results.iter().map(|r| r.1).collect::<HashSet<_>>();
        let gt_set = gt.iter().map(|r| r.1).collect::<HashSet<_>>();

        let recall = results_set.intersection(&gt_set).count() as f32 / k as f32;
        assert!(
            recall >= 0.9,
            "recall: {}\n results: {:?}\n\ngt: {:?}",
            recall,
            results,
            gt,
        );
    }

    #[tokio::test]
    async fn test_create_ivf_hnsw_with_empty_partition() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        // the generate_test_dataset function generates a dataset with 1000 vectors,
        // so 1001 partitions will have at least one empty partition
        let nlist = 1001;
        let (mut dataset, vector_array) = generate_test_dataset(test_uri, 0.0..1.0).await;

        let centroids = generate_random_array(nlist * DIM);
        let ivf_centroids = FixedSizeListArray::try_new_from_values(centroids, DIM as i32).unwrap();
        let ivf_params =
            IvfBuildParams::try_with_centroids(nlist, Arc::new(ivf_centroids)).unwrap();

        let distance_type = DistanceType::L2;
        let sq_params = SQBuildParams::default();
        let hnsw_params = HnswBuildParams::default();
        let params = VectorIndexParams::with_ivf_hnsw_sq_params(
            distance_type,
            ivf_params,
            hnsw_params,
            sq_params,
        );

        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let query = vector_array.value(0);
        let query = query.as_primitive::<Float32Type>();
        let k = 100;
        let results = dataset
            .scan()
            .with_row_id()
            .nearest("vector", query, k)
            .unwrap()
            .minimum_nprobes(nlist)
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(1, results.len());
        assert_eq!(k, results[0].num_rows());

        let row_ids = results[0]
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .iter()
            .map(|v| v.unwrap() as u32)
            .collect::<Vec<_>>();
        let dists = results[0]
            .column_by_name("_distance")
            .unwrap()
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .values()
            .to_vec();

        let results = dists.into_iter().zip(row_ids.into_iter()).collect_vec();
        let gt = ground_truth(&vector_array, query.values(), k, distance_type);

        let results_set = results.iter().map(|r| r.1).collect::<HashSet<_>>();
        let gt_set = gt.iter().map(|r| r.1).collect::<HashSet<_>>();

        let recall = results_set.intersection(&gt_set).count() as f32 / k as f32;
        assert!(
            recall >= 0.9,
            "recall: {}\n results: {:?}\n\ngt: {:?}",
            recall,
            results,
            gt,
        );
    }

    #[tokio::test]
    async fn test_check_cosine_normalization() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        const DIM: usize = 32;

        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                DIM as i32,
            ),
            true,
        )]));

        let arr = generate_random_array_with_range::<Float32Type>(1000 * DIM, 1000.0..1001.0);
        let fsl = FixedSizeListArray::try_new_from_values(arr.clone(), DIM as i32).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap();
        let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(batches, test_uri, None).await.unwrap();

        let params = VectorIndexParams::ivf_pq(2, 8, 4, MetricType::Cosine, 50);
        dataset
            .create_index(&["vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();
        let indices = dataset.load_indices().await.unwrap();
        let idx = dataset
            .open_generic_index("vector", &indices[0].uuid, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ivf_idx = idx.as_any().downcast_ref::<v2::IvfPq>().unwrap();

        assert!(
            ivf_idx
                .ivf_model()
                .centroids
                .as_ref()
                .unwrap()
                .values()
                .as_primitive::<Float32Type>()
                .values()
                .iter()
                .all(|v| (0.0..=1.0).contains(v))
        );

        // PQ code is on residual space
        let pq_store = ivf_idx.load_partition_storage(0, None).await.unwrap();
        pq_store
            .codebook()
            .values()
            .as_primitive::<Float32Type>()
            .values()
            .iter()
            .for_each(|v| assert!((-1.0..=1.0).contains(v), "Got {}", v));

        let dataset = Dataset::open(test_uri).await.unwrap();

        let mut correct_times = 0;
        for query_id in 0..10 {
            let query = &arr.slice(query_id * DIM, DIM);
            let results = dataset
                .scan()
                .with_row_id()
                .nearest("vector", query, 1)
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();
            assert_eq!(results.num_rows(), 1);
            let row_id = results
                .column_by_name("_rowid")
                .unwrap()
                .as_primitive::<UInt64Type>()
                .value(0);
            if row_id == (query_id as u64) {
                correct_times += 1;
            }
        }

        assert!(correct_times >= 9, "correct: {}", correct_times);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_build_ivf_model_progress_callback() {
        use lance_index::progress::IndexBuildProgress;
        use tokio::sync::Mutex;

        #[derive(Debug)]
        struct RecordingProgress {
            calls: Arc<Mutex<Vec<(String, u64)>>>,
        }

        #[async_trait::async_trait]
        impl IndexBuildProgress for RecordingProgress {
            async fn stage_start(&self, _: &str, _: Option<u64>, _: &str) -> Result<()> {
                Ok(())
            }
            async fn stage_progress(&self, stage: &str, completed: u64) -> Result<()> {
                self.calls.lock().await.push((stage.to_string(), completed));
                Ok(())
            }
            async fn stage_complete(&self, _: &str) -> Result<()> {
                Ok(())
            }
        }

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let (dataset, _) = generate_test_dataset(test_uri, 1000.0..1100.0).await;

        let ivf_params = IvfBuildParams::new(2);
        let calls: Arc<Mutex<Vec<(String, u64)>>> = Arc::new(Mutex::new(Vec::new()));
        let progress: Arc<dyn IndexBuildProgress> = Arc::new(RecordingProgress {
            calls: calls.clone(),
        });

        let ivf_model = build_ivf_model(
            &dataset,
            "vector",
            DIM,
            MetricType::L2,
            &ivf_params,
            None,
            progress,
        )
        .await
        .unwrap();
        assert_eq!(2, ivf_model.num_partitions());

        // Let spawned progress tasks complete.
        tokio::task::yield_now().await;

        let recorded = calls.lock().await;
        assert!(
            !recorded.is_empty(),
            "Expected progress callbacks to be called"
        );
        // All calls should be for train_ivf stage
        for (stage, _) in recorded.iter() {
            assert_eq!(stage, "train_ivf");
        }
        // Completed values should be monotonically increasing
        for window in recorded.windows(2) {
            assert!(
                window[1].1 >= window[0].1,
                "Expected monotonically increasing progress: {} >= {}",
                window[1].1,
                window[0].1,
            );
        }
    }

    #[tokio::test]
    async fn test_prewarm_ivf_legacy() {
        use lance_io::assert_io_eq;

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let dim = DIM as i32;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
            false,
        )]));
        let vectors = generate_random_array(512 * DIM);
        let fsl = FixedSizeListArray::try_new_from_values(vectors, dim).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap();
        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let mut dataset = Dataset::write(batches, test_uri, None).await.unwrap();

        let nlist = 4;
        let params = VectorIndexParams::with_ivf_pq_params(
            DistanceType::L2,
            IvfBuildParams::new(nlist),
            PQBuildParams::default(),
        )
        .version(IndexFileVersion::Legacy)
        .clone();
        dataset
            .create_index(
                &["vector"],
                IndexType::Vector,
                Some("my_idx".to_owned()),
                &params,
                true,
            )
            .await
            .unwrap();

        // Reset IO stats after index creation
        dataset.object_store.as_ref().io_stats_incremental();

        // Prewarm should perform IO to load all partitions into cache
        dataset.prewarm_index("my_idx").await.unwrap();
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert!(
            stats.read_iops > 0,
            "prewarm should have read from disk, but read_iops was 0"
        );

        // Can query index without IO
        let q = Float32Array::from_iter_values(repeat_n(0.0, DIM));
        dataset
            .scan()
            .nearest("vector", &q, 10)
            .unwrap()
            .project(&["_rowid"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(
            stats,
            read_iops,
            0,
            "query should not perform IO after prewarm"
        );

        // Second prewarm should not need IO (already cached)
        dataset.prewarm_index("my_idx").await.unwrap();
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(stats, read_iops, 0, "second prewarm should not perform IO");
    }

    #[tokio::test]
    async fn test_prewarm_ivf_legacy_multiple_deltas() {
        use lance_io::assert_io_eq;

        let test_dir = copy_test_data_to_tmp("v0.21.0/bad_index_fragment_bitmap").unwrap();
        let test_uri = test_dir.path_str();
        let test_uri = &test_uri;

        // Trigger migration to repair legacy corrupt fragment bitmaps.
        let mut dataset = Dataset::open(test_uri).await.unwrap();
        dataset.index_statistics("vector_idx").await.unwrap();
        dataset.checkout_latest().await.unwrap();

        // Reopen dataset to avoid carrying index state in-memory from migration.
        let dataset = Dataset::open(test_uri).await.unwrap();
        let indices = dataset.load_indices_by_name("vector_idx").await.unwrap();
        assert_eq!(indices.len(), 2, "expected two index deltas for vector_idx");
        let unique_uuids: HashSet<_> = indices.iter().map(|meta| meta.uuid).collect();
        assert_eq!(unique_uuids.len(), 2, "expected two unique index UUIDs");

        let sample_batch = dataset
            .scan()
            .limit(Some(1), None)
            .unwrap()
            .project(&["vector"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let q = sample_batch["vector"]
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .value(0)
            .as_any()
            .downcast_ref::<Float32Array>()
            .unwrap()
            .clone();

        // Reset IO stats after migration and sampling.
        dataset.object_store.as_ref().io_stats_incremental();

        // Prewarm should perform IO to load all index deltas into cache.
        dataset.prewarm_index("vector_idx").await.unwrap();
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert!(
            stats.read_iops > 0,
            "prewarm should have read from disk, but read_iops was 0"
        );

        // Query should not perform index IO after prewarm of all deltas.
        dataset
            .scan()
            .nearest("vector", &q, 10)
            .unwrap()
            .project(&["_rowid"])
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(
            stats,
            read_iops,
            0,
            "query should not perform IO after prewarm"
        );

        // Second prewarm should not need IO (already cached).
        dataset.prewarm_index("vector_idx").await.unwrap();
        let stats = dataset.object_store.as_ref().io_stats_incremental();
        assert_io_eq!(stats, read_iops, 0, "second prewarm should not perform IO");
    }

    #[tokio::test]
    async fn test_optimize_ivf_flat_binary_vectors() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        const BIN_DIM: usize = 16;
        let schema = Arc::new(Schema::new(vec![Field::new(
            "bin_vector",
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::UInt8, true)),
                BIN_DIM as i32,
            ),
            true,
        )]));

        let arr = arrow_array::UInt8Array::from_iter_values((0..1000 * BIN_DIM).map(|i| i as u8));
        let fsl = FixedSizeListArray::try_new_from_values(arr, BIN_DIM as i32).unwrap();
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl)]).unwrap();
        let batches = RecordBatchIterator::new(vec![batch].into_iter().map(Ok), schema.clone());
        let mut dataset = Dataset::write(batches, test_uri, None).await.unwrap();

        let params = VectorIndexParams::ivf_flat(2, MetricType::Hamming);
        dataset
            .create_index(&["bin_vector"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        // Append more data so optimize_indices has unindexed fragments to merge
        let arr2 =
            arrow_array::UInt8Array::from_iter_values((0..500 * BIN_DIM).map(|i| (i + 7) as u8));
        let fsl2 = FixedSizeListArray::try_new_from_values(arr2, BIN_DIM as i32).unwrap();
        let batch2 = RecordBatch::try_new(schema.clone(), vec![Arc::new(fsl2)]).unwrap();
        let mut dataset = InsertBuilder::new(Arc::new(dataset))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![batch2])
            .await
            .unwrap();

        // This used to panic with "unsupported index type: FLAT, FLATBIN"
        dataset.optimize_indices(&Default::default()).await.unwrap();

        let indices = dataset.load_indices().await.unwrap();
        assert!(!indices.is_empty(), "should have at least one index");
    }

    #[tokio::test]
    async fn test_optimize_ivf_hnsw_records_actual_file_sizes() {
        // Regression test: `optimize_ivf_hnsw_indices` must record the on-disk
        // file sizes, which are only known after `finish()` writes the footer.
        const DIM: usize = 16;
        let data = gen_batch()
            .col(
                "vec",
                array::rand_vec::<Float32Type>(Dimension::from(DIM as u32)),
            )
            .into_batch_rows(RowCount::from(1_000))
            .unwrap();
        let schema = data.schema();

        let mut dataset = InsertBuilder::new("memory://")
            .execute(vec![data])
            .await
            .unwrap();

        // Legacy file version keeps the index on the v1 path that goes through
        // `optimize_ivf_hnsw_indices`.
        let mut params = VectorIndexParams::with_ivf_hnsw_sq_params(
            MetricType::L2,
            IvfBuildParams::new(2),
            HnswBuildParams::default().num_edges(50),
            SQBuildParams::default(),
        );
        params.version(IndexFileVersion::Legacy);
        dataset
            .create_index(&["vec"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        // Append unindexed data so `optimize_indices` has something to merge.
        let more = gen_batch()
            .col(
                "vec",
                array::rand_vec::<Float32Type>(Dimension::from(DIM as u32)),
            )
            .into_batch_rows(RowCount::from(500))
            .unwrap();
        let more = RecordBatch::try_new(schema.clone(), more.columns().to_vec()).unwrap();
        let mut dataset = InsertBuilder::new(Arc::new(dataset))
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![more])
            .await
            .unwrap();

        dataset.optimize_indices(&Default::default()).await.unwrap();

        let indices = dataset.load_indices().await.unwrap();
        let index = indices.first().expect("should have an index");
        let files = index
            .files
            .as_ref()
            .expect("optimized index should record file sizes");
        assert!(!files.is_empty(), "index should record at least one file");

        let indices_dir = dataset.indices_dir().join(index.uuid.to_string());
        for file in files {
            let actual = dataset
                .object_store
                .size(&indices_dir.clone().join(file.path.as_str()))
                .await
                .unwrap();
            assert_eq!(
                file.size_bytes, actual,
                "recorded size for {} must match the on-disk size",
                file.path
            );
        }
    }
}
