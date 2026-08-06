// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::borrow::Cow;
use std::collections::{BinaryHeap, HashMap};
use std::ops::Sub;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};

use arrow::array::AsArray;
use arrow::datatypes::{Float16Type, Float32Type, Float64Type, UInt8Type, UInt64Type};
use arrow_array::{
    Array, FixedSizeListArray, Float32Array, RecordBatch, UInt8Array, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, SchemaRef};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use itertools::{Itertools, izip};
use lance_arrow::{ArrowFloatType, FixedSizeListArrayExt, FloatArray, RecordBatchExt};
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, ROW_ID, Result};
use lance_file::versions::v1::reader::FileReader as V1FileReader;
use lance_linalg::distance::{DistanceType, Dot, dot, l2::l2};
use lance_linalg::simd::{
    self,
    dist_table::{BATCH_SIZE, PERM0, PERM0_INVERSE},
};
#[cfg(any(
    target_arch = "x86_64",
    target_arch = "aarch64",
    target_arch = "loongarch64"
))]
use lance_linalg::simd::{SIMD, f32::f32x16};
use lance_table::utils::LanceIteratorExtension;
use num_traits::AsPrimitive;
use prost::Message;
use serde::{Deserialize, Serialize};

use crate::frag_reuse::FragReuseIndex;
use crate::pb;
use crate::vector::ApproxMode;
use crate::vector::bq::dist_table_quant::{
    DistTableDequant, quantize_dist_table_into, quantize_dist_table_u16_into,
};
use crate::vector::bq::ex_dot::{
    EX_DOT_BLOCK_DIMS, ExDotFn, blocked_ex_code_bytes, ex_dot_kernel, pad_query_into,
    padded_query_len, repack_sequential_row, sequential_matches_blocked,
};
use crate::vector::bq::prune::{LowerBoundTerms, PRUNE_LANES, prune_mask_kernel};
use crate::vector::bq::rotation::{apply_fast_rotation, apply_fast_rotation_in_place};
use crate::vector::bq::transform::{
    ADD_FACTORS_COLUMN, ERROR_FACTORS_COLUMN, EX_ADD_FACTORS_COLUMN, EX_SCALE_FACTORS_COLUMN,
    SCALE_FACTORS_COLUMN,
};
use crate::vector::bq::{
    RQRotationType, rabit_binary_code_bytes, rabit_ex_bits, rabit_ex_code_bytes,
    validate_rq_num_bits,
};
use crate::vector::graph::{OrderedFloat, OrderedNode};
use crate::vector::pq::storage::transpose;
use crate::vector::quantizer::{QuantizerMetadata, QuantizerStorage};
use crate::vector::storage::{
    DistCalculator, DistanceCalculatorOptions, QueryResidual, RabitRawQueryContext, VectorStore,
};

pub const RABIT_METADATA_KEY: &str = "lance:rabit";
pub const RABIT_CODE_COLUMN: &str = "_rabit_codes";
/// Legacy ex-code column: sequential LSB-first bit stream per row. Read-only;
/// rows are repacked into the blocked layout at load time.
pub const RABIT_EX_CODE_COLUMN: &str = "__ex_codes";
/// Ex-code column in the blocked layout consumed by the ex-dot kernels (see
/// `ex_dot` module docs). Indexes written with this column cannot be read by
/// older versions, which fail with a missing-column error instead of
/// misinterpreting the bytes.
pub const RABIT_BLOCKED_EX_CODE_COLUMN: &str = "__blocked_ex_codes";
pub const SEGMENT_LENGTH: usize = 4;
pub const SEGMENT_NUM_CODES: usize = 1 << SEGMENT_LENGTH;
const RABIT_PRUNE_STATS_ENV: &str = "LANCE_RQ_PRUNE_STATS";
const RABIT_PRUNE_STATS_INTERVAL_ENV: &str = "LANCE_RQ_PRUNE_STATS_INTERVAL";
const DEFAULT_RABIT_PRUNE_STATS_INTERVAL: u64 = 1024;

#[derive(Default)]
struct RabitPruneStats {
    calls: AtomicU64,
    candidates: AtomicU64,
    pruned_upper_bound: AtomicU64,
    pruned_heap: AtomicU64,
    exact: AtomicU64,
    exact_rejected: AtomicU64,
}

#[derive(Default)]
struct RabitPruneBypassStats {
    calls: AtomicU64,
}

static RABIT_PRUNE_STATS: OnceLock<RabitPruneStats> = OnceLock::new();
static RABIT_PRUNE_BYPASS_STATS: OnceLock<RabitPruneBypassStats> = OnceLock::new();
static RABIT_PRUNE_STATS_ENABLED: OnceLock<bool> = OnceLock::new();
static RABIT_PRUNE_STATS_INTERVAL: OnceLock<u64> = OnceLock::new();

fn rabit_prune_stats_enabled() -> bool {
    *RABIT_PRUNE_STATS_ENABLED.get_or_init(|| match std::env::var(RABIT_PRUNE_STATS_ENV) {
        Ok(value) => {
            let value = value.to_ascii_lowercase();
            !matches!(value.as_str(), "" | "0" | "false" | "off" | "no")
        }
        Err(_) => false,
    })
}

fn rabit_prune_stats_interval() -> u64 {
    *RABIT_PRUNE_STATS_INTERVAL.get_or_init(|| {
        std::env::var(RABIT_PRUNE_STATS_INTERVAL_ENV)
            .ok()
            .and_then(|value| value.parse::<u64>().ok())
            .filter(|interval| *interval > 0)
            .unwrap_or(DEFAULT_RABIT_PRUNE_STATS_INTERVAL)
    })
}

fn ratio(numerator: u64, denominator: u64) -> f64 {
    if denominator == 0 {
        0.0
    } else {
        numerator as f64 / denominator as f64
    }
}

fn emit_rabit_prune_stats(message: &str) {
    log::warn!(
        target: "lance_index::vector::bq::prune_stats",
        "{}",
        message
    );
}

/// Per-scan tallies of the raw-query lower-bound gating, reported through
/// `record_rabit_prune_stats`.
#[derive(Default)]
struct RabitPruneCounters {
    candidates: usize,
    pruned_upper_bound: usize,
    pruned_heap: usize,
    exact: usize,
    exact_rejected: usize,
}

fn record_rabit_prune_stats(counters: &RabitPruneCounters) {
    if !rabit_prune_stats_enabled() {
        return;
    }
    let RabitPruneCounters {
        candidates,
        pruned_upper_bound,
        pruned_heap,
        exact,
        exact_rejected,
    } = *counters;

    let stats = RABIT_PRUNE_STATS.get_or_init(RabitPruneStats::default);
    let calls = stats.calls.fetch_add(1, Ordering::Relaxed) + 1;
    let candidates = stats
        .candidates
        .fetch_add(candidates as u64, Ordering::Relaxed)
        + candidates as u64;
    let pruned_upper_bound = stats
        .pruned_upper_bound
        .fetch_add(pruned_upper_bound as u64, Ordering::Relaxed)
        + pruned_upper_bound as u64;
    let pruned_heap = stats
        .pruned_heap
        .fetch_add(pruned_heap as u64, Ordering::Relaxed)
        + pruned_heap as u64;
    let exact = stats.exact.fetch_add(exact as u64, Ordering::Relaxed) + exact as u64;
    let exact_rejected = stats
        .exact_rejected
        .fetch_add(exact_rejected as u64, Ordering::Relaxed)
        + exact_rejected as u64;
    let interval = rabit_prune_stats_interval();
    if calls.is_multiple_of(interval) {
        let pruned = pruned_upper_bound + pruned_heap;
        emit_rabit_prune_stats(&format!(
            "ivf_rq_prune_stats calls={} candidates={} pruned={} pruned_upper_bound={} pruned_heap={} prune_ratio={:.6} exact={} exact_ratio={:.6} exact_rejected={} exact_reject_ratio={:.6}",
            calls,
            candidates,
            pruned,
            pruned_upper_bound,
            pruned_heap,
            ratio(pruned, candidates),
            exact,
            ratio(exact, candidates),
            exact_rejected,
            ratio(exact_rejected, exact),
        ));
    }
}

fn record_rabit_prune_bypass(reason: &'static str) {
    if !rabit_prune_stats_enabled() {
        return;
    }

    let stats = RABIT_PRUNE_BYPASS_STATS.get_or_init(RabitPruneBypassStats::default);
    let calls = stats.calls.fetch_add(1, Ordering::Relaxed) + 1;
    if calls.is_multiple_of(rabit_prune_stats_interval()) {
        emit_rabit_prune_stats(&format!(
            "ivf_rq_prune_stats_bypass calls={} reason={}",
            calls, reason
        ));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RabitQueryEstimator {
    ResidualQuery,
    RawQuery,
}

pub fn rabit_binary_code_field(rotated_dim: usize) -> Field {
    Field::new(
        RABIT_CODE_COLUMN,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::UInt8, true)),
            rabit_binary_code_bytes(rotated_dim) as i32,
        ),
        true,
    )
}

pub fn rabit_ex_code_field(rotated_dim: usize, num_bits: u8) -> Result<Option<Field>> {
    let ex_bits = rabit_ex_bits(num_bits)?;
    if ex_bits == 0 {
        return Ok(None);
    }
    Ok(Some(Field::new(
        RABIT_BLOCKED_EX_CODE_COLUMN,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::UInt8, true)),
            blocked_ex_code_bytes(rotated_dim, ex_bits) as i32,
        ),
        true,
    )))
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabitQuantizationMetadata {
    // this rotate matrix is large, and lance index would store all metadata in schema metadata,
    // which is in JSON format, so we skip it in serialization and deserialization, and store it
    // in the global buffer, which is a binary format (protobuf for now) for efficiency.
    #[serde(skip)]
    pub rotate_mat: Option<FixedSizeListArray>,
    #[serde(default)]
    pub rotate_mat_position: Option<u32>,
    #[serde(default)]
    pub fast_rotation_signs: Option<Vec<u8>>,
    #[serde(default = "default_rotation_type_compat")]
    pub rotation_type: RQRotationType,
    #[serde(default)]
    pub code_dim: u32,
    pub num_bits: u8,
    pub packed: bool,
    #[serde(default = "default_query_estimator_compat")]
    pub query_estimator: RabitQueryEstimator,
}

impl RabitQuantizationMetadata {
    pub fn rotated_dim(&self) -> usize {
        if self.code_dim > 0 {
            self.code_dim as usize
        } else {
            self.rotate_mat
                .as_ref()
                .map(|rotate_mat| rotate_mat.len())
                .unwrap_or(0)
        }
    }

    pub fn binary_code_bytes(&self) -> usize {
        rabit_binary_code_bytes(self.rotated_dim())
    }
}

fn default_rotation_type_compat() -> RQRotationType {
    // Older metadata does not have this field and always used dense matrices.
    RQRotationType::Matrix
}

fn default_query_estimator_compat() -> RabitQueryEstimator {
    // Released IVF_RQ indexes predate this marker and used residual queries.
    RabitQueryEstimator::ResidualQuery
}

impl RabitQuantizationMetadata {
    fn code_dim(&self) -> usize {
        self.rotated_dim()
    }

    fn rotate_vector_with_residual_into(
        &self,
        vector: &dyn Array,
        residual_centroid: Option<&dyn Array>,
        output: &mut [f32],
    ) {
        debug_assert_eq!(output.len(), self.code_dim());
        match self.rotation_type {
            RQRotationType::Matrix => {
                let rotate_mat = self
                    .rotate_mat
                    .as_ref()
                    .expect("RabitQ dense rotation metadata not loaded");

                match rotate_mat.value_type() {
                    DataType::Float16 => {
                        RabitQuantizationStorage::rotate_query_vector_dense_into::<Float16Type>(
                            rotate_mat,
                            vector,
                            residual_centroid,
                            output,
                        )
                    }
                    DataType::Float32 => {
                        RabitQuantizationStorage::rotate_query_vector_dense_into::<Float32Type>(
                            rotate_mat,
                            vector,
                            residual_centroid,
                            output,
                        )
                    }
                    DataType::Float64 => {
                        RabitQuantizationStorage::rotate_query_vector_dense_into::<Float64Type>(
                            rotate_mat,
                            vector,
                            residual_centroid,
                            output,
                        )
                    }
                    dt => unimplemented!("RabitQ does not support data type: {}", dt),
                }
            }
            RQRotationType::Fast => {
                let signs = self
                    .fast_rotation_signs
                    .as_ref()
                    .expect("RabitQ fast rotation metadata not loaded");
                match vector.data_type() {
                    DataType::Float16 => RabitQuantizationStorage::rotate_query_vector_fast_into::<
                        Float16Type,
                    >(
                        signs, vector, residual_centroid, output
                    ),
                    DataType::Float32 => {
                        RabitQuantizationStorage::rotate_query_vector_fast_f32_into(
                            signs,
                            vector,
                            residual_centroid,
                            output,
                        )
                    }
                    DataType::Float64 => RabitQuantizationStorage::rotate_query_vector_fast_into::<
                        Float64Type,
                    >(
                        signs, vector, residual_centroid, output
                    ),
                    dt => unimplemented!("RabitQ does not support data type: {}", dt),
                }
            }
        }
    }

    pub fn prepare_raw_query_context(&self, query: &dyn Array) -> Result<RabitRawQueryContext> {
        validate_rq_num_bits(self.num_bits)?;
        let code_dim = self.code_dim();
        let ex_bits = rabit_ex_bits(self.num_bits)?;
        let dist_table_len = code_dim * 4;

        let mut rotated_query = vec![0.0; code_dim];
        self.rotate_vector_with_residual_into(query, None, &mut rotated_query);

        let mut dist_table = vec![0.0; dist_table_len];
        build_dist_table_direct_into::<Float32Type>(&rotated_query, &mut dist_table);

        // The kernels consume the rotated query directly; a zero-padded copy
        // is only needed when the rotated dim is not block-aligned.
        let mut ex_query = Vec::new();
        if ex_bits > 0 && !code_dim.is_multiple_of(EX_DOT_BLOCK_DIMS) {
            ex_query.resize(padded_query_len(code_dim), 0.0);
            pad_query_into(&rotated_query, &mut ex_query);
        }

        let sum_q = rotated_query.iter().copied().sum();
        Ok(RabitRawQueryContext {
            code_dim,
            ex_bits,
            rotated_query,
            dist_table,
            ex_query,
            sum_q,
        })
    }
}

impl DeepSizeOf for RabitQuantizationMetadata {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.rotate_mat
            .as_ref()
            .map(|inv_p| (inv_p as &dyn arrow_array::Array).deep_size_of_children(context))
            .unwrap_or(0)
            + self
                .fast_rotation_signs
                .as_ref()
                .map(|signs| signs.len())
                .unwrap_or(0)
    }
}

#[async_trait]
impl QuantizerMetadata for RabitQuantizationMetadata {
    fn buffer_index(&self) -> Option<u32> {
        match self.rotation_type {
            RQRotationType::Matrix => self.rotate_mat_position,
            RQRotationType::Fast => None,
        }
    }

    fn set_buffer_index(&mut self, index: u32) {
        self.rotate_mat_position = Some(index);
    }

    fn parse_buffer(&mut self, bytes: Bytes) -> Result<()> {
        if self.rotation_type != RQRotationType::Matrix {
            return Ok(());
        }
        debug_assert!(!bytes.is_empty());
        let codebook_tensor: pb::Tensor = pb::Tensor::decode(bytes)?;
        self.rotate_mat = Some(FixedSizeListArray::try_from(&codebook_tensor)?);
        if self.code_dim == 0 {
            self.code_dim = self
                .rotate_mat
                .as_ref()
                .map(|rotate_mat| rotate_mat.len() as u32)
                .unwrap_or(0);
        }
        Ok(())
    }

    fn extra_metadata(&self) -> Result<Option<Bytes>> {
        match self.rotation_type {
            RQRotationType::Matrix => {
                if let Some(inv_p) = &self.rotate_mat {
                    let inv_p_tensor = pb::Tensor::try_from(inv_p)?;
                    let mut bytes = BytesMut::new();
                    inv_p_tensor.encode(&mut bytes)?;
                    Ok(Some(bytes.freeze()))
                } else {
                    Ok(None)
                }
            }
            RQRotationType::Fast => Ok(None),
        }
    }

    async fn load(reader: &V1FileReader) -> Result<Self> {
        let metadata_str = reader
            .schema()
            .metadata
            .get(RABIT_METADATA_KEY)
            .ok_or(Error::index(format!(
                "Reading Rabit metadata: metadata key {} not found",
                RABIT_METADATA_KEY
            )))?;
        serde_json::from_str(metadata_str)
            .map_err(|_| Error::index(format!("Failed to parse index metadata: {}", metadata_str)))
    }
}

#[derive(Debug, Clone)]
pub struct RabitQuantizationStorage {
    metadata: RabitQuantizationMetadata,
    batch: RecordBatch,
    distance_type: DistanceType,

    // helper fields
    row_ids: UInt64Array,
    codes: FixedSizeListArray,
    add_factors: Float32Array,
    scale_factors: Float32Array,
    error_factors: Option<Float32Array>,
    // ex codes in the blocked kernel layout; always aliases the batch column
    // (legacy sequential batches are normalized at load, replacing the
    // sequential column with the repacked one, so rewrites emit the blocked
    // format).
    ex_codes: Option<FixedSizeListArray>,
    packed_ex_codes: Option<FixedSizeListArray>,
    ex_add_factors: Option<Float32Array>,
    ex_scale_factors: Option<Float32Array>,
}

impl DeepSizeOf for RabitQuantizationStorage {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.metadata.deep_size_of_children(context)
            + self.batch.deep_size_of_children(context)
            + self
                .packed_ex_codes
                .as_ref()
                .map(|codes| (codes as &dyn Array).deep_size_of_children(context))
                .unwrap_or_default()
    }
}

impl RabitQuantizationStorage {
    fn code_dim(&self) -> usize {
        self.metadata.code_dim()
    }

    fn residual_query_factor(&self, dist_q_c: f32) -> f32 {
        match self.distance_type {
            DistanceType::L2 => dist_q_c,
            DistanceType::Cosine | DistanceType::Dot => dist_q_c - 1.0,
            _ => unimplemented!(
                "RabitQ does not support distance type: {}",
                self.distance_type
            ),
        }
    }

    fn raw_query_factor(
        &self,
        dist_q_c: f32,
        rotated_query: &[f32],
        rotated_centroid: Option<&[f32]>,
    ) -> f32 {
        match self.distance_type {
            DistanceType::L2 => dist_q_c,
            DistanceType::Dot => rotated_centroid
                .map(|centroid| -dot(rotated_query, centroid))
                .unwrap_or(dist_q_c - 1.0),
            DistanceType::Cosine => dist_q_c - 1.0,
            _ => unimplemented!(
                "RabitQ does not support distance type: {}",
                self.distance_type
            ),
        }
    }

    fn raw_query_error(
        &self,
        dist_q_c: f32,
        rotated_query: &[f32],
        rotated_centroid: Option<&[f32]>,
    ) -> f32 {
        match self.distance_type {
            DistanceType::L2 => dist_q_c.max(0.0).sqrt(),
            DistanceType::Dot => rotated_centroid
                .map(|centroid| l2(rotated_query, centroid).sqrt())
                .unwrap_or_else(|| dist_q_c.max(0.0).sqrt()),
            DistanceType::Cosine => dist_q_c.max(0.0).sqrt(),
            _ => unimplemented!(
                "RabitQ does not support distance type: {}",
                self.distance_type
            ),
        }
    }

    fn uses_raw_query_lower_bound_gating(&self) -> bool {
        self.metadata.query_estimator == RabitQueryEstimator::RawQuery
            && self.metadata.num_bits > 1
            && self.error_factors.is_some()
    }

    fn raw_query_error_for_gating(
        &self,
        dist_q_c: f32,
        rotated_query: &[f32],
        rotated_centroid: Option<&[f32]>,
    ) -> f32 {
        if self.uses_raw_query_lower_bound_gating() {
            self.raw_query_error(dist_q_c, rotated_query, rotated_centroid)
        } else {
            0.0
        }
    }

    fn distance_calculator_from_parts<'a>(
        &'a self,
        parts: RabitDistCalculatorParts<'a>,
    ) -> RabitDistCalculator<'a> {
        let RabitDistCalculatorParts {
            dim,
            dist_table,
            ex_query,
            sum_q,
            query_factor,
            query_error,
            approx_mode,
        } = parts;
        let ex_code_len = self
            .ex_codes
            .as_ref()
            .map(|codes| codes.value_length() as usize)
            .unwrap_or_default();
        let ex_codes = self
            .ex_codes
            .as_ref()
            .map(|codes| codes.values().as_primitive::<UInt8Type>().values().as_ref());
        let packed_ex_codes = self
            .packed_ex_codes
            .as_ref()
            .map(|codes| codes.values().as_primitive::<UInt8Type>().values().as_ref());
        RabitDistCalculator::new(
            dim,
            self.metadata.num_bits,
            self.metadata.query_estimator,
            dist_table,
            ex_query,
            sum_q,
            self.codes.values().as_primitive::<UInt8Type>().values(),
            ex_codes,
            ex_code_len,
            self.add_factors.values(),
            self.scale_factors.values(),
            self.error_factors
                .as_ref()
                .map(|factors| factors.values().as_ref()),
            self.ex_add_factors
                .as_ref()
                .map(|factors| factors.values().as_ref()),
            self.ex_scale_factors
                .as_ref()
                .map(|factors| factors.values().as_ref()),
            packed_ex_codes,
            query_factor,
            query_error,
            approx_mode,
        )
    }

    fn rotate_query_vector(&self, code_dim: usize, qr: &dyn Array) -> Vec<f32> {
        let mut output = vec![0.0f32; code_dim];
        self.rotate_query_vector_into(code_dim, qr, None, &mut output);
        output
    }

    fn rotate_query_vector_into(
        &self,
        code_dim: usize,
        qr: &dyn Array,
        residual_centroid: Option<&dyn Array>,
        output: &mut [f32],
    ) {
        debug_assert_eq!(output.len(), code_dim);
        self.metadata
            .rotate_vector_with_residual_into(qr, residual_centroid, output);
    }

    fn rotate_query_vector_dense_into<T: ArrowFloatType>(
        rotate_mat: &FixedSizeListArray,
        qr: &dyn Array,
        residual_centroid: Option<&dyn Array>,
        output: &mut [f32],
    ) where
        T::Native: AsPrimitive<f32> + Dot + Sub<Output = T::Native>,
    {
        let d = qr.len();
        let code_dim = rotate_mat.len();
        debug_assert_eq!(output.len(), code_dim);
        let rotate_mat = rotate_mat
            .values()
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .unwrap()
            .as_slice();

        let qr = qr
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .unwrap()
            .as_slice();

        if let Some(residual_centroid) = residual_centroid {
            let residual_centroid = residual_centroid
                .as_any()
                .downcast_ref::<T::ArrayType>()
                .unwrap()
                .as_slice();
            debug_assert_eq!(residual_centroid.len(), d);
            for (chunk, out) in rotate_mat.chunks_exact(code_dim).zip(output.iter_mut()) {
                let mut sum = 0.0;
                for idx in 0..d {
                    let residual = qr[idx] - residual_centroid[idx];
                    sum += chunk[idx].as_() * residual.as_();
                }
                *out = sum;
            }
        } else {
            rotate_mat
                .chunks_exact(code_dim)
                .zip(output.iter_mut())
                .for_each(|(chunk, out)| {
                    *out = lance_linalg::distance::dot(&chunk[..d], qr);
                });
        }
    }

    fn rotate_query_vector_fast_into<T: ArrowFloatType>(
        signs: &[u8],
        qr: &dyn Array,
        residual_centroid: Option<&dyn Array>,
        output: &mut [f32],
    ) where
        T::Native: AsPrimitive<f32> + Sub<Output = T::Native>,
    {
        let qr = qr
            .as_any()
            .downcast_ref::<T::ArrayType>()
            .unwrap()
            .as_slice();

        if let Some(residual_centroid) = residual_centroid {
            let residual_centroid = residual_centroid
                .as_any()
                .downcast_ref::<T::ArrayType>()
                .unwrap()
                .as_slice();
            let input_len = qr.len().min(output.len());
            debug_assert!(residual_centroid.len() >= input_len);
            for idx in 0..input_len {
                output[idx] = (qr[idx] - residual_centroid[idx]).as_();
            }
            if input_len < output.len() {
                output[input_len..].fill(0.0);
            }
            apply_fast_rotation_in_place(output, signs);
        } else {
            apply_fast_rotation(qr, output, signs);
        }
    }

    fn rotate_query_vector_fast_f32_into(
        signs: &[u8],
        qr: &dyn Array,
        residual_centroid: Option<&dyn Array>,
        output: &mut [f32],
    ) {
        let qr = qr.as_any().downcast_ref::<Float32Array>().unwrap().values();

        if let Some(residual_centroid) = residual_centroid {
            let residual_centroid = residual_centroid
                .as_any()
                .downcast_ref::<Float32Array>()
                .unwrap()
                .values();
            copy_subtract_f32(qr, residual_centroid, output);
            apply_fast_rotation_in_place(output, signs);
        } else {
            apply_fast_rotation(qr, output, signs);
        }
    }
}

#[inline]
fn copy_subtract_f32(lhs: &[f32], rhs: &[f32], output: &mut [f32]) {
    let input_len = lhs.len().min(output.len());
    debug_assert!(rhs.len() >= input_len);

    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "loongarch64"
    ))]
    let simd_len = input_len / f32x16::LANES * f32x16::LANES;
    #[cfg(not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "loongarch64"
    )))]
    let simd_len = 0;

    #[cfg(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "loongarch64"
    ))]
    for idx in (0..simd_len).step_by(f32x16::LANES) {
        let lhs = f32x16::from(&lhs[idx..]);
        let rhs = f32x16::from(&rhs[idx..]);
        let result = lhs - rhs;
        unsafe {
            result.store_unaligned(output.as_mut_ptr().add(idx));
        }
    }

    for idx in simd_len..input_len {
        output[idx] = lhs[idx] - rhs[idx];
    }
    if input_len < output.len() {
        output[input_len..].fill(0.0);
    }
}

struct RabitDistCalculatorParts<'a> {
    dim: usize,
    dist_table: Cow<'a, [f32]>,
    ex_query: Cow<'a, [f32]>,
    sum_q: f32,
    query_factor: f32,
    query_error: f32,
    approx_mode: ApproxMode,
}

/// Loop-invariant inputs of the raw-query multi-bit top-k scans: the row
/// count, the resolved ex-code state for exact reranking, and the query
/// bounds.
struct RawQueryTopkContext<'a> {
    n: usize,
    k: usize,
    ex_bits: u8,
    ex_codes: &'a [u8],
    ex_add_factors: &'a [f32],
    ex_scale_factors: &'a [f32],
    query_lower_bound: f32,
    query_upper_bound: f32,
}

/// Pick the query slice the ex-dot kernels consume: the rotated query itself
/// when the dim is block-aligned, otherwise a zero-padded copy.
fn kernel_query<'a>(rotated_query: &'a [f32], padded: &'a [f32]) -> &'a [f32] {
    if rotated_query.len().is_multiple_of(EX_DOT_BLOCK_DIMS) {
        rotated_query
    } else {
        padded
    }
}

pub struct RabitDistCalculator<'a> {
    dim: usize,
    num_bits: u8,
    query_estimator: RabitQueryEstimator,
    // n * d / 8 binary-code bytes
    codes: &'a [u8],
    // per-row ex codes in the blocked kernel layout
    ex_codes: Option<&'a [u8]>,
    // bytes per ex-code row; legacy rows for layout-compatible widths may be
    // shorter than the blocked size, which the kernels treat as zero padding
    ex_code_len: usize,
    // this is a flattened 2D array of size d/4 * 16,
    // we split the query codes into d/4 chunks, each chunk is with 4 elements,
    // then dist_table[i][j] is the distance between the i-th query code and the code j
    dist_table: Cow<'a, [f32]>,
    // the rotated query, zero-padded to a 64-dim multiple when needed; also
    // the source for the FastScan ex LUT on the legacy bypass path
    ex_query: Cow<'a, [f32]>,
    ex_dot: Option<ExDotFn>,
    add_factors: &'a [f32],
    scale_factors: &'a [f32],
    error_factors: Option<&'a [f32]>,
    ex_add_factors: Option<&'a [f32]>,
    ex_scale_factors: Option<&'a [f32]>,
    packed_ex_codes: Option<&'a [u8]>,
    query_factor: f32,
    query_error: f32,
    approx_mode: ApproxMode,

    sum_q: f32,
    sqrt_d: f32,
}

impl<'a> RabitDistCalculator<'a> {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        dim: usize,
        num_bits: u8,
        query_estimator: RabitQueryEstimator,
        dist_table: Cow<'a, [f32]>,
        ex_query: Cow<'a, [f32]>,
        sum_q: f32,
        codes: &'a [u8],
        ex_codes: Option<&'a [u8]>,
        ex_code_len: usize,
        add_factors: &'a [f32],
        scale_factors: &'a [f32],
        error_factors: Option<&'a [f32]>,
        ex_add_factors: Option<&'a [f32]>,
        ex_scale_factors: Option<&'a [f32]>,
        packed_ex_codes: Option<&'a [u8]>,
        query_factor: f32,
        query_error: f32,
        approx_mode: ApproxMode,
    ) -> Self {
        let ex_dot = (num_bits > 1).then(|| ex_dot_kernel(num_bits - 1));
        Self {
            dim,
            num_bits,
            query_estimator,
            codes,
            ex_codes,
            ex_code_len,
            dist_table,
            ex_query,
            ex_dot,
            add_factors,
            scale_factors,
            error_factors,
            ex_add_factors,
            ex_scale_factors,
            packed_ex_codes,
            query_factor,
            query_error,
            approx_mode,
            sqrt_d: (dim as f32 * num_bits as f32).sqrt(),
            sum_q,
        }
    }

    /// `sum_d query[d] * ex_code[d]` for the candidate's packed ex codes.
    #[inline]
    fn ex_code_dot(&self, ex_codes: &[u8], id: usize) -> f32 {
        let ex_dot = self
            .ex_dot
            .expect("raw-query multi-bit RQ requires an ex-dot kernel");
        ex_dot(
            self.ex_query.as_ref(),
            &ex_codes[id * self.ex_code_len..(id + 1) * self.ex_code_len],
        )
    }

    /// Fill `dists[0..n]` with exact per-row binary distances computed
    /// directly from the f32 dist table — the fallback when the quantized
    /// reconstruction scale would be non-finite ([`DistTableDequant::Exact`]).
    #[allow(clippy::uninit_vec)]
    fn fill_exact_binary_distances(&self, n: usize, code_len: usize, dists: &mut Vec<f32>) {
        dists.clear();
        dists.reserve(n);
        // SAFETY: the loop initializes every element in [0, n).
        unsafe {
            dists.set_len(n);
        }
        dists.iter_mut().enumerate().for_each(|(id, dist)| {
            *dist = compute_single_rq_distance(self.codes, id, n, code_len, &self.dist_table);
        });
    }

    #[allow(clippy::uninit_vec)]
    fn binary_distances_with_scratch(
        &self,
        n: usize,
        code_len: usize,
        dists: &mut Vec<f32>,
        quantized_dists: &mut Vec<u16>,
        quantized_dists_table: &mut Vec<u8>,
        hacc_quantized_dists: &mut Vec<u32>,
    ) -> usize {
        if self.approx_mode == ApproxMode::Accurate {
            return self.binary_distances_hacc_with_scratch(
                n,
                code_len,
                dists,
                quantized_dists,
                quantized_dists_table,
                hacc_quantized_dists,
            );
        }

        let (qmin, qmax) = match quantize_dist_table_into(&self.dist_table, quantized_dists_table) {
            DistTableDequant::Affine { qmin, qmax } => (qmin, qmax),
            DistTableDequant::Exact => {
                // The affine reconstruction would be non-finite; compute every
                // binary distance exactly and report no SIMD rows so the
                // ex-rerank caller takes the per-row path for all of them.
                self.fill_exact_binary_distances(n, code_len, dists);
                return 0;
            }
        };
        let remainder = n % BATCH_SIZE;
        let simd_len = n - remainder;
        quantized_dists.clear();
        quantized_dists.reserve(simd_len);
        // SAFETY: sum_4bit_dist_table overwrites each element in the SIMD batch range.
        unsafe {
            quantized_dists.set_len(simd_len);
        }
        simd::dist_table::sum_4bit_dist_table(
            simd_len,
            code_len,
            self.codes,
            quantized_dists_table,
            quantized_dists,
        );

        let range = (qmax - qmin) / 255.0;
        let num_tables = quantized_dists_table.len() / SEGMENT_NUM_CODES;
        let sum_min = num_tables as f32 * qmin;
        dists.clear();
        dists.reserve(n);
        // SAFETY: the SIMD section below writes [0, simd_len), and the
        // remainder section writes [simd_len, n).
        unsafe {
            dists.set_len(n);
        }
        let (simd_dists, remainder_dists) = dists.split_at_mut(simd_len);
        simd_dists
            .iter_mut()
            .zip(quantized_dists.iter())
            .for_each(|(dist, q_dist)| {
                *dist = (*q_dist as f32) * range + sum_min;
            });

        remainder_dists
            .iter_mut()
            .enumerate()
            .for_each(|(id, dist)| {
                *dist = compute_single_rq_distance(
                    self.codes,
                    simd_len + id,
                    n,
                    code_len,
                    &self.dist_table,
                );
            });
        simd_len
    }

    #[allow(clippy::uninit_vec)]
    fn binary_distances_hacc_with_scratch(
        &self,
        n: usize,
        code_len: usize,
        dists: &mut Vec<f32>,
        quantized_dist_table: &mut Vec<u16>,
        hacc_dist_table: &mut Vec<u8>,
        quantized_dists: &mut Vec<u32>,
    ) -> usize {
        let (qmin, qmax) =
            match quantize_dist_table_u16_into(&self.dist_table, quantized_dist_table) {
                DistTableDequant::Affine { qmin, qmax } => (qmin, qmax),
                DistTableDequant::Exact => {
                    // See binary_distances_with_scratch: non-finite affine
                    // scale falls back to exact per-row distances.
                    self.fill_exact_binary_distances(n, code_len, dists);
                    return 0;
                }
            };
        simd::dist_table::transfer_4bit_dist_table_u16(quantized_dist_table, hacc_dist_table);
        let remainder = n % BATCH_SIZE;
        let simd_len = n - remainder;
        quantized_dists.clear();
        quantized_dists.reserve(simd_len);
        // SAFETY: sum_4bit_hacc_dist_table overwrites each element in the batch range.
        unsafe {
            quantized_dists.set_len(simd_len);
        }
        simd::dist_table::sum_4bit_hacc_dist_table(
            simd_len,
            code_len,
            self.codes,
            hacc_dist_table,
            quantized_dists,
        );

        let range = (qmax - qmin) / u16::MAX as f32;
        let num_tables = quantized_dist_table.len() / SEGMENT_NUM_CODES;
        let sum_min = num_tables as f32 * qmin;
        dists.clear();
        dists.reserve(n);
        // SAFETY: the batch section writes [0, simd_len), and the
        // remainder section writes [simd_len, n).
        unsafe {
            dists.set_len(n);
        }
        let (simd_dists, remainder_dists) = dists.split_at_mut(simd_len);
        simd_dists
            .iter_mut()
            .zip(quantized_dists.iter())
            .for_each(|(dist, q_dist)| {
                *dist = (*q_dist as f32) * range + sum_min;
            });

        remainder_dists
            .iter_mut()
            .enumerate()
            .for_each(|(id, dist)| {
                *dist = compute_single_rq_distance(
                    self.codes,
                    simd_len + id,
                    n,
                    code_len,
                    &self.dist_table,
                );
            });
        simd_len
    }

    #[inline]
    fn binary_distance_factor_params(&self) -> (f32, f32) {
        match self.query_estimator {
            RabitQueryEstimator::ResidualQuery => (2.0 / self.sqrt_d, -self.sum_q / self.sqrt_d),
            RabitQueryEstimator::RawQuery => (1.0, -0.5 * self.sum_q),
        }
    }

    #[allow(clippy::uninit_vec)]
    fn one_bit_distances_with_scratch(
        &self,
        n: usize,
        code_len: usize,
        dists: &mut Vec<f32>,
        quantized_dists: &mut Vec<u16>,
        quantized_dists_table: &mut Vec<u8>,
        hacc_quantized_dists: &mut Vec<u32>,
    ) {
        self.binary_distances_with_scratch(
            n,
            code_len,
            dists,
            quantized_dists,
            quantized_dists_table,
            hacc_quantized_dists,
        );
        let (binary_distance_multiplier, binary_distance_offset) =
            self.binary_distance_factor_params();
        dists.iter_mut().enumerate().for_each(|(id, dist)| {
            let binary_dist = *dist;
            *dist = (binary_dist * binary_distance_multiplier + binary_distance_offset)
                * self.scale_factors[id]
                + self.add_factors[id]
                + self.query_factor;
        });
    }

    #[allow(clippy::uninit_vec)]
    fn apply_raw_query_multi_bit_distances(
        &self,
        simd_len: usize,
        dists: &mut [f32],
        quantized_dists: &mut Vec<u16>,
        quantized_dists_table: &mut Vec<u8>,
    ) {
        let ex_bits = self.num_bits - 1;
        let ex_codes = self
            .ex_codes
            .expect("raw-query multi-bit RQ requires ex codes");
        let ex_add_factors = self
            .ex_add_factors
            .expect("raw-query multi-bit RQ requires ex add factors");
        let ex_scale_factors = self
            .ex_scale_factors
            .expect("raw-query multi-bit RQ requires ex scale factors");
        let code_scale = (1u32 << ex_bits) as f32;
        let code_bias = -(code_scale - 0.5);

        let fastscan_len = if simd_len > 0 && supports_ex_fastscan(ex_bits) {
            self.packed_ex_codes
                .map(|packed_ex_codes| {
                    let fastscan_len = simd_len;
                    let fastscan_code_len = self.ex_code_len;
                    let (qmin, qmax, quantization_max) = quantize_ex_fastscan_dist_table_into(
                        ex_bits,
                        self.ex_code_len,
                        self.ex_query.as_ref(),
                        quantized_dists_table,
                    );
                    quantized_dists.clear();
                    quantized_dists.reserve(fastscan_len);
                    // SAFETY: sum_4bit_dist_table overwrites each element in the SIMD batch range.
                    unsafe {
                        quantized_dists.set_len(fastscan_len);
                    }
                    simd::dist_table::sum_4bit_dist_table(
                        fastscan_len,
                        fastscan_code_len,
                        packed_ex_codes,
                        quantized_dists_table,
                        quantized_dists,
                    );

                    let range = (qmax - qmin) / quantization_max;
                    let num_tables = quantized_dists_table.len() / SEGMENT_NUM_CODES;
                    let sum_min = num_tables as f32 * qmin;
                    dists
                        .iter_mut()
                        .take(fastscan_len)
                        .zip(quantized_dists.iter())
                        .enumerate()
                        .for_each(|(id, (dist, q_ex_dist))| {
                            let ex_dist = (*q_ex_dist as f32) * range + sum_min;
                            let full_dot = code_scale * *dist + ex_dist + code_bias * self.sum_q;
                            *dist = full_dot * ex_scale_factors[id]
                                + ex_add_factors[id]
                                + self.query_factor;
                        });
                    fastscan_len
                })
                .unwrap_or_default()
        } else {
            0
        };

        dists
            .iter_mut()
            .enumerate()
            .skip(fastscan_len)
            .for_each(|(id, dist)| {
                let ex_dist = self.ex_code_dot(ex_codes, id);
                let full_dot = code_scale * *dist + ex_dist + code_bias * self.sum_q;
                *dist = full_dot * ex_scale_factors[id] + ex_add_factors[id] + self.query_factor;
            });
    }

    #[inline]
    fn raw_query_binary_distance(&self, id: usize, binary_ip: f32) -> f32 {
        (binary_ip - 0.5 * self.sum_q) * self.scale_factors[id]
            + self.add_factors[id]
            + self.query_factor
    }

    #[inline]
    fn raw_query_lower_bound(&self, id: usize, binary_ip: f32) -> Option<f32> {
        let error_factors = self.error_factors?;
        Some(self.raw_query_binary_distance(id, binary_ip) - error_factors[id] * self.query_error)
    }

    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn raw_query_multi_bit_exact_distance(
        &self,
        id: usize,
        binary_ip: f32,
        ex_bits: u8,
        ex_codes: &[u8],
        ex_add_factors: &[f32],
        ex_scale_factors: &[f32],
    ) -> f32 {
        let ex_dist = self.ex_code_dot(ex_codes, id);
        let code_bias = -((1u32 << ex_bits) as f32 - 0.5);
        let full_dot = (1u32 << ex_bits) as f32 * binary_ip + ex_dist + code_bias * self.sum_q;
        full_dot * ex_scale_factors[id] + ex_add_factors[id] + self.query_factor
    }

    /// Compute the binary inner products into `dists` and resolve the inputs
    /// shared by the raw-query multi-bit top-k scans. Returns `None` when the
    /// partition has no rows.
    #[allow(clippy::too_many_arguments)]
    fn raw_query_multi_bit_topk_context(
        &self,
        k: usize,
        lower_bound: Option<f32>,
        upper_bound: Option<f32>,
        dists: &mut Vec<f32>,
        quantized_dists: &mut Vec<u16>,
        quantized_dists_table: &mut Vec<u8>,
        hacc_quantized_dists: &mut Vec<u32>,
    ) -> Option<RawQueryTopkContext<'_>> {
        let code_len = rabit_binary_code_bytes(self.dim);
        let n = self.codes.len() / code_len;
        if n == 0 {
            dists.clear();
            quantized_dists.clear();
            hacc_quantized_dists.clear();
            return None;
        }

        self.binary_distances_with_scratch(
            n,
            code_len,
            dists,
            quantized_dists,
            quantized_dists_table,
            hacc_quantized_dists,
        );

        Some(RawQueryTopkContext {
            n,
            k,
            ex_bits: self.num_bits - 1,
            ex_codes: self
                .ex_codes
                .expect("raw-query multi-bit RQ requires ex codes"),
            ex_add_factors: self
                .ex_add_factors
                .expect("raw-query multi-bit RQ requires ex add factors"),
            ex_scale_factors: self
                .ex_scale_factors
                .expect("raw-query multi-bit RQ requires ex scale factors"),
            query_lower_bound: lower_bound.unwrap_or(f32::MIN),
            query_upper_bound: upper_bound.unwrap_or(f32::MAX),
        })
    }

    /// Process one candidate row given its lower bound: the bound checks,
    /// the exact rerank, and the heap update shared by the sparse scan and
    /// the dense scan's surviving lanes and tail.
    #[inline]
    #[allow(clippy::too_many_arguments)]
    fn accumulate_raw_query_multi_bit_row(
        &self,
        ctx: &RawQueryTopkContext<'_>,
        id: usize,
        row_id: u64,
        binary_ip: f32,
        raw_lower_bound: f32,
        res: &mut BinaryHeap<OrderedNode<u64>>,
        max_dist: &mut Option<OrderedFloat>,
        counters: &mut RabitPruneCounters,
    ) {
        if raw_lower_bound >= ctx.query_upper_bound {
            counters.pruned_upper_bound += 1;
            return;
        }
        if res.len() >= ctx.k && max_dist.is_some_and(|max_dist| raw_lower_bound >= max_dist.0) {
            counters.pruned_heap += 1;
            return;
        }

        counters.exact += 1;
        let dist = self.raw_query_multi_bit_exact_distance(
            id,
            binary_ip,
            ctx.ex_bits,
            ctx.ex_codes,
            ctx.ex_add_factors,
            ctx.ex_scale_factors,
        );
        if dist < ctx.query_lower_bound || dist >= ctx.query_upper_bound {
            counters.exact_rejected += 1;
            return;
        }
        let dist = OrderedFloat(dist);
        if res.len() < ctx.k {
            res.push(OrderedNode::new(row_id, dist));
            if res.len() == ctx.k {
                *max_dist = res.peek().map(|node| node.dist);
            }
        } else if max_dist.is_some_and(|max_dist| max_dist > dist) {
            res.pop();
            res.push(OrderedNode::new(row_id, dist));
            *max_dist = res.peek().map(|node| node.dist);
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn accumulate_raw_query_multi_bit_topk_with_scratch(
        &self,
        k: usize,
        lower_bound: Option<f32>,
        upper_bound: Option<f32>,
        row_ids: impl Iterator<Item = (usize, u64)>,
        res: &mut BinaryHeap<OrderedNode<u64>>,
        dists: &mut Vec<f32>,
        quantized_dists: &mut Vec<u16>,
        quantized_dists_table: &mut Vec<u8>,
        hacc_quantized_dists: &mut Vec<u32>,
    ) {
        let Some(ctx) = self.raw_query_multi_bit_topk_context(
            k,
            lower_bound,
            upper_bound,
            dists,
            quantized_dists,
            quantized_dists_table,
            hacc_quantized_dists,
        ) else {
            return;
        };
        let mut max_dist = res.peek().map(|node| node.dist);
        let mut counters = RabitPruneCounters::default();

        for (id, row_id) in row_ids {
            let Some(binary_ip) = dists.get(id).copied() else {
                continue;
            };
            counters.candidates += 1;
            let Some(raw_lower_bound) = self.raw_query_lower_bound(id, binary_ip) else {
                continue;
            };
            self.accumulate_raw_query_multi_bit_row(
                &ctx,
                id,
                row_id,
                binary_ip,
                raw_lower_bound,
                res,
                &mut max_dist,
                &mut counters,
            );
        }
        record_rabit_prune_stats(&counters);
    }

    /// Top-k scan over all rows `0..n` in order: classify [`PRUNE_LANES`]
    /// rows at a time with the SIMD lower-bound kernel and run the scalar
    /// rerank only for the surviving lanes.
    #[allow(clippy::too_many_arguments)]
    fn accumulate_raw_query_multi_bit_topk_dense_with_scratch(
        &self,
        k: usize,
        lower_bound: Option<f32>,
        upper_bound: Option<f32>,
        row_id: impl Fn(u32) -> u64,
        res: &mut BinaryHeap<OrderedNode<u64>>,
        dists: &mut Vec<f32>,
        quantized_dists: &mut Vec<u16>,
        quantized_dists_table: &mut Vec<u8>,
        hacc_quantized_dists: &mut Vec<u32>,
    ) {
        let Some(ctx) = self.raw_query_multi_bit_topk_context(
            k,
            lower_bound,
            upper_bound,
            dists,
            quantized_dists,
            quantized_dists_table,
            hacc_quantized_dists,
        ) else {
            return;
        };
        let dists = dists.as_slice();
        debug_assert_eq!(dists.len(), ctx.n);
        let scale_factors = &self.scale_factors[..ctx.n];
        let add_factors = &self.add_factors[..ctx.n];
        let error_factors = &self
            .error_factors
            .expect("raw-query lower-bound gating requires error factors")[..ctx.n];
        // Same expression as `raw_query_lower_bound` with `error_factors`
        // already resolved; the masks below match it bit for bit.
        let lower_bound_of = |id: usize, binary_ip: f32| {
            self.raw_query_binary_distance(id, binary_ip) - error_factors[id] * self.query_error
        };
        let terms = LowerBoundTerms {
            half_sum_q: 0.5 * self.sum_q,
            query_factor: self.query_factor,
            query_error: self.query_error,
        };
        let prune_masks = prune_mask_kernel();
        let mut max_dist = res.peek().map(|node| node.dist);
        let mut counters = RabitPruneCounters::default();

        let (dist_groups, dist_tail) = dists.as_chunks::<PRUNE_LANES>();
        let (scale_groups, _) = scale_factors.as_chunks::<PRUNE_LANES>();
        let (add_groups, _) = add_factors.as_chunks::<PRUNE_LANES>();
        let (error_groups, _) = error_factors.as_chunks::<PRUNE_LANES>();
        for (group, (dist16, scale16, add16, error16)) in
            izip!(dist_groups, scale_groups, add_groups, error_groups).enumerate()
        {
            counters.candidates += PRUNE_LANES;
            // The heap threshold only ever tightens, so this group-start
            // snapshot can only over-select survivors (which the per-row
            // processing below re-checks against live values), never prune a
            // row the scalar scan would have kept.
            let heap_threshold = (res.len() >= ctx.k)
                .then(|| max_dist.map(|max_dist| max_dist.0))
                .flatten();
            let (pruned_upper_bound, pruned_heap) = prune_masks(
                dist16,
                scale16,
                add16,
                error16,
                terms,
                ctx.query_upper_bound,
                heap_threshold,
            );
            counters.pruned_upper_bound += pruned_upper_bound.count_ones() as usize;
            counters.pruned_heap += pruned_heap.count_ones() as usize;
            let mut survivors = !(pruned_upper_bound | pruned_heap);
            while survivors != 0 {
                let lane = survivors.trailing_zeros() as usize;
                survivors &= survivors - 1;
                let id = group * PRUNE_LANES + lane;
                let binary_ip = dists[id];
                self.accumulate_raw_query_multi_bit_row(
                    &ctx,
                    id,
                    row_id(id as u32),
                    binary_ip,
                    lower_bound_of(id, binary_ip),
                    res,
                    &mut max_dist,
                    &mut counters,
                );
            }
        }

        let tail_start = ctx.n - dist_tail.len();
        for (offset, binary_ip) in dist_tail.iter().copied().enumerate() {
            let id = tail_start + offset;
            counters.candidates += 1;
            self.accumulate_raw_query_multi_bit_row(
                &ctx,
                id,
                row_id(id as u32),
                binary_ip,
                lower_bound_of(id, binary_ip),
                res,
                &mut max_dist,
                &mut counters,
            );
        }
        record_rabit_prune_stats(&counters);
    }

    fn raw_query_lower_bound_gating_disabled_reason(&self) -> Option<&'static str> {
        if self.approx_mode == ApproxMode::Fast {
            Some("approx_mode_fast")
        } else if self.query_estimator != RabitQueryEstimator::RawQuery {
            Some("residual_query_estimator")
        } else if self.num_bits <= 1 {
            Some("num_bits_le_one")
        } else if self.error_factors.is_none() {
            Some("missing_error_factors")
        } else {
            None
        }
    }
}

#[inline]
fn lowbit(x: usize) -> usize {
    1 << x.trailing_zeros()
}

#[inline]
pub fn build_dist_table_direct<T: ArrowFloatType>(qc: &[T::Native]) -> Vec<f32>
where
    T::Native: AsPrimitive<f32>,
{
    // every 4 bits (SEGMENT_LENGTH) is a segment, and we need to compute the distance between the segment and all the codes
    // so there are dim/4 segments, and the number of codes is 16 (2^{SEGMENT_LENGTH}),
    // so we have dim/4 * 16 = dim * 4 elements in the dist_table
    let mut dist_table = vec![0.0; qc.len() * 4];
    build_dist_table_direct_into::<T>(qc, &mut dist_table);
    dist_table
}

fn build_dist_table_direct_into<T: ArrowFloatType>(qc: &[T::Native], dist_table: &mut [f32])
where
    T::Native: AsPrimitive<f32>,
{
    debug_assert_eq!(dist_table.len(), qc.len() * 4);
    qc.chunks_exact(SEGMENT_LENGTH)
        .zip(dist_table.chunks_exact_mut(SEGMENT_NUM_CODES))
        .for_each(|(sub_vec, dist_table)| {
            dist_table[0] = 0.0;
            build_dist_table_for_subvec::<T>(sub_vec, dist_table);
        });
}

#[inline(always)]
fn build_dist_table_for_subvec<T: ArrowFloatType>(sub_vec: &[T::Native], dist_table: &mut [f32])
where
    T::Native: AsPrimitive<f32>,
{
    // skip 0 because it's always 0
    (1..SEGMENT_NUM_CODES).for_each(|j| {
        // this is a little bit tricky,
        // j represents a subset of 4 bits, that if the i-th bit of `j` is 1,
        // then we need to add the distance of the i-th dim of the segment.
        // but we don't need to check all bits of `j`,
        // because `j` = `j - lowbit(j)` + `lowbit(j)`,
        // where `j-lowbit(j)` is less than `j`,
        // which means dist_table[j-lowbit(j)] is already computed,
        // and we can use it to compute dist_table[j]
        // for example, if j = 0b1010, then j - lowbit(j) = 0b1000,
        // and dist_table[0b1000] is already computed,
        // so dist_table[0b1010] = dist_table[0b1000] + sub_vec[LOWBIT_IDX[0b1010]];
        // where lowbit(0b1010) = 0b10, LOWBIT_IDX[0b1010] = LOWBIT_IDX[0b10] = 1.
        dist_table[j] = dist_table[j - lowbit(j)] + sub_vec[LOWBIT_IDX[j]].as_();
    })
}

/// Build the u8 FastScan LUT for the ex codes directly from the rotated
/// query (`ex_query`, natural dim order, padding dims zero): the underlying
/// per-dim table is the pure multiplication `q[d] * code`, so no intermediate
/// `dim * 2^ex_bits` table is materialized.
fn quantize_ex_fastscan_dist_table_into(
    ex_bits: u8,
    ex_code_len: usize,
    ex_query: &[f32],
    quantized_dist_table: &mut Vec<u8>,
) -> (f32, f32, f32) {
    debug_assert!(supports_ex_fastscan(ex_bits));

    // One split table per code nibble of the row.
    let num_split_tables = ex_code_len * 2;
    let quantization_max = (u16::MAX as usize / num_split_tables)
        .min(u8::MAX as usize)
        .max(1) as f32;

    let mut qmin = f32::INFINITY;
    let mut qmax = f32::NEG_INFINITY;
    for table_idx in 0..num_split_tables {
        for code in 0..SEGMENT_NUM_CODES {
            let value = ex_fastscan_dist_table_value(ex_query, ex_bits, table_idx, code);
            qmin = qmin.min(value);
            qmax = qmax.max(value);
        }
    }

    quantized_dist_table.clear();
    quantized_dist_table.reserve(num_split_tables * SEGMENT_NUM_CODES);
    if qmin == qmax {
        quantized_dist_table.resize(num_split_tables * SEGMENT_NUM_CODES, 0);
        return (qmin, qmax, quantization_max);
    }

    let factor = quantization_max / (qmax - qmin);
    for table_idx in 0..num_split_tables {
        for code in 0..SEGMENT_NUM_CODES {
            let value = ex_fastscan_dist_table_value(ex_query, ex_bits, table_idx, code);
            quantized_dist_table.push(((value - qmin) * factor).round() as u8);
        }
    }

    (qmin, qmax, quantization_max)
}

#[inline]
fn supports_ex_fastscan(ex_bits: u8) -> bool {
    matches!(ex_bits, 2 | 4 | 8)
}

/// The FastScan LUT value for one nibble of a blocked-layout code byte:
/// `table_idx / 2` is the byte position within a row and `table_idx % 2`
/// selects its low/high nibble (see the `ex_dot` module docs for the
/// byte-to-dim mapping per width). Dims beyond the query length (block
/// padding) contribute zero.
#[inline]
fn ex_fastscan_dist_table_value(
    ex_query: &[f32],
    ex_bits: u8,
    table_idx: usize,
    code: usize,
) -> f32 {
    let query = |dim_idx: usize| ex_query.get(dim_idx).copied().unwrap_or(0.0);
    let byte_idx = table_idx / 2;
    let high_nibble = table_idx % 2 == 1;
    match ex_bits {
        2 => {
            // byte 16g+b = dims {64g+b, +16, +32, +48} at bit pairs; the low
            // nibble covers the first two dims, the high nibble the last two.
            let dim_idx = 64 * (byte_idx / 16) + byte_idx % 16 + 32 * usize::from(high_nibble);
            let low = (code & 0b11) as f32;
            let high = ((code >> 2) & 0b11) as f32;
            query(dim_idx) * low + query(dim_idx + 16) * high
        }
        4 => {
            // byte 32g+8j+b = dim 64g+16j+b (low nibble) | dim +8 (high).
            let in_block = byte_idx % 32;
            let dim_idx = 64 * (byte_idx / 32)
                + 16 * (in_block / 8)
                + in_block % 8
                + 8 * usize::from(high_nibble);
            query(dim_idx) * code as f32
        }
        8 => {
            // byte = dim identity; the high nibble carries code bits 4..8.
            let code = if high_nibble {
                code << SEGMENT_LENGTH
            } else {
                code
            };
            query(byte_idx) * code as f32
        }
        _ => unreachable!("unsupported RabitQ ex_bits={ex_bits} for FastScan"),
    }
}

/// Transpose ex codes for the FastScan bulk path. That path is only reachable
/// when lower-bound gating is disabled, i.e. for legacy indexes without error
/// factors; gated indexes rerank per candidate with the ex-dot kernels and
/// never touch this copy, so skip the transpose (and its resident memory).
fn maybe_pack_ex_codes(
    ex_codes: Option<&FixedSizeListArray>,
    ex_bits: u8,
    error_factors: Option<&Float32Array>,
) -> Option<FixedSizeListArray> {
    let ex_codes = ex_codes?;
    if error_factors.is_some() {
        return None;
    }
    match ex_bits {
        2 | 4 | 8 => Some(pack_codes(ex_codes)),
        _ => None,
    }
}

/// Bring legacy sequential ex codes into the blocked kernel layout: rows are
/// repacked, except for the widths whose layouts agree byte-for-byte (then
/// the column is used as stored).
fn blocked_ex_codes_from_sequential(
    seq_codes: &FixedSizeListArray,
    dim: usize,
    ex_bits: u8,
) -> Result<FixedSizeListArray> {
    if sequential_matches_blocked(ex_bits)
        && seq_codes.value_length() as usize == blocked_ex_code_bytes(dim, ex_bits)
    {
        return Ok(seq_codes.clone());
    }
    let seq_code_len = seq_codes.value_length() as usize;
    let seq_values = seq_codes.values().as_primitive::<UInt8Type>().values();
    let blocked_code_len = blocked_ex_code_bytes(dim, ex_bits);
    let mut blocked_values = vec![0u8; seq_codes.len() * blocked_code_len];
    for (seq_row, blocked_row) in seq_values
        .chunks_exact(seq_code_len)
        .zip(blocked_values.chunks_exact_mut(blocked_code_len))
    {
        repack_sequential_row(seq_row, dim, ex_bits, blocked_row);
    }
    Ok(FixedSizeListArray::try_new_from_values(
        UInt8Array::from(blocked_values),
        blocked_code_len as i32,
    )?)
}

/// Load the ex-code column of an index batch into the blocked kernel layout,
/// accepting both the blocked format and the legacy sequential format. Legacy
/// batches are normalized in place (the sequential column is replaced by the
/// blocked one), so rewrites — remap, optimize merges — always emit the
/// blocked format and legacy indexes upgrade on their next rewrite.
pub(crate) fn load_blocked_ex_codes(
    batch: RecordBatch,
    rotated_dim: usize,
    num_bits: u8,
) -> Result<(RecordBatch, FixedSizeListArray)> {
    let ex_bits = rabit_ex_bits(num_bits)?;
    if let Some(column) = batch.column_by_name(RABIT_BLOCKED_EX_CODE_COLUMN) {
        let codes = column.as_fixed_size_list().clone();
        let expected_bytes = blocked_ex_code_bytes(rotated_dim, ex_bits);
        if codes.value_length() as usize != expected_bytes {
            return Err(Error::invalid_input(format!(
                "RabitQ ex-code byte width mismatch: column {} has {} bytes, metadata rotated_dim={} ex_bits={} requires {} bytes",
                RABIT_BLOCKED_EX_CODE_COLUMN,
                codes.value_length(),
                rotated_dim,
                ex_bits,
                expected_bytes
            )));
        }
        return Ok((batch, codes));
    }
    let column = batch.column_by_name(RABIT_EX_CODE_COLUMN).ok_or_else(|| {
        Error::invalid_input(format!(
            "RabitQ num_bits={} requires {} column",
            num_bits, RABIT_BLOCKED_EX_CODE_COLUMN
        ))
    })?;
    let codes = column.as_fixed_size_list().clone();
    let expected_bytes = rabit_ex_code_bytes(rotated_dim, ex_bits)?;
    if codes.value_length() as usize != expected_bytes {
        return Err(Error::invalid_input(format!(
            "RabitQ ex-code byte width mismatch: column {} has {} bytes, metadata rotated_dim={} ex_bits={} requires {} bytes",
            RABIT_EX_CODE_COLUMN,
            codes.value_length(),
            rotated_dim,
            ex_bits,
            expected_bytes
        )));
    }
    let blocked = blocked_ex_codes_from_sequential(&codes, rotated_dim, ex_bits)?;
    let ex_code_field = rabit_ex_code_field(rotated_dim, num_bits)?
        .expect("multi-bit RabitQ always has an ex-code field");
    let batch = batch
        .drop_column(RABIT_EX_CODE_COLUMN)?
        .try_with_column(ex_code_field, Arc::new(blocked.clone()))?;
    Ok((batch, blocked))
}

impl DistCalculator for RabitDistCalculator<'_> {
    #[inline(always)]
    fn distance(&self, id: u32) -> f32 {
        let id = id as usize;
        let code_len = rabit_binary_code_bytes(self.dim);
        let num_vectors = self.codes.len() / code_len;
        let dist =
            compute_single_rq_distance(self.codes, id, num_vectors, code_len, &self.dist_table);

        match self.query_estimator {
            RabitQueryEstimator::ResidualQuery => {
                // distance between quantized residual vector and residual query vector
                let dist_vq_qr = (2.0 * dist - self.sum_q) / self.sqrt_d;
                dist_vq_qr * self.scale_factors[id] + self.add_factors[id] + self.query_factor
            }
            RabitQueryEstimator::RawQuery => {
                let ex_bits = self.num_bits - 1;
                if ex_bits == 0 || self.approx_mode == ApproxMode::Fast {
                    return self.raw_query_binary_distance(id, dist);
                }

                let ex_codes = self
                    .ex_codes
                    .expect("raw-query multi-bit RQ requires ex codes");
                let ex_add_factors = self
                    .ex_add_factors
                    .expect("raw-query multi-bit RQ requires ex add factors");
                let ex_scale_factors = self
                    .ex_scale_factors
                    .expect("raw-query multi-bit RQ requires ex scale factors");
                self.raw_query_multi_bit_exact_distance(
                    id,
                    dist,
                    ex_bits,
                    ex_codes,
                    ex_add_factors,
                    ex_scale_factors,
                )
            }
        }
    }

    #[inline(always)]
    fn distance_all(&self, _: usize) -> Vec<f32> {
        let mut dists = Vec::new();
        let mut quantized_dists = Vec::new();
        let mut quantized_dists_table = Vec::new();
        let mut hacc_quantized_dists = Vec::new();
        self.distance_all_with_scratch(
            0,
            &mut dists,
            &mut quantized_dists,
            &mut quantized_dists_table,
            &mut hacc_quantized_dists,
        );
        dists
    }

    #[inline(always)]
    #[allow(clippy::uninit_vec)]
    fn distance_all_with_scratch(
        &self,
        _: usize,
        dists: &mut Vec<f32>,
        quantized_dists: &mut Vec<u16>,
        quantized_dists_table: &mut Vec<u8>,
        hacc_quantized_dists: &mut Vec<u32>,
    ) {
        let code_len = rabit_binary_code_bytes(self.dim);
        let n = self.codes.len() / code_len;
        if n == 0 {
            dists.clear();
            quantized_dists.clear();
            return;
        }

        if self.query_estimator == RabitQueryEstimator::ResidualQuery
            || self.num_bits == 1
            || self.approx_mode == ApproxMode::Fast
        {
            self.one_bit_distances_with_scratch(
                n,
                code_len,
                dists,
                quantized_dists,
                quantized_dists_table,
                hacc_quantized_dists,
            );
            return;
        }

        let simd_len = self.binary_distances_with_scratch(
            n,
            code_len,
            dists,
            quantized_dists,
            quantized_dists_table,
            hacc_quantized_dists,
        );

        self.apply_raw_query_multi_bit_distances(
            simd_len,
            dists,
            quantized_dists,
            quantized_dists_table,
        );
    }

    #[allow(clippy::too_many_arguments)]
    fn accumulate_topk_with_scratch(
        &self,
        k: usize,
        lower_bound: Option<f32>,
        upper_bound: Option<f32>,
        row_id: impl Fn(u32) -> u64,
        res: &mut BinaryHeap<OrderedNode<u64>>,
        dists: &mut Vec<f32>,
        quantized_dists: &mut Vec<u16>,
        quantized_dists_table: &mut Vec<u8>,
        hacc_quantized_dists: &mut Vec<u32>,
    ) {
        if k == 0 {
            return;
        }
        if let Some(reason) = self.raw_query_lower_bound_gating_disabled_reason() {
            record_rabit_prune_bypass(reason);
            self.distance_all_with_scratch(
                k,
                dists,
                quantized_dists,
                quantized_dists_table,
                hacc_quantized_dists,
            );
            accumulate_distances_into_heap(k, lower_bound, upper_bound, row_id, res, dists);
            return;
        }

        self.accumulate_raw_query_multi_bit_topk_dense_with_scratch(
            k,
            lower_bound,
            upper_bound,
            row_id,
            res,
            dists,
            quantized_dists,
            quantized_dists_table,
            hacc_quantized_dists,
        );
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
        dists: &mut Vec<f32>,
        quantized_dists: &mut Vec<u16>,
        quantized_dists_table: &mut Vec<u8>,
        hacc_quantized_dists: &mut Vec<u32>,
    ) {
        if k == 0 {
            return;
        }
        if let Some(reason) = self.raw_query_lower_bound_gating_disabled_reason() {
            record_rabit_prune_bypass(reason);
            self.distance_all_with_scratch(
                k,
                dists,
                quantized_dists,
                quantized_dists_table,
                hacc_quantized_dists,
            );
            accumulate_filtered_distances_into_heap(
                k,
                lower_bound,
                upper_bound,
                row_ids,
                accept_row,
                res,
                dists,
            );
            return;
        }

        self.accumulate_raw_query_multi_bit_topk_with_scratch(
            k,
            lower_bound,
            upper_bound,
            row_ids
                .filter(|(_, row_id)| accept_row(*row_id))
                .map(|(id, row_id)| (id as usize, row_id)),
            res,
            dists,
            quantized_dists,
            quantized_dists_table,
            hacc_quantized_dists,
        );
    }
}

fn accumulate_distances_into_heap(
    k: usize,
    lower_bound: Option<f32>,
    upper_bound: Option<f32>,
    row_id: impl Fn(u32) -> u64,
    res: &mut BinaryHeap<OrderedNode<u64>>,
    dists: &[f32],
) {
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

fn accumulate_filtered_distances_into_heap(
    k: usize,
    lower_bound: Option<f32>,
    upper_bound: Option<f32>,
    row_ids: impl Iterator<Item = (u32, u64)>,
    accept_row: impl Fn(u64) -> bool,
    res: &mut BinaryHeap<OrderedNode<u64>>,
    dists: &[f32],
) {
    let lower_bound = lower_bound.unwrap_or(f32::MIN).into();
    let upper_bound = upper_bound.unwrap_or(f32::MAX).into();
    let mut max_dist = res.peek().map(|node| node.dist);
    for (id, row_id) in row_ids {
        if !accept_row(row_id) {
            continue;
        }
        let Some(dist) = dists.get(id as usize).copied() else {
            continue;
        };
        let dist = OrderedFloat(dist);
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

impl VectorStore for RabitQuantizationStorage {
    type DistanceCalculator<'a> = RabitDistCalculator<'a>;

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn schema(&self) -> &SchemaRef {
        self.batch.schema_ref()
    }

    fn to_batches(&self) -> Result<impl Iterator<Item = RecordBatch> + Send> {
        Ok(std::iter::once(self.batch.clone()))
    }

    fn append_batch(&self, _batch: RecordBatch, _vector_column: &str) -> Result<Self> {
        unimplemented!("RabitQ does not support append_batch")
    }

    fn len(&self) -> usize {
        self.batch.num_rows()
    }

    fn row_id(&self, id: u32) -> u64 {
        self.row_ids.value(id as usize)
    }

    fn row_ids(&self) -> impl Iterator<Item = &u64> {
        self.row_ids.values().iter()
    }

    fn distance_type(&self) -> DistanceType {
        self.distance_type
    }

    // qr = (q-c)
    #[inline(never)]
    fn dist_calculator(&self, qr: Arc<dyn Array>, dist_q_c: f32) -> Self::DistanceCalculator<'_> {
        let code_dim = self.code_dim();
        let rotated_qr = self.rotate_query_vector(code_dim, &qr);
        let dist_table = build_dist_table_direct::<Float32Type>(&rotated_qr);
        let query_factor = match self.metadata.query_estimator {
            RabitQueryEstimator::ResidualQuery => self.residual_query_factor(dist_q_c),
            RabitQueryEstimator::RawQuery => self.raw_query_factor(dist_q_c, &rotated_qr, None),
        };
        let query_error = match self.metadata.query_estimator {
            RabitQueryEstimator::ResidualQuery => 0.0,
            RabitQueryEstimator::RawQuery => {
                self.raw_query_error_for_gating(dist_q_c, &rotated_qr, None)
            }
        };
        let sum_q = rotated_qr.iter().copied().sum();
        // The kernels read the rotated query directly; only unaligned dims
        // need a zero-padded copy.
        let ex_query = if code_dim.is_multiple_of(EX_DOT_BLOCK_DIMS) {
            rotated_qr
        } else {
            let mut padded = vec![0.0; padded_query_len(code_dim)];
            pad_query_into(&rotated_qr, &mut padded);
            padded
        };

        self.distance_calculator_from_parts(RabitDistCalculatorParts {
            dim: code_dim,
            dist_table: Cow::Owned(dist_table),
            ex_query: Cow::Owned(ex_query),
            sum_q,
            query_factor,
            query_error,
            approx_mode: ApproxMode::Normal,
        })
    }

    // qr = (q-c)
    #[inline(never)]
    fn dist_calculator_with_scratch<'a>(
        &'a self,
        qr: Arc<dyn Array>,
        dist_q_c: f32,
        residual: Option<QueryResidual<'a>>,
        f32_scratch: &'a mut Vec<f32>,
        options: DistanceCalculatorOptions,
    ) -> Self::DistanceCalculator<'a> {
        let code_dim = self.code_dim();
        if let (
            RabitQueryEstimator::RawQuery,
            Some(QueryResidual::RabitRawQuery {
                rotated_centroid,
                query: Some(raw_query),
            }),
        ) = (self.metadata.query_estimator, residual)
        {
            debug_assert_eq!(raw_query.code_dim, code_dim);
            debug_assert_eq!(raw_query.ex_bits, self.metadata.num_bits - 1);
            let query_factor =
                self.raw_query_factor(dist_q_c, &raw_query.rotated_query, rotated_centroid);
            let query_error = self.raw_query_error_for_gating(
                dist_q_c,
                &raw_query.rotated_query,
                rotated_centroid,
            );
            return self.distance_calculator_from_parts(RabitDistCalculatorParts {
                dim: code_dim,
                dist_table: Cow::Borrowed(&raw_query.dist_table),
                ex_query: Cow::Borrowed(kernel_query(
                    &raw_query.rotated_query,
                    &raw_query.ex_query,
                )),
                sum_q: raw_query.sum_q,
                query_factor,
                query_error,
                approx_mode: options.approx_mode,
            });
        }

        let dist_table_len = code_dim * 4;
        let ex_bits = self.metadata.num_bits - 1;
        // The kernels read the rotated query in place; a zero-padded copy is
        // only needed when the rotated dim is not block-aligned.
        let ex_query_table_len = if ex_bits == 0 || code_dim.is_multiple_of(EX_DOT_BLOCK_DIMS) {
            0
        } else {
            padded_query_len(code_dim)
        };
        f32_scratch.resize(code_dim + dist_table_len + ex_query_table_len, 0.0);

        let query_factor;
        let query_error;
        let sum_q = {
            let (rotated_qr, remaining) = f32_scratch.split_at_mut(code_dim);
            let (dist_table, ex_query) = remaining.split_at_mut(dist_table_len);
            match residual {
                Some(QueryResidual::Centroid(residual_centroid)) => {
                    self.rotate_query_vector_into(
                        code_dim,
                        &qr,
                        Some(residual_centroid),
                        rotated_qr,
                    );
                }
                Some(QueryResidual::RabitRawQuery { .. }) | None => {
                    self.rotate_query_vector_into(code_dim, &qr, None, rotated_qr);
                }
            }
            query_factor = match (self.metadata.query_estimator, residual) {
                (RabitQueryEstimator::ResidualQuery, _) => self.residual_query_factor(dist_q_c),
                (
                    RabitQueryEstimator::RawQuery,
                    Some(QueryResidual::RabitRawQuery {
                        rotated_centroid, ..
                    }),
                ) => self.raw_query_factor(dist_q_c, rotated_qr, rotated_centroid),
                (RabitQueryEstimator::RawQuery, _) => {
                    self.raw_query_factor(dist_q_c, rotated_qr, None)
                }
            };
            query_error = match (self.metadata.query_estimator, residual) {
                (RabitQueryEstimator::ResidualQuery, _) => 0.0,
                (
                    RabitQueryEstimator::RawQuery,
                    Some(QueryResidual::RabitRawQuery {
                        rotated_centroid, ..
                    }),
                ) => self.raw_query_error_for_gating(dist_q_c, rotated_qr, rotated_centroid),
                (RabitQueryEstimator::RawQuery, _) => {
                    self.raw_query_error_for_gating(dist_q_c, rotated_qr, None)
                }
            };
            build_dist_table_direct_into::<Float32Type>(rotated_qr, dist_table);
            if ex_query_table_len > 0 {
                pad_query_into(rotated_qr, ex_query);
            }
            rotated_qr.iter().copied().sum()
        };

        let ex_query_start = code_dim + dist_table_len;
        self.distance_calculator_from_parts(RabitDistCalculatorParts {
            dim: code_dim,
            dist_table: Cow::Borrowed(&f32_scratch[code_dim..ex_query_start]),
            ex_query: Cow::Borrowed(kernel_query(
                &f32_scratch[..code_dim],
                &f32_scratch[ex_query_start..ex_query_start + ex_query_table_len],
            )),
            sum_q,
            query_factor,
            query_error,
            approx_mode: options.approx_mode,
        })
    }

    // TODO: implement this
    // This method is required for HNSW, we can't support HNSW_RABIT before this is implemented
    fn dist_calculator_from_id(&self, _: u32) -> Self::DistanceCalculator<'_> {
        unimplemented!("RabitQ does not support dist_calculator_from_id")
    }
}

const LOWBIT_IDX: [usize; 16] = {
    let mut array = [0; 16];
    let mut i = 1;
    while i < 16 {
        array[i] = i.trailing_zeros() as usize;
        i += 1;
    }
    array
};

fn get_column(
    quantization_code: &[u8],
    code_len: usize,
    row: usize,
    col_idx: usize,
    codes: &mut [u8; 32],
) {
    for (i, code) in codes.iter_mut().enumerate() {
        let vec_idx = row + i;
        *code = quantization_code[vec_idx * code_len + col_idx];
    }
}

pub fn pack_codes(codes: &FixedSizeListArray) -> FixedSizeListArray {
    let code_len = codes.value_length() as usize;

    // round up num of vectors to multiple of batch size (32)
    let num_blocks = codes.len() / BATCH_SIZE;
    let num_packed_vectors = num_blocks * BATCH_SIZE;

    // calculate total size for packed blocks
    // we pack each 32 vectors into a block, each block contains 2 codes (1byte) of each vector
    // so every 32 vectors would produce code_len blocks
    // the low 16 bytes of each block is the codes for the low 4 bits of each vector
    // the high 16 bytes of each block is the codes for the high 4 bits of each vector
    let mut blocks = vec![0u8; codes.values().len()];

    let codes_values = codes
        .slice(0, num_packed_vectors)
        .values()
        .as_primitive::<UInt8Type>()
        .clone();
    let codes_values = codes_values.values();

    // Pack codes batch by batch
    // Each batch contains codes for 32 vectors
    let mut col = [0u8; 32];
    let mut col_0 = [0u8; 32]; // lower 4 bits
    let mut col_1 = [0u8; 32]; // higher 4 bits
    for row in (0..num_packed_vectors).step_by(BATCH_SIZE) {
        // Get quantization codes for each column for each batch
        // i.e., we get the codes for 8 dims of 32 vectors and reorganize the data layout
        // based on the shuffle SIMD instruction used during querying
        for i in 0..code_len {
            get_column(codes_values, code_len, row, i, &mut col);

            for j in 0..32 {
                col_0[j] = col[j] & 0xF;
                col_1[j] = col[j] >> 4;
            }

            let block_offset = (row / BATCH_SIZE) * code_len * BATCH_SIZE + i * BATCH_SIZE;
            for j in 0..16 {
                // The lower 4 bits represent vector 0 to 15
                // The upper 4 bits represent vector 16 to 31
                let val0 = col_0[PERM0[j]] | (col_0[PERM0[j] + 16] << 4);
                let val1 = col_1[PERM0[j]] | (col_1[PERM0[j] + 16] << 4);
                blocks[block_offset + j] = val0;
                blocks[block_offset + j + 16] = val1;
            }
        }
    }

    // for the left codes, transpose them for better cache locality
    let transposed_codes = transpose(
        &codes.values().as_primitive::<UInt8Type>().slice(
            num_packed_vectors * code_len,
            (codes.len() - num_packed_vectors) * code_len,
        ),
        codes.len() - num_packed_vectors,
        code_len,
    );

    let offset = codes.values().len() - transposed_codes.len();
    for (i, v) in transposed_codes.values().iter().enumerate() {
        blocks[offset + i] = *v;
    }

    assert_eq!(blocks.len(), codes.values().len());
    FixedSizeListArray::try_new_from_values(UInt8Array::from(blocks), code_len as i32).unwrap()
}

// Inverse of pack_codes
pub fn unpack_codes(codes: &FixedSizeListArray) -> FixedSizeListArray {
    let code_len = codes.value_length() as usize;
    let num_vectors = codes.len();

    // Calculate number of complete batches
    let num_blocks = num_vectors / BATCH_SIZE;
    let num_packed_vectors = num_blocks * BATCH_SIZE;

    let mut unpacked = vec![0u8; codes.values().len()];

    let codes_values = codes.values().as_primitive::<UInt8Type>().values();

    // Unpack complete batches
    for batch_idx in 0..num_blocks {
        let block_start = batch_idx * code_len * BATCH_SIZE;

        for i in 0..code_len {
            let block_offset = block_start + i * BATCH_SIZE;
            let block = &codes_values[block_offset..block_offset + BATCH_SIZE];

            // Reverse the permutation
            for j in 0..16 {
                let val0 = block[j];
                let val1 = block[j + 16];

                let low_0 = val0 & 0xF;
                let high_0 = val0 >> 4;
                let low_1 = val1 & 0xF;
                let high_1 = val1 >> 4;

                let vec_idx_0 = batch_idx * BATCH_SIZE + PERM0[j];
                let vec_idx_1 = batch_idx * BATCH_SIZE + PERM0[j] + 16;

                unpacked[vec_idx_0 * code_len + i] = low_0 | (low_1 << 4);
                unpacked[vec_idx_1 * code_len + i] = high_0 | (high_1 << 4);
            }
        }
    }

    // Transpose back the remainder
    if num_packed_vectors < num_vectors {
        let remainder = num_vectors - num_packed_vectors;
        let offset = num_packed_vectors * code_len;
        let transposed_data = &codes_values[offset..];

        // Transpose from column-major back to row-major
        for row in 0..remainder {
            for col in 0..code_len {
                unpacked[offset + row * code_len + col] = transposed_data[col * remainder + row];
            }
        }
    }

    FixedSizeListArray::try_new_from_values(UInt8Array::from(unpacked), code_len as i32).unwrap()
}

/// Build a row-id remapping for the rows present in this partition from a
/// fragment-reuse index, mirroring the PQ storage frag-reuse path.
///
/// Returns `None` when there is nothing to do (no fragment-reuse index, or the
/// index leaves every present row id unchanged), so callers keep the zero-cost
/// no-op path. Otherwise, returns a `HashMap` mapping every affected old row id
/// to `Some(new_id)` for surviving rows or `None` for rows whose covering
/// fragment was compacted away, suitable for `RabitQuantizationStorage::remap`.
fn build_frag_reuse_mapping(
    fri: Option<&FragReuseIndex>,
    row_ids: &UInt64Array,
) -> Option<HashMap<u64, Option<u64>>> {
    let fri = fri?;
    if fri.row_id_maps.is_empty() {
        return None;
    }
    let mut mapping: HashMap<u64, Option<u64>> = HashMap::new();
    for row_id in row_ids.values().iter() {
        match fri.remap_row_id(*row_id) {
            Some(new_id) if new_id == *row_id => {}
            mapped => {
                mapping.insert(*row_id, mapped);
            }
        }
    }
    if mapping.is_empty() {
        None
    } else {
        Some(mapping)
    }
}

#[async_trait]
impl QuantizerStorage for RabitQuantizationStorage {
    type Metadata = RabitQuantizationMetadata;

    fn try_from_batch(
        batch: RecordBatch,
        metadata: &Self::Metadata,
        distance_type: DistanceType,
        fri: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self> {
        let distance_type = match (metadata.query_estimator, distance_type) {
            (RabitQueryEstimator::RawQuery, DistanceType::Cosine) => DistanceType::L2,
            _ => distance_type,
        };
        validate_rq_num_bits(metadata.num_bits)?;
        let row_ids = batch[ROW_ID].as_primitive::<UInt64Type>().clone();
        let codes = batch[RABIT_CODE_COLUMN].as_fixed_size_list().clone();
        let expected_code_bytes = metadata.binary_code_bytes();
        if expected_code_bytes > 0 && codes.value_length() as usize != expected_code_bytes {
            return Err(Error::invalid_input(format!(
                "RabitQ code byte width mismatch: column {} has {} bytes, metadata rotated_dim={} requires {} bytes",
                RABIT_CODE_COLUMN,
                codes.value_length(),
                metadata.rotated_dim(),
                expected_code_bytes
            )));
        }
        let add_factors = batch[ADD_FACTORS_COLUMN]
            .as_primitive::<Float32Type>()
            .clone();
        let scale_factors = batch[SCALE_FACTORS_COLUMN]
            .as_primitive::<Float32Type>()
            .clone();
        let error_factors = batch
            .column_by_name(ERROR_FACTORS_COLUMN)
            .map(|factors| factors.as_primitive::<Float32Type>().clone());
        let ex_bits = rabit_ex_bits(metadata.num_bits)?;
        let mut batch = batch;
        let mut ex_codes = None;
        let mut ex_add_factors = None;
        let mut ex_scale_factors = None;
        if ex_bits != 0 {
            let (normalized_batch, codes) =
                load_blocked_ex_codes(batch, metadata.rotated_dim(), metadata.num_bits)?;
            batch = normalized_batch;
            ex_codes = Some(codes);
            ex_add_factors = Some(
                batch
                    .column_by_name(EX_ADD_FACTORS_COLUMN)
                    .ok_or_else(|| {
                        Error::invalid_input(format!(
                            "RabitQ num_bits={} requires {} column",
                            metadata.num_bits, EX_ADD_FACTORS_COLUMN
                        ))
                    })?
                    .as_primitive::<Float32Type>()
                    .clone(),
            );
            ex_scale_factors = Some(
                batch
                    .column_by_name(EX_SCALE_FACTORS_COLUMN)
                    .ok_or_else(|| {
                        Error::invalid_input(format!(
                            "RabitQ num_bits={} requires {} column",
                            metadata.num_bits, EX_SCALE_FACTORS_COLUMN
                        ))
                    })?
                    .as_primitive::<Float32Type>()
                    .clone(),
            );
        } else if metadata.query_estimator == RabitQueryEstimator::RawQuery {
            if batch.column_by_name(EX_ADD_FACTORS_COLUMN).is_some()
                || batch.column_by_name(EX_SCALE_FACTORS_COLUMN).is_some()
                || batch.column_by_name(RABIT_EX_CODE_COLUMN).is_some()
                || batch.column_by_name(RABIT_BLOCKED_EX_CODE_COLUMN).is_some()
            {
                return Err(Error::invalid_input(
                    "RabitQ num_bits=1 raw-query indexes must not contain ex-code columns"
                        .to_string(),
                ));
            }
        } else if batch.column_by_name(RABIT_EX_CODE_COLUMN).is_some()
            || batch.column_by_name(RABIT_BLOCKED_EX_CODE_COLUMN).is_some()
        {
            return Err(Error::invalid_input(format!(
                "RabitQ num_bits={} does not support ex-code columns",
                metadata.num_bits
            )));
        }

        let (batch, codes) = if !metadata.packed {
            let codes = pack_codes(&codes);
            let batch = batch.replace_column_by_name(RABIT_CODE_COLUMN, Arc::new(codes))?;
            let codes = batch[RABIT_CODE_COLUMN].as_fixed_size_list().clone();
            (batch, codes)
        } else {
            (batch, codes)
        };

        let mut metadata = metadata.clone();
        metadata.packed = true;
        let packed_ex_codes =
            maybe_pack_ex_codes(ex_codes.as_ref(), ex_bits, error_factors.as_ref());

        let storage = Self {
            metadata,
            batch,
            distance_type,
            row_ids,
            codes,
            add_factors,
            scale_factors,
            error_factors,
            ex_codes,
            packed_ex_codes,
            ex_add_factors,
            ex_scale_factors,
        };

        match build_frag_reuse_mapping(fri.as_deref(), &storage.row_ids) {
            Some(mapping) => storage.remap(&RowAddrRemap::direct(mapping)),
            None => Ok(storage),
        }
    }

    fn metadata(&self) -> &Self::Metadata {
        &self.metadata
    }

    async fn load_partition(
        reader: &V1FileReader,
        range: std::ops::Range<usize>,
        distance_type: DistanceType,
        metadata: &Self::Metadata,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self> {
        let schema = reader.schema();
        let batch = reader.read_range(range, schema).await?;
        Self::try_from_batch(batch, metadata, distance_type, frag_reuse_index)
    }

    fn remap(&self, mapping: &RowAddrRemap) -> Result<Self> {
        let num_vectors = self.codes.len();
        let num_code_bytes = self.codes.value_length() as usize;
        let codes = self.codes.values().as_primitive::<UInt8Type>().values();
        let mut indices = Vec::with_capacity(num_vectors);
        let mut new_row_ids = Vec::with_capacity(num_vectors);
        let mut new_codes = Vec::with_capacity(codes.len());

        let row_ids = self.row_ids.values();
        for (i, row_id) in row_ids.iter().enumerate() {
            match mapping.get(*row_id) {
                Some(Some(new_id)) => {
                    indices.push(i as u32);
                    new_row_ids.push(new_id);
                    new_codes.extend(get_rq_code(codes, i, num_vectors, num_code_bytes));
                }
                Some(None) => {}
                None => {
                    indices.push(i as u32);
                    new_row_ids.push(*row_id);
                    new_codes.extend(get_rq_code(codes, i, num_vectors, num_code_bytes));
                }
            }
        }

        let new_row_ids = UInt64Array::from(new_row_ids);
        let new_codes = FixedSizeListArray::try_new_from_values(
            UInt8Array::from(new_codes),
            num_code_bytes as i32,
        )?;
        let batch = if new_row_ids.is_empty() {
            RecordBatch::new_empty(self.schema().clone())
        } else {
            let codes = Arc::new(pack_codes(&new_codes));
            self.batch
                .take(&UInt32Array::from(indices))?
                .replace_column_by_name(ROW_ID, Arc::new(new_row_ids.clone()))?
                .replace_column_by_name(RABIT_CODE_COLUMN, codes)?
        };
        let codes = batch[RABIT_CODE_COLUMN].as_fixed_size_list().clone();
        let add_factors = batch[ADD_FACTORS_COLUMN]
            .as_primitive::<Float32Type>()
            .clone();
        let scale_factors = batch[SCALE_FACTORS_COLUMN]
            .as_primitive::<Float32Type>()
            .clone();
        let error_factors = batch
            .column_by_name(ERROR_FACTORS_COLUMN)
            .map(|factors| factors.as_primitive::<Float32Type>().clone());
        let ex_bits = rabit_ex_bits(self.metadata.num_bits)?;
        let (batch, ex_codes) = if ex_bits == 0 {
            (batch, None)
        } else {
            // `self.batch` is already normalized at load, so this is a
            // zero-copy column lookup.
            let (batch, codes) =
                load_blocked_ex_codes(batch, self.metadata.rotated_dim(), self.metadata.num_bits)?;
            (batch, Some(codes))
        };
        let packed_ex_codes =
            maybe_pack_ex_codes(ex_codes.as_ref(), ex_bits, error_factors.as_ref());
        let ex_add_factors = batch
            .column_by_name(EX_ADD_FACTORS_COLUMN)
            .map(|factors| factors.as_primitive::<Float32Type>().clone());
        let ex_scale_factors = batch
            .column_by_name(EX_SCALE_FACTORS_COLUMN)
            .map(|factors| factors.as_primitive::<Float32Type>().clone());

        Ok(Self {
            metadata: self.metadata.clone(),
            distance_type: self.distance_type,
            batch,
            codes,
            add_factors,
            scale_factors,
            error_factors,
            ex_codes,
            packed_ex_codes,
            ex_add_factors,
            ex_scale_factors,
            row_ids: new_row_ids,
        })
    }
}

/// Compute the raw distance for a single vector without allocating.
///
/// Fuses code extraction from the packed layout with distance accumulation
/// in a single pass, avoiding the intermediate `Vec` allocation that
/// `get_rq_code` + iterator would require.
#[inline]
fn compute_single_rq_distance(
    codes: &[u8],
    id: usize,
    num_vectors: usize,
    num_code_bytes: usize,
    dist_table: &[f32],
) -> f32 {
    let remainder = num_vectors % BATCH_SIZE;
    let mut dist_table_iter = dist_table.chunks_exact(SEGMENT_NUM_CODES).tuples();

    if id < num_vectors - remainder {
        let batch_codes = &codes[id / BATCH_SIZE * BATCH_SIZE * num_code_bytes
            ..(id / BATCH_SIZE + 1) * BATCH_SIZE * num_code_bytes];

        let id_in_batch = id % BATCH_SIZE;
        let idx = PERM0_INVERSE[id_in_batch % 16];
        let is_lower = id_in_batch < 16;

        let mut dist = 0.0f32;
        for block in batch_codes.chunks_exact(BATCH_SIZE) {
            let code_byte = if is_lower {
                (block[idx] & 0xF) | (block[idx + 16] << 4)
            } else {
                (block[idx] >> 4) | (block[idx + 16] & 0xF0)
            };
            if let Some((current_dt, next_dt)) = dist_table_iter.next() {
                let current_code = (code_byte & 0x0F) as usize;
                let next_code = (code_byte >> 4) as usize;
                dist += current_dt[current_code] + next_dt[next_code];
            }
        }
        dist
    } else {
        let offset_id = id - (num_vectors - remainder);
        let remainder_codes = &codes[(num_vectors - remainder) * num_code_bytes..];

        let mut dist = 0.0f32;
        for &code_byte in remainder_codes.iter().skip(offset_id).step_by(remainder) {
            if let Some((current_dt, next_dt)) = dist_table_iter.next() {
                let current_code = (code_byte & 0x0F) as usize;
                let next_code = (code_byte >> 4) as usize;
                dist += current_dt[current_code] + next_dt[next_code];
            }
        }
        dist
    }
}

#[inline]
fn get_rq_code(
    codes: &[u8],
    id: usize,
    num_vectors: usize,
    num_code_bytes: usize,
) -> impl Iterator<Item = u8> + '_ {
    let remainder = num_vectors % BATCH_SIZE;

    if id < num_vectors - remainder {
        // the codes are packed
        let codes = &codes[id / BATCH_SIZE * BATCH_SIZE * num_code_bytes
            ..(id / BATCH_SIZE + 1) * BATCH_SIZE * num_code_bytes];

        let id_in_batch = id % BATCH_SIZE;
        if id_in_batch < 16 {
            let idx = PERM0_INVERSE[id_in_batch];
            codes
                .chunks_exact(BATCH_SIZE)
                .map(|block| (block[idx] & 0xF) | (block[idx + 16] << 4))
                .exact_size(num_code_bytes)
                .collect_vec()
                .into_iter()
        } else {
            let idx = PERM0_INVERSE[id_in_batch - 16];
            codes
                .chunks_exact(BATCH_SIZE)
                .map(|block| (block[idx] >> 4) | (block[idx + 16] & 0xF0))
                .exact_size(num_code_bytes)
                .collect_vec()
                .into_iter()
        }
    } else {
        let id = id - (num_vectors - remainder);
        let codes = &codes[(num_vectors - remainder) * num_code_bytes..];
        codes
            .iter()
            .skip(id)
            .step_by(remainder)
            .copied()
            .exact_size(num_code_bytes)
            .collect_vec()
            .into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::collections::{BinaryHeap, HashMap};

    use arrow_array::{ArrayRef, Float32Array, Float64Array, UInt64Array};
    use lance_core::ROW_ID;
    use lance_linalg::distance::DistanceType;
    use rand::rngs::SmallRng;
    use rand::{Rng, SeedableRng};

    use crate::vector::bq::{RQRotationType, builder::RabitQuantizer};
    use crate::vector::quantizer::{Quantization, QuantizerStorage};

    fn build_dist_table_not_optimized<T: ArrowFloatType>(
        sub_vec: &[T::Native],
        dist_table: &mut [f32],
    ) where
        T::Native: AsPrimitive<f32>,
    {
        for (j, dist) in dist_table.iter_mut().enumerate().take(SEGMENT_NUM_CODES) {
            for (k, v) in sub_vec.iter().enumerate().take(SEGMENT_LENGTH) {
                if j & (1 << k) != 0 {
                    *dist += v.as_();
                }
            }
        }
    }

    #[test]
    fn test_build_dist_table_not_optimized() {
        let sub_vec = vec![1.0, 2.0, 3.0, 4.0];
        let mut expected = vec![0.0; SEGMENT_NUM_CODES];
        build_dist_table_not_optimized::<Float32Type>(&sub_vec, &mut expected);
        let mut dist_table = vec![0.0; SEGMENT_NUM_CODES];
        build_dist_table_for_subvec::<Float32Type>(&sub_vec, &mut dist_table);
        assert_eq!(dist_table, expected);
    }

    #[test]
    fn test_dist_calculator_with_scratch_matches_owned_and_reuses_buffer() {
        let code_dim = 64;
        let original_codes = make_test_codes(50, code_dim);
        let metadata = make_test_metadata(original_codes.value_length() as usize * 8);
        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch(original_codes),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();
        let query = Arc::new(Float32Array::from_iter_values(
            (0..code_dim).map(|idx| idx as f32 / code_dim as f32),
        )) as ArrayRef;

        let expected = storage.dist_calculator(query.clone(), 0.25).distance_all(0);
        let expected_scratch_len = code_dim as usize + code_dim as usize * 4;
        let mut scratch = Vec::with_capacity(expected_scratch_len);
        let initial_ptr = scratch.as_ptr();
        {
            let calc = storage.dist_calculator_with_scratch(
                query.clone(),
                0.25,
                None,
                &mut scratch,
                DistanceCalculatorOptions::default(),
            );
            assert_eq!(calc.distance_all(0), expected);
        }
        assert_eq!(scratch.len(), expected_scratch_len);
        assert_eq!(scratch.as_ptr(), initial_ptr);

        scratch.fill(f32::NAN);
        {
            let calc = storage.dist_calculator_with_scratch(
                query,
                0.25,
                None,
                &mut scratch,
                DistanceCalculatorOptions::default(),
            );
            assert_eq!(calc.distance_all(0), expected);
        }
        assert_eq!(scratch.as_ptr(), initial_ptr);
    }

    #[test]
    fn test_dist_calculator_with_scratch_applies_residual_centroid_without_residual_array() {
        let code_dim = 64usize;
        let original_codes = make_test_codes(50, code_dim as i32);
        let mut metadata = make_test_metadata(original_codes.value_length() as usize * 8);
        metadata.query_estimator = RabitQueryEstimator::ResidualQuery;
        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch(original_codes),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();
        let query_values = (0..code_dim)
            .map(|idx| idx as f32 / code_dim as f32)
            .collect::<Vec<_>>();
        let centroid_values = (0..code_dim)
            .map(|idx| (idx % 7) as f32 / code_dim as f32)
            .collect::<Vec<_>>();
        let residual_values = query_values
            .iter()
            .zip(centroid_values.iter())
            .map(|(query, centroid)| query - centroid)
            .collect::<Vec<_>>();
        let query = Arc::new(Float32Array::from(query_values)) as ArrayRef;
        let centroid = Arc::new(Float32Array::from(centroid_values)) as ArrayRef;
        let residual = Arc::new(Float32Array::from(residual_values)) as ArrayRef;

        let expected = storage.dist_calculator(residual, 0.25).distance_all(0);
        let mut scratch = Vec::new();
        let calc = storage.dist_calculator_with_scratch(
            query.clone(),
            0.25,
            Some(QueryResidual::Centroid(centroid.as_ref())),
            &mut scratch,
            DistanceCalculatorOptions::default(),
        );

        assert_eq!(calc.distance_all(0), expected);
    }

    #[test]
    fn test_dist_calculator_with_scratch_applies_float64_residual_before_f32_cast() {
        let code_dim = 64usize;
        let original_codes = make_test_codes(50, code_dim as i32);
        let mut metadata = make_test_metadata(original_codes.value_length() as usize * 8);
        metadata.query_estimator = RabitQueryEstimator::ResidualQuery;
        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch(original_codes),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();
        let query_values = (0..code_dim)
            .map(|idx| 1.0 + idx as f64 * 1.0e-9)
            .collect::<Vec<_>>();
        let centroid_values = vec![1.0; code_dim];
        let residual_values = query_values
            .iter()
            .zip(centroid_values.iter())
            .map(|(query, centroid)| query - centroid)
            .collect::<Vec<_>>();
        let query = Arc::new(Float64Array::from(query_values)) as ArrayRef;
        let centroid = Arc::new(Float64Array::from(centroid_values)) as ArrayRef;
        let residual = Arc::new(Float64Array::from(residual_values)) as ArrayRef;

        let expected = storage.dist_calculator(residual, 0.25).distance_all(0);
        let mut scratch = Vec::new();
        let calc = storage.dist_calculator_with_scratch(
            query,
            0.25,
            Some(QueryResidual::Centroid(centroid.as_ref())),
            &mut scratch,
            DistanceCalculatorOptions::default(),
        );

        assert_eq!(calc.distance_all(0), expected);
    }

    #[test]
    fn test_pack_unpack_codes() {
        // Test with multiple batch sizes to cover both packed and transposed sections
        for num_vectors in [10, 32, 50, 64, 100] {
            let code_len = 8;

            // Create test data with known pattern
            let mut codes_data = Vec::new();
            for i in 0..num_vectors {
                for j in 0..code_len {
                    codes_data.push((i * code_len + j) as u8);
                }
            }

            let original_codes = FixedSizeListArray::try_new_from_values(
                UInt8Array::from(codes_data.clone()),
                code_len,
            )
            .unwrap();

            // Pack and then unpack
            let packed = pack_codes(&original_codes);
            let unpacked = unpack_codes(&packed);

            // Verify they match
            assert_eq!(original_codes.len(), unpacked.len());
            assert_eq!(original_codes.value_length(), unpacked.value_length());

            let original_values = original_codes.values().as_primitive::<UInt8Type>().values();
            let unpacked_values = unpacked.values().as_primitive::<UInt8Type>().values();

            assert_eq!(
                original_values, unpacked_values,
                "Mismatch for num_vectors={}",
                num_vectors
            );
        }
    }

    #[test]
    fn test_rabit_split_code_fields() {
        let bin_field = rabit_binary_code_field(128);
        let DataType::FixedSizeList(_, bin_code_bytes) = bin_field.data_type() else {
            panic!("binary code field should be FixedSizeList");
        };
        assert_eq!(*bin_code_bytes, 16);

        assert!(rabit_ex_code_field(128, 1).unwrap().is_none());
        let ex_field = rabit_ex_code_field(128, 9).unwrap().unwrap();
        assert_eq!(ex_field.name(), RABIT_BLOCKED_EX_CODE_COLUMN);
        let DataType::FixedSizeList(_, ex_code_bytes) = ex_field.data_type() else {
            panic!("ex-code field should be FixedSizeList");
        };
        assert_eq!(*ex_code_bytes, 128);
    }

    fn make_test_codes(num_vectors: usize, code_dim: i32) -> FixedSizeListArray {
        let quantizer =
            RabitQuantizer::new_with_rotation::<Float32Type>(1, code_dim, RQRotationType::Fast);
        let values = Float32Array::from_iter_values(
            (0..num_vectors * code_dim as usize).map(|idx| idx as f32 / code_dim as f32),
        );
        let vectors = FixedSizeListArray::try_new_from_values(values, code_dim).unwrap();
        quantizer
            .quantize(&vectors)
            .unwrap()
            .as_fixed_size_list()
            .clone()
    }

    fn make_test_metadata(code_dim: usize) -> RabitQuantizationMetadata {
        RabitQuantizer::new_with_rotation::<Float32Type>(1, code_dim as i32, RQRotationType::Fast)
            .metadata(None)
    }

    #[test]
    fn test_rabit_metadata_defaults_old_indexes_to_residual_query() {
        let metadata: RabitQuantizationMetadata = serde_json::from_str(
            r#"{"rotate_mat_position":0,"rotation_type":"matrix","code_dim":64,"num_bits":1,"packed":true}"#,
        )
        .unwrap();
        assert_eq!(metadata.query_estimator, RabitQueryEstimator::ResidualQuery);
    }

    #[test]
    fn test_new_rabit_metadata_uses_raw_query_estimator() {
        let metadata = make_test_metadata(64);
        assert_eq!(metadata.query_estimator, RabitQueryEstimator::RawQuery);
    }

    fn make_test_batch(codes: FixedSizeListArray) -> RecordBatch {
        let num_rows = codes.len();
        RecordBatch::try_from_iter(vec![
            (
                ROW_ID,
                Arc::new(UInt64Array::from_iter_values(0..num_rows as u64)) as ArrayRef,
            ),
            (RABIT_CODE_COLUMN, Arc::new(codes) as ArrayRef),
            (
                ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|v| v as f32),
                )) as ArrayRef,
            ),
            (
                SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|v| v as f32 + 0.5),
                )) as ArrayRef,
            ),
            (
                ERROR_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|v| v as f32 + 0.25),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn make_test_ex_codes(num_vectors: usize, code_dim: usize, num_bits: u8) -> FixedSizeListArray {
        let ex_bits = rabit_ex_bits(num_bits).unwrap();
        let ex_code_bytes = rabit_ex_code_bytes(code_dim, ex_bits).unwrap();
        let values = (0..num_vectors * ex_code_bytes)
            .map(|idx| (idx % 251) as u8)
            .collect::<Vec<_>>();
        FixedSizeListArray::try_new_from_values(UInt8Array::from(values), ex_code_bytes as i32)
            .unwrap()
    }

    fn make_test_batch_with_ex(
        codes: FixedSizeListArray,
        ex_codes: FixedSizeListArray,
    ) -> RecordBatch {
        let num_rows = codes.len();
        RecordBatch::try_from_iter(vec![
            (
                ROW_ID,
                Arc::new(UInt64Array::from_iter_values(0..num_rows as u64)) as ArrayRef,
            ),
            (RABIT_CODE_COLUMN, Arc::new(codes) as ArrayRef),
            (
                ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|v| v as f32),
                )) as ArrayRef,
            ),
            (
                SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|v| v as f32 + 0.5),
                )) as ArrayRef,
            ),
            (
                ERROR_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|v| v as f32 + 0.25),
                )) as ArrayRef,
            ),
            (RABIT_EX_CODE_COLUMN, Arc::new(ex_codes) as ArrayRef),
            (
                EX_ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|v| v as f32 + 10.5),
                )) as ArrayRef,
            ),
            (
                EX_SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|v| v as f32 + 1.5),
                )) as ArrayRef,
            ),
        ])
        .unwrap()
    }

    fn assert_codes_eq(actual: &FixedSizeListArray, expected: &FixedSizeListArray) {
        assert_eq!(actual.len(), expected.len());
        assert_eq!(actual.value_length(), expected.value_length());
        assert_eq!(
            actual.values().as_primitive::<UInt8Type>().values(),
            expected.values().as_primitive::<UInt8Type>().values()
        );
    }

    #[test]
    fn test_raw_query_multi_bit_distance_uses_ex_factors() {
        let code_dim = 8usize;
        let identity = Float32Array::from_iter_values(
            (0..code_dim)
                .flat_map(|row| (0..code_dim).map(move |col| if row == col { 1.0 } else { 0.0 })),
        );
        let rotate_mat =
            FixedSizeListArray::try_new_from_values(identity, code_dim as i32).unwrap();
        let metadata = RabitQuantizationMetadata {
            rotate_mat: Some(rotate_mat),
            rotate_mat_position: None,
            fast_rotation_signs: None,
            rotation_type: RQRotationType::Matrix,
            code_dim: code_dim as u32,
            num_bits: 2,
            packed: false,
            query_estimator: RabitQueryEstimator::RawQuery,
        };
        let codes =
            FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![0xff, 0xff]), 1).unwrap();
        let ex_codes =
            FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![0x00, 0xff]), 1).unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            (ROW_ID, Arc::new(UInt64Array::from(vec![0, 1])) as ArrayRef),
            (RABIT_CODE_COLUMN, Arc::new(codes) as ArrayRef),
            (
                ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0, 0.0])) as ArrayRef,
            ),
            (
                SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0, 0.0])) as ArrayRef,
            ),
            (RABIT_EX_CODE_COLUMN, Arc::new(ex_codes) as ArrayRef),
            (
                EX_ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![100.0, 10.0])) as ArrayRef,
            ),
            (
                EX_SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![1.0, 1.0])) as ArrayRef,
            ),
        ])
        .unwrap();
        let storage =
            RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None)
                .unwrap();
        let query = Arc::new(Float32Array::from(vec![1.0; code_dim])) as ArrayRef;
        let calc = storage.dist_calculator(query, 0.0);

        assert_eq!(calc.distance(0), 104.0);
        assert_eq!(calc.distance(1), 22.0);
        let mut distances = Vec::new();
        let mut u16_scratch = Vec::new();
        let mut u8_scratch = Vec::new();
        let mut u32_scratch = Vec::new();
        calc.distance_all_with_scratch(
            0,
            &mut distances,
            &mut u16_scratch,
            &mut u8_scratch,
            &mut u32_scratch,
        );
        assert_eq!(distances, vec![104.0, 22.0]);
    }

    /// Exercise the ex-dot kernel through the storage API for every ex width,
    /// including the widths without FastScan support ({1, 3, 5, 6, 7}), and a
    /// dim that is not a multiple of the 64-dim kernel group.
    ///
    /// The dim must be a multiple of 8: the binary distance stage consumes
    /// two 4-dim segments per code byte and ignores trailing dims otherwise.
    #[test]
    fn test_raw_query_multi_bit_distance_matches_reference_for_all_ex_widths() {
        use rand::rngs::SmallRng;
        use rand::{Rng, SeedableRng};

        // 72 exercises the kernels' padded-tail path; 1536 is a production
        // embedding dim exercising the full-group path. Both the blocked
        // format and the legacy sequential format must produce the same
        // distances.
        for (code_dim, num_rows) in [(72usize, 33usize), (1536, 33)] {
            for num_bits in 2..=9u8 {
                for legacy_format in [false, true] {
                    let ex_bits = num_bits - 1;
                    let mut rng = SmallRng::seed_from_u64(num_bits as u64);

                    let sign_bits = (0..num_rows * code_dim)
                        .map(|_| rng.random_bool(0.5))
                        .collect::<Vec<_>>();
                    let max_code = ((1u16 << ex_bits) - 1) as u8;
                    let ex_values = (0..num_rows * code_dim)
                        .map(|_| rng.random_range(0..=max_code))
                        .collect::<Vec<_>>();

                    let code_len = rabit_binary_code_bytes(code_dim);
                    let mut code_bytes = vec![0u8; num_rows * code_len];
                    for (row, bits) in sign_bits.chunks_exact(code_dim).enumerate() {
                        for (dim, &bit) in bits.iter().enumerate() {
                            code_bytes[row * code_len + dim / 8] |= (bit as u8) << (dim % 8);
                        }
                    }
                    let (ex_code_column, ex_code_len, ex_code_bytes) = if legacy_format {
                        let ex_code_len = rabit_ex_code_bytes(code_dim, ex_bits).unwrap();
                        let mut ex_code_bytes = vec![0u8; num_rows * ex_code_len];
                        for (row, values) in ex_values.chunks_exact(code_dim).enumerate() {
                            for (dim, &value) in values.iter().enumerate() {
                                let bit_offset = dim * ex_bits as usize;
                                let bits = (value as u16) << (bit_offset % 8);
                                ex_code_bytes[row * ex_code_len + bit_offset / 8] |= bits as u8;
                                if bits >> 8 != 0 {
                                    ex_code_bytes[row * ex_code_len + bit_offset / 8 + 1] |=
                                        (bits >> 8) as u8;
                                }
                            }
                        }
                        (RABIT_EX_CODE_COLUMN, ex_code_len, ex_code_bytes)
                    } else {
                        let ex_code_len = blocked_ex_code_bytes(code_dim, ex_bits);
                        let mut ex_code_bytes = vec![0u8; num_rows * ex_code_len];
                        for (row, values) in ex_code_bytes
                            .chunks_exact_mut(ex_code_len)
                            .zip(ex_values.chunks_exact(code_dim))
                        {
                            crate::vector::bq::ex_dot::pack_blocked_row(values, ex_bits, row);
                        }
                        (RABIT_BLOCKED_EX_CODE_COLUMN, ex_code_len, ex_code_bytes)
                    };

                    let identity = Float32Array::from_iter_values((0..code_dim).flat_map(|row| {
                        (0..code_dim).map(move |col| if row == col { 1.0 } else { 0.0 })
                    }));
                    let rotate_mat =
                        FixedSizeListArray::try_new_from_values(identity, code_dim as i32).unwrap();
                    let metadata = RabitQuantizationMetadata {
                        rotate_mat: Some(rotate_mat),
                        rotate_mat_position: None,
                        fast_rotation_signs: None,
                        rotation_type: RQRotationType::Matrix,
                        code_dim: code_dim as u32,
                        num_bits,
                        packed: false,
                        query_estimator: RabitQueryEstimator::RawQuery,
                    };
                    let codes = FixedSizeListArray::try_new_from_values(
                        UInt8Array::from(code_bytes),
                        code_len as i32,
                    )
                    .unwrap();
                    let ex_codes = FixedSizeListArray::try_new_from_values(
                        UInt8Array::from(ex_code_bytes),
                        ex_code_len as i32,
                    )
                    .unwrap();
                    let ex_add_factors = (0..num_rows)
                        .map(|_| rng.random_range(-1.0f32..1.0))
                        .collect::<Vec<_>>();
                    let ex_scale_factors = (0..num_rows)
                        .map(|_| rng.random_range(0.1f32..1.0))
                        .collect::<Vec<_>>();
                    let batch = RecordBatch::try_from_iter(vec![
                        (
                            ROW_ID,
                            Arc::new(UInt64Array::from_iter_values(0..num_rows as u64)) as ArrayRef,
                        ),
                        (RABIT_CODE_COLUMN, Arc::new(codes) as ArrayRef),
                        (
                            ADD_FACTORS_COLUMN,
                            Arc::new(Float32Array::from(vec![0.0; num_rows])) as ArrayRef,
                        ),
                        (
                            SCALE_FACTORS_COLUMN,
                            Arc::new(Float32Array::from(vec![0.0; num_rows])) as ArrayRef,
                        ),
                        (ex_code_column, Arc::new(ex_codes) as ArrayRef),
                        (
                            EX_ADD_FACTORS_COLUMN,
                            Arc::new(Float32Array::from(ex_add_factors.clone())) as ArrayRef,
                        ),
                        (
                            EX_SCALE_FACTORS_COLUMN,
                            Arc::new(Float32Array::from(ex_scale_factors.clone())) as ArrayRef,
                        ),
                    ])
                    .unwrap();
                    let storage = RabitQuantizationStorage::try_from_batch(
                        batch,
                        &metadata,
                        DistanceType::L2,
                        None,
                    )
                    .unwrap();

                    let query = (0..code_dim)
                        .map(|_| rng.random_range(-1.0f32..1.0))
                        .collect::<Vec<_>>();
                    let sum_q = query.iter().sum::<f32>();
                    let calc = storage.dist_calculator(
                        Arc::new(Float32Array::from(query.clone())) as ArrayRef,
                        0.0,
                    );

                    let code_scale = (1u32 << ex_bits) as f32;
                    let code_bias = -(code_scale - 0.5);
                    let expected = (0..num_rows)
                        .map(|row| {
                            let binary_ip = (0..code_dim)
                                .map(|dim| {
                                    query[dim] * sign_bits[row * code_dim + dim] as u8 as f32
                                })
                                .sum::<f32>();
                            let ex_dist = (0..code_dim)
                                .map(|dim| query[dim] * ex_values[row * code_dim + dim] as f32)
                                .sum::<f32>();
                            let full_dot = code_scale * binary_ip + ex_dist + code_bias * sum_q;
                            full_dot * ex_scale_factors[row] + ex_add_factors[row]
                        })
                        .collect::<Vec<_>>();

                    for (row, &want) in expected.iter().enumerate() {
                        let got = calc.distance(row as u32);
                        assert!(
                            (got - want).abs() <= 1e-3 * want.abs().max(1.0),
                            "num_bits={num_bits} row={row}: {got} != {want}"
                        );
                    }

                    let mut distances = Vec::new();
                    let mut u16_scratch = Vec::new();
                    let mut u8_scratch = Vec::new();
                    let mut u32_scratch = Vec::new();
                    calc.distance_all_with_scratch(
                        0,
                        &mut distances,
                        &mut u16_scratch,
                        &mut u8_scratch,
                        &mut u32_scratch,
                    );
                    assert_eq!(distances.len(), num_rows);
                    // The bulk path quantizes the binary LUT to u8, and that error is
                    // amplified by 2^ex_bits in the multi-bit estimate, so the value
                    // assertions need a quantization-aware bound. The FastScan ex
                    // widths additionally quantize the ex LUT and are covered by
                    // `test_raw_query_multi_bit_distance_all_uses_fastscan_for_split_ex_codes`.
                    if !matches!(ex_bits, 2 | 4 | 8) {
                        // Worst-case |error| of one u8-quantized binary LUT lookup is
                        // (table range) / 255 / 2, accumulated over one lookup per
                        // 8-dim pair of segments.
                        let num_tables = code_dim.div_ceil(4);
                        let mut table_min = f32::INFINITY;
                        let mut table_max = f32::NEG_INFINITY;
                        for segment in query.chunks(4) {
                            for subset in 0..16usize {
                                let value = segment
                                    .iter()
                                    .enumerate()
                                    .filter(|(idx, _)| subset & (1 << idx) != 0)
                                    .map(|(_, q)| *q)
                                    .sum::<f32>();
                                table_min = table_min.min(value);
                                table_max = table_max.max(value);
                            }
                        }
                        let binary_bound =
                            code_scale * num_tables as f32 * (table_max - table_min) / 255.0 / 2.0
                                * ex_scale_factors.iter().fold(0.0f32, |max, &s| max.max(s));
                        for (row, (&got, &want)) in
                            distances.iter().zip(expected.iter()).enumerate()
                        {
                            assert!(
                                (got - want).abs() <= binary_bound + 1e-3,
                                "num_bits={num_bits} row={row} (distance_all): {got} != {want} (bound {binary_bound})"
                            );
                        }
                        // Rows past the SIMD batch use the exact binary path, so the
                        // final remainder row must match the per-candidate distance.
                        let remainder_row = num_rows - 1;
                        let got = distances[remainder_row];
                        let want = calc.distance(remainder_row as u32);
                        assert!(
                            (got - want).abs() <= 1e-3 * want.abs().max(1.0),
                            "num_bits={num_bits} remainder row (distance_all): {got} != {want}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_fast_approx_mode_uses_one_bit_scores_for_multi_bit_raw_query() {
        let code_dim = 8usize;
        let identity = Float32Array::from_iter_values(
            (0..code_dim)
                .flat_map(|row| (0..code_dim).map(move |col| if row == col { 1.0 } else { 0.0 })),
        );
        let rotate_mat =
            FixedSizeListArray::try_new_from_values(identity, code_dim as i32).unwrap();
        let metadata = RabitQuantizationMetadata {
            rotate_mat: Some(rotate_mat),
            rotate_mat_position: None,
            fast_rotation_signs: None,
            rotation_type: RQRotationType::Matrix,
            code_dim: code_dim as u32,
            num_bits: 2,
            packed: false,
            query_estimator: RabitQueryEstimator::RawQuery,
        };
        let codes =
            FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![0xff, 0xff]), 1).unwrap();
        let ex_codes =
            FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![0x00, 0xff]), 1).unwrap();
        let batch = make_test_batch_with_ex(codes, ex_codes)
            .replace_column_by_name(
                SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0, 0.0])),
            )
            .unwrap();
        let storage =
            RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None)
                .unwrap();
        let query = Arc::new(Float32Array::from(vec![1.0; code_dim])) as ArrayRef;
        let normal = storage.dist_calculator(query.clone(), 0.0).distance_all(0);

        let mut f32_scratch = Vec::new();
        let calc = storage.dist_calculator_with_scratch(
            query,
            0.0,
            None,
            &mut f32_scratch,
            DistanceCalculatorOptions {
                approx_mode: ApproxMode::Fast,
            },
        );
        let mut distances = Vec::new();
        let mut u16_scratch = Vec::new();
        let mut u8_scratch = Vec::new();
        let mut u32_scratch = Vec::new();
        calc.distance_all_with_scratch(
            0,
            &mut distances,
            &mut u16_scratch,
            &mut u8_scratch,
            &mut u32_scratch,
        );

        let expected_fast = (0..2)
            .map(|id| calc.distance(id as u32))
            .collect::<Vec<_>>();
        assert_ne!(normal, distances);
        assert_eq!(distances, expected_fast);
        assert_eq!(
            calc.raw_query_lower_bound_gating_disabled_reason(),
            Some("approx_mode_fast")
        );
    }

    #[test]
    fn test_accurate_approx_mode_reduces_binary_lut_quantization_error() {
        let code_dim = 64usize;
        let num_rows = BATCH_SIZE;
        let original_codes = make_test_codes(num_rows, code_dim as i32);
        let metadata = make_test_metadata(code_dim);
        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch(original_codes),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();
        let query = Arc::new(Float32Array::from_iter_values(
            (0..code_dim).map(|idx| (idx as f32 * 0.137).sin() + idx as f32 * 0.003),
        )) as ArrayRef;
        let exact_calc = storage.dist_calculator(query.clone(), 0.0);
        let exact = (0..num_rows)
            .map(|id| exact_calc.distance(id as u32))
            .collect::<Vec<_>>();

        let normal = {
            let mut f32_scratch = Vec::new();
            let calc = storage.dist_calculator_with_scratch(
                query.clone(),
                0.0,
                None,
                &mut f32_scratch,
                DistanceCalculatorOptions::default(),
            );
            let mut distances = Vec::new();
            let mut u16_scratch = Vec::new();
            let mut u8_scratch = Vec::new();
            let mut u32_scratch = Vec::new();
            calc.distance_all_with_scratch(
                0,
                &mut distances,
                &mut u16_scratch,
                &mut u8_scratch,
                &mut u32_scratch,
            );
            distances
        };

        let (accurate, hacc_table_len, hacc_packed_table_len, hacc_accum_len) = {
            let mut f32_scratch = Vec::new();
            let calc = storage.dist_calculator_with_scratch(
                query,
                0.0,
                None,
                &mut f32_scratch,
                DistanceCalculatorOptions {
                    approx_mode: ApproxMode::Accurate,
                },
            );
            let mut distances = Vec::new();
            let mut u16_scratch = Vec::new();
            let mut u8_scratch = Vec::new();
            let mut u32_scratch = Vec::new();
            calc.distance_all_with_scratch(
                0,
                &mut distances,
                &mut u16_scratch,
                &mut u8_scratch,
                &mut u32_scratch,
            );
            (
                distances,
                u16_scratch.len(),
                u8_scratch.len(),
                u32_scratch.len(),
            )
        };

        let normal_error = normal
            .iter()
            .zip(exact.iter())
            .map(|(actual, expected)| (actual - expected).abs())
            .sum::<f32>();
        let accurate_error = accurate
            .iter()
            .zip(exact.iter())
            .map(|(actual, expected)| (actual - expected).abs())
            .sum::<f32>();

        assert!(normal_error > 0.0);
        assert!(
            accurate_error < normal_error,
            "accurate_error={accurate_error}, normal_error={normal_error}"
        );
        assert_eq!(hacc_table_len, code_dim * 4);
        assert_eq!(hacc_packed_table_len, code_dim * 8);
        assert_eq!(hacc_accum_len, num_rows);
    }

    fn assert_raw_query_multi_bit_distance_all_uses_fastscan(
        num_bits: u8,
        legacy_format: bool,
        with_error_factors: bool,
    ) {
        // Not a multiple of 64, so the padded-tail LUT entries are exercised;
        // a multiple of 8 as the binary stage requires.
        let code_dim = 72usize;
        let num_rows = BATCH_SIZE + 1;
        let ex_bits = rabit_ex_bits(num_bits).unwrap();
        let max_code = ((1u16 << ex_bits) - 1) as u8;
        let identity = Float32Array::from_iter_values(
            (0..code_dim)
                .flat_map(|row| (0..code_dim).map(move |col| if row == col { 1.0 } else { 0.0 })),
        );
        let rotate_mat =
            FixedSizeListArray::try_new_from_values(identity, code_dim as i32).unwrap();
        let metadata = RabitQuantizationMetadata {
            rotate_mat: Some(rotate_mat),
            rotate_mat_position: None,
            fast_rotation_signs: None,
            rotation_type: RQRotationType::Matrix,
            code_dim: code_dim as u32,
            num_bits,
            packed: false,
            query_estimator: RabitQueryEstimator::RawQuery,
        };
        let code_len = rabit_binary_code_bytes(code_dim);
        let codes = FixedSizeListArray::try_new_from_values(
            UInt8Array::from_iter_values((0..num_rows * code_len).map(|idx| (idx * 13) as u8)),
            code_len as i32,
        )
        .unwrap();
        let ex_values = (0..num_rows * code_dim)
            .map(|idx| ((idx * 37) % (max_code as usize + 1)) as u8)
            .collect::<Vec<_>>();
        let (ex_code_column, ex_code_len, ex_code_bytes) = if legacy_format {
            let ex_code_len = rabit_ex_code_bytes(code_dim, ex_bits).unwrap();
            let mut ex_code_bytes = vec![0u8; num_rows * ex_code_len];
            for (row, values) in ex_values.chunks_exact(code_dim).enumerate() {
                for (dim, &value) in values.iter().enumerate() {
                    let bit_offset = dim * ex_bits as usize;
                    let bits = (value as u16) << (bit_offset % 8);
                    ex_code_bytes[row * ex_code_len + bit_offset / 8] |= bits as u8;
                    if bits >> 8 != 0 {
                        ex_code_bytes[row * ex_code_len + bit_offset / 8 + 1] |= (bits >> 8) as u8;
                    }
                }
            }
            (RABIT_EX_CODE_COLUMN, ex_code_len, ex_code_bytes)
        } else {
            let ex_code_len = blocked_ex_code_bytes(code_dim, ex_bits);
            let mut ex_code_bytes = vec![0u8; num_rows * ex_code_len];
            for (row, values) in ex_code_bytes
                .chunks_exact_mut(ex_code_len)
                .zip(ex_values.chunks_exact(code_dim))
            {
                crate::vector::bq::ex_dot::pack_blocked_row(values, ex_bits, row);
            }
            (RABIT_BLOCKED_EX_CODE_COLUMN, ex_code_len, ex_code_bytes)
        };
        let ex_codes = FixedSizeListArray::try_new_from_values(
            UInt8Array::from(ex_code_bytes),
            ex_code_len as i32,
        )
        .unwrap();
        let batch = RecordBatch::try_from_iter(vec![
            (
                ROW_ID,
                Arc::new(UInt64Array::from_iter_values(0..num_rows as u64)) as ArrayRef,
            ),
            (RABIT_CODE_COLUMN, Arc::new(codes) as ArrayRef),
            (
                ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0; num_rows])) as ArrayRef,
            ),
            (
                SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![1.0; num_rows])) as ArrayRef,
            ),
            (ex_code_column, Arc::new(ex_codes) as ArrayRef),
            (
                EX_ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![0.0; num_rows])) as ArrayRef,
            ),
            (
                EX_SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![1.0; num_rows])) as ArrayRef,
            ),
        ])
        .unwrap();
        let batch = if with_error_factors {
            batch
                .try_with_column(
                    crate::vector::bq::transform::ERROR_FACTORS_FIELD.clone(),
                    Arc::new(Float32Array::from(vec![1000.0; num_rows])) as ArrayRef,
                )
                .unwrap()
        } else {
            batch
        };
        let storage =
            RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None)
                .unwrap();
        // The FastScan transpose only exists for indexes that can reach the
        // bulk bypass path (no error factors); gated indexes fall through to
        // the exact per-row kernels in `distance_all`.
        assert_eq!(storage.packed_ex_codes.is_some(), !with_error_factors);

        // A per-dim varying query so that any dim-mapping error in the
        // FastScan LUT shows up as a value mismatch.
        let query_values = (0..code_dim)
            .map(|dim| (dim % 11) as f32 * 0.3 - 1.5)
            .collect::<Vec<_>>();
        let query = Arc::new(Float32Array::from(query_values.clone())) as ArrayRef;
        let calc = storage.dist_calculator(query, 0.0);
        let mut distances = Vec::new();
        let mut u16_scratch = Vec::new();
        let mut u8_scratch = Vec::new();
        let mut u32_scratch = Vec::new();
        calc.distance_all_with_scratch(
            0,
            &mut distances,
            &mut u16_scratch,
            &mut u8_scratch,
            &mut u32_scratch,
        );

        assert_eq!(distances.len(), num_rows);
        assert_eq!(u16_scratch.len(), BATCH_SIZE);
        let loaded_ex_code_len = storage.ex_codes.as_ref().unwrap().value_length() as usize;
        if with_error_factors {
            // The gated path never builds the ex LUT; the scratch holds the
            // binary LUT only.
            assert_eq!(u8_scratch.len(), code_dim * 4);
        } else {
            assert_eq!(u8_scratch.len(), loaded_ex_code_len * 2 * SEGMENT_NUM_CODES);
        }

        // The fastscan estimate differs from the exact path only by the u8
        // quantization of the binary LUT (amplified by 2^ex_bits) and of the
        // ex LUT, so bound the comparison by those quantization errors.
        let mut table_min = f32::INFINITY;
        let mut table_max = f32::NEG_INFINITY;
        for segment in query_values.chunks(4) {
            for subset in 0..SEGMENT_NUM_CODES {
                let value = segment
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| subset & (1 << idx) != 0)
                    .map(|(_, q)| *q)
                    .sum::<f32>();
                table_min = table_min.min(value);
                table_max = table_max.max(value);
            }
        }
        let code_scale = (1u32 << ex_bits) as f32;
        let binary_bound =
            code_scale * code_dim.div_ceil(4) as f32 * (table_max - table_min) / 510.0;
        let mut padded_query = vec![0.0f32; crate::vector::bq::ex_dot::padded_query_len(code_dim)];
        crate::vector::bq::ex_dot::pad_query_into(&query_values, &mut padded_query);
        let mut quantized_table = Vec::new();
        let (ex_qmin, ex_qmax, ex_qcap) = quantize_ex_fastscan_dist_table_into(
            ex_bits,
            loaded_ex_code_len,
            &padded_query,
            &mut quantized_table,
        );
        // Without the FastScan transpose the ex stage is exact, so only the
        // binary LUT quantization remains.
        let ex_bound = if with_error_factors {
            0.0
        } else {
            (loaded_ex_code_len * 2) as f32 * (ex_qmax - ex_qmin) / ex_qcap / 2.0
        };
        let bound = (binary_bound + ex_bound) * 1.5 + 1e-3;
        for (id, distance) in distances.iter().take(BATCH_SIZE).enumerate() {
            let exact = calc.distance(id as u32);
            assert!(
                (*distance - exact).abs() <= bound,
                "distance_all fastscan mismatch for id {id} (num_bits={num_bits} legacy={legacy_format}): actual={distance}, exact={exact}, bound={bound}"
            );
        }
        assert_eq!(distances[BATCH_SIZE], calc.distance(BATCH_SIZE as u32));
    }

    #[test]
    fn test_raw_query_multi_bit_distance_all_uses_fastscan_for_split_ex_codes() {
        for num_bits in [3, 5, 9] {
            for legacy_format in [false, true] {
                assert_raw_query_multi_bit_distance_all_uses_fastscan(
                    num_bits,
                    legacy_format,
                    false,
                );
            }
            // Gated indexes (with error factors) skip the FastScan artifacts
            // and score the bulk path with the exact kernels.
            assert_raw_query_multi_bit_distance_all_uses_fastscan(num_bits, false, true);
        }
    }

    /// A dist table whose `num_tables`-scaled reconstruction overflows `f32`
    /// must fall back to exact distances rather than the affine dequant's
    /// `0 * inf = NaN`. Covers both the u8 (Normal) and u16 (Accurate) LUT
    /// paths end-to-end through `distance_all`, asserting the result is
    /// NaN-free and bit-identical to the always-exact per-row computation.
    #[rstest]
    fn test_degenerate_dist_table_falls_back_to_exact_distances(
        #[values(ApproxMode::Normal, ApproxMode::Accurate)] approx_mode: ApproxMode,
    ) {
        let code_dim = 8usize;
        let num_rows = BATCH_SIZE + 5;
        let num_bits = 3;
        let ex_bits = rabit_ex_bits(num_bits).unwrap();
        let identity = Float32Array::from_iter_values(
            (0..code_dim)
                .flat_map(|row| (0..code_dim).map(move |col| if row == col { 1.0 } else { 0.0 })),
        );
        let rotate_mat =
            FixedSizeListArray::try_new_from_values(identity, code_dim as i32).unwrap();
        let metadata = RabitQuantizationMetadata {
            rotate_mat: Some(rotate_mat),
            rotate_mat_position: None,
            fast_rotation_signs: None,
            rotation_type: RQRotationType::Matrix,
            code_dim: code_dim as u32,
            num_bits,
            packed: false,
            query_estimator: RabitQueryEstimator::RawQuery,
        };
        let codes = FixedSizeListArray::try_new_from_values(
            UInt8Array::from_iter_values((0..num_rows).map(|idx| (idx * 19) as u8)),
            rabit_binary_code_bytes(code_dim) as i32,
        )
        .unwrap();
        let ex_codes = make_test_ex_codes(num_rows, code_dim, num_bits);
        let batch = make_test_batch_with_ex(codes, ex_codes);
        let storage =
            RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None)
                .unwrap();
        let query = Arc::new(Float32Array::from(vec![1.0; code_dim])) as ArrayRef;

        let mut calc = storage.dist_calculator(query, 4.0);
        calc.approx_mode = approx_mode;
        // num_tables = (code_dim * 4) / SEGMENT_NUM_CODES = 2; the extrema sum
        // (qmax - qmin = 4e38) overflows when scaled by num_tables, so the
        // quantizer returns `Exact`. Per-row sums stay finite (each row reads
        // one entry per segment), so the exact path is well-defined.
        let mut degenerate = vec![0.0f32; code_dim * 4];
        degenerate[0] = -2e38;
        degenerate[1] = 2e38;
        calc.dist_table = Cow::Owned(degenerate);

        let code_len = rabit_binary_code_bytes(code_dim);
        let ex_codes = calc.ex_codes.unwrap();
        let ex_add_factors = calc.ex_add_factors.unwrap();
        let ex_scale_factors = calc.ex_scale_factors.unwrap();
        let expected = (0..num_rows)
            .map(|id| {
                let binary_ip = compute_single_rq_distance(
                    calc.codes,
                    id,
                    num_rows,
                    code_len,
                    &calc.dist_table,
                );
                calc.raw_query_multi_bit_exact_distance(
                    id,
                    binary_ip,
                    ex_bits,
                    ex_codes,
                    ex_add_factors,
                    ex_scale_factors,
                )
            })
            .collect::<Vec<_>>();

        let actual = calc.distance_all(0);
        assert_eq!(actual.len(), num_rows);
        for id in 0..num_rows {
            assert!(
                !actual[id].is_nan(),
                "approx_mode={approx_mode:?} id={id}: degenerate table produced NaN"
            );
            assert_eq!(
                actual[id].to_bits(),
                expected[id].to_bits(),
                "approx_mode={approx_mode:?} id={id}: distance_all must match the exact path"
            );
        }
    }

    #[test]
    fn test_raw_query_multi_bit_accumulate_topk_uses_lower_bound_gating() {
        let code_dim = 8usize;
        let num_rows = BATCH_SIZE + 9;
        let num_bits = 3;
        let ex_bits = rabit_ex_bits(num_bits).unwrap();
        let identity = Float32Array::from_iter_values(
            (0..code_dim)
                .flat_map(|row| (0..code_dim).map(move |col| if row == col { 1.0 } else { 0.0 })),
        );
        let rotate_mat =
            FixedSizeListArray::try_new_from_values(identity, code_dim as i32).unwrap();
        let metadata = RabitQuantizationMetadata {
            rotate_mat: Some(rotate_mat),
            rotate_mat_position: None,
            fast_rotation_signs: None,
            rotation_type: RQRotationType::Matrix,
            code_dim: code_dim as u32,
            num_bits,
            packed: false,
            query_estimator: RabitQueryEstimator::RawQuery,
        };
        let codes = FixedSizeListArray::try_new_from_values(
            UInt8Array::from_iter_values((0..num_rows).map(|idx| (idx * 19) as u8)),
            1,
        )
        .unwrap();
        let ex_code_len = rabit_ex_code_bytes(code_dim, ex_bits).unwrap();
        let ex_codes = FixedSizeListArray::try_new_from_values(
            UInt8Array::from_iter_values(
                (0..num_rows * ex_code_len).map(|idx| (idx * 29 % 251) as u8),
            ),
            ex_code_len as i32,
        )
        .unwrap();
        let batch = make_test_batch_with_ex(codes, ex_codes)
            .replace_column_by_name(
                ERROR_FACTORS_COLUMN,
                Arc::new(Float32Array::from(vec![1000.0; num_rows])),
            )
            .unwrap();
        let storage =
            RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None)
                .unwrap();
        let query = Arc::new(Float32Array::from(vec![1.0; code_dim])) as ArrayRef;
        let calc = storage.dist_calculator(query, 4.0);
        assert!(
            calc.raw_query_lower_bound_gating_disabled_reason()
                .is_none()
        );

        let k = 5;
        let mut binary_ips = Vec::new();
        let mut binary_u16_scratch = Vec::new();
        let mut binary_u8_scratch = Vec::new();
        let mut binary_u32_scratch = Vec::new();
        calc.binary_distances_with_scratch(
            num_rows,
            rabit_binary_code_bytes(code_dim),
            &mut binary_ips,
            &mut binary_u16_scratch,
            &mut binary_u8_scratch,
            &mut binary_u32_scratch,
        );
        let ex_codes = calc.ex_codes.unwrap();
        let ex_add_factors = calc.ex_add_factors.unwrap();
        let ex_scale_factors = calc.ex_scale_factors.unwrap();
        let mut expected = binary_ips
            .iter()
            .copied()
            .enumerate()
            .map(|(id, binary_ip)| {
                (
                    id,
                    calc.raw_query_multi_bit_exact_distance(
                        id,
                        binary_ip,
                        ex_bits,
                        ex_codes,
                        ex_add_factors,
                        ex_scale_factors,
                    ),
                )
            })
            .collect::<Vec<_>>();
        expected.sort_by(|left, right| left.1.total_cmp(&right.1));
        expected.truncate(k);
        let mut expected = expected
            .into_iter()
            .map(|(id, dist)| (id as u64, dist))
            .collect::<Vec<_>>();
        expected.sort_by_key(|left| left.0);

        let mut heap = BinaryHeap::with_capacity(k);
        let mut distances = Vec::new();
        let mut u16_scratch = Vec::new();
        let mut u8_scratch = Vec::new();
        let mut u32_scratch = Vec::new();
        calc.accumulate_topk_with_scratch(
            k,
            None,
            None,
            |id| id as u64,
            &mut heap,
            &mut distances,
            &mut u16_scratch,
            &mut u8_scratch,
            &mut u32_scratch,
        );
        let mut actual = heap
            .into_iter()
            .map(|node| (node.id, node.dist.0))
            .collect::<Vec<_>>();
        actual.sort_by_key(|left| left.0);

        assert_eq!(actual.len(), expected.len());
        for ((actual_id, actual_dist), (expected_id, expected_dist)) in
            actual.into_iter().zip(expected)
        {
            assert_eq!(actual_id, expected_id);
            assert!(
                (actual_dist - expected_dist).abs() < 1e-5,
                "actual={actual_dist}, expected={expected_dist}"
            );
        }
    }

    /// Inputs crafted so the top-k scan outcomes are fully determined by the
    /// factor columns: with zero scale factors, a zero query factor, and a
    /// query error of one, the lower bound is
    /// `add_factors[id] - error_factors[id]`, and with zero ex scale factors
    /// the exact distance is `ex_add_factors[id]`, regardless of the random
    /// codes and query.
    struct CraftedTopkData {
        codes: Vec<u8>,
        ex_codes: Vec<u8>,
        dist_table: Vec<f32>,
        ex_query: Vec<f32>,
        scale_factors: Vec<f32>,
        add_factors: Vec<f32>,
        error_factors: Vec<f32>,
        ex_scale_factors: Vec<f32>,
        ex_add_factors: Vec<f32>,
    }

    const CRAFTED_TOPK_DIM: usize = 64;
    const CRAFTED_TOPK_NUM_BITS: u8 = 5;

    impl CraftedTopkData {
        fn new(
            exact_dists: &[f32],
            lower_bound_margins: &[f32],
            error_factors: Vec<f32>,
            rng: &mut SmallRng,
        ) -> Self {
            let n = exact_dists.len();
            let code_len = rabit_binary_code_bytes(CRAFTED_TOPK_DIM);
            let ex_code_len = blocked_ex_code_bytes(CRAFTED_TOPK_DIM, CRAFTED_TOPK_NUM_BITS - 1);
            let add_factors = izip!(exact_dists, lower_bound_margins, &error_factors)
                .map(|(dist, margin, error)| dist - margin + error)
                .collect();
            Self {
                codes: (0..n * code_len).map(|_| rng.random()).collect(),
                ex_codes: (0..n * ex_code_len).map(|_| rng.random()).collect(),
                dist_table: (0..CRAFTED_TOPK_DIM * 4)
                    .map(|_| rng.random_range(-1.0f32..1.0))
                    .collect(),
                ex_query: (0..CRAFTED_TOPK_DIM)
                    .map(|_| rng.random_range(-1.0f32..1.0))
                    .collect(),
                scale_factors: vec![0.0; n],
                add_factors,
                error_factors,
                ex_scale_factors: vec![0.0; n],
                ex_add_factors: exact_dists.to_vec(),
            }
        }

        fn calculator(&self, approx_mode: ApproxMode) -> RabitDistCalculator<'_> {
            RabitDistCalculator::new(
                CRAFTED_TOPK_DIM,
                CRAFTED_TOPK_NUM_BITS,
                RabitQueryEstimator::RawQuery,
                Cow::Borrowed(self.dist_table.as_slice()),
                Cow::Borrowed(self.ex_query.as_slice()),
                0.7,
                &self.codes,
                Some(&self.ex_codes),
                blocked_ex_code_bytes(CRAFTED_TOPK_DIM, CRAFTED_TOPK_NUM_BITS - 1),
                &self.add_factors,
                &self.scale_factors,
                Some(&self.error_factors),
                Some(&self.ex_add_factors),
                Some(&self.ex_scale_factors),
                None,
                0.0,
                1.0,
                approx_mode,
            )
        }
    }

    fn canonical_heap_rows(heap: BinaryHeap<OrderedNode<u64>>) -> Vec<(u32, u64)> {
        let mut rows = heap
            .into_iter()
            .map(|node| (node.dist.0.to_bits(), node.id))
            .collect::<Vec<_>>();
        rows.sort_unstable();
        rows
    }

    /// The dense (SIMD-pruned) scan must reproduce the sparse scalar scan
    /// exactly: identical heap contents including row ids, and the k smallest
    /// in-bounds exact distances overall.
    #[rstest]
    fn test_raw_query_multi_bit_topk_dense_matches_sparse(
        #[values(ApproxMode::Normal, ApproxMode::Accurate)] approx_mode: ApproxMode,
        #[values("descending", "ascending", "random", "duplicates", "duplicate_ties")]
        ordering: &str,
    ) {
        for n in [1usize, 15, 16, 17, 100, 4109] {
            let mut rng = SmallRng::seed_from_u64(n as u64 * 31 + ordering.len() as u64);
            let exact_dists: Vec<f32> = match ordering {
                // Improving rows force constant heap updates.
                "descending" => (0..n).map(|id| (n - id) as f32).collect(),
                // Worsening rows force mass pruning, the common regime.
                "ascending" => (0..n).map(|id| id as f32).collect(),
                "random" => (0..n).map(|_| rng.random_range(0.0..n as f32)).collect(),
                "duplicates" => (0..n).map(|id| (id % 7) as f32).collect(),
                // Lower bound equals the distance, so heap-threshold and
                // upper-bound comparisons hit exact `>=` ties.
                "duplicate_ties" => (0..n).map(|id| (id % 5) as f32).collect(),
                _ => unreachable!(),
            };
            let (margins, error_factors) = if ordering == "duplicate_ties" {
                (vec![0.0; n], vec![0.0; n])
            } else if ordering == "random" {
                (
                    (0..n).map(|_| rng.random_range(0.0f32..2.0)).collect(),
                    (0..n).map(|_| rng.random_range(0.0f32..1.0)).collect(),
                )
            } else {
                (
                    vec![1.0; n],
                    (0..n).map(|_| rng.random_range(0.0f32..1.0)).collect(),
                )
            };
            let data = CraftedTopkData::new(&exact_dists, &margins, error_factors, &mut rng);
            let calc = data.calculator(approx_mode);
            assert!(
                calc.raw_query_lower_bound_gating_disabled_reason()
                    .is_none()
            );

            let max_dist = exact_dists.iter().fold(0.0f32, |acc, dist| acc.max(*dist));
            for k in [1usize, 10, n + 7] {
                for bounds in [(None, None), (Some(max_dist * 0.25), Some(max_dist * 0.7))] {
                    let (lower_bound, upper_bound) = bounds;
                    let mut dense_heap = BinaryHeap::new();
                    let mut sparse_heap = BinaryHeap::new();
                    let mut dists = Vec::new();
                    let mut u16_scratch = Vec::new();
                    let mut u8_scratch = Vec::new();
                    let mut u32_scratch = Vec::new();
                    // Two passes sharing the heap, as IVF partition probing
                    // does: the second pass starts with a full, tight heap.
                    for pass in 0..2u64 {
                        let offset = pass * n as u64;
                        calc.accumulate_topk_with_scratch(
                            k,
                            lower_bound,
                            upper_bound,
                            |id| id as u64 + offset,
                            &mut dense_heap,
                            &mut dists,
                            &mut u16_scratch,
                            &mut u8_scratch,
                            &mut u32_scratch,
                        );
                        calc.accumulate_filtered_topk_with_scratch(
                            k,
                            lower_bound,
                            upper_bound,
                            (0..n as u32).map(|id| (id, id as u64 + offset)),
                            |_| true,
                            &mut sparse_heap,
                            &mut dists,
                            &mut u16_scratch,
                            &mut u8_scratch,
                            &mut u32_scratch,
                        );
                    }
                    let dense = canonical_heap_rows(dense_heap);
                    let sparse = canonical_heap_rows(sparse_heap);
                    assert_eq!(
                        dense, sparse,
                        "ordering={ordering} n={n} k={k} bounds={bounds:?} mode={approx_mode:?}"
                    );

                    // The distance multiset must be the k smallest in-bounds
                    // distances over both passes. Row ids are not compared:
                    // evictions among tied maxima depend on heap layout.
                    let query_lower_bound = lower_bound.unwrap_or(f32::MIN);
                    let query_upper_bound = upper_bound.unwrap_or(f32::MAX);
                    let mut expected = (0..2 * n)
                        .map(|row| exact_dists[row % n])
                        .filter(|dist| *dist >= query_lower_bound && *dist < query_upper_bound)
                        .map(|dist| dist.to_bits())
                        .collect::<Vec<_>>();
                    expected.sort_unstable();
                    expected.truncate(k);
                    let actual = dense.iter().map(|(dist, _)| *dist).collect::<Vec<_>>();
                    assert_eq!(
                        actual, expected,
                        "ordering={ordering} n={n} k={k} bounds={bounds:?} mode={approx_mode:?}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_raw_query_one_bit_distance_uses_binary_factors_without_ex_columns() {
        let code_dim = 8usize;
        let identity = Float32Array::from_iter_values(
            (0..code_dim)
                .flat_map(|row| (0..code_dim).map(move |col| if row == col { 1.0 } else { 0.0 })),
        );
        let rotate_mat =
            FixedSizeListArray::try_new_from_values(identity, code_dim as i32).unwrap();
        let metadata = RabitQuantizationMetadata {
            rotate_mat: Some(rotate_mat),
            rotate_mat_position: None,
            fast_rotation_signs: None,
            rotation_type: RQRotationType::Matrix,
            code_dim: code_dim as u32,
            num_bits: 1,
            packed: false,
            query_estimator: RabitQueryEstimator::RawQuery,
        };
        let codes =
            FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![0xff, 0x00]), 1).unwrap();
        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch(codes),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();
        let query = Arc::new(Float32Array::from(vec![1.0; code_dim])) as ArrayRef;
        let calc = storage.dist_calculator(query, 3.0);

        assert_eq!(calc.distance_all(0), vec![5.0, -2.0]);
    }

    #[test]
    fn test_raw_query_context_matches_fallback_and_only_updates_partition_factor() {
        let code_dim = 8usize;
        let identity = Float32Array::from_iter_values(
            (0..code_dim)
                .flat_map(|row| (0..code_dim).map(move |col| if row == col { 1.0 } else { 0.0 })),
        );
        let rotate_mat =
            FixedSizeListArray::try_new_from_values(identity, code_dim as i32).unwrap();
        let metadata = RabitQuantizationMetadata {
            rotate_mat: Some(rotate_mat),
            rotate_mat_position: None,
            fast_rotation_signs: None,
            rotation_type: RQRotationType::Matrix,
            code_dim: code_dim as u32,
            num_bits: 2,
            packed: false,
            query_estimator: RabitQueryEstimator::RawQuery,
        };
        let codes =
            FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![0xff, 0xff]), 1).unwrap();
        let ex_codes =
            FixedSizeListArray::try_new_from_values(UInt8Array::from(vec![0x00, 0xff]), 1).unwrap();
        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch_with_ex(codes, ex_codes),
            &metadata,
            DistanceType::Dot,
            None,
        )
        .unwrap();
        let query = Arc::new(Float32Array::from(vec![1.0; code_dim])) as ArrayRef;
        let rotated_centroid = vec![0.25; code_dim];
        let raw_query = metadata.prepare_raw_query_context(query.as_ref()).unwrap();

        let mut fallback_scratch = Vec::new();
        let expected = storage
            .dist_calculator_with_scratch(
                query.clone(),
                123.0,
                Some(QueryResidual::RabitRawQuery {
                    rotated_centroid: Some(&rotated_centroid),
                    query: None,
                }),
                &mut fallback_scratch,
                DistanceCalculatorOptions::default(),
            )
            .distance_all(0);

        let mut prepared_scratch = Vec::new();
        let actual = storage
            .dist_calculator_with_scratch(
                query,
                456.0,
                Some(QueryResidual::RabitRawQuery {
                    rotated_centroid: Some(&rotated_centroid),
                    query: Some(&raw_query),
                }),
                &mut prepared_scratch,
                DistanceCalculatorOptions::default(),
            )
            .distance_all(0);

        assert_eq!(actual, expected);
        assert!(prepared_scratch.is_empty());
    }

    #[test]
    fn test_try_from_batch_canonicalizes_rq_codes_to_packed_layout() {
        let original_codes = make_test_codes(50, 64);
        let metadata = make_test_metadata(original_codes.value_length() as usize * 8);
        assert!(!metadata.packed);

        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch(original_codes.clone()),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();

        assert!(storage.metadata().packed);
        let stored_batch = storage.to_batches().unwrap().next().unwrap();
        let stored_codes = stored_batch[RABIT_CODE_COLUMN].as_fixed_size_list();
        let expected_codes = pack_codes(&original_codes);
        assert_codes_eq(stored_codes, &expected_codes);
    }

    #[test]
    fn test_try_from_batch_uses_l2_for_cosine() {
        let original_codes = make_test_codes(50, 64);
        let metadata = make_test_metadata(original_codes.value_length() as usize * 8);

        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch(original_codes),
            &metadata,
            DistanceType::Cosine,
            None,
        )
        .unwrap();

        assert_eq!(storage.distance_type(), DistanceType::L2);
    }

    #[test]
    fn test_try_from_batch_keeps_cosine_for_legacy_residual_query() {
        let original_codes = make_test_codes(50, 64);
        let mut metadata = make_test_metadata(original_codes.value_length() as usize * 8);
        metadata.query_estimator = RabitQueryEstimator::ResidualQuery;

        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch(original_codes),
            &metadata,
            DistanceType::Cosine,
            None,
        )
        .unwrap();

        assert_eq!(storage.distance_type(), DistanceType::Cosine);
    }

    #[test]
    fn test_try_from_batch_requires_ex_columns_for_multi_bit_rq() {
        let original_codes = make_test_codes(50, 64);
        let mut metadata = make_test_metadata(original_codes.value_length() as usize * 8);
        metadata.num_bits = 2;

        let err = RabitQuantizationStorage::try_from_batch(
            make_test_batch(original_codes),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap_err();
        assert!(
            err.to_string()
                .contains("requires __blocked_ex_codes column"),
            "{}",
            err
        );
    }

    #[test]
    fn test_try_from_batch_requires_ex_add_factors_for_multi_bit_rq() {
        let original_codes = make_test_codes(50, 64);
        let code_dim = original_codes.value_length() as usize * 8;
        let ex_codes = make_test_ex_codes(original_codes.len(), code_dim, 9);
        let mut metadata = make_test_metadata(code_dim);
        metadata.num_bits = 9;
        let batch = make_test_batch_with_ex(original_codes, ex_codes)
            .drop_column(EX_ADD_FACTORS_COLUMN)
            .unwrap();

        let err =
            RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None)
                .unwrap_err();
        assert!(
            err.to_string().contains("requires __add_factors_ex column"),
            "{}",
            err
        );
    }

    #[test]
    fn test_try_from_batch_accepts_multi_bit_rq_split_codes() {
        let original_codes = make_test_codes(50, 64);
        let code_dim = original_codes.value_length() as usize * 8;
        let ex_codes = make_test_ex_codes(original_codes.len(), code_dim, 9);
        let mut metadata = make_test_metadata(code_dim);
        metadata.num_bits = 9;

        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch_with_ex(original_codes, ex_codes),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();

        assert!(storage.metadata().packed);
        // Legacy batches are normalized to the blocked column at load.
        let stored_batch = storage.to_batches().unwrap().next().unwrap();
        assert!(stored_batch.column_by_name(RABIT_EX_CODE_COLUMN).is_none());
        assert_eq!(
            stored_batch[RABIT_BLOCKED_EX_CODE_COLUMN]
                .as_fixed_size_list()
                .value_length(),
            64
        );
        assert!(stored_batch.column_by_name(ERROR_FACTORS_COLUMN).is_some());
    }

    #[test]
    fn test_try_from_batch_accepts_missing_error_factors_for_compatibility() {
        let original_codes = make_test_codes(50, 64);
        let code_dim = original_codes.value_length() as usize * 8;
        let ex_codes = make_test_ex_codes(original_codes.len(), code_dim, 9);
        let mut metadata = make_test_metadata(code_dim);
        metadata.num_bits = 9;
        let batch = make_test_batch_with_ex(original_codes, ex_codes)
            .drop_column(ERROR_FACTORS_COLUMN)
            .unwrap();

        let storage =
            RabitQuantizationStorage::try_from_batch(batch, &metadata, DistanceType::L2, None)
                .unwrap();
        let query = Arc::new(Float32Array::from(vec![1.0; code_dim])) as ArrayRef;
        let calc = storage.dist_calculator(query, 4.0);

        assert!(storage.error_factors.is_none());
        assert_eq!(
            calc.raw_query_lower_bound_gating_disabled_reason(),
            Some("missing_error_factors")
        );
    }

    #[test]
    fn test_remap_preserves_packed_rq_storage_layout() {
        let original_codes = make_test_codes(50, 64);
        let metadata = make_test_metadata(original_codes.value_length() as usize * 8);
        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch(original_codes.clone()),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();

        let mut mapping = HashMap::new();
        mapping.insert(1, Some(101));
        mapping.insert(3, None);
        mapping.insert(4, Some(104));

        let remapped = storage.remap(&RowAddrRemap::direct(mapping)).unwrap();
        assert!(remapped.metadata().packed);

        let remapped_batch = remapped.to_batches().unwrap().next().unwrap();
        let remapped_row_ids = remapped_batch[ROW_ID].as_primitive::<UInt64Type>().values();
        let expected_row_ids = UInt64Array::from_iter_values(
            [0, 101, 2, 104]
                .into_iter()
                .chain(5..original_codes.len() as u64),
        );
        assert_eq!(remapped_row_ids, expected_row_ids.values());

        let remapped_codes = remapped_batch[RABIT_CODE_COLUMN].as_fixed_size_list();
        let repacked = pack_codes(&unpack_codes(remapped_codes));
        assert_codes_eq(remapped_codes, &repacked);
    }

    #[test]
    fn test_remap_preserves_multi_bit_rq_split_columns() {
        // num_bits=9 keeps sequential ex codes; num_bits 4/6/8 (ex_bits
        // 3/5/7) also exercise the bit-plane repack rebuild in `remap`.
        for num_bits in [4, 6, 8, 9u8] {
            test_remap_preserves_multi_bit_rq_split_columns_impl(num_bits);
        }
    }

    fn test_remap_preserves_multi_bit_rq_split_columns_impl(num_bits: u8) {
        let original_codes = make_test_codes(50, 64);
        let code_dim = original_codes.value_length() as usize * 8;
        let ex_codes = make_test_ex_codes(original_codes.len(), code_dim, num_bits);
        let mut metadata = make_test_metadata(code_dim);
        metadata.num_bits = num_bits;
        let storage = RabitQuantizationStorage::try_from_batch(
            make_test_batch_with_ex(original_codes.clone(), ex_codes),
            &metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();

        let mut mapping = HashMap::new();
        mapping.insert(1, Some(101));
        mapping.insert(3, None);
        mapping.insert(4, Some(104));

        let remapped = storage.remap(&RowAddrRemap::direct(mapping)).unwrap();
        let remapped_batch = remapped.to_batches().unwrap().next().unwrap();
        let remapped_row_ids = remapped_batch[ROW_ID].as_primitive::<UInt64Type>().values();
        let expected_row_ids = UInt64Array::from_iter_values(
            [0, 101, 2, 104]
                .into_iter()
                .chain(5..original_codes.len() as u64),
        );
        assert_eq!(remapped_row_ids, expected_row_ids.values());

        // Legacy batches are normalized to the blocked format at load, so the
        // remapped batch carries the blocked column.
        let ex_code_len = blocked_ex_code_bytes(code_dim, rabit_ex_bits(num_bits).unwrap());
        assert_eq!(
            remapped_batch[RABIT_BLOCKED_EX_CODE_COLUMN]
                .as_fixed_size_list()
                .value_length(),
            ex_code_len as i32
        );
        assert_eq!(
            &remapped_batch[EX_ADD_FACTORS_COLUMN]
                .as_primitive::<Float32Type>()
                .values()[..5],
            &[10.5, 11.5, 12.5, 14.5, 15.5]
        );
        assert_eq!(
            &remapped_batch[EX_SCALE_FACTORS_COLUMN]
                .as_primitive::<Float32Type>()
                .values()[..5],
            &[1.5, 2.5, 3.5, 5.5, 6.5]
        );
        assert_eq!(
            &remapped_batch[ERROR_FACTORS_COLUMN]
                .as_primitive::<Float32Type>()
                .values()[..5],
            &[0.25, 1.25, 2.25, 4.25, 5.25]
        );

        // The remapped storage must hold the same kernel-layout ex codes as a
        // storage freshly loaded from the remapped batch.
        let reloaded = RabitQuantizationStorage::try_from_batch(
            remapped_batch,
            &remapped.metadata,
            DistanceType::L2,
            None,
        )
        .unwrap();
        assert_eq!(remapped.ex_codes, reloaded.ex_codes);
        assert_eq!(
            remapped.ex_codes.as_ref().unwrap().value_length() as usize,
            blocked_ex_code_bytes(code_dim, rabit_ex_bits(num_bits).unwrap())
        );
    }
}
