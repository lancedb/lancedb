// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Flat Vector Index.
//!

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::collections::BinaryHeap;
use std::sync::Arc;

use arrow::array::AsArray;
use arrow_array::{Array, ArrayRef, Float32Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, ROW_ID_FIELD, Result};
use lance_file::versions::v1::reader::FileReader as V1FileReader;
use lance_linalg::distance::DistanceType;
use serde::{Deserialize, Serialize};

use crate::{
    metrics::MetricsCollector,
    prefilter::PreFilter,
    vector::{
        ApproxMode, DIST_COL, Query,
        graph::{OrderedFloat, OrderedNode},
        quantizer::{Quantization, QuantizationType, Quantizer, QuantizerMetadata},
        storage::{
            DistCalculator, DistanceCalculatorOptions, QueryResidual, QueryScratch, VectorStore,
        },
        v3::subindex::IvfSubIndex,
    },
};

use super::storage::{FLAT_COLUMN, FlatBinStorage, FlatFloatStorage};

#[inline(always)]
fn push_candidate_local(
    res: &mut BinaryHeap<OrderedNode<u64>>,
    k: usize,
    row_id: u64,
    dist: OrderedFloat,
) {
    if k == 0 {
        return;
    }
    if res.len() < k {
        res.push(OrderedNode::new(row_id, dist));
    } else if res.peek().is_some_and(|node| node.dist > dist) {
        res.pop();
        res.push(OrderedNode::new(row_id, dist));
    }
}

/// A Flat index is any index that stores no metadata, and
/// during query, it simply scans over the storage and returns the top k results
#[derive(Debug, Clone, Default, DeepSizeOf)]
pub struct FlatIndex {}

use std::sync::LazyLock;

static ANN_SEARCH_SCHEMA: LazyLock<SchemaRef> = LazyLock::new(|| {
    Schema::new(vec![
        Field::new(DIST_COL, DataType::Float32, true),
        ROW_ID_FIELD.clone(),
    ])
    .into()
});

#[derive(Default)]
pub struct FlatQueryParams {
    lower_bound: Option<f32>,
    upper_bound: Option<f32>,
    dist_q_c: f32,
    approx_mode: ApproxMode,
}

impl From<&Query> for FlatQueryParams {
    fn from(q: &Query) -> Self {
        Self {
            lower_bound: q.lower_bound,
            upper_bound: q.upper_bound,
            dist_q_c: q.dist_q_c,
            approx_mode: q.approx_mode,
        }
    }
}

impl IvfSubIndex for FlatIndex {
    type QueryParams = FlatQueryParams;
    type BuildParams = ();

    fn name() -> &'static str {
        "FLAT"
    }

    fn metadata_key() -> &'static str {
        "lance:flat"
    }

    fn schema() -> arrow_schema::SchemaRef {
        Schema::new(vec![Field::new("__flat_marker", DataType::UInt64, false)]).into()
    }

    fn search(
        &self,
        query: ArrayRef,
        k: usize,
        params: Self::QueryParams,
        storage: &impl VectorStore,
        prefilter: Arc<dyn PreFilter>,
        metrics: &dyn MetricsCollector,
    ) -> Result<RecordBatch> {
        let mut scratch = QueryScratch::new();
        self.search_with_scratch(
            query,
            k,
            params,
            storage,
            prefilter,
            metrics,
            None,
            &mut scratch,
        )
    }

    fn search_with_scratch(
        &self,
        query: ArrayRef,
        k: usize,
        params: Self::QueryParams,
        storage: &impl VectorStore,
        prefilter: Arc<dyn PreFilter>,
        metrics: &dyn MetricsCollector,
        residual: Option<QueryResidual<'_>>,
        scratch: &mut QueryScratch,
    ) -> Result<RecordBatch> {
        let is_range_query = params.lower_bound.is_some() || params.upper_bound.is_some();
        let row_ids = storage.row_ids();
        let dist_calc = storage.dist_calculator_with_scratch(
            query,
            params.dist_q_c,
            residual,
            &mut scratch.query_f32,
            DistanceCalculatorOptions {
                approx_mode: params.approx_mode,
            },
        );
        let mut res = BinaryHeap::with_capacity(k);
        metrics.record_comparisons(storage.len());

        match prefilter.is_empty() {
            true => {
                dist_calc.distance_all_with_scratch(
                    k,
                    &mut scratch.distances,
                    &mut scratch.u16,
                    &mut scratch.u8,
                    &mut scratch.u32,
                );
                let dists = scratch.distances.iter().copied();

                if is_range_query {
                    let lower_bound = params.lower_bound.unwrap_or(f32::MIN).into();
                    let upper_bound = params.upper_bound.unwrap_or(f32::MAX).into();

                    for (&row_id, dist) in row_ids.zip(dists) {
                        let dist = dist.into();
                        if dist < lower_bound || dist >= upper_bound {
                            continue;
                        }
                        push_candidate_local(&mut res, k, row_id, dist);
                    }
                } else {
                    for (&row_id, dist) in row_ids.zip(dists) {
                        let dist = dist.into();
                        push_candidate_local(&mut res, k, row_id, dist);
                    }
                }
            }
            false => {
                let row_addr_mask = prefilter.mask();
                if is_range_query {
                    let lower_bound = params.lower_bound.unwrap_or(f32::MIN).into();
                    let upper_bound = params.upper_bound.unwrap_or(f32::MAX).into();
                    for (id, &row_addr) in row_ids.enumerate() {
                        if !row_addr_mask.selected(row_addr) {
                            continue;
                        }
                        let dist = dist_calc.distance(id as u32).into();
                        if dist < lower_bound || dist >= upper_bound {
                            continue;
                        }

                        push_candidate_local(&mut res, k, row_addr, dist);
                    }
                } else {
                    for (id, &row_addr) in row_ids.enumerate() {
                        if !row_addr_mask.selected(row_addr) {
                            continue;
                        }

                        let dist = dist_calc.distance(id as u32).into();
                        push_candidate_local(&mut res, k, row_addr, dist);
                    }
                }
            }
        };

        // we don't need to sort the results by distances here
        // because there's a SortExec node in the query plan which sorts the results from all partitions
        let (row_ids, dists): (Vec<_>, Vec<_>) = res.into_iter().map(|r| (r.id, r.dist.0)).unzip();
        let (row_ids, dists) = (UInt64Array::from(row_ids), Float32Array::from(dists));

        Ok(RecordBatch::try_new(
            ANN_SEARCH_SCHEMA.clone(),
            vec![Arc::new(dists), Arc::new(row_ids)],
        )?)
    }

    fn supports_global_topk_heap() -> bool {
        true
    }

    fn accumulate_topk(
        &self,
        query: ArrayRef,
        k: usize,
        params: Self::QueryParams,
        storage: &impl VectorStore,
        prefilter: Arc<dyn PreFilter>,
        res: &mut BinaryHeap<OrderedNode<u64>>,
        metrics: &dyn MetricsCollector,
    ) -> Result<()> {
        let mut scratch = QueryScratch::new();
        self.accumulate_topk_with_scratch(
            query,
            k,
            params,
            storage,
            prefilter,
            res,
            None,
            &mut scratch,
            metrics,
        )
    }

    fn accumulate_topk_with_scratch(
        &self,
        query: ArrayRef,
        k: usize,
        params: Self::QueryParams,
        storage: &impl VectorStore,
        prefilter: Arc<dyn PreFilter>,
        res: &mut BinaryHeap<OrderedNode<u64>>,
        residual: Option<QueryResidual<'_>>,
        scratch: &mut QueryScratch,
        metrics: &dyn MetricsCollector,
    ) -> Result<()> {
        let row_ids = storage.row_ids();
        let dist_calc = storage.dist_calculator_with_scratch(
            query,
            params.dist_q_c,
            residual,
            &mut scratch.query_f32,
            DistanceCalculatorOptions {
                approx_mode: params.approx_mode,
            },
        );
        metrics.record_comparisons(storage.len());

        match prefilter.is_empty() {
            true => {
                dist_calc.accumulate_topk_with_scratch(
                    k,
                    params.lower_bound,
                    params.upper_bound,
                    |id| storage.row_id(id),
                    res,
                    &mut scratch.distances,
                    &mut scratch.u16,
                    &mut scratch.u8,
                    &mut scratch.u32,
                );
            }
            false => {
                let row_addr_mask = prefilter.mask();
                dist_calc.accumulate_filtered_topk_with_scratch(
                    k,
                    params.lower_bound,
                    params.upper_bound,
                    row_ids.enumerate().map(|(id, &row_id)| (id as u32, row_id)),
                    |row_id| row_addr_mask.selected(row_id),
                    res,
                    &mut scratch.distances,
                    &mut scratch.u16,
                    &mut scratch.u8,
                    &mut scratch.u32,
                );
            }
        };
        Ok(())
    }

    fn load(_: RecordBatch) -> Result<Self> {
        Ok(Self {})
    }

    fn index_vectors(_: &impl VectorStore, _: Self::BuildParams) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(Self {})
    }

    fn remap(&self, _: &RowAddrRemap, _: &impl VectorStore) -> Result<Self> {
        Ok(self.clone())
    }

    fn to_batch(&self) -> Result<RecordBatch> {
        Ok(RecordBatch::new_empty(Schema::empty().into()))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, DeepSizeOf)]
pub struct FlatMetadata {
    pub dim: usize,
}

#[async_trait::async_trait]
impl QuantizerMetadata for FlatMetadata {
    async fn load(_: &V1FileReader) -> Result<Self> {
        unimplemented!("Flat will be used in new index builder which doesn't require this")
    }
}

#[derive(Debug, Clone, DeepSizeOf)]
pub struct FlatQuantizer {
    dim: usize,
    distance_type: DistanceType,
}

impl FlatQuantizer {
    pub fn new(dim: usize, distance_type: DistanceType) -> Self {
        Self { dim, distance_type }
    }
}

impl Quantization for FlatQuantizer {
    type BuildParams = ();
    type Metadata = FlatMetadata;
    type Storage = FlatFloatStorage;

    fn build(data: &dyn Array, distance_type: DistanceType, _: &Self::BuildParams) -> Result<Self> {
        let dim = data.as_fixed_size_list().value_length();
        Ok(Self::new(dim as usize, distance_type))
    }

    fn retrain(&mut self, _: &dyn Array) -> Result<()> {
        Ok(())
    }

    fn code_dim(&self) -> usize {
        self.dim
    }

    fn column(&self) -> &'static str {
        FLAT_COLUMN
    }

    fn from_metadata(metadata: &Self::Metadata, distance_type: DistanceType) -> Result<Quantizer> {
        Ok(Quantizer::Flat(Self {
            dim: metadata.dim,
            distance_type,
        }))
    }

    fn metadata(&self, _: Option<crate::vector::quantizer::QuantizationMetadata>) -> FlatMetadata {
        FlatMetadata { dim: self.dim }
    }

    fn metadata_key() -> &'static str {
        "flat"
    }

    fn quantization_type() -> QuantizationType {
        QuantizationType::Flat
    }

    fn quantize(&self, vectors: &dyn Array) -> Result<ArrayRef> {
        Ok(vectors.slice(0, vectors.len()))
    }

    fn field(&self) -> Field {
        Field::new(
            FLAT_COLUMN,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::Float32, true)),
                self.dim as i32,
            ),
            true,
        )
    }
}

impl From<FlatQuantizer> for Quantizer {
    fn from(value: FlatQuantizer) -> Self {
        Self::Flat(value)
    }
}

impl TryFrom<Quantizer> for FlatQuantizer {
    type Error = Error;

    fn try_from(value: Quantizer) -> Result<Self> {
        match value {
            Quantizer::Flat(quantizer) => Ok(quantizer),
            _ => Err(Error::invalid_input("quantizer is not FlatQuantizer")),
        }
    }
}

#[derive(Debug, Clone, DeepSizeOf)]
pub struct FlatBinQuantizer {
    dim: usize,
    distance_type: DistanceType,
}

impl FlatBinQuantizer {
    pub fn new(dim: usize, distance_type: DistanceType) -> Self {
        Self { dim, distance_type }
    }
}

impl Quantization for FlatBinQuantizer {
    type BuildParams = ();
    type Metadata = FlatMetadata;
    type Storage = FlatBinStorage;

    fn build(data: &dyn Array, distance_type: DistanceType, _: &Self::BuildParams) -> Result<Self> {
        let dim = data.as_fixed_size_list().value_length();
        Ok(Self::new(dim as usize, distance_type))
    }

    fn retrain(&mut self, _: &dyn Array) -> Result<()> {
        Ok(())
    }

    fn code_dim(&self) -> usize {
        self.dim
    }

    fn column(&self) -> &'static str {
        FLAT_COLUMN
    }

    fn from_metadata(metadata: &Self::Metadata, distance_type: DistanceType) -> Result<Quantizer> {
        Ok(Quantizer::FlatBin(Self {
            dim: metadata.dim,
            distance_type,
        }))
    }

    fn metadata(&self, _: Option<crate::vector::quantizer::QuantizationMetadata>) -> FlatMetadata {
        FlatMetadata { dim: self.dim }
    }

    fn metadata_key() -> &'static str {
        "flat"
    }

    fn quantization_type() -> QuantizationType {
        QuantizationType::FlatBin
    }

    fn quantize(&self, vectors: &dyn Array) -> Result<ArrayRef> {
        Ok(vectors.slice(0, vectors.len()))
    }

    fn field(&self) -> Field {
        Field::new(
            FLAT_COLUMN,
            DataType::FixedSizeList(
                Arc::new(Field::new("item", DataType::UInt8, true)),
                self.dim as i32,
            ),
            true,
        )
    }
}

impl From<FlatBinQuantizer> for Quantizer {
    fn from(value: FlatBinQuantizer) -> Self {
        Self::FlatBin(value)
    }
}

impl TryFrom<Quantizer> for FlatBinQuantizer {
    type Error = Error;

    fn try_from(value: Quantizer) -> Result<Self> {
        match value {
            Quantizer::FlatBin(quantizer) => Ok(quantizer),
            _ => Err(Error::invalid_input("quantizer is not FlatBinQuantizer")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::FixedSizeListArray;
    use async_trait::async_trait;
    use lance_arrow::FixedSizeListArrayExt;
    use lance_select::{RowAddrMask, RowAddrTreeMap};

    use crate::metrics::NoOpMetricsCollector;
    use crate::prefilter::NoFilter;

    struct MaskPreFilter {
        mask: Arc<RowAddrMask>,
    }

    #[async_trait]
    impl PreFilter for MaskPreFilter {
        async fn wait_for_ready(&self) -> Result<()> {
            Ok(())
        }

        fn is_empty(&self) -> bool {
            false
        }

        fn mask(&self) -> Arc<RowAddrMask> {
            self.mask.clone()
        }

        fn filter_row_ids<'a>(&self, row_ids: Box<dyn Iterator<Item = &'a u64> + 'a>) -> Vec<u64> {
            self.mask.selected_indices(row_ids)
        }
    }

    fn test_storage() -> FlatFloatStorage {
        let values = Float32Array::from(vec![
            0.0, 0.0, // row 0
            1.0, 0.0, // row 1
            1.0, 1.0, // row 2
            3.0, 3.0, // row 3
            4.0, 4.0, // row 4
        ]);
        let vectors = FixedSizeListArray::try_new_from_values(values, 2).unwrap();
        FlatFloatStorage::new(vectors, DistanceType::L2)
    }

    fn query() -> ArrayRef {
        Arc::new(Float32Array::from(vec![1.0, 1.0]))
    }

    fn batch_results(batch: RecordBatch) -> Vec<(u64, f32)> {
        let dists = batch
            .column(0)
            .as_primitive::<arrow_array::types::Float32Type>();
        let row_ids = batch
            .column(1)
            .as_primitive::<arrow_array::types::UInt64Type>();
        let mut results = row_ids
            .values()
            .iter()
            .zip(dists.values().iter())
            .map(|(row_id, dist)| (*row_id, *dist))
            .collect::<Vec<_>>();
        results.sort_by_key(|left| left.0);
        results
    }

    fn heap_results(heap: BinaryHeap<OrderedNode<u64>>) -> Vec<(u64, f32)> {
        let mut results = heap
            .into_iter()
            .map(|node| (node.id, node.dist.0))
            .collect::<Vec<_>>();
        results.sort_by_key(|left| left.0);
        results
    }

    #[test]
    fn test_flat_search_matches_accumulate_topk_without_prefilter() {
        let index = FlatIndex::default();
        let storage = test_storage();
        let k = 3;
        let search_results = batch_results(
            index
                .search(
                    query(),
                    k,
                    FlatQueryParams::default(),
                    &storage,
                    Arc::new(NoFilter),
                    &NoOpMetricsCollector,
                )
                .unwrap(),
        );

        let mut heap = BinaryHeap::with_capacity(k);
        index
            .accumulate_topk(
                query(),
                k,
                FlatQueryParams::default(),
                &storage,
                Arc::new(NoFilter),
                &mut heap,
                &NoOpMetricsCollector,
            )
            .unwrap();

        assert_eq!(search_results, heap_results(heap));
    }

    #[test]
    fn test_flat_search_matches_accumulate_topk_with_prefilter() {
        let index = FlatIndex::default();
        let storage = test_storage();
        let k = 2;
        let filter = Arc::new(MaskPreFilter {
            mask: Arc::new(RowAddrMask::from_allowed(RowAddrTreeMap::from_iter([
                0_u64, 3, 4,
            ]))),
        });
        let search_results = batch_results(
            index
                .search(
                    query(),
                    k,
                    FlatQueryParams::default(),
                    &storage,
                    filter.clone(),
                    &NoOpMetricsCollector,
                )
                .unwrap(),
        );

        let mut heap = BinaryHeap::with_capacity(k);
        index
            .accumulate_topk(
                query(),
                k,
                FlatQueryParams::default(),
                &storage,
                filter,
                &mut heap,
                &NoOpMetricsCollector,
            )
            .unwrap();

        assert_eq!(search_results, heap_results(heap));
        assert_eq!(
            search_results.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![0, 3]
        );
    }
}
