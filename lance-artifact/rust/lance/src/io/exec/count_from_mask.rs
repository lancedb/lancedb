// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Execute-time half of the count-from-mask category of aggregate pushdown.
//!
//! [`CountFromMaskExec`] computes a `COUNT(*)`-style aggregate's partial
//! state directly from index/manifest metadata, without scanning column
//! data. Conceptually:
//!
//! ```text
//! result = | fragments_allow ∩ optional_prefilter_mask − deletion_mask |
//! ```
//!
//! Its output schema matches what `AggregateExec(AggregateMode::Partial)`
//! would produce for the same `COUNT` aggregates, so a downstream
//! `AggregateExec(Final)` can combine the result unchanged.
//!
//! This is one of four categories of aggregate acceleration we plan to
//! support; the others (mask-to-answer, zone-aware, dimension-keyed) each
//! need additional plumbing — see the corresponding design issue.

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::{Array, BinaryArray, Int64Array, RecordBatch};
use arrow_schema::{Schema, SchemaRef};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    execution_plan::{Boundedness, EmissionType},
    metrics::{ExecutionPlanMetricsSet, MetricsSet},
};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_expr::aggregate::AggregateFunctionExpr;
use futures::{StreamExt, TryStreamExt, stream};
use lance_core::{Error, Result};
use lance_select::{RowAddrMask, RowAddrSelection, RowAddrTreeMap};
use lance_table::format::Fragment;
use roaring::RoaringBitmap;
use tracing::instrument;

use super::utils::InstrumentedRecordBatchStreamAdapter;
use crate::Dataset;
use crate::dataset::rowids::load_row_id_sequences;
use crate::index::prefilter::DatasetPreFilter;

/// An execution node that computes a `COUNT(*)`-style aggregate from an
/// optional row-address mask supplied by an upstream scalar-index search,
/// combined with the dataset's deletion mask and an optional restriction to
/// a fragment subset.
///
/// The node returns one record batch with one row whose columns are the
/// partial-state representation of each `COUNT` in `aggregate_funcs` — i.e.
/// the same shape an `AggregateExec(Partial)` would emit.
#[derive(Debug)]
pub struct CountFromMaskExec {
    dataset: Arc<Dataset>,
    /// One per output column. Used only for `state_fields()` to build the
    /// output schema; the actual count is computed identically for all of
    /// them since every entry is a non-distinct `COUNT(<literal>)`.
    aggregate_funcs: Vec<Arc<AggregateFunctionExpr>>,
    /// Optional [`super::scalar_index::ScalarIndexExec`] producing the row-
    /// address mask to count.
    prefilter_input: Option<Arc<dyn ExecutionPlan>>,
    /// Restrict the count to this fragment subset. `None` means "every
    /// fragment in the dataset." The optimizer rule uses this to scope the
    /// pushdown branch of a partial-coverage split plan to the indexed
    /// fragments only — the uncovered fragments are handled by a parallel
    /// scan branch.
    restrict_to_fragments: Option<RoaringBitmap>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for CountFromMaskExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "CountFromMask")
            }
            DisplayFormatType::TreeRender => write!(f, "CountFromMask"),
        }
    }
}

impl CountFromMaskExec {
    /// Build a new node.
    ///
    /// `aggregate_funcs` must be a non-empty set of non-distinct `COUNT`
    /// aggregates (the optimizer rule guarantees this). `prefilter_input`,
    /// if present, must produce a single batch in the scalar-index result
    /// schema; that mask is intersected with the dataset's covered
    /// fragments and the active deletion mask.
    pub fn try_new(
        dataset: Arc<Dataset>,
        aggregate_funcs: Vec<Arc<AggregateFunctionExpr>>,
        prefilter_input: Option<Arc<dyn ExecutionPlan>>,
    ) -> Result<Self> {
        Self::try_new_restricted(dataset, aggregate_funcs, prefilter_input, None)
    }

    /// Like [`Self::try_new`] but scopes the count to a fragment subset
    /// rather than the whole dataset. The optimizer rule uses this for the
    /// pushdown branch of a partial-coverage split plan, so the count only
    /// covers the fragments the prefilter's index can answer for.
    pub fn try_new_restricted(
        dataset: Arc<Dataset>,
        aggregate_funcs: Vec<Arc<AggregateFunctionExpr>>,
        prefilter_input: Option<Arc<dyn ExecutionPlan>>,
        restrict_to_fragments: Option<RoaringBitmap>,
    ) -> Result<Self> {
        if aggregate_funcs.is_empty() {
            return Err(Error::invalid_input(
                "CountFromMaskExec requires at least one aggregate".to_string(),
            ));
        }

        let state_fields = aggregate_funcs
            .iter()
            .map(|agg| agg.state_fields())
            .collect::<datafusion::error::Result<Vec<_>>>()
            .map_err(|e| Error::invalid_input(e.to_string()))?
            .into_iter()
            .flatten()
            .collect::<Vec<_>>();
        let state_fields_owned: Vec<arrow_schema::Field> =
            state_fields.iter().map(|f| f.as_ref().clone()).collect();
        let schema: SchemaRef = Arc::new(Schema::new(state_fields_owned));

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::RoundRobinBatch(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            dataset,
            aggregate_funcs,
            prefilter_input,
            restrict_to_fragments,
            schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    /// Drain `prefilter_input` (a [`super::scalar_index::ScalarIndexExec`])
    /// to produce the row-address mask it serialized.
    async fn load_prefilter(
        prefilter_input: Arc<dyn ExecutionPlan>,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<RowAddrMask> {
        let mut stream = prefilter_input.execute(0, context).map_err(Error::from)?;
        let batch = stream
            .try_next()
            .await
            .map_err(Error::from)?
            .ok_or_else(|| {
                Error::internal(
                    "CountFromMaskExec: prefilter input produced no batches".to_string(),
                )
            })?;
        // Drain any remaining batches so the upstream sees a clean shutdown.
        while stream.try_next().await.map_err(Error::from)?.is_some() {}

        let result_col = batch
            .column(0)
            .as_any()
            .downcast_ref::<BinaryArray>()
            .ok_or_else(|| {
                Error::internal(format!(
                    "CountFromMaskExec: prefilter result column has type {:?}, expected Binary",
                    batch.column(0).data_type()
                ))
            })?;
        RowAddrMask::from_arrow(result_col)
    }

    /// Fold the prefilter, fragment allow list, and deletion mask into a
    /// single `AllowList`-shaped [`RowAddrMask`] suitable for counting.
    fn combine_masks(
        fragments_allow: RowAddrTreeMap,
        prefilter: Option<RowAddrMask>,
        deletion_mask: Option<Arc<RowAddrMask>>,
    ) -> RowAddrMask {
        let base = RowAddrMask::AllowList(fragments_allow);
        let after_prefilter = match prefilter {
            None => base,
            Some(prefilter) => base & prefilter,
        };
        match deletion_mask {
            None => after_prefilter,
            Some(deletion_mask) => after_prefilter & (*deletion_mask).clone(),
        }
    }

    /// Count the rows selected by `mask`, looking up `Full`-marker fragments
    /// in the manifest so we never need to materialize a
    /// `RoaringBitmap::full()`.
    fn count_from_mask(mask: &RowAddrMask, dataset: &Dataset) -> Result<i64> {
        let allow = mask.allow_list().ok_or_else(|| {
            Error::internal("CountFromMaskExec: combined mask is not an AllowList".to_string())
        })?;
        let frag_map: HashMap<u32, &Fragment> = dataset
            .fragments()
            .iter()
            .map(|f| (f.id as u32, f))
            .collect();
        let mut count = 0i64;
        for (frag_id, sel) in allow.iter() {
            match sel {
                RowAddrSelection::Full => {
                    // The fragment is in the allow list with no deletions
                    // touching it — its row count is the physical row count.
                    let frag = frag_map.get(frag_id).ok_or_else(|| {
                        Error::internal(format!(
                            "CountFromMaskExec: fragment {} not found in manifest",
                            frag_id
                        ))
                    })?;
                    let n = frag.physical_rows.ok_or_else(|| {
                        Error::internal(format!(
                            "CountFromMaskExec: physical_rows missing for fragment {}",
                            frag_id
                        ))
                    })?;
                    count += n as i64;
                }
                RowAddrSelection::Partial(bitmap) => {
                    count += bitmap.len() as i64;
                }
            }
        }
        Ok(count)
    }

    /// Row-address-space fragments-allow list: concrete `[0..physical_rows)`
    /// ranges per covered fragment.
    ///
    /// Concrete ranges, not `Full` markers: subtracting a `BlockList` from a
    /// `Full` entry materializes a `RoaringBitmap::full()` (2^32) per fragment,
    /// which is slow and throws off `len()`.
    fn address_fragments_allow(
        dataset: &Dataset,
        fragments_covered: &RoaringBitmap,
    ) -> Result<RowAddrTreeMap> {
        let frag_map: HashMap<u32, &Fragment> = dataset
            .fragments()
            .iter()
            .map(|f| (f.id as u32, f))
            .collect();
        let mut fragments_allow = RowAddrTreeMap::new();
        for frag_id in fragments_covered.iter() {
            let frag = frag_map.get(&frag_id).ok_or_else(|| {
                Error::internal(format!(
                    "CountFromMaskExec: fragment {} not in manifest",
                    frag_id
                ))
            })?;
            let physical = frag.physical_rows.ok_or_else(|| {
                Error::internal(format!(
                    "CountFromMaskExec: physical_rows missing for fragment {}",
                    frag_id
                ))
            })?;
            let mut bitmap = RoaringBitmap::new();
            bitmap.insert_range(0u32..(physical as u32));
            fragments_allow.insert_bitmap(frag_id, bitmap);
        }
        Ok(fragments_allow)
    }

    /// Live (non-deleted) row count of the covered fragments, from fragment
    /// metadata. Used for an unfiltered count: no prefilter to intersect, so no
    /// need to build the stable-id universe.
    async fn count_live_rows(dataset: &Dataset, fragments_covered: &RoaringBitmap) -> Result<i64> {
        let frags = dataset
            .get_fragments()
            .into_iter()
            .filter(|f| fragments_covered.contains(f.id() as u32));
        let counts = stream::iter(frags)
            .map(|f| async move { f.count_rows(None).await })
            .buffer_unordered(dataset.object_store.as_ref().io_parallelism())
            .try_collect::<Vec<_>>()
            .await?;
        Ok(counts.iter().sum::<usize>() as i64)
    }

    /// Count universe in stable-id space: live stable row ids whose current home
    /// is in `fragments_covered`. Staying in stable-id space lets it intersect
    /// the index prefilter directly; deletions are already folded in, so the
    /// caller passes no separate deletion mask.
    async fn stable_id_universe(
        dataset: &Arc<Dataset>,
        fragments_covered: RoaringBitmap,
    ) -> Result<RowAddrTreeMap> {
        // create_restricted_deletion_mask gives a live-id allow list restricted
        // to `fragments_covered`. It returns None only with no deletions and full
        // coverage — then the universe is every stable id, loaded below.
        if let Some(fut) = DatasetPreFilter::create_restricted_deletion_mask(
            dataset.clone(),
            fragments_covered.clone(),
        ) {
            let mask = fut.await?;
            return mask.allow_list().cloned().ok_or_else(|| {
                Error::internal(
                    "CountFromMaskExec: stable-row-id deletion mask must be an AllowList"
                        .to_string(),
                )
            });
        }
        Self::load_stable_id_universe(dataset, &fragments_covered).await
    }

    /// Every stable row id in the covered fragments, from their row-id sequences
    /// (metadata, not column data). Only used with no deletions and full coverage.
    async fn load_stable_id_universe(
        dataset: &Dataset,
        fragments_covered: &RoaringBitmap,
    ) -> Result<RowAddrTreeMap> {
        let frags: Vec<Fragment> = dataset
            .fragments()
            .iter()
            .filter(|f| fragments_covered.contains(f.id as u32))
            .cloned()
            .collect();
        let mut sequences = load_row_id_sequences(dataset, &frags);
        let mut universe = RowAddrTreeMap::new();
        while let Some((_frag_id, sequence)) = sequences.try_next().await? {
            universe |= RowAddrTreeMap::from(sequence.as_ref());
        }
        Ok(universe)
    }

    #[instrument(name = "count_from_mask", skip_all, level = "debug")]
    async fn do_execute(
        dataset: Arc<Dataset>,
        aggregate_funcs_len: usize,
        prefilter_input: Option<Arc<dyn ExecutionPlan>>,
        restrict_to_fragments: Option<RoaringBitmap>,
        context: Arc<datafusion::execution::context::TaskContext>,
        schema: SchemaRef,
    ) -> Result<RecordBatch> {
        let prefilter = match prefilter_input {
            None => None,
            Some(input) => Some(Self::load_prefilter(input, context.clone()).await?),
        };

        // Anchor the deletion mask against either every dataset fragment or
        // the caller-supplied restricted subset.
        let dataset_fragments: RoaringBitmap =
            dataset.fragments().iter().map(|f| f.id as u32).collect();
        let fragments_covered = match restrict_to_fragments {
            Some(restrict) => dataset_fragments & restrict,
            None => dataset_fragments,
        };

        // Under stable row ids the prefilter and deletion masks are in stable-id
        // space, so the universe must be too (see `stable_id_universe`); the
        // default path builds it in row-address space.
        let count = if dataset.manifest.uses_stable_row_ids() {
            match prefilter {
                // No prefilter: just the live row count, from metadata.
                None => Self::count_live_rows(&dataset, &fragments_covered).await?,
                Some(prefilter) => {
                    let universe = Self::stable_id_universe(&dataset, fragments_covered).await?;
                    let combined = Self::combine_masks(universe, Some(prefilter), None);
                    Self::count_from_mask(&combined, dataset.as_ref())?
                }
            }
        } else {
            let fragments_allow = Self::address_fragments_allow(&dataset, &fragments_covered)?;
            // Load the deletion mask for the covered fragments.
            let deletion_mask =
                match DatasetPreFilter::create_deletion_mask(dataset.clone(), fragments_covered) {
                    Some(fut) => Some(fut.await?),
                    None => None,
                };
            let combined = Self::combine_masks(fragments_allow, prefilter, deletion_mask);
            Self::count_from_mask(&combined, dataset.as_ref())?
        };

        // Every aggregate is the same non-distinct COUNT shape — emit the
        // count once per output column.
        let arrays: Vec<Arc<dyn Array>> = (0..aggregate_funcs_len)
            .map(|_| Arc::new(Int64Array::from(vec![count])) as Arc<dyn Array>)
            .collect();
        Ok(RecordBatch::try_new(schema, arrays)?)
    }
}

impl ExecutionPlan for CountFromMaskExec {
    fn name(&self) -> &str {
        "CountFromMaskExec"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        match &self.prefilter_input {
            Some(input) => vec![input],
            None => vec![],
        }
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        let prefilter_input = match children.len() {
            0 => None,
            1 => Some(children.into_iter().next().unwrap()),
            n => {
                return Err(datafusion::error::DataFusionError::Internal(format!(
                    "CountFromMaskExec accepts 0 or 1 children, got {}",
                    n
                )));
            }
        };
        Ok(Arc::new(Self {
            dataset: self.dataset.clone(),
            aggregate_funcs: self.aggregate_funcs.clone(),
            prefilter_input,
            restrict_to_fragments: self.restrict_to_fragments.clone(),
            schema: self.schema.clone(),
            properties: self.properties.clone(),
            metrics: self.metrics.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> datafusion::error::Result<datafusion::physical_plan::SendableRecordBatchStream> {
        let schema = self.schema.clone();
        let batch_fut = Self::do_execute(
            self.dataset.clone(),
            self.aggregate_funcs.len(),
            self.prefilter_input.clone(),
            self.restrict_to_fragments.clone(),
            context,
            schema.clone(),
        );
        let stream = futures::stream::iter(vec![batch_fut])
            .then(|fut| async move { fut.await.map_err(|err| err.into()) })
            .boxed();
        Ok(Box::pin(InstrumentedRecordBatchStreamAdapter::new(
            schema,
            stream,
            partition,
            &self.metrics,
        )))
    }

    fn partition_statistics(
        &self,
        _partition: Option<usize>,
    ) -> datafusion::error::Result<Arc<datafusion::physical_plan::Statistics>> {
        Ok(Arc::new(datafusion::physical_plan::Statistics {
            num_rows: datafusion::common::stats::Precision::Exact(1),
            ..datafusion::physical_plan::Statistics::new_unknown(&self.schema)
        }))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn supports_limit_pushdown(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use std::{ops::Bound, sync::Arc};

    use arrow::datatypes::{Int64Type, UInt64Type};
    use datafusion::common::DFSchema;
    use datafusion::execution::TaskContext;
    use datafusion::functions_aggregate;
    use datafusion::logical_expr::lit;
    use datafusion::physical_expr::execution_props::ExecutionProps;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::scalar::ScalarValue;
    use futures::TryStreamExt;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datagen::gen_batch;
    use lance_index::IndexType;
    use lance_index::scalar::{
        SargableQuery, ScalarIndexParams,
        expression::{ScalarIndexExpr, ScalarIndexSearch},
    };
    use lance_select::result::IndexExprResultWireFormat;
    use lance_select::{RowAddrMask, RowAddrTreeMap};

    use super::*;
    use crate::Dataset;
    use crate::index::DatasetIndexExt;
    use crate::io::exec::scalar_index::ScalarIndexExec;
    use crate::utils::test::{DatagenExt, FragmentCount, FragmentRowCount};
    #[allow(deprecated)]
    use datafusion::physical_planner::create_aggregate_expr_and_maybe_filter;

    /// Build an `AggregateFunctionExpr` matching `COUNT(*)`.
    // TODO(datafusion-54): migrate off the deprecated
    // create_aggregate_expr_and_maybe_filter to LoweredAggregateBuilder.
    #[allow(deprecated)]
    fn count_star_expr(input_schema: &SchemaRef) -> Arc<AggregateFunctionExpr> {
        let expr = functions_aggregate::count::count(lit(1));
        let df_schema = DFSchema::try_from(input_schema.as_ref().clone()).unwrap();
        let (agg_expr, _filter, _order_by) = create_aggregate_expr_and_maybe_filter(
            &expr,
            &df_schema,
            input_schema.as_ref(),
            &ExecutionProps::default(),
        )
        .unwrap();
        agg_expr
    }

    struct Fixture {
        dataset: Arc<Dataset>,
        _tmp: TempStrDir,
    }

    /// 4 fragments × 10 rows, ascending `ordered` column with a BTree index.
    async fn make_fixture() -> Fixture {
        let tmp = TempStrDir::default();
        let mut dataset = gen_batch()
            .col("ordered", lance_datagen::array::step::<UInt64Type>())
            .into_dataset(
                tmp.as_str(),
                FragmentCount::from(4),
                FragmentRowCount::from(10),
            )
            .await
            .unwrap();
        dataset
            .create_index(
                &["ordered"],
                IndexType::BTree,
                None,
                &ScalarIndexParams::default(),
                true,
            )
            .await
            .unwrap();
        Fixture {
            dataset: Arc::new(dataset),
            _tmp: tmp,
        }
    }

    fn input_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![arrow_schema::Field::new(
            "ordered",
            arrow_schema::DataType::UInt64,
            false,
        )]))
    }

    async fn run(plan: CountFromMaskExec) -> i64 {
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(batches.len(), 1);
        assert_eq!(batches[0].num_rows(), 1);
        batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<arrow_array::PrimitiveArray<Int64Type>>()
            .expect("count partial state should be Int64")
            .value(0)
    }

    #[tokio::test]
    async fn try_new_rejects_empty_aggregate_funcs() {
        let fixture = make_fixture().await;
        let err = CountFromMaskExec::try_new(fixture.dataset, vec![], None).unwrap_err();
        assert!(err.to_string().contains("at least one aggregate"), "{err}");
    }

    #[tokio::test]
    async fn count_from_mask_mixes_full_and_partial() {
        // Synthesize an AllowList containing one Full-marker fragment and
        // one Partial bitmap; verify the Full fragment falls back to
        // physical_rows from the manifest and Partial falls back to
        // bitmap.len().
        let fixture = make_fixture().await;
        let mut tm = RowAddrTreeMap::new();
        // Fragment 0: full (10 physical rows).
        tm.insert_fragment(0);
        // Fragment 1: partial with explicit row addrs.
        let row_addr_for = |frag_id: u32, offset: u32| ((frag_id as u64) << 32) | offset as u64;
        tm.insert(row_addr_for(1, 0));
        tm.insert(row_addr_for(1, 1));
        tm.insert(row_addr_for(1, 2));

        let mask = RowAddrMask::AllowList(tm);
        let count = CountFromMaskExec::count_from_mask(&mask, fixture.dataset.as_ref()).unwrap();
        assert_eq!(count, 10 + 3);
    }

    #[tokio::test]
    async fn execute_count_no_prefilter() {
        let fixture = make_fixture().await;
        let schema = input_schema();
        let plan = CountFromMaskExec::try_new(
            fixture.dataset.clone(),
            vec![count_star_expr(&schema)],
            None,
        )
        .unwrap();
        let count = run(plan).await;
        assert_eq!(count, 40); // 4 fragments × 10 rows
    }

    #[tokio::test]
    async fn execute_count_with_allow_list_prefilter() {
        let fixture = make_fixture().await;
        let schema = input_schema();

        // `ordered < 25` matches 25 rows across the four fragments.
        let prefilter_expr = ScalarIndexExpr::Query(ScalarIndexSearch {
            column: "ordered".to_string(),
            index_name: "ordered_idx".to_string(),
            index_type: "BTree".to_string(),
            query: Arc::new(SargableQuery::Range(
                Bound::Unbounded,
                Bound::Excluded(ScalarValue::UInt64(Some(25))),
            )),
            needs_recheck: false,
            fragment_bitmap: None,
        });
        let prefilter: Arc<dyn ExecutionPlan> = Arc::new(ScalarIndexExec::new(
            fixture.dataset.clone(),
            prefilter_expr,
            IndexExprResultWireFormat::default(),
        ));

        let plan = CountFromMaskExec::try_new(
            fixture.dataset.clone(),
            vec![count_star_expr(&schema)],
            Some(prefilter),
        )
        .unwrap();
        let count = run(plan).await;
        assert_eq!(count, 25);
    }

    #[tokio::test]
    async fn execute_count_with_block_list_prefilter() {
        let fixture = make_fixture().await;
        let schema = input_schema();

        // NOT(ordered < 25) is a block list of those 25 rows — 40 − 25 = 15.
        let prefilter_expr =
            ScalarIndexExpr::Not(Box::new(ScalarIndexExpr::Query(ScalarIndexSearch {
                column: "ordered".to_string(),
                index_name: "ordered_idx".to_string(),
                index_type: "BTree".to_string(),
                query: Arc::new(SargableQuery::Range(
                    Bound::Unbounded,
                    Bound::Excluded(ScalarValue::UInt64(Some(25))),
                )),
                needs_recheck: false,
                fragment_bitmap: None,
            })));
        let prefilter: Arc<dyn ExecutionPlan> = Arc::new(ScalarIndexExec::new(
            fixture.dataset.clone(),
            prefilter_expr,
            IndexExprResultWireFormat::default(),
        ));

        let plan = CountFromMaskExec::try_new(
            fixture.dataset.clone(),
            vec![count_star_expr(&schema)],
            Some(prefilter),
        )
        .unwrap();
        let count = run(plan).await;
        assert_eq!(count, 15);
    }

    #[tokio::test]
    async fn execute_count_respects_deletions() {
        let fixture = make_fixture().await;
        let mut dataset = (*fixture.dataset).clone();
        // Delete the first ten rows of the dataset (which live in fragment 0).
        dataset.delete("ordered < 10").await.unwrap();
        let dataset = Arc::new(dataset);

        let schema = input_schema();
        let plan =
            CountFromMaskExec::try_new(dataset, vec![count_star_expr(&schema)], None).unwrap();
        let count = run(plan).await;
        assert_eq!(count, 30);
    }
}
