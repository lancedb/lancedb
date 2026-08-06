// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::borrow::Cow;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::task::Poll;

use arrow::array::AsArray;
use arrow::compute::{TakeOptions, concat_batches};
use arrow::datatypes::UInt64Type;
use arrow_array::{Array, BooleanArray, UInt32Array};
use arrow_array::{RecordBatch, UInt64Array};
use arrow_schema::{Schema as ArrowSchema, SchemaRef};
use datafusion::common::Statistics;
use datafusion::error::{DataFusionError, Result};
use datafusion::physical_plan::metrics::{
    BaselineMetrics, Count, ExecutionPlanMetricsSet, MetricBuilder, MetricValue, MetricsSet,
};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties, SendableRecordBatchStream,
};
use datafusion_physical_expr::EquivalenceProperties;
use futures::FutureExt;
use futures::stream::{FuturesOrdered, Stream, StreamExt, TryStreamExt};
use lance_arrow::RecordBatchExt;
use lance_core::datatypes::{Field, OnMissing, Projection};
use lance_core::error::{DataFusionResult, LanceOptionExt};
use lance_core::utils::address::RowAddress;
use lance_core::utils::futures::FinallyStreamExt;
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_core::{ROW_ADDR, ROW_ID};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use tracing::error;

use crate::dataset::Dataset;
use crate::dataset::fragment::{FragReadConfig, FragmentReader};
use crate::dataset::rowids::get_row_id_index;
use crate::datatypes::Schema;

use super::utils::IoMetrics;

#[derive(Debug, Clone)]
struct TakeStreamMetrics {
    baseline_metrics: BaselineMetrics,
    batches_processed: Count,
    io_metrics: IoMetrics,
}

impl TakeStreamMetrics {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        let batches_processed = Count::new();
        MetricBuilder::new(metrics)
            .with_partition(partition)
            .build(MetricValue::Count {
                name: Cow::Borrowed("batches_processed"),
                count: batches_processed.clone(),
            });
        Self {
            baseline_metrics: BaselineMetrics::new(metrics, partition),
            batches_processed,
            io_metrics: IoMetrics::new(metrics, partition),
        }
    }
}

struct TakeStream {
    /// The dataset to take from
    dataset: Arc<Dataset>,
    /// The fields to take from the input stream
    fields_to_take: Arc<Schema>,
    /// The descriptor-view schema used for storage reads when blob payloads
    /// must be materialized after take.
    read_fields: Arc<Schema>,
    materialize_blob_v2_binary: bool,
    /// The output schema, needed for us to merge the new columns
    /// into the input data in the correct order
    output_schema: SchemaRef,
    /// A cache of opened file readers
    ///
    /// This is a map from fragment id to a reader.
    readers_cache: Arc<Mutex<HashMap<u32, Arc<FragmentReader>>>>,
    /// The scan scheduler to use for reading fragments
    scan_scheduler: Arc<ScanScheduler>,
    /// The metrics for the stream
    metrics: TakeStreamMetrics,
}

impl TakeStream {
    fn new(
        dataset: Arc<Dataset>,
        fields_to_take: Arc<Schema>,
        output_schema: SchemaRef,
        scan_scheduler: Arc<ScanScheduler>,
        metrics: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Self {
        let materialize_blob_v2_binary =
            crate::dataset::blob::schema_has_blob_v2_binary_view(fields_to_take.as_ref());
        let read_fields = if materialize_blob_v2_binary {
            Arc::new(crate::dataset::blob::blob_v2_descriptor_schema(
                fields_to_take.as_ref(),
            ))
        } else {
            fields_to_take.clone()
        };
        Self {
            dataset,
            fields_to_take,
            read_fields,
            materialize_blob_v2_binary,
            output_schema,
            readers_cache: Arc::new(Mutex::new(HashMap::new())),
            scan_scheduler,
            metrics: TakeStreamMetrics::new(metrics, partition),
        }
    }

    async fn do_open_reader(&self, fragment_id: u32) -> DataFusionResult<Arc<FragmentReader>> {
        let fragment = self
            .dataset
            .get_fragment(fragment_id as usize)
            .ok_or_else(|| {
                let branch = self
                    .dataset
                    .manifest()
                    .branch
                    .as_deref()
                    .unwrap_or("main");
                error!(
                    fragment_id,
                    dataset_uri = %self.dataset.uri(),
                    manifest_version = self.dataset.manifest().version,
                    manifest_path = %self.dataset.manifest_location().path,
                    branch = ?self.dataset.manifest().branch,
                    "Missing fragment id during take operation",
                );
                DataFusionError::Execution(format!(
                    "The input to a take operation specified fragment id {} but this fragment does not exist in the dataset (uri={}, version={}, manifest={}, branch={})",
                    fragment_id,
                    self.dataset.uri(),
                    self.dataset.manifest().version,
                    self.dataset.manifest_location().path,
                    branch
                ))
            })?;

        let mut read_config =
            FragReadConfig::default().with_scan_scheduler(self.scan_scheduler.clone());
        if self.materialize_blob_v2_binary {
            read_config = read_config.with_row_address(true);
        }
        let reader = Arc::new(fragment.open(&self.read_fields, read_config).await?);

        let mut readers = self.readers_cache.lock().unwrap();
        readers.insert(fragment_id, reader.clone());
        Ok(reader)
    }

    async fn open_reader(&self, fragment_id: u32) -> DataFusionResult<Arc<FragmentReader>> {
        if let Some(reader) = self
            .readers_cache
            .lock()
            .unwrap()
            .get(&fragment_id)
            .cloned()
        {
            return Ok(reader);
        }

        self.do_open_reader(fragment_id).await
    }

    /// Returns the row addresses for the given batch, plus an optional validity
    /// mask. When stable row IDs are used, some row IDs from stale index results
    /// (e.g. FTS matches for deleted rows) may no longer exist in the row ID
    /// index. These are excluded from the returned addresses, and the mask
    /// indicates which input rows are still valid so the caller can filter the
    /// batch to match.
    async fn get_row_addrs(
        &self,
        batch: &RecordBatch,
    ) -> Result<(Arc<dyn Array>, Option<BooleanArray>)> {
        if let Some(row_addr_array) = batch.column_by_name(ROW_ADDR) {
            Ok((row_addr_array.clone(), None))
        } else {
            let row_id_array = batch.column_by_name(ROW_ID).expect_ok()?;

            if let Some(row_id_index) = get_row_id_index(&self.dataset).await? {
                let row_id_array = row_id_array.as_primitive::<UInt64Type>();
                let mut addresses = Vec::with_capacity(row_id_array.len());
                let mut valid = Vec::with_capacity(row_id_array.len());

                for id in row_id_array.values().iter() {
                    if let Some(address) = row_id_index.get(*id) {
                        addresses.push(u64::from(address));
                        valid.push(true);
                    } else {
                        valid.push(false);
                    }
                }

                let mask = if addresses.len() < row_id_array.len() {
                    Some(BooleanArray::from(valid))
                } else {
                    None
                };
                Ok((Arc::new(UInt64Array::from(addresses)), mask))
            } else {
                Ok((row_id_array.clone(), None))
            }
        }
    }

    async fn map_batch(
        self: Arc<Self>,
        batch: RecordBatch,
        batch_number: u32,
    ) -> DataFusionResult<RecordBatch> {
        let compute_timer = self.metrics.baseline_metrics.elapsed_compute().timer();
        let (row_addrs_arr, validity_mask) = self.get_row_addrs(&batch).await?;

        // Filter out rows whose row IDs no longer exist (e.g. stale FTS/vector
        // index entries pointing to deleted rows). Without this, the downstream
        // merge would fail with a row-count mismatch.
        let batch = if let Some(mask) = validity_mask {
            arrow::compute::filter_record_batch(&batch, &mask)?
        } else {
            batch
        };

        let row_addrs = row_addrs_arr.as_primitive::<UInt64Type>();

        debug_assert!(
            row_addrs.null_count() == 0,
            "{} nulls in row addresses",
            row_addrs.null_count()
        );

        // Fast path: check if addresses are already sorted with no duplicates (common case).
        // This avoids all sorting, dedup, and permutation overhead.
        let is_sorted_and_unique = row_addrs.values().windows(2).all(|w| w[0] < w[1]);

        let sorted_addrs: Arc<dyn Array>;
        let (unique_addrs, permutation, sorted_to_unique) = if is_sorted_and_unique {
            (Cow::Borrowed(row_addrs.values().as_ref()), None, None)
        } else {
            // Sort and compute inverse permutation to restore original order later
            let permutation = arrow::compute::sort_to_indices(&row_addrs_arr, None, None).unwrap();
            sorted_addrs = arrow::compute::take(
                &row_addrs_arr,
                &permutation,
                Some(TakeOptions {
                    check_bounds: false,
                }),
            )
            .unwrap();
            let mut inverse_permutation = vec![0; permutation.len()];
            for (i, p) in permutation.values().iter().enumerate() {
                inverse_permutation[*p as usize] = i as u32;
            }
            let sorted_values = sorted_addrs.as_primitive::<UInt64Type>().values();

            // Deduplicate sorted addresses. FTS on List<Utf8> can produce duplicate
            // row addresses when multiple list elements in the same row match. The
            // encoding layer requires strictly increasing indices, so we dedup here
            // and expand the results back afterwards.
            let has_duplicates = sorted_values.windows(2).any(|w| w[0] == w[1]);
            if has_duplicates {
                let mut deduped: Vec<u64> = Vec::with_capacity(sorted_values.len());
                let mut mapping: Vec<usize> = Vec::with_capacity(sorted_values.len());
                for &addr in sorted_values.iter() {
                    if deduped.last() != Some(&addr) {
                        deduped.push(addr);
                    }
                    mapping.push(deduped.len() - 1);
                }
                (
                    Cow::Owned(deduped),
                    Some(UInt32Array::from(inverse_permutation)),
                    Some(mapping),
                )
            } else {
                (
                    Cow::Borrowed(sorted_values.as_ref()),
                    Some(UInt32Array::from(inverse_permutation)),
                    None,
                )
            }
        };

        let mut futures = FuturesOrdered::new();
        let mut current_offsets = Vec::new();
        let mut current_fragment_id = None;

        for row_addr in unique_addrs.iter() {
            let addr = RowAddress::new_from_u64(*row_addr);

            if Some(addr.fragment_id()) != current_fragment_id {
                // Start a new group
                if let Some(fragment_id) = current_fragment_id {
                    let reader = self.open_reader(fragment_id).await?;
                    let offsets = std::mem::take(&mut current_offsets);
                    futures.push_back(
                        async move { reader.take_as_batch(&offsets, Some(batch_number)).await }
                            .boxed(),
                    );
                }
                current_fragment_id = Some(addr.fragment_id());
            }

            current_offsets.push(addr.row_offset());
        }

        // Handle the last group
        if let Some(fragment_id) = current_fragment_id {
            let reader = self.open_reader(fragment_id).await?;
            futures.push_back(
                async move {
                    reader
                        .take_as_batch(&current_offsets, Some(batch_number))
                        .await
                }
                .boxed(),
            );
        }

        // Stop the compute timer, don't count I/O time
        drop(compute_timer);

        let batches = futures.try_collect::<Vec<_>>().await?;

        if batches.is_empty() {
            return Ok(RecordBatch::new_empty(self.output_schema.clone()));
        }

        let _compute_timer = self.metrics.baseline_metrics.elapsed_compute().timer();
        let schema = batches.first().expect_ok()?.schema();
        let mut new_data = concat_batches(&schema, batches.iter())?;

        // Expand deduplicated rows and restore original order.
        // When both are needed, combine into a single take to avoid two passes.
        match (sorted_to_unique, permutation) {
            (Some(expand_map), Some(inv_perm)) => {
                // Compose: for each original position, look up its sorted position
                // via the inverse permutation, then map through the dedup expand.
                let combined = UInt32Array::from(
                    inv_perm
                        .values()
                        .iter()
                        .map(|&p| expand_map[p as usize] as u32)
                        .collect::<Vec<_>>(),
                );
                new_data = arrow_select::take::take_record_batch(&new_data, &combined).unwrap();
            }
            (None, Some(inv_perm)) => {
                new_data = arrow_select::take::take_record_batch(&new_data, &inv_perm).unwrap();
            }
            (Some(expand_map), None) => {
                // Sorted and unique was false but no permutation — shouldn't happen,
                // but handle defensively.
                let expand_indices =
                    UInt32Array::from(expand_map.iter().map(|&i| i as u32).collect::<Vec<_>>());
                new_data =
                    arrow_select::take::take_record_batch(&new_data, &expand_indices).unwrap();
            }
            (None, None) => {}
        }

        if self.materialize_blob_v2_binary {
            new_data = crate::dataset::blob::materialize_blob_v2_binary_batch(
                &self.dataset,
                self.fields_to_take.as_ref(),
                new_data,
            )
            .await?;
        }

        Ok(batch.merge_with_schema(&new_data, self.output_schema.as_ref())?)
    }

    fn apply<S: Stream<Item = Result<RecordBatch>> + Send + 'static>(
        self: Arc<Self>,
        input: S,
    ) -> impl Stream<Item = Result<RecordBatch>> {
        let result_scan_scheduler = self.scan_scheduler.clone();
        let final_scan_scheduler = self.scan_scheduler.clone();
        let result_metrics = self.metrics.clone();
        let final_metrics = self.metrics.clone();
        let batches = input
            .enumerate()
            .map(move |(batch_index, batch)| {
                let batch = batch?;
                let this = self.clone();
                Ok(
                    tokio::task::spawn(this.map_batch(batch, batch_index as u32))
                        .map(|res| res.unwrap()),
                )
            })
            .boxed();
        batches
            .try_buffered(get_num_compute_intensive_cpus())
            .map(move |result| {
                if result.is_ok() {
                    result_metrics.batches_processed.add(1);
                }
                result_metrics.io_metrics.record(&result_scan_scheduler);
                match result_metrics
                    .baseline_metrics
                    .record_poll(Poll::Ready(Some(result)))
                {
                    Poll::Ready(Some(result)) => result,
                    _ => unreachable!("record_poll returned a different poll state"),
                }
            })
            .finally(move || {
                final_metrics.baseline_metrics.done();
                final_metrics.io_metrics.record(&final_scan_scheduler);
            })
    }
}

#[derive(Debug)]
pub struct TakeExec {
    // The dataset to take from
    dataset: Arc<Dataset>,
    // The desired output projection of the relation (input schema + take schema)
    //
    // This is used to re-calculate output_projection and extra_schema when
    // with_new_children is called.
    output_projection: Projection,
    // The schema of the extra columns to take from the dataset
    schema_to_take: Arc<Schema>,
    // The schema of the output
    output_schema: SchemaRef,
    input: Arc<dyn ExecutionPlan>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl DisplayAs for TakeExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let extra_fields = self
            .schema_to_take
            .fields
            .iter()
            .map(|f| f.name.clone())
            .collect::<HashSet<_>>();
        let columns = self
            .output_schema
            .fields
            .iter()
            .map(|f| {
                let name = f.name();
                if extra_fields.contains(name) {
                    format!("({})", name)
                } else {
                    name.clone()
                }
            })
            .collect::<Vec<_>>()
            .join(", ");
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(f, "Take: columns={:?}", columns)
            }
            DisplayFormatType::TreeRender => {
                write!(f, "Take\ncolumns={:?}", columns)
            }
        }
    }
}

impl TakeExec {
    /// Create a [`TakeExec`] node.
    ///
    /// - dataset: the dataset to read from
    /// - input: the upstream [`ExecutionPlan`] to feed data in.
    /// - projection: the desired output projection, can overlap with the input schema if desired
    ///
    /// Returns None if no extra columns are required (everything in the projection exists in the input schema).
    pub fn try_new(
        dataset: Arc<Dataset>,
        input: Arc<dyn ExecutionPlan>,
        projection: Projection,
    ) -> Result<Option<Self>> {
        let original_projection = projection.clone();
        let projection =
            projection.subtract_arrow_schema(input.schema().as_ref(), OnMissing::Ignore)?;
        if !projection.has_data_fields() {
            return Ok(None);
        }

        // We actually need a take so lets make sure we have a row id
        if input.schema().column_with_name(ROW_ADDR).is_none()
            && input.schema().column_with_name(ROW_ID).is_none()
        {
            return Err(DataFusionError::Plan(format!(
                "TakeExec requires the input plan to have a column named '{}' or '{}'",
                ROW_ADDR, ROW_ID
            )));
        }

        // Can't use take if we don't want any fields and we can't use take to add row_id or row_addr
        assert!(
            !projection.with_row_id && !projection.with_row_addr,
            "Take should not be used to insert row_id / row_addr: {:#?}",
            projection
        );

        let output_schema =
            Self::calculate_output_schema(dataset.schema(), &input.schema(), &projection);
        let output_schema = Arc::new(crate::dataset::blob::public_blob_v2_binary_output_schema(
            &output_schema,
        ));
        let output_arrow = Arc::new(ArrowSchema::from(output_schema.as_ref()));
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(EquivalenceProperties::new(output_arrow.clone())),
        );

        Ok(Some(Self {
            dataset,
            output_projection: original_projection,
            schema_to_take: projection.into_schema_ref(),
            input,
            output_schema: output_arrow,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        }))
    }

    /// The output of a take operation will be all columns from the input schema followed
    /// by any new columns from the dataset.
    ///
    /// The output fields will always be added in dataset schema order
    ///
    /// Nested columns in the input schema may have new fields inserted into them.
    ///
    /// If this happens the order of the new nested fields will match the order defined in
    /// the dataset schema.
    pub(crate) fn calculate_output_schema(
        dataset_schema: &Schema,
        input_schema: &ArrowSchema,
        projection: &Projection,
    ) -> Schema {
        // TakeExec doesn't reorder top-level fields and so the first thing we need to do is determine the
        // top-level field order.
        let mut top_level_fields_added = HashSet::with_capacity(input_schema.fields.len());
        let projected_schema = projection.to_schema();

        let mut output_fields =
            Vec::with_capacity(input_schema.fields.len() + projected_schema.fields.len());
        // TakeExec always moves the _rowid to the start of the output schema
        output_fields.extend(input_schema.fields.iter().map(|f| {
            let f = Field::try_from(f.as_ref()).unwrap();
            if let Some(ds_field) = dataset_schema.field(&f.name) {
                top_level_fields_added.insert(ds_field.id);
                // Field is in the dataset, it might have new fields added to it
                if let Some(projected_field) = ds_field.apply_projection(projection) {
                    f.merge_with_reference(&projected_field, ds_field)
                } else {
                    // No new fields added, keep as-is
                    f
                }
            } else {
                // Field not in dataset, not possible to add extra fields, use as-is
                f
            }
        }));

        // Now we add to the end any brand new top-level fields.  These will be added
        // dataset schema order.
        output_fields.extend(
            projected_schema
                .fields
                .into_iter()
                .filter(|f| !top_level_fields_added.contains(&f.id)),
        );
        Schema {
            fields: output_fields,
            metadata: dataset_schema.metadata.clone(),
        }
    }

    /// Get the dataset.
    pub fn dataset(&self) -> &Arc<Dataset> {
        &self.dataset
    }
}

impl ExecutionPlan for TakeExec {
    fn name(&self) -> &str {
        "TakeExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // This is an I/O bound operation and wouldn't really benefit from partitioning
        //
        // Plus, if we did that, we would be creating multiple schedulers which could use
        // a lot of RAM.
        vec![false]
    }

    /// This preserves the output schema.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Internal(
                "TakeExec wrong number of children".to_string(),
            ));
        }

        let projection = self.output_projection.clone();

        let plan = Self::try_new(self.dataset.clone(), children[0].clone(), projection)?;

        if let Some(plan) = plan {
            Ok(Arc::new(plan))
        } else {
            // Is this legal or do we need to insert a no-op node?
            Ok(children[0].clone())
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let dataset = self.dataset.clone();
        let schema_to_take = self.schema_to_take.clone();
        let output_schema = self.output_schema.clone();
        let metrics = self.metrics.clone();

        // ScanScheduler::new launches the I/O scheduler in the background.
        // We aren't allowed to do work in `execute` and so we defer creation of the
        // TakeStream until the stream is polled.
        let lazy_take_stream = futures::stream::once(async move {
            let obj_store = dataset.object_store.clone();
            let scheduler_config = SchedulerConfig::max_bandwidth(&obj_store);
            // unwrap is safe since SchedulerConfig::max_bandwidth is always valid
            let scan_scheduler = ScanScheduler::new(obj_store, scheduler_config);

            let take_stream = Arc::new(TakeStream::new(
                dataset,
                schema_to_take,
                output_schema,
                scan_scheduler,
                &metrics,
                partition,
            ));
            take_stream.apply(input_stream)
        });
        let output_schema = self.output_schema.clone();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            output_schema,
            lazy_take_stream.flatten(),
        )))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Arc<datafusion::physical_plan::Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: self.input.partition_statistics(partition)?.num_rows,
            ..Statistics::new_unknown(self.schema().as_ref())
        }))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow_array::{
        ArrayRef, Float32Array, Int32Array, RecordBatchIterator, StringArray, StructArray,
    };
    use arrow_schema::{DataType, Field, Fields};
    use datafusion::execution::TaskContext;
    use lance_arrow::SchemaExt;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_core::{ROW_ID, datatypes::OnMissing};
    use lance_datafusion::{datagen::DatafusionDatagenExt, exec::OneShotExec, utils::MetricsExt};
    use lance_datagen::{BatchCount, RowCount};
    use rstest::rstest;

    use crate::{
        dataset::WriteParams,
        io::exec::{LanceScanConfig, LanceScanExec},
        utils::test::NoContextTestFixture,
    };

    struct TestFixture {
        dataset: Arc<Dataset>,
        _tmp_dir_guard: TempStrDir,
    }

    async fn test_fixture() -> TestFixture {
        let struct_fields = Fields::from(vec![
            Arc::new(Field::new("x", DataType::Int32, false)),
            Arc::new(Field::new("y", DataType::Int32, false)),
        ]);

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("f", DataType::Float32, false),
            Field::new("s", DataType::Utf8, false),
            Field::new("struct", DataType::Struct(struct_fields.clone()), false),
        ]));

        // Write 3 batches.
        let expected_batches: Vec<RecordBatch> = (0..3)
            .map(|batch_id| {
                let value_range = batch_id * 10..batch_id * 10 + 10;
                let columns: Vec<ArrayRef> = vec![
                    Arc::new(Int32Array::from_iter_values(value_range.clone())),
                    Arc::new(Float32Array::from_iter(
                        value_range.clone().map(|v| v as f32),
                    )),
                    Arc::new(StringArray::from_iter_values(
                        value_range.clone().map(|v| format!("str-{v}")),
                    )),
                    Arc::new(StructArray::new(
                        struct_fields.clone(),
                        vec![
                            Arc::new(Int32Array::from_iter(value_range.clone())),
                            Arc::new(Int32Array::from_iter(value_range)),
                        ],
                        None,
                    )),
                ];
                RecordBatch::try_new(schema.clone(), columns).unwrap()
            })
            .collect();

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        let params = WriteParams {
            max_rows_per_file: 10,
            ..Default::default()
        };
        let reader =
            RecordBatchIterator::new(expected_batches.clone().into_iter().map(Ok), schema.clone());
        Dataset::write(reader, test_uri, Some(params))
            .await
            .unwrap();

        TestFixture {
            dataset: Arc::new(Dataset::open(test_uri).await.unwrap()),
            _tmp_dir_guard: test_dir,
        }
    }

    #[tokio::test]
    async fn test_take_schema() {
        let TestFixture { dataset, .. } = test_fixture().await;

        let scan_arrow_schema = ArrowSchema::new(vec![Field::new("i", DataType::Int32, false)]);
        let scan_schema = Arc::new(Schema::try_from(&scan_arrow_schema).unwrap());

        // With row id
        let config = LanceScanConfig {
            with_row_id: true,
            ..Default::default()
        };
        let input = Arc::new(LanceScanExec::new(
            dataset.clone(),
            dataset.fragments().clone(),
            None,
            scan_schema,
            config,
        ));

        let projection = dataset
            .empty_projection()
            .union_column("s", OnMissing::Error)
            .unwrap();
        let take_exec = TakeExec::try_new(dataset, input, projection)
            .unwrap()
            .unwrap();
        let schema = take_exec.schema();
        assert_eq!(
            schema.fields.iter().map(|f| f.name()).collect::<Vec<_>>(),
            vec!["i", ROW_ID, "s"]
        );
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum TakeInput {
        Ids,
        Addrs,
        IdsAndAddrs,
    }

    #[rstest]
    #[tokio::test]
    async fn test_simple_take(
        #[values(TakeInput::Ids, TakeInput::Addrs, TakeInput::IdsAndAddrs)] take_input: TakeInput,
    ) {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;

        let scan_schema = Arc::new(dataset.schema().project(&["i"]).unwrap());
        let config = LanceScanConfig {
            with_row_address: take_input != TakeInput::Ids,
            with_row_id: take_input != TakeInput::Addrs,
            ..Default::default()
        };
        let input = Arc::new(LanceScanExec::new(
            dataset.clone(),
            dataset.fragments().clone(),
            None,
            scan_schema,
            config,
        ));

        let projection = dataset
            .empty_projection()
            .union_column("s", OnMissing::Error)
            .unwrap();
        let take_exec = TakeExec::try_new(dataset, input, projection)
            .unwrap()
            .unwrap();
        let schema = take_exec.schema();

        let mut expected_fields = vec!["i"];
        if take_input != TakeInput::Addrs {
            expected_fields.push(ROW_ID);
        }
        if take_input != TakeInput::Ids {
            expected_fields.push(ROW_ADDR);
        }
        expected_fields.push("s");
        assert_eq!(&schema.field_names(), &expected_fields);

        let mut stream = take_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();

        while let Some(batch) = stream.try_next().await.unwrap() {
            assert_eq!(&batch.schema().field_names(), &expected_fields);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn test_take_records_output_and_io_metrics() {
        use datafusion::physical_plan::metrics::MetricValue;
        use lance_datafusion::utils::{BYTES_READ_METRIC, IOPS_METRIC, REQUESTS_METRIC};
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;

        let row_addrs = UInt64Array::from(vec![0_u64, 1, 2, 3, 4]);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            ROW_ADDR,
            DataType::UInt64,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(row_addrs)]).unwrap();
        let stream = futures::stream::iter(vec![Ok(batch)]);
        let stream = Box::pin(RecordBatchStreamAdapter::new(schema, stream));
        let input = Arc::new(OneShotExec::new(stream));

        let projection = dataset
            .empty_projection()
            .union_column("s", OnMissing::Error)
            .unwrap();

        let take_exec = TakeExec::try_new(dataset, input, projection)
            .unwrap()
            .unwrap();

        let stream = take_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(batches.iter().map(|b| b.num_rows()).sum::<usize>(), 5);

        let metrics = take_exec.metrics().unwrap();

        let output_batches: usize = metrics
            .iter()
            .filter_map(|m| match m.value() {
                MetricValue::OutputBatches(count) => Some(count.value()),
                _ => None,
            })
            .sum();

        let output_bytes: usize = metrics
            .iter()
            .filter_map(|m| match m.value() {
                MetricValue::OutputBytes(count) => Some(count.value()),
                _ => None,
            })
            .sum();

        let gauge = |name: &str| -> usize {
            metrics
                .iter_gauges()
                .find_map(|(metric_name, gauge)| {
                    (metric_name.as_ref() == name).then(|| gauge.value())
                })
                .unwrap_or(0)
        };

        let bytes_read = gauge(BYTES_READ_METRIC);
        let iops = gauge(IOPS_METRIC);
        let requests = gauge(REQUESTS_METRIC);

        assert_eq!(metrics.output_rows(), Some(5));
        assert_eq!(metrics.find_count("batches_processed").unwrap().value(), 1);
        assert!(
            output_batches > 0 && output_bytes > 0 && bytes_read > 0 && iops > 0 && requests > 0,
            "expected positive TakeExec metrics, got output_batches={output_batches}, output_bytes={output_bytes}, bytes_read={bytes_read}, iops={iops}, requests={requests}"
        );
    }

    #[tokio::test]
    async fn test_take_order() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;

        // Grab all row addresses, shuffle them, and select the first 15 (half of the rows)
        let data = dataset
            .scan()
            .project(&["s"])
            .unwrap()
            .with_row_address()
            .try_into_batch()
            .await
            .unwrap();
        let indices = UInt64Array::from(vec![8, 13, 1, 7, 4, 5, 12, 9, 10, 2, 11, 6, 3, 0, 28]);
        let data = arrow_select::take::take_record_batch(&data, &indices).unwrap();

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            ROW_ADDR,
            DataType::UInt64,
            true,
        )]));
        let row_addrs = data.project_by_schema(&schema).unwrap();

        // Split into 3 batches of 5
        let batches = (0..3)
            .map(|i| {
                let start = i * 5;
                row_addrs.slice(start, 5)
            })
            .collect::<Vec<_>>();

        let row_addr_stream = futures::stream::iter(batches.clone().into_iter().map(Ok));
        let row_addr_stream = Box::pin(RecordBatchStreamAdapter::new(schema, row_addr_stream));

        let input = Arc::new(OneShotExec::new(row_addr_stream));

        let projection = dataset
            .empty_projection()
            .union_column("s", OnMissing::Error)
            .unwrap();
        let take_exec = TakeExec::try_new(dataset, input, projection)
            .unwrap()
            .unwrap();

        let stream = take_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();

        let expected = vec![data.slice(0, 5), data.slice(5, 5), data.slice(10, 5)];

        let batches = stream.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(batches.len(), 3);
        for (batch, expected) in batches.into_iter().zip(expected) {
            assert_eq!(batch.schema().field_names(), vec![ROW_ADDR, "s"]);
            let expected = expected.project_by_schema(&batch.schema()).unwrap();
            assert_eq!(batch, expected);
        }

        let metrics = take_exec.metrics().unwrap();
        assert_eq!(metrics.output_rows(), Some(15));
        assert_eq!(metrics.find_count("batches_processed").unwrap().value(), 3);
    }

    /// Regression test: FTS on List<Utf8> can produce duplicate row addresses when
    /// multiple list elements in the same row match. These duplicates caused
    /// `indices_to_ranges` in the encoding layer to produce overlapping ranges,
    /// panicking in BinaryPageScheduler with "attempt to subtract with overflow".
    #[tokio::test]
    async fn test_take_with_duplicate_row_addrs() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;

        // Simulate duplicate row addresses (same row matched twice),
        // already sorted as they would be within a single fragment.
        let row_addrs = UInt64Array::from(vec![0u64, 0, 1, 2, 2]);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            ROW_ADDR,
            DataType::UInt64,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(row_addrs)]).unwrap();

        let row_addr_stream = futures::stream::iter(vec![Ok(batch)]);
        let row_addr_stream = Box::pin(RecordBatchStreamAdapter::new(schema, row_addr_stream));
        let input = Arc::new(OneShotExec::new(row_addr_stream));

        let projection = dataset
            .empty_projection()
            .union_column("s", OnMissing::Error)
            .unwrap();
        let take_exec = TakeExec::try_new(dataset, input, projection)
            .unwrap()
            .unwrap();

        let stream = take_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        let all_data = concat_batches(&batches[0].schema(), &batches).unwrap();
        let s_col = all_data
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Duplicated rows should have identical values
        assert_eq!(s_col.value(0), s_col.value(1));
        assert_eq!(s_col.value(3), s_col.value(4));
    }

    /// Same as above but with unsorted duplicates, exercising the sort+dedup path.
    #[tokio::test]
    async fn test_take_with_unsorted_duplicate_row_addrs() {
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;

        let row_addrs = UInt64Array::from(vec![2u64, 0, 1, 0, 2]);
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            ROW_ADDR,
            DataType::UInt64,
            true,
        )]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(row_addrs)]).unwrap();

        let row_addr_stream = futures::stream::iter(vec![Ok(batch)]);
        let row_addr_stream = Box::pin(RecordBatchStreamAdapter::new(schema, row_addr_stream));
        let input = Arc::new(OneShotExec::new(row_addr_stream));

        let projection = dataset
            .empty_projection()
            .union_column("s", OnMissing::Error)
            .unwrap();
        let take_exec = TakeExec::try_new(dataset, input, projection)
            .unwrap()
            .unwrap();

        let stream = take_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 5);

        let all_data = concat_batches(&batches[0].schema(), &batches).unwrap();
        let s_col = all_data
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        // Original order was [2, 0, 1, 0, 2] — duplicates should match
        assert_eq!(s_col.value(0), s_col.value(4)); // both row 2
        assert_eq!(s_col.value(1), s_col.value(3)); // both row 0
    }

    #[tokio::test]
    async fn test_take_struct() {
        // When taking fields into an existing struct, the field order should be maintained
        // according to the schema of the struct.
        let TestFixture {
            dataset,
            _tmp_dir_guard,
        } = test_fixture().await;

        let scan_schema = Arc::new(dataset.schema().project(&["struct.y"]).unwrap());

        let config = LanceScanConfig {
            with_row_address: true,
            ..Default::default()
        };
        let input = Arc::new(LanceScanExec::new(
            dataset.clone(),
            dataset.fragments().clone(),
            None,
            scan_schema,
            config,
        ));

        let projection = dataset
            .empty_projection()
            .union_column("struct.x", OnMissing::Error)
            .unwrap();

        let take_exec = TakeExec::try_new(dataset, input, projection)
            .unwrap()
            .unwrap();

        let expected_schema = ArrowSchema::new(vec![
            Field::new(
                "struct",
                DataType::Struct(Fields::from(vec![
                    Arc::new(Field::new("x", DataType::Int32, false)),
                    Arc::new(Field::new("y", DataType::Int32, false)),
                ])),
                false,
            ),
            Field::new(ROW_ADDR, DataType::UInt64, true),
        ]);
        let schema = take_exec.schema();
        assert_eq!(schema.as_ref(), &expected_schema);

        let mut stream = take_exec
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap();

        while let Some(batch) = stream.try_next().await.unwrap() {
            assert_eq!(batch.schema().as_ref(), &expected_schema);
        }
    }

    #[tokio::test]
    async fn test_take_no_row_addr() {
        let TestFixture { dataset, .. } = test_fixture().await;

        let scan_arrow_schema = ArrowSchema::new(vec![Field::new("i", DataType::Int32, false)]);
        let scan_schema = Arc::new(Schema::try_from(&scan_arrow_schema).unwrap());

        let projection = dataset
            .empty_projection()
            .union_column("s", OnMissing::Error)
            .unwrap();

        // No row address
        let input = Arc::new(LanceScanExec::new(
            dataset.clone(),
            dataset.fragments().clone(),
            None,
            scan_schema,
            LanceScanConfig::default(),
        ));
        assert!(TakeExec::try_new(dataset, input, projection).is_err());
    }

    #[tokio::test]
    async fn test_with_new_children() -> Result<()> {
        let TestFixture { dataset, .. } = test_fixture().await;

        let config = LanceScanConfig {
            with_row_id: true,
            ..Default::default()
        };

        let input_schema = Arc::new(dataset.schema().project(&["i"])?);
        let projection = dataset
            .empty_projection()
            .union_column("s", OnMissing::Error)
            .unwrap();

        let input = Arc::new(LanceScanExec::new(
            dataset.clone(),
            dataset.fragments().clone(),
            None,
            input_schema,
            config,
        ));

        assert_eq!(input.schema().field_names(), vec!["i", ROW_ID],);
        let take_exec = TakeExec::try_new(dataset.clone(), input.clone(), projection)?.unwrap();
        assert_eq!(take_exec.schema().field_names(), vec!["i", ROW_ID, "s"],);

        let projection = dataset
            .empty_projection()
            .union_columns(["s", "f"], OnMissing::Error)
            .unwrap();

        let outer_take =
            Arc::new(TakeExec::try_new(dataset, Arc::new(take_exec), projection)?.unwrap());
        assert_eq!(
            outer_take.schema().field_names(),
            vec!["i", ROW_ID, "s", "f"],
        );

        // with_new_children should preserve the output schema.
        let edited = outer_take.with_new_children(vec![input])?;
        assert_eq!(edited.schema().field_names(), vec!["i", ROW_ID, "f", "s"],);
        Ok(())
    }

    #[test]
    fn no_context_take() {
        // These tests ensure we can create nodes and call execute without a tokio Runtime
        // being active.  This is a requirement for proper implementation of a Datafusion foreign
        // table provider.
        let fixture = NoContextTestFixture::new();
        let arc_dasaset = Arc::new(fixture.dataset);

        let input = lance_datagen::gen_batch()
            .col(ROW_ID, lance_datagen::array::step::<UInt64Type>())
            .into_df_exec(RowCount::from(50), BatchCount::from(2));

        let take = TakeExec::try_new(
            arc_dasaset.clone(),
            input,
            arc_dasaset
                .empty_projection()
                .union_column("text", OnMissing::Error)
                .unwrap(),
        )
        .unwrap()
        .unwrap();

        take.execute(0, Arc::new(TaskContext::default())).unwrap();
    }
}
