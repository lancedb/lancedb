// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Int64Type, UInt64Type};
use arrow_array::{Array, BooleanArray, Int64Array, PrimitiveArray, RecordBatch, UInt32Array};
use arrow_schema::{DataType, Schema as ArrowSchema, SchemaRef};
use arrow_select::filter::filter_record_batch;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::col;
use datafusion::logical_expr::interval_arithmetic::{Interval, NullableInterval};
use datafusion::optimizer::simplify_expressions::{ExprSimplifier, SimplifyContext};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{ColumnarValue, PlanProperties};
use datafusion::scalar::ScalarValue;
use datafusion::{
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, SendableRecordBatchStream,
    },
    prelude::Expr,
};
use datafusion_functions::core::expr_ext::FieldAccessor;
use datafusion_physical_expr::EquivalenceProperties;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt, TryStreamExt};
use lance_arrow::{RecordBatchExt, SchemaExt};
use lance_core::utils::tokio::get_num_compute_intensive_cpus;
use lance_core::{ROW_ADDR, ROW_ADDR_FIELD, ROW_ID_FIELD};
use lance_file::reader::FileReaderOptions;
use lance_io::ReadBatchParams;
use lance_table::format::Fragment;

use crate::Error;
use crate::dataset::fragment::FragReadConfig;
use crate::dataset::scanner::LEGACY_DEFAULT_FRAGMENT_READAHEAD;
use crate::{
    Dataset,
    dataset::{
        ROW_ID,
        fragment::{FileFragment, FragmentReader},
    },
    datatypes::Schema,
};

use super::Planner;
use super::utils::InstrumentedRecordBatchStreamAdapter;

#[derive(Debug, Clone)]
pub struct ScanConfig {
    /// Number of batches to read ahead, potentially concurrently. This determines
    /// the amount of concurrent IO a scan may perform as well as the amount of
    /// data that may be buffered.
    pub batch_readahead: usize,
    /// Number of fragments to read ahead, potentially concurrently.
    pub fragment_readahead: usize,
    /// If true, a row id column will be added to the output. This will be the
    /// first column.
    pub with_row_id: bool,
    /// If true, a row address column will be added to the output. This will be
    /// the first column after row id.
    pub with_row_address: bool,
    /// If true, the scan will emit batches that contain deleted rows but have
    /// a null _rowid. This option is only valid if with_row_id is true.
    pub make_deletions_null: bool,
    /// If true (default), the scan will emit batches in the same order as the
    /// fragments. If false, the scan may emit batches in an arbitrary order.
    pub ordered_output: bool,
    /// File reader options to use when reading data files.
    pub file_reader_options: Option<FileReaderOptions>,
}

impl Default for ScanConfig {
    fn default() -> Self {
        Self {
            batch_readahead: get_num_compute_intensive_cpus(),
            fragment_readahead: LEGACY_DEFAULT_FRAGMENT_READAHEAD,
            with_row_id: false,
            with_row_address: false,
            make_deletions_null: false,
            ordered_output: true,
            file_reader_options: None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct LancePushdownScanExec {
    dataset: Arc<Dataset>,
    fragments: Arc<Vec<Fragment>>,
    projection: Arc<Schema>,
    predicate_projection: Arc<Schema>,
    predicate: Expr,
    config: ScanConfig,
    output_schema: Arc<ArrowSchema>,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
}

impl LancePushdownScanExec {
    pub fn try_new(
        dataset: Arc<Dataset>,
        fragments: Arc<Vec<Fragment>>,
        projection: Arc<Schema>,
        predicate: Expr,
        config: ScanConfig,
    ) -> Result<Self> {
        // This should be infallible.
        let columns: Vec<_> = predicate
            .column_refs()
            .into_iter()
            .map(|col| col.name.as_str())
            .collect();
        let dataset_schema = dataset.schema();
        let predicate_projection = Arc::new(dataset_schema.project(&columns)
            .map_err(|err| Error::invalid_input(format!("Filter predicate '{:?}' references columns {:?}, but some of them were not found in the dataset schema: {}\nInner error: {:?}", predicate, columns, dataset_schema, err)))?);

        if config.make_deletions_null && !config.with_row_id {
            return Err(DataFusionError::Configuration(
                "make_deletions_null is only valid if with_row_id is true".into(),
            ));
        }

        let mut output_schema: ArrowSchema = projection.as_ref().into();
        if config.with_row_address {
            output_schema = output_schema.try_with_column_at(0, ROW_ADDR_FIELD.clone())?;
        }
        if config.with_row_id {
            output_schema = output_schema.try_with_column_at(0, ROW_ID_FIELD.clone())?;
        }
        let output_schema = Arc::new(output_schema);

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        Ok(Self {
            dataset,
            fragments,
            projection,
            predicate,
            predicate_projection,
            config,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }
}

impl ExecutionPlan for LancePushdownScanExec {
    fn name(&self) -> &str {
        "LancePushdownScanExec"
    }

    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            Err(DataFusionError::Internal(
                "LancePushdownScanExec does not accept children".to_string(),
            ))
        } else {
            Ok(self)
        }
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        // To get a stream with a static lifetime, we clone self put it into
        // a stream.
        let state = (self.clone(), 0);
        let fragment_stream = futures::stream::unfold(state, |(exec, fragment_i)| async move {
            if fragment_i == exec.fragments.len() {
                None
            } else {
                let fragment = exec.fragments[fragment_i].clone();
                let exec_ref = exec.clone();
                let res = (exec_ref, fragment);
                let new_state = (exec, fragment_i + 1);
                Some((res, new_state))
            }
        });

        let batch_stream = fragment_stream.map(|(exec, fragment)| async move {
            let frag_scanner = FragmentScanner::open(
                fragment,
                exec.dataset,
                exec.projection,
                exec.predicate_projection,
                exec.predicate,
                exec.config.clone(),
            )
            .await?;

            frag_scanner.scan()
        });

        let batch_stream = batch_stream
            .buffered(self.config.fragment_readahead)
            .try_flatten();

        Ok(Box::pin(InstrumentedRecordBatchStreamAdapter::new(
            self.schema(),
            batch_stream,
            partition,
            &self.metrics,
        )))
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
}

impl DisplayAs for LancePushdownScanExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let columns = self
            .projection
            .fields
            .iter()
            .map(|f| f.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "LancePushdownScan: uri={}, projection=[{}], predicate={}, row_id={}, row_addr={}, ordered={}",
                    self.dataset.data_dir(),
                    columns,
                    self.predicate,
                    self.config.with_row_id,
                    self.config.with_row_address,
                    self.config.ordered_output
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "LancePushdownScan\nuri={}\nprojection=[{}]\npredicate={}\nrow_id={}\nrow_addr={}\nordered={}",
                    self.dataset.data_dir(),
                    columns,
                    self.predicate,
                    self.config.with_row_id,
                    self.config.with_row_address,
                    self.config.ordered_output
                )
            }
        }
    }
}

#[derive(Debug)]
struct FragmentScanner {
    fragment: FileFragment,
    projection: Arc<Schema>,
    predicate_projection: Arc<Schema>,
    predicate: Expr,
    config: ScanConfig,
    reader: FragmentReader,
    stats: Option<RecordBatch>,
}

impl FragmentScanner {
    pub async fn open(
        fragment: Fragment,
        dataset: Arc<Dataset>,
        projection: Arc<Schema>,
        predicate_projection: Arc<Schema>,
        predicate: Expr,
        config: ScanConfig,
    ) -> Result<Self> {
        let fragment = FileFragment::new(dataset.clone(), fragment);

        // We will call the reader with projections. In order for this to work
        // we must ensure that we open the fragment with the maximal schema.
        let mut frag_config = FragReadConfig::default();
        if let Some(file_reader_options) = config.file_reader_options.clone() {
            frag_config = frag_config.with_file_reader_options(file_reader_options);
        }
        let mut reader = fragment.open(dataset.schema(), frag_config).await?;
        if config.make_deletions_null {
            reader.with_make_deletions_null();
        }

        // We only need the statistics for the predicate projection.
        let stats = reader
            .legacy_read_page_stats(Some(&predicate_projection))
            .await?;

        Ok(Self {
            fragment,
            projection,
            predicate_projection,
            predicate,
            config,
            reader,
            stats,
        })
    }

    pub fn scan(self) -> Result<BoxStream<'static, Result<RecordBatch>>> {
        let batch_readahead = self.config.batch_readahead;
        let simplified_predicates = self.simplified_predicates()?;
        let ordered_output = self.config.ordered_output;

        let scanner = Arc::new(self);

        let stream = futures::stream::iter(simplified_predicates.into_iter().enumerate())
            // We can skip any batches where the predicate is guaranteed to be unsatisfied.
            // By skipping at this point, we prevent these batches from taking a slot in
            // the batch readahead buffer.
            .filter(|(_, predicate)| {
                futures::future::ready(!matches!(
                    predicate,
                    Expr::Literal(ScalarValue::Boolean(Some(false)), _)
                ))
            })
            .map(move |(batch_id, predicate)| {
                let scanner_ref = scanner.clone();
                tokio::task::spawn(async move { scanner_ref.read_batch(batch_id, predicate).await })
                    .map(|res| match res {
                        Ok(Ok(batch)) => Ok(batch),
                        Ok(Err(err)) => Err(err),
                        Err(join_err) => Err(DataFusionError::ExecutionJoin(Box::new(join_err))),
                    })
            });

        let stream = if ordered_output {
            stream
                .buffered(batch_readahead)
                // Batches that were completely skipped might be None, so we
                // filter them out here.
                .try_filter_map(|res: Option<RecordBatch>| futures::future::ready(Ok(res)))
                .boxed()
        } else {
            stream
                .buffer_unordered(batch_readahead)
                .try_filter_map(|res| futures::future::ready(Ok(res)))
                .boxed()
        };

        Ok(stream)
    }

    async fn read_batch(&self, batch_id: usize, predicate: Expr) -> Result<Option<RecordBatch>> {
        match predicate {
            Expr::Literal(ScalarValue::Boolean(Some(true)), _) => {
                // Predicate is always true, we just need to load the projection.
                let mut projection_reader = self.reader.clone();
                if self.config.with_row_id {
                    projection_reader.with_row_id();
                }
                if self.config.with_row_address {
                    projection_reader.with_row_address();
                }
                let batch = projection_reader
                    .legacy_read_batch_projected(batch_id, .., &self.projection)
                    .await?;
                let batch = self.final_projection(batch)?;
                Ok(Some(batch))
            }
            Expr::Literal(ScalarValue::Boolean(Some(false)), _) => {
                // Predicate is always false, we can skip this batch.
                Ok(None)
            }
            _ => {
                // Predicate is not always true or always false, we need to load
                // the filter columns to evaluate it.

                // 1. Load needed filter columns, which might be a subset of all filter
                //    columns if statistics obviated the need for some columns.
                let columns: Vec<_> = predicate
                    .column_refs()
                    .into_iter()
                    .map(|col| col.name.as_str())
                    .collect();
                let predicate_projection =
                    Arc::new(self.fragment.dataset().schema().project(&columns).unwrap());
                let mut reader = self.reader.clone();

                // Make deletions null so we can have correct indices when we
                // request additional columns. See ColumnarValue::Array branch below.
                reader.with_make_deletions_null();
                if self.config.with_row_id {
                    reader.with_row_id();
                }
                reader.with_row_address();

                let batch = reader
                    .legacy_read_batch_projected(batch_id, .., &predicate_projection)
                    .await?;

                // 2. Evaluate predicate
                let planner = Planner::new(batch.schema());
                let physical_expr = planner.create_physical_expr(&predicate)?;
                let result = physical_expr.evaluate(&batch)?;
                let selection = match result {
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))) => {
                        ReadBatchParams::RangeFull
                    }
                    ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))) => {
                        // Nothing matched
                        return Ok(None);
                    }
                    ColumnarValue::Scalar(ScalarValue::Boolean(None)) => {
                        // The predicate evaluated to null for all inputs.  Usually
                        // this means that all inputs were null.  When it comes to
                        // filtering, null means no
                        return Ok(None);
                    }
                    ColumnarValue::Array(array) => {
                        let array = array
                            .as_any()
                            .downcast_ref::<arrow_array::BooleanArray>()
                            .unwrap();

                        // Selection is a list of indices into the batch. These
                        // indices are physical, which is why we made deletions
                        // null earlier.
                        let selection: UInt32Array = array
                            .iter()
                            .zip(batch[ROW_ADDR].as_primitive::<UInt64Type>())
                            .enumerate()
                            .filter_map(|(i, (matched, row_addr))| {
                                if matched.unwrap_or_default() && row_addr.is_some() {
                                    Some(i as u32)
                                } else {
                                    None
                                }
                            })
                            .collect();

                        if selection.is_empty() {
                            // Nothing matched the predicate, so the result is empty.
                            return Ok(None);
                        }

                        ReadBatchParams::Indices(selection)
                    }
                    _ => {
                        return Err(DataFusionError::Internal(format!(
                            "Unexpected result from predicate evaluation: {:?}",
                            result
                        )));
                    }
                };

                // 3. Take the subset of rows that pass the predicate. Some of the
                //    columns in the projection may have already been loaded as a
                //    filter column, so we just load the remaining columns.
                let predicate_field_ids = predicate_projection.field_ids();
                let projection_field_ids = self.projection.field_ids();
                let remaining_fields: Vec<i32> = projection_field_ids
                    .iter()
                    .cloned()
                    .filter(|i| !predicate_field_ids.contains(i))
                    .collect();

                let remainder_batch = if !remaining_fields.is_empty() {
                    let remaining_projection =
                        self.projection.project_by_ids(&remaining_fields, true);
                    Some(
                        self.reader
                            .legacy_read_batch_projected(
                                batch_id,
                                selection.clone(),
                                &remaining_projection,
                            )
                            .await?,
                    )
                } else {
                    None
                };

                // 4. If filter columns are in the projection, merge them in
                //    and project the final schema.
                let batch = if let ReadBatchParams::Indices(indices) = &selection {
                    batch.take(indices)?
                } else {
                    // It's possible there were some deleted rows.
                    if let Some(deletion_mask) = batch[ROW_ADDR].nulls() {
                        filter_record_batch(
                            &batch,
                            &BooleanArray::new(deletion_mask.clone().into_inner(), None),
                        )?
                    } else {
                        batch
                    }
                };
                let batch = if let Some(remainder_batch) = remainder_batch {
                    debug_assert_eq!(batch.num_rows(), remainder_batch.num_rows());
                    batch.merge(&remainder_batch)?
                } else {
                    batch
                };

                let batch = self.final_projection(batch)?;

                Ok(Some(batch))
            }
        }
    }

    fn final_projection(&self, batch: RecordBatch) -> Result<RecordBatch> {
        let row_id_column = batch.column_by_name(ROW_ID).cloned();
        let row_addr_column = batch.column_by_name(ROW_ADDR).cloned();

        let mut batch = batch
            .project_by_schema(&self.projection.as_ref().into())
            .map_err(|err| {
                Error::internal(format!(
                    "Failed to select schema {} from batch with schema {}\nInner error: {}",
                    self.projection,
                    batch.schema(),
                    err
                ))
            })?;

        // Row id nor row address weren't part of the projection, so we need to
        // add them back if they were requested. We always put them at the front.
        if self.config.with_row_address {
            batch =
                batch.try_with_column_at(0, ROW_ADDR_FIELD.clone(), row_addr_column.unwrap())?;
        }
        if self.config.with_row_id {
            batch = batch.try_with_column_at(0, ROW_ID_FIELD.clone(), row_id_column.unwrap())?;
        }

        Ok(batch)
    }

    /// Parse the statistics into a set of guarantees for each batch.
    fn extract_guarantees<'a>(
        predicate_projection: &'a Schema,
        batch_sizes: &'a [usize],
        stats: &RecordBatch,
    ) -> impl Iterator<Item = Vec<(Expr, NullableInterval)>> + 'a {
        let mut null_counts: HashMap<i32, PrimitiveArray<Int64Type>> = HashMap::new();
        let mut min_values: HashMap<i32, Arc<dyn Array>> = HashMap::new();
        let mut max_values: HashMap<i32, Arc<dyn Array>> = HashMap::new();

        for field_id in predicate_projection.field_ids() {
            let field_stats = stats.column_by_name(&field_id.to_string());
            if let Some(field_stats) = field_stats {
                if !matches!(field_stats.data_type(), DataType::Struct(_)) {
                    log::error!(
                        "Invalid statistics: Field stats for field {} is not a struct, but a {}",
                        field_id,
                        field_stats.data_type()
                    );
                    continue;
                }
                let field_stats_ref = field_stats.as_struct();

                if let Some(null_count_col) = field_stats_ref.column_by_name("null_count") {
                    let null_count_col = null_count_col
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap();
                    null_counts.insert(field_id, null_count_col.clone());
                }

                if let Some(min_col) = field_stats_ref.column_by_name("min_value") {
                    min_values.insert(field_id, min_col.clone());
                }

                if let Some(max_col) = field_stats_ref.column_by_name("max_value") {
                    max_values.insert(field_id, max_col.clone());
                }
            }
        }

        (0..stats.num_rows())
            .map(move |batch_id| {
                let mut guarantees = Vec::new();
                for field in predicate_projection.fields_pre_order() {
                    let null_count = if field.nullable {
                        let maybe_null_count = null_counts.get(&field.id).map(|arr| arr.value(batch_id));
                        if let Some(null_count) = maybe_null_count {
                            null_count
                        } else {
                            continue
                        }
                    } else {
                        0
                    };

                    let min_value = {
                        let maybe_min_value = min_values.get(&field.id)
                            .map(|arr| ScalarValue::try_from_array(arr, batch_id));
                        match maybe_min_value {
                            Some(Ok(min_value)) => min_value,
                            Some(Err(err)) => {
                                log::error!("Invalid statistics: Failed to convert min_value for field {} to ScalarValue: {}", field.id, err);
                                continue
                            }
                            None => continue
                        }
                    };

                    let max_value = {
                        let maybe_max_value = max_values.get(&field.id)
                            .map(|arr| ScalarValue::try_from_array(arr, batch_id));
                        match maybe_max_value {
                            Some(Ok(max_value)) => max_value,
                            Some(Err(err)) => {
                                log::error!("Invalid statistics: Failed to convert max_value for field {} to ScalarValue: {}", field.id, err);
                                continue
                            }
                            None => continue
                        }
                    };

                    let values = Interval::try_new(min_value, max_value).unwrap();
                    let batch_size = batch_sizes[batch_id];
                    let interval = match (null_count, batch_size) {
                        (0, _) => NullableInterval::NotNull { values },
                        (null_count, batch_size) if null_count == batch_size as i64 => NullableInterval::Null { datatype: field.data_type() },
                        _ => NullableInterval::MaybeNull { values }
                    };
                    let column_path = predicate_projection.field_ancestry_by_id(field.id).unwrap();
                    let mut parts_iter = column_path.into_iter().map(|part| part.name.as_str());
                    let mut expr = col(parts_iter.next().unwrap());
                    for part in parts_iter {
                        expr = expr.field(part);
                    }
                    guarantees.push((expr, interval));
                }
                guarantees
            })
    }

    fn simplified_predicates(&self) -> Result<Vec<Expr>> {
        let num_batches = self.reader.legacy_num_batches();

        if let Some(stats) = &self.stats {
            let batch_sizes: Vec<usize> = (0..num_batches as u32)
                .map(|batch_id| {
                    self.reader
                        .legacy_num_rows_in_batch(batch_id)
                        .expect("Operation does not yet support v2 fragments")
                        as usize
                })
                .collect();
            let schema =
                Arc::new(ArrowSchema::from(self.predicate_projection.as_ref()).try_into()?);
            let context = SimplifyContext::builder().with_schema(schema).build();
            let mut simplifier = ExprSimplifier::new(context);

            let mut predicates = Vec::with_capacity(num_batches);
            for guarantees in
                Self::extract_guarantees(&self.predicate_projection, &batch_sizes, stats)
            {
                simplifier = simplifier.with_guarantees(guarantees);
                let simplified_expr = match simplifier.simplify(self.predicate.clone()) {
                    Ok(expr) => expr,
                    Err(err) => {
                        // TODO: this logs on each iteration, but maybe should should
                        // only log once per call of this func?
                        log::debug!("Failed to simplify predicate: {}", err);
                        self.predicate.clone()
                    }
                };

                predicates.push(simplified_expr);
            }
            Ok(predicates)
        } else {
            Ok(vec![self.predicate.clone(); num_batches])
        }
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{
        ArrayRef, DictionaryArray, FixedSizeListArray, Float32Array, Int32Array,
        RecordBatchIterator, StringArray, StructArray, TimestampMicrosecondArray, UInt64Array,
        types::{Float32Type, Int32Type},
    };
    use arrow_ord::sort::sort_to_indices;
    use arrow_schema::{Field, TimeUnit};
    use arrow_select::concat::concat_batches;
    use datafusion::prelude::{Column, SessionContext, lit};
    use lance_arrow::{FixedSizeListArrayExt, SchemaExt};
    use lance_core::utils::tempfile::TempStrDir;
    use lance_file::version::LanceFileVersion;
    use pretty_assertions::assert_eq;

    use crate::dataset::WriteParams;
    use lance_datafusion::logical_expr::ExprExt;

    use super::*;

    // TODO: test pushdown with nested column once https://github.com/apache/arrow-datafusion/pull/8256
    // is released.

    #[tokio::test]
    async fn test_empty_result() {
        // Test we can get no results
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "i",
            DataType::Int32,
            false,
        )]));
        let num_rows: usize = 10;
        let batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(0..num_rows as i32))],
            )
            .unwrap(),
        ];
        let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

        let dataset = Dataset::write(
            batches,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::Legacy),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let fragments = dataset.fragments().clone();
        let projection = Arc::new(dataset.schema().clone());

        let predicate = col("i").eq(lit(42));

        let exec = LancePushdownScanExec::try_new(
            Arc::new(dataset),
            fragments,
            projection,
            predicate,
            ScanConfig::default(),
        )
        .unwrap();

        let ctx = SessionContext::new();

        let results = exec.execute(0, ctx.task_ctx()).unwrap();
        assert_eq!(results.schema(), exec.schema());
        let results = results.try_collect::<Vec<_>>().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_null_batch() {
        // If every row in a batch is null then a predicate can evaluate to Scalar(NULL)
        // Ensure we handle that.
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "s",
            DataType::Utf8,
            true,
        )]));
        let num_rows: usize = 10;
        // Create a batch where every row is null
        let batches = vec![
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(StringArray::from_iter(
                    (0..num_rows).map(|_| Option::<String>::None),
                ))],
            )
            .unwrap(),
        ];
        let batches = RecordBatchIterator::new(batches.into_iter().map(Ok), schema.clone());

        let dataset = Dataset::write(
            batches,
            test_uri,
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::Legacy),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let fragments = dataset.fragments().clone();
        let projection = Arc::new(dataset.schema().clone());

        let predicate = col("s").eq(lit("x"));

        let exec = LancePushdownScanExec::try_new(
            Arc::new(dataset),
            fragments,
            projection,
            predicate,
            ScanConfig::default(),
        )
        .unwrap();

        let ctx = SessionContext::new();

        let results = exec.execute(0, ctx.task_ctx()).unwrap();
        assert_eq!(results.schema(), exec.schema());
        let results = results.try_collect::<Vec<_>>().await.unwrap();
        assert!(results.is_empty());
    }

    #[tokio::test]
    async fn test_nested_filter() {
        // Validate we can filter and project nested columns and they will be
        // merged correctly.
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let field_a = Arc::new(Field::new("a", DataType::Int32, false));
        let field_b = Arc::new(Field::new("b", DataType::Int32, false));
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new(
                "x",
                DataType::Struct(vec![field_a.clone(), field_b.clone()].into()),
                false,
            ),
            Field::new(
                "y",
                DataType::Struct(vec![field_a.clone(), field_b.clone()].into()),
                false,
            ),
        ]));

        let array = Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StructArray::from(vec![
                    (field_a.clone(), array.clone() as ArrayRef),
                    (field_b.clone(), array.clone() as ArrayRef),
                ])),
                Arc::new(StructArray::from(vec![
                    (field_a.clone(), array.clone() as ArrayRef),
                    (field_b.clone(), array.clone() as ArrayRef),
                ])),
            ],
        )
        .unwrap();

        let batches = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let dataset = Arc::new(
            Dataset::write(
                batches,
                test_uri,
                Some(WriteParams {
                    data_storage_version: Some(LanceFileVersion::Legacy),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        );

        let fragments = dataset.fragments().clone();
        // [x.b, y.a]
        let projection = Arc::new(dataset.schema().clone().project_by_ids(&[2, 4], true));

        let predicate = col("x")
            .field_newstyle("a")
            .lt(lit(8))
            .and(col("y").field_newstyle("b").gt(lit(3)));

        let exec = LancePushdownScanExec::try_new(
            dataset.clone(),
            fragments.clone(),
            projection,
            predicate.clone(),
            ScanConfig::default(),
        )
        .unwrap();

        let ctx = SessionContext::new();

        let results = exec.execute(0, ctx.task_ctx()).unwrap();
        let results = results.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 4);

        let expected_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("x", DataType::Struct(vec![field_b.clone()].into()), false),
            Field::new("y", DataType::Struct(vec![field_a.clone()].into()), false),
        ]));
        assert_eq!(results[0].schema().as_ref(), expected_schema.as_ref());

        // Also try where projection is same as filter columns
        let projection = Arc::new(dataset.schema().clone().project_by_ids(&[1, 5], true));
        let exec = LancePushdownScanExec::try_new(
            dataset.clone(),
            fragments,
            projection,
            predicate,
            ScanConfig::default(),
        )
        .unwrap();
        let results = exec.execute(0, ctx.task_ctx()).unwrap();
        let results = results.try_collect::<Vec<_>>().await.unwrap();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].num_rows(), 4);

        let expected_schema = Arc::new(ArrowSchema::new(vec![
            Field::new("x", DataType::Struct(vec![field_a.clone()].into()), false),
            Field::new("y", DataType::Struct(vec![field_b.clone()].into()), false),
        ]));
        assert_eq!(results[0].schema().as_ref(), expected_schema.as_ref());
    }

    #[tokio::test]
    async fn test_with_row_id() {
        // Want to hit all three code paths: all filtered out, partially filtered
        // out, and none filtered out.
        // Batches: [0..100], [100..200], [200..300]
        // Predicate: s.x >= 150
        // Expected simplification: false, s.x > 150, true
        // Expected result: [150..300]

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let field = Arc::new(Field::new("int", DataType::Int32, false));

        let arrow_schema = Arc::new(ArrowSchema::new(vec![field.clone()]));

        let batches = [0..100, 100..200, 200..300].map(|range| {
            RecordBatch::try_new(
                arrow_schema.clone(),
                vec![Arc::new(Int32Array::from_iter_values(range)) as ArrayRef],
            )
        });

        let batches = RecordBatchIterator::new(batches, arrow_schema.clone());

        let write_params = WriteParams {
            max_rows_per_group: 100,
            data_storage_version: Some(LanceFileVersion::Legacy),
            ..Default::default()
        };
        let dataset = Arc::new(
            Dataset::write(batches, test_uri, Some(write_params))
                .await
                .unwrap(),
        );

        let fragments = dataset.fragments().clone();
        assert_eq!(fragments.len(), 1);
        let fragment = fragments[0].clone();

        let predicate = col("int").gt_eq(lit(150));

        let schema = Arc::new(dataset.schema().clone());

        let fragment_scanner = FragmentScanner::open(
            fragment,
            dataset.clone(),
            schema.clone(),
            schema.clone(),
            predicate.clone(),
            ScanConfig {
                with_row_id: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let predicates = fragment_scanner.simplified_predicates().unwrap();
        assert_eq!(predicates.len(), 3);
        assert_eq!(&predicates[0], &lit(false));
        assert_eq!(&predicates[1], &predicate);
        assert_eq!(&predicates[2], &lit(true));

        let batch0 = fragment_scanner
            .read_batch(0, predicates[0].clone())
            .await
            .unwrap();
        assert!(batch0.is_none());

        let batch1 = fragment_scanner
            .read_batch(1, predicates[1].clone())
            .await
            .unwrap();
        assert!(batch1.is_some());
        let batch1 = batch1.unwrap();
        let expected_schema = Arc::new(ArrowSchema::new(vec![
            ROW_ID_FIELD.clone(),
            field.as_ref().clone(),
        ]));
        let expected = RecordBatch::try_new(
            expected_schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(150..200)),
                Arc::new(Int32Array::from_iter_values(150..200)),
            ],
        )
        .unwrap();
        assert_eq!(batch1, expected);

        let batch2 = fragment_scanner
            .read_batch(2, predicates[2].clone())
            .await
            .unwrap();
        assert!(batch2.is_some());
        let batch2 = batch2.unwrap();
        let expected = RecordBatch::try_new(
            expected_schema.clone(),
            vec![
                Arc::new(UInt64Array::from_iter_values(200..300)),
                Arc::new(Int32Array::from_iter_values(200..300)),
            ],
        )
        .unwrap();
        assert_eq!(batch2, expected);
    }

    fn test_data() -> RecordBatch {
        let arrow_schema = ArrowSchema::new(vec![
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
                false,
            ),
            Field::new(
                "struct",
                DataType::Struct(vec![Field::new("int", DataType::Int32, false)].into()),
                false,
            ),
            Field::new("str", DataType::Utf8, true),
        ]);

        let num_rows = 9;
        let values: Float32Array = (0..num_rows).flat_map(|i| vec![i as f32; 3]).collect();
        let vector = FixedSizeListArray::try_new_from_values(values, 3).unwrap();

        let struct_int = Int32Array::from_iter_values(0..num_rows);
        let struct_array = StructArray::new(
            vec![Field::new("int", DataType::Int32, false)].into(),
            vec![Arc::new(struct_int) as ArrayRef],
            None,
        );

        let strings = StringArray::from(vec![
            Some("a"),
            None,
            Some("b"),
            Some("c"),
            None,
            Some("d"),
            Some("e"),
            None,
            Some("g"),
        ]);

        RecordBatch::try_new(
            Arc::new(arrow_schema),
            vec![
                Arc::new(vector) as ArrayRef,
                Arc::new(struct_array) as ArrayRef,
                Arc::new(strings) as ArrayRef,
            ],
        )
        .unwrap()
    }

    async fn test_dataset() -> Dataset {
        let data = test_data();
        let schema = data.schema();
        let reader = RecordBatchIterator::new(vec![Ok(data)], schema);
        let params = WriteParams {
            max_rows_per_group: 3,
            data_storage_version: Some(LanceFileVersion::Legacy),
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, "memory://test", Some(params))
            .await
            .unwrap();

        dataset.delete("struct.int = 6").await.unwrap();

        dataset
    }

    #[tokio::test]
    async fn test_codepath_options() {
        struct TestCase {
            with_row_id: bool,
            make_deletions_null: bool,
            ordered_output: bool,
        }

        // This is a little galaxy-brained, but at least it's concise.
        let cases = (0..16).map(|i| TestCase {
            with_row_id: i & 1 != 0 || i & 2 != 0, // If make_deletions_null is true, with_row_id is required
            make_deletions_null: i & 2 != 0,
            ordered_output: i & 4 != 0,
        });

        let dataset = Arc::new(test_dataset().await);

        let predicate = col("struct")
            .field_newstyle("int")
            .gt(lit(4))
            .and(col(Column::from_name("str")).is_not_null());

        for case in cases {
            let config = ScanConfig {
                with_row_id: case.with_row_id,
                make_deletions_null: case.make_deletions_null,
                ordered_output: case.ordered_output,
                ..Default::default()
            };
            let scan = LancePushdownScanExec::try_new(
                dataset.clone(),
                dataset.fragments().clone(),
                Arc::new(dataset.schema().clone()),
                predicate.clone(),
                config,
            )
            .unwrap();

            let ctx = SessionContext::new();
            let batches = scan
                .execute(0, ctx.task_ctx())
                .unwrap()
                .try_collect::<Vec<_>>()
                .await
                .unwrap();
            assert_eq!(batches[0].schema(), scan.schema());
            let mut result = concat_batches(&batches[0].schema(), &batches).unwrap();

            let mut expected_schema = ArrowSchema::from(dataset.schema());
            if case.with_row_id {
                expected_schema = expected_schema
                    .try_with_column_at(0, ROW_ID_FIELD.clone())
                    .unwrap();
            }
            assert_eq!(result.schema().as_ref(), &expected_schema);

            if !case.ordered_output {
                let indices = sort_to_indices(
                    result.column_by_qualified_name("struct.int").unwrap(),
                    Default::default(),
                    None,
                )
                .unwrap();
                result = result.take(&indices).unwrap();
            }

            if case.make_deletions_null {
                let predicate = col(ROW_ID).is_not_null();
                let planner = Planner::new(result.schema());
                let physical_expr = planner.create_physical_expr(&predicate).unwrap();
                let mask = physical_expr
                    .evaluate(&result)
                    .unwrap()
                    .into_array(result.num_rows())
                    .unwrap();
                result = filter_record_batch(&result, mask.as_boolean()).unwrap();
            }

            assert_eq!(
                result
                    .column_by_qualified_name("struct.int")
                    .unwrap()
                    .as_primitive::<Int32Type>(),
                &Int32Array::from(vec![5, 8])
            );
        }
    }

    async fn pushdown_scan(
        ctx: &SessionContext,
        dataset: Arc<Dataset>,
        projection_indices: Vec<i32>,
        predicate: Expr,
        scan_config: ScanConfig,
    ) -> Result<RecordBatch> {
        let scan = LancePushdownScanExec::try_new(
            dataset.clone(),
            dataset.fragments().clone(),
            Arc::new(
                dataset
                    .schema()
                    .clone()
                    .project_by_ids(&projection_indices, true),
            ),
            predicate,
            scan_config,
        )
        .unwrap();
        let result: Vec<RecordBatch> = scan.execute(0, ctx.task_ctx())?.try_collect().await?;
        let schema = scan.schema();
        if result.is_empty() {
            Ok(RecordBatch::new_empty(schema))
        } else {
            Ok(concat_batches(&schema, &result)?)
        }
    }

    #[derive(Debug)]
    struct PredicateCombinationsTest {
        projection_indices: Vec<i32>,
        predicate: Expr,
        data: RecordBatch,
    }

    impl PredicateCombinationsTest {
        fn new(projection_indices: Vec<i32>, predicate: Expr) -> Self {
            Self {
                projection_indices,
                predicate,
                data: Self::data(),
            }
        }

        /// Generate the test data
        fn data() -> RecordBatch {
            // Properties for test data:
            // * Want a good mix of data types
            // * Want multiple batches (will do 3 rows per batch when writing dataset)
            let schema = Arc::new(ArrowSchema::new(vec![
                Field::new("int", DataType::Int32, false),
                Field::new("float", DataType::Float32, false),
                Field::new("str", DataType::Utf8, true),
                Field::new(
                    "timestamp",
                    DataType::Timestamp(TimeUnit::Microsecond, None),
                    false,
                ),
                Field::new(
                    "str_dict",
                    DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
                    true,
                ),
            ]));

            RecordBatch::try_new(
                schema,
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3, 4, 5, 6, 1, 3, 6])),
                    Arc::new(Float32Array::from(vec![
                        1.0,
                        2.0,
                        3.0,
                        4.0,
                        5.0,
                        6.0,
                        f32::NAN,
                        3.0,
                        6.0,
                    ])),
                    Arc::new(StringArray::from(vec![
                        Some("a"),
                        None,
                        Some("b"),
                        Some("c"),
                        None,
                        Some("d"),
                        Some("e"),
                        Some("f"),
                        Some("g"),
                    ])),
                    Arc::new(TimestampMicrosecondArray::from(vec![
                        1, 2, 3, 4, 5, 6, 1, 3, 6,
                    ])),
                    Arc::new(DictionaryArray::new(
                        // "a", null, "b", "a", "a", "a", null, null, "b"
                        Int32Array::from(vec![0, 1, 2, 0, 0, 0, 1, 1, 2]),
                        Arc::new(StringArray::from(vec![Some("a"), None, Some("b")])),
                    )),
                ],
            )
            .unwrap()
        }

        async fn dataset(uri: &str, data: RecordBatch) -> Dataset {
            let schema = data.schema();
            let reader = RecordBatchIterator::new(vec![Ok(data)], schema.clone());

            let write_params = WriteParams {
                max_rows_per_group: 3,
                data_storage_version: Some(LanceFileVersion::Legacy),
                ..Default::default()
            };
            Dataset::write(reader, uri, Some(write_params))
                .await
                .unwrap()
        }

        /// Generate the expected result using DataFusion executed on the test data.
        async fn expected_result(&self) -> RecordBatch {
            let ctx = SessionContext::new();
            let schema = self.data.schema();
            let schema_names = schema.field_names();
            let columns = self
                .projection_indices
                .iter()
                .map(|i| schema_names[*i as usize].as_str())
                .collect::<Vec<&str>>();
            let res = ctx
                .read_batch(self.data.clone())
                .unwrap()
                .filter(self.predicate.clone())
                .unwrap()
                .select_columns(&columns)
                .unwrap()
                .collect()
                .await
                .unwrap();
            concat_batches(&res[0].schema(), &res).unwrap()
        }

        async fn run_test(&self) {
            let test_uri = TempStrDir::default();

            let dataset = Arc::new(Self::dataset(&test_uri, self.data.clone()).await);
            let expected = self.expected_result().await;

            let ctx = SessionContext::new();

            let result = pushdown_scan(
                &ctx,
                dataset.clone(),
                self.projection_indices.clone(),
                self.predicate.clone(),
                Default::default(),
            )
            .await
            .unwrap();
            assert_eq!(&expected, &result, "Failed with: {:#?}", self);
        }
    }

    #[tokio::test]
    async fn test_predicate_combinations() {
        struct TestCase {
            projection_indices: Vec<i32>,
            predicate: Expr,
        }

        let cases = [
            TestCase {
                projection_indices: vec![0, 1, 2, 3, 4],
                predicate: col("int").eq(lit(1)),
            },
            TestCase {
                projection_indices: vec![1, 4],
                predicate: col("int").lt_eq(lit(3)),
            },
            TestCase {
                projection_indices: vec![1],
                predicate: col("float").gt(lit(2.0_f32)),
            },
            TestCase {
                projection_indices: vec![0, 1, 2, 3, 4],
                predicate: col("str").in_list(vec![lit("a"), lit("g")], false),
            },
            TestCase {
                projection_indices: vec![0, 1, 4],
                predicate: col("str").in_list(vec![lit("a"), lit("g")], true),
            },
            TestCase {
                projection_indices: vec![0, 1, 4],
                predicate: col("str").is_null().or(col("int").eq(lit(1))),
            },
            TestCase {
                projection_indices: vec![0, 1, 3],
                predicate: col("timestamp")
                    .lt(lit(ScalarValue::TimestampMicrosecond(Some(3), None))),
            },
            // TODO: I think there's something wrong with how we handle nulls with dictionaries.
            // TestCase {
            //     projection_indices: vec![0, 1, 4],
            //     predicate: col("str_dict").eq(lit("b")).or(col("str_dict").is_null()),
            // },
        ];

        for case in cases {
            let test = PredicateCombinationsTest::new(case.projection_indices, case.predicate);
            test.run_test().await;
        }
    }

    #[tokio::test]
    async fn test_retrieve_just_row_id() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "int",
            DataType::Int32,
            false,
        )]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..50))],
        )
        .unwrap();

        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let write_params = WriteParams {
            max_rows_per_group: 10,
            data_storage_version: Some(LanceFileVersion::Legacy),
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        // Modified each group in a different way:
        // Group 0: all kept
        // Group 1: all deleted
        // Group 2: first half deleted
        // Group 3: some indices deleted
        dataset
            .delete("(int >= 10 AND int < 25) OR int in (33, 35, 37)")
            .await
            .unwrap();

        let dataset = Arc::new(dataset.clone());
        let ctx = SessionContext::new();

        let result = pushdown_scan(
            &ctx,
            dataset.clone(),
            vec![],
            col("int").lt(lit(40)),
            ScanConfig {
                with_row_id: true,
                ..Default::default()
            },
        )
        .await
        .unwrap();
        assert_eq!(result.num_columns(), 1);
        let row_ids = result[ROW_ID].as_primitive::<UInt64Type>();
        assert_eq!(
            row_ids,
            &UInt64Array::from_iter_values((0..10).chain(25..33).chain([34, 36, 38, 39]))
        );
    }

    #[tokio::test]
    async fn test_with_deletions() {
        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();

        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("int", DataType::Int32, false),
            Field::new("float", DataType::Float32, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..12)),
                Arc::new(Float32Array::from_iter_values((0..12).map(|x| x as f32))),
            ],
        )
        .unwrap();

        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());

        let write_params = WriteParams {
            max_rows_per_group: 4,
            data_storage_version: Some(LanceFileVersion::Legacy),
            ..Default::default()
        };
        let mut dataset = Dataset::write(reader, test_uri, Some(write_params))
            .await
            .unwrap();

        // Delete group 1
        dataset.delete("int >= 4 AND int < 8").await.unwrap();
        // Delete part of group 2
        dataset.delete("int = 9").await.unwrap();

        let expected_int = [0, 1, 2, 3, 8, 10, 11];

        let dataset = Arc::new(dataset.clone());
        let ctx = SessionContext::new();

        for max_value in 0..12 {
            let result = pushdown_scan(
                &ctx,
                dataset.clone(),
                vec![0, 1],
                col("int").lt(lit(max_value)),
                ScanConfig {
                    with_row_id: true,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            assert_eq!(result.num_columns(), 3);

            let ints = result["int"].as_primitive::<Int32Type>();
            let expected =
                Int32Array::from_iter_values(expected_int.into_iter().filter(|x| x < &max_value));
            assert_eq!(ints, &expected, "Failed with max_value = {}", max_value);

            let floats = result["float"].as_primitive::<Float32Type>();
            let expected = Float32Array::from_iter_values(
                expected_int
                    .into_iter()
                    .filter(|x| x < &max_value)
                    .map(|x| x as f32),
            );
            assert_eq!(floats, &expected);
        }
    }
}
