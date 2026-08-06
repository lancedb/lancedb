// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use arrow_array::{Array, ArrayRef, RecordBatch, UInt64Array, cast::AsArray, types::UInt64Type};
use arrow_schema::{Schema, SchemaRef};
use datafusion::common::ColumnStatistics;
use datafusion::common::stats::Precision;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::{BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_physical_expr::EquivalenceProperties;
use datafusion_physical_plan::Statistics;
use datafusion_physical_plan::execution_plan::CardinalityEffect;
use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
use futures::{StreamExt, TryStreamExt};
use lance_core::utils::address::RowAddress;
use lance_core::utils::deletion::DeletionVector;
use lance_core::{
    Error as LanceError, ROW_ADDR, ROW_ADDR_FIELD, ROW_ID, ROW_OFFSET, ROW_OFFSET_FIELD,
    Result as LanceResult,
};
use lance_table::rowids::RowIdIndex;

use crate::Dataset;
use crate::dataset::rowids::get_row_id_index;
use crate::utils::future::SharedPrerequisite;

/// Add a `_rowaddr` column to a stream of record batches that have a `_rowid`.
///
/// It's generally more efficient to scan the `_rowaddr` column, but this can be
/// useful when reading secondary indices, which only have the `_rowid` column.
#[derive(Clone)]
pub struct AddRowAddrExec {
    input: Arc<dyn ExecutionPlan>,
    dataset: Arc<Dataset>,
    /// Task to get the rowid index. Is not initialized until the first call to
    /// `execute`.
    row_id_index: OnceLock<Arc<SharedPrerequisite<Option<Arc<RowIdIndex>>>>>,
    /// Position in the input schema where the rowids are located
    rowid_pos: usize,
    /// Position in the output schema where to insert the row address
    rowaddr_pos: usize,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,

    metrics: ExecutionPlanMetricsSet,
}

impl std::fmt::Debug for AddRowAddrExec {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("AddRowAddrExec")
            .field("input", &self.input)
            .field("dataset", &self.dataset)
            .field("rowid_pos", &self.rowid_pos)
            .field("rowaddr_pos", &self.rowaddr_pos)
            .field("output_schema", &self.output_schema)
            .field("properties", &self.properties)
            .finish()
    }
}

impl AddRowAddrExec {
    /// Create a new AddRowAddrExec node.
    ///
    /// This adds a `_rowaddr` column to streams where there is a `_rowid`
    /// column.
    ///
    /// # Errors
    ///
    /// If the `_rowid` field is not found in the input schema.
    ///
    /// # Arguments
    /// * `input` - The input plan to add row addresses to.
    /// * `dataset` - The dataset to get the row id index from.
    /// * `rowaddr_pos` - The position in the output schema where to insert the row address.
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
        rowaddr_pos: usize,
    ) -> Result<Self> {
        // Need to know the physical position of the row id field, so we don't
        // have to do a schema lookup for every batch.
        let input_schema = input.schema();
        let rowid_pos = input_schema
            .fields()
            .iter()
            .position(|f| f.name() == ROW_ID)
            .ok_or_else(|| {
                DataFusionError::Internal("rowid field not found in input schema".into())
            })?;

        let mut fields = input_schema.fields().iter().cloned().collect::<Vec<_>>();
        fields.insert(rowaddr_pos, Arc::new(ROW_ADDR_FIELD.clone()));
        let output_schema = Arc::new(Schema::new_with_metadata(
            fields,
            input_schema.metadata().clone(),
        ));

        let row_id_index = OnceLock::new();

        // Is just a simple projections, so it inherits the partitioning and
        // execution mode from parent.
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(EquivalenceProperties::new(output_schema.clone())),
        );

        Ok(Self {
            input,
            dataset,
            row_id_index,
            rowid_pos,
            rowaddr_pos,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
        })
    }

    fn compute_row_addrs(
        row_ids: &ArrayRef,
        row_id_index: Option<&RowIdIndex>,
    ) -> Result<ArrayRef> {
        let row_id_values = row_ids.as_primitive_opt::<UInt64Type>().ok_or_else(|| {
            DataFusionError::Internal("AddRowAddrExec: rowid column is not a UInt64Array".into())
        })?;
        if let Some(row_id_index) = row_id_index {
            if row_id_values.null_count() > 0 {
                let mut builder = arrow::array::UInt64Builder::with_capacity(row_id_values.len());
                for rowid in row_id_values.iter() {
                    if let Some(rowid) = rowid {
                        if let Some(row_addr) = row_id_index.get(rowid) {
                            builder.append_value(row_addr.into());
                        } else {
                            return Err(DataFusionError::Internal(format!(
                                "AddRowAddrExec: rowid not found in index: {}",
                                rowid
                            )));
                        }
                    } else {
                        builder.append_null();
                    }
                }
                Ok(Arc::new(builder.finish()))
            } else {
                // Fast path - no branching for null values
                let mut rowaddrs: Vec<u64> = Vec::with_capacity(row_id_values.len());
                for rowid in row_id_values.values() {
                    if let Some(row_addr) = row_id_index.get(*rowid) {
                        rowaddrs.push(row_addr.into());
                    } else {
                        return Err(DataFusionError::Internal(format!(
                            "AddRowAddrExec: rowid not found in index: {}",
                            rowid
                        )));
                    }
                }
                Ok(Arc::new(UInt64Array::from(rowaddrs)))
            }
        } else {
            // No index, then we should just copy the rowids
            Ok(row_ids.clone())
        }
    }

    fn do_execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let index_prereq = self
            .row_id_index
            .get_or_init(|| {
                let dataset = self.dataset.clone();
                let fut = async move { get_row_id_index(dataset.as_ref()).await };
                SharedPrerequisite::spawn(fut)
            })
            .clone();

        let input_stream = self.input.execute(partition, context)?;

        let rowid_pos = self.rowid_pos;
        let rowaddr_pos = self.rowaddr_pos;
        let output_schema = self.output_schema.clone();
        let baseline = BaselineMetrics::new(&self.metrics, partition);
        let elapsed_compute = baseline.elapsed_compute().clone();
        let stream = input_stream.then(move |batch_result| {
            let output_schema = output_schema.clone();
            let index_prereq = index_prereq.clone();
            let elapsed_compute = elapsed_compute.clone();
            async move {
                let batch = batch_result?;
                index_prereq.wait_ready().await?;
                let row_id_index = index_prereq.get_ready();
                let index_ref = row_id_index.as_deref();

                let _t = elapsed_compute.timer();
                let row_addr = Self::compute_row_addrs(batch.column(rowid_pos), index_ref)?;

                let mut columns = Vec::with_capacity(batch.num_columns() + 1);
                let existing_columns = batch.columns();
                columns.extend_from_slice(&existing_columns[..rowaddr_pos]);
                columns.push(row_addr);
                columns.extend_from_slice(&existing_columns[rowaddr_pos..]);

                Ok(RecordBatch::try_new(output_schema.clone(), columns)?)
            }
        });
        let stream = stream.map(move |batch| {
            let poll = baseline.record_poll(std::task::Poll::Ready(Some(batch)));
            match poll {
                std::task::Poll::Ready(Some(b)) => b,
                _ => unreachable!("record_poll preserves Ready(Some) input"),
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }
}

impl DisplayAs for AddRowAddrExec {
    fn fmt_as(
        &self,
        _format_type: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "AddRowAddrExec")
    }
}

impl ExecutionPlan for AddRowAddrExec {
    fn name(&self) -> &str {
        "AddRowAddrExec"
    }

    fn schema(&self) -> Arc<Schema> {
        self.output_schema.clone()
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        // We aren't doing much work here, best to avoid the thread overhead
        vec![false]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            Err(DataFusionError::Internal(
                "AddRowAddrExec: invalid number of children".into(),
            ))
        } else {
            Ok(Arc::new(Self::try_new(
                children.into_iter().next().unwrap(),
                self.dataset.clone(),
                self.rowaddr_pos,
            )?))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::context::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let schema = self.schema();
        let this = self.clone();
        let stream = futures::stream::once(async move { this.do_execute(partition, context) });
        let stream = stream.try_flatten();
        Ok(Box::pin(RecordBatchStreamAdapter::new(schema, stream)))
    }

    fn partition_statistics(
        &self,
        partition: Option<usize>,
    ) -> Result<Arc<datafusion::physical_plan::Statistics>> {
        let mut stats = Arc::unwrap_or_clone(self.input.partition_statistics(partition)?);

        let row_id_col_stats = stats.column_statistics.get(self.rowid_pos).ok_or_else(|| {
            DataFusionError::Internal("RowAddrExec: rowid column stats not found".into())
        })?;
        let row_addr_col_stats = ColumnStatistics {
            null_count: row_id_col_stats.null_count,
            distinct_count: row_id_col_stats.distinct_count,
            sum_value: Precision::Absent,
            max_value: Precision::Absent,
            min_value: Precision::Absent,
            byte_size: Precision::Absent,
        };

        let base_size = std::mem::size_of::<UInt64Array>();
        // Buffer size is the number of rows times 8 bytes per row, but there
        // is a minimum size of 64 bytes.
        let mut added_byte_size = stats
            .num_rows
            .map(|n| n * 8)
            .add(&Precision::Exact(base_size));
        if row_id_col_stats
            .null_count
            .get_value()
            .map(|v| *v > 0)
            .unwrap_or_default()
        {
            // Account for null buffer.
            added_byte_size = added_byte_size.add(&stats.num_rows.map(|n| n.div_ceil(8).max(64)));
        }
        stats.total_byte_size = stats.total_byte_size.add(&added_byte_size);
        stats
            .column_statistics
            .insert(self.rowaddr_pos, row_addr_col_stats);

        Ok(Arc::new(stats))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
}

#[derive(Debug)]
struct FragInfo {
    // The dataset offset of the first row in the fragment
    row_offset: u64,
    // The key is the cumulative row offset in the fragment, the value is the number of rows deleted so far
    deletion_vector: Option<Arc<DeletionVector>>,
}

/// Add a `_rowoffset` column to a stream of record batches that have a `_rowaddr` column.
///
/// The row offset is the number of rows between the current row and the first row in the dataset.
#[derive(Clone, Debug)]
pub struct AddRowOffsetExec {
    input: Arc<dyn ExecutionPlan>,
    row_addr_pos: usize,
    frag_id_to_offset: Arc<HashMap<u32, FragInfo>>,
    properties: Arc<PlanProperties>,
}

impl AddRowOffsetExec {
    fn internal_new(
        input: Arc<dyn ExecutionPlan>,
        frag_id_to_offset: Arc<HashMap<u32, FragInfo>>,
    ) -> LanceResult<Self> {
        let input_schema = input.schema();
        let row_addr_pos = input_schema.index_of(ROW_ADDR).map_err(|_| {
            LanceError::internal(format!("Input plan does not have a {} column", ROW_ADDR))
        })?;

        if input_schema.field_with_name(ROW_OFFSET).is_ok() {
            return Err(LanceError::internal(format!(
                "Input plan already has a {} column",
                ROW_OFFSET
            )));
        }

        let mut fields = input.schema().fields().iter().cloned().collect::<Vec<_>>();
        fields.push(Arc::new(ROW_OFFSET_FIELD.clone()));
        let schema = Arc::new(Schema::new_with_metadata(
            fields,
            input.schema().metadata().clone(),
        ));

        let new_eq_props =
            EquivalenceProperties::new(schema).extend(input.properties().eq_properties.clone())?;
        let properties = Arc::new(
            input
                .properties()
                .as_ref()
                .clone()
                .with_eq_properties(new_eq_props),
        );

        Ok(Self {
            input,
            row_addr_pos,
            frag_id_to_offset,
            properties,
        })
    }

    pub async fn try_new(
        input: Arc<dyn ExecutionPlan>,
        dataset: Arc<Dataset>,
    ) -> LanceResult<Self> {
        let frag_id_to_offset = Self::compute_frag_id_to_offset(dataset).await?;
        Self::internal_new(input, frag_id_to_offset)
    }

    async fn compute_frag_id_to_offset(
        dataset: Arc<Dataset>,
    ) -> LanceResult<Arc<HashMap<u32, FragInfo>>> {
        let mut frag_id_to_offset = HashMap::new();
        let mut row_offset = 0;
        for frag in dataset.get_fragments() {
            let deletion_vector = frag.get_deletion_vector().await?;
            frag_id_to_offset.insert(
                frag.id() as u32,
                FragInfo {
                    row_offset,
                    deletion_vector,
                },
            );
            // Should be sync unless the dataset was written by an extremely old lance version
            row_offset += frag.count_rows(None).await? as u64;
        }

        Ok(Arc::new(frag_id_to_offset))
    }

    pub async fn compute_row_offset_array(
        row_addr: &ArrayRef,
        dataset: Arc<Dataset>,
    ) -> Result<ArrayRef> {
        let frag_id_to_offset = Self::compute_frag_id_to_offset(dataset).await?;
        Self::compute_row_offsets(row_addr, frag_id_to_offset.as_ref())
    }

    fn compute_row_offsets(
        row_addr: &ArrayRef,
        frag_id_to_offset: &HashMap<u32, FragInfo>,
    ) -> Result<ArrayRef> {
        let row_addr_values = row_addr.as_primitive::<UInt64Type>().values();
        let mut row_offsets = vec![0; row_addr_values.len()];
        // The deletion iterator only moves forward, so compute in address order and
        // scatter the offsets back into input order.
        let mut sorted_row_indices = (0..row_addr_values.len()).collect::<Vec<_>>();
        sorted_row_indices.sort_unstable_by_key(|index| row_addr_values[*index]);

        let mut last_frag_id = u32::MAX;
        let mut last_frag_offset = 0;
        let mut last_frag_delete_count = 0;
        let mut dv_iter = None;

        for row_index in sorted_row_indices {
            let addr = RowAddress::new_from_u64(row_addr_values[row_index]);
            let frag_id = addr.fragment_id();
            if frag_id != last_frag_id {
                last_frag_id = frag_id;
                let Some(frag_info) = frag_id_to_offset.get(&frag_id) else {
                    return Err(DataFusionError::External(Box::new(LanceError::internal(
                        format!(
                            "A row address referred to a fragment {} that wasn't in the frag_id_to_offset map",
                            frag_id
                        ),
                    ))));
                };
                last_frag_offset = frag_info.row_offset;
                last_frag_delete_count = 0;
                dv_iter = frag_info
                    .deletion_vector
                    .as_ref()
                    .map(|dv| dv.to_sorted_iter().peekable());
            }

            let row_offset = addr.row_offset();
            if let Some(dv_iter) = &mut dv_iter {
                while dv_iter.peek().is_some() {
                    if *dv_iter.peek().unwrap() < row_offset {
                        dv_iter.next();
                        last_frag_delete_count += 1;
                    } else {
                        break;
                    }
                }
                row_offsets[row_index] =
                    last_frag_offset + row_offset as u64 - last_frag_delete_count as u64;
            } else {
                row_offsets[row_index] = last_frag_offset + row_offset as u64;
            }
        }

        Ok(Arc::new(UInt64Array::from(row_offsets)))
    }
}

impl DisplayAs for AddRowOffsetExec {
    fn fmt_as(
        &self,
        _format_type: DisplayFormatType,
        f: &mut std::fmt::Formatter,
    ) -> std::fmt::Result {
        write!(f, "AddRowOffsetExec")
    }
}

impl ExecutionPlan for AddRowOffsetExec {
    fn name(&self) -> &str {
        "AddRowOffsetExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn maintains_input_order(&self) -> Vec<bool> {
        vec![true]
    }

    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>> {
        self.input.partition_statistics(partition)
    }

    fn supports_limit_pushdown(&self) -> bool {
        true
    }

    fn cardinality_effect(&self) -> CardinalityEffect {
        CardinalityEffect::Equal
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            Err(DataFusionError::Internal(
                "AddRowOffsetExec: invalid number of children".into(),
            ))
        } else {
            Ok(Arc::new(Self::internal_new(
                children.into_iter().next().unwrap(),
                self.frag_id_to_offset.clone(),
            )?))
        }
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let input_stream = self.input.execute(partition, context)?;
        let schema = self.schema();
        let row_addr_pos = self.row_addr_pos;
        let frag_id_to_offset = self.frag_id_to_offset.clone();
        let stream = input_stream.then(move |batch| {
            let schema = schema.clone();
            let row_addr_pos = row_addr_pos;
            let frag_id_to_offset = frag_id_to_offset.clone();
            async move {
                let batch = batch?;
                let row_addr = batch.column(row_addr_pos);
                let row_offsets = Self::compute_row_offsets(row_addr, frag_id_to_offset.as_ref())?;
                let mut columns = batch.columns().to_vec();
                columns.push(Arc::new(row_offsets));
                Ok(RecordBatch::try_new(schema.clone(), columns)?)
            }
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            stream,
        )))
    }
}

#[cfg(test)]
mod test {
    use arrow_array::{Int32Array, RecordBatchIterator};
    use arrow_schema::{DataType, Field};
    use datafusion::{datasource::memory::MemorySourceConfig, prelude::SessionContext};
    use futures::TryStreamExt;
    use lance_core::{ROW_ADDR, ROW_ID_FIELD};
    use lance_datafusion::exec::OneShotExec;

    use crate::dataset::WriteParams;

    use super::*;

    async fn apply_to_batch(batch: RecordBatch, dataset: Arc<Dataset>) -> Result<RecordBatch> {
        let memory_exec = OneShotExec::from_batch(batch);
        let exec = AddRowAddrExec::try_new(Arc::new(memory_exec), dataset, 0)?;
        let session = SessionContext::new();
        let task_ctx = session.task_ctx();
        let stream = exec.execute(0, task_ctx)?;
        let batches = stream.try_collect::<Vec<_>>().await?;
        assert_eq!(batches.len(), 1);
        Ok(batches.into_iter().next().unwrap())
    }

    #[tokio::test]
    async fn test_address_style_ids() {
        // Creating a dataset with no stable row ids means that the row address
        // will be the same as the row id.
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let reader = RecordBatchIterator::new(vec![], schema.clone());
        let dataset = Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                enable_stable_row_ids: false,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let dataset = Arc::new(dataset);

        let rowids = Arc::new(UInt64Array::from(vec![1, 2, 3]));
        let schema = Schema::new(vec![ROW_ID_FIELD.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![rowids.clone()]).unwrap();

        let result = apply_to_batch(batch, dataset).await.unwrap();
        let result = result[ROW_ADDR].clone();

        assert_eq!(result.as_ref(), rowids.as_ref() as &dyn Array);
        // The array should be just a copy of the _rowid array pointer.
        assert_eq!(Arc::as_ptr(&result), Arc::as_ptr(&rowids));
    }

    async fn sample_dataset_with_rowid_index() -> Arc<Dataset> {
        // Create a row id index
        // 0 -> 0
        // 1 -> 1 << 32
        // 2 -> 2 << 32
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1, 2, 3]))],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let dataset = Dataset::write(
            reader,
            "memory://",
            Some(WriteParams {
                enable_stable_row_ids: true,
                max_rows_per_file: 1,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        assert_eq!(dataset.get_fragments().len(), 3);
        Arc::new(dataset)
    }

    #[tokio::test]
    async fn test_row_ids_no_nulls() {
        let dataset = sample_dataset_with_rowid_index().await;

        let rowids = Arc::new(UInt64Array::from(vec![0, 1, 2]));
        let schema = Schema::new(vec![ROW_ID_FIELD.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![rowids.clone()]).unwrap();

        let result = apply_to_batch(batch, dataset).await.unwrap();
        let result = result[ROW_ADDR].clone();

        assert_eq!(
            result.as_ref(),
            Arc::new(UInt64Array::from(vec![0, 1 << 32, 2 << 32])).as_ref() as &dyn Array
        );
    }

    #[tokio::test]
    async fn test_row_ids_with_nulls() {
        let dataset = sample_dataset_with_rowid_index().await;

        let rowids = Arc::new(UInt64Array::from(vec![Some(0), None, Some(2)]));
        let schema = Schema::new(vec![ROW_ID_FIELD.clone()]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![rowids.clone()]).unwrap();

        let result = apply_to_batch(batch, dataset).await.unwrap();
        let result = result[ROW_ADDR].clone();

        assert_eq!(
            result.as_ref(),
            Arc::new(UInt64Array::from(vec![Some(0), None, Some(2 << 32)])).as_ref() as &dyn Array
        );
    }

    #[tokio::test]
    async fn test_invalid_schema() {
        let dataset = sample_dataset_with_rowid_index().await;

        let rowids = Arc::new(Int32Array::from(vec![0, 1, 2]));
        let schema = Schema::new(vec![Field::new("invalid", DataType::Int32, true)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![rowids.clone()]).unwrap();

        let result = apply_to_batch(batch, dataset).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_stats() {
        let dataset = sample_dataset_with_rowid_index().await;

        let rowids = Arc::new(UInt64Array::from(vec![Some(0), None, Some(2)]));
        let schema = Arc::new(Schema::new(vec![ROW_ID_FIELD.clone()]));
        let batch = RecordBatch::try_new(schema.clone(), vec![rowids.clone()]).unwrap();

        let memory_exec =
            MemorySourceConfig::try_new_exec(&[vec![batch.clone()]], schema, None).unwrap();
        let exec = AddRowAddrExec::try_new(memory_exec, dataset.clone(), 0).unwrap();
        let stats = exec.partition_statistics(None).unwrap();
        let result = apply_to_batch(batch, dataset).await.unwrap();

        assert_eq!(stats.num_rows, Precision::Exact(3));
        assert_eq!(stats.column_statistics.len(), 2);
        assert_eq!(stats.column_statistics[0].null_count, Precision::Exact(1));
        assert_eq!(stats.column_statistics[1].null_count, Precision::Exact(1));

        let actual_byte_size = result
            .columns()
            .iter()
            .fold(0, |acc, col| acc + col.get_array_memory_size());
        assert_eq!(stats.total_byte_size, Precision::Exact(actual_byte_size));
    }

    #[test]
    fn test_row_offsets_with_unsorted_addresses() {
        let row_addrs: ArrayRef = Arc::new(UInt64Array::from(vec![
            u64::from(RowAddress::new_from_parts(0, 100)),
            u64::from(RowAddress::new_from_parts(0, 50)),
        ]));
        let frag_id_to_offset = HashMap::from([(
            0,
            FragInfo {
                row_offset: 1_000,
                deletion_vector: Some(Arc::new(DeletionVector::from_iter([10, 60]))),
            },
        )]);

        let row_offsets =
            AddRowOffsetExec::compute_row_offsets(&row_addrs, &frag_id_to_offset).unwrap();

        assert_eq!(
            row_offsets.as_primitive::<UInt64Type>().values(),
            &[1_098, 1_049]
        );
    }
}
