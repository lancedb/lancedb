// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! FtsIndexExec - Full-text search with MVCC visibility.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use arrow_array::builder::{ListBuilder, UInt32Builder};
use arrow_array::{BooleanArray, Float32Array, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use datafusion::common::ScalarValue;
use datafusion::common::stats::Precision;
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::TaskContext;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::metrics::{ExecutionPlanMetricsSet, MetricsSet};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
    SendableRecordBatchStream, Statistics,
};
use datafusion_physical_expr::{EquivalenceProperties, PhysicalExprRef};
use futures::stream::{self, StreamExt};
use lance_core::{Error, Result};
use lance_index::scalar::inverted::DOC_INDEX_FIELD;

use super::super::builder::{FtsQuery, FtsQueryType};
use super::newest_pk_positions;
use crate::dataset::mem_wal::index::{FtsQueryExpr, SearchOptions};
use crate::dataset::mem_wal::scanner::exec::resolve_pk_indices;
use crate::dataset::mem_wal::write::{BatchStore, IndexStore};

/// Score column name in output.
pub const SCORE_COLUMN: &str = "_score";

/// Batch range info for efficient row position lookup.
#[derive(Debug, Clone)]
struct BatchRange {
    start: usize,
    end: usize,
    batch_id: usize,
}

type MaterializedFtsRows = (
    Vec<Arc<dyn arrow_array::Array>>,
    Vec<f32>,
    Vec<u64>,
    Vec<Option<Vec<u32>>>,
);

/// ExecutionPlan node that queries FTS index with MVCC visibility.
pub struct FtsIndexExec {
    batch_store: Arc<BatchStore>,
    indexes: Arc<IndexStore>,
    query: FtsQuery,
    visible_count: usize,
    projection: Option<Vec<usize>>,
    output_schema: SchemaRef,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    /// Pre-computed batch ranges for O(log n) lookup.
    batch_ranges: Vec<BatchRange>,
    /// Maximum visible row position based on visible_count (None if nothing visible).
    max_visible_row: Option<u64>,
    /// Whether to include _rowid column (row position) in output.
    with_row_id: bool,
    /// Whether results identify element documents with `_doc_index`.
    with_doc_index: bool,
    /// Optional prefilter predicate, compiled against the memtable schema.
    /// Applied to the materialized full-schema hits before projection so the
    /// FTS arm only returns rows matching the predicate.
    filter: Option<PhysicalExprRef>,
    /// Primary-key columns. When set, materialized hits are kept only if their
    /// row position is the newest visible version of that PK.
    pk_columns: Option<Vec<String>>,
}

impl Debug for FtsIndexExec {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FtsIndexExec")
            .field("column", &self.query.column)
            .field("query_type", &self.query.query_type)
            .field("visible_count", &self.visible_count)
            .field("with_row_id", &self.with_row_id)
            .finish()
    }
}

impl FtsIndexExec {
    /// Create a new FtsIndexExec.
    ///
    /// # Arguments
    ///
    /// * `batch_store` - Lock-free batch store containing data
    /// * `indexes` - Index registry with FTS indexes
    /// * `query` - FTS query parameters
    /// * `visible_count` - MVCC visibility sequence number
    /// * `projection` - Optional column indices to project
    /// * `base_schema` - Schema before adding score column (and _rowid if with_row_id)
    /// * `with_row_id` - Whether to include _rowid column (row position)
    pub fn new(
        batch_store: Arc<BatchStore>,
        indexes: Arc<IndexStore>,
        query: FtsQuery,
        visible_count: usize,
        projection: Option<Vec<usize>>,
        base_schema: SchemaRef,
        with_row_id: bool,
    ) -> Result<Self> {
        // Verify the index exists for this column
        let column = &query.column;
        let Some(_index) =
            indexes.get_fts_by_column_and_granularity(column, query.document_granularity)
        else {
            return Err(Error::invalid_input(format!(
                "No FTS index found for column '{}'",
                column
            )));
        };
        let with_doc_index = query.document_granularity.is_list_element();

        // Build output schema: base fields + optional _doc_index + _score + optional _rowid
        let mut fields: Vec<Field> = base_schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone())
            .collect();
        if with_doc_index {
            fields.push(DOC_INDEX_FIELD.clone());
        }
        // `_score` is nullable here to stay schema-compatible with
        // `lance_index::scalar::inverted::FTS_SCHEMA` (the schema base/SSTable
        // FTS exec nodes emit). The LSM `full_text_search` planner unions the
        // active arm with base/SSTable arms; UnionExec requires schema equality
        // including nullability. The actual emitted column is always populated.
        fields.push(Field::new(SCORE_COLUMN, DataType::Float32, true));
        if with_row_id {
            fields.push(Field::new(lance_core::ROW_ID, DataType::UInt64, true));
        }
        let output_schema = Arc::new(Schema::new(fields));

        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(output_schema.clone()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));

        // Pre-compute batch ranges for O(log n) lookup and max visible row
        let mut batch_ranges = Vec::new();
        let mut current_row = 0usize;
        let mut max_visible_row_exclusive: u64 = 0;

        for (batch_id, stored_batch) in batch_store.iter().enumerate() {
            let batch_start = current_row;
            let batch_end = current_row + stored_batch.num_rows;
            batch_ranges.push(BatchRange {
                start: batch_start,
                end: batch_end,
                batch_id,
            });
            if batch_id < visible_count {
                max_visible_row_exclusive = batch_end as u64;
            }
            current_row = batch_end;
        }

        // Convert exclusive end to inclusive last position, or None if nothing visible
        let max_visible_row = if max_visible_row_exclusive > 0 {
            Some(max_visible_row_exclusive - 1)
        } else {
            None
        };

        Ok(Self {
            batch_store,
            indexes,
            query,
            visible_count,
            projection,
            output_schema,
            properties,
            metrics: ExecutionPlanMetricsSet::new(),
            batch_ranges,
            max_visible_row,
            with_row_id,
            with_doc_index,
            filter: None,
            pk_columns: None,
        })
    }

    /// Attach an optional prefilter predicate (compiled against the memtable
    /// schema). Hits that fail the predicate are dropped before projection.
    pub fn with_filter(mut self, filter: Option<PhysicalExprRef>) -> Self {
        self.filter = filter;
        self
    }

    /// Provide primary-key columns for newest-version filtering.
    pub fn with_pk_columns(mut self, pk_columns: Option<Vec<String>>) -> Self {
        self.pk_columns = pk_columns;
        self
    }

    /// Find batch for a row position using binary search. O(log n).
    #[inline]
    fn find_batch(&self, row_pos: usize) -> Option<&BatchRange> {
        // Binary search: find the batch where start <= row_pos < end
        let idx = self.batch_ranges.partition_point(|b| b.end <= row_pos);
        self.batch_ranges
            .get(idx)
            .filter(|b| row_pos >= b.start && row_pos < b.end)
    }

    /// Query the index and return matching rows with BM25 scores.
    fn query_index(&self) -> Vec<(u64, Option<Vec<u32>>, f32)> {
        let Some(index) = self
            .indexes
            .get_fts_by_column_and_granularity(&self.query.column, self.query.document_granularity)
        else {
            return vec![];
        };

        // Convert FtsQueryType to FtsQueryExpr
        let query_expr = match &self.query.query_type {
            FtsQueryType::Match {
                query,
                operator,
                boost,
            } => FtsQueryExpr::match_query_with_operator(query, *operator).with_boost(*boost),
            FtsQueryType::Phrase { query, slop } => FtsQueryExpr::phrase_with_slop(query, *slop),
            FtsQueryType::Boolean {
                must,
                should,
                must_not,
            } => {
                let mut builder = FtsQueryExpr::boolean();
                for term in must {
                    builder = builder.must(FtsQueryExpr::match_query(term));
                }
                for term in should {
                    builder = builder.should(FtsQueryExpr::match_query(term));
                }
                for term in must_not {
                    builder = builder.must_not(FtsQueryExpr::match_query(term));
                }
                builder.build()
            }
            FtsQueryType::Fuzzy {
                query,
                fuzziness,
                prefix_length,
                max_expansions,
                boost,
            } => {
                FtsQueryExpr::fuzzy_with_options(query, *fuzziness, *prefix_length, *max_expansions)
                    .with_boost(*boost)
            }
        };

        let all_rows_visible = self.batch_ranges.last().is_none_or(|last| {
            self.max_visible_row
                .map(|max_visible| max_visible + 1 >= last.end as u64)
                .unwrap_or(last.end == 0)
        });
        let pk_recency_is_noop = self.pk_columns.is_none()
            || (self.indexes.has_pk_index() && !self.indexes.pk_has_overrides());
        let can_prune_in_index = self.filter.is_none() && pk_recency_is_noop && all_rows_visible;

        // Search the index using the query expression. WAND pruning is only
        // safe when the index search itself sees the final candidate set.
        let mut options = SearchOptions::new().with_include_tail(self.query.include_tail);
        if can_prune_in_index {
            options = options.with_wand_factor(self.query.wand_factor);
            if let Some(limit) = self.query.limit {
                options = options.with_limit(limit);
            }
        }
        let entries = index.search_with_options(&query_expr, options);

        // Convert to (row_position, element ordinal, score) tuples.
        entries
            .into_iter()
            .map(|entry| (entry.row_position, entry.doc_index, entry.score))
            .collect()
    }

    /// Filter results by MVCC visibility using max_row_position. O(n).
    fn filter_by_visibility(
        &self,
        results: Vec<(u64, Option<Vec<u32>>, f32)>,
    ) -> Vec<(u64, Option<Vec<u32>>, f32)> {
        let Some(max_visible) = self.max_visible_row else {
            return vec![];
        };
        results
            .into_iter()
            .filter(|(pos, _, _)| *pos <= max_visible)
            .collect()
    }

    /// Materialize rows from batch store preserving input order (for sorted results).
    ///
    /// This method processes results one at a time to preserve the score-sorted order,
    /// then combines them into a single batch.
    fn materialize_rows_sorted(
        &self,
        results: &[(u64, Option<Vec<u32>>, f32)],
    ) -> DataFusionResult<Vec<RecordBatch>> {
        if results.is_empty() {
            return Ok(vec![]);
        }

        // Process each result in order to preserve sorting
        let mut all_rows: Vec<u32> = Vec::with_capacity(results.len());
        let mut all_scores: Vec<f32> = Vec::with_capacity(results.len());
        let mut all_row_positions: Vec<u64> = Vec::with_capacity(results.len());
        let mut all_doc_indices: Vec<Option<Vec<u32>>> = Vec::with_capacity(results.len());
        let mut all_columns: Vec<Vec<Arc<dyn arrow_array::Array>>> = Vec::new();

        // Initialize column vectors based on first batch's schema
        let first_batch = self.batch_store.get(0);
        if let Some(stored) = first_batch {
            for _ in 0..stored.data.num_columns() {
                all_columns.push(Vec::with_capacity(results.len()));
            }
        }

        for (pos, doc_index, score) in results {
            let pos = *pos;
            let score = *score;
            if let Some(batch_range) = self.find_batch(pos as usize)
                && let Some(stored) = self.batch_store.get(batch_range.batch_id)
            {
                let row_in_batch = (pos as usize - batch_range.start) as u32;
                let indices = UInt32Array::from(vec![row_in_batch]);

                // Take each column value
                for (col_idx, col) in stored.data.columns().iter().enumerate() {
                    let taken = arrow_select::take::take(col.as_ref(), &indices, None).unwrap();
                    if all_columns.len() <= col_idx {
                        all_columns.push(Vec::new());
                    }
                    all_columns[col_idx].push(taken);
                }

                all_rows.push(row_in_batch);
                all_scores.push(score);
                all_row_positions.push(pos);
                all_doc_indices.push(doc_index.clone());
            }
        }

        if all_scores.is_empty() {
            return Ok(vec![]);
        }

        // Concatenate all column arrays
        let mut final_columns: Vec<Arc<dyn arrow_array::Array>> = Vec::new();

        for col_arrays in &all_columns {
            if !col_arrays.is_empty() {
                let refs: Vec<&dyn arrow_array::Array> =
                    col_arrays.iter().map(|a| a.as_ref()).collect();
                let concatenated = arrow_select::concat::concat(&refs)?;
                final_columns.push(concatenated);
            }
        }

        // Prefilter: evaluate the predicate against the full-schema hits and drop
        // non-matching rows before applying the query limit and projection (a
        // NULL result excludes the row, matching SQL). When a predicate exists,
        // query_index deliberately avoids pushing the limit into the index so
        // this remains an exact prefilter, not a lossy post-filter.
        let (final_columns, all_scores, all_row_positions, all_doc_indices) =
            if let Some(ref predicate) = self.filter {
                let Some(first) = self.batch_store.get(0) else {
                    return Ok(vec![]);
                };
                let data_batch = RecordBatch::try_new(first.data.schema(), final_columns)?;
                let mask = predicate
                    .evaluate(&data_batch)?
                    .into_array(data_batch.num_rows())?;
                let mask = mask
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .ok_or_else(|| {
                        datafusion::error::DataFusionError::Internal(
                            "FTS prefilter predicate did not evaluate to boolean".to_string(),
                        )
                    })?;
                let filtered_columns = data_batch
                    .columns()
                    .iter()
                    .map(|c| arrow_select::filter::filter(c.as_ref(), mask))
                    .collect::<std::result::Result<Vec<_>, _>>()?;
                let filtered_scores: Vec<f32> = all_scores
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(s, keep)| keep.unwrap_or(false).then_some(*s))
                    .collect();
                let filtered_positions: Vec<u64> = all_row_positions
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(p, keep)| keep.unwrap_or(false).then_some(*p))
                    .collect();
                let filtered_doc_indices: Vec<Option<Vec<u32>>> = all_doc_indices
                    .iter()
                    .zip(mask.iter())
                    .filter(|(_, keep)| keep.unwrap_or(false))
                    .map(|(index, _)| index.clone())
                    .collect();
                (
                    filtered_columns,
                    filtered_scores,
                    filtered_positions,
                    filtered_doc_indices,
                )
            } else {
                (
                    final_columns,
                    all_scores,
                    all_row_positions,
                    all_doc_indices,
                )
            };

        let (mut final_columns, mut all_scores, mut all_row_positions, mut all_doc_indices) = self
            .filter_to_newest_pk(
                final_columns,
                all_scores,
                all_row_positions,
                all_doc_indices,
            )?;

        if all_scores.is_empty() {
            return Ok(vec![]);
        }

        if let Some(limit) = self.query.limit
            && all_scores.len() > limit
        {
            final_columns = final_columns
                .into_iter()
                .map(|column| column.slice(0, limit))
                .collect();
            all_scores.truncate(limit);
            all_row_positions.truncate(limit);
            all_doc_indices.truncate(limit);
        }

        if self.with_doc_index {
            let mut builder = ListBuilder::new(UInt32Builder::new()).with_field(Field::new(
                "item",
                DataType::UInt32,
                false,
            ));
            for doc_index in all_doc_indices {
                let doc_index = doc_index.ok_or_else(|| {
                    datafusion::error::DataFusionError::Internal(
                        "element-document FTS result is missing its document coordinate"
                            .to_string(),
                    )
                })?;
                builder.values().append_slice(&doc_index);
                builder.append(true);
            }
            final_columns.push(Arc::new(builder.finish()));
        }

        // Add score column
        final_columns.push(Arc::new(Float32Array::from(all_scores)));

        // Apply projection if needed
        let mut projected_columns = if let Some(ref proj_indices) = self.projection {
            let mut projected: Vec<_> = proj_indices
                .iter()
                .map(|&i| final_columns[i].clone())
                .collect();
            if self.with_doc_index {
                projected.push(final_columns[final_columns.len() - 2].clone());
            }
            // Always include score as last column
            projected.push(final_columns.last().unwrap().clone());
            projected
        } else {
            final_columns
        };

        // Add _rowid column if requested
        if self.with_row_id {
            projected_columns.push(Arc::new(UInt64Array::from(all_row_positions)));
        }

        let batch = RecordBatch::try_new(self.output_schema.clone(), projected_columns)?;
        Ok(vec![batch])
    }

    fn filter_to_newest_pk(
        &self,
        final_columns: Vec<Arc<dyn arrow_array::Array>>,
        all_scores: Vec<f32>,
        all_row_positions: Vec<u64>,
        all_doc_indices: Vec<Option<Vec<u32>>>,
    ) -> DataFusionResult<MaterializedFtsRows> {
        let Some(pk_columns) = &self.pk_columns else {
            return Ok((
                final_columns,
                all_scores,
                all_row_positions,
                all_doc_indices,
            ));
        };
        if pk_columns.is_empty() || all_scores.is_empty() {
            return Ok((
                final_columns,
                all_scores,
                all_row_positions,
                all_doc_indices,
            ));
        }
        let Some(max_visible_row) = self.max_visible_row else {
            return Ok((
                final_columns,
                all_scores,
                all_row_positions,
                all_doc_indices,
            ));
        };
        if self.indexes.has_pk_index() && !self.indexes.pk_has_overrides() {
            return Ok((
                final_columns,
                all_scores,
                all_row_positions,
                all_doc_indices,
            ));
        }
        let Some(first) = self.batch_store.get(0) else {
            return Ok((
                final_columns,
                all_scores,
                all_row_positions,
                all_doc_indices,
            ));
        };
        let newest_positions = if self.indexes.has_pk_index() {
            None
        } else {
            Some(newest_pk_positions(
                &self.batch_store,
                pk_columns,
                self.visible_count,
                max_visible_row,
            )?)
        };

        let data_batch = RecordBatch::try_new(first.data.schema(), final_columns)?;
        let pk_indices = resolve_pk_indices(&data_batch, pk_columns)?;
        let keep = (0..data_batch.num_rows())
            .map(|row| {
                Ok(match &newest_positions {
                    Some(newest) => newest.contains(&all_row_positions[row]),
                    None => {
                        let values: Vec<ScalarValue> = pk_indices
                            .iter()
                            .map(|&col| ScalarValue::try_from_array(data_batch.column(col), row))
                            .collect::<DataFusionResult<_>>()?;
                        self.indexes
                            .pk_is_newest(&values, all_row_positions[row], max_visible_row)
                    }
                })
            })
            .collect::<DataFusionResult<Vec<_>>>()?;

        let mask = BooleanArray::from_iter(keep.iter().copied());
        let filtered_columns = data_batch
            .columns()
            .iter()
            .map(|c| arrow_select::filter::filter(c.as_ref(), &mask))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let filtered_scores = all_scores
            .into_iter()
            .zip(keep.iter())
            .filter_map(|(s, keep)| keep.then_some(s))
            .collect();
        let filtered_positions = all_row_positions
            .into_iter()
            .zip(keep.iter())
            .filter_map(|(p, keep)| keep.then_some(p))
            .collect();
        let filtered_doc_indices = all_doc_indices
            .into_iter()
            .zip(keep.iter())
            .filter_map(|(index, keep)| keep.then_some(index))
            .collect();

        Ok((
            filtered_columns,
            filtered_scores,
            filtered_positions,
            filtered_doc_indices,
        ))
    }
}

impl DisplayAs for FtsIndexExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut Formatter<'_>) -> std::fmt::Result {
        match t {
            DisplayFormatType::Default | DisplayFormatType::Verbose => {
                write!(
                    f,
                    "FtsIndexExec: column={}, query_type={:?}, with_row_id={}",
                    self.query.column, self.query.query_type, self.with_row_id
                )
            }
            DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "FtsIndexExec\ncolumn={}\nquery_type={:?}\nwith_row_id={}",
                    self.query.column, self.query.query_type, self.with_row_id
                )
            }
        }
    }
}

impl ExecutionPlan for FtsIndexExec {
    fn name(&self) -> &str {
        "FtsIndexExec"
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
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if !children.is_empty() {
            return Err(datafusion::error::DataFusionError::Internal(
                "FtsIndexExec does not have children".to_string(),
            ));
        }
        Ok(self)
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        // Query the index
        let results = self.query_index();

        // Filter by visibility
        let mut visible_results = self.filter_by_visibility(results);

        // Sort by score descending (best matches first)
        visible_results.sort_by(|a, b| b.2.partial_cmp(&a.2).unwrap_or(std::cmp::Ordering::Equal));

        // Materialize the rows (preserving sort order)
        let batches = self.materialize_rows_sorted(&visible_results)?;

        let stream = stream::iter(batches.into_iter().map(Ok)).boxed();

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.output_schema.clone(),
            stream,
        )))
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DataFusionResult<Arc<Statistics>> {
        Ok(Arc::new(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![],
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
    use super::*;
    use arrow::array::AsArray;
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::{Array, Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use futures::TryStreamExt;
    use lance_index::scalar::InvertedIndexParams;
    use lance_index::scalar::inverted::{DOC_INDEX_COL, DocumentGranularity};

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("text", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &Schema, start_id: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![start_id, start_id + 1, start_id + 2])),
                Arc::new(StringArray::from(vec![
                    "hello world",
                    "goodbye world",
                    "hello again",
                ])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_fts_index_search() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        // Create index registry with FTS index on "text" (field_id = 1)
        let mut registry = IndexStore::new();
        registry.add_fts("text_idx".to_string(), 1, "text".to_string());

        // Insert test data and update index
        let batch = create_test_batch(&schema, 0);
        registry.insert(&batch, 0).unwrap();
        batch_store.append(batch).unwrap();

        let indexes = Arc::new(registry);

        let query = FtsQuery::match_query("text", "hello");

        let exec = FtsIndexExec::new(batch_store, indexes, query, 1, None, schema, false).unwrap();

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        // "hello" appears in docs 0 and 2
        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2);

        // Check that _score column exists
        let result_schema = batches[0].schema();
        assert!(result_schema.field_with_name(SCORE_COLUMN).is_ok());
    }

    #[tokio::test]
    async fn test_element_document_fts_index_search() {
        let mut tags = ListBuilder::new(StringBuilder::new());
        tags.values().append_value("alpha beta");
        tags.values().append_value("beta gamma");
        tags.append(true);
        tags.values().append_value("beta");
        tags.append(true);
        let tags = tags.finish();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("tags", tags.data_type().clone(), true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![0, 1])), Arc::new(tags)],
        )
        .unwrap();
        let batch_store = Arc::new(BatchStore::with_capacity(10));
        let mut registry = IndexStore::new();
        registry
            .add_fts_with_params(
                "tags_element_idx".to_string(),
                1,
                "tags".to_string(),
                InvertedIndexParams::default()
                    .document_granularity(DocumentGranularity::ListElement),
            )
            .unwrap();
        registry.insert(&batch, 0).unwrap();
        batch_store.append(batch).unwrap();

        let exec = FtsIndexExec::new(
            batch_store,
            Arc::new(registry),
            FtsQuery::match_query("tags", "beta")
                .with_document_granularity(DocumentGranularity::ListElement),
            1,
            None,
            schema,
            false,
        )
        .unwrap();
        let stream = exec.execute(0, Arc::new(TaskContext::default())).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        let mut hits = Vec::new();
        for batch in batches {
            let ids = batch["id"].as_primitive::<arrow::datatypes::Int32Type>();
            let coordinates = batch[DOC_INDEX_COL].as_list::<i32>();
            for row in 0..batch.num_rows() {
                let coordinate = coordinates
                    .value(row)
                    .as_primitive::<arrow::datatypes::UInt32Type>()
                    .value(0);
                hits.push((ids.value(row), coordinate));
            }
        }
        hits.sort_unstable();
        assert_eq!(hits, vec![(0, 0), (0, 1), (1, 0)]);
    }

    #[tokio::test]
    async fn test_fts_index_visibility() {
        let schema = create_test_schema();
        let batch_store = Arc::new(BatchStore::with_capacity(100));

        let mut registry = IndexStore::new();
        registry.add_fts("text_idx".to_string(), 1, "text".to_string());

        // Insert two batches at positions 0 and 1
        // Each batch has 3 rows, so batch1 has rows 0-2, batch2 has rows 3-5
        let batch1 = create_test_batch(&schema, 0);
        let batch2 = create_test_batch(&schema, 5);
        registry.insert(&batch1, 0).unwrap();
        registry.insert(&batch2, 3).unwrap(); // start_row_id=3 since batch1 has 3 rows
        batch_store.append(batch1).unwrap();
        batch_store.append(batch2).unwrap();

        let indexes = Arc::new(registry);

        let query = FtsQuery::match_query("text", "hello");

        // Query with max_visible=0 should only see first batch
        let exec = FtsIndexExec::new(
            batch_store.clone(),
            indexes.clone(),
            query.clone(),
            1,
            None,
            schema.clone(),
            false,
        )
        .unwrap();

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2); // "hello" in batch1 docs 0 and 2

        // Query with max_visible=1 should see both batches
        let exec = FtsIndexExec::new(batch_store, indexes, query, 2, None, schema, false).unwrap();

        let ctx = Arc::new(TaskContext::default());
        let stream = exec.execute(0, ctx).unwrap();
        let batches: Vec<RecordBatch> = stream.try_collect().await.unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 4); // "hello" in both batches
    }

    #[test]
    fn test_score_column_name() {
        assert_eq!(SCORE_COLUMN, "_score");
    }
}
