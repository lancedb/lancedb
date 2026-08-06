// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! The merge insert operation merges a batch of new data into an existing batch of old data.  This can be
//! used to implement a bulk update-or-insert (upsert), bulk delete or find-or-create operation.  It can also be used to
//! replace a specified region of data with new data (e.g. replace the data for the month of January)
//!
//! The terminology for this operation can be slightly confusing.  We try and stick with the terminology from
//! SQL.  The "target table" is the OLD data that already exists.  The "source table" is the NEW data which is
//! being inserted into the dataset.
//!
//! In order for this operation to work we need to be able to match rows from the source table with rows in the
//! target table.  For example, given a row we need to know if this is a brand-new row or matches an existing row.
//!
//! This match condition is currently limited to a key-match.  This means we consider a row to be a match if the
//! key columns are identical in both the source and the target.  This means that you will need some kind of
//! meaningful key column to be able to perform a merge insert.

// Internal column name for the merge action. Using "__action" to avoid collisions with user columns.
const MERGE_ACTION_COLUMN: &str = "__action";
// ## NULL-safe source row detection via sentinel column
//
// The merge join uses standard SQL equality for ON columns, which means NULL != NULL.
// After an outer join we need to know for each output row whether it came from the
// source side, the target side, or both.  The naive approach — checking whether an ON
// column IS NOT NULL — is wrong: a source row whose ON column is legitimately NULL is
// indistinguishable from a NULL introduced by the outer join on the target side.
//
// Solution: inject a `lit(true)` sentinel into every source row *before* the join.
// After the join:
//   - source rows (matched or unmatched)  → sentinel = true   (never NULL)
//   - target-only rows                    → sentinel = NULL   (outer-join fill)
//
// `assign_action` then uses `sentinel IS NOT NULL` instead of key-column IS NOT NULL
// to determine which side each row came from.  The sentinel is stripped by
// `prepare_stream_schema` and never written to the dataset.
pub(super) const MERGE_SOURCE_SENTINEL: &str = "__merge_source_sentinel";

pub mod inserted_rows;

use assign_action::merge_insert_action;
use inserted_rows::KeyExistenceFilter;

use super::cleanup_data_fragments;
use super::retry::{RetryConfig, RetryExecutor, execute_with_retry};
use super::{
    CommitBuilder, TargetBaseInfo, WriteMode, WriteParams,
    validate_and_resolve_target_bases_with_primary, write_fragments_internal,
};
use crate::dataset::rowids::get_row_id_index;
use crate::dataset::transaction::UpdateMode::{RewriteColumns, RewriteRows};
use crate::dataset::utils::CapturedRowIds;
use crate::index::DatasetIndexExt;
use crate::{
    Dataset,
    datafusion::dataframe::SessionContextExt,
    dataset::{
        fragment::{FileFragment, FragReadConfig},
        transaction::{Operation, Transaction},
        write::merge_insert::logical_plan::MergeInsertPlanner,
    },
    index::DatasetIndexInternalExt,
    io::exec::{
        AddRowAddrExec, Planner, TakeExec,
        filtered_read::{FilteredReadExec, FilteredReadOptions},
        project,
        scalar_index::{IndexLookup, MapIndexExec},
        utils::ReplayExec,
    },
};
use arrow_array::{
    BooleanArray, RecordBatch, RecordBatchIterator, StructArray, UInt32Array, UInt64Array,
    cast::AsArray, types::UInt64Type,
};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::take::take_record_batch;
use datafusion::common::NullEquality;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::error::DataFusionError;
use datafusion::{
    catalog::{TableProvider, streaming::StreamingTable},
    datasource::MemTable,
    execution::{
        context::{SessionConfig, SessionContext},
        memory_pool::MemoryConsumer,
    },
    logical_expr::{self, Expr, Extension, JoinType, LogicalPlan},
    physical_plan::{
        ColumnarValue, ExecutionPlan, PhysicalExpr, SendableRecordBatchStream,
        display::DisplayableExecutionPlan,
        joins::{HashJoinExec, PartitionMode},
        projection::ProjectionExec,
        repartition::RepartitionExec,
        sorts::sort::SortExec,
        stream::RecordBatchStreamAdapter,
        streaming::PartitionStream,
        union::UnionExec,
    },
    physical_planner::{DefaultPhysicalPlanner, PhysicalPlanner},
    prelude::DataFrame,
    scalar::ScalarValue,
};
use datafusion_physical_expr::expressions::Column;
use futures::{
    Stream, StreamExt, TryStreamExt,
    stream::{self},
};
use lance_arrow::json::{convert_json_columns, has_json_fields, is_arrow_json_field};
use lance_arrow::{RecordBatchExt, SchemaExt, interleave_batches};
use lance_core::datatypes::NullabilityComparison;
use lance_core::utils::address::RowAddress;
use lance_core::{
    Error, ROW_ADDR, ROW_ADDR_FIELD, ROW_ID, ROW_ID_FIELD, Result,
    datatypes::{OnMissing, OnTypeMismatch, SchemaCompareOptions},
    error::{InvalidInputSnafu, box_error},
    utils::{futures::Capacity, tokio::get_num_compute_intensive_cpus},
};
use lance_datafusion::{
    chunker::chunk_stream,
    dataframe::BatchStreamGrouper,
    exec::{
        HardCapBatchSizeExec, LanceExecutionOptions, OneShotExec, OneShotPartitionStream,
        analyze_plan, execute_plan, get_session_context, provider_to_stream,
    },
    spill::spilling_table_provider,
    utils::{StreamingWriteSource, reader_to_stream},
};
use lance_file::version::LanceFileVersion;
use lance_index::IndexCriteria;
use lance_index::mem_wal::CompactedSsTable;
use lance_select::RowAddrTreeMap;
use lance_table::format::{Fragment, IndexMetadata, RowIdMeta};
use log::info;
use roaring::RoaringTreemap;
use snafu::ResultExt;
use std::collections::HashMap;
use std::{
    collections::{BTreeMap, HashSet},
    iter::Peekable,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::task::JoinSet;
use tracing::error;

mod assign_action;
mod exec;
mod logical_plan;

struct UpdatedRowAddrReconciler<I>
where
    I: Iterator<Item = (u64, (usize, usize))>,
{
    updated_rows: Peekable<I>,
}

impl<I> UpdatedRowAddrReconciler<I>
where
    I: Iterator<Item = (u64, (usize, usize))>,
{
    fn new(updated_rows: I) -> Self {
        Self {
            updated_rows: updated_rows.peekable(),
        }
    }

    fn reconcile_batch(&mut self, original_row_addrs: &[u64]) -> Result<Vec<(usize, usize)>> {
        let mut indices = Vec::with_capacity(original_row_addrs.len());

        for (original_offset, original_row_addr) in original_row_addrs.iter().enumerate() {
            match self.updated_rows.peek().copied() {
                Some((updated_row_addr, updated_row_index))
                    if updated_row_addr == *original_row_addr =>
                {
                    self.updated_rows.next();
                    indices.push(updated_row_index);
                }
                Some((updated_row_addr, _)) if updated_row_addr < *original_row_addr => {
                    return Err(Self::missing_row_error(
                        updated_row_addr,
                        Some(*original_row_addr),
                    ));
                }
                _ => indices.push((0, original_offset)),
            }
        }

        Ok(indices)
    }

    fn finish(mut self) -> Result<()> {
        if let Some((updated_row_addr, _)) = self.updated_rows.next() {
            Err(Self::missing_row_error(updated_row_addr, None))
        } else {
            Ok(())
        }
    }

    fn missing_row_error(updated_row_addr: u64, next_original_row_addr: Option<u64>) -> Error {
        let updated_row_addr = RowAddress::from(updated_row_addr);
        let position = next_original_row_addr.map_or_else(
            || "no target rows remain".to_string(),
            |row_addr| format!("next target row address is {}", RowAddress::from(row_addr)),
        );
        Error::internal(format!(
            "Merge insert update row address {updated_row_addr} is missing from the target fragment; {position}"
        ))
    }
}

// "update if" expressions typically compare fields from the source table to the target table.
// These tables have the same schema and so filter expressions need to differentiate.  To do that
// we wrap the left side and the right side in a struct and make a single "combined schema"
fn combined_schema(schema: &Schema) -> Schema {
    let target = Field::new("target", DataType::Struct(schema.fields.clone()), false);
    let source = Field::new("source", DataType::Struct(schema.fields.clone()), false);
    Schema::new(vec![source, target])
}

// This takes a double-wide table (e.g. the result of the outer join below) and takes the left
// half, puts it into a struct, then takes the right half, and puts that into a struct.  This
// makes the table match the "combined schema" so we can apply an "update if" expression
fn unzip_batch(batch: &RecordBatch, schema: &Schema) -> RecordBatch {
    // The schema of the combined batches will be:
    // target_data_keys, target_data_non_keys, target_data_row_id, source_data_keys, source_data_non_keys
    // The keys and non_keys on both sides will be equal
    let num_fields = batch.num_columns();
    debug_assert_eq!(num_fields % 2, 1);
    let half_num_fields = num_fields / 2;
    let row_id_col = num_fields - 1;

    let source_arrays = batch.columns()[0..half_num_fields].to_vec();
    let source = StructArray::new(schema.fields.clone(), source_arrays, None);

    let target_arrays = batch.columns()[half_num_fields..row_id_col].to_vec();
    let target = StructArray::new(schema.fields.clone(), target_arrays, None);

    let combined_schema = combined_schema(schema);
    RecordBatch::try_new(
        Arc::new(combined_schema),
        vec![Arc::new(source), Arc::new(target)],
    )
    .unwrap()
}

/// Format key values for error messages via extracting "on" column values from the given RecordBatch.
pub fn format_key_values_on_columns(
    batch: &RecordBatch,
    row_idx: usize,
    on_columns: &[String],
) -> String {
    let mut on_values = Vec::new();

    for col_name in on_columns {
        if let Some(col_idx) = batch.schema().column_with_name(col_name) {
            let column = batch.column(col_idx.0);
            let value_str = if column.is_null(row_idx) {
                "NULL".to_string()
            } else {
                // Convert the value to string representation
                match ScalarValue::try_from_array(column, row_idx) {
                    Ok(scalar_value) => match &scalar_value {
                        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
                            format!("\"{}\"", s)
                        }
                        _ => scalar_value.to_string(),
                    },
                    Err(_) => format!("<{:?}>", column.data_type()),
                }
            };
            on_values.push(format!("{} = {}", col_name, value_str));
        }
    }

    if on_values.is_empty() {
        "<unable to extract on column values>".to_string()
    } else {
        on_values.join(", ")
    }
}

/// Create duplicate rows error via extracting "on" column values from the given RecordBatch.
pub fn create_duplicate_row_error(
    batch: &RecordBatch,
    row_idx: usize,
    on_columns: &[String],
) -> DataFusionError {
    DataFusionError::External(Box::new(Error::invalid_input(format!(
        "Ambiguous merge inserts are prohibited: multiple source rows match the same target row on ({}). \
                    Please ensure each target row is matched by at most one source row.",
        format_key_values_on_columns(batch, row_idx, on_columns)
    ))))
}

/// Tracks non-null join keys for source rows that will be inserted.
///
/// NULL join keys are deliberately not tracked because merge insert uses SQL
/// equality, where a key containing NULL does not equal another such key.
#[derive(Debug, Default)]
struct InsertedKeyTracker {
    keys: HashSet<Vec<ScalarValue>>,
}

impl InsertedKeyTracker {
    /// Returns true when the row has a new key or a key containing NULL.
    fn insert(
        &mut self,
        batch: &RecordBatch,
        row_idx: usize,
        on_columns: &[String],
    ) -> datafusion::common::Result<bool> {
        let mut key = Vec::with_capacity(on_columns.len());
        for column_name in on_columns {
            let column = batch.column_by_name(column_name).ok_or_else(|| {
                DataFusionError::Internal(format!(
                    "merge insert key column '{}' not found in source batch",
                    column_name
                ))
            })?;
            let value = ScalarValue::try_from_array(column, row_idx)?;
            if value.is_null() {
                return Ok(true);
            }
            key.push(value);
        }
        Ok(self.keys.insert(key))
    }
}

/// Describes how rows should be handled when there is no matching row in the source table
///
/// These are old rows which do not match any new data
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum WhenNotMatchedBySource {
    /// Do not delete rows from the target table
    ///
    /// This can be used for a find-or-create or an upsert operation
    Keep,
    /// Delete all rows from target table that don't match a row in the source table
    Delete,
    /// Delete rows from the target table if there is no match AND the expression evaluates to true
    ///
    /// This can be used to replace a region of data with new data
    DeleteIf(Expr),
}

impl WhenNotMatchedBySource {
    /// Create an instance of WhenNotMatchedBySource::DeleteIf from
    /// an SQL filter string
    ///
    /// This will parse the filter string (using the schema of the provided
    /// dataset) and simplify the resulting expression
    pub fn delete_if(dataset: &Dataset, expr: &str) -> Result<Self> {
        let planner = Planner::new(Arc::new(dataset.schema().into()));
        let expr = planner
            .parse_filter(expr)
            .map_err(box_error)
            .context(InvalidInputSnafu {})?;
        let expr = planner
            .optimize_expr(expr)
            .map_err(box_error)
            .context(InvalidInputSnafu {})?;
        Ok(Self::DeleteIf(expr))
    }
}

/// Describes how rows should be handled when there is a match between the target table and source table
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum WhenMatched {
    /// The row is deleted from the target table and a new row is inserted based on the source table
    ///
    /// This can be used to achieve upsert behavior
    UpdateAll,
    /// The row is kept unchanged
    ///
    /// This can be used to achieve find-or-create behavior
    DoNothing,
    /// The row is updated (similar to UpdateAll) only for rows where the expression evaluates to
    /// true
    UpdateIf(String),
    /// The row is updated (similar to UpdateAll) only for rows where the expression evaluates to
    /// true
    UpdateIfExpr(Expr),
    /// Fail the operation if a match is found
    ///
    /// This can be used to ensure that no existing rows are overwritten or modified after inserted.
    Fail,
    /// The matching row is deleted from the target table
    ///
    /// This can be used for bulk deletion by matching on key columns.
    /// Unlike UpdateAll, no new row is inserted - the matched row is simply removed.
    Delete,
}

impl WhenMatched {
    pub fn update_if(_dataset: &Dataset, expr: &str) -> Result<Self> {
        // Store the expression string and defer parsing until we know which path to take
        Ok(Self::UpdateIf(expr.to_string()))
    }

    pub fn update_if_expr(expr: Expr) -> Self {
        Self::UpdateIfExpr(expr)
    }
}

/// Describes how rows should be handled when there is no matching row in the target table
///
/// These are new rows which do not match any old data
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum WhenNotMatched {
    /// The new row is inserted into the target table
    ///
    /// This is used in both find-or-create and upsert operations
    InsertAll,
    /// The new row is ignored
    DoNothing,
}

/// Describes how to handle duplicate source rows.
///
/// If the source contains duplicates and `FirstSeen` behavior doesn't match your needs,
/// sort the source data before passing it to the merge insert operation.
/// Rows whose join keys contain NULL are not duplicates because merge insert uses SQL
/// equality, where NULL does not equal NULL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord, Default)]
pub enum SourceDedupeBehavior {
    /// Fail if multiple source rows match the same target row (default)
    #[default]
    Fail,
    /// Keep the first row for each join key and skip subsequent rows
    ///
    /// This applies both to rows that match a target row and to unmatched rows that
    /// would otherwise insert the same non-null join key more than once.
    FirstSeen,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
struct MergeInsertParams {
    // The column(s) to join on
    on: Vec<String>,
    // If true, then update all columns of the old data to the new data when there is a match
    when_matched: WhenMatched,
    // If true, then insert all columns of the new data when there is no match in the old data
    insert_not_matched: bool,
    // Controls whether data that is not matched by the source is deleted or not
    delete_not_matched_by_source: WhenNotMatchedBySource,
    conflict_retries: u32,
    // When the source is a one-shot stream and `conflict_retries > 0`, the source
    // is spilled (memory, then disk) so it can be replayed on each retry. Set to
    // false to fail fast on contention instead of buffering the stream. Has no
    // effect on re-scannable sources (materialized batches, files), which never
    // spill.
    spill_for_retry: bool,
    retry_timeout: Duration,
    // MemWAL SSTables to mark as compacted when this commit succeeds.
    compacted_sstables: Vec<CompactedSsTable>,
    // If true, skip auto cleanup during commits. This should be set to true
    // for high frequency writes to improve performance. This is also useful
    // if the writer does not have delete permissions and the clean up would
    // just try and log a failure anyway.
    skip_auto_cleanup: bool,
    // Controls whether to use indices for the merge operation. Default is true.
    // Setting to false forces a full table scan even if an index exists.
    use_index: bool,
    // Controls how to handle duplicate source rows that match the same target row.
    source_dedupe_behavior: SourceDedupeBehavior,
    // Number of inner commit retries for manifest version conflicts. Default is 20.
    commit_retries: Option<u32>,
    // Target base IDs for routing new fragments, mirroring WriteParams::target_bases.
    target_bases: Option<Vec<u32>>,
    // Target base names or path URIs (unresolved), mirroring
    // WriteParams::target_base_names_or_paths. Resolved at execution time.
    target_base_names_or_paths: Option<Vec<String>>,
    // Target all registered bases, mirroring WriteParams::target_all_bases.
    // Some(include_primary); resolved at execution time.
    target_all_bases: Option<bool>,
}

/// A MergeInsertJob inserts new rows, deletes old rows, and updates existing rows all as
/// part of a single transaction.
#[derive(Clone)]
pub struct MergeInsertJob {
    // The column to merge the new data into
    dataset: Arc<Dataset>,
    // The parameters controlling how to merge the two streams
    params: MergeInsertParams,
}

/// Build a merge insert operation.
///
/// This operation is similar to SQL's MERGE statement. It allows you to merge
/// new data with existing data.
///
/// Use the [MergeInsertBuilder] to construct an merge insert job.
///
/// If the `on` parameter is empty, the builder will fall back to the
/// schema's unenforced primary key (if configured). If neither `on` nor a
/// primary key is available, this constructor returns an error.
/// For example:
///
/// ```
/// # use lance::{Dataset, Result};
/// # use lance::dataset::{MergeInsertBuilder, WhenNotMatched, WhenNotMatchedBySource};
/// # use datafusion::physical_plan::SendableRecordBatchStream;
/// # use datafusion::prelude::Expr;
/// # use std::sync::Arc;
/// # async fn example(dataset: Arc<Dataset>, new_data1: SendableRecordBatchStream, new_data2: SendableRecordBatchStream, new_data3: SendableRecordBatchStream, month_eq_jan: Expr) -> Result<()> {
/// // find-or-create, insert new rows only
/// let (updated_dataset, _stats) = MergeInsertBuilder::try_new(dataset.clone(), vec!["my_key".to_string()])?
///     .try_build()?
///     .execute(new_data1)
///     .await?;
///
/// // upsert, insert or update
/// let (updated_dataset, _stats) = MergeInsertBuilder::try_new(dataset.clone(), vec!["my_key".to_string()])?
///     .when_not_matched(WhenNotMatched::InsertAll)
///     .try_build()?
///     .execute(new_data2)
///     .await?;
///
/// // replace data for month=january
/// let (updated_dataset, _stats) = MergeInsertBuilder::try_new(dataset.clone(), vec!["my_key".to_string()])?
///     .when_not_matched(WhenNotMatched::InsertAll)
///     .when_not_matched_by_source(WhenNotMatchedBySource::DeleteIf(month_eq_jan))
///     .try_build()?
///     .execute(new_data3)
///     .await?;
/// # Ok(())
/// # }
/// ```
///
#[derive(Debug, Clone)]
pub struct MergeInsertBuilder {
    dataset: Arc<Dataset>,
    params: MergeInsertParams,
}

impl MergeInsertBuilder {
    /// Creates a new builder
    ///
    /// By default this will build a job that has the same semantics as find-or-create
    ///  - matching rows will be kept as-is
    ///  - new rows in the new data will be inserted
    ///  - rows in the old data that do not match will be left as-is
    ///
    /// Use the methods on this builder to customize that behavior
    pub fn try_new(dataset: Arc<Dataset>, on: Vec<String>) -> Result<Self> {
        // Determine the join keys to use. If `on` is empty, fall back to the
        // schema's unenforced primary key (if configured).
        let resolved_on = if on.is_empty() {
            let schema = dataset.schema();
            let pk_fields = schema.unenforced_primary_key();

            if pk_fields.is_empty() {
                return Err(Error::invalid_input(
                    "A merge insert operation requires join keys: specify `on` columns explicitly or configure a primary key in the dataset schema",
                ));
            }

            pk_fields
                .iter()
                .map(|field| schema.field_path(field.id))
                .collect::<Result<Vec<_>>>()?
        } else {
            // Resolve column names using case-insensitive matching to handle
            // lowercased column names from SQL parsing or user input
            on.iter()
                .map(|col| {
                    dataset
                        .schema()
                        .field_case_insensitive(col)
                        .map(|f| f.name.clone())
                        .ok_or_else(|| {
                            Error::invalid_input(format!(
                                "Merge insert key column '{}' does not exist in schema",
                                col
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?
        };

        Ok(Self {
            dataset,
            params: MergeInsertParams {
                on: resolved_on,
                when_matched: WhenMatched::DoNothing,
                insert_not_matched: true,
                delete_not_matched_by_source: WhenNotMatchedBySource::Keep,
                conflict_retries: 10,
                spill_for_retry: true,
                retry_timeout: Duration::from_secs(30),
                compacted_sstables: Vec::new(),
                skip_auto_cleanup: false,
                use_index: true,
                source_dedupe_behavior: SourceDedupeBehavior::Fail,
                commit_retries: None,
                target_bases: None,
                target_base_names_or_paths: None,
                target_all_bases: None,
            },
        })
    }

    /// Specify what should happen when a target row matches a row in the source
    pub fn when_matched(&mut self, behavior: WhenMatched) -> &mut Self {
        self.params.when_matched = behavior;
        self
    }

    /// Specify what should happen when a source row has no match in the target
    ///
    /// These are typically "new rows"
    pub fn when_not_matched(&mut self, behavior: WhenNotMatched) -> &mut Self {
        self.params.insert_not_matched = match behavior {
            WhenNotMatched::DoNothing => false,
            WhenNotMatched::InsertAll => true,
        };
        self
    }

    /// Specify what should happen when a target row has no match in the source
    ///
    /// These are typically "old rows"
    pub fn when_not_matched_by_source(&mut self, behavior: WhenNotMatchedBySource) -> &mut Self {
        self.params.delete_not_matched_by_source = behavior;
        self
    }

    /// Set number of times to retry the operation if there is contention.
    ///
    /// If this is set > 0, then the operation will keep a copy of the input data
    /// either in memory or on disk (depending on the size of the data) and will
    /// retry the operation if there is contention.
    ///
    /// Default is 10.
    pub fn conflict_retries(&mut self, retries: u32) -> &mut Self {
        self.params.conflict_retries = retries;
        self
    }

    /// Controls whether a one-shot stream source is spilled so it can be replayed
    /// across retries.
    ///
    /// When the source is a one-shot stream (e.g. [`MergeInsertJob::execute`]) and
    /// `conflict_retries > 0`, the source is buffered in memory and spilled to disk
    /// so each retry can re-read it. Set this to `false` to skip that buffering and
    /// fail fast with a contention error instead of writing the stream to disk.
    ///
    /// This has no effect on re-scannable sources (materialized batches via
    /// [`MergeInsertJob::execute_batches`], or a [`TableProvider`] via
    /// [`MergeInsertJob::execute_provider`]), which are replayed directly and never
    /// spill.
    ///
    /// Default is true.
    ///
    /// [`TableProvider`]: datafusion::catalog::TableProvider
    pub fn spill_for_retry(&mut self, spill: bool) -> &mut Self {
        self.params.spill_for_retry = spill;
        self
    }

    /// Set the timeout used to limit retries.
    ///
    /// This is the maximum time to spend on the operation before giving up. At
    /// least one attempt will be made, regardless of how long it takes to complete.
    /// Subsequent attempts will be cancelled once this timeout is reached. If
    /// the timeout has been reached during the first attempt, the operation
    /// will be cancelled immediately.
    ///
    /// The default is 30 seconds.
    pub fn retry_timeout(&mut self, timeout: Duration) -> &mut Self {
        self.params.retry_timeout = timeout;
        self
    }

    pub fn skip_auto_cleanup(&mut self, skip: bool) -> &mut Self {
        self.params.skip_auto_cleanup = skip;
        self
    }

    /// Controls whether to use indices for the merge operation.
    ///
    /// When set to false, forces a full table scan even if an index exists on the join key.
    /// This can be useful for benchmarking or when the optimizer chooses a suboptimal path.
    ///
    /// Default is true (use index if available).
    pub fn use_index(&mut self, use_index: bool) -> &mut Self {
        self.params.use_index = use_index;
        self
    }

    /// Specify how to handle duplicate source rows.
    ///
    /// Default is `Fail`, which errors when multiple source rows match one target row.
    /// Use `FirstSeen` to keep the first encountered row for each non-null join key,
    /// including unmatched keys that will be inserted, and skip subsequent rows.
    /// Join keys containing NULL are not deduplicated because merge insert uses SQL
    /// equality, where NULL does not equal NULL.
    ///
    /// If the source contains duplicates and `FirstSeen` behavior doesn't match your needs,
    /// sort the source data before passing it to the merge insert operation.
    pub fn source_dedupe_behavior(&mut self, behavior: SourceDedupeBehavior) -> &mut Self {
        self.params.source_dedupe_behavior = behavior;
        self
    }

    /// Mark MemWAL SSTables as compacted when this commit succeeds.
    ///
    /// This updates `compacted_sstables` in the MemWAL index atomically with
    /// the data commit.
    pub fn mark_sstables_as_compacted(&mut self, sstables: Vec<CompactedSsTable>) -> &mut Self {
        self.params.compacted_sstables.extend(sstables);
        self
    }

    /// Set the number of inner commit retries for manifest version conflicts.
    /// Different from `conflict_retries` which handles semantic conflicts.
    /// Default: 20
    pub fn commit_retries(&mut self, retries: u32) -> &mut Self {
        self.params.commit_retries = Some(retries);
        self
    }

    /// Write new fragments produced by this merge insert to these base IDs.
    ///
    /// New data files are distributed across the target bases round-robin,
    /// the same way a normal write with [`WriteParams::target_bases`] routes
    /// them. The IDs must be registered in the dataset manifest, or
    /// [`super::PRIMARY_BASE_ID`] (0) to include the dataset's primary
    /// storage in the rotation (e.g. `vec![0, 1, 2]` spreads across primary
    /// plus bases 1 and 2). Data files that patch existing fragments and
    /// deletion files are always written to the dataset's primary storage.
    ///
    /// Cannot be combined with [`Self::target_base_names_or_paths`].
    pub fn target_bases(&mut self, base_ids: Vec<u32>) -> &mut Self {
        self.params.target_bases = Some(base_ids);
        self
    }

    /// Like [`Self::target_bases`], but referencing bases by name or path URI.
    ///
    /// References are resolved against the base paths registered in the
    /// dataset manifest when the merge insert executes. An entry equal to the
    /// dataset's URI includes the dataset's primary storage in the rotation.
    ///
    /// Cannot be combined with [`Self::target_bases`].
    pub fn target_base_names_or_paths(&mut self, refs: Vec<String>) -> &mut Self {
        self.params.target_base_names_or_paths = Some(refs);
        self
    }

    /// Write new fragments produced by this merge insert to every base
    /// registered in the dataset manifest, resolved when the merge executes.
    /// When `include_primary` is true the dataset's primary storage
    /// participates in the rotation as the first slot.
    ///
    /// Cannot be combined with [`Self::target_bases`] or
    /// [`Self::target_base_names_or_paths`].
    pub fn target_all_bases(&mut self, include_primary: bool) -> &mut Self {
        self.params.target_all_bases = Some(include_primary);
        self
    }

    /// Crate a merge insert job
    pub fn try_build(&mut self) -> Result<MergeInsertJob> {
        if !self.params.insert_not_matched
            && self.params.when_matched == WhenMatched::DoNothing
            && self.params.delete_not_matched_by_source == WhenNotMatchedBySource::Keep
        {
            return Err(Error::invalid_input(
                "The merge insert job is not configured to change the data in any way",
            ));
        }
        if self.params.target_bases.is_some() && self.params.target_base_names_or_paths.is_some() {
            return Err(Error::invalid_input(
                "Cannot specify both target_base_names_or_paths and target_bases. Use one or the other.",
            ));
        }
        if self.params.target_all_bases.is_some()
            && (self.params.target_bases.is_some()
                || self.params.target_base_names_or_paths.is_some())
        {
            return Err(Error::invalid_input(
                "Cannot specify target_all_bases together with target_bases or target_base_names_or_paths.",
            ));
        }
        Ok(MergeInsertJob {
            dataset: self.dataset.clone(),
            params: self.params.clone(),
        })
    }
}

/// Resolve the merge insert target bases against the base paths registered in
/// the dataset manifest. Returns `None` when no target bases were requested.
/// Base id [`super::PRIMARY_BASE_ID`] and the dataset's URI refer to the
/// dataset's primary storage.
///
/// Resolution runs once per execution attempt so retries validate against the
/// manifest version they are writing to.
async fn resolve_target_bases(
    dataset: &Dataset,
    params: &MergeInsertParams,
) -> Result<Option<Vec<TargetBaseInfo>>> {
    if params.target_bases.is_none()
        && params.target_base_names_or_paths.is_none()
        && params.target_all_bases.is_none()
    {
        return Ok(None);
    }
    // Reuse the normal write path resolution (validation, name/path lookup,
    // and per-base credential handling) through a parameter shim.
    let mut write_params = WriteParams {
        mode: WriteMode::Append,
        target_bases: params.target_bases.clone(),
        target_base_names_or_paths: params.target_base_names_or_paths.clone(),
        target_all_bases: params.target_all_bases,
        session: Some(dataset.session.clone()),
        store_params: dataset.store_params.as_deref().cloned(),
        base_store_params: dataset.base_store_params.as_deref().cloned(),
        ..Default::default()
    };
    validate_and_resolve_target_bases_with_primary(
        &mut write_params,
        Some(&dataset.manifest.base_paths),
        &dataset.object_store,
        &dataset.base,
        dataset.uri(),
    )
    .await
}

enum SchemaComparison {
    FullCompatible,
    Subschema,
}

/// Wrap a one-shot stream in a non-replayable [`StreamingTable`] provider.
///
/// The provider can only be scanned once (its single partition hands out the
/// underlying stream), so it must not be used where retries may re-scan it.
fn one_shot_provider(stream: SendableRecordBatchStream) -> Result<Arc<dyn TableProvider>> {
    let schema = stream.schema();
    let partition = Arc::new(OneShotPartitionStream::new(stream));
    Ok(Arc::new(StreamingTable::try_new(schema, vec![partition])?))
}

/// Scans source partitions sequentially and removes duplicate non-null keys.
///
/// Deduplicating before the join fixes the `FirstSeen` winner at the source
/// boundary, before DataFusion can reorder rows. The tracker retains only keys,
/// so this stays streaming without buffering source batches.
#[derive(Debug)]
struct DeduplicatingSourcePartitionStream {
    input: Arc<dyn ExecutionPlan>,
    schema: Arc<Schema>,
    on_columns: Vec<String>,
    skipped_duplicates: Arc<AtomicU64>,
}

impl DeduplicatingSourcePartitionStream {
    fn new(
        input: Arc<dyn ExecutionPlan>,
        on_columns: Vec<String>,
        skipped_duplicates: Arc<AtomicU64>,
    ) -> Self {
        let schema = input.schema();
        Self {
            input,
            schema,
            on_columns,
            skipped_duplicates,
        }
    }
}

impl PartitionStream for DeduplicatingSourcePartitionStream {
    fn schema(&self) -> &Arc<Schema> {
        &self.schema
    }

    fn execute(
        &self,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> SendableRecordBatchStream {
        let input = self.input.clone();
        let partition_count = input.properties().output_partitioning().partition_count();
        let partition_streams = stream::iter(0..partition_count)
            .map(move |partition| input.execute(partition, context.clone()))
            .try_flatten();

        let mut tracker = InsertedKeyTracker::default();
        let on_columns = self.on_columns.clone();
        let skipped_duplicates = self.skipped_duplicates.clone();
        skipped_duplicates.store(0, Ordering::Relaxed);
        let deduplicated = partition_streams.map(move |batch| {
            let batch = batch?;
            let mut keep = Vec::with_capacity(batch.num_rows());
            let mut num_skipped = 0_u64;
            for row_idx in 0..batch.num_rows() {
                let is_first = tracker.insert(&batch, row_idx, &on_columns)?;
                keep.push(is_first);
                if !is_first {
                    num_skipped = num_skipped.checked_add(1).ok_or_else(|| {
                        DataFusionError::Execution(
                            "source duplicate count overflowed u64".to_string(),
                        )
                    })?;
                }
            }

            let mut current = skipped_duplicates.load(Ordering::Relaxed);
            loop {
                let updated = current.checked_add(num_skipped).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "source duplicate count overflow at {} with batch count {}",
                        current, num_skipped
                    ))
                })?;
                match skipped_duplicates.compare_exchange_weak(
                    current,
                    updated,
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                ) {
                    Ok(_) => break,
                    Err(actual) => current = actual,
                }
            }

            if num_skipped == 0 {
                Ok(batch)
            } else {
                arrow::compute::filter_record_batch(&batch, &BooleanArray::from(keep))
                    .map_err(DataFusionError::from)
            }
        });

        Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            deduplicated,
        ))
    }
}

impl MergeInsertJob {
    pub async fn execute_reader(
        self,
        source: impl StreamingWriteSource,
    ) -> Result<(Arc<Dataset>, MergeStats)> {
        let stream = source.into_stream();
        self.execute(stream).await
    }

    fn check_compatible_schema(&self, schema: &Schema) -> Result<SchemaComparison> {
        let lance_schema: lance_core::datatypes::Schema = schema.try_into()?;
        let target_schema = self.dataset.schema();

        let mut options = SchemaCompareOptions {
            compare_dictionary: self.dataset.is_legacy_storage(),
            compare_nullability: NullabilityComparison::Ignore,
            ..Default::default()
        };

        // Try full schema match first.
        if lance_schema
            .check_compatible(target_schema, &options)
            .is_ok()
        {
            return Ok(SchemaComparison::FullCompatible);
        }

        // If full match fails, try subschema match.
        options.allow_subschema = true;
        options.ignore_field_order = true; // Subschema matching should typically ignore order.

        lance_schema
            .check_compatible(target_schema, &options)
            .map(|_| SchemaComparison::Subschema)
    }

    /// Collect every join column that has a scalar index supporting exact
    /// equality.
    ///
    /// For a single-column join this matches the previous behavior. For a
    /// multi-column (composite key) join, every indexed column contributes
    /// an additional `IsIn` probe inside one [`MapIndexExec`]: their AND
    /// yields the row addresses where every indexed column matches some
    /// source value. The downstream hash join still filters by the full
    /// composite key, so unindexed columns simply do not prune the
    /// candidate set — they are checked by the post-filter.
    ///
    /// Returns an empty vec when no join column has a usable scalar index;
    /// callers should then fall through to the full-scan path.
    async fn indexed_join_keys(&self) -> Result<Vec<(String, IndexMetadata)>> {
        let mut indexed = Vec::with_capacity(self.params.on.len());
        for col in &self.params.on {
            if let Some(idx) = self
                .dataset
                .load_scalar_index(
                    IndexCriteria::default()
                        .for_column(col)
                        // Unclear if this would work if the index does not support exact equality
                        .supports_exact_equality(),
                )
                .await?
            {
                indexed.push((col.clone(), idx));
            }
        }
        Ok(indexed)
    }

    /// Fragments that cannot be reached by every index in `indexed_keys`
    /// and therefore must be scanned separately and unioned in alongside
    /// the indexed take.  A fragment is "reachable" by the composite probe
    /// only if it is in the intersection of all the indices' fragment
    /// bitmaps; everything else falls into the unindexed set.
    async fn unindexed_fragments_for_keys(
        &self,
        indexed_keys: &[(String, IndexMetadata)],
    ) -> Result<Vec<Fragment>> {
        let mut unindexed: HashMap<u64, Fragment> = HashMap::new();
        for (_, index) in indexed_keys {
            for frag in self.dataset.unindexed_fragments(&index.name).await? {
                unindexed.entry(frag.id).or_insert(frag);
            }
        }
        Ok(unindexed.into_values().collect())
    }

    async fn create_indexed_scan_joined_stream(
        &self,
        source: SendableRecordBatchStream,
        indexed_keys: Vec<(String, IndexMetadata)>,
    ) -> Result<SendableRecordBatchStream> {
        // This relies on a few non-standard physical operators and so we cannot use the
        // datafusion dataframe API and need to construct the plan manually :'(
        debug_assert!(
            !indexed_keys.is_empty(),
            "create_indexed_scan_joined_stream requires at least one indexed key"
        );
        let schema = source.schema();
        let add_row_addr = match self.check_compatible_schema(&schema)? {
            SchemaComparison::FullCompatible => false,
            SchemaComparison::Subschema => true,
        };

        // 1 - Input from user
        let input = Arc::new(OneShotExec::new(source));

        // 2 - Fork/Replay the input
        // Regrettably, this needs to have unbounded capacity, and so we need to fully read
        // the new data into memory.  In the future, we can do better
        let shared_input = Arc::new(ReplayExec::new(Capacity::Unbounded, input));

        // 3 - Probe every indexed join column.  For composite keys this is
        //     the AND of one `IsIn` query per indexed column, which yields
        //     a tighter candidate set than probing a single column.  The
        //     downstream hash join still filters by the full composite key,
        //     so unindexed `on` columns simply do not prune the candidates.
        let lookup_fields = indexed_keys
            .iter()
            .map(|(col, _)| Ok(schema.field_with_name(col)?.clone()))
            .collect::<Result<Vec<_>>>()?;
        let index_mapper_input =
            Arc::new(project(shared_input.clone(), &Schema::new(lookup_fields))?);

        let lookups = indexed_keys
            .iter()
            .map(|(col, idx)| IndexLookup::new(col.clone(), idx.name.clone()))
            .collect::<Vec<_>>();
        let mut index_mapper: Arc<dyn ExecutionPlan> = Arc::new(MapIndexExec::new_multi(
            self.dataset.clone(),
            lookups,
            index_mapper_input,
        ));

        // 4 - Take the mapped row ids (TakeExec stays for legacy storage:
        //     the v1 reader cannot serve a FilteredReadExec)
        let projection = self
            .dataset
            .empty_projection()
            .union_arrow_schema(schema.as_ref(), OnMissing::Error)?;
        let mut target: Arc<dyn ExecutionPlan> = if self.dataset.is_legacy_storage() {
            if add_row_addr {
                let pos = index_mapper.schema().fields().len(); // Add to end
                index_mapper = Arc::new(AddRowAddrExec::try_new(
                    index_mapper,
                    self.dataset.clone(),
                    pos,
                )?);
            }
            Arc::new(TakeExec::try_new(self.dataset.clone(), index_mapper, projection)?.unwrap())
        } else {
            // Keep the mapped row ids; the read synthesizes the row addresses
            // if requested (no AddRowAddrExec needed)
            let mut projection = projection.with_row_id();
            if add_row_addr {
                projection = projection.with_row_addr();
            }
            Arc::new(FilteredReadExec::try_new(
                self.dataset.clone(),
                FilteredReadOptions::new(projection),
                Some(index_mapper),
            )?)
        };

        // 5 - Take puts the row id and row addr at the beginning.  A full scan (used when there is
        //     no scalar index) puts the row id and addr at the end.  We need to match these up so
        //     we reorder those columns at the end.
        let schema = target.schema();
        let mut columns = schema
            .fields()
            .iter()
            .filter(|f| f.name() != ROW_ID && f.name() != ROW_ADDR)
            .cloned()
            .collect::<Vec<_>>();
        columns.push(Arc::new(ROW_ID_FIELD.clone()));
        if add_row_addr {
            columns.push(Arc::new(ROW_ADDR_FIELD.clone()));
        }
        target = Arc::new(project(target, &Schema::new(columns))?);

        let column_names = schema
            .field_names()
            .into_iter()
            .filter(|name| name.as_str() != ROW_ID && name.as_str() != ROW_ADDR)
            .collect::<Vec<_>>();

        // 5a - We also need to scan any new unindexed data and union it in.
        //      A row can be reached by the composite index probe only if it
        //      lives in a fragment covered by *every* chosen index, so the
        //      "unindexed" set is the union of fragments missing from any
        //      one of them.
        let unindexed_fragments = self.unindexed_fragments_for_keys(&indexed_keys).await?;
        if !unindexed_fragments.is_empty() {
            let mut builder = self.dataset.scan();
            if add_row_addr {
                builder.with_row_address();
            }
            let unindexed_data = builder
                .with_row_id()
                .with_fragments(unindexed_fragments)
                .project(&column_names)
                .unwrap()
                .create_plan()
                .await?;
            let unioned = UnionExec::try_new(vec![target, unindexed_data])?;
            // Enforce only 1 partition.
            target = Arc::new(RepartitionExec::try_new(
                unioned,
                datafusion::physical_plan::Partitioning::RoundRobinBatch(1),
            )?);
        }

        // We need to prefix the fields in the target with target_ so that we don't have any duplicate
        // field names (DF doesn't support this as of version 44)
        target = Self::prefix_columns_phys(target, "target_");

        // 6 - Join the source against the taken target rows on the full
        //     composite key.  Probing the index produces a super-set of the
        //     actual matches (when not every key column has an index, or
        //     even when they do — the per-column `IsIn` lists do not
        //     correlate values across the tuple), so this join is what
        //     trims candidates down to the exact composite-key matches.
        let on_keys = self
            .params
            .on
            .iter()
            .map(|col| {
                let source_key = Column::new_with_schema(col, shared_input.schema().as_ref())?;
                let target_key =
                    Column::new_with_schema(&format!("target_{}", col), target.schema().as_ref())?;
                Ok::<_, Error>((
                    Arc::new(source_key) as Arc<dyn PhysicalExpr>,
                    Arc::new(target_key) as Arc<dyn PhysicalExpr>,
                ))
            })
            .collect::<Result<Vec<_>>>()?;

        // Use standard SQL NULL semantics for composite keys so this path
        // produces the same result as the full-scan path.  The
        // single-column case keeps its historical `NullEqualsNull` behavior
        // to avoid changing semantics for existing callers.
        let null_equality = if self.params.on.len() == 1 {
            NullEquality::NullEqualsNull
        } else {
            NullEquality::NullEqualsNothing
        };

        let joined = Arc::new(
            HashJoinExec::try_new(
                shared_input,
                target,
                on_keys,
                None,
                &JoinType::Full,
                None,
                PartitionMode::CollectLeft,
                null_equality,
                false,
            )
            .unwrap(),
        );
        execute_plan(
            joined,
            LanceExecutionOptions {
                use_spilling: true,
                ..Default::default()
            },
        )
    }

    fn prefix_columns(df: DataFrame, prefix: &str) -> DataFrame {
        let schema = df.schema();
        let columns = schema
            .fields()
            .iter()
            .map(|f| {
                // Need to "quote" the column name so it gets interpreted case-sensitively
                logical_expr::col(format!("\"{}\"", f.name())).alias(format!(
                    "{}{}",
                    prefix,
                    f.name()
                ))
            })
            .collect::<Vec<_>>();
        df.select(columns).unwrap()
    }

    fn prefix_columns_phys(inp: Arc<dyn ExecutionPlan>, prefix: &str) -> Arc<dyn ExecutionPlan> {
        let schema = inp.schema();
        let exprs = schema
            .fields()
            .iter()
            .enumerate()
            .map(|(idx, f)| {
                let col = Arc::new(Column::new(f.name(), idx)) as Arc<dyn PhysicalExpr>;
                let new_name = format!("{}{}", prefix, f.name());
                (col, new_name)
            })
            .collect::<Vec<_>>();
        Arc::new(ProjectionExec::try_new(exprs, inp).unwrap())
    }

    // If the join keys are not indexed then we need to do a full scan of the table
    async fn create_full_table_joined_stream(
        &self,
        source: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        let session_config = SessionConfig::default().with_target_partitions(1);
        let session_ctx = SessionContext::new_with_config(session_config);
        let schema = source.schema();
        let new_data = session_ctx.read_one_shot(source)?;
        let join_cols = self
            .params
            .on // columns to join on
            .iter()
            .map(|c| c.as_str())
            .collect::<Vec<_>>(); // vector of strings of col names to join
        let target_cols = self
            .params
            .on
            .iter()
            .map(|c| format!("target_{}", c))
            .collect::<Vec<_>>();
        let target_cols = target_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>();

        match self.check_compatible_schema(&schema)? {
            SchemaComparison::FullCompatible => {
                let existing = session_ctx.read_lance(self.dataset.clone(), true, false)?;
                // We need to rename the columns from the target table so that they don't conflict with the source table
                let existing = Self::prefix_columns(existing, "target_");
                let joined =
                    new_data.join(existing, JoinType::Full, &join_cols, &target_cols, None)?; // full join
                Ok(joined.execute_stream().await?)
            }
            SchemaComparison::Subschema => {
                let existing = session_ctx.read_lance(self.dataset.clone(), true, true)?;
                let columns = schema
                    .field_names()
                    .iter()
                    .map(|s| s.as_str())
                    .chain([ROW_ID, ROW_ADDR])
                    .collect::<Vec<_>>();
                let projected = existing.select_columns(&columns)?;
                // We need to rename the columns from the target table so that they don't conflict with the source table
                let projected = Self::prefix_columns(projected, "target_");
                // We aren't supporting inserts or deletes right now, so we can use inner join
                let join_type = if self.params.insert_not_matched {
                    JoinType::Left
                } else {
                    JoinType::Inner
                };
                let joined = new_data.join(projected, join_type, &join_cols, &target_cols, None)?;
                Ok(joined.execute_stream().await?)
            }
        }
    }

    /// Join the source and target data streams
    ///
    /// If there is a scalar index on the join key, we can use it to do an indexed join.  Otherwise we need to do
    /// a full outer join.
    ///
    /// Datafusion doesn't allow duplicate column names so during this join we rename the columns from target and
    /// prefix them with _target.
    async fn create_joined_stream(
        &self,
        source: SendableRecordBatchStream,
    ) -> Result<SendableRecordBatchStream> {
        if self.params.use_index
            && matches!(
                self.params.delete_not_matched_by_source,
                WhenNotMatchedBySource::Keep
            )
        {
            // keeping unmatched rows, no deletion. Use the indexed-scan path
            // only when EVERY join column is indexed; a partially-indexed
            // composite key under-matches there (the probe can't resolve the
            // full tuple), so fall through to the correct full-table join.
            let indexed_keys = self.indexed_join_keys().await?;
            if indexed_keys.len() == self.params.on.len() {
                return self
                    .create_indexed_scan_joined_stream(source, indexed_keys)
                    .await;
            }
        }

        if !matches!(
            self.params.delete_not_matched_by_source,
            WhenNotMatchedBySource::Keep
        ) {
            info!(
                "The merge insert operation is configured to delete rows from the target table, this requires a potentially costly full table scan"
            );
        }

        self.create_full_table_joined_stream(source).await
    }

    async fn update_fragments(
        dataset: Arc<Dataset>,
        source: SendableRecordBatchStream,
        current_version: u64,
        target_bases_info: Option<Vec<TargetBaseInfo>>,
    ) -> Result<(Vec<Fragment>, Vec<Fragment>, Vec<u32>)> {
        // Shared across the per-group tasks spawned below; only new fragments
        // are routed to target bases, column patches stay in primary storage.
        let target_bases_info = Arc::new(target_bases_info);
        // Expected source schema: _rowaddr, updated_cols*
        use datafusion::logical_expr::{col, lit};
        let session_ctx = get_session_context(&LanceExecutionOptions {
            use_spilling: true,
            target_partition: Some(get_num_compute_intensive_cpus().min(8)),
            ..Default::default()
        });
        // 25 MiB hard cap on batch size.  DataFusion's sort cannot spill a
        // single batch that is larger than the memory pool, so we must
        // rechunk oversized batches before they reach the sort.
        const MAX_BATCH_BYTES: usize = 25 * 1024 * 1024;
        let sorted = session_ctx
            .read_one_shot(source)?
            .with_column("_fragment_id", col(ROW_ADDR) >> lit(32))?
            .sort(vec![col(ROW_ADDR).sort(true, true)])?;
        let sorted_plan = sorted.create_physical_plan().await?;
        // Walk the physical plan and insert HardCapBatchSizeExec below every
        // sort node so each input batch fits in the memory pool.
        let capped_plan = sorted_plan
            .transform_down(|node| {
                if node.downcast_ref::<SortExec>().is_some() {
                    let children = node.children();
                    let new_children: Vec<Arc<dyn ExecutionPlan>> = children
                        .into_iter()
                        .map(|c| {
                            Arc::new(HardCapBatchSizeExec::new(c.clone(), MAX_BATCH_BYTES))
                                as Arc<dyn ExecutionPlan>
                        })
                        .collect();
                    let new_node = node.with_new_children(new_children)?;
                    Ok(Transformed::yes(new_node))
                } else {
                    Ok(Transformed::no(node))
                }
            })?
            .data;
        let capped_stream = capped_plan.execute(0, session_ctx.task_ctx())?;
        let mut group_stream = BatchStreamGrouper::new(capped_stream, "_fragment_id".into());

        // Can update the fragments in parallel.
        let updated_fragments = Arc::new(Mutex::new(Vec::new()));
        let new_fragments = Arc::new(Mutex::new(Vec::new()));
        let mut tasks = JoinSet::new();
        let task_limit = dataset.object_store.as_ref().io_parallelism();
        let reservation =
            MemoryConsumer::new("MergeInsert").register(session_ctx.task_ctx().memory_pool());

        // Best-effort removal of uncommitted files after a mid-update failure.
        // Aborts in-flight tasks first, then deletes the new fragments written
        // so far (including ones routed to target bases). Column-patch files
        // from completed tasks stay in primary storage where regular dataset
        // cleanup can reclaim them.
        async fn cleanup_on_failure(
            dataset: &Dataset,
            target_bases_info: &Option<Vec<TargetBaseInfo>>,
            new_fragments: &Mutex<Vec<Fragment>>,
            tasks: &mut JoinSet<Result<usize>>,
        ) {
            tasks.shutdown().await;
            let written = new_fragments.lock().unwrap().clone();
            cleanup_data_fragments(
                &dataset.object_store,
                &dataset.base,
                target_bases_info.as_deref(),
                &written,
            )
            .await;
        }

        loop {
            let (frag_id, batches) = match group_stream.next().await.transpose() {
                Ok(Some(group)) => group,
                Ok(None) => break,
                Err(e) => {
                    cleanup_on_failure(&dataset, &target_bases_info, &new_fragments, &mut tasks)
                        .await;
                    return Err(e.into());
                }
            };
            async fn handle_fragment(
                dataset: Arc<Dataset>,
                fragment: FileFragment,
                mut metadata: Fragment,
                mut batches: Vec<RecordBatch>,
                updated_fragments: Arc<Mutex<Vec<Fragment>>>,
                reservation_size: usize,
                current_version: u64,
            ) -> Result<usize> {
                // batches still have _rowaddr
                let write_schema = batches[0]
                    .schema()
                    .as_ref()
                    .without_column(ROW_ADDR)
                    .without_column(ROW_ID);
                let write_schema = dataset.schema().project_by_schema(
                    &write_schema,
                    OnMissing::Error,
                    OnTypeMismatch::Error,
                )?;

                let updated_rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();

                // This function is here to help rustc with lifetimes.
                fn get_row_addr_iter(
                    batches: &[RecordBatch],
                ) -> impl Iterator<Item = (u64, (usize, usize))> + '_ + Send {
                    batches.iter().enumerate().flat_map(|(batch_idx, batch)| {
                        // The index in source batches will be one more.
                        let batch_idx = batch_idx + 1;
                        let row_addrs = batch
                            .column_by_name(ROW_ADDR)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        row_addrs
                            .values()
                            .iter()
                            .enumerate()
                            .map(move |(offset, row_addr)| (*row_addr, (batch_idx, offset)))
                    })
                }

                let has_full_fragment_coverage = metadata.deletion_file.is_none()
                    && Some(updated_rows) == metadata.physical_rows
                    && get_row_addr_iter(&batches)
                        .map(|(row_addr, _)| row_addr)
                        .eq(RowAddress::address_range(metadata.id as u32).take(updated_rows));
                if has_full_fragment_coverage {
                    // Exact, deletion-free coverage can be written directly because the
                    // batches are sorted by row address.

                    let data_storage_version = dataset
                        .manifest()
                        .data_storage_format
                        .lance_file_version()?;
                    let mut writer = crate::dataset::versions::open_writer(
                        data_storage_version.into(),
                        &dataset.object_store,
                        &write_schema,
                        &dataset.base,
                        super::WriterOptions {
                            add_data_dir: true,
                            ..Default::default()
                        },
                    )
                    .await?;

                    // We need to remove rowaddr before writing.
                    batches
                        .iter_mut()
                        .try_for_each(|batch| match batch.drop_column(ROW_ADDR) {
                            Ok(b) => {
                                *batch = b;
                                Ok(())
                            }
                            Err(e) => Err(e),
                        })?;

                    // Convert Arrow JSON columns (Utf8) to Lance JSON (LargeBinary/JSONB)
                    // before writing. Without this, Utf8 data is written raw while the
                    // schema says LargeBinary, causing decoder panics on subsequent reads.
                    let needs_json_conversion = batches[0]
                        .schema()
                        .fields()
                        .iter()
                        .any(|f| is_arrow_json_field(f) || has_json_fields(f));
                    if needs_json_conversion {
                        for batch in batches.iter_mut() {
                            *batch = convert_json_columns(batch).map_err(Error::from)?;
                        }
                    }

                    if data_storage_version == LanceFileVersion::Legacy {
                        // Need to match the existing batch size exactly, otherwise
                        // we'll get errors.
                        let reader = fragment
                            .open(
                                dataset.schema(),
                                FragReadConfig::default().with_row_address(true),
                            )
                            .await?;
                        let batch_size = reader.legacy_num_rows_in_batch(0).unwrap();
                        let stream = stream::iter(batches.into_iter().map(Ok));
                        let stream = Box::pin(RecordBatchStreamAdapter::new(
                            Arc::new((&write_schema).into()),
                            stream,
                        ));
                        let mut stream = chunk_stream(stream, batch_size as usize);
                        while let Some(chunk) = stream.next().await {
                            writer.write(&chunk?).await?;
                        }
                    } else {
                        writer.write(batches.as_slice()).await?;
                    }

                    let (_num_rows, data_file) = writer.finish().await?;

                    metadata.files.push(data_file);

                    if dataset.manifest.uses_stable_row_ids() {
                        // in-place frag override: refresh row-level latest update version meta
                        lance_table::rowids::version::refresh_row_latest_update_meta_for_full_frag_rewrite_cols(
                            &mut metadata,
                            current_version,
                        )?;
                    }

                    updated_fragments.lock().unwrap().push(metadata);
                } else {
                    // TODO: we could skip scanning row addresses we don't need.
                    let update_schema = batches[0].schema();
                    let read_columns = update_schema.field_names();
                    let mut updater = fragment
                        .updater(
                            Some(&read_columns),
                            Some((write_schema, dataset.schema().clone())),
                            None,
                        )
                        .await?;

                    // We will use interleave to update the rows. The first batch
                    // will be the original source data, and all subsequent batches
                    // will be updates.
                    let mut source_batches = Vec::with_capacity(batches.len() + 1);
                    // Convert Arrow JSON columns (Utf8) to Lance JSON (LargeBinary) so every
                    // batch is in physical format, matching what the updater reads from the
                    // fragment. `convert_json_columns` is a no-op clone when there is nothing
                    // to convert, so it can be applied unconditionally. The first entry is a
                    // placeholder for the source data (overwritten each iteration below); it
                    // must be converted too, otherwise its schema would diverge from the rest.
                    source_batches.push(convert_json_columns(&batches[0]).map_err(Error::from)?);
                    for batch in &batches {
                        let dropped = batch.drop_column(ROW_ADDR)?;
                        source_batches.push(convert_json_columns(&dropped).map_err(Error::from)?);
                    }

                    let mut updated_rows =
                        UpdatedRowAddrReconciler::new(get_row_addr_iter(&batches));

                    while let Some(batch) = updater.next().await? {
                        source_batches[0] =
                            batch.project_by_schema(source_batches[1].schema().as_ref())?;

                        let original_row_addrs = batch
                            .column_by_name(ROW_ADDR)
                            .unwrap()
                            .as_any()
                            .downcast_ref::<UInt64Array>()
                            .unwrap();
                        let indices = updated_rows.reconcile_batch(original_row_addrs.values())?;

                        let updated_batch = interleave_batches(&source_batches, &indices)?;

                        updater.update(updated_batch).await?;
                    }
                    updated_rows.finish()?;

                    let mut updated_fragment = updater.finish().await?;

                    if dataset.manifest.uses_stable_row_ids() {
                        // in-place frag partial rows update, do the in-place refresh the frag's row_latest_update_version_meta
                        // via compute updated local row offsets and write row-level version meta
                        let mut updated_offsets: Vec<usize> = Vec::new();
                        for b in batches.iter() {
                            let row_addrs = b
                                .column_by_name(ROW_ADDR)
                                .unwrap()
                                .as_any()
                                .downcast_ref::<UInt64Array>()
                                .unwrap();
                            updated_offsets.extend(
                                row_addrs
                                    .values()
                                    .iter()
                                    .map(|addr| RowAddress::from(*addr).row_offset() as usize),
                            );
                        }
                        updated_offsets.sort_unstable();
                        updated_offsets.dedup();

                        lance_table::rowids::version::refresh_row_latest_update_meta_for_partial_frag_rewrite_cols(
                            &mut updated_fragment,
                            &updated_offsets,
                            current_version,
                            dataset.manifest.version,
                        )?;
                    }

                    updated_fragments.lock().unwrap().push(updated_fragment);
                }
                Ok(reservation_size)
            }

            async fn handle_new_fragments(
                dataset: Arc<Dataset>,
                batches: Vec<RecordBatch>,
                new_fragments: Arc<Mutex<Vec<Fragment>>>,
                reservation_size: usize,
                target_bases_info: Arc<Option<Vec<TargetBaseInfo>>>,
            ) -> Result<usize> {
                // Batches still have _rowaddr (used elsewhere to merge with existing data)
                // We need to remove it before writing to Lance files.
                let num_fields = batches[0].schema().fields().len();
                let mut projection = Vec::with_capacity(num_fields - 1);
                for (i, field) in batches[0].schema().fields().iter().enumerate() {
                    if field.name() != ROW_ADDR {
                        projection.push(i);
                    }
                }
                let write_schema = Arc::new(batches[0].schema().project(&projection).unwrap());

                let batches = batches
                    .into_iter()
                    .map(move |batch| batch.project(&projection));
                let reader = RecordBatchIterator::new(batches, write_schema.clone());
                let stream = reader_to_stream(Box::new(reader));

                let write_schema = dataset.schema().project_by_schema(
                    write_schema.as_ref(),
                    OnMissing::Error,
                    OnTypeMismatch::Error,
                )?;

                let (fragments, _) = write_fragments_internal(
                    dataset.manifest.data_storage_format.lance_file_format(),
                    Some(dataset.as_ref()),
                    dataset.object_store.clone(),
                    &dataset.base,
                    write_schema,
                    stream,
                    Default::default(), // TODO: support write params.
                    (*target_bases_info).clone(),
                )
                .await?;

                new_fragments.lock().unwrap().extend(fragments);
                Ok(reservation_size)
            }
            // We shouldn't need much more memory beyond what is already in the batches.
            let mut memory_size = batches
                .iter()
                .map(|batch| batch.get_array_memory_size())
                .sum();

            loop {
                let have_additional_cpus = tasks.len() < task_limit;
                if have_additional_cpus {
                    if reservation.try_grow(memory_size).is_ok() {
                        break;
                    } else if tasks.is_empty() {
                        // If there are no tasks running, we can bypass the pool limits.
                        // This lets us handle the case where we have a single large batch.
                        memory_size = 0;
                        break;
                    }
                    // If we can't grow the reservation, we will wait for a task to finish
                }

                if let Some(res) = tasks.join_next().await {
                    match res.map_err(Error::from).and_then(|size| size) {
                        Ok(size) => reservation.shrink(size),
                        Err(e) => {
                            cleanup_on_failure(
                                &dataset,
                                &target_bases_info,
                                &new_fragments,
                                &mut tasks,
                            )
                            .await;
                            return Err(e);
                        }
                    }
                }
            }

            match frag_id.first() {
                Some(ScalarValue::UInt64(Some(frag_id))) => {
                    let frag_id = *frag_id;
                    let Some(fragment) = dataset.get_fragment(frag_id as usize) else {
                        error!(
                            fragment_id = frag_id,
                            dataset_uri = %dataset.uri(),
                            manifest_version = dataset.manifest().version,
                            manifest_path = %dataset.manifest_location().path,
                            branch = ?dataset.manifest().branch,
                            "Non-existent fragment id returned from merge result",
                        );
                        cleanup_on_failure(
                            &dataset,
                            &target_bases_info,
                            &new_fragments,
                            &mut tasks,
                        )
                        .await;
                        return Err(Error::internal(format!(
                            "Got non-existent fragment id from merge result: {} (uri={}, version={}, manifest={}, branch={})",
                            frag_id,
                            dataset.uri(),
                            dataset.manifest().version,
                            dataset.manifest_location().path,
                            dataset.manifest().branch.as_deref().unwrap_or("main"),
                        )));
                    };
                    let metadata = fragment.metadata.clone();

                    let fut = handle_fragment(
                        dataset.clone(),
                        fragment,
                        metadata,
                        batches,
                        updated_fragments.clone(),
                        memory_size,
                        current_version,
                    );
                    tasks.spawn(fut);
                }
                Some(ScalarValue::Null | ScalarValue::UInt64(None)) => {
                    let fut = handle_new_fragments(
                        dataset.clone(),
                        batches,
                        new_fragments.clone(),
                        memory_size,
                        target_bases_info.clone(),
                    );
                    tasks.spawn(fut);
                }
                _ => {
                    cleanup_on_failure(&dataset, &target_bases_info, &new_fragments, &mut tasks)
                        .await;
                    return Err(Error::internal(format!(
                        "Got non-fragment id from merge result: {:?}",
                        frag_id
                    )));
                }
            };
        }

        while let Some(res) = tasks.join_next().await {
            match res.map_err(Error::from).and_then(|size| size) {
                Ok(size) => reservation.shrink(size),
                Err(e) => {
                    cleanup_on_failure(&dataset, &target_bases_info, &new_fragments, &mut tasks)
                        .await;
                    return Err(e);
                }
            }
        }
        let mut updated_fragments = Arc::try_unwrap(updated_fragments)
            .unwrap()
            .into_inner()
            .unwrap();

        // We keep track of all fields that are updated so we can prune the indices.
        // We could maybe be more precise since some fields are not modified in some
        // fragments (if they were already null) but this is simpler and good enough
        // for now.
        let mut all_fields_updated = HashSet::new();

        // Collect the updated fragments, and map the field ids. Tombstone old ones
        // as needed.
        for fragment in &mut updated_fragments {
            let updated_fields = fragment.files.last().unwrap().fields.clone();
            all_fields_updated.extend(updated_fields.iter().map(|&f| f as u32));
            for data_file in &mut fragment.files.iter_mut().rev().skip(1) {
                let new_fields: Arc<[i32]> = data_file
                    .fields
                    .iter()
                    .map(|field| {
                        if updated_fields.contains(field) {
                            -2 // Tombstone
                        } else {
                            *field
                        }
                    })
                    .collect::<Vec<_>>()
                    .into();
                data_file.fields = new_fields;
            }
        }

        let new_fragments = Arc::try_unwrap(new_fragments)
            .unwrap()
            .into_inner()
            .unwrap();

        Ok((
            updated_fragments,
            new_fragments,
            all_fields_updated.into_iter().collect(),
        ))
    }

    /// Executes the merge insert job from a one-shot stream source.
    ///
    /// This will take in the source, merge it with the existing target data, and insert new
    /// rows, update existing rows, and delete existing rows.
    ///
    /// A stream can only be read once, so when `conflict_retries > 0` the stream is
    /// spilled (in memory, then to disk) so it can be replayed on each retry. See
    /// [`MergeInsertBuilder::spill_for_retry`] to fail fast instead, and
    /// [`Self::execute_batches`] / [`Self::execute_provider`] for re-scannable
    /// sources that never spill.
    pub async fn execute(
        self,
        source: SendableRecordBatchStream,
    ) -> Result<(Arc<Dataset>, MergeStats)> {
        let (provider, replayable) = self.stream_source_to_provider(source).await?;
        self.execute_inner(provider, replayable).await
    }

    /// Executes the merge insert job from a re-scannable [`TableProvider`].
    ///
    /// This is the canonical entry point: [`Self::execute`] and
    /// [`Self::execute_batches`] are thin wrappers that build a provider and call
    /// this method. Because a provider can be scanned repeatedly, retries re-read
    /// the source directly and never spill to disk. The provider's reported
    /// statistics (e.g. from a [`MemTable`] or file source) also let DataFusion
    /// optimize the merge join.
    ///
    /// [`MemTable`]: datafusion::datasource::MemTable
    pub async fn execute_provider(
        self,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(Arc<Dataset>, MergeStats)> {
        // A genuine TableProvider is re-scannable by contract, so retries are safe.
        self.execute_inner(provider, true).await
    }

    /// Executes the merge insert job from materialized record batches.
    ///
    /// The batches are wrapped in an in-memory [`MemTable`], which is re-scannable
    /// (retries replay from memory, never spilling) and reports exact statistics to
    /// the merge join. This is the preferred entry point when the full source is
    /// already in memory.
    pub async fn execute_batches(
        self,
        batches: Vec<RecordBatch>,
    ) -> Result<(Arc<Dataset>, MergeStats)> {
        let provider = self.batches_to_provider(batches)?;
        self.execute_inner(provider, true).await
    }

    /// Like [`Self::execute_batches`] but returns the uncommitted transaction.
    ///
    /// Use [`CommitBuilder`] to commit the returned transaction.
    pub async fn execute_uncommitted_batches(
        self,
        batches: Vec<RecordBatch>,
    ) -> Result<UncommittedMergeInsert> {
        let provider = self.batches_to_provider(batches)?;
        self.execute_uncommitted_impl(provider).await
    }

    /// Wrap materialized batches in a multi-partition in-memory [`MemTable`].
    fn batches_to_provider(&self, batches: Vec<RecordBatch>) -> Result<Arc<dyn TableProvider>> {
        let schema = batches
            .first()
            .map(|batch| batch.schema())
            .unwrap_or_else(|| Arc::new(Schema::from(self.dataset.schema())));
        // FirstSeen needs a defined encounter order. Keep materialized batches in
        // their caller-provided order; other modes retain parallel source scans.
        let partitions = if self.params.source_dedupe_behavior == SourceDedupeBehavior::FirstSeen {
            vec![batches]
        } else {
            Self::batches_into_partitions(batches)
        };
        Ok(Arc::new(MemTable::try_new(schema, partitions)?))
    }

    /// Distribute batches round-robin across up to `num_compute_intensive_cpus`
    /// partitions, so a [`MemTable`] built from them can be scanned in parallel.
    /// Always returns at least one (possibly empty) partition so an empty source
    /// still produces a valid provider.
    fn batches_into_partitions(batches: Vec<RecordBatch>) -> Vec<Vec<RecordBatch>> {
        let num_partitions = batches.len().min(get_num_compute_intensive_cpus()).max(1);
        let mut partitions = vec![Vec::new(); num_partitions];
        for (idx, batch) in batches.into_iter().enumerate() {
            partitions[idx % num_partitions].push(batch);
        }
        partitions
    }

    /// Wrap a one-shot stream source in a provider, returning whether it can be
    /// replayed across retries.
    ///
    /// With retries enabled and spilling allowed, the stream is drained into a
    /// replayable spill (memory up to 100MB, then disk). Otherwise the stream is
    /// wrapped in a non-replayable one-shot provider and any conflict fails fast.
    async fn stream_source_to_provider(
        &self,
        source: SendableRecordBatchStream,
    ) -> Result<(Arc<dyn TableProvider>, bool)> {
        if self.params.conflict_retries > 0 && self.params.spill_for_retry {
            // Allow buffering up to 100MB in memory before spilling to disk.
            let provider = spilling_table_provider(source, 100 * 1024 * 1024).await?;
            Ok((provider, true))
        } else {
            Ok((one_shot_provider(source)?, false))
        }
    }

    /// Run the retry loop against a provider, re-scanning it on each attempt.
    ///
    /// `replayable` indicates whether the provider can be scanned more than once.
    /// When it cannot (a one-shot stream that was not spilled), retries are
    /// disabled so we never scan it twice; the operation runs once and surfaces any
    /// commit conflict directly.
    async fn execute_inner(
        self,
        provider: Arc<dyn TableProvider>,
        replayable: bool,
    ) -> Result<(Arc<Dataset>, MergeStats)> {
        let dataset = self.dataset.clone();
        let config = RetryConfig {
            max_retries: if replayable {
                self.params.conflict_retries
            } else {
                0
            },
            retry_timeout: self.params.retry_timeout,
        };

        let wrapper = MergeInsertJobWithProvider {
            job: self,
            provider,
            attempt_count: Arc::new(AtomicU32::new(0)),
        };

        Box::pin(execute_with_retry(wrapper, dataset, config)).await
    }

    /// Execute the merge insert job without committing the changes.
    ///
    /// Use [`CommitBuilder`] to commit the returned transaction.
    pub async fn execute_uncommitted(
        self,
        source: impl StreamingWriteSource,
    ) -> Result<UncommittedMergeInsert> {
        let stream = source.into_stream();
        self.execute_uncommitted_impl(one_shot_provider(stream)?)
            .await
    }

    fn create_plan_join_type(&self) -> JoinType {
        let keep_unmatched_source_rows = self.params.insert_not_matched;
        let keep_unmatched_target_rows = !matches!(
            self.params.delete_not_matched_by_source,
            WhenNotMatchedBySource::Keep
        );

        match (keep_unmatched_target_rows, keep_unmatched_source_rows) {
            (false, false) => JoinType::Inner,
            (false, true) => JoinType::Right,
            (true, false) => JoinType::Left,
            (true, true) => JoinType::Full,
        }
    }

    async fn create_plan(self, provider: Arc<dyn TableProvider>) -> Result<Arc<dyn ExecutionPlan>> {
        // Goal: we shouldn't manually have to specify which columns to scan.
        //       DataFusion's optimizer should be able to automatically perform
        //       projection pushdown for us.
        // Goal: we shouldn't have to add new branches in this code to handle
        //       indexed vs non-indexed cases. That should be handled by optimizer rules.
        let session_ctx = SessionContext::new();
        let binary_blob_field_ids = self
            .dataset
            .schema()
            .fields_pre_order()
            .filter(|field| field.is_blob() && !field.is_blob_v2())
            .map(|field| field.id as u32)
            .collect();
        let target_provider = Arc::new(
            crate::datafusion::dataframe::LanceTableProvider::new_with_ordering(
                self.dataset.clone(),
                true,
                true,
                false,
            )
            .with_blob_handling(lance_core::datatypes::BlobHandling::SomeBlobsBinary(
                binary_blob_field_ids,
            )),
        );
        let scan = session_ctx.read_table(target_provider)?;
        // Wrap column names in double quotes to preserve case (DataFusion lowercases unquoted identifiers)
        let on_cols = self
            .params
            .on
            .iter()
            .map(|name| format!("\"{}\"", name))
            .collect::<Vec<_>>();
        let on_cols_refs = on_cols.iter().map(|s| s.as_str()).collect::<Vec<_>>();
        // FirstSeen must observe the caller's source order even though the join can
        // reorder batches. Deduplicating source partitions sequentially before
        // the join fixes the winner at that contract boundary. Other modes plan
        // directly against the provider so its statistics reach the optimizer.
        let deduplicate_source =
            self.params.source_dedupe_behavior == SourceDedupeBehavior::FirstSeen;
        let source_skipped_duplicates = Arc::new(AtomicU64::new(0));
        let source_df = if deduplicate_source {
            let source_plan = provider.scan(&session_ctx.state(), None, &[], None).await?;
            let deduplicated_partition = Arc::new(DeduplicatingSourcePartitionStream::new(
                source_plan,
                self.params.on.clone(),
                source_skipped_duplicates.clone(),
            ));
            let deduplicated_provider = Arc::new(StreamingTable::try_new(
                deduplicated_partition.schema().clone(),
                vec![deduplicated_partition],
            )?);
            session_ctx.read_table(deduplicated_provider)?
        } else {
            session_ctx.read_table(provider)?
        };
        // Capture the source field names *before* aliasing / joining so we
        // can tell which dataset columns are missing from the source and
        // need to be filled from the target side of the join below.
        let source_field_names: std::collections::HashSet<String> = source_df
            .schema()
            .fields()
            .iter()
            .map(|f| f.name().clone())
            .collect();
        // Inject a sentinel literal column so we can reliably determine, after the join,
        // whether the source side contributed a row.  This is NULL-safe: even when every
        // ON column is NULL the sentinel lets us distinguish a source-only row from a
        // target-only row (where the sentinel is filled with NULL by the outer join).
        let source_df = source_df
            .with_column(MERGE_SOURCE_SENTINEL, logical_expr::lit(true))
            .map_err(crate::Error::from)?;
        let source_df_aliased = source_df.alias("source")?;
        let scan_aliased = scan.alias("target")?;
        let join_type = self.create_plan_join_type();
        let dataset_schema: Schema = self.dataset.schema().into();
        let mut df = scan_aliased
            .join(
                source_df_aliased,
                join_type,
                &on_cols_refs,
                &on_cols_refs,
                None,
            )?
            .with_column(
                MERGE_ACTION_COLUMN,
                merge_insert_action(&self.params, Some(&dataset_schema))?,
            )?;

        // Partial-schema upsert: for every dataset column missing from the
        // source, add a synthetic unqualified column that copies the target
        // side's value for that column. For matched rows this carries the
        // existing target value (preserving non-source columns on update);
        // for unmatched source rows (inserts) the outer join leaves the
        // target side NULL, so inserts get NULL for missing columns. The
        // unqualified name matches the dataset field and becomes a normal
        // data column from the write exec's perspective.
        //
        // We iterate the dataset schema in order so that the resulting
        // physical plan is deterministic and easy to inspect in tests.
        for field in dataset_schema.fields() {
            if !source_field_names.contains(field.name()) {
                df = df.with_column(
                    field.name(),
                    logical_expr::col(format!("target.\"{}\"", field.name())),
                )?;
            }
        }

        let (session_state, logical_plan) = df.into_parts();

        let write_node = logical_plan::MergeInsertWriteNode::new(
            logical_plan,
            self.dataset.clone(),
            self.params.clone(),
            source_skipped_duplicates,
        );
        let logical_plan = LogicalPlan::Extension(Extension {
            node: Arc::new(write_node),
        });

        let logical_plan = session_state.optimize(&logical_plan)?;

        let planner =
            DefaultPhysicalPlanner::with_extension_planners(vec![Arc::new(MergeInsertPlanner {})]);
        // This method already does the optimization for us.
        let physical_plan = planner
            .create_physical_plan(&logical_plan, &session_state)
            .await?;

        Ok(physical_plan)
    }

    async fn execute_uncommitted_v2(
        self,
        provider: Arc<dyn TableProvider>,
    ) -> Result<(
        Transaction,
        MergeStats,
        Option<RowAddrTreeMap>,
        Option<KeyExistenceFilter>,
    )> {
        let plan = self.create_plan(provider).await?;

        // Execute the plan
        // Assert that we have exactly one partition since we're designed for single-partition execution
        let partition_count = match plan.properties().output_partitioning() {
            datafusion_physical_expr::Partitioning::RoundRobinBatch(n) => *n,
            datafusion_physical_expr::Partitioning::Hash(_, n) => *n,
            datafusion_physical_expr::Partitioning::UnknownPartitioning(n) => *n,
        };

        if partition_count != 1 {
            return Err(Error::invalid_input(format!(
                "Expected exactly 1 partition, got {}",
                partition_count
            )));
        }

        // Execute partition 0 (the only partition)
        let task_context = Arc::new(datafusion::execution::TaskContext::default());
        let mut stream = plan.execute(0, task_context)?;

        // Assert that the execution produces no output (this is a write operation)
        if let Some(batch) = stream.next().await {
            let batch = batch?;
            if batch.num_rows() > 0 {
                return Err(Error::invalid_input(format!(
                    "Expected no output from write operation, got {} rows",
                    batch.num_rows()
                )));
            }
        }

        // Extract merge stats from the execution plan
        let (stats, transaction, affected_rows, inserted_rows_filter) = if let Some(full_exec) =
            plan.downcast_ref::<exec::FullSchemaMergeInsertExec>()
        {
            let stats = full_exec.merge_stats().ok_or_else(|| {
                Error::internal("Merge stats not available - execution may not have completed")
            })?;
            let transaction = full_exec.transaction().ok_or_else(|| {
                Error::internal("Transaction not available - execution may not have completed")
            })?;
            let affected_rows = full_exec.affected_rows().map(RowAddrTreeMap::from);
            let inserted_rows_filter = full_exec.inserted_rows_filter();
            (stats, transaction, affected_rows, inserted_rows_filter)
        } else if let Some(delete_exec) = plan.downcast_ref::<exec::DeleteOnlyMergeInsertExec>() {
            let stats = delete_exec.merge_stats().ok_or_else(|| {
                Error::internal("Merge stats not available - execution may not have completed")
            })?;
            let transaction = delete_exec.transaction().ok_or_else(|| {
                Error::internal("Transaction not available - execution may not have completed")
            })?;
            let affected_rows = delete_exec.affected_rows().map(RowAddrTreeMap::from);
            (stats, transaction, affected_rows, None)
        } else {
            return Err(Error::internal(
                "Expected FullSchemaMergeInsertExec or DeleteOnlyMergeInsertExec",
            ));
        };

        Ok((transaction, stats, affected_rows, inserted_rows_filter))
    }

    /// Check if the merge insert operation can use the fast path (create_plan).
    ///
    /// The fast path is available when:
    /// - `when_matched` is `UpdateAll`, `UpdateIf`, `Fail`, `Delete`, or `DoNothing`
    /// - Either `use_index` is false OR there's no scalar index on the join key
    /// - The source schema is either (a) the full dataset schema, or (b) a
    ///   subset of it (partial-schema upsert), or (c) just the key columns for
    ///   delete-only operations
    /// - `when_not_matched_by_source` is `Keep`, `Delete`, or `DeleteIf`
    ///
    /// For partial-schema upserts with `insert_not_matched=true`, every missing
    /// target column must be nullable — otherwise this method returns an
    /// `InvalidInput` error, because inserted rows would otherwise attempt to
    /// write a non-nullable NULL downstream.
    async fn can_use_create_plan(&self, source_schema: &Schema) -> Result<bool> {
        // Convert to lance schema for comparison
        let lance_schema = lance_core::datatypes::Schema::try_from(source_schema)?;
        let full_schema = self.dataset.schema();
        let is_full_schema = full_schema.compare_with_options(
            &lance_schema,
            &SchemaCompareOptions {
                compare_metadata: false,
                // Allow nullable source fields for non-nullable targets.
                compare_nullability: NullabilityComparison::Ignore,
                // Allow columns to be in a different order; they will be matched by name.
                ignore_field_order: true,
                ..Default::default()
            },
        );

        // Partial-schema upsert: every source field must exist in the target
        // and have a compatible data type. Missing target columns will be
        // filled from the target side of the join in `create_plan`.
        let is_subset_schema = !is_full_schema
            && lance_schema.fields.iter().all(|sf| {
                full_schema
                    .field(&sf.name)
                    .map(|tf| tf.data_type() == sf.data_type())
                    .unwrap_or(false)
            });

        // If the user is inserting unmatched rows with a partial source, any
        // target column missing from the source would receive NULL for those
        // inserts. Non-nullable targets cannot accept that, so reject early
        // with a descriptive error instead of failing later in the writer.
        if is_subset_schema && self.params.insert_not_matched {
            let non_nullable_missing: Vec<&str> = full_schema
                .fields
                .iter()
                .filter(|tf| lance_schema.field(&tf.name).is_none() && !tf.nullable)
                .map(|tf| tf.name.as_str())
                .collect();
            if !non_nullable_missing.is_empty() {
                return Err(Error::invalid_input(format!(
                    "Cannot insert rows with a partial-schema source: target column(s) \
                     {:?} are non-nullable and not provided by the source. Either add \
                     them to the source or set when_not_matched to DoNothing.",
                    non_nullable_missing
                )));
            }
        }

        // A partial-schema source that both deletes matched rows and inserts
        // unmatched rows cannot be expressed by the indexed-scan delete path
        // (the delete cannot be folded into a partial write). Keep it off the
        // scalar-index route so it falls through to the v2 plan, which handles
        // delete + insert directly.
        let is_partial_delete_with_insert = is_subset_schema
            && self.params.insert_not_matched
            && matches!(self.params.when_matched, WhenMatched::Delete);

        let would_use_scalar_index = if self.params.use_index
            && !is_partial_delete_with_insert
            && matches!(
                self.params.delete_not_matched_by_source,
                WhenNotMatchedBySource::Keep
            ) {
            // Only when EVERY join column is indexed. A partially-indexed
            // composite key cannot be fully resolved by the index probe and the
            // indexed-scan path then under-matches (e.g. a delete silently
            // no-ops), so it must fall through to the correct full path.
            self.indexed_join_keys().await?.len() == self.params.on.len()
        } else {
            false
        };

        // Check if this is a delete-only operation (no update/insert writes needed from source)
        // For delete-only, we don't need the full source schema, just key columns for matching
        let no_upsert = matches!(
            self.params.when_matched,
            WhenMatched::Delete | WhenMatched::DoNothing
        ) && !self.params.insert_not_matched;

        // For delete-only, verify source has all key columns
        let source_has_key_columns = self.params.on.iter().all(|key| {
            source_schema
                .fields()
                .iter()
                .any(|f| f.name() == key.as_str())
        });
        let schema_ok = is_full_schema || is_subset_schema || (no_upsert && source_has_key_columns);

        Ok(matches!(
            self.params.when_matched,
            WhenMatched::UpdateAll
                | WhenMatched::UpdateIf(_)
                | WhenMatched::UpdateIfExpr(_)
                | WhenMatched::Fail
                | WhenMatched::Delete
                | WhenMatched::DoNothing
        ) && !would_use_scalar_index
            && schema_ok
            && matches!(
                self.params.delete_not_matched_by_source,
                WhenNotMatchedBySource::Keep
                    | WhenNotMatchedBySource::Delete
                    | WhenNotMatchedBySource::DeleteIf(_)
            ))
    }

    async fn execute_uncommitted_impl(
        self,
        provider: Arc<dyn TableProvider>,
    ) -> Result<UncommittedMergeInsert> {
        // Check if we can use the fast path
        let can_use_fast_path = self.can_use_create_plan(provider.schema().as_ref()).await?;

        if can_use_fast_path {
            let (transaction, stats, affected_rows, inserted_rows_filter) =
                self.execute_uncommitted_v2(provider).await?;
            return Ok(UncommittedMergeInsert {
                transaction,
                affected_rows,
                stats,
                inserted_rows_filter,
            });
        }

        let target_bases_info = resolve_target_bases(&self.dataset, &self.params).await?;

        // The slow path consumes a single stream; adapt the provider back into one.
        let source = provider_to_stream(provider).await?;
        let source_schema = source.schema();
        let lance_schema = lance_core::datatypes::Schema::try_from(source_schema.as_ref())?;
        let full_schema = self.dataset.schema();
        let is_full_schema = full_schema.compare_with_options(
            &lance_schema,
            &SchemaCompareOptions {
                compare_metadata: false,
                // Allow nullable source fields for non-nullable targets.
                compare_nullability: NullabilityComparison::Ignore,
                ..Default::default()
            },
        );
        let joined = self.create_joined_stream(source).await?;
        let merger = Merger::try_new(
            self.params.clone(),
            source_schema,
            !is_full_schema,
            self.dataset.manifest.uses_stable_row_ids(),
        )?;
        let merge_statistics = merger.merge_stats.clone();
        let deleted_rows = merger.deleted_rows.clone();
        let updating_row_ids = merger.updating_row_ids.clone();
        let merger_schema = merger.output_schema().clone();
        let stream = joined
            .and_then(move |batch| merger.clone().execute_batch(batch))
            .try_flatten();
        let stream = RecordBatchStreamAdapter::new(merger_schema, stream);

        // A partial-schema source can patch columns or delete matched rows, but
        // not both in one commit: the writer rejects subschema rows up front and
        // the delete cannot be folded into the write. Reject that combination.
        if !is_full_schema
            && matches!(self.params.when_matched, WhenMatched::Delete)
            && self.params.insert_not_matched
        {
            return Err(Error::not_supported_source("Combining when_matched(Delete) with inserts from a partial-schema source is not supported; provide the full target schema in the source".into()));
        }

        // The commit strategy follows what the merge does to matched rows. A
        // pure delete (no inserts) writes nothing: the merger emits no batches
        // and only records the matched row ids. This holds for any source schema
        // width, so it is keyed on the operation rather than `is_full_schema`.
        let is_delete_only = matches!(self.params.when_matched, WhenMatched::Delete)
            && !self.params.insert_not_matched;

        let (operation, affected_rows) = if is_delete_only {
            // Consume the stream so the merger records the matched row ids in
            // `deleted_rows`; it produces no batches.
            let drained: Vec<RecordBatch> = Box::pin(stream).try_collect().await?;
            debug_assert!(drained.is_empty(), "delete-only merge must not emit rows");

            let removed_row_ids = Arc::into_inner(deleted_rows).unwrap().into_inner().unwrap();
            let removed_row_addr_vec =
                if let Some(row_id_index) = get_row_id_index(&self.dataset).await? {
                    removed_row_ids
                        .iter()
                        .filter_map(|id| row_id_index.get(*id).map(|address| address.into()))
                        .collect::<Vec<_>>()
                } else {
                    removed_row_ids
                };
            let removed_row_addrs = RoaringTreemap::from_iter(removed_row_addr_vec);

            let (updated_fragments, removed_fragment_ids) =
                Self::apply_deletions(&self.dataset, &removed_row_addrs).await?;

            let operation = Operation::Update {
                removed_fragment_ids,
                updated_fragments,
                new_fragments: vec![],
                fields_modified: vec![],
                compacted_sstables: self.params.compacted_sstables.clone(),
                fields_for_preserving_frag_bitmap: full_schema
                    .fields
                    .iter()
                    .map(|f| f.id as u32)
                    .collect(),
                update_mode: Some(RewriteRows),
                inserted_rows_filter: None, // not implemented for v1
                updated_fragment_offsets: None,
            };

            let affected_rows = Some(RowAddrTreeMap::from(removed_row_addrs));
            (operation, affected_rows)
        } else if !is_full_schema {
            // Non-delete partial-schema merge: patch the provided columns into
            // existing fragments in place. (Delete is handled above; a wider
            // full source takes the row-rewrite branch below.)
            if !matches!(
                self.params.delete_not_matched_by_source,
                WhenNotMatchedBySource::Keep
            ) {
                return Err(Error::not_supported_source("Deleting rows from the target table when there is no match in the source table is not supported when the source data has a different schema than the target data".into()));
            }

            // We will have a different commit path here too, as we are modifying
            // fragments rather than writing new ones
            let (updated_fragments, new_fragments, fields_modified) = Self::update_fragments(
                self.dataset.clone(),
                Box::pin(stream),
                self.dataset.manifest.version + 1,
                target_bases_info,
            )
            .await?;

            let operation = Operation::Update {
                removed_fragment_ids: Vec::new(),
                updated_fragments,
                new_fragments,
                fields_modified,
                compacted_sstables: self.params.compacted_sstables.clone(),
                fields_for_preserving_frag_bitmap: vec![], // in-place update do not affect preserving frag bitmap
                update_mode: Some(RewriteColumns),
                inserted_rows_filter: None, // not implemented for v1
                updated_fragment_offsets: None,
            };
            // We have rewritten the fragments, not just the deletion files, so
            // we can't use affected rows here.
            (operation, None)
        } else {
            let cleanup_bases = target_bases_info.clone();
            let (mut new_fragments, _) = write_fragments_internal(
                self.dataset
                    .manifest
                    .data_storage_format
                    .lance_file_format(),
                Some(&self.dataset),
                self.dataset.object_store.clone(),
                &self.dataset.base,
                self.dataset.schema().clone(),
                Box::pin(stream),
                WriteParams::default(),
                target_bases_info,
            )
            .await?;

            // The new data files exist but are not committed yet; clean them up
            // (including files routed to target bases) if any later step fails.
            let post_write_result: Result<RoaringTreemap> = async {
                if let Some(row_id_sequence) = updating_row_ids.lock().unwrap().row_id_sequence() {
                    let fragment_sizes = new_fragments
                        .iter()
                        .map(|f| f.physical_rows.unwrap() as u64);

                    let sequences = lance_table::rowids::rechunk_sequences(
                        [row_id_sequence.clone()],
                        fragment_sizes,
                        true,
                    )
                    .map_err(|e| {
                        Error::internal(format!(
                            "Captured row ids not equal to number of rows written: {}",
                            e
                        ))
                    })?;

                    for (fragment, sequence) in new_fragments.iter_mut().zip(sequences) {
                        let serialized = lance_table::rowids::write_row_ids(&sequence);
                        fragment.row_id_meta = Some(RowIdMeta::Inline(serialized));
                    }
                }

                // Apply deletions
                let removed_row_ids = Arc::into_inner(deleted_rows).unwrap().into_inner().unwrap();

                let removed_row_addr_vec =
                    if let Some(row_id_index) = get_row_id_index(&self.dataset).await? {
                        let addresses: Vec<u64> = removed_row_ids
                            .iter()
                            .filter_map(|id| row_id_index.get(*id).map(|address| address.into()))
                            .collect::<Vec<_>>();
                        addresses
                    } else {
                        removed_row_ids
                    };

                Ok(RoaringTreemap::from_iter(removed_row_addr_vec))
            }
            .await;
            let removed_row_addrs = match post_write_result {
                Ok(removed_row_addrs) => removed_row_addrs,
                Err(e) => {
                    cleanup_data_fragments(
                        &self.dataset.object_store,
                        &self.dataset.base,
                        cleanup_bases.as_deref(),
                        &new_fragments,
                    )
                    .await;
                    return Err(e);
                }
            };

            let deletions_result = Self::apply_deletions(&self.dataset, &removed_row_addrs).await;
            let (old_fragments, removed_fragment_ids) = match deletions_result {
                Ok(v) => v,
                Err(e) => {
                    cleanup_data_fragments(
                        &self.dataset.object_store,
                        &self.dataset.base,
                        cleanup_bases.as_deref(),
                        &new_fragments,
                    )
                    .await;
                    return Err(e);
                }
            };

            // Commit updated and new fragments
            let operation = Operation::Update {
                removed_fragment_ids,
                updated_fragments: old_fragments,
                new_fragments,
                // On this path we only make deletions against updated_fragments and will not
                // modify any field values.
                fields_modified: vec![],
                compacted_sstables: self.params.compacted_sstables.clone(),
                fields_for_preserving_frag_bitmap: full_schema
                    .fields
                    .iter()
                    .map(|f| f.id as u32)
                    .collect(),
                update_mode: Some(RewriteRows),
                inserted_rows_filter: None, // not implemented for v1
                updated_fragment_offsets: None,
            };

            let affected_rows = Some(RowAddrTreeMap::from(removed_row_addrs));
            (operation, affected_rows)
        };

        let stats = Arc::into_inner(merge_statistics)
            .unwrap()
            .into_inner()
            .unwrap();

        let transaction = Transaction::new(self.dataset.manifest.version, operation, None);

        Ok(UncommittedMergeInsert {
            transaction,
            affected_rows,
            stats,
            inserted_rows_filter: None, // not implemented for v1
        })
    }

    // Delete a batch of rows by id, returns the fragments modified and the fragments removed
    async fn apply_deletions(
        dataset: &Dataset,
        removed_row_ids: &RoaringTreemap,
    ) -> Result<(Vec<Fragment>, Vec<u64>)> {
        let bitmaps = Arc::new(removed_row_ids.bitmaps().collect::<BTreeMap<_, _>>());

        enum FragmentChange {
            Unchanged,
            Modified(Box<Fragment>),
            Removed(u64),
        }

        let mut updated_fragments = Vec::new();
        let mut removed_fragments = Vec::new();

        let mut stream = futures::stream::iter(dataset.get_fragments())
            .map(move |fragment| {
                let bitmaps_ref = bitmaps.clone();
                async move {
                    let fragment_id = fragment.id();
                    if let Some(bitmap) = bitmaps_ref.get(&(fragment_id as u32)) {
                        match fragment.extend_deletions(*bitmap).await {
                            Ok(Some(new_fragment)) => {
                                Ok(FragmentChange::Modified(Box::new(new_fragment.metadata)))
                            }
                            Ok(None) => Ok(FragmentChange::Removed(fragment_id as u64)),
                            Err(e) => Err(e),
                        }
                    } else {
                        Ok(FragmentChange::Unchanged)
                    }
                }
            })
            .buffer_unordered(dataset.object_store.io_parallelism());

        while let Some(res) = stream.next().await.transpose()? {
            match res {
                FragmentChange::Unchanged => {}
                FragmentChange::Modified(fragment) => updated_fragments.push(*fragment),
                FragmentChange::Removed(fragment_id) => removed_fragments.push(fragment_id),
            }
        }

        Ok((updated_fragments, removed_fragments))
    }

    /// Generate the execution plan and return it as a formatted string for debugging.
    ///
    /// This method takes an optional schema representing the source data and calls `create_plan()`
    /// to generate the execution plan, then formats it for display. If no schema is provided,
    /// defaults to the dataset's schema. The verbose flag controls the level of detail shown.
    ///
    /// # Arguments
    ///
    /// * `schema` - Optional schema of the source data. If None, uses the dataset's schema
    /// * `verbose` - If true, provides more detailed information in the plan output
    ///
    /// # Errors
    ///
    /// Returns Error::NotSupported if the merge insert configuration doesn't support
    /// the fast path required for plan generation.
    pub async fn explain_plan(&self, schema: Option<&Schema>, verbose: bool) -> Result<String> {
        // Use provided schema or default to dataset schema
        let schema = match schema {
            Some(s) => s.clone(),
            None => arrow_schema::Schema::from(self.dataset.schema()),
        };

        // Check if we can use create_plan
        if !self.can_use_create_plan(&schema).await? {
            return Err(Error::not_supported_source("This merge insert configuration does not support explain_plan. Only full-schema merge insert operations without a scalar-index execution path are currently supported.".into()));
        }

        // Create an empty batch with the provided schema to pass to create_plan
        let empty_batch = RecordBatch::new_empty(Arc::new(schema.clone()));
        let stream = RecordBatchStreamAdapter::new(
            Arc::new(schema.clone()),
            futures::stream::once(async { Ok(empty_batch) }).boxed(),
        );

        // Clone self since create_plan consumes the job
        let cloned_job = self.clone();
        let plan = cloned_job
            .create_plan(one_shot_provider(Box::pin(stream))?)
            .await?;
        let display = DisplayableExecutionPlan::new(plan.as_ref());

        Ok(format!("{}", display.indent(verbose)))
    }

    /// Generate the execution plan, execute it with the provided data to collect metrics,
    /// and return the analysis.
    ///
    /// This method takes actual source data, calls `create_plan()` to generate the plan,
    /// and executes it to collect performance metrics and analysis.
    ///
    /// **Note:** This method executes the merge insert operation to collect metrics
    /// but **does not commit the changes**. While data files may be written to storage
    /// during execution, they will not be referenced by any dataset version and the
    /// dataset remains unchanged. This is intended for performance analysis only.
    ///
    /// # Arguments
    ///
    /// * `source` - The source data stream that would be used in the merge insert
    ///
    /// # Errors
    ///
    /// Returns Error::NotSupported if the merge insert configuration doesn't support
    /// the fast path required for plan generation.
    pub async fn analyze_plan(&self, source: SendableRecordBatchStream) -> Result<String> {
        // Check if we can use create_plan
        if !self.can_use_create_plan(source.schema().as_ref()).await? {
            return Err(Error::not_supported_source("This merge insert configuration does not support analyze_plan. Only full-schema merge insert operations without a scalar-index execution path are currently supported.".into()));
        }

        // Clone self since create_plan consumes the job
        let cloned_job = self.clone();
        let plan = cloned_job.create_plan(one_shot_provider(source)?).await?;

        // Use the analyze_plan function from lance_datafusion, but strip out the wrapper lines
        let options = LanceExecutionOptions::default();
        let full_analysis = analyze_plan(plan, options).await?;

        // Remove the AnalyzeExec and TracedExec lines from the output
        let lines: Vec<&str> = full_analysis.lines().collect();
        let filtered_lines: Vec<&str> = lines
            .into_iter()
            .filter(|line| {
                !line.trim_start().starts_with("AnalyzeExec")
                    && !line.trim_start().starts_with("TracedExec")
            })
            .collect();

        Ok(filtered_lines.join("\n"))
    }
}

/// Merger will store these statistics as it runs (for each batch)
#[derive(Debug, Default, Clone)]
pub struct MergeStats {
    /// Number of inserted rows (for user statistics)
    pub num_inserted_rows: u64,
    /// Number of updated rows (for user statistics)
    pub num_updated_rows: u64,
    /// Number of deleted rows (for user statistics)
    /// Note: This is different from internal references to 'deleted_rows', since we technically "delete" updated rows during processing.
    /// However those rows are not shared with the user.
    pub num_deleted_rows: u64,
    /// Number of attempts performed.
    ///
    /// See [`MergeInsertBuilder::conflict_retries`] for more information.
    pub num_attempts: u32,
    /// Total bytes written to storage. This currently only includes data files.
    pub bytes_written: u64,
    /// Number of data files written. This currently only includes data files.
    pub num_files_written: u64,
    /// Number of duplicate source rows skipped (when SourceDedupeBehavior::FirstSeen)
    pub num_skipped_duplicates: u64,
}

pub struct UncommittedMergeInsert {
    pub transaction: Transaction,
    pub affected_rows: Option<RowAddrTreeMap>,
    pub stats: MergeStats,
    pub inserted_rows_filter: Option<KeyExistenceFilter>,
}

/// Wrapper struct that combines MergeInsertJob with the source provider for retry functionality
#[derive(Clone)]
struct MergeInsertJobWithProvider {
    job: MergeInsertJob,
    provider: Arc<dyn TableProvider>,
    attempt_count: Arc<AtomicU32>,
}

impl RetryExecutor for MergeInsertJobWithProvider {
    type Data = UncommittedMergeInsert;
    type Result = (Arc<Dataset>, MergeStats);

    async fn execute_impl(&self) -> Result<Self::Data> {
        // Increment attempt counter
        self.attempt_count.fetch_add(1, Ordering::SeqCst);

        // Re-scan the provider on each retry attempt.
        self.job
            .clone()
            .execute_uncommitted_impl(self.provider.clone())
            .await
    }

    async fn commit(&self, dataset: Arc<Dataset>, mut data: Self::Data) -> Result<Self::Result> {
        // Update stats with the current attempt count
        data.stats.num_attempts = self.attempt_count.load(Ordering::SeqCst);

        // The dataset argument is the refreshed per-attempt dataset (the same
        // manifest execute_impl resolved against); keep a handle so conflict
        // cleanup resolves bases added between attempts.
        let cleanup_dataset = dataset.clone();
        let mut commit_builder =
            CommitBuilder::new(dataset).with_skip_auto_cleanup(self.job.params.skip_auto_cleanup);
        if let Some(commit_retries) = self.job.params.commit_retries {
            commit_builder = commit_builder.with_max_retries(commit_retries);
        }
        if let Some(affected_rows) = data.affected_rows {
            commit_builder = commit_builder.with_affected_rows(affected_rows);
        }

        let new_fragments = match &data.transaction.operation {
            Operation::Update { new_fragments, .. } => new_fragments.clone(),
            _ => Vec::new(),
        };
        match commit_builder.execute(data.transaction).await {
            Ok(new_dataset) => Ok((Arc::new(new_dataset), data.stats)),
            Err(e) => {
                // A retryable conflict discards this attempt and re-executes it,
                // so its data files are provably uncommitted; remove them
                // (including files routed to target bases, which version cleanup
                // never scans). Other commit errors may be ambiguous about
                // whether the manifest was written, so leave the files alone.
                if matches!(e, Error::RetryableCommitConflict { .. }) && !new_fragments.is_empty() {
                    let target_bases_info =
                        resolve_target_bases(&cleanup_dataset, &self.job.params)
                            .await
                            .ok()
                            .flatten();
                    cleanup_data_fragments(
                        &cleanup_dataset.object_store,
                        &cleanup_dataset.base,
                        target_bases_info.as_deref(),
                        &new_fragments,
                    )
                    .await;
                }
                Err(e)
            }
        }
    }

    fn update_dataset(&mut self, dataset: Arc<Dataset>) {
        self.job.dataset = dataset;
    }
}

// A sync-safe structure that is shared by all of the "process batch" tasks.
//
// Note: we are not currently using parallelism but this still needs to be sync because it is
//       held across an await boundary (and we might use parallelism someday)
#[derive(Debug, Clone)]
struct Merger {
    // As the merger runs it will update the list of deleted rows
    deleted_rows: Arc<Mutex<Vec<u64>>>,
    // Shared collection to capture row ids that need to be updated
    updating_row_ids: Arc<Mutex<CapturedRowIds>>,
    // Physical delete expression, only set if params.delete_not_matched_by_source is DeleteIf
    delete_expr: Option<Arc<dyn PhysicalExpr>>,
    // User statistics for merging
    merge_stats: Arc<Mutex<MergeStats>>,
    // Physical "when matched update if" expression, only set if params.when_matched is UpdateIf
    match_filter_expr: Option<Arc<dyn PhysicalExpr>>,
    // The parameters controlling the merge
    params: MergeInsertParams,
    // The schema of the input data, used to recover nullability information
    schema: Arc<Schema>,
    /// Whether the output schema should include a row address column
    with_row_addr: bool,
    /// The output schema of the stream.
    output_schema: Arc<Schema>,
    /// Whether to enable stable row ids
    enable_stable_row_ids: bool,
    /// Set to track processed row IDs to detect duplicates
    processed_row_ids: Arc<Mutex<HashSet<u64>>>,
    /// Set to track non-null keys of rows inserted by FirstSeen mode
    processed_insert_keys: Arc<Mutex<InsertedKeyTracker>>,
}

impl Merger {
    // Creates a new merger with an empty set of deleted rows, compiles expressions, if present
    fn try_new(
        params: MergeInsertParams,
        schema: Arc<Schema>,
        with_row_addr: bool,
        enable_stable_row_ids: bool,
    ) -> Result<Self> {
        let delete_expr = if let WhenNotMatchedBySource::DeleteIf(expr) =
            &params.delete_not_matched_by_source
        {
            let planner = Planner::new(schema.clone());
            let expr = planner.optimize_expr(expr.clone())?;
            let physical_expr = planner.create_physical_expr(&expr)?;
            let data_type = physical_expr.data_type(&schema)?;
            if data_type != DataType::Boolean {
                return Err(Error::invalid_input(format!(
                    "Merge insert conditions must be expressions that return a boolean value, received expression ({}) which has data type {}",
                    expr, data_type
                )));
            }
            Some(physical_expr)
        } else {
            None
        };
        let match_filter_expr = match &params.when_matched {
            WhenMatched::UpdateIf(_) | WhenMatched::UpdateIfExpr(_) => {
                let combined_schema = Arc::new(combined_schema(&schema));
                let planner = Planner::new(combined_schema.clone());
                let expr = match &params.when_matched {
                    WhenMatched::UpdateIf(expr_str) => planner.parse_filter(expr_str)?,
                    WhenMatched::UpdateIfExpr(expr) => expr.clone(),
                    _ => unreachable!(),
                };
                let expr = planner.optimize_expr(expr)?;
                let match_expr = planner.create_physical_expr(&expr)?;
                let data_type = match_expr.data_type(combined_schema.as_ref())?;
                if data_type != DataType::Boolean {
                    return Err(Error::invalid_input(format!(
                        "Merge insert conditions must be expressions that return a boolean value, received a 'when matched update if' expression ({}) which has data type {}",
                        expr, data_type
                    )));
                }
                Some(match_expr)
            }
            _ => None,
        };
        let output_schema = if with_row_addr {
            Arc::new(schema.try_with_column(ROW_ADDR_FIELD.clone())?)
        } else {
            schema.clone()
        };

        Ok(Self {
            deleted_rows: Arc::new(Mutex::new(Vec::new())),
            updating_row_ids: Arc::new(Mutex::new(CapturedRowIds::new(enable_stable_row_ids))),
            delete_expr,
            merge_stats: Arc::new(Mutex::new(MergeStats::default())),
            match_filter_expr,
            params,
            schema,
            with_row_addr,
            output_schema,
            enable_stable_row_ids,
            processed_row_ids: Arc::new(Mutex::new(HashSet::new())),
            processed_insert_keys: Arc::new(Mutex::new(InsertedKeyTracker::default())),
        })
    }

    fn output_schema(&self) -> &Arc<Schema> {
        &self.output_schema
    }

    // Retrieves a bitmap of rows where at least one of the given columns is
    // not null.
    fn not_all_null(batch: &RecordBatch, cols: &[usize]) -> Result<BooleanArray> {
        // For our purposes we know there is always at least 1 on key
        debug_assert!(!cols.is_empty());
        let mut at_least_one_valid = arrow::compute::is_not_null(batch.column(cols[0]))?;
        for &idx in &cols[1..] {
            let is_valid = arrow::compute::is_not_null(batch.column(idx))?;
            at_least_one_valid = arrow::compute::or(&at_least_one_valid, &is_valid)?;
        }
        Ok(at_least_one_valid)
    }

    // Since we are performing an
    // outer join below we expect the results to look like:
    //
    // | LEFT KEYS | LEFT PAYLOAD | RIGHT KEYS | RIGHT PAYLOAD |
    // | NULL      | NULL         | NOT NULL   | ************* | <- when not matched
    // | ********* | ************ | ********** | ************* | <- when matched
    // | ********* | ************ | NULL       | NULL          | <- when not matched by source
    //
    // To test which case we are in we check to see if all of LEFT KEYS or RIGHT KEYS are null
    //
    // This returns three selection bitmaps
    //
    //  - The first is true for rows that are in the left side only
    //  - The second is true for rows in both the left and the right
    //  - The third is true for rows in the right side only
    fn extract_selections(
        &self,
        combined_batch: &RecordBatch,
        right_offset: usize,
        num_keys: usize,
    ) -> Result<(BooleanArray, BooleanArray, BooleanArray)> {
        // The outer join distinguishes its three cases by which side's join
        // keys were NULL-padded: a present row always has non-null keys, while
        // the absent side is filled with NULLs. We therefore test the *key*
        // columns, located by name. They are NOT necessarily the first
        // `num_keys` columns — a partial-schema source can place a payload
        // column (e.g. an all-null vector) at position 0, and checking
        // positions [0, num_keys) there misreads an all-null leading payload
        // column as an absent join side, silently dropping every matched row
        // (https://github.com/lancedb/lancedb/issues/3515). The target half
        // carries the same columns in the same order, offset by `right_offset`.
        let source_key_cols = self
            .params
            .on
            .iter()
            .map(|key| {
                combined_batch.schema().index_of(key).map_err(|_| {
                    Error::internal(format!(
                        "merge insert key column '{}' not found in joined batch",
                        key
                    ))
                })
            })
            .collect::<Result<Vec<_>>>()?;
        debug_assert_eq!(source_key_cols.len(), num_keys);
        let target_key_cols = source_key_cols
            .iter()
            .map(|c| c + right_offset)
            .collect::<Vec<_>>();

        let in_left = Self::not_all_null(combined_batch, &source_key_cols)?;
        let in_right = Self::not_all_null(combined_batch, &target_key_cols)?;
        let in_both = arrow::compute::and(&in_left, &in_right)?;
        let left_only = arrow::compute::and(&in_left, &arrow::compute::not(&in_right)?)?;
        let right_only = arrow::compute::and(&arrow::compute::not(&in_left)?, &in_right)?;
        Ok((left_only, in_both, right_only))
    }

    // Given a batch of outer join data, split it into three different batches
    //
    // Process each sub-batch according to the merge insert params
    //
    // Returns 0, 1, or 2 batches
    // Potentially updates (as a side-effect) the deleted rows vec
    async fn execute_batch(
        self,
        batch: RecordBatch,
    ) -> datafusion::common::Result<impl Stream<Item = datafusion::common::Result<RecordBatch>>>
    {
        let mut merge_statistics = self.merge_stats.lock().unwrap();
        let num_fields = batch.schema().fields.len();
        // The schema of the combined batches will be:
        // source_keys, source_payload, target_keys, target_payload, row_id, row_addr?
        // The keys and non_keys on both sides will be equal
        let (row_id_col, row_addr_col, right_offset) = if num_fields % 2 == 1 {
            // No rowaddr
            assert!(!self.with_row_addr);
            (num_fields - 1, None, num_fields / 2)
        } else {
            // Has rowaddr
            assert!(self.with_row_addr);
            (num_fields - 2, Some(num_fields - 1), (num_fields - 2) / 2)
        };

        let num_keys = self.params.on.len();

        let left_cols = Vec::from_iter(0..right_offset);
        let right_cols_with_id = Vec::from_iter(right_offset..num_fields);

        let mut batches = Vec::with_capacity(2);
        let (left_only, in_both, right_only) =
            self.extract_selections(&batch, right_offset, num_keys)?;

        // There is no contention on this mutex.  We're only using it to bypass the rust
        // borrow checker (the stream needs to be `sync` since it crosses an await point)
        let mut deleted_row_ids = self.deleted_rows.lock().unwrap();

        // Each `WhenMatched` variant handles the matched rows (`in_both`)
        // differently.
        let match_filter_expr = self.match_filter_expr;
        match &self.params.when_matched {
            WhenMatched::DoNothing => {}
            WhenMatched::Delete => {
                // Matched rows are removed, not rewritten: record their row ids
                // for the commit to delete and emit no replacement batch. A
                // source with duplicate keys matches the same target row more
                // than once; apply the same `source_dedupe_behavior` policy as
                // updates so a duplicate either aborts (`Fail`) or is skipped
                // and counted once (`FirstSeen`) — the commit deletes the row a
                // single time regardless.
                let matched = arrow::compute::filter_record_batch(&batch, &in_both)?;
                let row_ids = matched.column(row_id_col).as_primitive::<UInt64Type>();

                let mut processed_row_ids = self.processed_row_ids.lock().unwrap();
                for (row_idx, &row_id) in row_ids.values().iter().enumerate() {
                    if processed_row_ids.insert(row_id) {
                        merge_statistics.num_deleted_rows += 1;
                        deleted_row_ids.push(row_id);
                    } else {
                        match self.params.source_dedupe_behavior {
                            SourceDedupeBehavior::Fail => {
                                return Err(create_duplicate_row_error(
                                    &matched,
                                    row_idx,
                                    &self.params.on,
                                ));
                            }
                            SourceDedupeBehavior::FirstSeen => {
                                merge_statistics.num_skipped_duplicates += 1;
                            }
                        }
                    }
                }
            }
            WhenMatched::Fail => {
                // Any matched row aborts the whole operation.
                if let Some(row_idx) = (0..in_both.len()).find(|&i| in_both.value(i)) {
                    return Err(DataFusionError::Execution(format!(
                        "Merge insert failed: found matching row with key values: {}",
                        format_key_values_on_columns(&batch, row_idx, &self.params.on)
                    )));
                }
            }
            WhenMatched::UpdateAll | WhenMatched::UpdateIf(_) | WhenMatched::UpdateIfExpr(_) => {
                let mut matched = arrow::compute::filter_record_batch(&batch, &in_both)?;

                if let Some(match_filter) = match_filter_expr {
                    let unzipped = unzip_batch(&matched, &self.schema);
                    let filtered = match_filter.evaluate(&unzipped)?;
                    match filtered {
                        ColumnarValue::Array(mask) => {
                            // Some rows matched, filter down and replace those rows
                            matched =
                                arrow::compute::filter_record_batch(&matched, mask.as_boolean())?;
                        }
                        ColumnarValue::Scalar(scalar) => {
                            if let ScalarValue::Boolean(Some(true)) = scalar {
                                // All rows matched, go ahead and replace the whole batch
                            } else {
                                // Nothing matched, replace nothing
                                matched = RecordBatch::new_empty(matched.schema());
                            }
                        }
                    }
                }

                merge_statistics.num_updated_rows += matched.num_rows() as u64;

                // If the filter eliminated all rows then its important we don't try and write
                // the batch at all.  Writing an empty batch currently panics
                if matched.num_rows() > 0 {
                    let row_ids = matched.column(row_id_col).as_primitive::<UInt64Type>();

                    let mut processed_row_ids = self.processed_row_ids.lock().unwrap();
                    let mut keep_indices: Vec<u32> = Vec::with_capacity(matched.num_rows());
                    for (row_idx, &row_id) in row_ids.values().iter().enumerate() {
                        if processed_row_ids.insert(row_id) {
                            keep_indices.push(row_idx as u32);
                        } else {
                            match self.params.source_dedupe_behavior {
                                SourceDedupeBehavior::Fail => {
                                    return Err(create_duplicate_row_error(
                                        &matched,
                                        row_idx,
                                        &self.params.on,
                                    ));
                                }
                                SourceDedupeBehavior::FirstSeen => {
                                    // Skip this duplicate row (don't add to keep_indices)
                                }
                            }
                        }
                    }
                    drop(processed_row_ids);

                    // Filter out duplicate rows if any were skipped
                    let num_skipped = matched.num_rows() - keep_indices.len();
                    if num_skipped > 0 {
                        merge_statistics.num_skipped_duplicates += num_skipped as u64;
                        merge_statistics.num_updated_rows -= num_skipped as u64;

                        let indices = UInt32Array::from(keep_indices);
                        matched = take_record_batch(&matched, &indices)?;
                    }

                    // Only process and write if there are remaining rows after filtering duplicates
                    if matched.num_rows() > 0 {
                        // Get row_ids again after filtering (if any duplicates were removed)
                        let row_ids = matched.column(row_id_col).as_primitive::<UInt64Type>();
                        deleted_row_ids.extend(row_ids.values());
                        if self.enable_stable_row_ids {
                            self.updating_row_ids
                                .lock()
                                .unwrap()
                                .capture(row_ids.values())?;
                        }

                        let projection = if let Some(row_addr_col) = row_addr_col {
                            let mut cols = Vec::from_iter(left_cols.iter().cloned());
                            cols.push(row_addr_col);
                            cols
                        } else {
                            #[allow(clippy::redundant_clone)]
                            left_cols.clone()
                        };
                        let matched = matched.project(&projection)?;
                        // The payload columns of an outer join are always nullable.  We need to restore
                        // non-nullable to columns that were originally non-nullable.  This should be safe
                        // since the not_matched rows should all be valid on the right_cols
                        //
                        // Sadly we can't use with_schema because it doesn't let you toggle nullability
                        let matched = RecordBatch::try_new(
                            self.output_schema.clone(),
                            Vec::from_iter(matched.columns().iter().cloned()),
                        )?;
                        batches.push(Ok(matched));
                    }
                }
            }
        }
        if self.params.insert_not_matched {
            let mut not_matched = arrow::compute::filter_record_batch(&batch, &left_only)?;
            if self.params.source_dedupe_behavior == SourceDedupeBehavior::FirstSeen {
                let mut processed_insert_keys = self.processed_insert_keys.lock().unwrap();
                let mut keep_indices = Vec::with_capacity(not_matched.num_rows());
                for row_idx in 0..not_matched.num_rows() {
                    if processed_insert_keys.insert(&not_matched, row_idx, &self.params.on)? {
                        keep_indices.push(row_idx as u32);
                    } else {
                        merge_statistics.num_skipped_duplicates += 1;
                    }
                }
                drop(processed_insert_keys);

                if keep_indices.len() != not_matched.num_rows() {
                    not_matched =
                        take_record_batch(&not_matched, &UInt32Array::from(keep_indices))?;
                }
            }
            let left_cols_with_id = left_cols
                .into_iter()
                .chain(row_addr_col)
                .collect::<Vec<_>>();
            let not_matched = not_matched.project(&left_cols_with_id)?;
            // See comment above explaining this schema replacement
            let not_matched = RecordBatch::try_new(
                self.output_schema.clone(),
                Vec::from_iter(not_matched.columns().iter().cloned()),
            )?;

            merge_statistics.num_inserted_rows += not_matched.num_rows() as u64;
            batches.push(Ok(not_matched));
        }
        match self.params.delete_not_matched_by_source {
            WhenNotMatchedBySource::Delete => {
                let unmatched = arrow::compute::filter(batch.column(row_id_col), &right_only)?;
                merge_statistics.num_deleted_rows += unmatched.len() as u64;
                let row_ids = unmatched.as_primitive::<UInt64Type>();
                deleted_row_ids.extend(row_ids.values());
            }
            WhenNotMatchedBySource::DeleteIf(_) => {
                let target_data = batch.project(&right_cols_with_id)?;
                let unmatched = arrow::compute::filter_record_batch(&target_data, &right_only)?;
                let row_id_col = unmatched.num_columns() - 1;
                let to_delete = self.delete_expr.unwrap().evaluate(&unmatched)?;

                match to_delete {
                    ColumnarValue::Array(mask) => {
                        let row_ids = arrow::compute::filter(
                            unmatched.column(row_id_col),
                            mask.as_boolean(),
                        )?;
                        let row_ids = row_ids.as_primitive::<UInt64Type>();
                        merge_statistics.num_deleted_rows += row_ids.len() as u64;
                        deleted_row_ids.extend(row_ids.values());
                    }
                    ColumnarValue::Scalar(scalar) => {
                        if let ScalarValue::Boolean(Some(true)) = scalar {
                            let row_ids = unmatched.column(row_id_col).as_primitive::<UInt64Type>();
                            merge_statistics.num_deleted_rows += row_ids.len() as u64;
                            deleted_row_ids.extend(row_ids.values());
                        }
                    }
                }
            }
            WhenNotMatchedBySource::Keep => {}
        }

        Ok(stream::iter(batches))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dataset::scanner::ColumnOrdering;
    use crate::dataset::write::merge_insert::inserted_rows::{
        KeyExistenceFilter, KeyExistenceFilterBuilder, extract_key_value_from_batch,
    };
    use crate::index::vector::VectorIndexParams;
    use crate::io::commit::read_transaction_file;
    use crate::{
        dataset::{InsertBuilder, ReadParams, WriteMode, WriteParams, builder::DatasetBuilder},
        session::Session,
        utils::test::{
            DatagenExt, FragmentCount, FragmentRowCount, ThrottledStoreWrapper,
            assert_plan_node_equals, assert_string_matches,
        },
    };
    use arrow_array::builder::{ListBuilder, StringBuilder};
    use arrow_array::types::Float32Type;
    use arrow_array::{
        Array, FixedSizeListArray, Float32Array, Float64Array, Int32Array, Int64Array, ListArray,
        NullArray, RecordBatchIterator, RecordBatchReader, StringArray, StructArray, UInt32Array,
        types::{Int32Type, UInt32Type},
    };
    use arrow_array::{RecordBatch, record_batch};
    use arrow_buffer::{OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Schema};
    use arrow_select::concat::concat_batches;
    use datafusion::common::Column;
    use datafusion_physical_plan::stream::RecordBatchStreamAdapter;
    use futures::{FutureExt, StreamExt, TryStreamExt, future::try_join_all};
    use lance_arrow::FixedSizeListArrayExt;
    use lance_core::utils::tempfile::TempStrDir;
    use lance_datafusion::{datagen::DatafusionDatagenExt, utils::reader_to_stream};
    use lance_datagen::{BatchCount, Dimension, RowCount, Seed, array};
    use lance_index::IndexType;
    use lance_index::scalar::{FullTextSearchQuery, InvertedIndexParams, ScalarIndexParams};
    use lance_io::object_store::ObjectStoreParams;
    use lance_linalg::distance::MetricType;
    use mock_instant::thread_local::MockClock;
    use object_store::throttle::ThrottleConfig;
    use roaring::RoaringBitmap;
    use std::collections::HashMap;
    use tokio::sync::{Barrier, Notify};

    // Used to validate that futures returned are Send.
    fn assert_send<T: Send>(t: T) -> T {
        t
    }

    #[test]
    fn test_inserted_key_tracker_preserves_logical_nulls() {
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("id", DataType::Null, true)])),
            vec![Arc::new(NullArray::new(2))],
        )
        .unwrap();
        let mut tracker = InsertedKeyTracker::default();
        let on_columns = ["id".to_string()];

        assert!(tracker.insert(&batch, 0, &on_columns).unwrap());
        assert!(tracker.insert(&batch, 1, &on_columns).unwrap());
    }

    #[test]
    fn test_updated_row_addr_missing_between_target_rows() {
        let row_addr = |offset| u64::from(RowAddress::new_from_parts(3, offset));
        let mut updated_rows = UpdatedRowAddrReconciler::new([(row_addr(1), (1, 0))].into_iter());

        let error = updated_rows
            .reconcile_batch(&[row_addr(0), row_addr(2)])
            .unwrap_err();

        assert!(matches!(error, Error::Internal { .. }));
        let message = error.to_string();
        assert!(message.contains("update row address (3, 1) is missing"));
        assert!(message.contains("next target row address is (3, 2)"));
    }

    #[test]
    fn test_updated_row_addr_missing_after_target_rows() {
        let row_addr = |offset| u64::from(RowAddress::new_from_parts(7, offset));
        let mut updated_rows = UpdatedRowAddrReconciler::new([(row_addr(2), (1, 0))].into_iter());

        assert_eq!(
            updated_rows
                .reconcile_batch(&[row_addr(0), row_addr(1)])
                .unwrap(),
            vec![(0, 0), (0, 1)]
        );
        let error = updated_rows.finish().unwrap_err();

        assert!(matches!(error, Error::Internal { .. }));
        let message = error.to_string();
        assert!(message.contains("update row address (7, 2) is missing"));
        assert!(message.contains("no target rows remain"));
    }

    #[tokio::test]
    async fn test_updated_row_addr_missing_in_full_fragment_update() {
        let initial = record_batch!(("value", Int32, [10, 20])).unwrap();
        let dataset = Arc::new(
            InsertBuilder::new("memory://")
                .execute(vec![initial])
                .await
                .unwrap(),
        );
        let fragment_id = dataset.get_fragments()[0].id() as u32;
        let row_addr = |offset| u64::from(RowAddress::new_from_parts(fragment_id, offset));
        let updates = record_batch!(
            (ROW_ADDR, UInt64, [row_addr(0), row_addr(2)]),
            ("value", Int32, [100, 200])
        )
        .unwrap();
        let update_stream =
            RecordBatchStreamAdapter::new(updates.schema(), futures::stream::iter([Ok(updates)]));

        let error = MergeInsertJob::update_fragments(
            dataset.clone(),
            Box::pin(update_stream),
            dataset.manifest().version + 1,
            None,
        )
        .await
        .unwrap_err();

        assert!(matches!(error, Error::Internal { .. }));
        let message = error.to_string();
        assert!(message.contains("update row address (0, 2) is missing"));
        assert!(message.contains("no target rows remain"));
    }

    // An update-style merge_insert leaves the source and new fragments with
    // overlapping id ranges; a scattered delete punches holes in that range. A
    // filtered `with_row_id` scan must still resolve every id (round-tripped via take).
    #[tokio::test(flavor = "multi_thread")]
    async fn merge_insert_then_delete_resolves_overlapping_row_ids() {
        use arrow_array::ArrayRef;
        use arrow_array::types::UInt64Type;
        let dir = TempStrDir::default();
        let uri = dir.as_str();
        let schema = Arc::new(Schema::new(vec![
            Field::new("slug", DataType::Utf8, false),
            Field::new("title", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
        ]));
        let mk = |slugs: Vec<String>, titles: Vec<String>| {
            let cats: Vec<String> = (0..slugs.len())
                .map(|i| ["A", "B", "C", "D", "E"][i % 5].to_string())
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(slugs)) as ArrayRef,
                    Arc::new(StringArray::from(titles)) as ArrayRef,
                    Arc::new(StringArray::from(cats)) as ArrayRef,
                ],
            )
            .unwrap()
        };

        // Empty dataset with stable row ids; write-seed 40 rows (ids 0..40).
        let params = WriteParams {
            mode: WriteMode::Create,
            enable_stable_row_ids: true,
            ..Default::default()
        };
        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(mk(vec![], vec![]))], schema.clone()),
            uri,
            Some(params),
        )
        .await
        .unwrap();
        ds.append(
            RecordBatchIterator::new(
                vec![Ok(mk(
                    (1..=40).map(|i| format!("t{i}")).collect(),
                    (1..=40).map(|i| format!("r{i}")).collect(),
                ))],
                schema.clone(),
            ),
            None,
        )
        .await
        .unwrap();

        // Update every other row (so the new fragment's ids interleave with -- and
        // its range overlaps -- the source's) plus a few inserts.
        let mut slugs: Vec<String> = (1..=40).step_by(2).map(|i| format!("t{i}")).collect();
        let mut titles: Vec<String> = (1..=40).step_by(2).map(|i| format!("e{i}")).collect();
        for i in 41..=45 {
            slugs.push(format!("t{i}"));
            titles.push(format!("e{i}"));
        }
        let mut b = MergeInsertBuilder::try_new(Arc::new(ds), vec!["slug".into()]).unwrap();
        b.when_matched(WhenMatched::UpdateAll);
        b.when_not_matched(WhenNotMatched::InsertAll);
        let (ds, _) = b
            .try_build()
            .unwrap()
            .execute_reader(RecordBatchIterator::new(
                vec![Ok(mk(slugs, titles))],
                schema.clone(),
            ))
            .await
            .unwrap();

        // Scattered delete -> interior holes in the overlapped id range.
        let mut ds = (*ds).clone();
        let ds = (*ds.delete("category = 'A'").await.unwrap().new_dataset).clone();

        // The `with_row_id` scan builds the RowIdIndex; every scanned id must
        // round-trip through take_rows.
        let batches: Vec<RecordBatch> = ds
            .scan()
            .with_row_id()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let row_ids: Vec<u64> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name(ROW_ID)
                    .unwrap()
                    .as_primitive::<UInt64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        let scanned_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        let taken = ds.take_rows(&row_ids, ds.schema().clone()).await.unwrap();
        assert_eq!(taken.num_rows(), scanned_rows);

        // A point lookup on a surviving row resolves to exactly one row.
        let filtered: Vec<RecordBatch> = ds
            .scan()
            .with_row_id()
            .filter("slug = 't30'")
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect()
            .await
            .unwrap();
        let n: usize = filtered.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 1, "expected exactly one row for slug='t30'");
    }

    async fn check_then_refresh_dataset(
        new_data: RecordBatch,
        mut job: MergeInsertJob,
        keys_from_left: &[u32],
        keys_from_right: &[u32],
        stats: &[u64],
    ) -> Arc<Dataset> {
        let mut dataset = (*job.dataset).clone();
        dataset.restore().await.unwrap();
        job.dataset = Arc::new(dataset);

        let schema = new_data.schema();
        let new_reader = Box::new(RecordBatchIterator::new([Ok(new_data)], schema.clone()));
        let new_stream = reader_to_stream(new_reader);

        let (merged_dataset, merge_stats) = job.execute(new_stream).boxed().await.unwrap();

        let batches = merged_dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let merged = concat_batches(&schema, &batches).unwrap();

        let keyvals = merged
            .column(0)
            .as_primitive::<UInt32Type>()
            .values()
            .iter()
            .zip(
                merged
                    .column(1)
                    .as_primitive::<UInt32Type>()
                    .values()
                    .iter(),
            );
        let mut left_keys = keyvals
            .clone()
            .filter(|&(_, &val)| val == 1)
            .map(|(key, _)| key)
            .copied()
            .collect::<Vec<_>>();
        let mut right_keys = keyvals
            .clone()
            .filter(|&(_, &val)| val == 2)
            .map(|(key, _)| key)
            .copied()
            .collect::<Vec<_>>();
        left_keys.sort();
        right_keys.sort();
        assert_eq!(left_keys, keys_from_left);
        assert_eq!(right_keys, keys_from_right);
        assert_eq!(merge_stats.num_inserted_rows, stats[0]);
        assert_eq!(merge_stats.num_updated_rows, stats[1]);
        assert_eq!(merge_stats.num_deleted_rows, stats[2]);

        merged_dataset
    }

    fn create_test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt32, true),
            Field::new("value", DataType::UInt32, true),
            Field::new("filterme", DataType::Utf8, true),
        ]))
    }

    fn create_new_batch(schema: Arc<Schema>) -> RecordBatch {
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(vec![4, 5, 6, 7, 8, 9])),
                Arc::new(UInt32Array::from(vec![2, 2, 2, 2, 2, 2])),
                Arc::new(StringArray::from(vec!["A", "B", "C", "A", "B", "C"])),
            ],
        )
        .unwrap()
    }

    async fn create_test_dataset(
        test_uri: &str,
        version: LanceFileVersion,
        enable_stable_row_ids: bool,
    ) -> Arc<Dataset> {
        let dataset = lance_datagen::gen_batch()
            .col("key", array::step_custom::<UInt32Type>(1, 1))
            .col("value", array::fill::<UInt32Type>(1u32))
            .col(
                "filterme",
                array::cycle_utf8_literals(&["A", "B", "A", "A", "B", "A"]),
            )
            .into_dataset_with_params(
                test_uri,
                FragmentCount(2),
                FragmentRowCount(3),
                Some(WriteParams {
                    max_rows_per_file: 3,
                    data_storage_version: Some(version),
                    enable_stable_row_ids,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        assert_eq!(2, dataset.get_fragments().len());

        Arc::new(dataset)
    }

    async fn get_row_ids_for_keys(dataset: &Dataset, keys: &[u32]) -> UInt64Array {
        let filter = format!(
            "key IN ({})",
            keys.iter()
                .map(|k| k.to_string())
                .collect::<Vec<_>>()
                .join(",")
        );

        let batch = dataset
            .scan()
            .filter(&filter)
            .unwrap()
            .with_row_id()
            .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
                "key".to_string(),
            )]))
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .clone()
    }

    fn create_delete_condition() -> Expr {
        Expr::gt(
            Expr::Column(Column::new_unqualified("key")),
            Expr::Literal(ScalarValue::UInt32(Some(1)), None),
        )
    }

    struct MergeInsertTestBuilder {
        version: LanceFileVersion,
        enable_stable_row_ids: bool,
        test_keys: Vec<u32>,
        expected_left_keys: Vec<u32>,
        expected_right_keys: Vec<u32>,
        expected_stats: Vec<u64>,
        job_builder: Option<Box<dyn FnOnce(Arc<Dataset>) -> MergeInsertJob>>,
    }

    impl MergeInsertTestBuilder {
        fn new() -> Self {
            Self {
                version: LanceFileVersion::default(),
                enable_stable_row_ids: false,
                test_keys: vec![],
                expected_left_keys: vec![],
                expected_right_keys: vec![],
                expected_stats: vec![],
                job_builder: None,
            }
        }

        fn with_version(mut self, version: LanceFileVersion) -> Self {
            self.version = version;
            self
        }

        fn with_stable_row_ids(mut self, enable: bool) -> Self {
            self.enable_stable_row_ids = enable;
            self
        }

        fn with_test_keys(mut self, keys: &[u32]) -> Self {
            self.test_keys = keys.to_vec();
            self
        }

        fn with_expected_left_keys(mut self, keys: &[u32]) -> Self {
            self.expected_left_keys = keys.to_vec();
            self
        }

        fn with_expected_right_keys(mut self, keys: &[u32]) -> Self {
            self.expected_right_keys = keys.to_vec();
            self
        }

        fn with_expected_stats(mut self, stats: &[u64]) -> Self {
            self.expected_stats = stats.to_vec();
            self
        }

        fn with_job_builder<F>(mut self, builder: F) -> Self
        where
            F: FnOnce(Arc<Dataset>) -> MergeInsertJob + 'static,
        {
            self.job_builder = Some(Box::new(builder));
            self
        }

        async fn run_test(self) {
            let schema = create_test_schema();
            let new_batch = create_new_batch(schema.clone());
            let test_uri = "memory://test.lance";

            let ds = create_test_dataset(test_uri, self.version, self.enable_stable_row_ids).await;
            let row_ids_before = get_row_ids_for_keys(&ds, &self.test_keys).await;

            let job_builder = self.job_builder.expect("job_builder must be set");
            let job = job_builder(ds);
            let ds = check_then_refresh_dataset(
                new_batch,
                job,
                &self.expected_left_keys,
                &self.expected_right_keys,
                &self.expected_stats,
            )
            .await;

            let row_ids_after = get_row_ids_for_keys(&ds, &self.test_keys).await;

            if self.enable_stable_row_ids {
                assert_eq!(row_ids_before, row_ids_after);
            } else {
                assert_ne!(row_ids_before, row_ids_after);
            }
        }
    }

    #[tokio::test]
    async fn test_merge_insert_requires_on_or_primary_key() {
        let test_uri = "memory://merge_insert_requires_keys";

        let ds = create_test_dataset(test_uri, LanceFileVersion::V2_0, false).await;

        let err = MergeInsertBuilder::try_new(ds, Vec::new()).unwrap_err();
        if let crate::Error::InvalidInput { source, .. } = err {
            let msg = source.to_string();
            assert!(
                msg.contains("requires join keys") && msg.contains("primary key"),
                "unexpected error message: {}",
                msg
            );
        } else {
            panic!("expected InvalidInput error");
        }
    }

    #[tokio::test]
    async fn test_merge_insert_defaults_to_unenforced_primary_key() {
        // Define a simple schema with an unenforced primary key on `id`.
        let id_field = Field::new("id", DataType::Int32, false).with_metadata(
            [(
                "lance-schema:unenforced-primary-key".to_string(),
                "true".to_string(),
            )]
            .into(),
        );
        let value_field = Field::new("value", DataType::Int32, false);
        let schema = Arc::new(Schema::new(vec![id_field, value_field]));

        let initial_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let reader = RecordBatchIterator::new(vec![Ok(initial_batch)], schema.clone());
        let dataset = Dataset::write(
            reader,
            "memory://merge_insert_pk_default",
            Some(WriteParams {
                data_storage_version: Some(LanceFileVersion::V2_0),
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let dataset = Arc::new(dataset);

        // New data: update ids 2 and 3, insert id 4.
        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 3, 4])),
                Arc::new(Int32Array::from(vec![200, 300, 400])),
            ],
        )
        .unwrap();

        let mut builder = MergeInsertBuilder::try_new(dataset.clone(), Vec::new()).unwrap();
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll);
        let job = builder.try_build().unwrap();

        let new_reader = Box::new(RecordBatchIterator::new([Ok(new_batch)], schema.clone()));
        let new_stream = reader_to_stream(new_reader);

        let (updated_dataset, stats) = job.execute(new_stream).await.unwrap();

        assert_eq!(stats.num_inserted_rows, 1);
        assert_eq!(stats.num_updated_rows, 2);
        assert_eq!(stats.num_deleted_rows, 0);

        let result_batch = updated_dataset.scan().try_into_batch().await.unwrap();
        let ids = result_batch
            .column_by_name("id")
            .unwrap()
            .as_primitive::<Int32Type>();
        let values = result_batch
            .column_by_name("value")
            .unwrap()
            .as_primitive::<Int32Type>();

        let mut pairs = (0..ids.len())
            .map(|i| (ids.value(i), values.value(i)))
            .collect::<Vec<_>>();
        pairs.sort_unstable();

        assert_eq!(pairs, vec![(1, 10), (2, 200), (3, 300), (4, 400)]);
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_basic_merge(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
    ) {
        let schema = create_test_schema();
        let new_batch = create_new_batch(schema.clone());

        let test_uri = "memory://test.lance";

        let ds = create_test_dataset(test_uri, version, false).await;

        // Quick test that no on-keys is not valid and fails
        assert!(MergeInsertBuilder::try_new(ds.clone(), vec![]).is_err());

        let keys = vec!["key".to_string()];
        // find-or-create, no delete
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .try_build()
            .unwrap();
        check_then_refresh_dataset(
            new_batch.clone(),
            job,
            &[1, 2, 3, 4, 5, 6],
            &[7, 8, 9],
            &[3, 0, 0],
        )
        .await;

        // upsert, no delete
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .try_build()
            .unwrap();
        check_then_refresh_dataset(
            new_batch.clone(),
            job,
            &[1, 2, 3],
            &[4, 5, 6, 7, 8, 9],
            &[3, 3, 0],
        )
        .await;

        // conditional upsert, no delete
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(
                WhenMatched::update_if(&ds, "source.filterme != target.filterme").unwrap(),
            )
            .try_build()
            .unwrap();
        check_then_refresh_dataset(
            new_batch.clone(),
            job,
            &[1, 2, 3, 4, 5],
            &[6, 7, 8, 9],
            &[3, 1, 0],
        )
        .await;

        // conditional update, no matches
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_not_matched(WhenNotMatched::DoNothing)
            .when_matched(WhenMatched::update_if(&ds, "target.filterme = 'z'").unwrap())
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[1, 2, 3, 4, 5, 6], &[], &[0, 0, 0])
            .await;

        // update only, no delete (useful for bulk update)
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[1, 2, 3], &[4, 5, 6], &[0, 3, 0])
            .await;

        // Conditional update
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(
                WhenMatched::update_if(&ds, "source.filterme == target.filterme").unwrap(),
            )
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[1, 2, 3, 6], &[4, 5], &[0, 2, 0])
            .await;

        // No-op (will raise an error)
        assert!(
            MergeInsertBuilder::try_new(ds.clone(), keys.clone())
                .unwrap()
                .when_not_matched(WhenNotMatched::DoNothing)
                .try_build()
                .is_err()
        );

        // find-or-create, with delete all
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[4, 5, 6], &[7, 8, 9], &[3, 0, 3])
            .await;

        // upsert, with delete all
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[], &[4, 5, 6, 7, 8, 9], &[3, 3, 3])
            .await;

        // conditional upsert, with delete all
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(
                WhenMatched::update_if(&ds, "source.filterme != target.filterme").unwrap(),
            )
            .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[4, 5], &[6, 7, 8, 9], &[3, 1, 3])
            .await;

        // update only, with delete all (unusual)
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[], &[4, 5, 6], &[0, 3, 3]).await;

        // just delete all (not real case, just use delete)
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_not_matched(WhenNotMatched::DoNothing)
            .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[4, 5, 6], &[], &[0, 0, 3]).await;

        // For the "delete some" tests we use key > 1
        let condition = create_delete_condition();
        // find-or-create, with delete some
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_not_matched_by_source(WhenNotMatchedBySource::DeleteIf(condition.clone()))
            .try_build()
            .unwrap();
        check_then_refresh_dataset(
            new_batch.clone(),
            job,
            &[1, 4, 5, 6],
            &[7, 8, 9],
            &[3, 0, 2],
        )
        .await;

        // upsert, with delete some
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched_by_source(WhenNotMatchedBySource::DeleteIf(condition.clone()))
            .try_build()
            .unwrap();
        check_then_refresh_dataset(
            new_batch.clone(),
            job,
            &[1],
            &[4, 5, 6, 7, 8, 9],
            &[3, 3, 2],
        )
        .await;

        // conditional upsert, with delete some
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(
                WhenMatched::update_if(&ds, "source.filterme != target.filterme").unwrap(),
            )
            .when_not_matched_by_source(WhenNotMatchedBySource::DeleteIf(condition.clone()))
            .try_build()
            .unwrap();
        check_then_refresh_dataset(
            new_batch.clone(),
            job,
            &[1, 4, 5],
            &[6, 7, 8, 9],
            &[3, 1, 2],
        )
        .await;

        // update only, witxh delete some (unusual)
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .when_not_matched_by_source(WhenNotMatchedBySource::DeleteIf(condition.clone()))
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[1], &[4, 5, 6], &[0, 3, 2]).await;

        // just delete some (not real case, just use delete)
        let job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_not_matched(WhenNotMatched::DoNothing)
            .when_not_matched_by_source(WhenNotMatchedBySource::DeleteIf(condition.clone()))
            .try_build()
            .unwrap();
        check_then_refresh_dataset(new_batch.clone(), job, &[1, 4, 5, 6], &[], &[0, 0, 2]).await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_upsert_and_delete_all_with_stable_row_id(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(true, false)] enable_stable_row_ids: bool,
    ) {
        MergeInsertTestBuilder::new()
            .with_version(version)
            .with_stable_row_ids(enable_stable_row_ids)
            .with_test_keys(&[4, 5, 6])
            .with_expected_left_keys(&[])
            .with_expected_right_keys(&[4, 5, 6, 7, 8, 9])
            .with_expected_stats(&[3, 3, 3])
            .with_job_builder(|ds| {
                MergeInsertBuilder::try_new(ds, vec!["key".to_string()])
                    .unwrap()
                    .when_matched(WhenMatched::UpdateAll)
                    .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
                    .try_build()
                    .unwrap()
            })
            .run_test()
            .await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_upsert_only_with_stable_row_id(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(true, false)] enable_stable_row_ids: bool,
    ) {
        MergeInsertTestBuilder::new()
            .with_version(version)
            .with_stable_row_ids(enable_stable_row_ids)
            .with_test_keys(&[4, 5, 6])
            .with_expected_left_keys(&[1, 2, 3])
            .with_expected_right_keys(&[4, 5, 6, 7, 8, 9])
            .with_expected_stats(&[3, 3, 0])
            .with_job_builder(|ds| {
                MergeInsertBuilder::try_new(ds, vec!["key".to_string()])
                    .unwrap()
                    .when_matched(WhenMatched::UpdateAll)
                    .try_build()
                    .unwrap()
            })
            .run_test()
            .await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_conditional_update_with_stable_row_id(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(true, false)] enable_stable_row_ids: bool,
    ) {
        MergeInsertTestBuilder::new()
            .with_version(version)
            .with_stable_row_ids(enable_stable_row_ids)
            .with_test_keys(&[6])
            .with_expected_left_keys(&[1, 2, 3, 4, 5])
            .with_expected_right_keys(&[6, 7, 8, 9])
            .with_expected_stats(&[3, 1, 0])
            .with_job_builder(|ds| {
                let keys = vec!["key".to_string()];
                MergeInsertBuilder::try_new(ds.clone(), keys)
                    .unwrap()
                    .when_matched(
                        WhenMatched::update_if(&ds, "source.filterme != target.filterme").unwrap(),
                    )
                    .try_build()
                    .unwrap()
            })
            .run_test()
            .await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_update_only_with_stable_row_id(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(true, false)] enable_stable_row_ids: bool,
    ) {
        MergeInsertTestBuilder::new()
            .with_version(version)
            .with_stable_row_ids(enable_stable_row_ids)
            .with_test_keys(&[4, 5, 6])
            .with_expected_left_keys(&[1, 2, 3])
            .with_expected_right_keys(&[4, 5, 6])
            .with_expected_stats(&[0, 3, 0])
            .with_job_builder(|ds| {
                let keys = vec!["key".to_string()];
                MergeInsertBuilder::try_new(ds, keys)
                    .unwrap()
                    .when_matched(WhenMatched::UpdateAll)
                    .when_not_matched(WhenNotMatched::DoNothing)
                    .try_build()
                    .unwrap()
            })
            .run_test()
            .await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_upsert_with_conditional_delete_and_stable_row_id(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(true, false)] enable_stable_row_ids: bool,
    ) {
        MergeInsertTestBuilder::new()
            .with_version(version)
            .with_stable_row_ids(enable_stable_row_ids)
            .with_test_keys(&[1, 4, 5, 6])
            .with_expected_left_keys(&[1])
            .with_expected_right_keys(&[4, 5, 6, 7, 8, 9])
            .with_expected_stats(&[3, 3, 2])
            .with_job_builder(|ds| {
                let keys = vec!["key".to_string()];
                let condition = create_delete_condition();
                MergeInsertBuilder::try_new(ds, keys)
                    .unwrap()
                    .when_matched(WhenMatched::UpdateAll)
                    .when_not_matched_by_source(WhenNotMatchedBySource::DeleteIf(condition))
                    .try_build()
                    .unwrap()
            })
            .run_test()
            .await;
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_multiple_merge_insert_stable_row_id(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(true, false)] enable_stable_row_ids: bool,
    ) {
        let schema = create_test_schema();
        let test_uri = "memory://test_multiple_merge.lance";

        let ds = create_test_dataset(test_uri, version, enable_stable_row_ids).await;

        let target_key = 2u32;
        let target_keys = vec![target_key];

        let initial_row_ids = get_row_ids_for_keys(&ds, &target_keys).await;
        let initial_row_id = initial_row_ids.value(0);

        let mut current_ds = ds;

        for iteration in 1..=3 {
            let new_value = 1000u32 + iteration * 10;
            let new_batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt32Array::from(vec![target_key])), // key
                    Arc::new(UInt32Array::from(vec![new_value])),  // value
                    Arc::new(StringArray::from(vec![format!("iteration_{}", iteration)])), // filterme
                ],
            )
            .unwrap();

            let job = MergeInsertBuilder::try_new(current_ds.clone(), vec!["key".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::DoNothing)
                .try_build()
                .unwrap();

            let new_reader = Box::new(RecordBatchIterator::new([Ok(new_batch)], schema.clone()));
            let new_stream = reader_to_stream(new_reader);
            let (updated_dataset, merge_stats) = job.execute(new_stream).await.unwrap();

            assert_eq!(
                merge_stats.num_updated_rows, 1,
                "Iteration {}: Expected 1 updated row",
                iteration
            );
            assert_eq!(
                merge_stats.num_inserted_rows, 0,
                "Iteration {}: Expected 0 inserted rows",
                iteration
            );
            assert_eq!(
                merge_stats.num_deleted_rows, 0,
                "Iteration {}: Expected 0 deleted rows",
                iteration
            );

            let updated_row_ids = get_row_ids_for_keys(&updated_dataset, &target_keys).await;
            let updated_row_id = updated_row_ids.value(0);

            let updated_batch = updated_dataset
                .scan()
                .filter(&format!("key = {}", target_key))
                .unwrap()
                .try_into_batch()
                .await
                .unwrap();

            let value_col = updated_batch
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let filterme_col = updated_batch
                .column_by_name("filterme")
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            assert_eq!(
                value_col.value(0),
                new_value,
                "Iteration {}: Value should be updated to {}",
                iteration,
                new_value
            );
            assert_eq!(filterme_col.value(0), format!("iteration_{}", iteration));

            if enable_stable_row_ids {
                assert_eq!(
                    updated_row_id, initial_row_id,
                    "Iteration {}: Row ID should remain stable across merge inserts when stable_row_ids is enabled. Initial: {}, Current: {}",
                    iteration, initial_row_id, updated_row_id
                );
            }

            current_ds = updated_dataset;
        }

        let final_batch = current_ds
            .scan()
            .filter(&format!("key = {}", target_key))
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        assert_eq!(
            final_batch.num_rows(),
            1,
            "Should have exactly one row for the target key"
        );

        let final_value = final_batch
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap()
            .value(0);
        let final_filterme = final_batch
            .column_by_name("filterme")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);

        assert_eq!(
            final_value, 1030u32,
            "Final value should be from last iteration"
        );
        assert_eq!(
            final_filterme, "iteration_3",
            "Final filterme should be from last iteration"
        );
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_multi_batch_upsert_preserves_stable_row_ids(
        #[values(true, false)] use_index: bool,
    ) {
        let mut dataset = (*create_test_dataset(
            "memory://test_multi_batch_upsert_row_ids",
            LanceFileVersion::default(),
            true,
        )
        .await)
            .clone();
        dataset
            .create_index(
                &["key"],
                IndexType::Scalar,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        let initial_keys = [1, 2, 3, 4, 5, 6];
        let initial_row_ids = get_row_ids_for_keys(&dataset, &initial_keys).await;
        let initial_row_ids = initial_row_ids
            .values()
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let updated_keys = [2, 4];
        let updated_row_ids_before = get_row_ids_for_keys(&dataset, &updated_keys).await;

        // Put inserts in the first source batch and updates in the second. Stable
        // row-id assignment still relies on the join emitting all updates first.
        let insert_batch = record_batch!(
            ("key", UInt32, [7, 8]),
            ("value", UInt32, [70, 80]),
            ("filterme", Utf8, ["inserted", "inserted"])
        )
        .unwrap();
        let update_batch = record_batch!(
            ("key", UInt32, [2, 4]),
            ("value", UInt32, [20, 40]),
            ("filterme", Utf8, ["updated", "updated"])
        )
        .unwrap();
        let source_schema = insert_batch.schema();
        let source = Box::new(RecordBatchIterator::new(
            [Ok(insert_batch), Ok(update_batch)],
            source_schema,
        ));

        let (dataset, stats) = MergeInsertBuilder::try_new(dataset, vec!["key".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .use_index(use_index)
            .try_build()
            .unwrap()
            .execute_reader(source)
            .await
            .unwrap();

        assert_eq!(stats.num_updated_rows, 2);
        assert_eq!(stats.num_inserted_rows, 2);
        let updated_row_ids_after = get_row_ids_for_keys(&dataset, &updated_keys).await;
        assert_eq!(updated_row_ids_after, updated_row_ids_before);

        let inserted_row_ids = get_row_ids_for_keys(&dataset, &[7, 8]).await;
        assert!(
            inserted_row_ids
                .values()
                .iter()
                .all(|row_id| !initial_row_ids.contains(row_id))
        );
    }

    #[rstest::rstest]
    #[tokio::test]
    async fn test_row_id_stability_across_update_and_merge_insert(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(true, false)] enable_stable_row_ids: bool,
    ) {
        let schema = create_test_schema();
        let test_uri = "memory://test_row_id_stability.lance";

        let mut dataset = create_test_dataset(test_uri, version, enable_stable_row_ids).await;

        let target_key = 2u32;
        let target_keys = vec![target_key];

        let initial_row_ids = get_row_ids_for_keys(&dataset, &target_keys).await;
        let initial_row_id = initial_row_ids.value(0);

        let initial_batch = dataset
            .scan()
            .filter(&format!("key = {}", target_key))
            .unwrap()
            .with_row_id()
            .try_into_batch()
            .await
            .unwrap();

        let initial_value = initial_batch
            .column_by_name("value")
            .unwrap()
            .as_primitive::<UInt32Type>()
            .value(0);

        let update_result = crate::dataset::UpdateBuilder::new(Arc::new((*dataset).clone()))
            .update_where(&format!("key = {}", target_key))
            .unwrap()
            .set("value", "value + 100")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();

        dataset = update_result.new_dataset.clone();

        let after_update_row_ids = get_row_ids_for_keys(&dataset, &target_keys).await;
        let after_update_row_id = after_update_row_ids.value(0);

        let after_update_batch = dataset
            .scan()
            .filter(&format!("key = {}", target_key))
            .unwrap()
            .with_row_id()
            .try_into_batch()
            .await
            .unwrap();

        let after_update_value = after_update_batch
            .column_by_name("value")
            .unwrap()
            .as_primitive::<UInt32Type>()
            .value(0);

        if enable_stable_row_ids {
            assert_eq!(
                initial_row_id, after_update_row_id,
                "Row ID should remain stable after update"
            );
        } else {
            assert_ne!(
                initial_row_id, after_update_row_id,
                "Row ID should change after update when stable row IDs are disabled"
            );
        }
        assert_eq!(
            after_update_value,
            initial_value + 100,
            "Value should be updated correctly"
        );

        let merge_new_value = 500u32;
        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![target_key])),
                Arc::new(UInt32Array::from(vec![merge_new_value])),
                Arc::new(StringArray::from(vec!["UPDATED"])),
            ],
        )
        .unwrap();

        let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["key".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .try_build()
            .unwrap();

        let new_reader = Box::new(RecordBatchIterator::new([Ok(new_batch)], schema.clone()));
        let new_stream = reader_to_stream(new_reader);

        let (merged_dataset, merge_stats) = job.execute(new_stream).await.unwrap();

        let after_merge_row_ids = get_row_ids_for_keys(&merged_dataset, &target_keys).await;
        let after_merge_row_id = after_merge_row_ids.value(0);

        let after_merge_batch = merged_dataset
            .scan()
            .filter(&format!("key = {}", target_key))
            .unwrap()
            .with_row_id()
            .try_into_batch()
            .await
            .unwrap();

        let after_merge_value = after_merge_batch
            .column_by_name("value")
            .unwrap()
            .as_primitive::<UInt32Type>()
            .value(0);

        let after_merge_filterme = after_merge_batch
            .column_by_name("filterme")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);

        if enable_stable_row_ids {
            assert_eq!(
                initial_row_id, after_merge_row_id,
                "Row ID should remain stable after merge insert"
            );
            assert_eq!(
                after_update_row_id, after_merge_row_id,
                "Row ID should remain the same across update and merge insert"
            );
        } else {
            assert_ne!(
                after_update_row_id, after_merge_row_id,
                "Row ID should change after merge insert when stable row IDs are disabled"
            );
        }

        assert_eq!(
            after_merge_value, merge_new_value,
            "Value should be updated by merge insert"
        );
        assert_eq!(
            after_merge_filterme, "UPDATED",
            "Filterme should be updated by merge insert"
        );

        assert_eq!(
            merge_stats.num_updated_rows, 1,
            "Should update exactly 1 row"
        );
        assert_eq!(
            merge_stats.num_inserted_rows, 0,
            "Should not insert any new rows"
        );
        assert_eq!(
            merge_stats.num_deleted_rows, 0,
            "Should not delete any rows"
        );

        if enable_stable_row_ids {
            assert_eq!(
                initial_row_id, after_merge_row_id,
                "Row ID should remain stable throughout the entire process of update and merge insert"
            );
        }
    }

    /// Reproduces https://github.com/lancedb/lancedb/issues/3515:
    /// a partial-schema `merge_insert` with a scalar index on the join key,
    /// where every fragment is covered by the index (no unindexed data),
    /// silently updates 0 rows instead of the expected matches.
    #[rstest::rstest]
    #[tokio::test]
    async fn test_repro_3515_partial_schema_fully_indexed(
        #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1, LanceFileVersion::V2_2)]
        version: LanceFileVersion,
    ) {
        const N: usize = 1000;
        const UPD: usize = 128;
        let vec_field = Field::new(
            "vector",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
            true,
        );
        let full_schema = Arc::new(Schema::new(vec![
            vec_field.clone(),
            Field::new("path", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
            Field::new("file_size", DataType::Int64, true),
        ]));

        // 1000 rows: vector all-null, path "/img/{i}.jpg", status "pending".
        let paths = StringArray::from((0..N).map(|i| format!("/img/{i}.jpg")).collect::<Vec<_>>());
        let statuses = StringArray::from(vec!["pending"; N]);
        let file_sizes = Int64Array::from((0..N as i64).map(|i| 1000 + i).collect::<Vec<_>>());
        let null_vectors = arrow_array::new_null_array(vec_field.data_type(), N);
        let batch = RecordBatch::try_new(
            full_schema.clone(),
            vec![
                null_vectors,
                Arc::new(paths),
                Arc::new(statuses),
                Arc::new(file_sizes),
            ],
        )
        .unwrap();

        let mut ds = Dataset::write(
            RecordBatchIterator::new([Ok(batch)], full_schema.clone()),
            "memory://",
            Some(WriteParams {
                data_storage_version: Some(version),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Scalar index on the merge key, covering every fragment.
        ds.create_index(
            &["path"],
            IndexType::Scalar,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();
        let ds = Arc::new(ds);

        // Partial-schema source (no `file_size`): update the first 128 rows.
        let upd_schema = Arc::new(Schema::new(vec![
            vec_field,
            Field::new("path", DataType::Utf8, false),
            Field::new("status", DataType::Utf8, true),
        ]));
        let upd_paths = StringArray::from(
            (0..UPD)
                .map(|i| format!("/img/{i}.jpg"))
                .collect::<Vec<_>>(),
        );
        let upd_vectors =
            FixedSizeListArray::try_new_from_values(Float32Array::from(vec![0.1f32; 4 * UPD]), 4)
                .unwrap();
        let upd_statuses = StringArray::from(vec!["indexed"; UPD]);
        let updates = RecordBatch::try_new(
            upd_schema.clone(),
            vec![
                Arc::new(upd_vectors),
                Arc::new(upd_paths),
                Arc::new(upd_statuses),
            ],
        )
        .unwrap();

        let (ds, stats) = MergeInsertBuilder::try_new(ds.clone(), vec!["path".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap()
            .execute_reader(RecordBatchIterator::new([Ok(updates)], upd_schema))
            .await
            .unwrap();

        assert_eq!(
            stats.num_updated_rows, UPD as u64,
            "expected {UPD} updated rows on {version:?}, got {}",
            stats.num_updated_rows
        );
        let n_indexed = ds
            .count_rows(Some("status = 'indexed'".to_string()))
            .await
            .unwrap();
        assert_eq!(n_indexed, UPD, "expected {UPD} rows flipped to 'indexed'");
    }

    #[tokio::test]
    async fn test_indexed_merge_insert() {
        let test_dir = TempStrDir::default();
        let test_uri = &test_dir;

        let data = lance_datagen::gen_batch()
            .with_seed(Seed::from(1))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let data = data.into_reader_rows(RowCount::from(1024), BatchCount::from(32));
        let schema = data.schema();

        // Create an input dataset with a scalar index on key
        let mut ds = Dataset::write(data, test_uri, None).await.unwrap();
        let index_params = ScalarIndexParams::default();
        ds.create_index(&["key"], IndexType::Scalar, None, &index_params, false)
            .await
            .unwrap();

        // Create some new (unindexed) data
        let data = lance_datagen::gen_batch()
            .with_seed(Seed::from(2))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let data = data.into_reader_rows(RowCount::from(1024), BatchCount::from(8));
        let ds = Dataset::write(
            data,
            test_uri,
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let ds = Arc::new(ds);

        let just_index_col = Schema::new(vec![Field::new("key", DataType::Utf8, false)]);

        // Sample 2048 random indices and then paste on a column of 9999999's
        let some_indices = ds
            .sample(2048, &(&just_index_col).try_into().unwrap(), None)
            .await
            .unwrap();
        let some_indices = some_indices.column(0).clone();
        let some_vals = lance_datagen::gen_batch()
            .anon_col(array::fill::<UInt32Type>(9999999))
            .into_batch_rows(RowCount::from(2048))
            .unwrap();
        let some_vals = some_vals.column(0).clone();
        let source_batch =
            RecordBatch::try_new(schema.clone(), vec![some_vals, some_indices]).unwrap();
        // To make things more interesting, lets make the input a stream of four batches
        let source_batches = vec![
            source_batch.slice(0, 512),
            source_batch.slice(512, 512),
            source_batch.slice(1024, 512),
            source_batch.slice(1536, 512),
        ];
        let source = Box::new(RecordBatchIterator::new(
            source_batches.clone().into_iter().map(Ok),
            schema.clone(),
        ));

        // Run merge_insert
        let (ds, _) = MergeInsertBuilder::try_new(ds.clone(), vec!["key".to_string()])
            .unwrap()
            .when_not_matched(WhenNotMatched::DoNothing)
            .when_matched(WhenMatched::UpdateAll)
            .try_build()
            .unwrap()
            .execute_reader(source)
            .await
            .unwrap();

        // Check that the data is as expected
        let updated = ds
            .count_rows(Some("value = 9999999".to_string()))
            .await
            .unwrap();
        assert_eq!(updated, 2048);

        // Make sure we don't use an indexed scan if there is a delete criteria
        let source = Box::new(RecordBatchIterator::new(
            source_batches.clone().into_iter().map(Ok),
            schema.clone(),
        ));
        // Run merge_insert
        let (ds, _) = MergeInsertBuilder::try_new(ds.clone(), vec!["key".to_string()])
            .unwrap()
            .when_not_matched(WhenNotMatched::DoNothing)
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
            .try_build()
            .unwrap()
            .execute_reader(source)
            .await
            .unwrap();

        // Check that the data is as expected
        assert_eq!(ds.count_rows(None).await.unwrap(), 2048);

        let source = Box::new(RecordBatchIterator::new(
            source_batches.clone().into_iter().map(Ok),
            schema.clone(),
        ));
        // Run merge_insert one last time.  The index is now completely out of date.  Every
        // row it points to is a deleted row.  Make sure that doesn't break.
        let (ds, _) = MergeInsertBuilder::try_new(ds.clone(), vec!["key".to_string()])
            .unwrap()
            .when_not_matched(WhenNotMatched::DoNothing)
            .when_matched(WhenMatched::UpdateAll)
            .try_build()
            .unwrap()
            .execute_reader(source)
            .await
            .unwrap();

        assert_eq!(ds.count_rows(None).await.unwrap(), 2048);
    }

    /// Multi-column (composite key) merge_insert when one or more join
    /// columns have a scalar index.  Before this change the indexed path
    /// was hard-gated to single-column joins; composite-key merges fell
    /// through to a full target scan even when every key column was
    /// indexed.  Now each indexed column contributes an AND-ed `IsIn`
    /// probe inside one `MapIndexExec`, and the downstream hash join trims
    /// the candidates to the exact composite-key matches — rows that
    /// happen to match one key column but not the other must NOT be
    /// touched.
    #[rstest::rstest]
    #[case::index_on_first(true, false)]
    #[case::index_on_second(false, true)]
    #[case::index_on_both(true, true)]
    #[tokio::test]
    async fn test_indexed_merge_insert_composite_key(
        #[case] index_on_a: bool,
        #[case] index_on_b: bool,
    ) {
        // Target rows: every (a, b) combination from {1,2} x {10,20}.
        let initial = record_batch!(
            ("a", Int32, [1, 1, 2, 2]),
            ("b", Int32, [10, 20, 10, 20]),
            ("value", Int32, [100, 200, 300, 400])
        )
        .unwrap();
        let schema = initial.schema();

        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        let params = ScalarIndexParams::default();
        if index_on_a {
            ds.create_index(&["a"], IndexType::Scalar, None, &params, false)
                .await
                .unwrap();
        }
        if index_on_b {
            ds.create_index(&["b"], IndexType::Scalar, None, &params, false)
                .await
                .unwrap();
        }

        // Update (1, 10) and insert (3, 30).  A naive single-column probe
        // on `a` would also pull (1, 20) into the candidate set; the
        // composite hash join must keep (1, 20) untouched.
        let source = record_batch!(
            ("a", Int32, [1, 3]),
            ("b", Int32, [10, 30]),
            ("value", Int32, [999, 333])
        )
        .unwrap();

        let (updated_ds, stats) =
            MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_updated_rows, 1);
        assert_eq!(stats.num_inserted_rows, 1);
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 5);

        let untouched = updated_ds
            .count_rows(Some("a = 1 AND b = 20 AND value = 200".to_string()))
            .await
            .unwrap();
        assert_eq!(
            untouched, 1,
            "(1, 20) must not be clobbered by an `a`-only probe"
        );

        let updated = updated_ds
            .count_rows(Some("a = 1 AND b = 10 AND value = 999".to_string()))
            .await
            .unwrap();
        assert_eq!(updated, 1);

        let inserted = updated_ds
            .count_rows(Some("a = 3 AND b = 30 AND value = 333".to_string()))
            .await
            .unwrap();
        assert_eq!(inserted, 1);
    }

    /// A composite index probe can over-match the exact join key, but a target
    /// row reached by more than one source batch must still enter the join once.
    #[tokio::test]
    async fn test_indexed_merge_insert_deduplicates_cross_batch_candidates() {
        let initial = record_batch!(
            ("a", Int32, [1, 1, 2, 2]),
            ("b", Int32, [10, 20, 10, 20]),
            ("value", Int32, [100, 200, 300, 400])
        )
        .unwrap();
        let schema = initial.schema();

        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial)], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        let params = ScalarIndexParams::default();
        dataset
            .create_index(&["a"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();
        dataset
            .create_index(&["b"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        let first = record_batch!(
            ("a", Int32, [1]),
            ("b", Int32, [10]),
            ("value", Int32, [901])
        )
        .unwrap();
        // This batch probes `a IN (1, 2) AND b IN (20, 10)`, which reaches
        // (1, 10) again even though that tuple is not present in this batch.
        let second = record_batch!(
            ("a", Int32, [1, 2]),
            ("b", Int32, [20, 10]),
            ("value", Int32, [902, 903])
        )
        .unwrap();

        let (dataset, stats) =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(first), Ok(second)],
                    schema,
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_updated_rows, 3);
        assert_eq!(stats.num_inserted_rows, 0);
        assert_eq!(dataset.count_rows(None).await.unwrap(), 4);
        for (a, b, value) in [(1, 10, 901), (1, 20, 902), (2, 10, 903), (2, 20, 400)] {
            assert_eq!(
                dataset
                    .count_rows(Some(format!("a = {a} AND b = {b} AND value = {value}")))
                    .await
                    .unwrap(),
                1,
            );
        }
    }

    /// Composite key merge_insert with no scalar index on any join column
    /// must keep working via the full-scan fallback.  Guards against the
    /// indexed path becoming a hard requirement after this optimization.
    #[tokio::test]
    async fn test_indexed_merge_insert_composite_key_no_index() {
        let initial = record_batch!(
            ("a", Int32, [1, 1, 2]),
            ("b", Int32, [10, 20, 10]),
            ("value", Int32, [100, 200, 300])
        )
        .unwrap();
        let schema = initial.schema();

        let ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        let source = record_batch!(
            ("a", Int32, [1]),
            ("b", Int32, [20]),
            ("value", Int32, [999])
        )
        .unwrap();

        let (updated_ds, stats) =
            MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_updated_rows, 1);
        assert_eq!(stats.num_inserted_rows, 0);
        let count = updated_ds
            .count_rows(Some("a = 1 AND b = 20 AND value = 999".to_string()))
            .await
            .unwrap();
        assert_eq!(count, 1);
    }

    /// Composite-key merge_insert must use standard SQL NULL semantics
    /// (NULL != NULL) on the post-filter hash join so its behavior is
    /// identical to the full-scan path; otherwise enabling the indexed
    /// path for multi-column joins would silently change semantics.
    #[tokio::test]
    async fn test_indexed_merge_insert_composite_key_null_semantics() {
        let initial = record_batch!(
            ("a", Int32, [Some(1)]),
            ("b", Utf8, [Option::<&str>::None]),
            ("value", Int32, [Some(10)])
        )
        .unwrap();
        let schema = initial.schema();

        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        ds.create_index(
            &["a"],
            IndexType::Scalar,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

        let source = record_batch!(
            ("a", Int32, [Some(1)]),
            ("b", Utf8, [Option::<&str>::None]),
            ("value", Int32, [Some(99)])
        )
        .unwrap();

        let (updated_ds, stats) =
            MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_inserted_rows, 1);
        assert_eq!(stats.num_updated_rows, 0);
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 2);
    }

    /// Composite-key merge_insert where new (unindexed) fragments are
    /// appended after the indices were built.  The indexed take only sees
    /// fragments covered by every chosen index, so the unindexed remainder
    /// must be unioned in via a full scan — otherwise updates to rows
    /// that live in those fragments are silently dropped.
    #[tokio::test]
    async fn test_indexed_merge_insert_composite_key_unindexed_fragments() {
        let first = record_batch!(
            ("a", Int32, [1, 2]),
            ("b", Int32, [10, 20]),
            ("value", Int32, [100, 200])
        )
        .unwrap();
        let schema = first.schema();

        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(first.clone())], schema.clone()),
            "memory://",
            Some(WriteParams {
                max_rows_per_file: 64,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let params = ScalarIndexParams::default();
        ds.create_index(&["a"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();
        ds.create_index(&["b"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Append a fragment AFTER both indices are built.  The new (3, 30)
        // row lives in a fragment neither index covers, so the indexed
        // take alone would miss it.
        let appended = record_batch!(
            ("a", Int32, [3]),
            ("b", Int32, [30]),
            ("value", Int32, [300])
        )
        .unwrap();
        ds.append(
            RecordBatchIterator::new(vec![Ok(appended.clone())], appended.schema()),
            None,
        )
        .await
        .unwrap();

        // Source updates one row in the indexed fragment AND one row in
        // the appended (unindexed) fragment.
        let source = record_batch!(
            ("a", Int32, [1, 3]),
            ("b", Int32, [10, 30]),
            ("value", Int32, [999, 333])
        )
        .unwrap();

        let (updated_ds, stats) =
            MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                )))
                .await
                .unwrap();

        assert_eq!(
            stats.num_updated_rows, 2,
            "row in the unindexed fragment must also be updated"
        );
        assert_eq!(stats.num_inserted_rows, 0);
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 3);
    }

    /// Composite-key delete-only merge_insert (`when_matched(Delete)`,
    /// `when_not_matched_by_source(Keep)`) removes the matched rows for every
    /// combination of which join columns carry a scalar index, including when
    /// every column is indexed.
    #[rstest::rstest]
    #[case::index_on_both(true, true)]
    #[case::index_on_first(true, false)]
    #[case::index_on_second(false, true)]
    #[case::no_index(false, false)]
    #[tokio::test]
    async fn test_indexed_merge_insert_composite_key_delete(
        #[case] index_on_a: bool,
        #[case] index_on_b: bool,
    ) {
        let initial = record_batch!(
            ("a", Int32, [1, 1, 2, 2]),
            ("b", Int32, [10, 20, 10, 20]),
            ("value", Int32, [100, 200, 300, 400])
        )
        .unwrap();
        let schema = initial.schema();

        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        let params = ScalarIndexParams::default();
        if index_on_a {
            ds.create_index(&["a"], IndexType::Scalar, None, &params, false)
                .await
                .unwrap();
        }
        if index_on_b {
            ds.create_index(&["b"], IndexType::Scalar, None, &params, false)
                .await
                .unwrap();
        }

        // Delete (1, 10) by composite key.  Only key columns in the source.
        let source = record_batch!(("a", Int32, [1]), ("b", Int32, [10])).unwrap();

        let (updated_ds, stats) =
            MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::Delete)
                .when_not_matched(WhenNotMatched::DoNothing)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_deleted_rows, 1, "matched row must be deleted");
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 3);
        assert_eq!(
            updated_ds
                .count_rows(Some("a = 1 AND b = 10".to_string()))
                .await
                .unwrap(),
            0,
            "(1, 10) must be gone after the delete"
        );
        // The sibling key (1, 20) must remain untouched.
        assert_eq!(
            updated_ds
                .count_rows(Some("a = 1 AND b = 20 AND value = 200".to_string()))
                .await
                .unwrap(),
            1,
        );
    }

    /// Delete-only merge_insert on a single indexed key removes the matched
    /// rows (the indexed path is not exclusive to composite keys).
    #[tokio::test]
    async fn test_indexed_merge_insert_single_key_delete() {
        let initial = record_batch!(
            ("id", Int32, [1, 2, 3, 4]),
            ("value", Int32, [10, 20, 30, 40])
        )
        .unwrap();
        let schema = initial.schema();

        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        ds.create_index(
            &["id"],
            IndexType::Scalar,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

        let source = record_batch!(("id", Int32, [2, 4])).unwrap();

        let (updated_ds, stats) = MergeInsertBuilder::try_new(Arc::new(ds), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap()
            .execute_reader(Box::new(RecordBatchIterator::new(
                vec![Ok(source.clone())],
                source.schema(),
            )))
            .await
            .unwrap();

        assert_eq!(stats.num_deleted_rows, 2);
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 2);
        assert_eq!(
            updated_ds
                .count_rows(Some("id = 2 OR id = 4".to_string()))
                .await
                .unwrap(),
            0,
        );
    }

    /// `when_matched(Fail)` on an indexed key aborts the operation when a
    /// source row matches an existing key, and inserts cleanly when none do.
    #[tokio::test]
    async fn test_indexed_merge_insert_when_matched_fail() {
        let initial =
            record_batch!(("id", Int32, [1, 2, 3]), ("value", Int32, [10, 20, 30])).unwrap();
        let schema = initial.schema();

        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        ds.create_index(
            &["id"],
            IndexType::Scalar,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();
        let ds = Arc::new(ds);

        // A source row matching an existing key must fail the operation.
        let matching = record_batch!(("id", Int32, [2]), ("value", Int32, [999])).unwrap();
        let err = MergeInsertBuilder::try_new(ds.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::Fail)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute_reader(Box::new(RecordBatchIterator::new(
                vec![Ok(matching.clone())],
                matching.schema(),
            )))
            .await
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("Merge insert failed"), "got: {msg}");
        assert!(msg.contains("found matching row"), "got: {msg}");

        // A source with no matching key inserts without failing.
        let new_rows = record_batch!(("id", Int32, [4]), ("value", Int32, [40])).unwrap();
        let (updated_ds, stats) = MergeInsertBuilder::try_new(ds.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::Fail)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute_reader(Box::new(RecordBatchIterator::new(
                vec![Ok(new_rows.clone())],
                new_rows.schema(),
            )))
            .await
            .unwrap();
        assert_eq!(stats.num_inserted_rows, 1);
        assert_eq!(stats.num_updated_rows, 0);
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 4);
    }

    /// Fully-indexed composite-key `when_matched(Delete)` combined with
    /// `when_not_matched(InsertAll)` must both delete matched rows and write
    /// the inserted rows.
    #[tokio::test]
    async fn test_indexed_merge_insert_composite_key_delete_with_insert() {
        let initial = record_batch!(
            ("a", Int32, [1, 1, 2, 2]),
            ("b", Int32, [10, 20, 10, 20]),
            ("value", Int32, [100, 200, 300, 400])
        )
        .unwrap();
        let schema = initial.schema();

        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        let params = ScalarIndexParams::default();
        ds.create_index(&["a"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();
        ds.create_index(&["b"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Source matches (1, 10) -> delete, and (3, 30) is new -> insert.
        let source = record_batch!(
            ("a", Int32, [1, 3]),
            ("b", Int32, [10, 30]),
            ("value", Int32, [999, 333])
        )
        .unwrap();

        let (updated_ds, stats) =
            MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::Delete)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_deleted_rows, 1);
        assert_eq!(stats.num_inserted_rows, 1);
        // 4 - 1 deleted + 1 inserted = 4.
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 4);
        assert_eq!(
            updated_ds
                .count_rows(Some("a = 1 AND b = 10".to_string()))
                .await
                .unwrap(),
            0,
            "matched row must be deleted, not updated"
        );
        assert_eq!(
            updated_ds
                .count_rows(Some("a = 3 AND b = 30 AND value = 333".to_string()))
                .await
                .unwrap(),
            1,
            "unmatched source row must be inserted"
        );
    }

    /// A delete whose source contains duplicate keys matching the same target
    /// row applies `source_dedupe_behavior` on the indexed-scan path, exactly
    /// like an update: the default `Fail` aborts (naming the ambiguous key),
    /// while `FirstSeen` removes and counts the row once and reports the extra
    /// match as a skipped duplicate.
    #[rstest::rstest]
    #[case::fail(SourceDedupeBehavior::Fail)]
    #[case::first_seen(SourceDedupeBehavior::FirstSeen)]
    #[tokio::test]
    async fn test_indexed_merge_insert_delete_source_duplicates(
        #[case] behavior: SourceDedupeBehavior,
    ) {
        let initial = record_batch!(
            ("a", Int32, [1, 1, 2, 2]),
            ("b", Int32, [10, 20, 10, 20]),
            ("value", Int32, [100, 200, 300, 400])
        )
        .unwrap();
        let schema = initial.schema();

        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        // Index every join column so the merge takes the indexed-scan delete path.
        let params = ScalarIndexParams::default();
        ds.create_index(&["a"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();
        ds.create_index(&["b"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Two source rows collide on the same target key (1, 10).
        let source = record_batch!(("a", Int32, [1, 1]), ("b", Int32, [10, 10])).unwrap();

        let result =
            MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::Delete)
                .when_not_matched(WhenNotMatched::DoNothing)
                .source_dedupe_behavior(behavior)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                )))
                .await;

        if behavior == SourceDedupeBehavior::Fail {
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("Ambiguous merge inserts") && err.contains("a = 1"),
                "Fail must abort naming the ambiguous key, got: {err}"
            );
            return;
        }

        let (updated_ds, stats) = result.unwrap();
        assert_eq!(stats.num_deleted_rows, 1);
        assert_eq!(stats.num_skipped_duplicates, 1);
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 3);
        assert_eq!(
            updated_ds
                .count_rows(Some("a = 1 AND b = 10".to_string()))
                .await
                .unwrap(),
            0,
            "the matched row must be removed exactly once"
        );
    }

    /// The v2 plans apply the same `source_dedupe_behavior` to deletes when the
    /// source has duplicate keys matching one target row — covering both
    /// `FullSchemaMergeInsertExec` (`Delete + InsertAll`) and
    /// `DeleteOnlyMergeInsertExec` (pure delete). No scalar index, so routing
    /// stays on the v2 path.
    #[rstest::rstest]
    #[case::full_schema_fail(true, SourceDedupeBehavior::Fail)]
    #[case::full_schema_first_seen(true, SourceDedupeBehavior::FirstSeen)]
    #[case::delete_only_fail(false, SourceDedupeBehavior::Fail)]
    #[case::delete_only_first_seen(false, SourceDedupeBehavior::FirstSeen)]
    #[tokio::test]
    async fn test_v2_merge_insert_delete_source_duplicates(
        #[case] with_insert: bool,
        #[case] behavior: SourceDedupeBehavior,
    ) {
        let initial =
            record_batch!(("a", Int32, [1, 2, 3]), ("value", Int32, [10, 20, 30])).unwrap();
        let schema = initial.schema();

        let ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        // Two source rows collide on target key a=1. With insert, a=4 is new.
        let (source, when_not_matched, expected_inserted, expected_total) = if with_insert {
            (
                record_batch!(("a", Int32, [1, 1, 4]), ("value", Int32, [99, 99, 40])).unwrap(),
                WhenNotMatched::InsertAll,
                1,
                3, // 3 - 1 deleted + 1 inserted
            )
        } else {
            (
                record_batch!(("a", Int32, [1, 1])).unwrap(),
                WhenNotMatched::DoNothing,
                0,
                2, // 3 - 1 deleted
            )
        };

        let result = MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string()])
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(when_not_matched)
            .source_dedupe_behavior(behavior)
            .try_build()
            .unwrap()
            .execute_reader(Box::new(RecordBatchIterator::new(
                vec![Ok(source.clone())],
                source.schema(),
            )))
            .await;

        if behavior == SourceDedupeBehavior::Fail {
            let err = result.unwrap_err().to_string();
            assert!(
                err.contains("Ambiguous merge inserts") && err.contains("a = 1"),
                "Fail must abort naming the ambiguous key, got: {err}"
            );
            return;
        }

        let (updated_ds, stats) = result.unwrap();
        assert_eq!(stats.num_deleted_rows, 1, "the matched row is removed once");
        assert_eq!(stats.num_skipped_duplicates, 1);
        assert_eq!(stats.num_inserted_rows, expected_inserted);
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), expected_total);
        assert_eq!(
            updated_ds
                .count_rows(Some("a = 1".to_string()))
                .await
                .unwrap(),
            0,
            "the matched row must be removed exactly once"
        );
    }

    /// A partial-schema source that combines `when_matched(Delete)` with
    /// `when_not_matched(InsertAll)` must succeed even when every join key is
    /// indexed. The indexed-scan delete path cannot fold a delete into a
    /// partial write, so this case routes to the v2 plan (which fills omitted
    /// nullable target columns) instead of being rejected.
    #[tokio::test]
    async fn test_indexed_merge_insert_partial_schema_delete_with_insert() {
        // Target carries two nullable non-key columns; the source omits `note`.
        let full_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
            Field::new("note", DataType::Utf8, true),
        ]));
        let full_batch = RecordBatch::try_new(
            full_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2, 2])),
                Arc::new(Int32Array::from(vec![10, 20, 10, 20])),
                Arc::new(Int32Array::from(vec![100, 200, 300, 400])),
                Arc::new(StringArray::from(vec!["w", "x", "y", "z"])),
            ],
        )
        .unwrap();

        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(full_batch)], full_schema.clone()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        let params = ScalarIndexParams::default();
        ds.create_index(&["a"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();
        ds.create_index(&["b"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Source deletes matched (1, 10) and inserts new (3, 30), omitting `note`.
        let partial_schema = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ]));
        let source = RecordBatch::try_new(
            partial_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 3])),
                Arc::new(Int32Array::from(vec![10, 30])),
                Arc::new(Int32Array::from(vec![999, 333])),
            ],
        )
        .unwrap();

        let (updated_ds, stats) =
            MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::Delete)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_deleted_rows, 1);
        assert_eq!(stats.num_inserted_rows, 1);
        // 4 - 1 deleted + 1 inserted = 4.
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 4);
        assert_eq!(
            updated_ds
                .count_rows(Some("a = 1 AND b = 10".to_string()))
                .await
                .unwrap(),
            0,
            "matched row must be deleted, not updated"
        );
        // Inserted row carries the omitted `note` column as NULL.
        assert_eq!(
            updated_ds
                .count_rows(Some(
                    "a = 3 AND b = 30 AND value = 333 AND note IS NULL".to_string()
                ))
                .await
                .unwrap(),
            1,
            "unmatched source row must be inserted with omitted column NULL-filled"
        );
    }

    /// Fully-indexed composite-key delete across multiple fragments, with
    /// stable row ids on/off.  Exercises the indexed-scan delete commit
    /// path: matched row ids are resolved to addresses (via the row-id
    /// index when stable) and removed without rewriting any fragments.
    /// Also covers an appended fragment that neither index covers, so the
    /// delete must reach rows via the unindexed-remainder union too.
    #[rstest::rstest]
    #[case(true)]
    #[case(false)]
    #[tokio::test]
    async fn test_indexed_merge_insert_composite_key_delete_multi_fragment(
        #[case] enable_stable_row_ids: bool,
    ) {
        let initial = record_batch!(
            ("a", Int32, [1, 1, 2, 2]),
            ("b", Int32, [10, 20, 10, 20]),
            ("value", Int32, [100, 200, 300, 400])
        )
        .unwrap();
        let schema = initial.schema();

        // One row per fragment so the delete spans multiple fragments.
        let mut ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial.clone())], schema.clone()),
            "memory://",
            Some(WriteParams {
                max_rows_per_file: 1,
                enable_stable_row_ids,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        let params = ScalarIndexParams::default();
        ds.create_index(&["a"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();
        ds.create_index(&["b"], IndexType::Scalar, None, &params, false)
            .await
            .unwrap();

        // Append a row AFTER the indices are built so it lives in a fragment
        // neither index covers.  The delete must still reach it.
        let appended = record_batch!(
            ("a", Int32, [3]),
            ("b", Int32, [30]),
            ("value", Int32, [500])
        )
        .unwrap();
        ds.append(
            RecordBatchIterator::new(vec![Ok(appended.clone())], appended.schema()),
            None,
        )
        .await
        .unwrap();

        // Delete an indexed row (1, 10) and the unindexed appended row (3, 30).
        let source = record_batch!(("a", Int32, [1, 3]), ("b", Int32, [10, 30])).unwrap();

        let (updated_ds, stats) =
            MergeInsertBuilder::try_new(Arc::new(ds), vec!["a".to_string(), "b".to_string()])
                .unwrap()
                .when_matched(WhenMatched::Delete)
                .when_not_matched(WhenNotMatched::DoNothing)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(source.clone())],
                    source.schema(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_deleted_rows, 2);
        assert_eq!(updated_ds.count_rows(None).await.unwrap(), 3);
        assert_eq!(
            updated_ds
                .count_rows(Some("(a = 1 AND b = 10) OR (a = 3 AND b = 30)".to_string()))
                .await
                .unwrap(),
            0,
            "both matched rows must be gone"
        );
        // Untouched rows survive with their original values.
        assert_eq!(
            updated_ds
                .count_rows(Some("a = 2 AND b = 20 AND value = 400".to_string()))
                .await
                .unwrap(),
            1,
        );
    }

    /// Composite-key `MapIndexExec` formats its Display so plans expose
    /// every probed column, and `with_new_children` round-trips the full
    /// lookup list rather than collapsing back to a single-column form.
    /// Lives here (not in scalar_index.rs's tests) so the new lines don't
    /// pile up in a file with pending upstream conflicts.
    #[test]
    fn map_index_exec_multi_lookup_plan_shape() {
        use crate::io::exec::scalar_index::{IndexLookup, MapIndexExec, ScalarIndexExec};
        use crate::utils::test::NoContextTestFixture;
        use datafusion::physical_plan::{ExecutionPlan, displayable};
        use datafusion::scalar::ScalarValue;
        use lance_index::scalar::{
            SargableQuery,
            expression::{ScalarIndexExpr, ScalarIndexSearch},
        };
        use lance_select::result::IndexExprResultWireFormat;

        let fixture = NoContextTestFixture::new();
        let dataset = Arc::new(fixture.dataset);

        let dummy_input: Arc<dyn ExecutionPlan> = Arc::new(ScalarIndexExec::new(
            dataset.clone(),
            ScalarIndexExpr::Query(ScalarIndexSearch {
                column: "ordered".to_string(),
                index_name: "ordered_idx".to_string(),
                index_type: "BTree".to_string(),
                query: Arc::new(SargableQuery::Equals(ScalarValue::UInt64(Some(1)))),
                needs_recheck: false,
                fragment_bitmap: None,
            }),
            IndexExprResultWireFormat::default(),
        ));

        let lookups = vec![
            IndexLookup::new("a", "a_idx"),
            IndexLookup::new("b", "b_idx"),
        ];
        let plan: Arc<dyn ExecutionPlan> = Arc::new(MapIndexExec::new_multi(
            dataset.clone(),
            lookups,
            dummy_input.clone(),
        ));

        let rendered = format!("{}", displayable(plan.as_ref()).indent(false));
        assert!(
            rendered.contains("IndexedLookup [a, b]"),
            "multi-lookup Display must list every probed column, got: {rendered}",
        );

        let rebuilt = plan
            .with_new_children(vec![dummy_input.clone()])
            .expect("with_new_children must accept exactly one child");
        let rebuilt_rendered = format!("{}", displayable(rebuilt.as_ref()).indent(false));
        assert!(
            rebuilt_rendered.contains("IndexedLookup [a, b]"),
            "with_new_children must preserve every lookup, got: {rebuilt_rendered}",
        );

        // The single-lookup convenience constructor still renders without
        // the column list, so existing EXPLAIN output is unchanged for
        // single-column joins.
        let single: Arc<dyn ExecutionPlan> = Arc::new(MapIndexExec::new(
            dataset,
            "ordered".to_string(),
            "ordered_idx".to_string(),
            dummy_input,
        ));
        let single_rendered = format!("{}", displayable(single.as_ref()).indent(false));
        assert!(
            single_rendered.contains("IndexedLookup")
                && !single_rendered.contains("IndexedLookup ["),
            "single-lookup Display must not include the column list, got: {single_rendered}",
        );
    }

    mod subcols {
        use super::*;
        use rstest::rstest;

        struct Fixtures {
            ds: Arc<Dataset>,
            new_data: RecordBatch,
        }

        async fn setup(scalar_index: bool) -> Fixtures {
            let data = lance_datagen::gen_batch()
                .with_seed(Seed::from(1))
                .col("other", array::rand_utf8(4.into(), false))
                .col("value", array::step::<UInt32Type>())
                .col("key", array::rand_pseudo_uuid_hex());
            let batch = data.into_batch_rows(RowCount::from(1024 + 2)).unwrap();
            let batch1 = batch.slice(0, 512);
            let batch2 = batch.slice(512, 512);
            let batch3 = batch.slice(1024, 2);
            let schema = batch.schema();

            let reader = Box::new(RecordBatchIterator::new(
                [Ok(batch1.clone())],
                schema.clone(),
            ));
            let write_params = WriteParams {
                max_rows_per_file: 256,
                max_rows_per_group: 32, // Non-standard group size to hit edge cases
                ..Default::default()
            };
            let mut ds = Dataset::write(reader, "memory://", Some(write_params.clone()))
                .await
                .unwrap();

            if scalar_index {
                let index_params = ScalarIndexParams::default();
                ds.create_index(&["key"], IndexType::Scalar, None, &index_params, false)
                    .await
                    .unwrap();
            }

            // Another two files, not in the scalar index (if there is one)
            let reader = Box::new(RecordBatchIterator::new(
                [Ok(batch2.clone())],
                batch2.schema(),
            ));
            ds.append(reader, Some(write_params)).await.unwrap();

            let ds = Arc::new(ds);

            // New data with only a subset of columns
            let update_schema = Arc::new(schema.project(&[2, 1]).unwrap());
            // Full second file and part of third file. Also two more new rows.
            let indices: Int64Array = (256..512).chain(600..612).chain([712, 715]).collect();
            let keys = arrow::compute::take(batch["key"].as_ref(), &indices, None).unwrap();
            let keys = arrow::compute::concat(&[&keys, &batch3["key"]]).unwrap();
            let num_rows = keys.len();
            let new_data = RecordBatch::try_new(
                update_schema,
                vec![
                    keys,
                    Arc::new((1024..(1024 + num_rows as u32)).collect::<UInt32Array>()),
                ],
            )
            .unwrap();

            Fixtures { ds, new_data }
        }

        #[tokio::test]
        async fn test_delete_not_matched_by_source_on_v2_subcols() {
            // Historical note: this combination used to be rejected outright
            // on the v1 subcols path because v1 could not delete by source
            // while rewriting only a subset of columns. The v2 path handles
            // it uniformly through the action column, so the previously-
            // rejected configuration now succeeds. This test asserts the
            // successful path to keep the negative-test history explicit.
            let Fixtures { ds, new_data } = Box::pin(setup(false)).await;

            let rows_before = ds.count_rows(None).await.unwrap() as u64;

            let reader = Box::new(RecordBatchIterator::new(
                [Ok(new_data.clone())],
                new_data.schema(),
            ));

            let job = MergeInsertBuilder::try_new(ds.clone(), vec!["key".to_string()])
                .unwrap()
                .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::DoNothing)
                .try_build()
                .unwrap();
            // assert_send also pins us to the "returned future is Send"
            // contract that the previous (negative) version of this test
            // used to guard.
            let (updated_ds, stats) = assert_send(job.execute_reader(reader))
                .await
                .expect("partial-schema + delete-by-source should succeed on v2");

            // 272 rows in source — 2 of them are inserts (ignored because
            // when_not_matched is DoNothing), the other 270 match existing
            // rows and update them. The remaining 754 target rows that are
            // not matched by the source get deleted.
            assert_eq!(stats.num_updated_rows, 270);
            assert_eq!(stats.num_inserted_rows, 0);
            assert_eq!(stats.num_deleted_rows, rows_before - 270);
            assert_eq!(
                updated_ds.count_rows(None).await.unwrap() as u64,
                270,
                "only the 270 updated rows should remain after the delete-by-source"
            );
        }

        #[tokio::test]
        async fn test_errors_on_bad_schema() {
            let Fixtures { ds, new_data } = Box::pin(setup(false)).await;

            // Schema with different names, which should be rejected.
            let bad_schema = Arc::new(Schema::new(vec![
                Field::new("wrong_key", DataType::Utf8, false),
                Field::new("wrong_value", DataType::UInt32, false),
            ]));

            // Should reject when data is not a subschema.
            let bad_batch =
                RecordBatch::try_new(bad_schema.clone(), new_data.columns().to_vec()).unwrap();
            let reader = Box::new(RecordBatchIterator::new([Ok(bad_batch)], bad_schema));

            let job = MergeInsertBuilder::try_new(ds.clone(), vec!["key".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::DoNothing)
                .try_build()
                .unwrap();
            let res = job.execute_reader(reader).await;
            assert!(
                matches!(
                    &res,
                    &Err(Error::SchemaMismatch { ref difference, .. })
                        if difference.clone().contains("fields did not match")
                ),
                "Expected SchemaMismatch error, got: {:?}",
                res
            );
        }

        #[rstest]
        #[tokio::test]
        async fn test_merge_insert_subcols(
            #[values(false, true)] scalar_index: bool,
            #[values(false, true)] insert: bool,
        ) {
            let Fixtures { ds, new_data } = Box::pin(setup(scalar_index)).await;
            let reader = Box::new(RecordBatchIterator::new(
                [Ok(new_data.clone())],
                new_data.schema(),
            ));
            let fragments_before = ds
                .get_fragments()
                .iter()
                .map(|f| f.metadata().clone())
                .collect::<Vec<_>>();
            let job = MergeInsertBuilder::try_new(ds.clone(), vec!["key".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(if insert {
                    WhenNotMatched::InsertAll
                } else {
                    WhenNotMatched::DoNothing
                })
                .try_build()
                .unwrap();

            let (ds, stats) = job.execute_reader(reader).await.unwrap();

            let fragments_after = ds
                .get_fragments()
                .iter()
                .map(|f| f.metadata().clone())
                .collect::<Vec<_>>();

            // Stats are path-independent: 272 source rows = 2 inserts
            // (if insert) + 270 updates, nothing deleted.
            assert_eq!(stats.num_updated_rows, (new_data.num_rows() - 2) as u64);
            assert_eq!(stats.num_deleted_rows, 0);
            if insert {
                assert_eq!(stats.num_inserted_rows, 2);
            } else {
                assert_eq!(stats.num_inserted_rows, 0);
            }

            if scalar_index {
                // v1 path: partial-schema upserts with a scalar index on the
                // join key fall back to the in-place Merger that rewrites
                // only the changed columns. Verify the legacy file-layout
                // optimization (tombstoned field ids + new partial data
                // files) is still produced on this path, including
                // unchanged fragment ids.
                assert_eq!(
                    fragments_before.iter().map(|f| f.id).collect::<Vec<_>>(),
                    fragments_after
                        .iter()
                        .take(fragments_before.len())
                        .map(|f| f.id)
                        .collect::<Vec<_>>()
                );
                assert_eq!(fragments_before[0], fragments_after[0]);
                assert_ne!(fragments_before[1], fragments_after[1]);
                assert_ne!(fragments_before[2], fragments_after[2]);
                assert_eq!(fragments_before[3], fragments_after[3]);

                let has_added_files = |frag: &Fragment| {
                    assert_eq!(frag.files.len(), 2);
                    let data_files = &frag.files;
                    // Updated columns should be only columns in new data files
                    // -2 field ids are tombstoned.
                    assert_eq!(data_files[0].fields.as_ref(), &[0, -2, -2]);
                    assert_eq!(data_files[1].fields.as_ref(), &[2, 1]);
                };
                has_added_files(&fragments_after[1]);
                has_added_files(&fragments_after[2]);

                if insert {
                    assert_eq!(fragments_after.len(), 5);
                } else {
                    assert_eq!(fragments_after.len(), 4);
                }
            } else {
                // v2 path: partial-schema upserts run through the same
                // FullSchemaMergeInsertExec as full-schema upserts and
                // write brand-new fragments. Fragment 1 is entirely
                // matched (all 256 rows) so it is removed; fragment 2 is
                // partially matched so it keeps its id with a deletion
                // vector; a new fragment holds the 270 updated rows
                // (and the 2 inserted rows when `insert` is set).
                let ids_after: Vec<u64> = fragments_after.iter().map(|f| f.id).collect();
                assert_eq!(
                    fragments_after.len(),
                    4,
                    "expected [frag 0, frag 2, frag 3, new frag], got {:?}",
                    ids_after
                );
                assert_eq!(
                    fragments_before[0], fragments_after[0],
                    "frag 0 (untouched) should be identical"
                );
                assert!(
                    !ids_after.contains(&1),
                    "frag 1 was fully matched by source and should have been removed"
                );
                assert!(
                    ids_after.contains(&2),
                    "frag 2 was only partially matched and should still be present"
                );
                assert!(
                    ids_after.contains(&3),
                    "frag 3 (untouched) should still be present"
                );
            }

            // Semantic data check (shared across both code paths): look
            // rows up by key so we don't depend on the scan-order mechanics
            // of v1 vs v2. For updated rows, `value` must be the new
            // source value and `other` must be the original (preserved
            // from the target). For untouched rows, both columns must be
            // unchanged.
            let data = ds
                .scan()
                .scan_in_order(true)
                .try_into_batch()
                .await
                .unwrap();
            assert_eq!(data.num_rows(), if insert { 1024 + 2 } else { 1024 });
            assert_eq!(data.num_columns(), 3);

            use std::collections::HashMap;
            let other_col = data
                .column_by_name("other")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let value_col = data
                .column_by_name("value")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let key_col = data
                .column_by_name("key")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let mut row_by_key: HashMap<String, (u32, String)> = HashMap::new();
            for i in 0..data.num_rows() {
                row_by_key.insert(
                    key_col.value(i).to_string(),
                    (value_col.value(i), other_col.value(i).to_string()),
                );
            }

            // Pull original column data for reference lookups.
            let orig_batch_schema = new_data.schema();
            assert_eq!(orig_batch_schema.field(0).name(), "key");
            assert_eq!(orig_batch_schema.field(1).name(), "value");
            let new_keys = new_data
                .column(0)
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let new_values = new_data
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            // Every updated source row (270 of them) should be present
            // with its new value and a preserved `other` string.
            for i in 0..(new_data.num_rows() - 2) {
                let key = new_keys.value(i).to_string();
                let (value, other) = row_by_key
                    .get(&key)
                    .unwrap_or_else(|| panic!("updated key {} missing from result", key));
                assert_eq!(*value, new_values.value(i));
                assert!(
                    !other.is_empty(),
                    "updated row for key {} should retain its original `other` value",
                    key
                );
            }
            // The 2 batch3 rows at the end of new_data are inserts.
            for i in (new_data.num_rows() - 2)..new_data.num_rows() {
                let key = new_keys.value(i).to_string();
                let found = row_by_key.get(&key);
                if insert {
                    let (value, _) =
                        found.unwrap_or_else(|| panic!("inserted key {} missing from result", key));
                    assert_eq!(*value, new_values.value(i));
                } else {
                    assert!(
                        found.is_none(),
                        "unmatched source row for key {} must not be present when insert=false",
                        key
                    );
                }
            }
        }

        /// Verifies that `explain_plan` succeeds for a partial-schema upsert
        /// and emits a plan that uses the v2 `FullSchemaMergeInsertExec`
        /// path. This is the explicit acceptance criterion for #6442: the
        /// partial-schema path must go through the same physical plan as
        /// full-schema upserts instead of falling back to v1.
        #[tokio::test]
        async fn test_merge_insert_subcols_v2_explain_plan() {
            let Fixtures { ds, new_data } = Box::pin(setup(false)).await;

            let job = MergeInsertBuilder::try_new(ds.clone(), vec!["key".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::DoNothing)
                .try_build()
                .unwrap();

            let source_schema: Schema = new_data.schema().as_ref().clone();
            let plan = job
                .explain_plan(Some(&source_schema), false)
                .await
                .expect("explain_plan must succeed for partial-schema upsert on v2");

            // The `MergeInsert: on=[...]` header is rendered by the v2
            // extension node's `fmt_for_explain` — it only appears when
            // `create_plan` was used (legacy v1 does not go through this
            // path at all). Combined with the presence of `HashJoinExec`
            // this uniquely identifies the v2 physical plan.
            assert!(
                plan.contains("MergeInsert: on=[key]"),
                "expected MergeInsert extension node in plan (v2 marker), got: {}",
                plan
            );
            assert!(
                plan.contains("HashJoinExec"),
                "expected HashJoinExec in plan, got: {}",
                plan
            );
            // Evidence that the partial-schema fix is active: the target
            // side of the join reads the `other` column (which is missing
            // from the source) and an explicit projection carries it
            // through to the write exec alongside source columns.
            assert!(
                plan.contains("LanceRead") && plan.contains("projection=[other"),
                "target-side scan should include the filled `other` column: {}",
                plan
            );
            assert!(
                plan.contains("other@0 as other"),
                "expected post-join projection to carry `other` from the target side: {}",
                plan
            );
        }

        /// Partial-schema upserts with `insert_not_matched=InsertAll` must
        /// reject non-nullable missing columns at the API boundary instead
        /// of producing a confusing downstream writer error. The user-
        /// facing error message must name the offending column(s).
        #[tokio::test]
        async fn test_merge_insert_subcols_v2_rejects_non_nullable_insert() {
            // Build a dataset whose `other` column is explicitly non-nullable
            // so that a partial source missing it cannot safely insert new rows.
            let full_schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::UInt32, true),
                Field::new("other", DataType::Utf8, false),
            ]));
            let full_batch = RecordBatch::try_new(
                full_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["k0", "k1", "k2"])),
                    Arc::new(UInt32Array::from(vec![0, 1, 2])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap();
            let ds = Dataset::write(
                Box::new(RecordBatchIterator::new([Ok(full_batch)], full_schema)),
                "memory://",
                None,
            )
            .await
            .unwrap();
            let ds = Arc::new(ds);

            // Source source lacks `other` and tries to insert a new key.
            let partial_schema = Arc::new(Schema::new(vec![
                Field::new("key", DataType::Utf8, false),
                Field::new("value", DataType::UInt32, true),
            ]));
            let partial_batch = RecordBatch::try_new(
                partial_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["k1", "k_new"])),
                    Arc::new(UInt32Array::from(vec![11, 99])),
                ],
            )
            .unwrap();
            let reader = Box::new(RecordBatchIterator::new(
                [Ok(partial_batch)],
                partial_schema,
            ));

            let res = MergeInsertBuilder::try_new(ds, vec!["key".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(reader)
                .await;

            match res {
                Err(Error::InvalidInput { source, .. }) => {
                    let msg = source.to_string();
                    assert!(
                        msg.contains("partial-schema")
                            && msg.contains("non-nullable")
                            && msg.contains("\"other\""),
                        "expected descriptive partial-schema / non-nullable error naming \
                         the `other` column, got: {}",
                        msg
                    );
                }
                other => panic!(
                    "expected InvalidInput error for non-nullable missing column on insert path, got: {:?}",
                    other
                ),
            }
        }

        /// Partial-schema v2 upsert must correctly handle `camelCase` column
        /// names both in the join key and in a column that is *omitted* from
        /// the source. DataFusion's `col()` lowercases unquoted identifiers,
        /// so the partial-schema fill-in wraps the target reference in double
        /// quotes (`target."<name>"`) and `on_cols` are likewise quoted. This
        /// test pins that behavior down — prior to this, the v2 partial-schema
        /// path had no coverage for case-sensitive column names.
        #[tokio::test]
        async fn test_merge_insert_subcols_v2_camel_case_column() {
            // Target dataset: camelCase join key AND a camelCase nullable
            // column that will be omitted from the source schema.
            let full_schema = Arc::new(Schema::new(vec![
                Field::new("userId", DataType::Utf8, false),
                Field::new("score", DataType::UInt32, true),
                Field::new("extraData", DataType::Utf8, true),
            ]));
            let full_batch = RecordBatch::try_new(
                full_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["u1", "u2", "u3"])),
                    Arc::new(UInt32Array::from(vec![10, 20, 30])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap();
            let ds = Dataset::write(
                Box::new(RecordBatchIterator::new([Ok(full_batch)], full_schema)),
                "memory://",
                None,
            )
            .await
            .unwrap();
            let ds = Arc::new(ds);

            // Partial-schema source: no `extraData`. Updates `u2` and inserts `u_new`.
            let partial_schema = Arc::new(Schema::new(vec![
                Field::new("userId", DataType::Utf8, false),
                Field::new("score", DataType::UInt32, true),
            ]));
            let partial_batch = RecordBatch::try_new(
                partial_schema.clone(),
                vec![
                    Arc::new(StringArray::from(vec!["u2", "u_new"])),
                    Arc::new(UInt32Array::from(vec![22, 99])),
                ],
            )
            .unwrap();
            let reader = Box::new(RecordBatchIterator::new(
                [Ok(partial_batch)],
                partial_schema,
            ));

            let job = MergeInsertBuilder::try_new(ds.clone(), vec!["userId".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap();
            let (updated_ds, stats) = job
                .execute_reader(reader)
                .await
                .expect("camelCase partial-schema upsert must succeed on v2");

            assert_eq!(stats.num_updated_rows, 1);
            assert_eq!(stats.num_inserted_rows, 1);
            assert_eq!(stats.num_deleted_rows, 0);

            // Read the whole dataset back and index by userId so assertions
            // are independent of physical row order.
            let data = updated_ds
                .scan()
                .scan_in_order(true)
                .try_into_batch()
                .await
                .unwrap();
            assert_eq!(data.num_rows(), 4);
            assert_eq!(data.num_columns(), 3);

            let user_ids = data
                .column_by_name("userId")
                .expect("camelCase join key column must be present in result")
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let scores = data
                .column_by_name("score")
                .unwrap()
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let extra = data
                .column_by_name("extraData")
                .expect("camelCase omitted column must be present in result")
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();

            let mut by_user: std::collections::HashMap<String, (u32, Option<String>)> =
                std::collections::HashMap::new();
            for i in 0..data.num_rows() {
                let extra_val = if extra.is_null(i) {
                    None
                } else {
                    Some(extra.value(i).to_string())
                };
                by_user.insert(user_ids.value(i).to_string(), (scores.value(i), extra_val));
            }

            // Untouched rows: unchanged.
            assert_eq!(by_user["u1"], (10, Some("a".to_string())));
            assert_eq!(by_user["u3"], (30, Some("c".to_string())));
            // Updated row: score bumped, camelCase `extraData` preserved from target.
            assert_eq!(
                by_user["u2"],
                (22, Some("b".to_string())),
                "partial-schema update must preserve camelCase `extraData` from the target side of the join"
            );
            // Inserted row: camelCase column must be NULL (outer-join target side is NULL).
            assert_eq!(
                by_user["u_new"],
                (99, None),
                "partial-schema insert must produce NULL for omitted camelCase column"
            );
        }

        /// End-to-end bloom-filter conflict-detection check for a
        /// partial-schema upsert. With the v2 path enabled for partial
        /// schema, the returned transaction must carry a populated
        /// `inserted_rows_filter` whenever the join key is an unenforced
        /// primary key. Previously (v1 path) this filter was always
        /// `None` for partial schema.
        #[tokio::test]
        async fn test_merge_insert_subcols_v2_bloom_filter() {
            let schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt32, false).with_metadata(
                    vec![(
                        "lance-schema:unenforced-primary-key".to_string(),
                        "true".to_string(),
                    )]
                    .into_iter()
                    .collect(),
                ),
                Field::new("value", DataType::UInt32, true),
                Field::new("tag", DataType::Utf8, true),
            ]));
            let initial = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt32Array::from(vec![0, 1, 2])),
                    Arc::new(UInt32Array::from(vec![0, 0, 0])),
                    Arc::new(StringArray::from(vec!["a", "b", "c"])),
                ],
            )
            .unwrap();
            let dataset = InsertBuilder::new("memory://")
                .execute(vec![initial])
                .await
                .unwrap();
            let dataset = Arc::new(dataset);

            // Partial source — only `id` and `value`, missing nullable `tag`.
            let partial_schema = Arc::new(Schema::new(vec![
                Field::new("id", DataType::UInt32, false).with_metadata(
                    vec![(
                        "lance-schema:unenforced-primary-key".to_string(),
                        "true".to_string(),
                    )]
                    .into_iter()
                    .collect(),
                ),
                Field::new("value", DataType::UInt32, true),
            ]));
            let partial = RecordBatch::try_new(
                partial_schema.clone(),
                vec![
                    Arc::new(UInt32Array::from(vec![1, 5])), // one update (1), one insert (5)
                    Arc::new(UInt32Array::from(vec![42, 99])),
                ],
            )
            .unwrap();
            let stream = RecordBatchStreamAdapter::new(
                partial_schema,
                futures::stream::iter(vec![Ok(partial)]),
            );

            let UncommittedMergeInsert { transaction, .. } =
                MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
                    .unwrap()
                    .when_matched(WhenMatched::UpdateAll)
                    .when_not_matched(WhenNotMatched::InsertAll)
                    .try_build()
                    .unwrap()
                    .execute_uncommitted(Box::pin(stream) as SendableRecordBatchStream)
                    .await
                    .unwrap();

            // The committed transaction must carry the populated bloom
            // filter — this is the core conflict-detection acceptance
            // criterion for #6442.
            let committed = CommitBuilder::new(dataset.clone())
                .execute(transaction)
                .await
                .unwrap();
            let tx_path = committed
                .manifest()
                .transaction_file
                .clone()
                .expect("transaction file must be written");
            let tx_read =
                read_transaction_file(dataset.object_store.as_ref(), &dataset.base, &tx_path)
                    .await
                    .unwrap();
            match &tx_read.operation {
                Operation::Update {
                    inserted_rows_filter,
                    ..
                } => {
                    let filter = inserted_rows_filter
                        .as_ref()
                        .expect("partial-schema upsert on a PK must emit a bloom filter");
                    // Exactly one key field (id).
                    assert_eq!(filter.field_ids.len(), 1);
                }
                other => panic!("expected Operation::Update, got: {:?}", other),
            }
        }
    }

    // For some reason, Windows isn't able to handle the timeout test. Possibly
    // a performance bug in their timer implementation?
    #[cfg(not(windows))]
    #[rstest::rstest]
    #[case::all_success(Duration::from_secs(100_000))]
    #[case::timeout(Duration::from_millis(200))]
    #[tokio::test]
    async fn test_merge_insert_concurrency(#[case] timeout: Duration) {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::UInt32, false),
        ]));
        // To benchmark scaling curve: measure how long to run
        //
        // And vary `concurrency` to see how it scales. Compare this again `main`.
        let concurrency = 10;
        let initial_data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from_iter_values(0..concurrency)),
                Arc::new(UInt32Array::from_iter_values(std::iter::repeat_n(
                    0,
                    concurrency as usize,
                ))),
            ],
        )
        .unwrap();

        // Increase likelihood of contention by throttling the store
        let throttled = Arc::new(ThrottledStoreWrapper {
            config: ThrottleConfig {
                // For benchmarking: Increase this to simulate object storage.
                wait_list_per_call: Duration::from_millis(20),
                wait_get_per_call: Duration::from_millis(20),
                wait_put_per_call: Duration::from_millis(20),
                ..Default::default()
            },
        });
        let session = Arc::new(Session::default());

        let mut dataset = InsertBuilder::new("memory://")
            .with_params(&WriteParams {
                store_params: Some(ObjectStoreParams {
                    object_store_wrapper: Some(throttled.clone()),
                    ..Default::default()
                }),
                session: Some(session.clone()),
                ..Default::default()
            })
            .execute(vec![initial_data])
            .await
            .unwrap();

        // do merge inserts in parallel based on the concurrency. Each will open the dataset,
        // signal they have opened, and then wait for a signal to proceed. Once the signal
        // is received, they will do a merge insert and close the dataset.

        let barrier = Arc::new(Barrier::new(concurrency as usize));
        let mut handles = Vec::new();
        for i in 0..concurrency {
            let session_ref = session.clone();
            let schema_ref = schema.clone();
            let barrier_ref = barrier.clone();
            let throttled_ref = throttled.clone();
            let handle = tokio::task::spawn(async move {
                let dataset = DatasetBuilder::from_uri("memory://")
                    .with_read_params(ReadParams {
                        store_options: Some(ObjectStoreParams {
                            object_store_wrapper: Some(throttled_ref.clone()),
                            ..Default::default()
                        }),
                        session: Some(session_ref.clone()),
                        ..Default::default()
                    })
                    .load()
                    .await
                    .unwrap();
                let dataset = Arc::new(dataset);

                let new_data = RecordBatch::try_new(
                    schema_ref.clone(),
                    vec![
                        Arc::new(UInt32Array::from(vec![i])),
                        Arc::new(UInt32Array::from(vec![1])),
                    ],
                )
                .unwrap();
                let source = Box::new(RecordBatchIterator::new([Ok(new_data)], schema_ref.clone()));

                let job = MergeInsertBuilder::try_new(dataset, vec!["id".to_string()])
                    .unwrap()
                    .when_matched(WhenMatched::UpdateAll)
                    .when_not_matched(WhenNotMatched::InsertAll)
                    .conflict_retries(100)
                    .retry_timeout(timeout)
                    .try_build()
                    .unwrap();
                barrier_ref.wait().await;

                job.execute_reader(source)
                    .await
                    .map(|(_ds, stats)| stats.num_attempts)
            });
            handles.push(handle);
        }

        let results = try_join_all(handles).await.unwrap();

        for attempts in results.iter() {
            match attempts {
                Ok(attempts) => {
                    assert!(*attempts <= 10, "Attempt count should be <= 10");
                }
                Err(err) => {
                    // If we get an error, it means the task was cancelled
                    // due to timeout. This is expected if the timeout is
                    // set to a low value.
                    assert!(
                        matches!(err, Error::TooMuchWriteContention { message, .. } if message.contains("failed on retry_timeout")),
                        "Expected TooMuchWriteContention error, got: {:?}",
                        err
                    );
                }
            }
        }

        if timeout.as_secs() > 10 {
            dataset.checkout_latest().await.unwrap();
            let batches = dataset.scan().try_into_batch().await.unwrap();

            let values = batches["value"].as_primitive::<UInt32Type>();
            assert!(
                values.values().iter().all(|&v| v == 1),
                "All values should be 1 after merge insert. Got: {:?}",
                values
            );
        }
    }

    #[tokio::test]
    async fn test_merge_insert_large_concurrent() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::UInt32, false),
        ]));
        let num_rows = 10;
        let initial_data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from_iter_values(0..num_rows)),
                Arc::new(UInt32Array::from_iter_values(std::iter::repeat_n(
                    0,
                    num_rows as usize,
                ))),
            ],
        )
        .unwrap();

        // Adding latency helps ensure we get contention
        let throttled = Arc::new(ThrottledStoreWrapper {
            config: ThrottleConfig {
                wait_list_per_call: Duration::from_millis(10),
                wait_get_per_call: Duration::from_millis(10),
                ..Default::default()
            },
        });
        let session = Arc::new(Session::default());

        let dataset = InsertBuilder::new("memory://")
            .with_params(&WriteParams {
                store_params: Some(ObjectStoreParams {
                    object_store_wrapper: Some(throttled.clone()),
                    ..Default::default()
                }),
                session: Some(session.clone()),
                ..Default::default()
            })
            .execute(vec![initial_data])
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        // Start one merge insert, but don't commit it yet.
        let new_data1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![1])),
                Arc::new(UInt32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let UncommittedMergeInsert {
            transaction: transaction1,
            ..
        } = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute_uncommitted(RecordBatchIterator::new(
                vec![Ok(new_data1)],
                schema.clone(),
            ))
            .await
            .unwrap();

        // Setup a "large" merge insert, with many batches
        let new_data2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from_iter_values(0..1000)),
                Arc::new(UInt32Array::from_iter_values(std::iter::repeat_n(2, 1000))),
            ],
        )
        .unwrap();
        let notify = Arc::new(Notify::new());
        let source = RecordBatchIterator::new(
            (0..10)
                .map(|i| {
                    let batch = new_data2.slice(i * 100, 100);
                    if i == 9 {
                        notify.notify_one();
                    }
                    Ok(batch)
                })
                .collect::<Vec<_>>(),
            schema.clone(),
        );
        let dataset2 = DatasetBuilder::from_uri("memory://")
            .with_read_params(ReadParams {
                store_options: Some(ObjectStoreParams {
                    object_store_wrapper: Some(throttled.clone()),
                    ..Default::default()
                }),
                session: Some(session.clone()),
                ..Default::default()
            })
            .load()
            .await
            .unwrap();
        let job = MergeInsertBuilder::try_new(Arc::new(dataset2), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute_reader(source);
        let task = tokio::task::spawn(job);

        // Right as the large merge insert has finished reading the last batch,
        // we will commit the first merge insert. This should trigger a conflict,
        // but we should resolve it automatically.
        notify.notified().await;
        let mut dataset = CommitBuilder::new(dataset)
            .execute(transaction1)
            .await
            .unwrap();

        task.await.unwrap().unwrap();
        dataset.checkout_latest().await.unwrap();

        let batches = dataset.scan().try_into_batch().await.unwrap();
        let values = batches["value"].as_primitive::<UInt32Type>();
        assert!(
            values.values().iter().all(|&v| v == 2),
            "All values should be 1 after merge insert. Got: {:?}",
            values
        );
    }

    #[tokio::test]
    async fn test_merge_insert_updates_indices() {
        let test_dataset = async || {
            let mut dataset = lance_datagen::gen_batch()
                .col("id", array::step::<UInt32Type>())
                .col("value", array::step::<UInt32Type>())
                .col("other_value", array::step::<UInt32Type>())
                .into_ram_dataset(FragmentCount::from(4), FragmentRowCount::from(20))
                .await
                .unwrap();

            dataset
                .create_index(
                    &["id"],
                    IndexType::BTree,
                    None,
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();
            dataset
                .create_index(
                    &["value"],
                    IndexType::BTree,
                    None,
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();
            dataset
                .create_index(
                    &["other_value"],
                    IndexType::BTree,
                    None,
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();
            Arc::new(dataset)
        };

        let check_indices = async |dataset: &Dataset, id_frags: &[u32], value_frags: &[u32]| {
            let id_index = dataset
                .load_scalar_index(IndexCriteria::default().with_name("id_idx"))
                .await
                .unwrap();

            if id_frags.is_empty() {
                assert!(id_index.is_none());
            } else {
                let id_index = id_index.unwrap();
                let id_frags_bitmap = RoaringBitmap::from_iter(id_frags.iter().copied());
                // Check the effective bitmap (raw bitmap intersected with existing fragments)
                let effective_bitmap = id_index
                    .effective_fragment_bitmap(&dataset.fragment_bitmap)
                    .unwrap();
                assert_eq!(effective_bitmap, id_frags_bitmap);
            }

            let value_index = dataset
                .load_scalar_index(IndexCriteria::default().with_name("value_idx"))
                .await
                .unwrap();

            if value_frags.is_empty() {
                assert!(value_index.is_none());
            } else {
                let value_index = value_index.unwrap();
                let value_frags_bitmap = RoaringBitmap::from_iter(value_frags.iter().copied());
                // Check the effective bitmap (raw bitmap intersected with existing fragments)
                let effective_bitmap = value_index
                    .effective_fragment_bitmap(&dataset.fragment_bitmap)
                    .unwrap();
                assert_eq!(effective_bitmap, value_frags_bitmap);
            }

            let other_value_index = dataset
                .load_scalar_index(IndexCriteria::default().with_name("other_value_idx"))
                .await
                .unwrap()
                .unwrap();

            // The other_value index retains its original bitmap [0,1,2,3] since
            // partial merges that don't modify other_value won't prune it.
            let effective_bitmap = other_value_index
                .effective_fragment_bitmap(&dataset.fragment_bitmap)
                .unwrap();

            // The effective bitmap is the intersection of the index's original bitmap
            // and the current dataset fragments. Since other_value is not modified by
            // partial merges, it retains its validity for fragments it was originally trained on
            // that still exist in the dataset.
            let index_bitmap = other_value_index.fragment_bitmap.as_ref().unwrap();
            let expected_bitmap = index_bitmap & dataset.fragment_bitmap.as_ref();
            assert_eq!(
                effective_bitmap, expected_bitmap,
                "other_value index effective bitmap should be intersection. index_bitmap: {:?}, dataset_fragments: {:?}, effective_bitmap: {:?}",
                index_bitmap, dataset.fragment_bitmap, effective_bitmap
            );
        };

        let dataset = test_dataset().await;

        // Sanity test on the initial dataset
        check_indices(&dataset, &[0, 1, 2, 3], &[0, 1, 2, 3]).await;

        // Vertical merge insert (full schema), one fragment is deleted and should be removed from
        // the index.
        let merge_insert = MergeInsertBuilder::try_new(dataset, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();

        let (dataset, _) = merge_insert
            .execute_reader(
                lance_datagen::gen_batch()
                    .col("id", array::step_custom::<UInt32Type>(50, 1))
                    .col("value", array::step_custom::<UInt32Type>(50, 1))
                    .col("other_value", array::step_custom::<UInt32Type>(50, 1))
                    .into_df_stream(RowCount::from(40), BatchCount::from(1)),
            )
            .await
            .unwrap();

        // Fragment 3 removed and correctly removed from the index bitmap.
        check_indices(&dataset, &[0, 1, 2], &[0, 1, 2]).await;

        // Now we do the same thing with a partial merge insert (only id and value)
        let dataset = test_dataset().await;

        // Vertical merge insert (full schema), one fragment is deleted and should be removed from
        // the index.
        let merge_insert = MergeInsertBuilder::try_new(dataset, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();

        let (dataset, _) = merge_insert
            .execute_reader(
                lance_datagen::gen_batch()
                    .col("id", array::step_custom::<UInt32Type>(50, 1))
                    .col("value", array::step_custom::<UInt32Type>(50, 1))
                    .into_df_stream(RowCount::from(40), BatchCount::from(1)),
            )
            .await
            .unwrap();

        // Fragment 3 is fully removed.  We could keep it technically but today it is removed
        // which is also fine.  Fragment 2 is partially and must be removed.
        //
        // TODO: We should not be modifying the id_index here.  A merge_insert should not need
        // to rewrite the id field.  However, it seems we are doing that today.  This should be
        // fixed in
        check_indices(&dataset, &[0, 1], &[0, 1]).await;

        // One more test but this time we touch all fragments which causes the index to be removed
        // entirely.
        let dataset = test_dataset().await;

        // Vertical merge insert (full schema), one fragment is deleted and should be removed from
        // the index.
        let merge_insert = MergeInsertBuilder::try_new(dataset, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();

        let (dataset, _) = merge_insert
            .execute_reader(
                lance_datagen::gen_batch()
                    .col("id", array::step_custom::<UInt32Type>(10, 1))
                    .col("value", array::step_custom::<UInt32Type>(10, 1))
                    .into_df_stream(RowCount::from(80), BatchCount::from(1)),
            )
            .await
            .unwrap();

        check_indices(&dataset, &[], &[]).await;
    }

    #[tokio::test]
    async fn test_upsert_concurrent_full_frag() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::UInt32, false),
        ]));
        let initial_data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1])),
                Arc::new(UInt32Array::from(vec![0, 0])),
            ],
        )
        .unwrap();

        // Increase likelihood of contention by throttling the store
        let throttled = Arc::new(ThrottledStoreWrapper {
            config: ThrottleConfig {
                wait_list_per_call: Duration::from_millis(5),
                wait_get_per_call: Duration::from_millis(5),
                wait_put_per_call: Duration::from_millis(5),
                ..Default::default()
            },
        });
        let session = Arc::new(Session::default());

        let mut dataset = InsertBuilder::new("memory://")
            .with_params(&WriteParams {
                store_params: Some(ObjectStoreParams {
                    object_store_wrapper: Some(throttled.clone()),
                    ..Default::default()
                }),
                session: Some(session.clone()),
                ..Default::default()
            })
            .execute(vec![initial_data])
            .await
            .unwrap();

        // Each merge insert will update one row. Combined, they should delete
        // all rows in the first fragment, and it should be dropped.
        let barrier = Arc::new(Barrier::new(2));
        let mut handles = Vec::new();
        for i in 0..2 {
            let new_data = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt32Array::from(vec![i])),
                    Arc::new(UInt32Array::from(vec![1])),
                ],
            )
            .unwrap();
            let source = Box::new(RecordBatchIterator::new([Ok(new_data)], schema.clone()));

            let dataset_ref = Arc::new(dataset.clone());
            let barrier = barrier.clone();
            let handle = tokio::spawn(async move {
                barrier.wait().await;
                MergeInsertBuilder::try_new(dataset_ref, vec!["id".to_string()])
                    .unwrap()
                    .when_matched(WhenMatched::UpdateAll)
                    .when_not_matched(WhenNotMatched::InsertAll)
                    .try_build()
                    .unwrap()
                    .execute_reader(source)
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }
        try_join_all(handles).await.unwrap();

        dataset.checkout_latest().await.unwrap();
        assert!(
            dataset
                .get_fragments()
                .iter()
                .all(|f| f.metadata().num_rows().unwrap() > 0),
            "No fragments should have zero rows after upsert"
        );

        let batches = dataset.scan().try_into_batch().await.unwrap();
        let values = batches["value"].as_primitive::<UInt32Type>();
        assert!(
            values.values().iter().all(|&v| v == 1),
            "All values should be 1 after merge insert. Got: {:?}",
            values
        );
    }

    #[tokio::test]
    async fn test_plan_upsert() {
        let data = lance_datagen::gen_batch()
            .with_seed(Seed::from(1))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let data = data.into_reader_rows(RowCount::from(1024), BatchCount::from(32));
        let _schema = data.schema();

        // Create dataset with initial data
        let ds = Dataset::write(data, "memory://", None).await.unwrap();

        // Create upsert job
        let merge_insert_job =
            crate::dataset::MergeInsertBuilder::try_new(Arc::new(ds), vec!["key".to_string()])
                .unwrap()
                .when_matched(crate::dataset::WhenMatched::UpdateAll)
                .try_build()
                .unwrap();

        // Create new data for upsert
        let new_data = lance_datagen::gen_batch()
            .with_seed(Seed::from(2))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let new_data = new_data.into_reader_rows(RowCount::from(512), BatchCount::from(16));
        let new_data_stream = reader_to_stream(Box::new(new_data));

        let plan = merge_insert_job
            .create_plan(one_shot_provider(new_data_stream).unwrap())
            .await
            .unwrap();

        // Assert the plan structure using portable plan matching
        // The optimized plan should have:
        // 1. FullSchemaMergeInsertExec at the top
        // 2. ProjectionExec that creates action based on _rowaddr nullness (sentinel is constant
        //    true so DataFusion folds `sentinel IS NOT NULL` away from the CASE expression)
        // 3. HashJoin with projection that includes the sentinel column
        // 4. LanceScan that only reads the key column (projection pushdown working!)
        // 5. ProjectionExec on the source side that materializes the sentinel literal
        assert_plan_node_equals(
            plan,
            "MergeInsert: on=[key], when_matched=UpdateAll, when_not_matched=InsertAll, when_not_matched_by_source=Keep
  CoalescePartitionsExec
    ProjectionExec: expr=[_rowid@0 as _rowid, _rowaddr@1 as _rowaddr, value@2 as value, key@3 as key, __merge_source_sentinel@4 as __merge_source_sentinel, CASE WHEN _rowaddr@1 IS NULL THEN 2 WHEN _rowaddr@1 IS NOT NULL THEN 1 ELSE 0 END as __action]
      HashJoinExec: mode=CollectLeft, join_type=Right, on=[(key@0, key@1)], projection=[_rowid@1, _rowaddr@2, value@3, key@4, __merge_source_sentinel@5]
        LanceRead: uri=..., projection=[key], num_fragments=1, range_before=None, range_after=None, \
        row_id=true, row_addr=true, full_filter=--, refine_filter=--
        RepartitionExec: partitioning=RoundRobinBatch(...), input_partitions=1
          ProjectionExec: expr=[value@0 as value, key@1 as key, true as __merge_source_sentinel]
            StreamingTableExec: partition_sizes=1, projection=[value, key]"
        ).await.unwrap();
    }

    #[tokio::test]
    async fn test_fast_path_update_only() {
        let data = lance_datagen::gen_batch()
            .with_seed(Seed::from(1))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let data = data.into_reader_rows(RowCount::from(1024), BatchCount::from(32));

        // Create dataset with initial data
        let ds = Dataset::write(data, "memory://", None).await.unwrap();

        // Create update-only job (insert_not_matched = false)
        let merge_insert_job =
            crate::dataset::MergeInsertBuilder::try_new(Arc::new(ds), vec!["key".to_string()])
                .unwrap()
                .when_matched(crate::dataset::WhenMatched::UpdateAll)
                .when_not_matched(crate::dataset::WhenNotMatched::DoNothing)
                .try_build()
                .unwrap();

        // Create new data for update
        let new_data = lance_datagen::gen_batch()
            .with_seed(Seed::from(2))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let new_data = new_data.into_reader_rows(RowCount::from(512), BatchCount::from(16));
        let new_data_stream = reader_to_stream(Box::new(new_data));

        // This should use the fast path (execute_uncommitted_v2)
        let plan = merge_insert_job
            .create_plan(one_shot_provider(new_data_stream).unwrap())
            .await
            .unwrap();

        // The optimized plan should use Inner join instead of Right join since we're not
        // inserting unmatched rows.  The sentinel IS NOT NULL condition is folded away by
        // DataFusion because the sentinel is lit(true), so the CASE only checks _rowaddr.
        assert_plan_node_equals(
            plan,
            "MergeInsert: on=[key], when_matched=UpdateAll, when_not_matched=DoNothing, when_not_matched_by_source=Keep
  CoalescePartitionsExec
    ProjectionExec: expr=[_rowid@0 as _rowid, _rowaddr@1 as _rowaddr, value@2 as value, key@3 as key, __merge_source_sentinel@4 as __merge_source_sentinel, CASE WHEN _rowaddr@1 IS NOT NULL THEN 1 ELSE 0 END as __action]
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(key@0, key@1)], projection=[_rowid@1, _rowaddr@2, value@3, key@4, __merge_source_sentinel@5]
        LanceRead: uri=..., projection=[key], num_fragments=1, range_before=None, range_after=None, row_id=true, row_addr=true, full_filter=--, refine_filter=--
        RepartitionExec...
          ProjectionExec: expr=[value@0 as value, key@1 as key, true as __merge_source_sentinel]
            StreamingTableExec: partition_sizes=1, projection=[value, key]"
        ).await.unwrap();
    }

    #[tokio::test]
    async fn test_fast_path_conditional_update() {
        let data = lance_datagen::gen_batch()
            .with_seed(Seed::from(1))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let data = data.into_reader_rows(RowCount::from(1024), BatchCount::from(32));

        // Create dataset with initial data
        let ds = Dataset::write(data, "memory://", None).await.unwrap();

        // Create conditional update job (WhenMatched::UpdateIf)
        let merge_insert_job = crate::dataset::MergeInsertBuilder::try_new(
            Arc::new(ds.clone()),
            vec!["key".to_string()],
        )
        .unwrap()
        .when_matched(crate::dataset::WhenMatched::update_if(&ds, "source.value > 20").unwrap())
        .when_not_matched(crate::dataset::WhenNotMatched::DoNothing)
        .try_build()
        .unwrap();

        // Create new data for conditional update
        let new_data = lance_datagen::gen_batch()
            .with_seed(Seed::from(2))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let new_data_reader = new_data.into_reader_rows(RowCount::from(512), BatchCount::from(16));
        let new_data_stream = reader_to_stream(Box::new(new_data_reader));

        let plan = merge_insert_job
            .create_plan(one_shot_provider(new_data_stream).unwrap())
            .await
            .unwrap();

        // The optimized plan should use Inner join and include the UpdateIf condition.
        // The sentinel IS NOT NULL condition is folded away (sentinel is lit(true)).
        assert_plan_node_equals(
            plan,
            "MergeInsert: on=[key], when_matched=UpdateIf(source.value > 20), when_not_matched=DoNothing, when_not_matched_by_source=Keep
  CoalescePartitionsExec
    ProjectionExec: expr=[_rowid@0 as _rowid, _rowaddr@1 as _rowaddr, value@2 as value, key@3 as key, __merge_source_sentinel@4 as __merge_source_sentinel, CASE WHEN _rowaddr@1 IS NOT NULL AND value@2 > 20 THEN 1 ELSE 0 END as __action]
      HashJoinExec: mode=CollectLeft, join_type=Inner, on=[(key@0, key@1)], projection=[_rowid@1, _rowaddr@2, value@3, key@4, __merge_source_sentinel@5]
        LanceRead: uri=..., projection=[key], num_fragments=1, range_before=None, range_after=None, row_id=true, row_addr=true, full_filter=--, refine_filter=--
        RepartitionExec...
          ProjectionExec: expr=[value@0 as value, key@1 as key, true as __merge_source_sentinel]
            StreamingTableExec: partition_sizes=1, projection=[value, key]"
        ).await.unwrap();
    }

    /// Verifies that a default find-or-create merge insert
    /// (`WhenMatched::DoNothing` + `WhenNotMatched::InsertAll`) is routed
    /// through the v2 `FullSchemaMergeInsertExec` path. Prior to this
    /// change, `can_use_create_plan` rejected `DoNothing` outright and the
    /// operation fell back to the legacy v1 `Merger`; the assertion below
    /// would fail on `main`. See lance-format/lance#6441.
    #[tokio::test]
    async fn test_fast_path_find_or_create() {
        let data = lance_datagen::gen_batch()
            .with_seed(Seed::from(1))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let data = data.into_reader_rows(RowCount::from(1024), BatchCount::from(32));

        // Create dataset with initial data
        let ds = Dataset::write(data, "memory://", None).await.unwrap();

        // Default MergeInsertBuilder config is find-or-create:
        //   when_matched = DoNothing, when_not_matched = InsertAll.
        let merge_insert_job =
            crate::dataset::MergeInsertBuilder::try_new(Arc::new(ds), vec!["key".to_string()])
                .unwrap()
                .try_build()
                .unwrap();

        // Source data with a mix of already-present and new keys.
        let new_data = lance_datagen::gen_batch()
            .with_seed(Seed::from(2))
            .col("value", array::step::<UInt32Type>())
            .col("key", array::rand_pseudo_uuid_hex());
        let new_data = new_data.into_reader_rows(RowCount::from(512), BatchCount::from(16));
        let new_data_stream = reader_to_stream(Box::new(new_data));

        // Should reach the v2 fast path (`create_plan` + FullSchemaMergeInsertExec).
        // Dropping to v1 here would return an error from create_plan instead.
        let plan = merge_insert_job
            .create_plan(one_shot_provider(new_data_stream).unwrap())
            .await
            .unwrap();

        // The join is Right because we keep unmatched source rows (InsertAll)
        // but discard unmatched target rows (DoNothing on when_matched,
        // Keep on when_not_matched_by_source). The CASE expression simplifies
        // to `_rowaddr IS NULL → Insert, else Nothing`.
        assert_plan_node_equals(
            plan,
            "MergeInsert: on=[key], when_matched=DoNothing, when_not_matched=InsertAll, when_not_matched_by_source=Keep
  CoalescePartitionsExec
    ProjectionExec: expr=[_rowid@0 as _rowid, _rowaddr@1 as _rowaddr, value@2 as value, key@3 as key, __merge_source_sentinel@4 as __merge_source_sentinel, CASE WHEN _rowaddr@1 IS NULL THEN 2 ELSE 0 END as __action]
      HashJoinExec: mode=CollectLeft, join_type=Right, on=[(key@0, key@1)], projection=[_rowid@1, _rowaddr@2, value@3, key@4, __merge_source_sentinel@5]
        LanceRead: uri=..., projection=[key], num_fragments=1, range_before=None, range_after=None, row_id=true, row_addr=true, full_filter=--, refine_filter=--
        RepartitionExec...
          ProjectionExec: expr=[value@0 as value, key@1 as key, true as __merge_source_sentinel]
            StreamingTableExec: partition_sizes=1, projection=[value, key]"
        )
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn test_skip_auto_cleanup() {
        let tmpdir = TempStrDir::default();
        let dataset_uri = format!("{}/{}", tmpdir, "test_dataset");

        // Create initial dataset with auto cleanup interval of 1 version
        let data = lance_datagen::gen_batch()
            .with_seed(Seed::from(1))
            .col("id", array::step::<UInt32Type>())
            .into_reader_rows(RowCount::from(100), BatchCount::from(1));

        let mut auto_cleanup_params = HashMap::new();
        auto_cleanup_params.insert("lance.auto_cleanup.interval".to_string(), "1".to_string());
        auto_cleanup_params.insert(
            "lance.auto_cleanup.older_than".to_string(),
            "0ms".to_string(),
        );

        let write_params = WriteParams {
            mode: WriteMode::Create,
            auto_cleanup: Some(crate::dataset::AutoCleanupParams {
                interval: 1,
                older_than: chrono::TimeDelta::try_milliseconds(0).unwrap(),
            }),
            ..Default::default()
        };

        // Start at 1 second after epoch
        MockClock::set_system_time(std::time::Duration::from_secs(1));

        let dataset = Dataset::write(data, &dataset_uri, Some(write_params))
            .await
            .unwrap();
        assert_eq!(dataset.version().version, 1);

        // Advance time
        MockClock::set_system_time(std::time::Duration::from_secs(2));

        // First merge insert WITHOUT skip_auto_cleanup - should trigger cleanup
        let new_data = lance_datagen::gen_batch()
            .with_seed(Seed::from(2))
            .col("id", array::step::<UInt32Type>())
            .into_df_stream(RowCount::from(50), BatchCount::from(1));

        let (dataset2, _) = MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute(new_data)
            .await
            .unwrap();

        assert_eq!(dataset2.version().version, 2);

        // Advance time
        MockClock::set_system_time(std::time::Duration::from_secs(3));

        // Need to do another merge insert for cleanup to take effect since cleanup runs on the old dataset
        let new_data_extra = lance_datagen::gen_batch()
            .with_seed(Seed::from(4))
            .col("id", array::step::<UInt32Type>())
            .into_df_stream(RowCount::from(10), BatchCount::from(1));

        let (dataset2_extra, _) =
            MergeInsertBuilder::try_new(dataset2.clone(), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute(new_data_extra)
                .await
                .unwrap();

        assert_eq!(dataset2_extra.version().version, 3);

        // Load the dataset from disk to check versions
        let ds_check1 = DatasetBuilder::from_uri(&dataset_uri).load().await.unwrap();

        // Version 1 should be cleaned up due to auto cleanup (cleanup runs every version)
        assert!(
            ds_check1.checkout_version(1).await.is_err(),
            "Version 1 should have been cleaned up"
        );
        // Version 2 should still exist
        assert!(
            ds_check1.checkout_version(2).await.is_ok(),
            "Version 2 should still exist"
        );

        // Advance time
        MockClock::set_system_time(std::time::Duration::from_secs(4));

        // Second merge insert WITH skip_auto_cleanup - should NOT trigger cleanup
        let new_data2 = lance_datagen::gen_batch()
            .with_seed(Seed::from(3))
            .col("id", array::step::<UInt32Type>())
            .into_df_stream(RowCount::from(30), BatchCount::from(1));

        let (dataset3, _) = MergeInsertBuilder::try_new(dataset2_extra, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .skip_auto_cleanup(true) // Skip auto cleanup
            .try_build()
            .unwrap()
            .execute(new_data2)
            .await
            .unwrap();

        assert_eq!(dataset3.version().version, 4);

        // Load the dataset from disk to check versions
        let ds_check2 = DatasetBuilder::from_uri(&dataset_uri).load().await.unwrap();

        // Version 2 should still exist because skip_auto_cleanup was enabled
        assert!(
            ds_check2.checkout_version(2).await.is_ok(),
            "Version 2 should still exist because skip_auto_cleanup was enabled"
        );
        // Version 3 should also still exist
        assert!(
            ds_check2.checkout_version(3).await.is_ok(),
            "Version 3 should still exist"
        );
    }

    #[tokio::test]
    async fn test_transaction_inserted_rows_filter_roundtrip() {
        // Create dataset with unenforced primary key on "id" column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false).with_metadata(
                vec![(
                    "lance-schema:unenforced-primary-key".to_string(),
                    "true".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("value", DataType::UInt32, false),
        ]));
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1, 2])),
                Arc::new(UInt32Array::from(vec![0, 0, 0])),
            ],
        )
        .unwrap();
        let dataset = InsertBuilder::new("memory://")
            .execute(vec![initial])
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        // Source with overlapping key 1
        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![1, 3])),
                Arc::new(UInt32Array::from(vec![2, 2])),
            ],
        )
        .unwrap();
        let stream = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(new_batch)]),
        );

        let UncommittedMergeInsert { transaction, .. } =
            MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_uncommitted(Box::pin(stream) as SendableRecordBatchStream)
                .await
                .unwrap();

        // Commit and read back transaction file
        let committed = CommitBuilder::new(dataset.clone())
            .execute(transaction)
            .await
            .unwrap();
        let tx_path = committed.manifest().transaction_file.clone().unwrap();
        let tx_read = read_transaction_file(dataset.object_store.as_ref(), &dataset.base, &tx_path)
            .await
            .unwrap();
        // Check that inserted_rows_filter is present in the Operation::Update
        if let Operation::Update {
            inserted_rows_filter,
            ..
        } = &tx_read.operation
        {
            assert!(inserted_rows_filter.is_some());
            let filter = inserted_rows_filter.as_ref().unwrap();
            // Field IDs are assigned by Lance schema; check that we tracked exactly 1 key field
            assert_eq!(filter.field_ids.len(), 1);
        } else {
            panic!("Expected Operation::Update");
        }
    }

    /// Test that two merge insert operations on the same existing key conflict.
    /// First merge insert commits successfully, second one fails with conflict error
    /// because both operations updated the same key (detected via bloom filter).
    #[tokio::test]
    async fn test_inserted_rows_filter_bloom_conflict_detection_concurrent() {
        // Create schema with unenforced primary key on "id" column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false).with_metadata(
                vec![(
                    "lance-schema:unenforced-primary-key".to_string(),
                    "true".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("value", DataType::UInt32, false),
        ]));
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3])),
                Arc::new(UInt32Array::from(vec![0, 0, 0, 0])),
            ],
        )
        .unwrap();

        let dataset = InsertBuilder::new("memory://")
            .execute(vec![initial])
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        // Both jobs update/insert the same key 2
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![2])),
                Arc::new(UInt32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![2])),
                Arc::new(UInt32Array::from(vec![2])),
            ],
        )
        .unwrap();

        // Create second merge insert job based on version 1 with 0 retries
        let b2 = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .conflict_retries(0)
            .try_build()
            .unwrap();

        // First merge insert commits (creates version 2)
        let s1 = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch1.clone())]),
        );
        let b1 = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();
        let result1 = b1.execute(Box::pin(s1) as SendableRecordBatchStream).await;
        assert!(result1.is_ok(), "First merge insert should succeed");

        // Second merge insert tries to commit based on version 1, needs to rebase against version 2
        let s2 = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch2.clone())]),
        );
        let result2 = b2.execute(Box::pin(s2) as SendableRecordBatchStream).await;

        // Second merge insert should fail because bloom filters show both updated key 2
        assert!(
            matches!(result2, Err(crate::Error::TooMuchWriteContention { .. })),
            "Expected TooMuchWriteContention (retryable conflict exhausted), got: {:?}",
            result2
        );
    }

    /// Test that two merge insert operations inserting the same NEW key conflict.
    /// First merge insert commits successfully (inserts id=100), second one fails
    /// with conflict error because both inserted the same new key (detected via bloom filter).
    #[tokio::test]
    async fn test_concurrent_insert_same_new_key() {
        // Create schema with unenforced primary key on "id" column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false).with_metadata(
                vec![(
                    "lance-schema:unenforced-primary-key".to_string(),
                    "true".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("value", DataType::UInt32, false),
        ]));
        // Initial dataset with ids 0, 1, 2, 3 - NOT containing id=100
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3])),
                Arc::new(UInt32Array::from(vec![0, 0, 0, 0])),
            ],
        )
        .unwrap();

        let dataset = InsertBuilder::new("memory://")
            .execute(vec![initial])
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        // Both jobs try to INSERT the same NEW key id=100 (doesn't exist in initial data)
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![100])), // NEW key id=100
                Arc::new(UInt32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![100])), // Same NEW key id=100
                Arc::new(UInt32Array::from(vec![2])),
            ],
        )
        .unwrap();

        // Create second merge insert job based on version 1 with 0 retries
        let b2 = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .conflict_retries(0)
            .try_build()
            .unwrap();

        // First merge insert commits (creates version 2, inserts id=100)
        let s1 = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch1.clone())]),
        );
        let b1 = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();
        let result1 = b1.execute(Box::pin(s1) as SendableRecordBatchStream).await;
        assert!(result1.is_ok(), "First merge insert should succeed");

        // Second merge insert tries to commit based on version 1, needs to rebase against version 2
        let s2 = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch2.clone())]),
        );
        let result2 = b2.execute(Box::pin(s2) as SendableRecordBatchStream).await;

        // Second merge insert should fail because bloom filters show both inserted key 100
        assert!(
            matches!(result2, Err(crate::Error::TooMuchWriteContention { .. })),
            "Expected TooMuchWriteContention (retryable conflict exhausted), got: {:?}",
            result2
        );
    }

    /// Concurrency regression for lance-format/lance#6441: two concurrent
    /// find-or-create jobs (`WhenMatched::DoNothing` + `WhenNotMatched::InsertAll`)
    /// both try to insert the same fresh key. The second must fail with
    /// `TooMuchWriteContention` because the bloom-filter-backed
    /// `inserted_rows_filter` detects the overlap during rebase. Before
    /// routing find-or-create through v2 this did not work at all: the v1
    /// path returned `inserted_rows_filter=None`, so there was nothing to
    /// intersect against during conflict resolution.
    #[tokio::test]
    async fn test_concurrent_find_or_create_same_new_key() {
        // Schema with an unenforced primary key on "id" — that is what
        // activates bloom-filter conflict detection.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false).with_metadata(
                vec![(
                    "lance-schema:unenforced-primary-key".to_string(),
                    "true".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("value", DataType::UInt32, false),
        ]));
        // Initial dataset with ids 0..=3 — id=100 is not present.
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3])),
                Arc::new(UInt32Array::from(vec![0, 0, 0, 0])),
            ],
        )
        .unwrap();

        let dataset = InsertBuilder::new("memory://")
            .execute(vec![initial])
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        // Both jobs try to find-or-create the same new id=100.
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![100])),
                Arc::new(UInt32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![100])),
                Arc::new(UInt32Array::from(vec![2])),
            ],
        )
        .unwrap();

        // b2 is built against version 1 with zero retries, so when it needs
        // to rebase against b1's commit the bloom-filter intersection decides
        // the outcome directly.
        let b2 = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::DoNothing)
            .when_not_matched(WhenNotMatched::InsertAll)
            .conflict_retries(0)
            .try_build()
            .unwrap();

        // First job commits successfully, producing version 2 with id=100.
        let s1 = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch1.clone())]),
        );
        let b1 = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::DoNothing)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();
        let result1 = b1.execute(Box::pin(s1) as SendableRecordBatchStream).await;
        assert!(result1.is_ok(), "First find-or-create should succeed");

        // Second job fails because its inserted_rows_filter overlaps b1's.
        let s2 = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch2.clone())]),
        );
        let result2 = b2.execute(Box::pin(s2) as SendableRecordBatchStream).await;

        assert!(
            matches!(result2, Err(crate::Error::TooMuchWriteContention { .. })),
            "Expected TooMuchWriteContention (bloom-filter conflict) for find-or-create, got: {:?}",
            result2
        );
    }

    #[test]
    fn test_concurrent_insert_different_new_list_key() {
        // Schema for list(string) key column "tags".
        let tags_field = Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        );
        let schema = Arc::new(Schema::new(vec![tags_field]));

        // Build two batches inserting list key ["a", "b"] and ["c", "d"].
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.append_value(["a", "b"].iter().copied().map(Some));
        let tags_array1 = builder.finish();
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(tags_array1)]).unwrap();

        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.append_value(["c", "d"].iter().copied().map(Some));
        let tags_array2 = builder.finish();
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(tags_array2)]).unwrap();

        // Build bloom filters for the list keys.
        let field_ids = vec![0_i32];
        let mut builder1 = KeyExistenceFilterBuilder::new(field_ids.clone());
        let mut builder2 = KeyExistenceFilterBuilder::new(field_ids);

        let key1 = extract_key_value_from_batch(&batch1, 0, &[String::from("tags")])
            .expect("first batch should produce key");
        let key2 = extract_key_value_from_batch(&batch2, 0, &[String::from("tags")])
            .expect("second batch should produce key");

        builder1.insert(key1).unwrap();
        builder2.insert(key2).unwrap();
        let filter1 = KeyExistenceFilter::from_bloom_filter(&builder1);
        let filter2 = KeyExistenceFilter::from_bloom_filter(&builder2);

        let (has_intersection, might_be_fp) = filter1.intersects(&filter2).unwrap();
        assert!(
            !has_intersection,
            "Expected bloom filters not intersect for different list(string) keys",
        );
        assert!(
            !might_be_fp,
            "Bloom filter intersection should be definitively not conflict",
        );
    }

    #[test]
    fn test_concurrent_insert_same_new_list_key() {
        // Schema for list(string) key column "tags".
        let tags_field = Field::new(
            "tags",
            DataType::List(Arc::new(Field::new("item", DataType::Utf8, true))),
            false,
        );
        let schema = Arc::new(Schema::new(vec![tags_field]));

        // Build two batches both inserting the same list key ["a", "b"].
        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.append_value(["a", "b"].iter().copied().map(Some));
        let tags_array1 = builder.finish();
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(tags_array1)]).unwrap();

        let mut builder = ListBuilder::new(StringBuilder::new());
        builder.append_value(["a", "b"].iter().copied().map(Some));
        let tags_array2 = builder.finish();
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(tags_array2)]).unwrap();

        // Build bloom filters for the list key.
        let field_ids = vec![0_i32];
        let mut builder1 = KeyExistenceFilterBuilder::new(field_ids.clone());
        let mut builder2 = KeyExistenceFilterBuilder::new(field_ids);

        let key1 = extract_key_value_from_batch(&batch1, 0, &[String::from("tags")])
            .expect("first batch should produce key");
        let key2 = extract_key_value_from_batch(&batch2, 0, &[String::from("tags")])
            .expect("second batch should produce key");

        builder1.insert(key1).unwrap();
        builder2.insert(key2).unwrap();
        let filter1 = KeyExistenceFilter::from_bloom_filter(&builder1);
        let filter2 = KeyExistenceFilter::from_bloom_filter(&builder2);

        let (has_intersection, might_be_fp) = filter1.intersects(&filter2).unwrap();
        assert!(
            has_intersection,
            "Expected bloom filters to intersect for identical list(string) keys",
        );
        assert!(
            might_be_fp,
            "Bloom filter intersection should be treated as potential conflict",
        );
    }

    #[test]
    fn test_concurrent_insert_same_new_nested_list_key() {
        // Build nested list(list(string)) value [["a", "b"], ["c"]] for the "tags" column.
        let nested_tags = make_nested_array(&[["a", "b"].as_slice(), ["c"].as_slice()]);
        let tags_field = Field::new("tags", nested_tags.data_type().clone(), false);
        let nested_tags2 = make_nested_array(&[["a", "b"].as_slice(), ["c"].as_slice()]);

        let schema = Arc::new(Schema::new(vec![tags_field]));
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(nested_tags)]).unwrap();
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(nested_tags2)]).unwrap();

        // Build bloom filters for the nested list key.
        let field_ids = vec![0_i32];
        let mut builder1 = KeyExistenceFilterBuilder::new(field_ids.clone());
        let mut builder2 = KeyExistenceFilterBuilder::new(field_ids);

        let key1 = extract_key_value_from_batch(&batch1, 0, &[String::from("tags")])
            .expect("first batch should produce key");
        let key2 = extract_key_value_from_batch(&batch2, 0, &[String::from("tags")])
            .expect("second batch should produce key");

        builder1.insert(key1).unwrap();
        builder2.insert(key2).unwrap();
        let filter1 = KeyExistenceFilter::from_bloom_filter(&builder1);
        let filter2 = KeyExistenceFilter::from_bloom_filter(&builder2);

        let (has_intersection, might_be_fp) = filter1.intersects(&filter2).unwrap();
        assert!(
            has_intersection,
            "Expected bloom filters to intersect for identical nested list(list(string)) keys",
        );
        assert!(
            might_be_fp,
            "Bloom filter intersection should be treated as potential conflict",
        );
    }

    #[test]
    fn test_concurrent_insert_different_new_struct_key() {
        let user_field = Field::new(
            "user",
            DataType::Struct(
                vec![
                    Field::new("first", DataType::Utf8, false),
                    Field::new("last", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        );
        let schema = Arc::new(Schema::new(vec![user_field]));

        // Build two batches inserting different struct keys.
        let struct_array1 = make_struct_array_first_last_name(vec!["alice"], vec!["smith"]);
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array1)]).unwrap();

        let struct_array2 = make_struct_array_first_last_name(vec!["bob"], vec!["jones"]);
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(struct_array2)]).unwrap();

        // Build bloom filters for the struct key.
        let field_ids = vec![0_i32];
        let mut builder1 = KeyExistenceFilterBuilder::new(field_ids.clone());
        let mut builder2 = KeyExistenceFilterBuilder::new(field_ids);

        let key1 = extract_key_value_from_batch(&batch1, 0, &[String::from("user")])
            .expect("first batch should produce key");
        let key2 = extract_key_value_from_batch(&batch2, 0, &[String::from("user")])
            .expect("second batch should produce key");

        builder1.insert(key1).unwrap();
        builder2.insert(key2).unwrap();
        let filter1 = KeyExistenceFilter::from_bloom_filter(&builder1);
        let filter2 = KeyExistenceFilter::from_bloom_filter(&builder2);

        let (has_intersection, might_be_fp) = filter1.intersects(&filter2).unwrap();
        assert!(
            !has_intersection,
            "Expected bloom filters not intersect for different struct keys",
        );
        assert!(
            !might_be_fp,
            "Bloom filter intersection should be definitively not conflict",
        );
    }

    #[test]
    fn test_concurrent_insert_same_new_struct_key() {
        let user_field = Field::new(
            "user",
            DataType::Struct(
                vec![
                    Field::new("first", DataType::Utf8, false),
                    Field::new("last", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        );
        let schema = Arc::new(Schema::new(vec![user_field]));

        // Build two batches both inserting the same struct key {first: "alice", last: "smith"}.
        let struct_array1 = make_struct_array_first_last_name(vec!["alice"], vec!["smith"]);
        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(struct_array1)]).unwrap();

        let struct_array2 = make_struct_array_first_last_name(vec!["alice"], vec!["smith"]);
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(struct_array2)]).unwrap();

        // Build bloom filters for the struct key.
        let field_ids = vec![0_i32];
        let mut builder1 = KeyExistenceFilterBuilder::new(field_ids.clone());
        let mut builder2 = KeyExistenceFilterBuilder::new(field_ids);

        let key1 = extract_key_value_from_batch(&batch1, 0, &[String::from("user")])
            .expect("first batch should produce key");
        let key2 = extract_key_value_from_batch(&batch2, 0, &[String::from("user")])
            .expect("second batch should produce key");

        builder1.insert(key1).unwrap();
        builder2.insert(key2).unwrap();
        let filter1 = KeyExistenceFilter::from_bloom_filter(&builder1);
        let filter2 = KeyExistenceFilter::from_bloom_filter(&builder2);

        let (has_intersection, might_be_fp) = filter1.intersects(&filter2).unwrap();
        assert!(
            has_intersection,
            "Expected bloom filters to intersect for identical struct keys",
        );
        assert!(
            might_be_fp,
            "Bloom filter intersection should be treated as potential conflict",
        );
    }

    #[test]
    fn test_concurrent_insert_same_new_nested_struct_key() {
        // Build nested struct value {address: {city: "seattle", zip: 98101}} for the "user" column.
        let outer_struct = make_nested_struct_array_city_zip("seattle", 98101);
        let user_field = Field::new("user", outer_struct.data_type().clone(), false);
        let schema = Arc::new(Schema::new(vec![user_field]));

        let batch1 = RecordBatch::try_new(schema.clone(), vec![Arc::new(outer_struct)]).unwrap();

        let outer_struct2 = make_nested_struct_array_city_zip("seattle", 98101);
        let batch2 = RecordBatch::try_new(schema, vec![Arc::new(outer_struct2)]).unwrap();

        // Build bloom filters for the nested struct key.
        let field_ids = vec![0_i32];
        let mut builder1 = KeyExistenceFilterBuilder::new(field_ids.clone());
        let mut builder2 = KeyExistenceFilterBuilder::new(field_ids);

        let key1 = extract_key_value_from_batch(&batch1, 0, &[String::from("user")])
            .expect("first batch should produce key");
        let key2 = extract_key_value_from_batch(&batch2, 0, &[String::from("user")])
            .expect("second batch should produce key");

        builder1.insert(key1).unwrap();
        builder2.insert(key2).unwrap();
        let filter1 = KeyExistenceFilter::from_bloom_filter(&builder1);
        let filter2 = KeyExistenceFilter::from_bloom_filter(&builder2);

        let (has_intersection, might_be_fp) = filter1.intersects(&filter2).unwrap();
        assert!(
            has_intersection,
            "Expected bloom filters to intersect for identical nested struct keys",
        );
        assert!(
            might_be_fp,
            "Bloom filter intersection should be treated as potential conflict",
        );
    }

    /// End-to-end test for merge_insert using a struct-typed key column.
    #[tokio::test]
    async fn test_merge_insert_struct_key_upsert() {
        let user_field = Field::new(
            "user",
            DataType::Struct(
                vec![
                    Field::new("first", DataType::Utf8, false),
                    Field::new("last", DataType::Utf8, false),
                ]
                .into(),
            ),
            false,
        );
        let schema = Arc::new(Schema::new(vec![
            user_field,
            Field::new("value", DataType::UInt32, false),
        ]));

        // Initial dataset:
        // (alice, smith) -> 1
        // (bob, jones)  -> 1
        // (carla, doe)  -> 1
        let user_array = make_struct_array_first_last_name(
            vec!["alice", "bob", "carla"],
            vec!["smith", "jones", "doe"],
        );
        let values = UInt32Array::from(vec![1, 1, 1]);
        let initial_batch =
            RecordBatch::try_new(schema.clone(), vec![Arc::new(user_array), Arc::new(values)])
                .unwrap();

        let test_uri = "memory://test_merge_insert_struct_key.lance";
        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial_batch)], schema.clone()),
            test_uri,
            None,
        )
        .await
        .unwrap();
        let dataset = Arc::new(dataset);

        // New data: update alice, insert david
        let new_user_array =
            make_struct_array_first_last_name(vec!["alice", "david"], vec!["smith", "brown"]);
        let new_values = UInt32Array::from(vec![10, 2]);
        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(new_user_array), Arc::new(new_values)],
        )
        .unwrap();

        let reader = RecordBatchIterator::new([Ok(new_batch)], schema.clone());
        let (merged_ds, stats) = MergeInsertBuilder::try_new(dataset, vec!["user".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute(reader_to_stream(Box::new(reader)))
            .await
            .unwrap();

        assert_eq!(stats.num_updated_rows, 1);
        assert_eq!(stats.num_inserted_rows, 1);
        assert_eq!(stats.num_deleted_rows, 0);

        let result = merged_ds.scan().try_into_batch().await.unwrap();
        let user_col = result
            .column_by_name("user")
            .unwrap()
            .as_any()
            .downcast_ref::<StructArray>()
            .unwrap();
        let first = user_col
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let last = user_col
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let values = result
            .column_by_name("value")
            .unwrap()
            .as_primitive::<UInt32Type>();

        let mut rows = Vec::new();
        for i in 0..result.num_rows() {
            rows.push((
                first.value(i).to_string(),
                last.value(i).to_string(),
                values.value(i),
            ));
        }
        rows.sort();

        assert_eq!(
            rows,
            vec![
                ("alice".to_string(), "smith".to_string(), 10),
                ("bob".to_string(), "jones".to_string(), 1),
                ("carla".to_string(), "doe".to_string(), 1),
                ("david".to_string(), "brown".to_string(), 2),
            ],
        );
    }

    fn make_struct_array_first_last_name(first: Vec<&str>, last: Vec<&str>) -> StructArray {
        let first = StringArray::from(first);
        let last = StringArray::from(last);

        StructArray::from(vec![
            (
                Arc::new(Field::new("first", DataType::Utf8, false)),
                Arc::new(first) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("last", DataType::Utf8, false)),
                Arc::new(last) as Arc<dyn Array>,
            ),
        ])
    }

    fn make_nested_struct_array_city_zip(city: &str, zip: i32) -> StructArray {
        let city = StringArray::from(vec![city]);
        let zip = Int32Array::from(vec![zip]);

        let inner_struct = StructArray::from(vec![
            (
                Arc::new(Field::new("city", DataType::Utf8, false)),
                Arc::new(city) as Arc<dyn Array>,
            ),
            (
                Arc::new(Field::new("zip", DataType::Int32, false)),
                Arc::new(zip) as Arc<dyn Array>,
            ),
        ]);

        StructArray::from(vec![(
            Arc::new(Field::new(
                "address",
                inner_struct.data_type().clone(),
                false,
            )),
            Arc::new(inner_struct) as Arc<dyn Array>,
        )])
    }

    fn make_nested_array(inner_lists: &[&[&str]]) -> ListArray {
        let mut inner_builder = ListBuilder::new(StringBuilder::new());
        for inner in inner_lists {
            inner_builder.append_value(inner.iter().map(|s| Some(*s)));
        }
        let inner_list_array = inner_builder.finish();

        let offsets = ScalarBuffer::<i32>::from(vec![0, inner_list_array.len() as i32]);
        let offsets = OffsetBuffer::new(offsets);
        ListArray::new(
            Arc::new(Field::new(
                "item",
                inner_list_array.data_type().clone(),
                inner_list_array.nulls().is_some(),
            )),
            offsets,
            Arc::new(inner_list_array),
            None,
        )
    }

    /// Test that merge_insert with bloom filter fails when committing against
    /// an Update transaction that doesn't have a filter. We can't determine if
    /// the Update operation conflicted with our inserted rows.
    #[tokio::test]
    async fn test_merge_insert_conflict_with_update_without_filter() {
        use crate::dataset::UpdateBuilder;

        // Create schema with unenforced primary key on "id" column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false).with_metadata(
                vec![(
                    "lance-schema:unenforced-primary-key".to_string(),
                    "true".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("value", DataType::UInt32, false),
        ]));
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3])),
                Arc::new(UInt32Array::from(vec![0, 0, 0, 0])),
            ],
        )
        .unwrap();

        let dataset = InsertBuilder::new("memory://")
            .execute(vec![initial])
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        // Create merge insert job based on version 1
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![100])),
                Arc::new(UInt32Array::from(vec![1])),
            ],
        )
        .unwrap();

        let b1 = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .conflict_retries(0)
            .try_build()
            .unwrap();

        // Regular Update without bloom filter commits first (creates version 2)
        let update_result = UpdateBuilder::new(dataset.clone())
            .update_where("id = 0")
            .unwrap()
            .set("value", "999")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await;
        assert!(update_result.is_ok(), "Update should succeed");

        // Now merge insert tries to commit based on version 1, needs to rebase against version 2
        let s1 = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch1.clone())]),
        );
        let merge_result = b1.execute(Box::pin(s1) as SendableRecordBatchStream).await;

        // Merge insert should fail with retryable conflict because it can't
        // determine if Update conflicted (Update has no inserted_rows_filter)
        assert!(
            matches!(
                merge_result,
                Err(crate::Error::TooMuchWriteContention { .. })
            ),
            "Expected TooMuchWriteContention (retryable conflict exhausted), got: {:?}",
            merge_result
        );
    }

    /// Test that merge_insert with bloom filter fails when committing against
    /// an Append operation. We can't determine if the appended rows conflict
    /// with our inserted rows.
    #[tokio::test]
    async fn test_merge_insert_conflict_with_append() {
        // Create schema with unenforced primary key on "id" column
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false).with_metadata(
                vec![(
                    "lance-schema:unenforced-primary-key".to_string(),
                    "true".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("value", DataType::UInt32, false),
        ]));
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3])),
                Arc::new(UInt32Array::from(vec![0, 0, 0, 0])),
            ],
        )
        .unwrap();

        let dataset = InsertBuilder::new("memory://")
            .execute(vec![initial])
            .await
            .unwrap();
        let dataset = Arc::new(dataset);

        // Create merge insert job based on version 1
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![100])),
                Arc::new(UInt32Array::from(vec![1])),
            ],
        )
        .unwrap();

        let b1 = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .conflict_retries(0)
            .try_build()
            .unwrap();

        // Append commits first (creates version 2)
        let append_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![50])),
                Arc::new(UInt32Array::from(vec![2])),
            ],
        )
        .unwrap();
        let append_result = InsertBuilder::new(dataset.clone())
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![append_batch])
            .await;
        assert!(append_result.is_ok(), "Append should succeed");

        // Now merge insert tries to commit based on version 1, needs to rebase against version 2
        let s1 = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(batch1.clone())]),
        );
        let merge_result = b1.execute(Box::pin(s1) as SendableRecordBatchStream).await;

        // Merge insert should fail with retryable conflict because it can't
        // determine if Append added conflicting keys
        assert!(
            matches!(
                merge_result,
                Err(crate::Error::TooMuchWriteContention { .. })
            ),
            "Expected TooMuchWriteContention (retryable conflict exhausted), got: {:?}",
            merge_result
        );
    }

    #[tokio::test]
    async fn test_explain_plan() {
        // Set up test data using lance_datagen
        let dataset = lance_datagen::gen_batch()
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col("name", array::cycle_utf8_literals(&["a", "b", "c"]))
            .into_ram_dataset(FragmentCount::from(1), FragmentRowCount::from(3))
            .await
            .unwrap();

        // Create merge insert job
        let merge_insert_job =
            MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap();

        // Test explain_plan with default schema (None)
        let plan = merge_insert_job.explain_plan(None, false).await.unwrap();

        // Also validate the full string structure with pattern matching
        let expected_pattern = "\
MergeInsert: on=[id], when_matched=UpdateAll, when_not_matched=InsertAll, when_not_matched_by_source=Keep...
  CoalescePartitionsExec...
    HashJoinExec...
      LanceRead...
      StreamingTableExec: partition_sizes=1, projection=[id, name]";
        assert_string_matches(&plan, expected_pattern).unwrap();

        // Test with explicit schema
        let source_schema = arrow_schema::Schema::from(dataset.schema());
        let explicit_plan = merge_insert_job
            .explain_plan(Some(&source_schema), false)
            .await
            .unwrap();
        assert_eq!(plan, explicit_plan); // Should be the same as default

        // Test verbose mode produces different (likely longer) output
        let verbose_plan = merge_insert_job.explain_plan(None, true).await.unwrap();
        assert!(verbose_plan.contains("MergeInsert"));
        // Verbose should also match the expected pattern
        assert_string_matches(&verbose_plan, expected_pattern).unwrap();
    }

    /// Asserts that `explain_plan()` is supported for a default find-or-create
    /// configuration (`WhenMatched::DoNothing` + `WhenNotMatched::InsertAll`).
    /// Before lance-format/lance#6441 this returned `Error::NotSupported`
    /// because the job fell back to the legacy v1 path.
    #[tokio::test]
    async fn test_explain_plan_find_or_create() {
        let dataset = lance_datagen::gen_batch()
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col("name", array::cycle_utf8_literals(&["a", "b", "c"]))
            .into_ram_dataset(FragmentCount::from(1), FragmentRowCount::from(3))
            .await
            .unwrap();

        // Default builder config == find-or-create.
        let merge_insert_job =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
                .unwrap()
                .try_build()
                .unwrap();

        let plan = merge_insert_job.explain_plan(None, false).await.unwrap();

        let expected_pattern = "\
MergeInsert: on=[id], when_matched=DoNothing, when_not_matched=InsertAll, when_not_matched_by_source=Keep...
  CoalescePartitionsExec...
    HashJoinExec...join_type=Right...
      LanceRead...
      StreamingTableExec: partition_sizes=1, projection=[id, name]";
        assert_string_matches(&plan, expected_pattern).unwrap();
    }

    #[tokio::test]
    async fn test_explain_plan_full_schema_delete_by_source_with_fsl() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
        ]));

        let dataset_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        Float32Array::from(vec![
                            1.0, 1.1, 1.2, 1.3, 2.0, 2.1, 2.2, 2.3, 3.0, 3.1, 3.2, 3.3,
                        ]),
                        4,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let dataset = Dataset::write(
            Box::new(RecordBatchIterator::new(
                [Ok(dataset_batch)],
                schema.clone(),
            )),
            "memory://test_explain_plan_full_schema_delete_by_source_with_fsl",
            None,
        )
        .await
        .unwrap();

        let merge_insert_job =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
                .use_index(false)
                .try_build()
                .unwrap();

        let plan = merge_insert_job.explain_plan(None, false).await.unwrap();
        assert!(plan.contains("HashJoinExec"));
        assert!(plan.contains("join_type=Full"));
        assert!(
            plan.lines().any(|line| line.contains("HashJoinExec")
                && line.contains("projection=[")
                && line.contains("_rowid")),
            "join should push down a projection that retains _rowid: {plan}"
        );
        assert!(
            plan.contains("LanceRead: uri=") && plan.contains("projection=[id]"),
            "target-side scan should prune the FSL payload from the join build side: {plan}"
        );
        assert!(
            !plan.contains("LanceRead: uri=test_explain_plan_full_schema_delete_by_source_with_fsl/data, projection=[id, vec]"),
            "target-side scan should not include the FSL payload in the join build side: {plan}"
        );
    }

    #[tokio::test]
    async fn test_explain_plan_full_schema_delete_by_source_with_fsl_and_scalar_index() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
        ]));

        let dataset_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        Float32Array::from(vec![
                            1.0, 1.1, 1.2, 1.3, 2.0, 2.1, 2.2, 2.3, 3.0, 3.1, 3.2, 3.3,
                        ]),
                        4,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let mut dataset = Dataset::write(
            Box::new(RecordBatchIterator::new(
                [Ok(dataset_batch)],
                schema.clone(),
            )),
            "memory://test_explain_plan_full_schema_delete_by_source_with_fsl_and_scalar_index",
            None,
        )
        .await
        .unwrap();

        let scalar_params = ScalarIndexParams::default();
        dataset
            .create_index(&["id"], IndexType::Scalar, None, &scalar_params, false)
            .await
            .unwrap();

        let merge_insert_job =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
                .try_build()
                .unwrap();

        let plan = merge_insert_job.explain_plan(None, false).await.unwrap();
        assert!(plan.contains("HashJoinExec"));
        assert!(plan.contains("join_type=Full"));
        assert!(
            plan.lines().any(|line| line.contains("HashJoinExec")
                && line.contains("projection=[")
                && line.contains("_rowid")),
            "join should push down a projection that retains _rowid: {plan}"
        );
        assert!(
            plan.contains("LanceRead: uri=") && plan.contains("projection=[id]"),
            "target-side scan should prune the FSL payload from the join build side even when a scalar index exists: {plan}"
        );
        assert!(
            !plan.contains(
                "LanceRead: uri=test_explain_plan_full_schema_delete_by_source_with_fsl_and_scalar_index/data, projection=[id, vec]"
            ),
            "target-side scan should not include the FSL payload in the join build side: {plan}"
        );
    }

    #[tokio::test]
    async fn test_merge_insert_full_schema_delete_by_source_with_fsl() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
        ]));

        let dataset_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        Float32Array::from(vec![
                            1.0, 1.1, 1.2, 1.3, 2.0, 2.1, 2.2, 2.3, 3.0, 3.1, 3.2, 3.3,
                        ]),
                        4,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let dataset = Dataset::write(
            Box::new(RecordBatchIterator::new(
                [Ok(dataset_batch)],
                schema.clone(),
            )),
            "memory://test_merge_insert_full_schema_delete_by_source_with_fsl",
            None,
        )
        .await
        .unwrap();

        let source_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        Float32Array::from(vec![20.0, 20.1, 20.2, 20.3, 40.0, 40.1, 40.2, 40.3]),
                        4,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let (merged_dataset, stats) =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    [Ok(source_batch)],
                    schema.clone(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_deleted_rows, 2);
        assert_eq!(stats.num_updated_rows, 1);
        assert_eq!(stats.num_inserted_rows, 1);

        let merged = merged_dataset.scan().try_into_batch().await.unwrap();
        let ids = merged["id"].as_primitive::<Int32Type>().values().to_vec();
        assert_eq!(ids, vec![2, 4]);

        let vecs = merged["vec"].as_fixed_size_list();
        let actual = vecs
            .values()
            .as_primitive::<Float32Type>()
            .values()
            .to_vec();
        assert_eq!(actual, vec![20.0, 20.1, 20.2, 20.3, 40.0, 40.1, 40.2, 40.3]);
    }

    #[tokio::test]
    async fn test_merge_insert_full_schema_delete_by_source_with_fsl_and_scalar_index() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
        ]));

        let dataset_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        Float32Array::from(vec![
                            1.0, 1.1, 1.2, 1.3, 2.0, 2.1, 2.2, 2.3, 3.0, 3.1, 3.2, 3.3,
                        ]),
                        4,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let mut dataset = Dataset::write(
            Box::new(RecordBatchIterator::new(
                [Ok(dataset_batch)],
                schema.clone(),
            )),
            "memory://test_merge_insert_full_schema_delete_by_source_with_fsl_and_scalar_index",
            None,
        )
        .await
        .unwrap();

        let scalar_params = ScalarIndexParams::default();
        dataset
            .create_index(&["id"], IndexType::Scalar, None, &scalar_params, false)
            .await
            .unwrap();

        let source_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        Float32Array::from(vec![20.0, 20.1, 20.2, 20.3, 40.0, 40.1, 40.2, 40.3]),
                        4,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let (merged_dataset, stats) =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .when_not_matched_by_source(WhenNotMatchedBySource::Delete)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    [Ok(source_batch)],
                    schema.clone(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_deleted_rows, 2);
        assert_eq!(stats.num_updated_rows, 1);
        assert_eq!(stats.num_inserted_rows, 1);

        let merged = merged_dataset.scan().try_into_batch().await.unwrap();
        let ids = merged["id"].as_primitive::<Int32Type>().values().to_vec();
        assert_eq!(ids, vec![2, 4]);

        let vecs = merged["vec"].as_fixed_size_list();
        let actual = vecs
            .values()
            .as_primitive::<Float32Type>()
            .values()
            .to_vec();
        assert_eq!(actual, vec![20.0, 20.1, 20.2, 20.3, 40.0, 40.1, 40.2, 40.3]);
    }

    #[tokio::test]
    async fn test_analyze_plan() {
        // Set up test data using lance_datagen
        let mut dataset = lance_datagen::gen_batch()
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col("name", array::cycle_utf8_literals(&["a", "b", "c"]))
            .into_ram_dataset(FragmentCount::from(1), FragmentRowCount::from(3))
            .await
            .unwrap();

        // Capture the original version before analyze_plan
        let original_version = dataset.version().version;

        // Create merge insert job
        let merge_insert_job =
            MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap();

        // Create source data stream with exact same schema
        let schema = Arc::new(arrow_schema::Schema::from(dataset.schema()));
        let source_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 4])), // 1 matches, 4 is new
                Arc::new(StringArray::from(vec!["updated_a", "d"])),
            ],
        )
        .unwrap();

        let source_stream = RecordBatchStreamAdapter::new(
            schema,
            futures::stream::once(async { Ok(source_batch) }).boxed(),
        );

        // Test analyze_plan. We enclose the analysis output string in brackets to make it easier
        // to use assert_string_matches.  (That function requires a known string at the beginning
        // and end.)
        let mut analysis = String::from("[");
        analysis.push_str(
            &merge_insert_job
                .analyze_plan(Box::pin(source_stream))
                .await
                .unwrap(),
        );
        analysis.push_str(&String::from("]"));

        // Verify the analysis contains expected components
        assert!(analysis.contains("MergeInsert"));
        assert!(analysis.contains("metrics"));
        // Note: AnalyzeExec is no longer in the output

        // Should show execution metrics including new write metrics
        assert!(analysis.contains("bytes_written"));
        assert!(analysis.contains("num_files_written"));

        // IMPORTANT: Verify that no new version was created
        // analyze_plan should not commit the transaction
        dataset.checkout_latest().await.unwrap();
        assert_eq!(
            dataset.version().version,
            original_version,
            "analyze_plan should not create a new dataset version"
        );

        // Also validate the full string structure with pattern matching
        let expected_pattern = "[...MergeInsert: elapsed=..., on=[id], when_matched=UpdateAll, when_not_matched=InsertAll, when_not_matched_by_source=Keep, metrics=...bytes_written=...num_deleted_rows=0, num_files_written=...num_inserted_rows=1, num_skipped_duplicates=0, num_updated_rows=1]
    ...
    StreamingTableExec: partition_sizes=1, projection=[id, name], metrics=[]...]";
        assert_string_matches(&analysis, expected_pattern).unwrap();
        assert!(analysis.contains("bytes_written"));
        assert!(analysis.contains("num_files_written"));
        assert!(analysis.contains("elapsed_compute"));
    }

    #[tokio::test]
    async fn test_merge_insert_with_action_column() {
        // Test that merge_insert works when the user has a column named "action"
        // This reproduces issue #4498

        // Create a dataset with an "action" column
        let initial_data = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
                arrow_schema::Field::new("action", arrow_schema::DataType::Utf8, true),
                arrow_schema::Field::new("value", arrow_schema::DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["create", "update", "delete"])),
                Arc::new(Int32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let tempdir = TempStrDir::default();
        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial_data.clone())], initial_data.schema()),
            &tempdir,
            None,
        )
        .await
        .unwrap();

        // Create new data for merge with matching "action" column
        let new_data = RecordBatch::try_new(
            Arc::new(arrow_schema::Schema::new(vec![
                arrow_schema::Field::new("id", arrow_schema::DataType::Int32, false),
                arrow_schema::Field::new("action", arrow_schema::DataType::Utf8, true),
                arrow_schema::Field::new("value", arrow_schema::DataType::Int32, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec!["modify", "insert"])),
                Arc::new(Int32Array::from(vec![25, 40])),
            ],
        )
        .unwrap();

        // Perform merge insert - this should work despite having "action" column
        let merge_insert_job =
            MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap();

        let new_reader = Box::new(RecordBatchIterator::new(
            [Ok(new_data.clone())],
            new_data.schema(),
        ));
        let new_stream = reader_to_stream(new_reader);

        let (merged_dataset, _) = merge_insert_job.execute(new_stream).await.unwrap();

        // Verify the merge worked correctly
        let result_batches = merged_dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let result_batch = concat_batches(&result_batches[0].schema(), &result_batches).unwrap();

        // Should have 4 rows: 1 (unchanged), 2 (updated), 3 (unchanged), 4 (inserted)
        assert_eq!(result_batch.num_rows(), 4);

        // Verify the "action" column values are preserved correctly
        let id_col = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let action_col = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let value_col = result_batch
            .column(2)
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();

        // Find each row by ID and verify
        for i in 0..result_batch.num_rows() {
            match id_col.value(i) {
                1 => {
                    assert_eq!(action_col.value(i), "create");
                    assert_eq!(value_col.value(i), 10);
                }
                2 => {
                    assert_eq!(action_col.value(i), "modify"); // Updated
                    assert_eq!(value_col.value(i), 25); // Updated
                }
                3 => {
                    assert_eq!(action_col.value(i), "delete");
                    assert_eq!(value_col.value(i), 30);
                }
                4 => {
                    assert_eq!(action_col.value(i), "insert"); // New row
                    assert_eq!(value_col.value(i), 40); // New row
                }
                _ => panic!("Unexpected id: {}", id_col.value(i)),
            }
        }
    }

    #[tokio::test]
    #[rstest::rstest]
    async fn test_duplicate_rowid_detection(
        #[values(false, true)] is_full_schema: bool,
        #[values(true, false)] enable_stable_row_ids: bool,
        #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1, LanceFileVersion::V2_2)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_uri = "memory://test_duplicate_rowid_multi_fragment.lance";

        // Create initial dataset with multiple fragments to test cross-fragment duplicate detection
        let dataset = lance_datagen::gen_batch()
            .col("key", array::step_custom::<UInt32Type>(1, 1))
            .col("value", array::step_custom::<UInt32Type>(10, 10))
            .into_dataset_with_params(
                test_uri,
                FragmentCount(3),
                FragmentRowCount(4),
                Some(WriteParams {
                    max_rows_per_file: 4,
                    enable_stable_row_ids,
                    data_storage_version: Some(data_storage_version),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        assert_eq!(dataset.get_fragments().len(), 3, "Should have 3 fragments");

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt32, is_full_schema),
            Field::new("value", DataType::UInt32, is_full_schema),
        ]));

        let source_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![2, 2, 6, 6, 10, 10, 15])),
                Arc::new(UInt32Array::from(vec![100, 200, 300, 400, 500, 600, 700])),
            ],
        )
        .unwrap();

        let job = MergeInsertBuilder::try_new(Arc::new(dataset), vec!["key".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .try_build()
            .unwrap();

        let reader = Box::new(RecordBatchIterator::new([Ok(source_batch)], schema.clone()));
        let stream = reader_to_stream(reader);

        let result = job.execute(stream).await;

        assert!(
            result.is_err(),
            "Expected merge insert to fail due to duplicate rows on key column."
        );

        assert!(
            matches!(&result, &Err(Error::InvalidInput { ref source, .. }) if source.to_string().contains("Ambiguous merge insert") && source.to_string().contains("multiple source rows")),
            "Expected error to be InvalidInput with message about ambiguous merge insert and multiple source rows, got: {:?}",
            result
        );
    }

    #[tokio::test]
    #[rstest::rstest]
    async fn test_source_dedupe_behavior_first_seen(
        #[values(false, true)] is_full_schema: bool,
        #[values(true, false)] enable_stable_row_ids: bool,
        #[values(LanceFileVersion::V2_0, LanceFileVersion::V2_1, LanceFileVersion::V2_2)]
        data_storage_version: LanceFileVersion,
    ) {
        let test_uri = format!(
            "memory://test_dedupe_first_seen_{}_{}.lance",
            is_full_schema, enable_stable_row_ids
        );

        // Create initial dataset with keys 1, 2, 3, 4
        let dataset = lance_datagen::gen_batch()
            .col("key", array::step_custom::<UInt32Type>(1, 1))
            .col("value", array::step_custom::<UInt32Type>(10, 10))
            .into_dataset_with_params(
                &test_uri,
                FragmentCount(1),
                FragmentRowCount(4),
                Some(WriteParams {
                    max_rows_per_file: 4,
                    enable_stable_row_ids,
                    data_storage_version: Some(data_storage_version),
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        // Initial data: key=1,value=10; key=2,value=20; key=3,value=30; key=4,value=40
        let initial_data: Vec<(u32, u32)> = dataset
            .scan()
            .try_into_batch()
            .await
            .unwrap()
            .columns()
            .iter()
            .map(|c| c.as_primitive::<UInt32Type>().values().to_vec())
            .collect::<Vec<_>>()
            .into_iter()
            .fold(Vec::new(), |mut acc, vals| {
                if acc.is_empty() {
                    acc = vals.into_iter().map(|v| (v, 0)).collect();
                } else {
                    for (i, v) in vals.into_iter().enumerate() {
                        acc[i].1 = v;
                    }
                }
                acc
            });
        assert_eq!(
            initial_data,
            vec![(1, 10), (2, 20), (3, 30), (4, 40)],
            "Initial data should be correct"
        );

        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt32, is_full_schema),
            Field::new("value", DataType::UInt32, is_full_schema),
        ]));

        // Source data with duplicates:
        // - key=2 appears 3 times with values 100, 200, 300 (first seen: 100)
        // - key=3 appears 2 times with values 400, 500 (first seen: 400)
        // - key=5 is a new insert (value=600)
        // Total duplicates: 3 (2 extra for key=2, 1 extra for key=3)
        let source_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![2, 2, 2, 3, 3, 5])),
                Arc::new(UInt32Array::from(vec![100, 200, 300, 400, 500, 600])),
            ],
        )
        .unwrap();

        let job = MergeInsertBuilder::try_new(Arc::new(dataset), vec!["key".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .source_dedupe_behavior(SourceDedupeBehavior::FirstSeen)
            .try_build()
            .unwrap();

        let reader = Box::new(RecordBatchIterator::new([Ok(source_batch)], schema.clone()));
        let stream = reader_to_stream(reader);

        let (dataset, stats) = job.execute(stream).await.unwrap();

        // Verify stats
        assert_eq!(
            stats.num_skipped_duplicates, 3,
            "Should have skipped 3 duplicate rows (2 extra for key=2, 1 extra for key=3)"
        );
        assert_eq!(
            stats.num_updated_rows, 2,
            "Should have updated 2 rows (key=2 and key=3)"
        );
        assert_eq!(
            stats.num_inserted_rows, 1,
            "Should have inserted 1 row (key=5)"
        );

        // Verify the actual data - first seen values should be kept
        let result_batch = dataset.scan().try_into_batch().await.unwrap();
        let keys = result_batch.column(0).as_primitive::<UInt32Type>();
        let values = result_batch.column(1).as_primitive::<UInt32Type>();

        let result_data: std::collections::HashMap<u32, u32> = keys
            .values()
            .iter()
            .zip(values.values().iter())
            .map(|(&k, &v)| (k, v))
            .collect();

        assert_eq!(result_data.len(), 5, "Should have 5 rows total");
        assert_eq!(
            result_data.get(&1),
            Some(&10),
            "key=1 should be unchanged (original value)"
        );
        assert_eq!(
            result_data.get(&2),
            Some(&100),
            "key=2 should have first seen value (100, not 200 or 300)"
        );
        assert_eq!(
            result_data.get(&3),
            Some(&400),
            "key=3 should have first seen value (400, not 500)"
        );
        assert_eq!(
            result_data.get(&4),
            Some(&40),
            "key=4 should be unchanged (original value)"
        );
        assert_eq!(
            result_data.get(&5),
            Some(&600),
            "key=5 should be inserted with value 600"
        );
    }

    #[rstest::rstest]
    #[case::v2(false)]
    #[case::indexed_scan(true)]
    #[tokio::test]
    async fn test_first_seen_dedupes_unmatched_source_rows(#[case] with_index: bool) {
        let initial =
            record_batch!(("id", Int32, [Some(1)]), ("value", Int32, [Some(10)])).unwrap();
        let initial = if with_index {
            initial
        } else {
            // Match the reported failure's empty-target setup on the v2 path.
            initial.slice(0, 0)
        };
        let mut dataset = Dataset::write(
            RecordBatchIterator::new([Ok(initial.clone())], initial.schema()),
            "memory://",
            None,
        )
        .await
        .unwrap();

        if with_index {
            dataset
                .create_index(
                    &["id"],
                    IndexType::Scalar,
                    None,
                    &ScalarIndexParams::default(),
                    false,
                )
                .await
                .unwrap();
        }

        // Split distinct duplicate values across batches to verify that FirstSeen
        // preserves source order across the entire stream. On the v2 path, also
        // verify that NULL keys remain distinct under SQL equality.
        let (first, second, expected_inserted) = if with_index {
            (
                record_batch!(("id", Int32, [Some(108)]), ("value", Int32, [Some(1)])).unwrap(),
                record_batch!(("id", Int32, [Some(108)]), ("value", Int32, [Some(2)])).unwrap(),
                1,
            )
        } else {
            (
                record_batch!(
                    ("id", Int32, [Some(108), None]),
                    ("value", Int32, [Some(1), Some(3)])
                )
                .unwrap(),
                record_batch!(
                    ("id", Int32, [Some(108), None]),
                    ("value", Int32, [Some(2), Some(4)])
                )
                .unwrap(),
                3,
            )
        };

        let (dataset, stats) =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .source_dedupe_behavior(SourceDedupeBehavior::FirstSeen)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    [Ok(first.clone()), Ok(second)],
                    first.schema(),
                )))
                .await
                .unwrap();

        assert_eq!(stats.num_inserted_rows, expected_inserted);
        assert_eq!(stats.num_updated_rows, 0);
        assert_eq!(stats.num_skipped_duplicates, 1);

        let inserted = dataset
            .scan()
            .filter("id = 108")
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();
        assert_eq!(inserted.num_rows(), 1);
        assert_eq!(inserted["value"].as_primitive::<Int32Type>().value(0), 1);
    }

    #[tokio::test]
    async fn test_merge_insert_use_index() {
        let data = lance_datagen::gen_batch()
            .col("id", lance_datagen::array::step::<Int32Type>())
            .col("value", array::step::<UInt32Type>());
        let data = data.into_reader_rows(RowCount::from(100), BatchCount::from(1));
        let schema = data.schema();
        let mut ds = Dataset::write(data, "memory://", None).await.unwrap();

        // Create a scalar index on id column
        let index_params = ScalarIndexParams::default();
        ds.create_index(&["id"], IndexType::Scalar, None, &index_params, false)
            .await
            .unwrap();

        let source_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 101])), // Two matches, one new
                Arc::new(UInt32Array::from(vec![999, 999, 999])),
            ],
        )
        .unwrap();

        // Test 1: use_index=false should allow explain_plan to succeed
        let merge_job_no_index =
            MergeInsertBuilder::try_new(Arc::new(ds.clone()), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .use_index(false) // Force not using index
                .try_build()
                .unwrap();

        // With use_index=false, explain_plan should succeed even with an index present
        let plan = merge_job_no_index.explain_plan(None, false).await;
        assert!(
            plan.is_ok(),
            "explain_plan should succeed with use_index=false"
        );
        let plan_str = plan.unwrap();
        assert!(plan_str.contains("MergeInsert"));
        assert!(plan_str.contains("HashJoinExec")); // Should use hash join, not index scan

        // Test 2: use_index=true (default) should fail explain_plan with index present
        let merge_job_with_index =
            MergeInsertBuilder::try_new(Arc::new(ds.clone()), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .use_index(true) // Explicitly set to use index (though it's the default)
                .try_build()
                .unwrap();

        // With use_index=true and an index present, explain_plan should fail
        let plan_result = merge_job_with_index.explain_plan(None, false).await;
        assert!(
            plan_result.is_err(),
            "explain_plan should fail with use_index=true when index exists"
        );

        match plan_result {
            Err(Error::NotSupported { source, .. }) => {
                assert!(source.to_string().contains("does not support explain_plan"));
            }
            _ => panic!("Expected NotSupported error"),
        }

        // Test 3: Verify actual execution works without index
        let source = Box::new(RecordBatchIterator::new(
            vec![Ok(source_batch.clone())],
            schema.clone(),
        ));
        let (result_ds, stats) = merge_job_no_index.execute_reader(source).await.unwrap();
        assert_eq!(stats.num_updated_rows, 2);
        assert_eq!(stats.num_inserted_rows, 1);

        // Verify the data was updated correctly
        let updated_count = result_ds
            .count_rows(Some("value = 999".to_string()))
            .await
            .unwrap();
        assert_eq!(updated_count, 3);
    }

    #[tokio::test]
    async fn test_full_schema_upsert_fragment_bitmap() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt32, true),
            Field::new("value", DataType::UInt32, true),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
        ]));

        let mut dataset = lance_datagen::gen_batch()
            .col("key", array::step_custom::<UInt32Type>(1, 1))
            .col("value", array::step_custom::<UInt32Type>(10, 10))
            .col(
                "vec",
                array::cycle_vec(
                    array::cycle::<Float32Type>(vec![
                        1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0,
                        15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
                    ]),
                    Dimension::from(4),
                ),
            )
            .into_ram_dataset_with_params(
                FragmentCount::from(2),
                FragmentRowCount::from(3),
                Some(WriteParams {
                    max_rows_per_file: 3,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        let scalar_params = ScalarIndexParams::default();
        dataset
            .create_index(
                &["value"],
                IndexType::Scalar,
                Some("value_idx".to_string()),
                &scalar_params,
                true,
            )
            .await
            .unwrap();

        let vector_params = VectorIndexParams::ivf_flat(1, MetricType::L2);
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("vec_idx".to_string()),
                &vector_params,
                true,
            )
            .await
            .unwrap();

        let indices = dataset.load_indices().await.unwrap();
        let value_index = indices.iter().find(|idx| idx.name == "value_idx").unwrap();
        let vec_index = indices.iter().find(|idx| idx.name == "vec_idx").unwrap();

        assert_eq!(
            value_index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            vec_index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        // update keys: 2,5
        let upsert_keys = UInt32Array::from(vec![2, 5]);
        let upsert_values = UInt32Array::from(vec![200, 500]);
        let upsert_vecs = FixedSizeListArray::try_new_from_values(
            Float32Array::from(vec![21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0]),
            4,
        )
        .unwrap();

        let upsert_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(upsert_keys),
                Arc::new(upsert_values),
                Arc::new(upsert_vecs),
            ],
        )
        .unwrap();

        let upsert_stream = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::once(async { Ok(upsert_batch) }).boxed(),
        );

        let (updated_dataset, _stats) =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["key".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::DoNothing)
                .when_not_matched_by_source(WhenNotMatchedBySource::Keep)
                .try_build()
                .unwrap()
                .execute(Box::pin(upsert_stream))
                .await
                .unwrap();

        let fragments = updated_dataset.get_fragments();
        assert_eq!(fragments.len(), 3);
    }

    #[tokio::test]
    async fn test_sub_schema_upsert_fragment_bitmap() {
        let mut dataset = lance_datagen::gen_batch()
            .col("key", array::step_custom::<UInt32Type>(1, 1))
            .col("value", array::step_custom::<UInt32Type>(10, 10))
            .col(
                "vec",
                array::cycle_vec(
                    array::cycle::<Float32Type>(vec![
                        1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0, 14.0,
                        15.0, 16.0, 17.0, 18.0, 19.0, 20.0, 21.0, 22.0, 23.0, 24.0,
                    ]),
                    Dimension::from(4),
                ),
            )
            .into_ram_dataset_with_params(
                FragmentCount::from(2),
                FragmentRowCount::from(3),
                Some(WriteParams {
                    max_rows_per_file: 3,
                    enable_stable_row_ids: true,
                    ..Default::default()
                }),
            )
            .await
            .unwrap();

        let scalar_params = ScalarIndexParams::default();
        dataset
            .create_index(
                &["value"],
                IndexType::Scalar,
                Some("value_idx".to_string()),
                &scalar_params,
                true,
            )
            .await
            .unwrap();

        let vector_params = VectorIndexParams::ivf_flat(1, MetricType::L2);
        dataset
            .create_index(
                &["vec"],
                IndexType::Vector,
                Some("vec_idx".to_string()),
                &vector_params,
                true,
            )
            .await
            .unwrap();

        let indices = dataset.load_indices().await.unwrap();
        let value_index = indices.iter().find(|idx| idx.name == "value_idx").unwrap();
        let vec_index = indices.iter().find(|idx| idx.name == "vec_idx").unwrap();

        assert_eq!(
            value_index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1]
        );
        assert_eq!(
            vec_index
                .fragment_bitmap
                .as_ref()
                .unwrap()
                .iter()
                .collect::<Vec<_>>(),
            vec![0, 1]
        );

        let sub_schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::UInt32, true),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
        ]));

        let upsert_keys = UInt32Array::from(vec![2, 5]);
        let upsert_vecs = FixedSizeListArray::try_new_from_values(
            Float32Array::from(vec![21.0, 22.0, 23.0, 24.0, 25.0, 26.0, 27.0, 28.0]),
            4,
        )
        .unwrap();

        let upsert_batch = RecordBatch::try_new(
            sub_schema.clone(),
            vec![Arc::new(upsert_keys), Arc::new(upsert_vecs)],
        )
        .unwrap();

        let upsert_stream = RecordBatchStreamAdapter::new(
            sub_schema.clone(),
            futures::stream::once(async { Ok(upsert_batch) }).boxed(),
        );

        let (updated_dataset, _stats) =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["key".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::DoNothing)
                .when_not_matched_by_source(WhenNotMatchedBySource::Keep)
                .try_build()
                .unwrap()
                .execute(Box::pin(upsert_stream))
                .await
                .unwrap();

        let fragments = updated_dataset.get_fragments();
        // v2 path: partial-schema upsert goes through FullSchemaMergeInsertExec
        // which writes a new fragment containing the updated rows. Fragments
        // 0 and 1 keep 2 rows each (with deletion vectors covering the
        // matched keys), fragment 2 is the new one holding the 2 updated
        // rows. The v1 RewriteColumns optimization (2 fragments, in-place
        // rewrite) is tracked separately as issue #4193.
        assert_eq!(fragments.len(), 3);

        let updated_indices = updated_dataset.load_indices().await.unwrap();
        // Both indices remain after the v2 upsert. The vector index still
        // covers the old fragments' non-deleted rows (the deleted rows are
        // filtered by deletion vectors), and queries that need the new
        // row values fall back to scanning the unindexed new fragment.
        // This is a behavior difference from v1, which eagerly invalidated
        // the vec index when any row in a fragment was updated.
        assert_eq!(updated_indices.len(), 2);
        let updated_value_index = updated_indices
            .iter()
            .find(|idx| idx.name == "value_idx")
            .unwrap();

        // The scalar index on `value` must still cover fragments 0 and 1 —
        // even though those fragments now carry deletion vectors, the
        // `value` column itself was not modified, so the existing index
        // entries remain valid for the rows that were not deleted.
        let value_bitmap = updated_value_index.fragment_bitmap.as_ref().unwrap();
        assert!(value_bitmap.contains(0));
        assert!(value_bitmap.contains(1));
    }

    #[tokio::test]
    async fn test_when_matched_fail() {
        let dataset = create_test_dataset("memory://test_fail", LanceFileVersion::V2_0, true).await;

        // Create new data with some existing keys (should fail)
        let new_data = RecordBatch::try_new(
            create_test_schema(),
            vec![
                Arc::new(UInt32Array::from(vec![1, 2, 10, 11])), // Keys: 1,2 exist, 10,11 are new
                Arc::new(UInt32Array::from(vec![100, 200, 1000, 1100])),
                Arc::new(StringArray::from(vec!["X", "Y", "Z", "W"])),
            ],
        )
        .unwrap();

        let reader = Box::new(RecordBatchIterator::new(
            [Ok(new_data.clone())],
            new_data.schema(),
        ));
        let new_stream = reader_to_stream(reader);

        let result = MergeInsertBuilder::try_new(dataset.clone(), vec!["key".to_string()])
            .unwrap()
            .when_matched(WhenMatched::Fail)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute(new_stream)
            .await;

        // Should fail because keys 1 and 2 already exist
        match result {
            Ok((_dataset, stats)) => {
                panic!(
                    "Expected merge insert to fail, but it succeeded. Stats: {:?}",
                    stats
                );
            }
            Err(e) => {
                let error_msg = e.to_string();
                assert!(error_msg.contains("Merge insert failed"));
                assert!(error_msg.contains("found matching row"));
            }
        }

        // Create new data with only new keys (should succeed)
        let new_data = RecordBatch::try_new(
            create_test_schema(),
            vec![
                Arc::new(UInt32Array::from(vec![10, 11, 12])), // All new keys
                Arc::new(UInt32Array::from(vec![1000, 1100, 1200])),
                Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
            ],
        )
        .unwrap();

        let reader = Box::new(RecordBatchIterator::new(
            [Ok(new_data.clone())],
            new_data.schema(),
        ));
        let new_stream = reader_to_stream(reader);

        let (updated_dataset, stats) =
            MergeInsertBuilder::try_new(dataset.clone(), vec!["key".to_string()])
                .unwrap()
                .when_matched(WhenMatched::Fail)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute(new_stream)
                .await
                .unwrap();

        // Should succeed with 3 new rows inserted
        assert_eq!(stats.num_inserted_rows, 3);
        assert_eq!(stats.num_updated_rows, 0);
        assert_eq!(stats.num_deleted_rows, 0);

        // Verify the data was inserted correctly
        let count = updated_dataset
            .count_rows(Some("key >= 10".to_string()))
            .await
            .unwrap();
        assert_eq!(count, 3);
    }

    /// Test case for Issue #4654: merge_insert should handle nullable source fields
    /// when target is non-nullable, as long as there are no actual null values.
    ///
    /// This test verifies that:
    /// - Dataset has non-nullable fields
    /// - Source data has nullable fields BUT no actual null values
    /// - merge_insert() succeeds (same behavior as insert)
    #[tokio::test]
    async fn test_merge_insert_permissive_nullability() {
        // Step 1: Create dataset with NON-NULLABLE schema
        let non_nullable_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false), // nullable=False
            Field::new("value", DataType::Int64, false), // nullable=False
        ]));

        let initial_data = RecordBatch::try_new(
            non_nullable_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();

        let test_uri = "memory://test_nullable_issue_4654";
        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial_data)], non_nullable_schema.clone()),
            test_uri,
            None,
        )
        .await
        .unwrap();

        // Step 2: Create new data with NULLABLE schema but NO actual null values
        let nullable_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),    // nullable=True
            Field::new("value", DataType::Int64, true), // nullable=True
        ]));

        let new_data = RecordBatch::try_new(
            nullable_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 4, 5])), // id=2 exists (update), 4,5 new (insert)
                Arc::new(Int64Array::from(vec![999, 400, 500])), // No nulls
            ],
        )
        .unwrap();

        // Step 3: Test merge_insert()
        let merge_result = MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute_reader(Box::new(RecordBatchIterator::new(
                vec![Ok(new_data.clone())],
                nullable_schema.clone(),
            )))
            .await;

        assert!(
            merge_result.is_ok(),
            "merge_insert() should succeed with nullable fields but no actual nulls. \
             This is the same behavior as insert/append. Error: {:?}",
            merge_result.err()
        );

        // Step 4: Verify the results
        let (merged_dataset, stats) = merge_result.unwrap();

        // Should have: 1 updated row (id=2), 2 new rows (id=4,5)
        assert_eq!(stats.num_updated_rows, 1, "Should update 1 row (id=2)");
        assert_eq!(
            stats.num_inserted_rows, 2,
            "Should insert 2 new rows (id=4,5)"
        );

        // Total: 3 original (id=1,2,3) + 2 new (id=4,5) = 5 rows
        let count = merged_dataset.count_rows(None).await.unwrap();
        assert_eq!(count, 5, "Should have 5 total rows");

        // Verify the updated value for id=2
        let result = merged_dataset
            .scan()
            .filter("id = 2")
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let batch = concat_batches(&result[0].schema(), &result).unwrap();
        assert_eq!(batch.num_rows(), 1);
        let value_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(
            value_array.value(0),
            999,
            "Value for id=2 should be updated to 999"
        );
    }

    /// Test case for Issue #4644: merge_insert should NOT skip source rows whose ON
    /// columns contain NULL.
    ///
    /// With standard SQL equality NULL != NULL, so a source row with a NULL key will
    /// never match any target row.  It must therefore be treated as "not matched" and
    /// inserted when `when_not_matched = InsertAll`.  The previous implementation
    /// incorrectly required all ON columns to be non-null before even considering the
    /// row, causing it to be silently dropped (Action::Nothing).
    #[tokio::test]
    async fn test_merge_insert_null_on_column_inserts() {
        // Initial dataset: one row with a NULL record_type.
        let initial_data = record_batch!(
            ("id", Int32, [0]),
            ("record_type", Utf8, [Option::<&str>::None]),
            ("value", Int32, [10])
        )
        .unwrap();

        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial_data.clone())], initial_data.schema()),
            "memory://test_null_on_column",
            None,
        )
        .await
        .unwrap();

        // New data: a row with a different id AND a NULL record_type.
        // Because id differs (2 vs 0) no match should be found even with NULL-safe
        // semantics, so this row must be INSERTED.
        let new_data = record_batch!(
            ("id", Int32, [Some(2)]),
            ("record_type", Utf8, [Option::<&str>::None]),
            ("value", Int32, [Some(99)])
        )
        .unwrap();

        let (merged_dataset, stats) = MergeInsertBuilder::try_new(
            Arc::new(dataset),
            vec!["id".to_string(), "record_type".to_string()],
        )
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .try_build()
        .unwrap()
        .execute_reader(Box::new(RecordBatchIterator::new(
            vec![Ok(new_data.clone())],
            new_data.schema(),
        )))
        .await
        .unwrap();

        // The source row (id=2, record_type=NULL) must be inserted, NOT silently skipped.
        assert_eq!(
            stats.num_inserted_rows, 1,
            "row with NULL ON column should be inserted"
        );
        assert_eq!(stats.num_updated_rows, 0, "no row should be updated");

        let count = merged_dataset.count_rows(None).await.unwrap();
        assert_eq!(
            count, 2,
            "dataset should have the original row plus the newly inserted row"
        );
    }

    /// Partial composite key match: the non-null part of the ON key (id) matches an
    /// existing target row, but the second ON column (record_type) is NULL in the source.
    /// Standard SQL equality treats NULL != NULL, so the composite key does NOT match
    /// and the source row must be inserted, not updated and not silently dropped.
    #[tokio::test]
    async fn test_merge_insert_partial_composite_key_null() {
        // Target: one row where id=1 and record_type="A".
        let initial_data = record_batch!(
            ("id", Int32, [Some(1)]),
            ("record_type", Utf8, [Some("A")]),
            ("value", Int32, [Some(10)])
        )
        .unwrap();

        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial_data.clone())], initial_data.schema()),
            "memory://test_partial_composite_null",
            None,
        )
        .await
        .unwrap();

        // Source: one row where id=1 (matches target) but record_type=NULL.
        // The composite key (1, NULL) does NOT match (1, "A") under standard equality,
        // so this is a "not matched" row that should be inserted.
        let new_data = record_batch!(
            ("id", Int32, [Some(1)]),
            ("record_type", Utf8, [Option::<&str>::None]),
            ("value", Int32, [Some(99)])
        )
        .unwrap();

        let (merged_dataset, stats) = MergeInsertBuilder::try_new(
            Arc::new(dataset),
            vec!["id".to_string(), "record_type".to_string()],
        )
        .unwrap()
        .when_matched(WhenMatched::UpdateAll)
        .when_not_matched(WhenNotMatched::InsertAll)
        .try_build()
        .unwrap()
        .execute_reader(Box::new(RecordBatchIterator::new(
            vec![Ok(new_data.clone())],
            new_data.schema(),
        )))
        .await
        .unwrap();

        // Source row (id=1, record_type=NULL) must be inserted, not updated and not dropped.
        assert_eq!(
            stats.num_inserted_rows, 1,
            "row with partial NULL composite key should be inserted"
        );
        assert_eq!(
            stats.num_updated_rows, 0,
            "existing (id=1, record_type=A) row must not be updated"
        );

        // Dataset: original (1, "A") row + newly inserted (1, NULL) row = 2 rows.
        let count = merged_dataset.count_rows(None).await.unwrap();
        assert_eq!(
            count, 2,
            "both the original and the new row must be present"
        );
    }

    /// Variant of test_merge_insert_null_on_column_inserts with a single ON column
    /// that is entirely NULL, and a target row that also has a NULL in that column.
    /// Since standard SQL equality treats NULL != NULL, the source row must not match
    /// the existing target row and must be inserted separately.
    #[tokio::test]
    async fn test_merge_insert_null_single_on_column() {
        // Dataset with a single row where id is NULL.
        let initial_data = record_batch!(
            ("id", Int32, [Option::<i32>::None]),
            ("value", Int32, [Some(1)])
        )
        .unwrap();

        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial_data.clone())], initial_data.schema()),
            "memory://test_null_single_on_column",
            None,
        )
        .await
        .unwrap();

        // Source has two rows: one with id=NULL and one with id=5.
        // id=NULL should not match the existing id=NULL row (standard equality), so it
        // gets inserted.  id=5 is a brand-new key and also gets inserted.
        let new_data = record_batch!(
            ("id", Int32, [Option::<i32>::None, Some(5)]),
            ("value", Int32, [Some(99), Some(50)])
        )
        .unwrap();

        let (merged_dataset, stats) =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(new_data.clone())],
                    new_data.schema(),
                )))
                .await
                .unwrap();

        // Both source rows must be inserted (not silently dropped).
        assert_eq!(
            stats.num_inserted_rows, 2,
            "both rows with NULL ON column should be inserted"
        );
        assert_eq!(stats.num_updated_rows, 0);

        // Dataset now has: original NULL-id row + 2 newly inserted rows = 3 total.
        let count = merged_dataset.count_rows(None).await.unwrap();
        assert_eq!(count, 3);
    }

    /// Test case for Issue #3634: merge_insert should provide a helpful error
    /// message when a subschema with a mismatched type is provided.
    #[tokio::test]
    async fn test_merge_insert_subschema_invalid_type_error() {
        // Step 1: Create a dataset with a multi-column schema.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Float64, true), // The target type is Float64.
            Field::new("extra", DataType::Utf8, true),
        ]));

        let initial_data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(Float64Array::from(vec![1.1, 2.2, 3.3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
            ],
        )
        .unwrap();

        let test_uri = "memory://test_issue_3634";
        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial_data)], schema),
            test_uri,
            None,
        )
        .await
        .unwrap();

        // Step 2: Create source data with a subschema where one field has a wrong type.
        let subschema_with_wrong_type = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("value", DataType::Int32, true),
        ]));

        let new_data = RecordBatch::try_new(
            subschema_with_wrong_type.clone(),
            vec![
                Arc::new(Int32Array::from(vec![2, 4])),
                Arc::new(Int32Array::from(vec![22, 44])),
            ],
        )
        .unwrap();

        // Step 3: Execute the merge_insert operation, which should fail.
        let merge_result = MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute_reader(Box::new(RecordBatchIterator::new(
                vec![Ok(new_data)],
                subschema_with_wrong_type,
            )))
            .await;

        // Step 4: Verify that the operation failed with the correct error type and message.
        let err = merge_result.expect_err("Merge insert should have failed but it succeeded.");
        assert!(
            matches!(err, lance_core::Error::SchemaMismatch { .. }),
            "Expected a SchemaMismatch error, but got a different error type: {:?}",
            err
        );

        let error_message = err.to_string();
        assert!(
            error_message.contains("`value` should have type double but type was int32"),
            "Error message should specify the expected (double) and actual (int32) types for 'value', but was: {}",
            error_message
        );

        assert!(
            !error_message.contains("missing="),
            "Error message should NOT complain about missing fields for a subschema check, but was: {}",
            error_message
        );
    }

    /// Test that merge_insert works with mixed-case column names as keys.
    /// This is a regression test for the fix in assign_action.rs that wraps
    /// column names in double quotes to preserve case in DataFusion expressions.
    #[tokio::test]
    async fn test_merge_insert_mixed_case_key() {
        // Create a schema with a mixed-case column name
        let schema = Arc::new(Schema::new(vec![
            Field::new("userId", DataType::UInt32, false),
            Field::new("value", DataType::UInt32, true),
        ]));

        // Initial data
        let initial_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![1, 2, 3])),
                Arc::new(UInt32Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        // Write initial dataset
        let test_uri = "memory://test_mixed_case.lance";
        let ds = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial_batch)], schema.clone()),
            test_uri,
            None,
        )
        .await
        .unwrap();

        // New data to merge (updates userId=2, inserts userId=4)
        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![2, 4])),
                Arc::new(UInt32Array::from(vec![200, 400])),
            ],
        )
        .unwrap();

        // Perform merge_insert using "userId" as the key
        let job = MergeInsertBuilder::try_new(Arc::new(ds), vec!["userId".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .try_build()
            .unwrap();

        let new_reader = Box::new(RecordBatchIterator::new([Ok(new_batch)], schema.clone()));
        let new_stream = reader_to_stream(new_reader);

        let (merged_ds, _merge_stats) = job.execute(new_stream).await.unwrap();

        // Verify the merge succeeded
        let result = merged_ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let result_batch = concat_batches(&schema, &result).unwrap();
        assert_eq!(result_batch.num_rows(), 4); // 3 original + 1 inserted

        // Verify that userId=2 was updated to value=200
        let user_ids = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let values = result_batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();

        // Find the row with userId=2 and check its value
        for i in 0..result_batch.num_rows() {
            if user_ids.value(i) == 2 {
                assert_eq!(
                    values.value(i),
                    200,
                    "userId=2 should have been updated to value=200"
                );
            }
        }
    }

    /// Test case for Issue #5323: merge_insert should use the full schema path
    /// when columns are provided in a different order than the dataset schema.
    #[tokio::test]
    async fn test_merge_insert_reordered_columns() {
        use arrow_array::record_batch;

        let initial_data = record_batch!(
            ("id", Int32, [1, 2, 3]),
            ("value", Float64, [1.1, 2.2, 3.3]),
            ("extra", Utf8, ["a", "b", "c"])
        )
        .unwrap();

        let dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(initial_data.clone())], initial_data.schema()),
            "memory://test_issue_5323",
            None,
        )
        .await
        .unwrap();

        // Source data with reordered columns: [extra, id, value] instead of [id, value, extra]
        let new_data = record_batch!(
            ("extra", Utf8, ["x", "y"]),
            ("id", Int32, [2, 4]), // id 2 exists, 4 is new
            ("value", Float64, [22.2, 44.4])
        )
        .unwrap();

        // Verify reordered columns can use the fast path
        let job = MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();
        assert!(
            job.can_use_create_plan(&new_data.schema()).await.unwrap(),
            "Reordered schema should be able to use fast path"
        );

        // Execute and verify data correctness
        let (merged_dataset, _) =
            MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
                .unwrap()
                .when_matched(WhenMatched::UpdateAll)
                .when_not_matched(WhenNotMatched::InsertAll)
                .try_build()
                .unwrap()
                .execute_reader(Box::new(RecordBatchIterator::new(
                    vec![Ok(new_data.clone())],
                    new_data.schema(),
                )))
                .await
                .unwrap();

        let result = merged_dataset
            .scan()
            .order_by(Some(vec![ColumnOrdering::asc_nulls_first(
                "id".to_string(),
            )]))
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let expected = record_batch!(
            ("id", Int32, [1, 2, 3, 4]),
            ("value", Float64, [1.1, 22.2, 3.3, 44.4]),
            ("extra", Utf8, ["a", "x", "c", "y"])
        )
        .unwrap();

        assert_eq!(result, expected);
    }

    /// Test WhenMatched::Delete with full schema source data.
    /// Source contains all columns (key, value, filterme) but we only use it to identify
    /// rows to delete - no data is written back.
    #[rstest::rstest]
    #[tokio::test]
    async fn test_when_matched_delete_full_schema(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(true, false)] enable_stable_row_ids: bool,
    ) {
        let schema = create_test_schema();
        let test_uri = "memory://test_delete_full.lance";

        // Create dataset with keys 1-6 (value=1)
        let ds = create_test_dataset(test_uri, version, enable_stable_row_ids).await;

        // Source data has keys 4, 5, 6, 7, 8, 9 with full schema
        // Keys 4, 5, 6 match existing rows and should be deleted
        // Keys 7, 8, 9 don't match (and we're not inserting)
        let new_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![4, 5, 6, 7, 8, 9])),
                Arc::new(UInt32Array::from(vec![2, 2, 2, 2, 2, 2])),
                Arc::new(StringArray::from(vec!["A", "B", "C", "A", "B", "C"])),
            ],
        )
        .unwrap();

        let keys = vec!["key".to_string()];

        // First, verify the execution plan structure
        // Delete-only should use Inner join and only include key columns (optimization)
        // Action 3 = Delete
        let plan_job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();
        let plan_stream = reader_to_stream(Box::new(RecordBatchIterator::new(
            [Ok(new_batch.clone())],
            schema.clone(),
        )));
        let plan = plan_job
            .create_plan(one_shot_provider(plan_stream).unwrap())
            .await
            .unwrap();
        assert_plan_node_equals(
            plan,
            "DeleteOnlyMergeInsert: on=[key], when_matched=Delete, when_not_matched=DoNothing
  ...
    HashJoinExec: ...join_type=Inner...
      ...
      ...
        StreamingTableExec: partition_sizes=1, projection=[key]",
        )
        .await
        .unwrap();
        let job = MergeInsertBuilder::try_new(ds.clone(), keys)
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();

        let new_reader = Box::new(RecordBatchIterator::new([Ok(new_batch)], schema.clone()));
        let new_stream = reader_to_stream(new_reader);

        let (merged_dataset, merge_stats) = job.execute(new_stream).await.unwrap();

        // Should have deleted 3 rows (keys 4, 5, 6)
        assert_eq!(merge_stats.num_deleted_rows, 3);
        assert_eq!(merge_stats.num_inserted_rows, 0);
        assert_eq!(merge_stats.num_updated_rows, 0);

        // Verify remaining data - only keys 1, 2, 3 should remain
        let batches = merged_dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let merged = concat_batches(&schema, &batches).unwrap();
        let mut remaining_keys: Vec<u32> = merged
            .column(0)
            .as_primitive::<UInt32Type>()
            .values()
            .to_vec();
        remaining_keys.sort();
        assert_eq!(remaining_keys, vec![1, 2, 3]);
    }

    /// Test WhenMatched::Delete with ID-only source data (just key column).
    /// This is the optimized bulk delete case where we only need key columns for matching.
    #[rstest::rstest]
    #[tokio::test]
    async fn test_when_matched_delete_id_only(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
        #[values(true, false)] enable_stable_row_ids: bool,
    ) {
        let test_uri = "memory://test_delete_id_only.lance";

        // Create dataset with keys 1-6 (full schema: key, value, filterme)
        let ds = create_test_dataset(test_uri, version, enable_stable_row_ids).await;
        let id_only_schema = Arc::new(Schema::new(vec![Field::new("key", DataType::UInt32, true)]));
        let new_batch = RecordBatch::try_new(
            id_only_schema.clone(),
            vec![Arc::new(UInt32Array::from(vec![2, 4, 6]))], // Delete keys 2, 4, 6
        )
        .unwrap();

        let keys = vec!["key".to_string()];

        // ID-only delete should use Inner join with key-only projection
        // on=[(key@0, key@0)] because key is at position 0 in both target and source
        let plan_job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();
        let plan_stream = reader_to_stream(Box::new(RecordBatchIterator::new(
            [Ok(new_batch.clone())],
            id_only_schema.clone(),
        )));
        let plan = plan_job
            .create_plan(one_shot_provider(plan_stream).unwrap())
            .await
            .unwrap();
        assert_plan_node_equals(
            plan,
            "DeleteOnlyMergeInsert: on=[key], when_matched=Delete, when_not_matched=DoNothing
  ...
    HashJoinExec: ...join_type=Inner...
      ...
      ...
        StreamingTableExec: partition_sizes=1, projection=[key]",
        )
        .await
        .unwrap();
        let job = MergeInsertBuilder::try_new(ds.clone(), keys)
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();

        let new_reader = Box::new(RecordBatchIterator::new(
            [Ok(new_batch)],
            id_only_schema.clone(),
        ));
        let new_stream = reader_to_stream(new_reader);

        let (merged_dataset, merge_stats) = job.execute(new_stream).await.unwrap();

        // Should have deleted 3 rows (keys 2, 4, 6)
        assert_eq!(merge_stats.num_deleted_rows, 3);
        assert_eq!(merge_stats.num_inserted_rows, 0);
        assert_eq!(merge_stats.num_updated_rows, 0);

        // Verify remaining data - only keys 1, 3, 5 should remain
        let full_schema = create_test_schema();
        let batches = merged_dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let merged = concat_batches(&full_schema, &batches).unwrap();
        let mut remaining_keys: Vec<u32> = merged
            .column(0)
            .as_primitive::<UInt32Type>()
            .values()
            .to_vec();
        remaining_keys.sort();
        assert_eq!(remaining_keys, vec![1, 3, 5]);
    }

    /// Test WhenMatched::Delete combined with WhenNotMatched::InsertAll.
    /// This replaces existing matching rows with nothing (delete) while inserting new rows.
    #[rstest::rstest]
    #[tokio::test]
    async fn test_when_matched_delete_with_insert(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
    ) {
        let schema = create_test_schema();
        let test_uri = "memory://test_delete_with_insert.lance";

        // Create dataset with keys 1-6
        let ds = create_test_dataset(test_uri, version, false).await;

        // Source has keys 4, 5, 6 (match - will be deleted) and 7, 8, 9 (new - will be inserted)
        let new_batch = create_new_batch(schema.clone());

        let keys = vec!["key".to_string()];

        // Delete + Insert should use Right join to see unmatched rows for insertion
        let plan_job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();
        let plan_stream = reader_to_stream(Box::new(RecordBatchIterator::new(
            [Ok(new_batch.clone())],
            schema.clone(),
        )));
        let plan = plan_job
            .create_plan(one_shot_provider(plan_stream).unwrap())
            .await
            .unwrap();
        assert_plan_node_equals(
            plan,
            "MergeInsert: on=[key], when_matched=Delete, when_not_matched=InsertAll, when_not_matched_by_source=Keep...THEN 2 WHEN...THEN 3 ELSE 0 END as __action]...projection=[key, value, filterme]"
        ).await.unwrap();

        // Delete matched rows, insert unmatched rows
        let job = MergeInsertBuilder::try_new(ds.clone(), keys)
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();

        let new_reader = Box::new(RecordBatchIterator::new([Ok(new_batch)], schema.clone()));
        let new_stream = reader_to_stream(new_reader);

        let (merged_dataset, merge_stats) = job.execute(new_stream).await.unwrap();

        // Deleted 3 (keys 4, 5, 6), inserted 3 (keys 7, 8, 9)
        assert_eq!(merge_stats.num_deleted_rows, 3);
        assert_eq!(merge_stats.num_inserted_rows, 3);
        assert_eq!(merge_stats.num_updated_rows, 0);

        // Verify: keys 1, 2, 3 (original, not matched), 7, 8, 9 (new inserts)
        let batches = merged_dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let merged = concat_batches(&schema, &batches).unwrap();
        let mut remaining_keys: Vec<u32> = merged
            .column(0)
            .as_primitive::<UInt32Type>()
            .values()
            .to_vec();
        remaining_keys.sort();
        assert_eq!(remaining_keys, vec![1, 2, 3, 7, 8, 9]);

        // Verify values: keys 1, 2, 3 have value=1 (original), keys 7, 8, 9 have value=2 (new)
        let keyvals: Vec<(u32, u32)> = merged
            .column(0)
            .as_primitive::<UInt32Type>()
            .values()
            .iter()
            .zip(
                merged
                    .column(1)
                    .as_primitive::<UInt32Type>()
                    .values()
                    .iter(),
            )
            .map(|(&k, &v)| (k, v))
            .collect();

        for (key, value) in keyvals {
            if key <= 3 {
                assert_eq!(value, 1, "Original keys should have value=1");
            } else {
                assert_eq!(value, 2, "New keys should have value=2");
            }
        }
    }

    /// Test WhenMatched::Delete when source data has no matching keys.
    /// This should result in zero deletes and the dataset remains unchanged.
    #[rstest::rstest]
    #[tokio::test]
    async fn test_when_matched_delete_no_matches(
        #[values(LanceFileVersion::Legacy, LanceFileVersion::V2_0)] version: LanceFileVersion,
    ) {
        let schema = create_test_schema();
        let test_uri = "memory://test_delete_no_matches.lance";

        // Create dataset with keys 1-6
        let ds = create_test_dataset(test_uri, version, false).await;

        // Source data has keys 100, 200, 300 - none match existing keys 1-6
        let non_matching_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![100, 200, 300])),
                Arc::new(UInt32Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec!["X", "Y", "Z"])),
            ],
        )
        .unwrap();

        let keys = vec!["key".to_string()];

        // Even with no matches, the plan structure should be the same
        let plan_job = MergeInsertBuilder::try_new(ds.clone(), keys.clone())
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();
        let plan_stream = reader_to_stream(Box::new(RecordBatchIterator::new(
            [Ok(non_matching_batch.clone())],
            schema.clone(),
        )));
        let plan = plan_job
            .create_plan(one_shot_provider(plan_stream).unwrap())
            .await
            .unwrap();
        assert_plan_node_equals(
            plan,
            "DeleteOnlyMergeInsert: on=[key], when_matched=Delete, when_not_matched=DoNothing
  ...
    HashJoinExec: ...join_type=Inner...
      ...
      ...
        StreamingTableExec: partition_sizes=1, projection=[key]",
        )
        .await
        .unwrap();
        let job = MergeInsertBuilder::try_new(ds.clone(), keys)
            .unwrap()
            .when_matched(WhenMatched::Delete)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();

        let new_reader = Box::new(RecordBatchIterator::new(
            [Ok(non_matching_batch)],
            schema.clone(),
        ));
        let new_stream = reader_to_stream(new_reader);

        let (merged_dataset, merge_stats) = job.execute(new_stream).await.unwrap();

        // Should have deleted 0 rows since no keys matched
        assert_eq!(merge_stats.num_deleted_rows, 0);
        assert_eq!(merge_stats.num_inserted_rows, 0);
        assert_eq!(merge_stats.num_updated_rows, 0);

        // Verify all original data remains unchanged - keys 1-6 should all still be present
        let batches = merged_dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        let merged = concat_batches(&schema, &batches).unwrap();
        let mut remaining_keys: Vec<u32> = merged
            .column(0)
            .as_primitive::<UInt32Type>()
            .values()
            .to_vec();
        remaining_keys.sort();
        assert_eq!(remaining_keys, vec![1, 2, 3, 4, 5, 6]);
    }

    /// Test that MergeInsertPlanner::is_delete_only correctly identifies delete-only operations.
    ///
    /// Delete-only is true only when:
    /// - when_matched = Delete
    /// - insert_not_matched = false (WhenNotMatched::DoNothing)
    /// - delete_not_matched_by_source = Keep
    ///
    /// This test iterates through all valid combinations of WhenMatched, WhenNotMatched,
    /// and WhenNotMatchedBySource to verify the is_delete_only logic.
    #[tokio::test]
    async fn test_is_delete_only() {
        use itertools::iproduct;

        // All variants to test (excluding UpdateIf and DeleteIf because they require expressions)
        let when_matched_variants = [
            WhenMatched::UpdateAll,
            WhenMatched::DoNothing,
            WhenMatched::Fail,
            WhenMatched::Delete,
        ];
        let when_not_matched_variants = [WhenNotMatched::InsertAll, WhenNotMatched::DoNothing];
        let when_not_matched_by_source_variants =
            [WhenNotMatchedBySource::Keep, WhenNotMatchedBySource::Delete];

        let schema = create_test_schema();

        for (idx, (when_matched, when_not_matched, when_not_matched_by_source)) in iproduct!(
            when_matched_variants.iter().cloned(),
            when_not_matched_variants.iter().cloned(),
            when_not_matched_by_source_variants.iter().cloned()
        )
        .enumerate()
        {
            // Check if this is a valid (non-no-op) combination, since this would fail try_build()
            let is_no_op = matches!(when_matched, WhenMatched::DoNothing | WhenMatched::Fail)
                && matches!(when_not_matched, WhenNotMatched::DoNothing)
                && matches!(when_not_matched_by_source, WhenNotMatchedBySource::Keep);
            if is_no_op {
                continue;
            }

            let test_uri = format!("memory://test_is_delete_only_{}.lance", idx);
            let ds = create_test_dataset(&test_uri, LanceFileVersion::V2_0, false).await;

            let new_batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt32Array::from(vec![4, 5, 6])),
                    Arc::new(UInt32Array::from(vec![2, 2, 2])),
                    Arc::new(StringArray::from(vec!["A", "B", "C"])),
                ],
            )
            .unwrap();

            let keys = vec!["key".to_string()];

            let mut builder = MergeInsertBuilder::try_new(ds.clone(), keys).unwrap();
            builder
                .when_matched(when_matched.clone())
                .when_not_matched(when_not_matched.clone())
                .when_not_matched_by_source(when_not_matched_by_source.clone());

            let job = builder.try_build().unwrap();

            let plan_stream = reader_to_stream(Box::new(RecordBatchIterator::new(
                [Ok(new_batch)],
                schema.clone(),
            )));
            let plan = job
                .create_plan(one_shot_provider(plan_stream).unwrap())
                .await
                .unwrap();

            let plan_str = datafusion::physical_plan::displayable(plan.as_ref())
                .indent(true)
                .to_string();

            let expected_delete_only = matches!(when_matched, WhenMatched::Delete)
                && matches!(when_not_matched, WhenNotMatched::DoNothing)
                && matches!(when_not_matched_by_source, WhenNotMatchedBySource::Keep);

            if expected_delete_only {
                assert!(
                    plan_str.contains("DeleteOnlyMergeInsert"),
                    "Expected DeleteOnlyMergeInsert for ({:?}, {:?}, {:?}), but got:\n{}",
                    when_matched,
                    when_not_matched,
                    when_not_matched_by_source,
                    plan_str
                );
            } else {
                assert!(
                    plan_str.contains("MergeInsert:")
                        && !plan_str.contains("DeleteOnlyMergeInsert"),
                    "Expected MergeInsert (not DeleteOnlyMergeInsert) for ({:?}, {:?}, {:?}), but got:\n{}",
                    when_matched,
                    when_not_matched,
                    when_not_matched_by_source,
                    plan_str
                );
            }
        }
    }

    /// Tests that apply_deletions correctly handles an error when applying the row deletions.
    #[tokio::test]
    async fn test_apply_deletions_invalid_row_address() {
        use super::exec::apply_deletions;
        use roaring::RoaringTreemap;

        let test_uri = "memory://test_apply_deletions_error.lance";

        // Create a dataset with 2 fragments, each with 3 rows
        let ds = create_test_dataset(test_uri, LanceFileVersion::V2_0, false).await;
        let fragment_id = ds.get_fragments()[0].id() as u32;

        // Create row addresses with invalid row offsets for this fragment
        // Row address format: high 32 bits = fragment_id, low 32 bits = row_offset
        // Each fragment has only 3 rows (offsets 0, 1, 2).
        //
        // The error in extend_deletions is triggered when deletion_vector.len() >= physical_rows
        // AND at least one row ID is >= physical_rows.
        // So we need to add enough deletions (at least 3) with some being invalid (>= 3).
        let mut invalid_row_addrs = RoaringTreemap::new();
        let base = (fragment_id as u64) << 32;
        // Add 4 deletions: rows 10, 11, 12, 13 (all invalid since only rows 0-2 exist)
        for row_offset in 10..14u64 {
            invalid_row_addrs.insert(base | row_offset);
        }

        let result = apply_deletions(&ds, &invalid_row_addrs).await;

        assert!(result.is_err(), "Expected error for invalid row addresses");
        let err = result.unwrap_err();
        assert!(
            err.to_string()
                .contains("Deletion vector includes rows that aren't in the fragment"),
            "Expected 'rows that aren't in the fragment' error, got: {}",
            err
        );
    }

    mod external_error {
        use super::*;
        use arrow_schema::{ArrowError, Field as ArrowField, Schema as ArrowSchema};
        use std::fmt;

        #[derive(Debug)]
        struct MyTestError {
            code: i32,
            details: String,
        }

        impl fmt::Display for MyTestError {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "MyTestError({}): {}", self.code, self.details)
            }
        }

        impl std::error::Error for MyTestError {}

        #[tokio::test]
        async fn test_merge_insert_execute_reader_preserves_error_message() {
            let schema = Arc::new(ArrowSchema::new(vec![
                ArrowField::new("key", DataType::Int32, false),
                ArrowField::new("value", DataType::Int32, false),
            ]));

            // Create initial dataset
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int32Array::from(vec![1, 2, 3])),
                    Arc::new(Int32Array::from(vec![10, 20, 30])),
                ],
            )
            .unwrap();
            let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
            let dataset = Arc::new(
                Dataset::write(reader, "memory://test_merge_external", None)
                    .await
                    .unwrap(),
            );

            // Try merge insert with failing source
            let error_code = 789;
            let iter = std::iter::once(Err(ArrowError::ExternalError(Box::new(MyTestError {
                code: error_code,
                details: "merge insert failure".to_string(),
            }))));
            let reader = RecordBatchIterator::new(iter, schema);

            let result = MergeInsertBuilder::try_new(dataset, vec!["key".to_string()])
                .unwrap()
                .try_build()
                .unwrap()
                .execute_reader(Box::new(reader) as Box<dyn RecordBatchReader + Send>)
                .await;

            // The source error is routed through the merge plan, which shares it
            // across join partitions, so its concrete type is not recoverable. The
            // message must still reach the caller.
            let err = result.expect_err("expected the source error to surface");
            assert!(
                err.to_string().contains("merge insert failure"),
                "source error message should be preserved; got: {err}"
            );
        }
    }

    /// Creates a 3-fragment dataset (100 rows each) with columns (id: Utf8, category: Utf8,
    /// value_a: Float64, value_b: Float64) and a BTree index on `id`.
    ///
    /// Fragment 0: id-0000..id-0099
    /// Fragment 1: id-0100..id-0199
    /// Fragment 2: id-0200..id-0299
    async fn create_indexed_3frag_dataset() -> Arc<Dataset> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("value_a", DataType::Float64, false),
            Field::new("value_b", DataType::Float64, false),
        ]));

        let make_batch = |frag_idx: usize| {
            let start = frag_idx * 100;
            let ids: Vec<String> = (start..start + 100).map(|j| format!("id-{j:04}")).collect();
            let categories: Vec<&str> = vec!["A"; 100];
            let value_a: Vec<f64> = (0..100)
                .map(|i| i as f64 + frag_idx as f64 * 100.0)
                .collect();
            let value_b: Vec<f64> = (0..100).map(|i| i as f64 * 0.1).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(StringArray::from(categories)),
                    Arc::new(Float64Array::from(value_a)),
                    Arc::new(Float64Array::from(value_b)),
                ],
            )
            .unwrap()
        };

        // Write first fragment
        let batch0 = make_batch(0);
        let reader = Box::new(RecordBatchIterator::new([Ok(batch0)], schema.clone()));
        let mut ds = Dataset::write(reader, "memory://indexed_3frag", None)
            .await
            .unwrap();

        // Append fragments 1 and 2
        for frag_idx in 1..3 {
            let batch = make_batch(frag_idx);
            let reader = Box::new(RecordBatchIterator::new([Ok(batch)], schema.clone()));
            ds.append(reader, None).await.unwrap();
        }

        // Create BTree index on id
        ds.create_index(
            &["id"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

        Arc::new(ds)
    }

    /// Perform a partial-schema merge_insert (only id + value_a) targeting specific id ranges.
    /// This causes touched fragments to drop from the index bitmap while btree data retains
    /// stale entries.
    async fn partial_merge_insert(
        dataset: Arc<Dataset>,
        id_range: std::ops::Range<usize>,
        value_a_val: f64,
    ) -> Arc<Dataset> {
        let ids: Vec<String> = id_range.map(|j| format!("id-{j:04}")).collect();
        let n = ids.len();
        let sub_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value_a", DataType::Float64, false),
        ]));
        let batch = RecordBatch::try_new(
            sub_schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(Float64Array::from(vec![value_a_val; n])),
            ],
        )
        .unwrap();
        let reader = Box::new(RecordBatchIterator::new([Ok(batch)], sub_schema));

        let (ds, _) = MergeInsertBuilder::try_new(dataset, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap()
            .execute_reader(reader)
            .await
            .unwrap();
        ds
    }

    // Regression test: partial-schema merge_insert followed by another partial merge_insert
    // on the same rows should not produce "Ambiguous merge inserts" errors.
    //
    // The bug: the first partial merge_insert drops the touched fragment from the index bitmap
    // but leaves stale btree entries. The second merge_insert finds the same rows via both
    // the stale btree lookup AND the unindexed fragment scan, causing duplicates.
    #[tokio::test]
    async fn test_partial_merge_insert_stale_index_ambiguous() {
        let dataset = create_indexed_3frag_dataset().await;

        // Step 2: Partial merge_insert on fragment 1 rows -> fragment 1 drops from bitmap
        let dataset = partial_merge_insert(dataset, 100..200, 999.0).await;

        // Step 3: Another partial merge_insert on the same rows.
        // This should succeed, not fail with "Ambiguous merge inserts".
        let dataset = partial_merge_insert(dataset, 100..200, 888.0).await;

        // Verify correctness: all 300 rows present, updated values correct
        let batches = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let all_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("value_a", DataType::Float64, false),
            Field::new("value_b", DataType::Float64, false),
        ]));
        let combined = concat_batches(&all_schema, &batches).unwrap();
        assert_eq!(combined.num_rows(), 300);

        // Check the updated rows have value_a = 888.0
        let result = dataset
            .scan()
            .filter("id >= 'id-0100' AND id < 'id-0200'")
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let result = concat_batches(&all_schema, &result).unwrap();
        assert_eq!(result.num_rows(), 100);
        let values = result
            .column_by_name("value_a")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..100 {
            assert_eq!(values.value(i), 888.0, "row {i} should have value_a=888.0");
        }
    }

    // Regression test for GitHub issue #6877.
    //
    // Two sequential full-schema merge_insert UpdateAll calls against the same
    // target row, on a dataset with stable_row_ids enabled and a BTREE scalar
    // index on the join column, used to fail on the second call with
    // "Ambiguous merge inserts are prohibited" — even though each call's
    // source had exactly one row per key.
    //
    // Mechanism: with stable row ids the BTREE stores stable_row_ids (not
    // physical addresses). After the first merge_insert, A's stable_row_id is
    // preserved but its physical home moves to an unindexed fragment. The
    // BTREE-side TakeExec resolves the stable_row_id to A's new location and
    // emits a row; the unindexed-fragments scan also covers the new fragment
    // and emits the same logical row. Both surface the same `_rowid`, so the
    // merge_insert source-dedup HashSet sees a duplicate and aborts.
    //
    // Fix: thread `restrict_to_fragments` into `do_create_deletion_mask_row_id`
    // so the allow-list only contains stable_row_ids whose current physical
    // home is inside the index's fragment_bitmap.
    #[tokio::test]
    async fn test_issue_6877_repeated_merge_insert_stable_row_ids() {
        use arrow_array::Int32Array;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("value", DataType::Int32, false),
        ]));

        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A", "B", "C"])),
                Arc::new(Int32Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();

        let mut ds = Dataset::write(
            Box::new(RecordBatchIterator::new([Ok(initial)], schema.clone())),
            "memory://test_6877",
            Some(WriteParams {
                mode: WriteMode::Overwrite,
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        ds.create_index(
            &["id"],
            IndexType::Scalar,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

        // First merge_insert: A 1 -> 11.
        let update_a = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(Int32Array::from(vec![11])),
            ],
        )
        .unwrap();
        let (ds, _) = MergeInsertBuilder::try_new(Arc::new(ds), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap()
            .execute_reader(Box::new(RecordBatchIterator::new(
                [Ok(update_a)],
                schema.clone(),
            )))
            .await
            .unwrap();

        // Second merge_insert: A 11 -> 22. Used to fail before the fix.
        let update_a_again = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(vec!["A"])),
                Arc::new(Int32Array::from(vec![22])),
            ],
        )
        .unwrap();
        let (ds, _) = MergeInsertBuilder::try_new(ds, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap()
            .execute_reader(Box::new(RecordBatchIterator::new(
                [Ok(update_a_again)],
                schema.clone(),
            )))
            .await
            .unwrap();

        // Sanity check: A's value is now 22.
        let batches = ds
            .scan()
            .filter("id = 'A'")
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let combined = concat_batches(&schema, &batches).unwrap();
        assert_eq!(combined.num_rows(), 1);
        let values = combined
            .column_by_name("value")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(values.value(0), 22);
    }

    // Regression test: partial-schema merge_insert followed by update (deleting all rows
    // in a fragment) followed by partial merge_insert should not produce
    // "fragment id N does not exist" errors.
    //
    // The bug: stale btree entries reference the deleted fragment. The deletion mask doesn't
    // block those addresses because the fragment isn't in the index bitmap. TakeExec tries
    // to read from a non-existent fragment.
    #[tokio::test]
    async fn test_partial_merge_insert_stale_index_fragment_not_exist() {
        let dataset = create_indexed_3frag_dataset().await;

        // Step 2: Partial merge_insert on fragment 1 rows -> fragment 1 drops from bitmap
        let dataset = partial_merge_insert(dataset, 100..200, 999.0).await;

        // Step 3: Update all rows that were in fragment 1, causing fragment 1 to be
        // fully deleted and replaced by a new fragment.
        let update_result = crate::dataset::UpdateBuilder::new(Arc::new((*dataset).clone()))
            .update_where("id >= 'id-0100' AND id < 'id-0200'")
            .unwrap()
            .set("category", "'B'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let dataset = update_result.new_dataset;

        // Step 4: Partial merge_insert on the same rows.
        // This should succeed, not fail with "fragment does not exist".
        let dataset = partial_merge_insert(dataset, 100..200, 888.0).await;

        // Verify correctness
        let batches = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let all_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("value_a", DataType::Float64, false),
            Field::new("value_b", DataType::Float64, false),
        ]));
        let combined = concat_batches(&all_schema, &batches).unwrap();
        assert_eq!(combined.num_rows(), 300);
    }

    // Regression test: partial-schema merge_insert followed by update (deleting SOME rows
    // in a fragment) followed by partial merge_insert should not produce
    // "RecordBatch size mismatch" errors.
    //
    // The bug: stale btree entries reference deleted rows in a fragment that still exists.
    // The deletion mask doesn't block those addresses (fragment not in bitmap). TakeExec
    // reads the fragment but the rows have deletion markers, returning 0 rows where N
    // were expected.
    #[tokio::test]
    async fn test_partial_merge_insert_stale_index_batch_size_mismatch() {
        let dataset = create_indexed_3frag_dataset().await;

        // Step 2: Partial merge_insert on fragment 1 rows -> fragment 1 drops from bitmap
        let dataset = partial_merge_insert(dataset, 100..200, 999.0).await;

        // Step 3: Update HALF of the rows that were in fragment 1. Fragment 1 survives
        // but the updated rows are deleted from it (moved to a new fragment).
        let update_result = crate::dataset::UpdateBuilder::new(Arc::new((*dataset).clone()))
            .update_where("id >= 'id-0100' AND id < 'id-0150'")
            .unwrap()
            .set("category", "'B'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let dataset = update_result.new_dataset;

        // Step 4: Partial merge_insert targeting the rows that were updated (and thus
        // deleted from fragment 1). Should succeed, not fail with batch size mismatch.
        let dataset = partial_merge_insert(dataset, 100..150, 888.0).await;

        // Verify correctness
        let batches = dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let all_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("value_a", DataType::Float64, false),
            Field::new("value_b", DataType::Float64, false),
        ]));
        let combined = concat_batches(&all_schema, &batches).unwrap();
        assert_eq!(combined.num_rows(), 300);
    }

    // Regression test: after a partial-schema merge_insert drops a fragment from the vector
    // index bitmap, a vector search should not return duplicate rows. The stale vector index
    // data still references the dropped fragment, and the scanner also flat-scans unindexed
    // fragments, causing the same rows to appear from both paths.
    #[tokio::test]
    async fn test_partial_merge_insert_stale_vector_index_duplicates() {
        let dim = 4i32;
        let rows_per_frag = 10usize;
        let num_frags = 3usize;
        let total_rows = rows_per_frag * num_frags;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
                false,
            ),
        ]));

        let make_batch = |frag_idx: usize, offset: f32| {
            let start = frag_idx * rows_per_frag;
            let ids: Vec<String> = (start..start + rows_per_frag)
                .map(|j| format!("id-{j:04}"))
                .collect();
            let cats: Vec<&str> = vec!["A"; rows_per_frag];
            let values: Vec<f32> = (0..rows_per_frag * dim as usize)
                .map(|i| (start * dim as usize + i) as f32 + offset)
                .collect();
            let vectors =
                FixedSizeListArray::try_new_from_values(Float32Array::from(values), dim).unwrap();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(StringArray::from(cats)),
                    Arc::new(vectors),
                ],
            )
            .unwrap()
        };

        // Write 3 fragments
        let batch0 = make_batch(0, 0.0);
        let reader = Box::new(RecordBatchIterator::new([Ok(batch0)], schema.clone()));
        let mut ds = Dataset::write(reader, "memory://vector_stale_test", None)
            .await
            .unwrap();
        for frag_idx in 1..num_frags {
            let batch = make_batch(frag_idx, 0.0);
            let reader = Box::new(RecordBatchIterator::new([Ok(batch)], schema.clone()));
            ds.append(reader, None).await.unwrap();
        }

        // Create IVF_FLAT vector index on vec
        let params = VectorIndexParams::ivf_flat(1, MetricType::L2);
        ds.create_index(&["vec"], IndexType::Vector, None, &params, false)
            .await
            .unwrap();

        let ds = Arc::new(ds);

        // Partial merge_insert with (id, vec) on fragment 1 rows - slightly different vectors.
        // This drops fragment 1 from the vector index bitmap.
        let frag1_start = rows_per_frag;
        let ids: Vec<String> = (frag1_start..frag1_start + rows_per_frag)
            .map(|j| format!("id-{j:04}"))
            .collect();
        let sub_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
                false,
            ),
        ]));
        let values: Vec<f32> = (0..rows_per_frag * dim as usize)
            .map(|i| (frag1_start * dim as usize + i) as f32 + 0.5)
            .collect();
        let vectors =
            FixedSizeListArray::try_new_from_values(Float32Array::from(values), dim).unwrap();
        let update_batch = RecordBatch::try_new(
            sub_schema.clone(),
            vec![Arc::new(StringArray::from(ids)), Arc::new(vectors)],
        )
        .unwrap();
        let reader = Box::new(RecordBatchIterator::new([Ok(update_batch)], sub_schema));
        let (ds, _) = MergeInsertBuilder::try_new(ds, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap()
            .execute_reader(reader)
            .await
            .unwrap();

        // KNN search with k = total_rows to retrieve all rows
        let query: Float32Array = (0..dim)
            .map(|i| (frag1_start * dim as usize + i as usize) as f32 + 0.5)
            .collect();
        let results = ds
            .scan()
            .nearest("vec", &query, total_rows)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        // Check no duplicate ids
        let ids = results
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let unique_ids: std::collections::HashSet<&str> =
            (0..ids.len()).map(|i| ids.value(i)).collect();
        assert_eq!(
            unique_ids.len(),
            ids.len(),
            "Found duplicate ids in KNN results: {} unique out of {} total",
            unique_ids.len(),
            ids.len()
        );
    }

    // Regression test: after a partial-schema merge_insert drops a fragment from the FTS
    // index bitmap, a full text search should not return duplicate rows. The stale inverted
    // index data still references the dropped fragment, and the scanner also flat-scans
    // unindexed fragments, causing the same rows to appear from both paths.
    #[tokio::test]
    async fn test_partial_merge_insert_stale_fts_index_duplicates() {
        let rows_per_frag = 10usize;
        let num_frags = 3usize;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
        ]));

        let make_batch = |frag_idx: usize| {
            let start = frag_idx * rows_per_frag;
            let ids: Vec<String> = (start..start + rows_per_frag)
                .map(|j| format!("id-{j:04}"))
                .collect();
            let cats: Vec<&str> = vec!["A"; rows_per_frag];
            // Every row contains "common" so we can search for it and expect all rows
            let texts: Vec<String> = (start..start + rows_per_frag)
                .map(|j| format!("common unique{j:04}"))
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(StringArray::from(cats)),
                    Arc::new(StringArray::from(texts)),
                ],
            )
            .unwrap()
        };

        // Write 3 fragments
        let batch0 = make_batch(0);
        let reader = Box::new(RecordBatchIterator::new([Ok(batch0)], schema.clone()));
        let mut ds = Dataset::write(reader, "memory://fts_stale_test", None)
            .await
            .unwrap();
        for frag_idx in 1..num_frags {
            let batch = make_batch(frag_idx);
            let reader = Box::new(RecordBatchIterator::new([Ok(batch)], schema.clone()));
            ds.append(reader, None).await.unwrap();
        }

        // Create inverted index on text
        let params = InvertedIndexParams::default();
        ds.create_index(&["text"], IndexType::Inverted, None, &params, true)
            .await
            .unwrap();

        let ds = Arc::new(ds);

        // Partial merge_insert with (id, text) on fragment 1 rows.
        // Text still contains "common" so FTS will find them via both paths.
        // This drops fragment 1 from the inverted index bitmap.
        let frag1_start = rows_per_frag;
        let ids: Vec<String> = (frag1_start..frag1_start + rows_per_frag)
            .map(|j| format!("id-{j:04}"))
            .collect();
        let texts: Vec<String> = (frag1_start..frag1_start + rows_per_frag)
            .map(|j| format!("common updated{j:04}"))
            .collect();
        let sub_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let update_batch = RecordBatch::try_new(
            sub_schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(texts)),
            ],
        )
        .unwrap();
        let reader = Box::new(RecordBatchIterator::new([Ok(update_batch)], sub_schema));
        let (ds, _) = MergeInsertBuilder::try_new(ds, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap()
            .execute_reader(reader)
            .await
            .unwrap();

        // FTS search for "common" — every row should match exactly once
        let query = FullTextSearchQuery::new("common".to_string());
        let results = ds
            .scan()
            .full_text_search(query)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        // Check no duplicate ids
        let ids = results
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let unique_ids: std::collections::HashSet<&str> =
            (0..ids.len()).map(|i| ids.value(i)).collect();
        assert_eq!(
            unique_ids.len(),
            ids.len(),
            "Found duplicate ids in FTS results: {} unique out of {} total",
            unique_ids.len(),
            ids.len()
        );
        // Also verify we got all rows
        assert_eq!(
            unique_ids.len(),
            rows_per_frag * num_frags,
            "Expected {} rows but got {}",
            rows_per_frag * num_frags,
            unique_ids.len()
        );
    }

    // Companion regression test for issue #6877 on the FTS path.
    //
    // The FTS prefilter shares `do_create_deletion_mask_row_id` with the
    // scalar-index path, so the same stable-row-id bypass that produced
    // duplicate rows in merge_insert can produce duplicate hits in FTS search
    // after a merge_insert moves rows to unindexed fragments. This test pins
    // the contract for the FTS consumer.
    #[tokio::test]
    async fn test_issue_6877_fts_no_duplicates_stable_row_ids() {
        let rows_per_frag = 10usize;
        let num_frags = 3usize;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("text", DataType::Utf8, false),
        ]));

        let make_batch = |frag_idx: usize| {
            let start = frag_idx * rows_per_frag;
            let ids: Vec<String> = (start..start + rows_per_frag)
                .map(|j| format!("id-{j:04}"))
                .collect();
            let texts: Vec<String> = (start..start + rows_per_frag)
                .map(|j| format!("common unique{j:04}"))
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(StringArray::from(texts)),
                ],
            )
            .unwrap()
        };

        let batch0 = make_batch(0);
        let reader = Box::new(RecordBatchIterator::new([Ok(batch0)], schema.clone()));
        let mut ds = Dataset::write(
            reader,
            "memory://fts_stable_row_id_test",
            Some(WriteParams {
                mode: WriteMode::Overwrite,
                enable_stable_row_ids: true,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        for frag_idx in 1..num_frags {
            let batch = make_batch(frag_idx);
            let reader = Box::new(RecordBatchIterator::new([Ok(batch)], schema.clone()));
            ds.append(reader, None).await.unwrap();
        }

        let params = InvertedIndexParams::default();
        ds.create_index(&["text"], IndexType::Inverted, None, &params, true)
            .await
            .unwrap();

        // Full-schema merge_insert rewriting fragment 1's rows. After this,
        // the original locations are tombstoned and the new locations live in
        // a new (unindexed) fragment; the stable_row_ids are preserved.
        let frag1_start = rows_per_frag;
        let ids: Vec<String> = (frag1_start..frag1_start + rows_per_frag)
            .map(|j| format!("id-{j:04}"))
            .collect();
        let texts: Vec<String> = (frag1_start..frag1_start + rows_per_frag)
            .map(|j| format!("common updated{j:04}"))
            .collect();
        let update_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(ids)),
                Arc::new(StringArray::from(texts)),
            ],
        )
        .unwrap();
        let reader = Box::new(RecordBatchIterator::new([Ok(update_batch)], schema.clone()));
        let (ds, _) = MergeInsertBuilder::try_new(Arc::new(ds), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap()
            .execute_reader(reader)
            .await
            .unwrap();

        // FTS search for "common" — every row should match exactly once.
        let query = FullTextSearchQuery::new("common".to_string());
        let results = ds
            .scan()
            .full_text_search(query)
            .unwrap()
            .try_into_batch()
            .await
            .unwrap();

        let ids = results
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let unique_ids: std::collections::HashSet<&str> =
            (0..ids.len()).map(|i| ids.value(i)).collect();
        assert_eq!(
            unique_ids.len(),
            ids.len(),
            "Found duplicate ids in FTS results: {} unique out of {} total",
            unique_ids.len(),
            ids.len()
        );
        assert_eq!(
            unique_ids.len(),
            rows_per_frag * num_frags,
            "Expected {} rows but got {}",
            rows_per_frag * num_frags,
            unique_ids.len()
        );
    }

    // Regression test: after a partial-schema merge_insert invalidates a fragment,
    // compaction should succeed and subsequent searches should return correct results.
    //
    // The compaction planner separates indexed and unindexed fragments into different
    // groups. After invalidating the middle fragment, the indexed fragments on either
    // side form separate compactable groups. After compaction the old invalidated
    // fragment ID may remain in invalidated_fragment_bitmap but this is harmless
    // because the fragment no longer exists and no index results reference it.
    #[tokio::test]
    async fn test_compaction_after_invalidated_fragment() {
        use crate::dataset::optimize::{CompactionOptions, compact_files};

        // Use 5 small fragments so that after invalidating the middle one (fragment 2),
        // the planner has enough neighbors to form compactable groups on each side:
        // {0,1} (indexed) and {3,4} (indexed), with {2} (unindexed) separate.
        let rows_per_frag = 20;
        let num_frags = 5;
        let total_rows = rows_per_frag * num_frags;
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Utf8, false),
            Field::new("category", DataType::Utf8, false),
            Field::new("value_a", DataType::Float64, false),
            Field::new("value_b", DataType::Float64, false),
        ]));

        let make_batch = |frag_idx: usize| {
            let start = frag_idx * rows_per_frag;
            let ids: Vec<String> = (start..start + rows_per_frag)
                .map(|j| format!("id-{j:04}"))
                .collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(ids)),
                    Arc::new(StringArray::from(vec!["A"; rows_per_frag])),
                    Arc::new(Float64Array::from(
                        (0..rows_per_frag).map(|i| i as f64).collect::<Vec<_>>(),
                    )),
                    Arc::new(Float64Array::from(
                        (0..rows_per_frag)
                            .map(|i| i as f64 * 0.1)
                            .collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap()
        };

        let batch0 = make_batch(0);
        let reader = Box::new(RecordBatchIterator::new([Ok(batch0)], schema.clone()));
        let mut ds = Dataset::write(reader, "memory://compaction_test", None)
            .await
            .unwrap();
        for frag_idx in 1..num_frags {
            let batch = make_batch(frag_idx);
            let reader = Box::new(RecordBatchIterator::new([Ok(batch)], schema.clone()));
            ds.append(reader, None).await.unwrap();
        }
        ds.create_index(
            &["id"],
            IndexType::BTree,
            None,
            &ScalarIndexParams::default(),
            false,
        )
        .await
        .unwrap();

        let ds = Arc::new(ds);

        // Invalidate fragment 2 (the middle one)
        let frag2_start = 2 * rows_per_frag;
        let ds = partial_merge_insert(ds, frag2_start..frag2_start + rows_per_frag, 999.0).await;

        // Verify pre-compaction state
        let indices = ds.load_indices().await.unwrap();
        let idx = indices.iter().find(|i| i.name == "id_idx").unwrap();
        assert!(!idx.fragment_bitmap.as_ref().unwrap().contains(2));

        // Run compaction with a target that forces merging of the small fragments.
        let mut ds = (*ds).clone();
        let opts = CompactionOptions {
            target_rows_per_fragment: total_rows,
            ..Default::default()
        };
        compact_files(&mut ds, opts, None).await.unwrap();

        // The indexed fragments (0,1 and 3,4) should be compacted.
        // Fragment 2 (unindexed) may or may not be compacted on its own.
        // Either way, the old fragment IDs in the bitmap should be replaced.
        let indices = ds.load_indices().await.unwrap();
        let idx = indices.iter().find(|i| i.name == "id_idx").unwrap();
        let bitmap = idx.fragment_bitmap.as_ref().unwrap();
        for &old_id in &[0u32, 1, 3, 4] {
            assert!(
                !bitmap.contains(old_id),
                "Old indexed fragment {} should not be in bitmap after compaction",
                old_id
            );
        }
        assert!(
            !bitmap.is_empty(),
            "Bitmap should have new compacted fragments"
        );

        // The invalidated bitmap may still reference old fragment 2.
        // This is harmless — fragment 2 no longer exists (or was compacted into
        // a new fragment), so blocking it is a no-op.

        // Verify search works correctly despite stale invalidated entries.
        let ds = Arc::new(ds);
        let ds = partial_merge_insert(ds, frag2_start..frag2_start + rows_per_frag, 888.0).await;

        // All rows present
        let batches = ds
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let combined = concat_batches(&schema, &batches).unwrap();
        assert_eq!(combined.num_rows(), total_rows);

        // Updated rows have correct value
        let result = ds
            .scan()
            .filter(&format!(
                "id >= 'id-{:04}' AND id < 'id-{:04}'",
                frag2_start,
                frag2_start + rows_per_frag
            ))
            .unwrap()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let result = concat_batches(&schema, &result).unwrap();
        assert_eq!(result.num_rows(), rows_per_frag);
        let values = result
            .column_by_name("value_a")
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        for i in 0..rows_per_frag {
            assert_eq!(values.value(i), 888.0, "row {i} should have value_a=888.0");
        }
    }

    fn count_data_files(base_dir: &str) -> usize {
        let data_dir = std::path::Path::new(base_dir).join("data");
        if !data_dir.exists() {
            return 0;
        }
        std::fs::read_dir(data_dir)
            .unwrap()
            .filter(|e| e.as_ref().unwrap().path().is_file())
            .count()
    }

    /// Site 3 in PR #6320: when `MergeInsertJob::apply_deletions` fails after
    /// the new fragments have been written, the new data files must be cleaned up.
    #[tokio::test]
    async fn test_merge_insert_cleans_up_data_on_apply_deletions_failure() {
        use crate::utils::test::FailingProxyStore;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(0..30)),
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    "foo", 30,
                ))),
            ],
        )
        .unwrap();

        let test_dir = TempStrDir::default();
        let test_uri = test_dir.as_str();
        // Prefix `/` so Windows drive letters (e.g. `C:`) don't get parsed as
        // the URL authority.
        let path_prefix = if test_uri.starts_with('/') { "" } else { "/" };
        let routed_uri = format!("file-object-store://{path_prefix}{test_uri}");

        let batches = RecordBatchIterator::new([Ok(initial)], schema.clone());
        let mut dataset = Dataset::write(
            batches,
            &routed_uri,
            Some(WriteParams {
                max_rows_per_file: 10,
                data_storage_version: Some(LanceFileVersion::V2_1),
                ..Default::default()
            }),
        )
        .await
        .unwrap();

        // Create a scalar index on the join key. This forces the merge insert
        // to take the slow (non-fast) path, which is the path that has the
        // post-write cleanup we want to exercise.
        dataset
            .create_index(
                &["id"],
                IndexType::Scalar,
                None,
                &ScalarIndexParams::default(),
                false,
            )
            .await
            .unwrap();

        let baseline_files = count_data_files(test_uri);
        assert!(baseline_files > 0);

        let failing = Arc::new(FailingProxyStore::new());
        failing.fail_when("put", "_deletions", "injected deletions failure");
        failing.fail_when("put_multipart", "_deletions", "injected deletions failure");

        let dataset = DatasetBuilder::from_uri(&routed_uri)
            .with_read_params(ReadParams {
                store_options: Some(ObjectStoreParams {
                    object_store_wrapper: Some(failing.clone()),
                    ..Default::default()
                }),
                ..Default::default()
            })
            .load()
            .await
            .unwrap();

        // Update existing keys (5..15 already exist) to force the apply_deletions path.
        let new_data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from_iter_values(5..15)),
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    "bar", 10,
                ))),
            ],
        )
        .unwrap();
        let new_reader = Box::new(RecordBatchIterator::new([Ok(new_data)], schema.clone()));

        let job = MergeInsertBuilder::try_new(Arc::new(dataset), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .try_build()
            .unwrap();

        let result = job.execute_reader(new_reader).await;
        assert!(
            result.is_err(),
            "Merge insert should fail when deletion-file write fails"
        );

        assert_eq!(
            count_data_files(test_uri),
            baseline_files,
            "Newly written merge-insert data files should be cleaned up on apply_deletions failure"
        );
    }

    #[tokio::test]
    async fn test_merge_insert_full_fragment_rewrite_with_json_columns() {
        // This test verifies the "all rows updated" fast path in handle_fragment
        // correctly converts Arrow JSON (Utf8) to Lance JSON (LargeBinary/JSONB)
        // before writing. Without conversion, the file would have Utf8 data (i32
        // offsets) but schema says LargeBinary (i64 offsets), causing decoder panic
        // on subsequent reads.
        //
        // To trigger the fast path we need:
        // 1. Subschema update (not all columns) → forces v1 update_fragments path
        // 2. ALL rows in a fragment updated → triggers the fast path
        use lance_arrow::ARROW_EXT_NAME_KEY;
        use lance_arrow::json::{ARROW_JSON_EXT_NAME, is_arrow_json_field};

        let test_dir = TempStrDir::default();
        let mut json_metadata = HashMap::new();
        json_metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Int64, true),
            Field::new("meta", DataType::Utf8, true).with_metadata(json_metadata.clone()),
        ]));
        // Small fragment so ALL rows will be updated
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(StringArray::from(vec![
                    r#"{"x":1}"#,
                    r#"{"x":2}"#,
                    r#"{"x":3}"#,
                ])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema.clone());
        let write_params = WriteParams {
            data_storage_version: Some(LanceFileVersion::V2_2),
            ..Default::default()
        };
        let dataset = Arc::new(
            Dataset::write(reader, test_dir.as_ref(), Some(write_params))
                .await
                .unwrap(),
        );
        assert_eq!(dataset.get_fragments().len(), 1);

        // Subschema update: only provide [id, meta] (missing "name" and "score")
        // This forces the v1 path (update_fragments) instead of v2 (create_plan).
        // Update ALL rows → triggers the "all rows updated" fast path.
        let update_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("meta", DataType::Utf8, true).with_metadata(json_metadata),
        ]));
        let update_batch = RecordBatch::try_new(
            update_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])), // all rows
                Arc::new(StringArray::from(vec![
                    r#"{"updated":true,"id":1}"#,
                    r#"{"updated":true,"id":2}"#,
                    r#"{"updated":true,"id":3}"#,
                ])),
            ],
        )
        .unwrap();
        let update_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(update_batch)],
            update_schema,
        ));
        let stream = reader_to_stream(update_reader);

        // Execute merge_insert with subschema
        let mut builder =
            MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()]).unwrap();
        builder.when_matched(WhenMatched::UpdateAll);
        builder.when_not_matched(WhenNotMatched::DoNothing);
        let job = builder.try_build().unwrap();
        let (updated_dataset, stats) = job.execute(stream).await.unwrap();

        assert_eq!(stats.num_updated_rows, 3);

        // Critical: read the data back. Without the fix, this would PANIC with:
        // "the offset of the new Buffer cannot exceed the existing Length:
        //  slice offset=0 Length=N selfLen=N/2"
        let batches = updated_dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let result = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(result.num_rows(), 3);

        // Verify JSON column is in Arrow JSON format (Utf8) on read
        let result_schema = result.schema();
        let meta_field = result_schema.field_with_name("meta").unwrap();
        assert!(
            is_arrow_json_field(meta_field),
            "Expected Arrow JSON (Utf8 + arrow.json), got {:?}",
            meta_field
        );

        // Verify data correctness
        let metas = result
            .column_by_name("meta")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("meta should be StringArray after read conversion");
        for i in 0..3 {
            let val = metas.value(i);
            assert!(
                val.contains("updated"),
                "row {} should have updated meta, got: {}",
                i,
                val
            );
        }

        // Verify non-updated columns are preserved
        let scores = result
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(scores.values(), &[10, 20, 30]);

        // Also verify via take (exercises the take read conversion path)
        let take_result = updated_dataset
            .take(&[0, 1, 2], updated_dataset.schema().clone())
            .await
            .unwrap();
        let take_schema = take_result.schema();
        let take_meta_field = take_schema.field_with_name("meta").unwrap();
        assert!(
            is_arrow_json_field(take_meta_field),
            "take() should return Arrow JSON, got {:?}",
            take_meta_field
        );
    }

    #[tokio::test]
    async fn test_merge_insert_subschema_with_json_columns() {
        use lance_arrow::ARROW_EXT_NAME_KEY;
        use lance_arrow::json::ARROW_JSON_EXT_NAME;

        // Create a dataset with an Arrow JSON extension column
        let test_dir = TempStrDir::default();
        let mut json_metadata = HashMap::new();
        json_metadata.insert(
            ARROW_EXT_NAME_KEY.to_string(),
            ARROW_JSON_EXT_NAME.to_string(),
        );
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("score", DataType::Int64, true),
            Field::new("meta", DataType::Utf8, true).with_metadata(json_metadata.clone()),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c", "d", "e"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
                Arc::new(StringArray::from(vec![
                    r#"{"x":1}"#,
                    r#"{"x":2}"#,
                    r#"{"x":3}"#,
                    r#"{"x":4}"#,
                    r#"{"x":5}"#,
                ])),
            ],
        )
        .unwrap();
        let reader = RecordBatchIterator::new(vec![Ok(batch)], schema);
        let dataset = Arc::new(
            Dataset::write(reader, test_dir.as_ref(), None)
                .await
                .unwrap(),
        );

        // Perform a subschema merge_insert: only update "meta" column (JSON type)
        // This exercises the update_fragments path with interleave_batches
        let update_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("meta", DataType::Utf8, true).with_metadata(json_metadata),
        ]));
        let update_batch = RecordBatch::try_new(
            update_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 4])),
                Arc::new(StringArray::from(vec![
                    r#"{"updated":true,"id":2}"#,
                    r#"{"updated":true,"id":4}"#,
                ])),
            ],
        )
        .unwrap();
        let update_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(update_batch)],
            update_schema,
        ));
        let stream = reader_to_stream(update_reader);

        // Execute merge_insert with subschema (only id + meta columns)
        let mut builder =
            MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()]).unwrap();
        builder.when_matched(WhenMatched::UpdateAll);
        builder.when_not_matched(WhenNotMatched::DoNothing);
        let job = builder.try_build().unwrap();
        let (updated_dataset, stats) = job.execute(stream).await.unwrap();

        // Verify: the merge should not fail with type mismatch
        assert_eq!(stats.num_updated_rows, 2);

        // Read back and verify the JSON column was updated correctly
        let batches = updated_dataset
            .scan()
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let result = concat_batches(&batches[0].schema(), &batches).unwrap();
        assert_eq!(result.num_rows(), 5);

        // Verify the "score" column (not in update) is preserved, and "meta" updated
        let ids = result
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let scores = result
            .column_by_name("score")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let metas = result
            .column_by_name("meta")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        for i in 0..5 {
            let id = ids.value(i);
            let score = scores.value(i);
            let meta = metas.value(i);
            // score = id * 10, regardless of row order
            assert_eq!(score, id * 10, "id={} score mismatch", id);
            if id == 2 || id == 4 {
                assert!(
                    meta.contains("updated"),
                    "id={} should have updated meta, got: {}",
                    id,
                    meta
                );
            } else {
                assert!(
                    meta.contains("\"x\""),
                    "id={} should have original meta, got: {}",
                    id,
                    meta
                );
            }
        }
    }

    fn id_value_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new("value", DataType::UInt32, false),
        ]))
    }

    /// `execute_provider` is the canonical entry point; a `MemTable` source merges
    /// the same way a stream does.
    #[tokio::test]
    async fn test_merge_insert_execute_provider() {
        let initial =
            record_batch!(("id", UInt32, [0, 1, 2]), ("value", UInt32, [0, 0, 0])).unwrap();
        let dataset = Arc::new(
            InsertBuilder::new("memory://")
                .execute(vec![initial])
                .await
                .unwrap(),
        );

        // Update id=1, insert id=3.
        let new_data = record_batch!(("id", UInt32, [1, 3]), ("value", UInt32, [10, 30])).unwrap();
        let provider: Arc<dyn TableProvider> = Arc::new(
            datafusion::datasource::MemTable::try_new(new_data.schema(), vec![vec![new_data]])
                .unwrap(),
        );

        let (merged, stats) = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute_provider(provider)
            .await
            .unwrap();

        assert_eq!(stats.num_updated_rows, 1);
        assert_eq!(stats.num_inserted_rows, 1);

        let batch = merged.scan().try_into_batch().await.unwrap();
        let ids = batch["id"].as_primitive::<UInt32Type>();
        let values = batch["value"].as_primitive::<UInt32Type>();
        let merged_rows: HashMap<u32, u32> = ids
            .values()
            .iter()
            .zip(values.values().iter())
            .map(|(id, value)| (*id, *value))
            .collect();
        assert_eq!(
            merged_rows,
            HashMap::from([(0, 0), (1, 10), (2, 0), (3, 30)])
        );
    }

    /// `execute_batches` merges materialized batches; multiple batches are spread
    /// across partitions and merged correctly.
    #[tokio::test]
    async fn test_merge_insert_execute_batches() {
        let initial =
            record_batch!(("id", UInt32, [0, 1, 2]), ("value", UInt32, [0, 0, 0])).unwrap();
        let dataset = Arc::new(
            InsertBuilder::new("memory://")
                .execute(vec![initial])
                .await
                .unwrap(),
        );

        // Two batches: update id=1 (batch 0), insert id=3 (batch 1).
        let batch0 = record_batch!(("id", UInt32, [1]), ("value", UInt32, [10])).unwrap();
        let batch1 = record_batch!(("id", UInt32, [3]), ("value", UInt32, [30])).unwrap();

        let (merged, stats) = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute_batches(vec![batch0, batch1])
            .await
            .unwrap();

        assert_eq!(stats.num_updated_rows, 1);
        assert_eq!(stats.num_inserted_rows, 1);

        let batch = merged.scan().try_into_batch().await.unwrap();
        let ids = batch["id"].as_primitive::<UInt32Type>();
        let values = batch["value"].as_primitive::<UInt32Type>();
        let merged_rows: HashMap<u32, u32> = ids
            .values()
            .iter()
            .zip(values.values().iter())
            .map(|(id, value)| (*id, *value))
            .collect();
        assert_eq!(
            merged_rows,
            HashMap::from([(0, 0), (1, 10), (2, 0), (3, 30)])
        );
    }

    /// An empty batch list still produces a valid (single, empty) partition, so the
    /// merge is a no-op and the target is unchanged.
    #[tokio::test]
    async fn test_merge_insert_execute_batches_empty() {
        let initial =
            record_batch!(("id", UInt32, [0, 1, 2]), ("value", UInt32, [0, 0, 0])).unwrap();
        let dataset = Arc::new(
            InsertBuilder::new("memory://")
                .execute(vec![initial])
                .await
                .unwrap(),
        );

        let (merged, stats) = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap()
            .execute_batches(vec![])
            .await
            .unwrap();

        assert_eq!(stats.num_updated_rows, 0);
        assert_eq!(stats.num_inserted_rows, 0);

        let batch = merged.scan().try_into_batch().await.unwrap();
        let ids = batch["id"].as_primitive::<UInt32Type>();
        let values = batch["value"].as_primitive::<UInt32Type>();
        let merged_rows: HashMap<u32, u32> = ids
            .values()
            .iter()
            .zip(values.values().iter())
            .map(|(id, value)| (*id, *value))
            .collect();
        assert_eq!(merged_rows, HashMap::from([(0, 0), (1, 0), (2, 0)]));
    }

    fn collect_exact_row_counts(plan: &Arc<dyn ExecutionPlan>, out: &mut Vec<usize>) {
        if let Ok(stats) = plan.partition_statistics(None)
            && let datafusion::common::stats::Precision::Exact(n) = stats.num_rows
        {
            out.push(n);
        }
        for child in plan.children() {
            collect_exact_row_counts(child, out);
        }
    }

    /// Use case 3: planning against the provider exposes its exact source
    /// statistics to the optimizer.
    #[tokio::test]
    async fn test_merge_insert_source_statistics_in_plan() {
        let schema = id_value_schema();
        let target_rows = 1000u32;
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from_iter_values(0..target_rows)),
                Arc::new(UInt32Array::from_iter_values(std::iter::repeat_n(
                    0,
                    target_rows as usize,
                ))),
            ],
        )
        .unwrap();
        let dataset = Arc::new(
            InsertBuilder::new("memory://")
                .execute(vec![initial])
                .await
                .unwrap(),
        );

        // A small source whose exact row count is distinct from the target's.
        let source_rows = 10usize;
        let new_data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from_iter_values(0..source_rows as u32)),
                Arc::new(UInt32Array::from_iter_values(std::iter::repeat_n(
                    1,
                    source_rows,
                ))),
            ],
        )
        .unwrap();
        let provider: Arc<dyn TableProvider> =
            Arc::new(MemTable::try_new(schema.clone(), vec![vec![new_data]]).unwrap());

        let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .try_build()
            .unwrap();

        // The provider's exact row count reaches the plan's statistics.
        let plan = job.create_plan(provider).await.unwrap();
        let mut row_counts = Vec::new();
        collect_exact_row_counts(&plan, &mut row_counts);
        assert!(
            row_counts.contains(&source_rows),
            "source provider's exact row count ({source_rows}) should reach the plan; got {row_counts:?}"
        );
    }

    /// With a one-shot stream source and `spill_for_retry(false)`, a commit
    /// conflict fails fast instead of replaying the stream. The non-replayable
    /// one-shot provider must be scanned exactly once (scanning it twice would
    /// panic), proving retries are disabled even though `conflict_retries > 0`.
    #[tokio::test]
    async fn test_merge_insert_spill_for_retry_false_fails_fast() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false).with_metadata(
                vec![(
                    "lance-schema:unenforced-primary-key".to_string(),
                    "true".to_string(),
                )]
                .into_iter()
                .collect(),
            ),
            Field::new("value", DataType::UInt32, false),
        ]));
        let initial = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![0, 1, 2, 3])),
                Arc::new(UInt32Array::from(vec![0, 0, 0, 0])),
            ],
        )
        .unwrap();
        let dataset = Arc::new(
            InsertBuilder::new("memory://")
                .execute(vec![initial])
                .await
                .unwrap(),
        );

        // Merge insert job based on version 1, with retries enabled but spilling off.
        let new_data = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![100])),
                Arc::new(UInt32Array::from(vec![1])),
            ],
        )
        .unwrap();
        let job = MergeInsertBuilder::try_new(dataset.clone(), vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .conflict_retries(10)
            .spill_for_retry(false)
            .try_build()
            .unwrap();

        // An append commits first (version 2), so the merge built on version 1 hits
        // an unresolvable conflict on commit.
        let append_batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(UInt32Array::from(vec![50])),
                Arc::new(UInt32Array::from(vec![2])),
            ],
        )
        .unwrap();
        InsertBuilder::new(dataset.clone())
            .with_params(&WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            })
            .execute(vec![append_batch])
            .await
            .unwrap();

        let source = RecordBatchStreamAdapter::new(
            schema.clone(),
            futures::stream::iter(vec![Ok(new_data)]),
        );
        let merge_result = job
            .execute(Box::pin(source) as SendableRecordBatchStream)
            .await;

        assert!(
            matches!(
                merge_result,
                Err(crate::Error::TooMuchWriteContention { .. })
            ),
            "Expected fail-fast TooMuchWriteContention, got: {:?}",
            merge_result
        );
    }

    #[tokio::test]
    async fn test_merge_insert_with_blob_v1_source_provides_blob() {
        use arrow_array::LargeBinaryArray;
        use arrow_schema::Schema as ArrowSchema;
        use lance_arrow::BLOB_META_KEY;

        let test_dir = TempStrDir::default();
        let schema = Arc::new(ArrowSchema::new(vec![
            Field::new("blobs", DataType::LargeBinary, true).with_metadata(HashMap::from([(
                BLOB_META_KEY.to_string(),
                "true".to_string(),
            )])),
            Field::new("id", DataType::Int64, true),
            Field::new("other", DataType::Int64, true),
        ]));
        let make_batch = |blob_values: Vec<Option<&[u8]>>, ids, others| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(LargeBinaryArray::from(blob_values)),
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(others)),
                ],
            )
            .unwrap()
        };
        let dataset = Arc::new(
            Dataset::write(
                RecordBatchIterator::new(
                    vec![Ok(make_batch(
                        vec![Some(b"foo"), Some(b"bar")],
                        vec![0, 1],
                        vec![10, 20],
                    ))],
                    schema.clone(),
                ),
                &test_dir,
                Some(WriteParams {
                    data_storage_version: Some(LanceFileVersion::V2_1),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        );
        let source = Box::new(RecordBatchIterator::new(
            vec![Ok(make_batch(
                vec![Some(b"baz"), Some(b"qux")],
                vec![1, 2],
                vec![200, 300],
            ))],
            schema,
        ));

        let job = MergeInsertBuilder::try_new(dataset, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();
        let (new_dataset, _) = job.execute_reader(source).await.unwrap();
        let blobs = new_dataset
            .take_blobs_by_indices(&[0, 1, 2], "blobs")
            .await
            .unwrap();
        assert_eq!(
            blobs[0].as_ref().unwrap().read().await.unwrap().as_ref(),
            b"foo"
        );
        assert_eq!(
            blobs[1].as_ref().unwrap().read().await.unwrap().as_ref(),
            b"baz"
        );
        assert_eq!(
            blobs[2].as_ref().unwrap().read().await.unwrap().as_ref(),
            b"qux"
        );
    }

    #[tokio::test]
    async fn test_merge_insert_with_blob_v2_source_provides_blob() {
        use crate::{BlobArrayBuilder, blob_field};
        use arrow_schema::Schema as ArrowSchema;

        let test_dir = TempStrDir::default();
        let schema = Arc::new(ArrowSchema::new(vec![
            blob_field("blobs", true),
            Field::new("id", DataType::Int64, true),
            Field::new("other", DataType::Int64, true),
        ]));
        let make_batch = |blob_values: &[&[u8]], ids, others| {
            let mut blobs = BlobArrayBuilder::new(blob_values.len());
            for value in blob_values {
                blobs.push_bytes(value).unwrap();
            }
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    blobs.finish().unwrap(),
                    Arc::new(Int64Array::from(ids)),
                    Arc::new(Int64Array::from(others)),
                ],
            )
            .unwrap()
        };
        let dataset = Arc::new(
            Dataset::write(
                RecordBatchIterator::new(
                    vec![Ok(make_batch(&[b"foo", b"bar"], vec![0, 1], vec![10, 20]))],
                    schema.clone(),
                ),
                &test_dir,
                Some(WriteParams {
                    data_storage_version: Some(LanceFileVersion::V2_2),
                    ..Default::default()
                }),
            )
            .await
            .unwrap(),
        );
        let source = Box::new(RecordBatchIterator::new(
            vec![Ok(make_batch(
                &[b"baz", b"qux"],
                vec![1, 2],
                vec![200, 300],
            ))],
            schema,
        ));

        let job = MergeInsertBuilder::try_new(dataset, vec!["id".to_string()])
            .unwrap()
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::InsertAll)
            .try_build()
            .unwrap();
        let (new_dataset, _) = job.execute_reader(source).await.unwrap();
        let blobs = new_dataset
            .take_blobs_by_indices(&[0, 1, 2], "blobs")
            .await
            .unwrap();
        assert_eq!(
            blobs[0].as_ref().unwrap().read().await.unwrap().as_ref(),
            b"foo"
        );
        assert_eq!(
            blobs[1].as_ref().unwrap().read().await.unwrap().as_ref(),
            b"baz"
        );
        assert_eq!(
            blobs[2].as_ref().unwrap().read().await.unwrap().as_ref(),
            b"qux"
        );
    }
}
