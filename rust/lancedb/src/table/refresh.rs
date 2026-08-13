// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Filling computed columns.
//!
//! A row without a value gets one; a row that has one keeps it. Refresh is
//! therefore idempotent and does not observe input mutation -- once a row is
//! filled, changing what the expression reads leaves the stored result alone.
//!
//! Convergence comes from staging nothing when nothing would change, so an
//! expression yielding null settles after one pass rather than re-selecting
//! the same rows forever. Fragments that already cover the column and hold no
//! nulls are skipped without evaluating it at all.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow_array::RecordBatch;
use arrow_schema::Schema as ArrowSchema;
use futures::{Stream, StreamExt, TryStreamExt, stream};
use lance::Dataset;
use lance::dataset::WriteDestination;
use lance::dataset::fragment::FileFragment;
use lance::dataset::transaction::Operation;
use lance_core::ROW_ID;
use lance_core::datatypes::Schema as LanceSchema;
use serde::{Deserialize, Serialize};

use super::NativeTable;
use super::computed_columns::{ComputedColumnKind, computed_column_from_field};
use crate::job::Job;
use crate::{Error, Result};

/// Alias the expression is projected under, so its result and the column's
/// current values can be read side by side.
const COMPUTED_ALIAS: &str = "__lancedb_computed";

/// The result of refreshing a computed column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RefreshColumnResult {
    /// Rows that had a value computed.
    #[serde(default)]
    pub rows_filled: u64,
    /// The commit version associated with the operation.
    #[serde(default)]
    pub version: u64,
}

/// Internal implementation of the refresh logic.
pub(crate) async fn execute_refresh_column(
    table: &NativeTable,
    column: &str,
) -> Result<RefreshColumnResult> {
    table.dataset.ensure_mutable()?;
    let dataset = table.dataset.get().await?;

    let expression = declared_expression(&dataset, column)?;
    let field = dataset
        .schema()
        .field(column)
        .ok_or_else(|| Error::ColumnNotFound {
            name: column.to_string(),
        })?;
    // The dataset's own field, so the identity write_column checks against the
    // manifest holds by construction.
    let column_schema = LanceSchema {
        fields: vec![field.clone()],
        metadata: Default::default(),
    };

    let mut rows_filled = 0u64;
    let mut replacements = Vec::new();
    for fragment in fragments_to_consider(&dataset, column, field.id).await? {
        let Some((filled, values)) =
            fill_fragment(&dataset, &fragment, column, &expression).await?
        else {
            continue;
        };
        replacements.push(fragment.write_column(values, &column_schema).await?);
        // The stream is fully consumed once write_column returns, so the
        // counter holds the fragment's total.
        rows_filled += filled.load(Ordering::Relaxed);
    }

    if replacements.is_empty() {
        return Ok(RefreshColumnResult {
            rows_filled: 0,
            version: dataset.version().version,
        });
    }

    let read_version = dataset.version().version;
    let new_dataset = Dataset::commit(
        WriteDestination::Dataset(dataset.clone()),
        Operation::DataReplacement { replacements },
        Some(read_version),
        None,
        None,
        Arc::new(Default::default()),
        false,
    )
    .await?;

    let version = new_dataset.version().version;
    table.dataset.update(new_dataset);
    Ok(RefreshColumnResult {
        rows_filled,
        version,
    })
}

/// Run the refresh as a [`Job`] in this process.
pub(crate) async fn execute_refresh_column_async(table: &NativeTable, column: &str) -> Result<Job> {
    // Validate before spawning so bad input is reported by this call rather
    // than only by the job.
    table.dataset.ensure_mutable()?;
    let dataset = table.dataset.get().await?;
    declared_expression(&dataset, column)?;
    drop(dataset);

    let table = table.clone();
    let column = column.to_string();
    Ok(Job::spawned(tokio::spawn(async move {
        execute_refresh_column(&table, &column).await?;
        table.bump_freshness();
        Ok(())
    })))
}

/// The SQL expression `column` is declared with.
fn declared_expression(dataset: &Dataset, column: &str) -> Result<String> {
    let schema = ArrowSchema::from(dataset.schema());
    let field = schema
        .field_with_name(column)
        .map_err(|_| Error::ColumnNotFound {
            name: column.to_string(),
        })?;
    let declaration =
        computed_column_from_field(field).ok_or_else(|| Error::NotAComputedColumn {
            name: column.to_string(),
        })?;
    match declaration.kind {
        ComputedColumnKind::Sql { expression } => Ok(expression),
        ComputedColumnKind::Unrecognized { kind } => Err(Error::NotSupported {
            message: format!(
                "computed column '{column}' is defined by '{kind}', which this version of \
                 lancedb cannot evaluate"
            ),
        }),
    }
}

/// Quote `name` as a lance SQL identifier.
///
/// Lance's dialect delimits with backticks, so a double-quoted name would
/// parse as a string literal rather than a column.
fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Fragments that could hold a row needing a value.
///
/// A fragment whose data files do not carry the field cannot hold one that
/// does. One that carries it is asked, since a row rewrite -- an update, or a
/// compaction folding an unfilled fragment into a filled one -- can leave
/// nulls behind a covering file.
async fn fragments_to_consider(
    dataset: &Dataset,
    column: &str,
    field_id: i32,
) -> Result<Vec<FileFragment>> {
    let unfilled = format!("{} IS NULL", quote_identifier(column));
    let mut considered = Vec::new();
    for fragment in dataset.get_fragments() {
        let covered = fragment
            .metadata()
            .files
            .iter()
            .any(|file| file.fields.contains(&field_id));
        if !covered || fragment.count_rows(Some(unfilled.clone())).await? > 0 {
            considered.push(fragment);
        }
    }
    Ok(considered)
}

/// Merge one scanned batch: computed values fill the nulls, existing values
/// are kept, and `filled` advances by the live rows that gained one.
fn merge_batch(
    batch: &RecordBatch,
    column: &str,
    projected: &Arc<ArrowSchema>,
    filled: &AtomicU64,
) -> lance_core::Result<RecordBatch> {
    let missing = |name: &str| {
        lance_core::Error::invalid_input(format!(
            "refreshing a computed column produced no {name} column"
        ))
    };
    let existing = batch
        .column_by_name(column)
        .ok_or_else(|| missing(column))?;
    let computed = batch
        .column_by_name(COMPUTED_ALIAS)
        .ok_or_else(|| missing("expression"))?;
    let row_ids = batch
        .column_by_name(ROW_ID)
        .ok_or_else(|| missing(ROW_ID))?;

    // A row is filled only if it gains a value: an expression yielding null
    // leaves it as unfilled as it was, which is what lets a refresh settle.
    // A deleted row has a null row id; its value is written but not counted.
    let unfilled = arrow::compute::is_null(existing.as_ref())?;
    let gained = (0..unfilled.len())
        .filter(|i| unfilled.value(*i) && row_ids.is_valid(*i) && computed.is_valid(*i))
        .count() as u64;
    filled.fetch_add(gained, Ordering::Relaxed);

    let merged = arrow_select::zip::zip(&unfilled, computed, existing)?;
    Ok(RecordBatch::try_new(projected.clone(), vec![merged])?)
}

/// Compute one fragment's column as a stream, keeping every value it already
/// holds.
///
/// Batches buffer only until the first row gains a value; from there the
/// stream feeds `write_column` a batch at a time, so peak memory is bounded
/// by a batch rather than the fragment's column. `Ok(None)` when no live row
/// gained one -- the whole scan buffered nothing durable and nothing is
/// staged, which is what keeps a refresh from restaging a fragment whose
/// expression yields null. Deleted rows are carried through so the values
/// line up positionally with the fragment's data files; they are never read
/// back, but the column file has to cover them.
///
/// The counter reports the fragment's total only once the returned stream has
/// been fully consumed.
async fn fill_fragment(
    dataset: &Dataset,
    fragment: &FileFragment,
    column: &str,
    expression: &str,
) -> Result<
    Option<(
        Arc<AtomicU64>,
        impl Stream<Item = lance_core::Result<RecordBatch>> + Send + use<>,
    )>,
> {
    let mut scanner = dataset.scan();
    scanner
        .with_fragments(vec![fragment.metadata().clone()])
        .with_row_id()
        .include_deleted_rows()
        .project_with_transform(&[
            (column, quote_identifier(column).as_str()),
            (COMPUTED_ALIAS, expression),
        ])?;

    let projected = Arc::new(ArrowSchema::new(vec![
        ArrowSchema::from(dataset.schema())
            .field_with_name(column)
            .map_err(|_| Error::ColumnNotFound {
                name: column.to_string(),
            })?
            .clone(),
    ]));

    let filled = Arc::new(AtomicU64::new(0));
    let mut buffered = Vec::new();
    let mut batches = scanner.try_into_stream().await?;
    while filled.load(Ordering::Relaxed) == 0 {
        match batches.try_next().await? {
            Some(batch) => buffered.push(merge_batch(&batch, column, &projected, &filled)?),
            None => return Ok(None),
        }
    }

    let column = column.to_string();
    let counter = filled.clone();
    let rest = batches.map(move |batch| {
        batch.and_then(|batch| merge_batch(&batch, &column, &projected, &counter))
    });
    let values = stream::iter(buffered.into_iter().map(Ok)).chain(rest);
    Ok(Some((filled, values)))
}

#[cfg(test)]
mod tests {
    use arrow_array::{Int32Array, record_batch};
    use futures::TryStreamExt;

    use crate::connect;
    use crate::query::{ExecutableQuery, QueryBase, Select};
    use crate::{Error, Result, Table};

    async fn table_with(name: &str, values: Vec<i32>) -> Table {
        let conn = connect("memory://").execute().await.unwrap();
        let batch = record_batch!(("x", Int32, values)).unwrap();
        conn.create_table(name, batch).execute().await.unwrap()
    }

    async fn declare_doubled(table: &Table) -> Result<u64> {
        Ok(table
            .add_columns()
            .computed("doubled", "x * 2")
            .execute()
            .await?
            .version)
    }

    async fn read(table: &Table, column: &str) -> Vec<Option<i32>> {
        let batches = table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut values: Vec<Option<i32>> = batches
            .iter()
            .flat_map(|batch| {
                batch[column]
                    .as_any()
                    .downcast_ref::<Int32Array>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>()
            })
            .collect();
        values.sort();
        values
    }

    async fn append(table: &Table, values: Vec<i32>) {
        let batch = record_batch!(("x", Int32, values)).unwrap();
        table.add(batch).execute().await.unwrap();
    }

    #[tokio::test]
    async fn test_refresh_fills_a_declared_column() {
        let table = table_with("refresh_fills", vec![1, 2, 3]).await;
        let declared = declare_doubled(&table).await.unwrap();
        assert_eq!(read(&table, "doubled").await, vec![None, None, None]);

        let result = table.refresh_column("doubled").await.unwrap();
        assert!(result.version > declared);
        assert_eq!(result.rows_filled, 3);
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(4), Some(6)]
        );
    }

    /// Values written after the last refresh must be reachable by another one.
    #[tokio::test]
    async fn test_refresh_fills_rows_appended_since_the_last_refresh() {
        let table = table_with("refresh_appended", vec![1, 2]).await;
        declare_doubled(&table).await.unwrap();
        table.refresh_column("doubled").await.unwrap();

        append(&table, vec![5, 6]).await;
        assert_eq!(
            read(&table, "doubled").await,
            vec![None, None, Some(2), Some(4)]
        );

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 2);
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(4), Some(10), Some(12)]
        );
    }

    #[tokio::test]
    async fn test_refresh_with_nothing_to_fill() {
        let table = table_with("refresh_noop", vec![1, 2, 3]).await;
        declare_doubled(&table).await.unwrap();
        table.refresh_column("doubled").await.unwrap();

        let again = table.refresh_column("doubled").await.unwrap();
        assert_eq!(again.rows_filled, 0);
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(4), Some(6)]
        );
    }

    /// A row is filled only by gaining a value, so an expression yielding null
    /// settles at once instead of re-selecting the same rows forever. Nothing
    /// is staged, so the version does not move either.
    #[tokio::test]
    async fn test_refresh_converges_on_a_null_result() {
        let table = table_with("refresh_null_result", vec![1, 2, 3]).await;
        let declared = table
            .add_columns()
            .computed("maybe", "nullif(x, x)")
            .execute()
            .await
            .unwrap()
            .version;

        let first = table.refresh_column("maybe").await.unwrap();
        assert_eq!(first.rows_filled, 0);
        assert_eq!(first.version, declared);
        assert_eq!(read(&table, "maybe").await, vec![None, None, None]);

        let again = table.refresh_column("maybe").await.unwrap();
        assert_eq!(again.rows_filled, 0);
        assert_eq!(again.version, declared);
    }

    /// The contract's boundary: a filled fragment is not revisited, so
    /// mutating an input leaves the value computed at fill time.
    #[tokio::test]
    async fn test_refresh_does_not_observe_input_mutation() {
        let table = table_with("refresh_mutation", vec![1]).await;
        declare_doubled(&table).await.unwrap();
        table.refresh_column("doubled").await.unwrap();
        assert_eq!(read(&table, "doubled").await, vec![Some(2)]);

        table.update().column("x", "3").execute().await.unwrap();

        let again = table.refresh_column("doubled").await.unwrap();
        assert_eq!(again.rows_filled, 0);
        assert_eq!(read(&table, "doubled").await, vec![Some(2)]);
    }

    /// A row rewrite before the first refresh materializes the declared
    /// column as null behind a covering data file. Those rows are still
    /// unfilled and a later refresh has to reach them.
    #[tokio::test]
    async fn test_update_before_the_first_refresh() {
        let table = table_with("refresh_update_first", vec![1]).await;
        declare_doubled(&table).await.unwrap();

        table.update().column("x", "3").execute().await.unwrap();

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 1);
        assert_eq!(read(&table, "doubled").await, vec![Some(6)]);
    }

    /// The contract holds row by row, not fragment by fragment: revisiting a
    /// fragment to fill one row must not recompute a filled row sitting beside
    /// it, even where the input behind it has since changed.
    #[tokio::test]
    async fn test_refresh_does_not_recompute_a_filled_row_beside_an_unfilled_one() {
        let table = table_with("refresh_mixed", vec![1, 2]).await;
        declare_doubled(&table).await.unwrap();
        table.refresh_column("doubled").await.unwrap();

        append(&table, vec![5]).await;
        table
            .update()
            .column("x", "100")
            .only_if("x = 1")
            .execute()
            .await
            .unwrap();
        table
            .optimize(crate::table::OptimizeAction::Compact {
                options: crate::table::CompactionOptions::default(),
                remap_options: None,
            })
            .await
            .unwrap();

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 1);
        // 2 is the mutated row keeping the value it was filled with, not 200.
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(4), Some(10)]
        );
    }

    /// Filling a fragment must not disturb the values it already holds, which
    /// is what makes a compaction-mixed fragment safe to revisit.
    #[tokio::test]
    async fn test_refresh_preserves_already_filled_rows() {
        let table = table_with("refresh_preserves", vec![1, 2]).await;
        declare_doubled(&table).await.unwrap();
        table.refresh_column("doubled").await.unwrap();

        append(&table, vec![5]).await;
        table
            .optimize(crate::table::OptimizeAction::Compact {
                options: crate::table::CompactionOptions::default(),
                remap_options: None,
            })
            .await
            .unwrap();

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 1);
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(4), Some(10)]
        );
    }

    #[tokio::test]
    async fn test_refresh_leaves_deleted_rows_alone() {
        let table = table_with("refresh_deleted", vec![1, 2, 3, 4]).await;
        declare_doubled(&table).await.unwrap();
        table.delete("x = 2").await.unwrap();

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 3);
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(6), Some(8)]
        );
    }

    #[tokio::test]
    async fn test_refresh_a_constant_expression() {
        let table = table_with("refresh_constant", vec![1, 2, 3]).await;
        table
            .add_columns()
            .computed("answer", "42")
            .execute()
            .await
            .unwrap();

        let result = table.refresh_column("answer").await.unwrap();
        assert_eq!(result.rows_filled, 3);
    }

    /// A name needing quotes reaches the evaluator intact: it is carried as a
    /// projection alias, never spliced into SQL text.
    #[tokio::test]
    async fn test_refresh_a_column_whose_name_needs_quoting() {
        let table = table_with("refresh_quoted", vec![1, 2, 3]).await;
        table
            .add_columns()
            .computed("double value", "x * 2")
            .execute()
            .await
            .unwrap();

        let result = table.refresh_column("double value").await.unwrap();
        assert_eq!(result.rows_filled, 3);
        assert_eq!(
            read(&table, "double value").await,
            vec![Some(2), Some(4), Some(6)]
        );
    }

    /// A fragment spanning several scan batches exercises the streamed fill:
    /// the probe buffers only until the first gained value and the rest flows
    /// through write_column a batch at a time.
    #[tokio::test]
    async fn test_refresh_streams_a_multi_batch_fragment() {
        let values: Vec<i32> = (0..20_000).collect();
        let table = table_with("refresh_multi_batch", values.clone()).await;
        declare_doubled(&table).await.unwrap();

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 20_000);

        let read_back = read(&table, "doubled").await;
        assert_eq!(read_back.len(), 20_000);
        let mut expected: Vec<Option<i32>> = values.iter().map(|v| Some(v * 2)).collect();
        expected.sort();
        assert_eq!(read_back, expected);
    }

    /// The async form's job settles with the fill visible, like
    /// create_index's execute_async.
    #[tokio::test]
    async fn test_refresh_async_job_waits_for_the_fill() {
        let table = table_with("refresh_async", vec![1, 2, 3]).await;
        declare_doubled(&table).await.unwrap();

        let job = table.refresh_column_async("doubled").await.unwrap();
        assert!(job.id().is_none(), "in-process jobs have no server id");
        job.wait().await.unwrap();
        assert_eq!(job.status().await.unwrap(), "finished");
        assert_eq!(
            read(&table, "doubled").await,
            vec![Some(2), Some(4), Some(6)]
        );
    }

    /// Bad input is reported by the call, not by the job.
    #[tokio::test]
    async fn test_refresh_async_rejects_bad_input_before_spawning() {
        let table = table_with("refresh_async_bad", vec![1, 2, 3]).await;

        let err = table.refresh_column_async("x").await.unwrap_err();
        assert!(matches!(err, Error::NotAComputedColumn { name } if name == "x"));

        let err = table.refresh_column_async("nope").await.unwrap_err();
        assert!(matches!(err, Error::ColumnNotFound { name } if name == "nope"));
    }

    #[tokio::test]
    async fn test_refresh_async_job_reports_success_to_every_waiter() {
        let table = table_with("refresh_async_waiters", vec![1, 2]).await;
        declare_doubled(&table).await.unwrap();

        let job = table.refresh_column_async("doubled").await.unwrap();
        job.wait().await.unwrap();
        // A second wait after completion observes the same outcome.
        job.wait().await.unwrap();
        assert_eq!(job.status().await.unwrap(), "finished");
    }

    #[tokio::test]
    async fn test_refresh_rejects_a_plain_column() {
        let table = table_with("refresh_plain", vec![1, 2, 3]).await;
        let err = table.refresh_column("x").await.unwrap_err();
        assert!(matches!(err, Error::NotAComputedColumn { name } if name == "x"));
    }

    #[tokio::test]
    async fn test_refresh_rejects_an_unknown_column() {
        let table = table_with("refresh_missing", vec![1, 2, 3]).await;
        let err = table.refresh_column("nope").await.unwrap_err();
        assert!(matches!(err, Error::ColumnNotFound { name } if name == "nope"));
    }

    /// A declaration of a kind this version cannot evaluate is refused by
    /// name, rather than mistaken for a plain column or fed to the SQL path.
    #[tokio::test]
    async fn test_refresh_rejects_a_kind_it_cannot_evaluate() {
        let table = table_with("refresh_foreign", vec![1, 2, 3]).await;
        super::super::computed_columns::add_foreign_kind(&table, "embedding", "udf").await;

        let err = table.refresh_column("embedding").await.unwrap_err();
        assert!(matches!(err, Error::NotSupported { message } if message.contains("udf")));
    }
}
