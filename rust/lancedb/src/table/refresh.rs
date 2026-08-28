// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Filling computed columns.
//!
//! A row without a value gets one; a row that has one keeps it. Refresh is
//! therefore idempotent and does not observe input mutation -- once a row is
//! filled, changing what the expression reads leaves the stored result alone.
//!
//! A column's computed inputs are filled first -- the dependency graph is
//! walked once, each reachable column filled once in dependency order, each
//! fill its own commit. Every fill in the pass, the requested column's
//! included, covers only the fragments of the snapshot the pass started
//! from: a commit may rebase over a concurrent append, and the fragment that
//! admits carries placeholder nulls no earlier fill covered, so it waits for
//! a later refresh rather than being read as values. Two concurrent fills of
//! one input collide on its field in lance's conflict check, so a dependent
//! fill can only commit over inputs that were durable when it read them.
//!
//! Two passes per fragment. The first scans only the unfilled live rows and
//! evaluates the expression over them, which yields the exact fill count and
//! decides whether the fragment is staged at all -- a fragment where nothing
//! would change stages nothing, which is what lets an expression yielding
//! null settle instead of restaging forever. The second streams the
//! fragment's physical rows into `write_columns` a batch at a time, so peak
//! memory is bounded by a scan batch. The expression is evaluated by this
//! module, never through a projection alias, and only over rows being
//! filled: every other row -- deleted, or already holding a value -- has its
//! inputs masked to null first, so a poison value in a row nobody is filling
//! cannot fail the refresh.

use std::collections::HashSet;
use std::sync::Arc;

use arrow_array::{
    Array, ArrayRef, BooleanArray, LargeBinaryArray, RecordBatch, RecordBatchOptions,
};
use arrow_schema::Schema as ArrowSchema;
use datafusion_expr::ColumnarValue;
use futures::{Stream, StreamExt, TryStreamExt};
use lance::Dataset;
use lance::dataset::WriteDestination;
use lance::dataset::fragment::FileFragment;
use lance::dataset::transaction::Operation;
use lance_core::ROW_ID;
use lance_core::datatypes::{BlobHandling, Schema as LanceSchema};
use serde::{Deserialize, Serialize};

use super::computed_columns::{BoundExpression, ComputedColumnKind, computed_column_from_field};
use super::{BaseTable, NativeTable};
use crate::job::Job;
use crate::{Error, Result};

/// The result of refreshing a computed column.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RefreshColumnResult {
    /// Rows that had a value computed, in the requested column only; inputs
    /// filled on its behalf are not counted.
    #[serde(default)]
    pub rows_filled: u64,
    /// The commit version associated with the operation.
    #[serde(default)]
    pub version: u64,
}

struct RefreshExecution {
    result: RefreshColumnResult,
    source_version: u64,
    published_version: Option<u64>,
}

/// Internal implementation of the refresh logic.
pub(crate) async fn execute_refresh_column(
    table: &NativeTable,
    column: &str,
) -> Result<RefreshColumnResult> {
    Ok(execute_refresh_column_with_source(table, column)
        .await?
        .result)
}

async fn execute_refresh_column_with_source(
    table: &NativeTable,
    column: &str,
) -> Result<RefreshExecution> {
    table.dataset.ensure_mutable()?;
    ensure_no_lsm_write_spec(table).await?;
    let dataset = table.dataset.get().await?;

    let expression = declared_expression(&dataset, column)?;
    let schema = Arc::new(ArrowSchema::from(dataset.schema()));
    let bound = Arc::new(super::computed_columns::bind(
        schema.clone(),
        column,
        &expression,
    )?);
    ensure_inputs_filled(&dataset, &schema, column, &bound).await?;
    let field = dataset
        .schema()
        .field(column)
        .ok_or_else(|| Error::ColumnNotFound {
            name: column.to_string(),
        })?;
    // The dataset's own field, so the identity write_columns checks against the
    // manifest holds by construction.
    let column_schema = LanceSchema {
        fields: vec![field.clone()],
        metadata: Default::default(),
    };
    let output_is_blob = field.is_blob_v2();

    let mut rows_filled = 0u64;
    let mut replacements = Vec::new();
    for fragment in dataset.get_fragments() {
        let gained = count_fragment_gains(&dataset, &fragment, &bound, column).await?;
        if gained == 0 {
            continue;
        }
        rows_filled += gained;
        let values =
            fill_stream(&dataset, &fragment, bound.clone(), column, output_is_blob).await?;
        replacements.push(fragment.write_columns(values, &column_schema).await?);
    }

    let source_version = dataset.version().version;
    if replacements.is_empty() {
        return Ok(RefreshExecution {
            result: RefreshColumnResult {
                rows_filled: 0,
                version: source_version,
            },
            source_version,
            published_version: None,
        });
    }

    // The dataset's own session, so registrations and caches survive the
    // commit being installed on the handle.
    let session = dataset.session();
    let new_dataset = Dataset::commit(
        WriteDestination::Dataset(dataset.clone()),
        Operation::DataReplacement { replacements },
        Some(source_version),
        None,
        None,
        session,
        false,
    )
    .await?;

    let version = new_dataset.version().version;
    table.dataset.update(new_dataset);
    Ok(RefreshExecution {
        result: RefreshColumnResult {
            rows_filled,
            version,
        },
        source_version,
        published_version: Some(version),
    })
}

/// Refuse while a computed input still has rows a refresh of it would fill:
/// read now, its placeholder null would be evaluated as a value and kept.
async fn ensure_inputs_filled(
    dataset: &Dataset,
    schema: &Arc<ArrowSchema>,
    column: &str,
    bound: &BoundExpression,
) -> Result<()> {
    for input in &bound.roots {
        let Some(declaration) = schema
            .field_with_name(input)
            .ok()
            .and_then(computed_column_from_field)
        else {
            continue;
        };
        let ComputedColumnKind::Sql { expression } = &declaration.kind else {
            return Err(Error::NotSupported {
                message: format!(
                    "computed column '{column}' reads '{input}', whose fill state this \
                     refresh cannot check; refresh '{input}' first"
                ),
            });
        };
        let input_bound = super::computed_columns::bind(schema.clone(), input, expression)?;
        let mut unfilled = 0u64;
        for fragment in dataset.get_fragments() {
            unfilled += count_fragment_gains(dataset, &fragment, &input_bound, input).await?;
        }
        if unfilled > 0 {
            return Err(Error::InvalidInput {
                message: format!(
                    "computed column '{column}' reads '{input}', which has {unfilled} unfilled \
                     rows; refresh '{input}' first"
                ),
            });
        }
    }
    Ok(())
}

/// Run the refresh as a [`Job`] in this process.
pub(crate) async fn execute_refresh_column_async(
    table: &NativeTable,
    column: &str,
) -> Result<Job<crate::function::RefreshColumnResult>> {
    // Validate before spawning so bad input is reported by this call rather
    // than only by the job.
    table.dataset.ensure_mutable()?;
    ensure_no_lsm_write_spec(table).await?;
    let dataset = table.dataset.get().await?;
    declared_expression(&dataset, column)?;
    drop(dataset);

    let table = table.clone();
    let column = column.to_string();
    Ok(Job::spawned(tokio::spawn(async move {
        let execution = execute_refresh_column_with_source(&table, &column).await?;
        table.bump_freshness();
        Ok(crate::function::RefreshColumnResult {
            rows_assigned: execution.result.rows_filled,
            rows_failed: 0,
            rows_remaining: 0,
            source_version: execution.source_version,
            published_version: execution.published_version,
        })
    })))
}

/// Refuse to refresh under an LSM write spec.
///
/// Refresh enumerates base fragments, and a write spec keeps visible rows in
/// un-compacted MemWAL tiers it cannot reach -- success would silently omit
/// readable rows.
async fn ensure_no_lsm_write_spec(table: &NativeTable) -> Result<()> {
    use lance::dataset::mem_wal::DatasetMemWalExt;

    // Unset drops the MemWAL index, so the spec alone stops describing a table
    // whose SSTables still hold rows. The shard directories outlive it and are
    // the durable evidence.
    let retained_sstables = !table
        .dataset
        .get()
        .await?
        .list_mem_wal_latest_shard_ids()
        .await?
        .is_empty();
    if retained_sstables || table.get_lsm_write_spec().await?.is_some() {
        return Err(Error::NotSupported {
            message: "refresh_column is not supported on a table with an LSM write \
                      spec: rows in un-compacted tiers are invisible to refresh"
                .into(),
        });
    }
    Ok(())
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
        ComputedColumnKind::Function { .. } => Err(Error::NotSupported {
            message: "registered Function columns are refreshed only by a remote server Job".into(),
        }),
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
pub(crate) fn quote_identifier(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

/// Assemble the batch evaluation runs against: the bound roots, in read-schema
/// order. Built by name so scan-side column order never matters.
fn evaluation_batch(
    batch: &RecordBatch,
    bound: &BoundExpression,
    mask_out: Option<&BooleanArray>,
) -> lance_core::Result<RecordBatch> {
    let mut columns = Vec::with_capacity(bound.roots.len());
    let mut fields = Vec::with_capacity(bound.roots.len());
    for name in &bound.roots {
        let index = batch.schema_ref().index_of(name).map_err(|_| {
            lance_core::Error::invalid_input(format!(
                "refreshing a computed column read no {name} column"
            ))
        })?;
        let column = batch.column(index);
        fields.push(batch.schema_ref().field(index).clone());
        // Rows outside the mask must not reach the expression: a value in a
        // deleted or already-filled row can be one it would choke on.
        columns.push(match mask_out {
            Some(mask) => arrow::compute::nullif(column, mask)?,
            None => column.clone(),
        });
    }
    Ok(RecordBatch::try_new_with_options(
        Arc::new(ArrowSchema::new(fields)),
        columns,
        &RecordBatchOptions::new().with_row_count(Some(batch.num_rows())),
    )?)
}

/// Evaluate the expression over `batch`, materializing a constant result to
/// the batch's length.
fn evaluate(bound: &BoundExpression, batch: &RecordBatch) -> lance_core::Result<ArrayRef> {
    let value = bound
        .physical
        .evaluate(batch)
        .map_err(lance_core::Error::from)?;
    match value {
        ColumnarValue::Array(array) => Ok(array),
        scalar => scalar
            .into_array(batch.num_rows())
            .map_err(lance_core::Error::from),
    }
}

fn materialized_blob_ids(schema: &LanceSchema, paths: &[String]) -> Result<HashSet<u32>> {
    paths
        .iter()
        .map(|path| {
            let field = schema
                .resolve(path)
                .and_then(|fields| fields.last().copied())
                .ok_or_else(|| Error::InvalidInput {
                    message: format!("computed Blob input '{path}' no longer exists"),
                })?;
            if !field.is_blob_v2() {
                return Err(Error::InvalidInput {
                    message: format!("computed Blob input '{path}' is no longer Blob v2"),
                });
            }
            u32::try_from(field.id).map_err(|_| Error::InvalidInput {
                message: format!(
                    "computed Blob input '{path}' has invalid field id {}",
                    field.id
                ),
            })
        })
        .collect()
}

fn configure_blob_inputs(
    scanner: &mut lance::dataset::scanner::Scanner,
    schema: &LanceSchema,
    bound: &BoundExpression,
    extra_blob_id: Option<u32>,
) -> Result<()> {
    let mut ids = materialized_blob_ids(schema, &bound.blob_paths)?;
    ids.extend(extra_blob_id);
    scanner.blob_handling(BlobHandling::SomeBlobsBinary(ids));
    Ok(())
}

fn blob_array_from_binary(array: &ArrayRef) -> lance_core::Result<ArrayRef> {
    let values = array
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| {
            lance_core::Error::invalid_input(format!(
                "a Blob v2 computed output produced {}, expected LargeBinary",
                array.data_type()
            ))
        })?;
    let mut builder = lance::blob::BlobArrayBuilder::new(values.len());
    for index in 0..values.len() {
        if values.is_null(index) {
            builder.push_null()?;
        } else {
            builder.push_bytes(values.value(index))?;
        }
    }
    builder.finish()
}

/// How many rows of one fragment would gain a value.
///
/// Scans only the unfilled live rows -- deleted rows never reach the
/// expression here, the filter having already excluded them -- and counts the
/// non-null results. Exact, so it is both the staging decision and the
/// fragment's contribution to `rows_filled`.
async fn count_fragment_gains(
    dataset: &Dataset,
    fragment: &FileFragment,
    bound: &BoundExpression,
    column: &str,
) -> Result<u64> {
    let mut scanner = dataset.scan();
    scanner
        .with_fragments(vec![fragment.metadata().clone()])
        .with_row_id()
        .filter(&format!("{} IS NULL", quote_identifier(column)))?
        .project(&bound.roots)?;
    configure_blob_inputs(&mut scanner, dataset.schema(), bound, None)?;

    let mut gained = 0u64;
    let mut batches = scanner.try_into_stream().await?;
    while let Some(batch) = batches.try_next().await? {
        let evaluated = evaluate(bound, &evaluation_batch(&batch, bound, None)?)?;
        gained += (batch.num_rows() - evaluated.null_count()) as u64;
    }
    Ok(gained)
}

/// Stream one fragment's column in physical order, filling the unfilled live
/// rows and keeping every other value.
///
/// Deleted rows are carried through so the values line up positionally with
/// the fragment's data files; they are never read back, but the column file
/// has to cover them.
async fn fill_stream(
    dataset: &Dataset,
    fragment: &FileFragment,
    bound: Arc<BoundExpression>,
    column: &str,
    output_is_blob: bool,
) -> Result<impl Stream<Item = lance_core::Result<RecordBatch>> + Send + use<>> {
    let mut projection: Vec<String> = bound.roots.clone();
    projection.push(column.to_string());
    let mut scanner = dataset.scan();
    scanner
        .with_fragments(vec![fragment.metadata().clone()])
        .with_row_id()
        .include_deleted_rows()
        .project(&projection)?;
    let output_blob_id = output_is_blob
        .then(|| {
            dataset
                .schema()
                .field(column)
                .and_then(|field| u32::try_from(field.id).ok())
        })
        .flatten();
    configure_blob_inputs(
        &mut scanner,
        dataset.schema(),
        bound.as_ref(),
        output_blob_id,
    )?;

    let projected = Arc::new(ArrowSchema::new(vec![
        ArrowSchema::from(dataset.schema())
            .field_with_name(column)
            .map_err(|_| Error::ColumnNotFound {
                name: column.to_string(),
            })?
            .clone(),
    ]));

    let column = column.to_string();
    let batches = scanner.try_into_stream().await?;
    Ok(batches.map(move |batch| {
        let batch = batch?;
        let missing = |name: &str| {
            lance_core::Error::invalid_input(format!(
                "refreshing a computed column read no {name} column"
            ))
        };
        let existing = batch
            .column_by_name(&column)
            .ok_or_else(|| missing(&column))?;
        let row_ids = batch
            .column_by_name(ROW_ID)
            .ok_or_else(|| missing(ROW_ID))?;

        // Only an unfilled live row gains a value; a deleted row has a null
        // row id and keeps its (null) slot.
        let unfilled = arrow::compute::is_null(existing.as_ref())?;
        let live = arrow::compute::is_not_null(row_ids.as_ref())?;
        let fill = arrow::compute::and(&unfilled, &live)?;
        let keep = arrow::compute::not(&fill)?;

        let computed = evaluate(&bound, &evaluation_batch(&batch, &bound, Some(&keep))?)?;
        let merged = arrow_select::zip::zip(&fill, &computed, existing)?;
        let merged = if output_is_blob {
            blob_array_from_binary(&merged)?
        } else {
            merged
        };
        Ok(RecordBatch::try_new(projected.clone(), vec![merged])?)
    }))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{Array, Int32Array, LargeBinaryArray, RecordBatch, record_batch};
    use futures::TryStreamExt;
    use lance_core::ROW_ID;

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

    async fn read(table: &Table, column: &str) -> Vec<Option<i64>> {
        use arrow_array::{Array, Int64Array};
        let batches = table
            .query()
            .select(Select::columns(&[column]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut values: Vec<Option<i64>> = batches
            .iter()
            .flat_map(|batch| {
                let array = &batch[column];
                match array.as_any().downcast_ref::<Int32Array>() {
                    Some(ints) => ints.iter().map(|v| v.map(i64::from)).collect::<Vec<_>>(),
                    None => array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .unwrap()
                        .iter()
                        .collect::<Vec<_>>(),
                }
            })
            .collect();
        values.sort();
        values
    }

    async fn append(table: &Table, values: Vec<i32>) {
        let batch = record_batch!(("x", Int32, values)).unwrap();
        table.add(batch).execute().await.unwrap();
    }

    /// The gate's reproducer: `b = coalesce(a, 0)` refreshed before `a`
    /// must not bake zeros from `a`'s placeholder null. It is refused, and
    /// names the input, until `a` is filled -- after every append too.
    #[tokio::test]
    async fn test_dependent_refresh_refuses_an_unfilled_input() {
        let table = table_with("dependent_refresh_order", vec![1, 2, 3]).await;
        table
            .add_columns()
            .computed("a", "x + 1")
            .computed("b", "coalesce(a, 0)")
            .execute()
            .await
            .unwrap();

        let err = table.refresh_column("b").await.unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("refresh 'a' first")),
            "{err}"
        );
        assert_eq!(read(&table, "b").await, vec![None, None, None]);

        assert_eq!(table.refresh_column("a").await.unwrap().rows_filled, 3);
        assert_eq!(table.refresh_column("b").await.unwrap().rows_filled, 3);
        assert_eq!(read(&table, "b").await, vec![Some(2), Some(3), Some(4)]);

        append(&table, vec![10]).await;
        assert!(table.refresh_column("b").await.is_err());
        table.refresh_column("a").await.unwrap();
        assert_eq!(table.refresh_column("b").await.unwrap().rows_filled, 1);
        assert_eq!(
            table.count_rows(Some("b = 0".to_string())).await.unwrap(),
            0
        );
    }

    /// Names that need quoting, and a nested input, survive the trip through
    /// declaration metadata and the dependency check: the recorded inputs
    /// are matched by name, never re-parsed as SQL.
    #[tokio::test]
    async fn test_dependent_refresh_handles_awkward_column_names() {
        use arrow_array::{Int32Array, StructArray};
        use arrow_schema::{DataType, Field, Fields};

        let conn = connect("memory://").execute().await.unwrap();
        let age_fields = Fields::from(vec![Field::new("age", DataType::Int32, true)]);
        let meta = StructArray::new(
            age_fields.clone(),
            vec![Arc::new(Int32Array::from(vec![10, 20])) as _],
            None,
        );
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            Field::new("camelCase", DataType::Int32, true),
            Field::new("with-hyphen", DataType::Int32, true),
            Field::new("meta", DataType::Struct(age_fields), true),
        ]));
        let batch = arrow_array::RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![1, 2])) as _,
                Arc::new(Int32Array::from(vec![100, 200])) as _,
                Arc::new(meta) as _,
            ],
        )
        .unwrap();
        let table = conn
            .create_table("awkward_names", batch)
            .execute()
            .await
            .unwrap();

        table
            .add_columns()
            .computed("y", "`camelCase` * 2")
            .computed("z", "coalesce(y, 0) + `with-hyphen` + meta.age")
            .execute()
            .await
            .unwrap();
        let z = crate::table::computed_columns::computed_columns(
            table.schema().await.unwrap().as_ref(),
        )
        .into_iter()
        .find(|c| c.name == "z")
        .unwrap();
        assert_eq!(z.inputs, vec!["meta.age", "with-hyphen", "y"]);

        let err = table.refresh_column("z").await.unwrap_err();
        assert!(err.to_string().contains("refresh 'y' first"), "{err}");
        assert_eq!(table.refresh_column("y").await.unwrap().rows_filled, 2);
        assert_eq!(table.refresh_column("z").await.unwrap().rows_filled, 2);
        assert_eq!(read(&table, "z").await, vec![Some(112), Some(224)]);
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

        let no_op = table
            .refresh_column_async("doubled")
            .await
            .unwrap()
            .wait()
            .await
            .unwrap();
        assert_eq!(no_op.rows_assigned, 0);
        assert_eq!(no_op.source_version, 3);
        assert_eq!(no_op.published_version, None);
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
    /// through write_columns a batch at a time.
    #[tokio::test]
    async fn test_refresh_streams_a_multi_batch_fragment() {
        let values: Vec<i32> = (0..20_000).collect();
        let table = table_with("refresh_multi_batch", values.clone()).await;
        declare_doubled(&table).await.unwrap();

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 20_000);

        let read_back = read(&table, "doubled").await;
        assert_eq!(read_back.len(), 20_000);
        let mut expected: Vec<Option<i64>> =
            values.iter().map(|v| Some(i64::from(v * 2))).collect();
        expected.sort();
        assert_eq!(read_back, expected);
    }

    /// The gate's reproducer: the commit must reuse the configured session,
    /// or registrations and caches vanish from the handle after a refresh.
    #[tokio::test]
    async fn test_refresh_preserves_the_configured_session() {
        let session = Arc::new(lance::session::Session::default());
        let conn = crate::connect("memory://")
            .session(session.clone())
            .execute()
            .await
            .unwrap();
        let batch = record_batch!(("x", Int32, [1, 2])).unwrap();
        let table = conn
            .create_table("session_kept", batch)
            .execute()
            .await
            .unwrap();
        declare_doubled(&table).await.unwrap();
        table.refresh_column("doubled").await.unwrap();

        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        assert!(Arc::ptr_eq(&dataset.session(), &session));
    }

    /// The async form's job settles with the fill visible, like
    /// create_index's execute_async.
    #[tokio::test]
    async fn test_refresh_async_job_waits_for_the_fill() {
        let table = table_with("refresh_async", vec![1, 2, 3]).await;
        declare_doubled(&table).await.unwrap();

        let job = table.refresh_column_async("doubled").await.unwrap();
        assert!(job.id().is_none(), "in-process jobs have no server id");
        let result = job.wait().await.unwrap();
        assert_eq!(result.rows_assigned, 3);
        assert_eq!(result.rows_failed, 0);
        assert_eq!(result.rows_remaining, 0);
        assert_eq!(result.source_version, 2);
        assert_eq!(result.published_version, Some(3));
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
        let first = job.wait().await.unwrap();
        // A second wait after completion observes the same outcome.
        assert_eq!(job.wait().await.unwrap(), first);
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

    /// The gate's reproducer: a poison value in a deleted row must not
    /// abort filling the live rows, since nobody can read it.
    #[tokio::test]
    async fn test_a_deleted_rows_value_is_never_evaluated() {
        let table = table_with("refresh_deleted_poison", vec![1, 0]).await;
        table
            .add_columns()
            .computed("quotient", "10 / x")
            .execute()
            .await
            .unwrap();
        table.delete("x = 0").await.unwrap();

        let result = table.refresh_column("quotient").await.unwrap();
        assert_eq!(result.rows_filled, 1);
        assert_eq!(read(&table, "quotient").await, vec![Some(10)]);
    }

    /// The gate's reproducer: an already-filled row's value must not be
    /// re-evaluated either -- its input may have mutated into one the
    /// expression chokes on.
    #[tokio::test]
    async fn test_a_filled_rows_value_is_never_evaluated() {
        let table = table_with("refresh_filled_poison", vec![1, 2]).await;
        table
            .add_columns()
            .computed("quotient", "10 / x")
            .execute()
            .await
            .unwrap();
        table.refresh_column("quotient").await.unwrap();

        table
            .update()
            .column("x", "0")
            .only_if("x = 1")
            .execute()
            .await
            .unwrap();
        append(&table, vec![5]).await;

        let result = table.refresh_column("quotient").await.unwrap();
        assert_eq!(result.rows_filled, 1);
        assert_eq!(
            read(&table, "quotient").await,
            vec![Some(2), Some(5), Some(10)]
        );
    }

    /// The gate's reproducer: the old internal projection alias is an
    /// ordinary column name; a computed column may use it.
    #[tokio::test]
    async fn test_refresh_a_column_named_like_the_old_alias() {
        let table = table_with("refresh_alias_name", vec![1, 2]).await;
        table
            .add_columns()
            .computed("__lancedb_computed", "x * 2")
            .execute()
            .await
            .unwrap();

        let result = table.refresh_column("__lancedb_computed").await.unwrap();
        assert_eq!(result.rows_filled, 2);
        assert_eq!(
            read(&table, "__lancedb_computed").await,
            vec![Some(2), Some(4)]
        );
    }

    /// The gate's reproducer: a late-gain fragment (filled, then one null row
    /// compacted onto the end) fills without the old probe's buffering, which
    /// this pins behaviorally; the memory bound is structural -- the fill
    /// stream retains no batches at all.
    #[tokio::test]
    async fn test_refresh_fills_a_late_gain_fragment() {
        let values: Vec<i32> = (0..20_000).collect();
        let table = table_with("refresh_late_gain", values).await;
        declare_doubled(&table).await.unwrap();
        table.refresh_column("doubled").await.unwrap();

        append(&table, vec![2_000_000]).await;
        table
            .optimize(crate::table::OptimizeAction::Compact {
                options: crate::table::CompactionOptions::default(),
                remap_options: None,
            })
            .await
            .unwrap();

        let result = table.refresh_column("doubled").await.unwrap();
        assert_eq!(result.rows_filled, 1);
        let read_back = read(&table, "doubled").await;
        assert_eq!(read_back.len(), 20_001);
        assert_eq!(read_back.last().unwrap(), &Some(4_000_000));
    }

    /// The gate's reproducer: a nested input declares, refreshes, and guards
    /// its root against invalidating schema changes.
    #[tokio::test]
    async fn test_a_nested_input_declares_and_refreshes() {
        use arrow_array::{Int32Array, StructArray};
        use arrow_schema::{DataType, Field, Fields};

        let conn = connect("memory://").execute().await.unwrap();
        let age = Arc::new(Int32Array::from(vec![30, 40]));
        let fields = Fields::from(vec![Field::new("age", DataType::Int32, true)]);
        let metadata = StructArray::new(fields.clone(), vec![age as _], None);
        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "metadata",
            DataType::Struct(fields),
            true,
        )]));
        let batch =
            arrow_array::RecordBatch::try_new(schema, vec![Arc::new(metadata) as _]).unwrap();
        let table = conn
            .create_table("refresh_nested", batch)
            .execute()
            .await
            .unwrap();

        table
            .add_columns()
            .computed("next_age", "metadata.age + 1")
            .execute()
            .await
            .unwrap();
        let declaration =
            &crate::table::computed_columns(table.schema().await.unwrap().as_ref())[0];
        assert_eq!(declaration.inputs, vec!["metadata.age".to_string()]);

        let result = table.refresh_column("next_age").await.unwrap();
        assert_eq!(result.rows_filled, 2);
        assert_eq!(read(&table, "next_age").await, vec![Some(31), Some(41)]);

        // The dotted input guards its root.
        let err = table.drop_columns(&["metadata"]).await.unwrap_err();
        assert!(
            matches!(&err, Error::InvalidInput { message } if message.contains("next_age")),
            "{err:?}"
        );

        // Masking a struct input for a deleted row goes through the same
        // nullif path as a primitive; a nested input plus deletions must not
        // be the combination that breaks it.
        table.delete("next_age = 31").await.unwrap();
        append_struct_row(&table, 50).await;
        let result = table.refresh_column("next_age").await.unwrap();
        assert_eq!(result.rows_filled, 1);
        assert_eq!(read(&table, "next_age").await, vec![Some(41), Some(51)]);
    }

    /// Append one `metadata: {age}` row to the nested-input table.
    async fn append_struct_row(table: &Table, age: i32) {
        use arrow_array::{Int32Array, StructArray};
        use arrow_schema::{DataType, Field, Fields};

        let ages = Arc::new(Int32Array::from(vec![age]));
        let fields = Fields::from(vec![Field::new("age", DataType::Int32, true)]);
        let metadata = StructArray::new(fields.clone(), vec![ages as _], None);
        let schema = Arc::new(arrow_schema::Schema::new(vec![Field::new(
            "metadata",
            DataType::Struct(fields),
            true,
        )]));
        let batch =
            arrow_array::RecordBatch::try_new(schema, vec![Arc::new(metadata) as _]).unwrap();
        table.add(batch).execute().await.unwrap();
    }

    /// Both orders of declare+spec are refused at the source (see the
    /// schema_evolution tests); refresh's own check covers a dataset another
    /// writer left in that state.
    #[tokio::test]
    async fn test_refresh_refuses_a_foreign_lsm_state() {
        use crate::table::LsmWriteSpec;

        let tmp_dir = tempfile::tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Int32,
            false,
        )]));
        let batch =
            arrow_array::RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![1]))])
                .unwrap();
        let table = conn.create_table("lsm", batch).execute().await.unwrap();
        table.set_unenforced_primary_key(["x"]).await.unwrap();
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();
        super::super::computed_columns::add_foreign_kind(&table, "doubled", "sql").await;

        let err = table.refresh_column("doubled").await.unwrap_err();
        assert!(
            matches!(&err, Error::NotSupported { message } if message.contains("LSM")),
            "{err:?}"
        );
        let err = table.refresh_column_async("doubled").await.unwrap_err();
        assert!(matches!(err, Error::NotSupported { .. }));
    }

    /// After catch-up activation and unset, no spec remains but the catch-up
    /// flag still marks retained SSTable rows; refresh refuses on the flag.
    #[tokio::test]
    async fn test_refresh_refuses_retained_catchup_state() {
        use crate::table::LsmWriteSpec;

        let tmp_dir = tempfile::tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let schema = Arc::new(arrow_schema::Schema::new(vec![arrow_schema::Field::new(
            "x",
            arrow_schema::DataType::Int32,
            false,
        )]));
        let batch = arrow_array::RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from(vec![1]))],
        )
        .unwrap();
        let table = conn
            .create_table("catchup", batch.clone())
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["x"]).await.unwrap();
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();
        let mut merge = table.merge_insert(&["x"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .use_lsm(true);
        merge
            .execute(Box::new(arrow_array::RecordBatchIterator::new(
                vec![Ok(batch)],
                schema,
            )))
            .await
            .unwrap();
        table.unset_lsm_write_spec().await.unwrap();
        super::super::computed_columns::add_foreign_kind(&table, "doubled", "sql").await;

        let err = table.refresh_column("doubled").await.unwrap_err();
        assert!(
            matches!(&err, Error::NotSupported { message } if message.contains("LSM")),
            "{err:?}"
        );
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

    fn blob_batch(ids: Vec<i32>, payloads: Vec<Option<&[u8]>>) -> RecordBatch {
        use arrow_array::Int32Array;
        use arrow_schema::{Field, Schema};

        let mut builder = lance::blob::BlobArrayBuilder::new(payloads.len());
        for payload in payloads {
            match payload {
                Some(payload) => builder.push_bytes(payload).unwrap(),
                None => builder.push_null().unwrap(),
            }
        }
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", arrow_schema::DataType::Int32, false),
                crate::blob("image", true),
            ])),
            vec![Arc::new(Int32Array::from(ids)), builder.finish().unwrap()],
        )
        .unwrap()
    }

    async fn create_blob_table(path: &std::path::Path, batch: RecordBatch) -> Table {
        let conn = connect(path.to_str().unwrap()).execute().await.unwrap();
        conn.create_table("blobs", batch).execute().await.unwrap()
    }

    #[tokio::test]
    async fn test_refresh_materializes_top_level_blob_input() {
        let tmp = tempfile::tempdir().unwrap();
        let table = create_blob_table(
            tmp.path(),
            blob_batch(vec![1, 2, 3], vec![Some(b"hello"), Some(b""), None]),
        )
        .await;
        table
            .add_columns()
            .computed("payload_copy", "image")
            .execute()
            .await
            .unwrap();

        let result = table.refresh_column("payload_copy").await.unwrap();
        assert_eq!(result.rows_filled, 2);
        let batches = table
            .query()
            .select(Select::columns(&["payload_copy"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let payloads = batches[0]
            .column_by_name("payload_copy")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(payloads.value(0), b"hello");
        assert_eq!(payloads.value(1), b"");
        assert!(payloads.is_null(2));
        assert_eq!(
            table
                .count_rows(Some("payload_copy IS NULL".to_string()))
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn test_refresh_publishes_explicit_blob_output() {
        use arrow_array::UInt64Array;
        use lance_arrow::{
            BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY, BLOB_INLINE_SIZE_THRESHOLD_META_KEY,
        };
        use lance_core::datatypes::BlobKind;

        use crate::table::schema_evolution::FieldMetadataUpdate;

        let tmp = tempfile::tempdir().unwrap();
        let table = create_blob_table(
            tmp.path(),
            blob_batch(
                vec![1, 2, 3, 4],
                vec![Some(b"hello"), Some(b"ab"), Some(b""), None],
            ),
        )
        .await;
        table
            .add_columns()
            .computed_blob("image_copy", "image")
            .execute()
            .await
            .unwrap();
        table
            .update_field_metadata(&[FieldMetadataUpdate::new("image_copy")
                .set(BLOB_INLINE_SIZE_THRESHOLD_META_KEY, "1")
                .set(BLOB_DEDICATED_SIZE_THRESHOLD_META_KEY, "4")])
            .await
            .unwrap();

        let first_refresh = table.refresh_column("image_copy").await.unwrap();
        assert_eq!(first_refresh.rows_filled, 3);
        assert_eq!(
            table.blob_columns().await.unwrap(),
            vec!["image".to_string(), "image_copy".to_string()]
        );

        let batches = table
            .query()
            .with_row_id()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = arrow_select::concat::concat_batches(&batches[0].schema(), &batches).unwrap();
        assert!(
            batch
                .column_by_name("image_copy")
                .unwrap()
                .as_any()
                .is::<arrow_array::StructArray>()
        );
        let row_ids = batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values()
            .to_vec();
        let original = table.fetch_blobs("image", &row_ids).await.unwrap();
        let copied = table.fetch_blobs("image_copy", &row_ids).await.unwrap();
        assert_eq!(original, copied);
        let ids = batch
            .column_by_name("id")
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        let files = table
            .fetch_blob_files("image_copy", &row_ids)
            .await
            .unwrap();
        let mut layouts = ids
            .values()
            .iter()
            .copied()
            .zip(files)
            .map(|(id, file)| (id, file.and_then(|file| file.kind())))
            .collect::<Vec<_>>();
        layouts.sort_by_key(|(id, _)| *id);
        assert_eq!(
            layouts,
            vec![
                (1, Some(BlobKind::Dedicated)),
                (2, Some(BlobKind::Packed)),
                (3, Some(BlobKind::Inline)),
                (4, None),
            ]
        );

        table
            .add(blob_batch(vec![5], vec![Some(b"appended")]))
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
        assert_eq!(
            table
                .refresh_column("image_copy")
                .await
                .unwrap()
                .rows_filled,
            1
        );
        assert_eq!(
            table
                .refresh_column("image_copy")
                .await
                .unwrap()
                .rows_filled,
            0
        );

        table.checkout(first_refresh.version).await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 4);
        assert_eq!(
            table.blob_columns().await.unwrap(),
            vec!["image".to_string(), "image_copy".to_string()]
        );
        table.checkout_latest().await.unwrap();
    }

    #[tokio::test]
    async fn test_explicit_blob_output_requires_large_binary_expression() {
        let tmp = tempfile::tempdir().unwrap();
        let table = create_blob_table(tmp.path(), blob_batch(vec![1], vec![Some(b"hello")])).await;

        let error = table
            .add_columns()
            .computed_blob("invalid", "id + 1")
            .execute()
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            Error::InvalidExpression { column, message }
                if column == "invalid" && message.contains("requires a LargeBinary expression")
        ));
    }

    #[tokio::test]
    async fn test_refresh_materializes_nested_struct_blob_input() {
        use arrow_array::{Int32Array, StructArray};
        use arrow_schema::{DataType, Field, Fields, Schema};

        let tmp = tempfile::tempdir().unwrap();
        let mut blob_builder = lance::blob::BlobArrayBuilder::new(2);
        blob_builder.push_bytes(b"nested").unwrap();
        blob_builder.push_null().unwrap();
        let blob_field = crate::blob("image", true);
        let metadata_fields = Fields::from(vec![blob_field.clone()]);
        let metadata = StructArray::new(
            metadata_fields.clone(),
            vec![blob_builder.finish().unwrap()],
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("metadata", DataType::Struct(metadata_fields), true),
            ])),
            vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(metadata)],
        )
        .unwrap();
        let table = create_blob_table(tmp.path(), batch).await;
        table
            .add_columns()
            .computed("payload_copy", "metadata.image")
            .execute()
            .await
            .unwrap();

        assert_eq!(
            table
                .refresh_column("payload_copy")
                .await
                .unwrap()
                .rows_filled,
            1
        );
        let batches = table
            .query()
            .select(Select::columns(&["payload_copy"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let payloads = batches[0]
            .column_by_name("payload_copy")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(payloads.value(0), b"nested");
        assert!(payloads.is_null(1));
    }

    #[tokio::test]
    async fn test_refresh_preserves_list_shape_when_materializing_blob_input() {
        use arrow_array::{Int32Array, ListArray};
        use arrow_buffer::{OffsetBuffer, ScalarBuffer};
        use arrow_schema::{DataType, Field, Schema};

        let tmp = tempfile::tempdir().unwrap();
        let mut blob_builder = lance::blob::BlobArrayBuilder::new(3);
        blob_builder.push_bytes(b"a").unwrap();
        blob_builder.push_bytes(b"bb").unwrap();
        blob_builder.push_null().unwrap();
        let item = Arc::new(crate::blob("item", true));
        let images = ListArray::new(
            item.clone(),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2, 3])),
            blob_builder.finish().unwrap(),
            None,
        );
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("images", DataType::List(item), true),
            ])),
            vec![Arc::new(Int32Array::from(vec![1, 2])), Arc::new(images)],
        )
        .unwrap();
        let table = create_blob_table(tmp.path(), batch).await;
        table
            .add_columns()
            .computed("image_payloads", "images")
            .execute()
            .await
            .unwrap();

        assert_eq!(
            table
                .refresh_column("image_payloads")
                .await
                .unwrap()
                .rows_filled,
            2
        );
        let batches = table
            .query()
            .select(Select::columns(&["image_payloads"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let output = batches[0]
            .column_by_name("image_payloads")
            .unwrap()
            .as_any()
            .downcast_ref::<ListArray>()
            .unwrap();
        assert_eq!(output.value_offsets(), &[0, 2, 3]);
        assert!(output.values().as_any().is::<LargeBinaryArray>());
    }

    #[tokio::test]
    async fn test_refresh_materializes_external_blob_input() {
        use arrow_array::{Int32Array, StringArray};
        use arrow_schema::{DataType, Field, Schema};

        let tmp = tempfile::tempdir().unwrap();
        let payload = b"external-payload";
        let path = tmp.path().join("payload.bin");
        std::fs::write(&path, payload).unwrap();
        let uri = url::Url::from_file_path(path).unwrap().to_string();
        let conn = connect(tmp.path().join("db").to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_empty_table(
                "external",
                Arc::new(Schema::new(vec![
                    Field::new("id", DataType::Int32, false),
                    crate::blob("image", true),
                ])),
            )
            .execute()
            .await
            .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int32, false),
                Field::new("image", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![Some(uri)])),
            ],
        )
        .unwrap();
        table
            .add(batch)
            .allow_external_blob_outside_bases(true)
            .execute()
            .await
            .unwrap();
        table
            .add_columns()
            .computed("payload_copy", "image")
            .execute()
            .await
            .unwrap();

        assert_eq!(
            table
                .refresh_column("payload_copy")
                .await
                .unwrap()
                .rows_filled,
            1
        );
        let batches = table
            .query()
            .select(Select::columns(&["payload_copy"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let payloads = batches[0]
            .column_by_name("payload_copy")
            .unwrap()
            .as_any()
            .downcast_ref::<LargeBinaryArray>()
            .unwrap();
        assert_eq!(payloads.value(0), payload);
    }
}
