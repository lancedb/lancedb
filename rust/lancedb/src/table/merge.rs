// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatchReader;
use arrow_schema::{DataType, Fields};
use futures::future::Either;
use futures::{FutureExt, TryFutureExt};
use lance::dataset::{
    MergeInsertBuilder as LanceMergeInsertBuilder, WhenMatched, WhenNotMatchedBySource,
};
use lance_datafusion::utils::StreamingWriteSource;
use serde::{Deserialize, Serialize};

use crate::error::{Error, Result};

use super::{BaseTable, NativeTable};

pub(crate) mod lsm;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct MergeResult {
    // The commit version associated with the operation.
    // A version of `0` indicates compatibility with legacy servers that do not return
    /// a commit version.
    #[serde(default)]
    pub version: u64,
    /// Number of inserted rows (for user statistics)
    #[serde(default)]
    pub num_inserted_rows: u64,
    /// Number of updated rows (for user statistics)
    #[serde(default)]
    pub num_updated_rows: u64,
    /// Number of deleted rows (for user statistics)
    /// Note: This is different from internal references to 'deleted_rows', since we technically "delete" updated rows during processing.
    /// However those rows are not shared with the user.
    #[serde(default)]
    pub num_deleted_rows: u64,
    /// Number of attempts performed during the merge operation.
    /// This includes the initial attempt plus any retries due to transaction conflicts.
    /// A value of 1 means the operation succeeded on the first try.
    #[serde(default)]
    pub num_attempts: u32,
    /// Total number of rows written.
    ///
    /// On the standard `merge_insert` path this equals
    /// `num_inserted_rows + num_updated_rows`. On the MemWAL LSM write path the
    /// insert/update breakdown is not known until compaction; in that mode
    /// `num_inserted_rows`, `num_updated_rows`, `num_deleted_rows`, `version`
    /// and `num_attempts` are all `0` and this field holds the total number of
    /// rows written through the shard writer.
    #[serde(default)]
    pub num_rows: u64,
}

#[derive(Debug, Clone)]
pub enum MergeFilter {
    Sql(String),
    Expr(datafusion_expr::Expr),
}

/// A builder used to create and run a merge insert operation
///
/// See [`super::Table::merge_insert`] for more context
#[derive(Debug, Clone)]
pub struct MergeInsertBuilder {
    table: Arc<dyn BaseTable>,
    pub(crate) on: Vec<String>,
    pub(crate) when_matched_update_all: bool,
    pub(crate) when_matched_update_all_filt: Option<MergeFilter>,
    pub(crate) when_not_matched_insert_all: bool,
    pub(crate) when_not_matched_by_source_delete: bool,
    pub(crate) when_not_matched_by_source_delete_filt: Option<MergeFilter>,
    pub(crate) timeout: Option<Duration>,
    pub(crate) use_index: bool,
    pub(crate) use_lsm: Option<bool>,
    pub(crate) validate_single_shard: bool,
}

impl MergeInsertBuilder {
    pub(super) fn new(table: Arc<dyn BaseTable>, on: Vec<String>) -> Self {
        Self {
            table,
            on,
            when_matched_update_all: false,
            when_matched_update_all_filt: None,
            when_not_matched_insert_all: false,
            when_not_matched_by_source_delete: false,
            when_not_matched_by_source_delete_filt: None,
            timeout: None,
            use_index: true,
            use_lsm: None,
            validate_single_shard: true,
        }
    }

    /// Rows that exist in both the source table (new data) and
    /// the target table (old data) will be updated, replacing
    /// the old row with the corresponding matching row.
    ///
    /// If there are multiple matches then the behavior is undefined.
    /// Currently this causes multiple copies of the row to be created
    /// but that behavior is subject to change.
    ///
    /// An optional condition may be specified.  If it is, then only
    /// matched rows that satisfy the condition will be updated.  Any
    /// rows that do not satisfy the condition will be left as they
    /// are.  Failing to satisfy the condition does not cause a
    /// "matched row" to become a "not matched" row.
    ///
    /// The condition should be an SQL string.  Use the prefix
    /// target. to refer to rows in the target table (old data)
    /// and the prefix source. to refer to rows in the source
    /// table (new data).
    ///
    /// For example, "target.last_update < source.last_update"
    pub fn when_matched_update_all(&mut self, condition: Option<String>) -> &mut Self {
        self.when_matched_update_all = true;
        self.when_matched_update_all_filt = condition.map(MergeFilter::Sql);
        self
    }

    /// Similar to [`Self::when_matched_update_all`] but accepts a DataFusion logical expression directly.
    pub fn when_matched_update_all_expr(&mut self, condition: datafusion_expr::Expr) -> &mut Self {
        self.when_matched_update_all = true;
        self.when_matched_update_all_filt = Some(MergeFilter::Expr(condition));
        self
    }

    /// Rows that exist only in the source table (new data) should
    /// be inserted into the target table.
    pub fn when_not_matched_insert_all(&mut self) -> &mut Self {
        self.when_not_matched_insert_all = true;
        self
    }

    /// Rows that exist only in the target table (old data) will be
    /// deleted.  An optional condition can be provided to limit what
    /// data is deleted.
    ///
    /// # Arguments
    ///
    /// * `condition` - If None then all such rows will be deleted.
    ///   Otherwise the condition will be used as an SQL filter to
    ///   limit what rows are deleted.
    pub fn when_not_matched_by_source_delete(&mut self, filter: Option<String>) -> &mut Self {
        self.when_not_matched_by_source_delete = true;
        self.when_not_matched_by_source_delete_filt = filter.map(MergeFilter::Sql);
        self
    }

    /// Similar to [`Self::when_not_matched_by_source_delete`] but accepts a DataFusion logical expression directly.
    pub fn when_not_matched_by_source_delete_expr(
        &mut self,
        filter: datafusion_expr::Expr,
    ) -> &mut Self {
        self.when_not_matched_by_source_delete = true;
        self.when_not_matched_by_source_delete_filt = Some(MergeFilter::Expr(filter));
        self
    }

    /// Maximum time to run the operation before cancelling it.
    ///
    /// By default, there is a 30-second timeout that is only enforced after the
    /// first attempt. This is to prevent spending too long retrying to resolve
    /// conflicts. For example, if a write attempt takes 20 seconds and fails,
    /// the second attempt will be cancelled after 10 seconds, hitting the
    /// 30-second timeout. However, a write that takes one hour and succeeds on the
    /// first attempt will not be cancelled.
    ///
    /// When this is set, the timeout is enforced on all attempts, including the first.
    pub fn timeout(&mut self, timeout: Duration) -> &mut Self {
        self.timeout = Some(timeout);
        self
    }

    /// Controls whether to use indexes for the merge operation.
    ///
    /// When set to `true` (the default), the operation will use an index if available
    /// on the join key for improved performance. When set to `false`, it forces a full
    /// table scan even if an index exists. This can be useful for benchmarking or when
    /// the query optimizer chooses a suboptimal path.
    ///
    /// If not set, defaults to `true` (use index if available).
    pub fn use_index(&mut self, use_index: bool) -> &mut Self {
        self.use_index = use_index;
        self
    }

    /// Control MemWAL routing for this `merge_insert`.
    ///
    /// By default (unset), a `merge_insert` on a table with an
    /// [`LsmWriteSpec`](super::LsmWriteSpec) installed is routed through Lance's
    /// MemWAL shard writer; a table without one uses the standard path.
    ///
    /// - `use_lsm(true)` forces MemWAL routing and errors if the table has no
    ///   LSM write spec.
    /// - `use_lsm(false)` forces the standard write path even when a spec is set.
    pub fn use_lsm(&mut self, enable: bool) -> &mut Self {
        self.use_lsm = Some(enable);
        self
    }

    /// Controls how an LSM `merge_insert` checks that its input targets a
    /// single shard.
    ///
    /// When a table has an LSM write spec, every row in a `merge_insert` call
    /// must route to the same shard. When `true` (the default), every row is
    /// inspected to verify this. When `false`, only the first row is inspected
    /// and the shard it routes to is used for the whole input — a faster path
    /// for callers that have already pre-sharded their input.
    ///
    /// Has no effect on tables without an LSM write spec.
    pub fn validate_single_shard(&mut self, validate_single_shard: bool) -> &mut Self {
        self.validate_single_shard = validate_single_shard;
        self
    }

    /// Executes the merge insert operation
    ///
    /// Returns version and statistics about the merge operation including the number of rows
    /// inserted, updated, and deleted.
    pub async fn execute(
        mut self,
        new_data: Box<dyn RecordBatchReader + Send>,
    ) -> Result<MergeResult> {
        self.canonicalize_filters()?;
        self.table.clone().merge_insert(self, new_data).await
    }

    pub(crate) fn canonicalize_filters(&mut self) -> Result<()> {
        self.when_matched_update_all_filt =
            canonicalize_merge_filter(self.when_matched_update_all_filt.take())?;
        self.when_not_matched_by_source_delete_filt =
            canonicalize_merge_filter(self.when_not_matched_by_source_delete_filt.take())?;
        Ok(())
    }
}

fn canonicalize_merge_filter(filter: Option<MergeFilter>) -> Result<Option<MergeFilter>> {
    filter
        .map(|filter| match filter {
            MergeFilter::Sql(predicate) => {
                crate::expr::canonicalize_sql_predicate(&predicate).map(MergeFilter::Sql)
            }
            filter @ MergeFilter::Expr(_) => Ok(filter),
        })
        .transpose()
}

// The JSON projection iterates target fields, so reject unknown source fields
// before it can erase them. Missing and reordered fields remain valid merge
// inputs; type compatibility is still checked by the cast and Lance writer.
fn validate_merge_source_fields(input: &Fields, target: &Fields, parent: &str) -> Result<()> {
    let mut names = HashSet::with_capacity(input.len());
    for field in input {
        let path = if parent.is_empty() {
            field.name().clone()
        } else {
            format!("{parent}.{}", field.name())
        };
        if !names.insert(field.name()) {
            return Err(Error::InvalidInput {
                message: format!("merge source field '{path}' is specified more than once"),
            });
        }
        let target_field = target
            .iter()
            .find(|candidate| candidate.name() == field.name())
            .ok_or_else(|| Error::InvalidInput {
                message: format!("merge source field '{path}' is not present in the table schema"),
            })?;
        validate_merge_source_type(field.data_type(), target_field.data_type(), &path)?;
    }
    Ok(())
}

fn validate_merge_source_type(input: &DataType, target: &DataType, path: &str) -> Result<()> {
    match (input, target) {
        (DataType::Struct(input), DataType::Struct(target)) => {
            validate_merge_source_fields(input, target, path)
        }
        (
            DataType::List(input)
            | DataType::LargeList(input)
            | DataType::FixedSizeList(input, _)
            | DataType::ListView(input)
            | DataType::LargeListView(input),
            DataType::List(target)
            | DataType::LargeList(target)
            | DataType::FixedSizeList(target, _)
            | DataType::ListView(target)
            | DataType::LargeListView(target),
        )
        | (DataType::Map(input, _), DataType::Map(target, _)) => {
            validate_merge_source_type(input.data_type(), target.data_type(), path)
        }
        (DataType::Dictionary(_, input), _) => validate_merge_source_type(input, target, path),
        (_, DataType::Dictionary(_, target)) => validate_merge_source_type(input, target, path),
        _ => Ok(()),
    }
}

/// Internal implementation of the merge insert logic
///
/// This logic was moved from NativeTable::merge_insert to keep table.rs clean.
pub(crate) async fn execute_merge_insert(
    table: &NativeTable,
    mut params: MergeInsertBuilder,
    new_data: Box<dyn RecordBatchReader + Send>,
) -> Result<MergeResult> {
    params.canonicalize_filters()?;
    super::computed_columns::ensure_no_function_bindings_for_mutation(
        table.schema().await?.as_ref(),
        "merge_insert",
    )?;
    match lsm::lsm_dispatch_decision(table, &params).await? {
        lsm::LsmDispatch::Lsm(plan) => {
            let future =
                lsm::execute_lsm_merge_insert(table, plan, params.validate_single_shard, new_data);
            return match params.timeout {
                Some(timeout) => match tokio::time::timeout(timeout, future).await {
                    Ok(result) => result,
                    Err(_) => Err(Error::Runtime {
                        message: "merge insert timed out".to_string(),
                    }),
                },
                None => future.await,
            };
        }
        lsm::LsmDispatch::Standard => {}
    }

    let dataset = table.dataset.get().await?;
    let schema = arrow_schema::Schema::from(dataset.schema());
    // JSON source fields must carry the stored extension metadata just as on
    // append. Keep arrow.json text labelled until Lance encodes it as JSONB.
    let source = if schema
        .fields()
        .iter()
        .any(|field| lance_arrow::json::has_json_fields(field))
    {
        validate_merge_source_fields(new_data.schema().fields(), schema.fields(), "")?;
        let plan = Arc::new(super::datafusion::scannable_exec::ScannableExec::new(
            Box::new(new_data),
            None,
        ));
        let plan = super::datafusion::cast::cast_to_table_schema(plan, &schema)?;
        datafusion_physical_plan::execute_stream(
            plan,
            Arc::new(datafusion_execution::TaskContext::default()),
        )?
    } else {
        new_data.into_stream()
    };
    let mut builder = LanceMergeInsertBuilder::try_new(dataset.clone(), params.on)?;
    match (
        params.when_matched_update_all,
        params.when_matched_update_all_filt,
    ) {
        (false, _) => builder.when_matched(WhenMatched::DoNothing),
        (true, None) => builder.when_matched(WhenMatched::UpdateAll),
        (true, Some(MergeFilter::Sql(filt))) => {
            builder.when_matched(WhenMatched::update_if(&dataset, &filt)?)
        }
        (true, Some(MergeFilter::Expr(expr))) => {
            builder.when_matched(WhenMatched::update_if_expr(expr))
        }
    };
    if params.when_not_matched_insert_all {
        builder.when_not_matched(lance::dataset::WhenNotMatched::InsertAll);
    } else {
        builder.when_not_matched(lance::dataset::WhenNotMatched::DoNothing);
    }
    if params.when_not_matched_by_source_delete {
        let behavior = match params.when_not_matched_by_source_delete_filt {
            Some(MergeFilter::Sql(filter)) => {
                WhenNotMatchedBySource::delete_if(dataset.as_ref(), &filter)?
            }
            Some(MergeFilter::Expr(expr)) => WhenNotMatchedBySource::DeleteIf(expr),
            None => WhenNotMatchedBySource::Delete,
        };
        builder.when_not_matched_by_source(behavior);
    } else {
        builder.when_not_matched_by_source(WhenNotMatchedBySource::Keep);
    }
    builder.use_index(params.use_index);

    let future = if let Some(timeout) = params.timeout {
        let future = builder.retry_timeout(timeout).try_build()?.execute(source);
        Either::Left(tokio::time::timeout(timeout, future).map(|res| match res {
            Ok(Ok((new_dataset, stats))) => Ok((new_dataset, stats)),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err(Error::Runtime {
                message: "merge insert timed out".to_string(),
            }),
        }))
    } else {
        let job = builder.try_build()?;
        Either::Right(job.execute(source).map_err(|e| e.into()))
    };
    let (new_dataset, stats) = future.await?;
    let version = new_dataset.manifest().version;
    table.dataset.update(new_dataset.as_ref().clone());
    Ok(MergeResult {
        version,
        num_updated_rows: stats.num_updated_rows,
        num_inserted_rows: stats.num_inserted_rows,
        num_deleted_rows: stats.num_deleted_rows,
        num_attempts: stats.num_attempts,
        num_rows: stats.num_inserted_rows + stats.num_updated_rows,
    })
}

#[cfg(test)]
mod tests {
    use arrow_array::builder::FixedSizeBinaryBuilder;
    use arrow_array::{
        FixedSizeListArray, Int32Array, NullArray, RecordBatch, RecordBatchIterator,
        RecordBatchReader, StringArray, UInt32Array, UInt64Array,
    };
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::connect;

    #[rstest::rstest]
    #[case(None)]
    #[case(Some("struct"))]
    #[case(Some("list"))]
    #[case(Some("large_list"))]
    #[case(Some("fixed_size_list"))]
    #[case(Some("map"))]
    #[tokio::test]
    async fn merge_json_rejects_ambiguous_source_fields(
        #[case] nested: Option<&str>,
        #[values(false, true)] duplicate: bool,
    ) {
        let mut target = vec![
            Field::new("id", DataType::Int32, false),
            lance_arrow::json::json_field("payload", true),
        ];
        let mut input = vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true),
        ];
        let mut columns: Vec<Arc<dyn arrow_array::Array>> = vec![
            Arc::new(Int32Array::from(vec![1])),
            Arc::new(StringArray::from(vec![r#"{"x":1}"#])),
        ];
        if let Some(nested) = nested {
            let wrap = |children: Vec<Field>| {
                let structure = DataType::Struct(children.into());
                let item = Arc::new(Field::new("item", structure.clone(), true));
                match nested {
                    "struct" => structure,
                    "list" => DataType::List(item),
                    "large_list" => DataType::LargeList(item),
                    "fixed_size_list" => DataType::FixedSizeList(item, 2),
                    "map" => DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(
                                vec![
                                    Field::new("key", DataType::Utf8, false),
                                    Field::new("value", structure, true),
                                ]
                                .into(),
                            ),
                            false,
                        )),
                        false,
                    ),
                    _ => unreachable!(),
                }
            };
            let known = Field::new("value", DataType::Int32, true);
            let unexpected = if duplicate { "value" } else { "extra" };
            let target_type = wrap(vec![known.clone()]);
            let input_type = wrap(vec![known, Field::new(unexpected, DataType::Int32, true)]);
            columns.push(arrow_array::new_null_array(&input_type, 1));
            target.push(Field::new("details", target_type, true));
            input.push(Field::new("details", input_type, true));
        } else {
            let unexpected = if duplicate { "id" } else { "extra" };
            input.push(Field::new(unexpected, DataType::Int32, true));
            columns.push(Arc::new(Int32Array::from(vec![99])));
        }
        let db = connect("memory://").execute().await.unwrap();
        let table = db
            .create_empty_table("ambiguous_json", Arc::new(Schema::new(target)))
            .execute()
            .await
            .unwrap();
        let version = table.version().await.unwrap();
        let schema = Arc::new(Schema::new(input));
        let batch = RecordBatch::try_new(schema.clone(), columns).unwrap();
        let mut merge = table.merge_insert(&["id"]);
        merge.when_not_matched_insert_all();
        let error = merge
            .execute(Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema)))
            .await
            .expect_err("JSON alignment must not drop source fields");
        let expected = if duplicate {
            "more than once"
        } else {
            "not present"
        };
        assert!(error.to_string().contains(expected), "{error}");
        assert_eq!(table.version().await.unwrap(), version);
        assert_eq!(table.count_rows(None).await.unwrap(), 0);
    }

    #[tokio::test]
    async fn merge_json_preserves_empty_extension_metadata() {
        use crate::query::{ExecutableQuery, QueryBase, Select};
        use futures::TryStreamExt;
        let mut stored_json = lance_arrow::json::json_field("payload", true);
        let mut metadata = stored_json.metadata().clone();
        metadata.insert(lance_arrow::ARROW_EXT_META_KEY.into(), String::new());
        stored_json = stored_json.with_metadata(metadata);
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            stored_json,
        ]));
        let db = connect("memory://").execute().await.unwrap();
        let table = db
            .create_empty_table("json_merge_metadata", schema)
            .execute()
            .await
            .unwrap();
        let input_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("payload", DataType::Utf8, true).with_metadata(
                std::collections::HashMap::from([(
                    lance_arrow::ARROW_EXT_NAME_KEY.into(),
                    lance_arrow::json::ARROW_JSON_EXT_NAME.into(),
                )]),
            ),
        ]));
        let seed = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(StringArray::from(vec![r#"{"old":true}"#])),
            ],
        )
        .unwrap();
        table.add(seed).execute().await.unwrap();
        let batch = RecordBatch::try_new(
            input_schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec![
                    r#"{"updated":true}"#,
                    r#"{"inserted":true}"#,
                ])),
            ],
        )
        .unwrap();
        let mut merge = table.merge_insert(&["id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let result = merge
            .execute(Box::new(RecordBatchIterator::new(
                vec![Ok(batch)],
                input_schema,
            )))
            .await
            .unwrap();
        assert_eq!(result.num_updated_rows, 1);
        assert_eq!(result.num_inserted_rows, 1);
        let output = table
            .query()
            .select(Select::columns(&["payload"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let values = output
            .iter()
            .flat_map(|b| {
                b.column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
            })
            .flatten()
            .collect::<Vec<_>>();
        assert_eq!(values.len(), 2);
        assert!(
            values
                .iter()
                .any(|v| serde_json::from_str::<serde_json::Value>(v).unwrap()["updated"] == true)
        );
        assert!(
            values
                .iter()
                .any(|v| serde_json::from_str::<serde_json::Value>(v).unwrap()["inserted"] == true)
        );
    }

    fn merge_insert_test_batches(offset: i32, age: i32) -> Box<dyn RecordBatchReader + Send> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("i", DataType::Int32, false),
            Field::new("age", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(offset..(offset + 10))),
                Arc::new(Int32Array::from_iter_values(std::iter::repeat_n(age, 10))),
            ],
        )
        .unwrap();
        Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
    }

    fn fixed_size_binary_merge_batch(
        id_range: std::ops::Range<u64>,
        price: u64,
    ) -> Box<dyn RecordBatchReader + Send> {
        let ids = id_range.collect::<Vec<_>>();
        let mut id_builder = FixedSizeBinaryBuilder::new(16);
        for id in &ids {
            let mut bytes = [0; 16];
            bytes[..8].copy_from_slice(&id.to_le_bytes());
            id_builder.append_value(bytes).unwrap();
        }

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::FixedSizeBinary(16), false),
            Field::new("id_as_int", DataType::UInt64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("market", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(id_builder.finish()),
                Arc::new(UInt64Array::from_iter_values(ids.iter().copied())),
                Arc::new(StringArray::from_iter_values(
                    ids.iter().map(|id| format!("name{id}")),
                )),
                Arc::new(StringArray::from_iter_values(std::iter::repeat_n(
                    format!("market_{price}"),
                    ids.len(),
                ))),
            ],
        )
        .unwrap();
        Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
    }

    #[tokio::test]
    async fn test_merge_insert() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a dataset with i=0..10
        let batches = merge_insert_test_batches(0, 0);
        let table = conn
            .create_table("my_table", batches)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);

        // Create new data with i=5..15
        let new_batches = merge_insert_test_batches(5, 1);

        // Perform a "insert if not exists"
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        merge_insert_builder.when_not_matched_insert_all();
        let result = merge_insert_builder.execute(new_batches).await.unwrap();
        // Only 5 rows should actually be inserted
        assert_eq!(table.count_rows(None).await.unwrap(), 15);
        assert_eq!(result.num_inserted_rows, 5);
        assert_eq!(result.num_updated_rows, 0);
        assert_eq!(result.num_deleted_rows, 0);
        assert_eq!(result.num_attempts, 1);

        // Create new data with i=15..25 (no id matches)
        let new_batches = merge_insert_test_batches(15, 2);
        // Perform a "bulk update" (should not affect anything)
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        merge_insert_builder.when_matched_update_all(None);
        merge_insert_builder.execute(new_batches).await.unwrap();
        // No new rows should have been inserted
        assert_eq!(table.count_rows(None).await.unwrap(), 15);
        assert_eq!(
            table.count_rows(Some("age = 2".to_string())).await.unwrap(),
            0
        );

        // Conditional update that only replaces the age=0 data
        let new_batches = merge_insert_test_batches(5, 3);
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        merge_insert_builder.when_matched_update_all(Some("target.age = 0".to_string()));
        merge_insert_builder.execute(new_batches).await.unwrap();
        assert_eq!(
            table.count_rows(Some("age = 3".to_string())).await.unwrap(),
            5
        );
    }

    #[tokio::test]
    async fn test_merge_insert_fixed_size_binary_non_nullable() {
        // Regression test for #2869: an unrelated FixedSizeBinary column used to corrupt the
        // outer join that implements when_not_matched_by_source_delete.
        let conn = connect("memory://").execute().await.unwrap();
        let table = conn
            .create_table(
                "fixed_size_binary_merge",
                fixed_size_binary_merge_batch(0..256, 100),
            )
            .execute()
            .await
            .unwrap();

        let mut merge_insert = table.merge_insert(&["id_as_int"]);
        merge_insert
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .when_not_matched_by_source_delete(None);
        let result = merge_insert
            .execute(fixed_size_binary_merge_batch(100..356, 200))
            .await
            .unwrap();

        assert_eq!(result.num_updated_rows, 156);
        assert_eq!(result.num_inserted_rows, 100);
        assert_eq!(result.num_deleted_rows, 100);
        assert_eq!(table.count_rows(None).await.unwrap(), 256);
    }

    #[tokio::test]
    async fn test_merge_insert_use_index() {
        let conn = connect("memory://").execute().await.unwrap();

        // Create a dataset with i=0..10
        let batches = merge_insert_test_batches(0, 0);
        let table = conn
            .create_table("my_table", batches)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);

        // Test use_index=true (default behavior)
        let new_batches = merge_insert_test_batches(5, 1);
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        merge_insert_builder.when_not_matched_insert_all();
        merge_insert_builder.use_index(true);
        merge_insert_builder.execute(new_batches).await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 15);

        // Test use_index=false (force table scan)
        let new_batches = merge_insert_test_batches(15, 2);
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        merge_insert_builder.when_not_matched_insert_all();
        merge_insert_builder.use_index(false);
        merge_insert_builder.execute(new_batches).await.unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 25);
    }

    #[tokio::test]
    async fn test_merge_insert_expr() {
        use datafusion_expr::{col, lit};

        let conn = connect("memory://").execute().await.unwrap();

        // Create a dataset with i=0..10
        let batches = merge_insert_test_batches(0, 0);
        let table = conn
            .create_table("my_table_expr", batches)
            .execute()
            .await
            .unwrap();
        assert_eq!(table.count_rows(None).await.unwrap(), 10);

        // Conditional update that only replaces the age=0 data
        let new_batches = merge_insert_test_batches(5, 3);
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        // use expression: target.age = 0
        let expr = col("target.age").eq(lit(0));
        merge_insert_builder.when_matched_update_all_expr(expr);
        merge_insert_builder.execute(new_batches).await.unwrap();
        assert_eq!(
            table.count_rows(Some("age = 3".to_string())).await.unwrap(),
            5
        );

        // Delete with expression
        // Create new batches with i=10..20 (so target rows i=0..9 are not matched by source)
        let new_batches = merge_insert_test_batches(10, 0); // won't insert or update since we don't enable matched/unmatched actions
        let mut merge_insert_builder = table.merge_insert(&["i"]);
        // delete if target.age = 3
        let delete_expr = col("target.age").eq(lit(3));
        merge_insert_builder.when_not_matched_by_source_delete_expr(delete_expr);
        let result = merge_insert_builder.execute(new_batches).await.unwrap();
        assert_eq!(result.num_deleted_rows, 5);
        assert_eq!(table.count_rows(None).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn test_merge_insert_fixed_size_list_above_u32_child_count() {
        // Arrow's FixedSizeList take kernel uses u32 child indices. Previously,
        // delete-by-source materialized the target payload in a full outer join,
        // causing the final list below to overflow those indices and panic.
        // A Null child keeps this boundary test small in memory.
        const LIST_SIZE: i32 = 65_536;
        const ROW_COUNT: usize = (u32::MAX as usize / LIST_SIZE as usize) + 1;
        const BATCH_SIZE: usize = 8_192;

        let item = Arc::new(Field::new("item", DataType::Null, true));
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::UInt32, false),
            Field::new(
                "vector",
                DataType::FixedSizeList(item.clone(), LIST_SIZE),
                false,
            ),
        ]));
        let batch = |start: usize, len: usize| {
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(UInt32Array::from_iter_values(
                        start as u32..(start + len) as u32,
                    )),
                    Arc::new(FixedSizeListArray::new(
                        item.clone(),
                        LIST_SIZE,
                        Arc::new(NullArray::new(len * LIST_SIZE as usize)),
                        None,
                    )),
                ],
            )
            .unwrap()
        };

        let target_batches = (0..ROW_COUNT)
            .step_by(BATCH_SIZE)
            .map(|start| {
                let len = (ROW_COUNT - start).min(BATCH_SIZE);
                Ok(batch(start, len))
            })
            .collect::<Vec<_>>();
        let target_data: Box<dyn RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(target_batches, schema.clone()));
        let conn = connect("memory://").execute().await.unwrap();
        let table = conn
            .create_table("fixed_size_list_overflow", target_data)
            .execute()
            .await
            .unwrap();

        let source = batch(ROW_COUNT - 1, 1);
        let mut merge = table.merge_insert(&["id"]);
        merge
            .when_matched_update_all(None)
            .when_not_matched_by_source_delete(None);
        let result = merge
            .execute(Box::new(RecordBatchIterator::new([Ok(source)], schema)))
            .await
            .unwrap();

        assert_eq!(result.num_updated_rows, 1);
        assert_eq!(result.num_deleted_rows, (ROW_COUNT - 1) as u64);
        assert_eq!(table.count_rows(None).await.unwrap(), 1);
    }
}

#[cfg(test)]
mod lsm_tests {
    use std::sync::Arc;

    use arrow_array::{
        Int64Array, RecordBatch, RecordBatchIterator, RecordBatchReader, StringArray,
    };
    use arrow_schema::{DataType, Field, Schema};
    use tempfile::{TempDir, tempdir};

    use crate::connect;
    use crate::error::Error;
    use crate::table::{LsmWriteSpec, Table};

    /// A reader of `[id: Int64, value: Int64]` rows; `value` is `0..n`.
    fn id_value_reader(ids: Vec<i64>) -> Box<dyn RecordBatchReader + Send> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("value", DataType::Int64, false),
        ]));
        let n = ids.len() as i64;
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(Int64Array::from_iter_values(0..n)),
            ],
        )
        .unwrap();
        Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
    }

    /// A reader of `[id: Int64, region: Utf8]` rows.
    fn id_region_reader(rows: Vec<(i64, &str)>) -> Box<dyn RecordBatchReader + Send> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        let regions: Vec<&str> = rows.iter().map(|(_, region)| *region).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(regions)),
            ],
        )
        .unwrap();
        Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
    }

    /// A multi-batch reader of `[id: Int64, region: Utf8]` rows.
    fn id_region_multi_reader(batches: Vec<Vec<(i64, &str)>>) -> Box<dyn RecordBatchReader + Send> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
        ]));
        let records: Vec<_> = batches
            .into_iter()
            .map(|rows| {
                let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
                let regions: Vec<&str> = rows.iter().map(|(_, region)| *region).collect();
                Ok(RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(ids)),
                        Arc::new(StringArray::from(regions)),
                    ],
                )
                .unwrap())
            })
            .collect();
        Box::new(RecordBatchIterator::new(records, schema))
    }

    /// Create an `[id, value]` table with `id` as the unenforced primary key.
    async fn id_value_table(dir: &TempDir) -> Table {
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("t", id_value_reader(vec![1, 2, 3]))
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
    }

    #[tokio::test]
    async fn lsm_merge_insert_bucket() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        // num_buckets = 1: every row routes to the single bucket.
        table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 1))
            .await
            .unwrap();

        // Empty `on` defaults to the primary key.
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let result = builder
            .execute(id_value_reader(vec![3, 4, 5]))
            .await
            .unwrap();

        // LSM path: rows go to the MemWAL, the breakdown is unknown until
        // compaction, so only `num_rows` is populated.
        assert_eq!(result.num_rows, 3);
        assert_eq!(result.version, 0);
        assert_eq!(result.num_inserted_rows, 0);
        assert_eq!(result.num_updated_rows, 0);
    }

    #[tokio::test]
    async fn lsm_merge_insert_unsharded() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();

        let mut builder = table.merge_insert(&["id"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let result = builder
            .execute(id_value_reader(vec![10, 11, 12, 13]))
            .await
            .unwrap();
        assert_eq!(result.num_rows, 4);
    }

    #[tokio::test]
    async fn lsm_merge_insert_identity() {
        let dir = tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("t", id_region_reader(vec![(1, "us"), (2, "us")]))
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .set_lsm_write_spec(LsmWriteSpec::identity("region"))
            .await
            .unwrap();

        // All rows share one identity value, so they route to one shard.
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let result = builder
            .execute(id_region_reader(vec![(3, "us"), (4, "us")]))
            .await
            .unwrap();
        assert_eq!(result.num_rows, 2);
    }

    #[tokio::test]
    async fn lsm_merge_insert_use_lsm_false_falls_back() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 1))
            .await
            .unwrap();

        // use_lsm(false) opts out: the standard path runs and commits even though
        // a spec is installed.
        let mut builder = table.merge_insert(&["id"]);
        builder.when_not_matched_insert_all().use_lsm(false);
        let result = builder
            .execute(id_value_reader(vec![3, 4, 5]))
            .await
            .unwrap();

        assert_eq!(result.num_inserted_rows, 2);
        assert_eq!(table.count_rows(None).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn lsm_merge_insert_use_lsm_true_without_spec_errors() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;

        // use_lsm(true) demands MemWAL routing; without a write spec it errors
        // rather than silently falling back to the standard path.
        let mut builder = table.merge_insert(&["id"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all()
            .use_lsm(true);
        let err = builder
            .execute(id_value_reader(vec![3, 4, 5]))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn lsm_merge_insert_rejects_on_not_primary_key() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 1))
            .await
            .unwrap();

        let mut builder = table.merge_insert(&["value"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let err = builder.execute(id_value_reader(vec![1])).await.unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn lsm_merge_insert_rejects_non_upsert() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 1))
            .await
            .unwrap();

        // Insert-only (no when_matched_update_all) is not the upsert shape.
        let mut builder = table.merge_insert(&[]);
        builder.when_not_matched_insert_all();
        let err = builder.execute(id_value_reader(vec![4])).await.unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn lsm_close_writers_then_reopen() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 1))
            .await
            .unwrap();

        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        builder.execute(id_value_reader(vec![7, 8])).await.unwrap();

        table.close_lsm_writers().await.unwrap();

        // The writer reopens lazily on the next merge_insert.
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let result = builder.execute(id_value_reader(vec![9])).await.unwrap();
        assert_eq!(result.num_rows, 1);
    }

    #[tokio::test]
    async fn lsm_merge_insert_multi_batch() {
        let dir = tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("t", id_region_reader(vec![(1, "us")]))
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .set_lsm_write_spec(LsmWriteSpec::identity("region"))
            .await
            .unwrap();

        // Multiple batches that all route to one shard are written together.
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let result = builder
            .execute(id_region_multi_reader(vec![
                vec![(2, "us"), (3, "us")],
                vec![(4, "us")],
            ]))
            .await
            .unwrap();
        assert_eq!(result.num_rows, 3);

        // Batches that route to different shards are rejected; the validation
        // runs before any write, so no partial write is left behind.
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let err = builder
            .execute(id_region_multi_reader(vec![
                vec![(5, "us")],
                vec![(6, "eu")],
            ]))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn lsm_merge_insert_no_spec_uses_standard_path() {
        let dir = tempdir().unwrap();
        // id_value_table sets a primary key but no LSM write spec.
        let table = id_value_table(&dir).await;

        // Without a spec, a default merge_insert (use_lsm unset) simply uses
        // the standard path and commits — no opt-out required, no error.
        let mut builder = table.merge_insert(&["id"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let result = builder
            .execute(id_value_reader(vec![3, 4, 5]))
            .await
            .unwrap();
        assert_eq!(result.num_inserted_rows, 2);
        assert_eq!(table.count_rows(None).await.unwrap(), 5);
    }

    #[tokio::test]
    async fn lsm_merge_insert_rejects_second_shard() {
        let dir = tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("t", id_region_reader(vec![(1, "us")]))
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .set_lsm_write_spec(LsmWriteSpec::identity("region"))
            .await
            .unwrap();

        // The first merge_insert opens the single writer for shard "us".
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        builder
            .execute(id_region_reader(vec![(2, "us")]))
            .await
            .unwrap();

        // A merge_insert routing to a different shard is rejected.
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let err = builder
            .execute(id_region_reader(vec![(3, "eu")]))
            .await
            .unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");

        // After closing the writer, a different shard can be written.
        table.close_lsm_writers().await.unwrap();
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        builder
            .execute(id_region_reader(vec![(4, "eu")]))
            .await
            .unwrap();
    }

    // ---------------------------------------------------------------------
    // LSM read path
    // ---------------------------------------------------------------------

    use crate::arrow::SendableRecordBatchStream;
    use crate::query::{ExecutableQuery, QueryBase};
    use arrow::array::AsArray;
    use arrow::datatypes::Int64Type;
    use futures::TryStreamExt;

    /// Collect `(id, value)` pairs from a result stream, sorted by id.
    async fn collect_id_value(stream: SendableRecordBatchStream) -> Vec<(i64, i64)> {
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let mut rows = Vec::new();
        for batch in &batches {
            let ids = batch
                .column_by_name("id")
                .unwrap()
                .as_primitive::<Int64Type>();
            let values = batch
                .column_by_name("value")
                .unwrap()
                .as_primitive::<Int64Type>();
            for i in 0..batch.num_rows() {
                rows.push((ids.value(i), values.value(i)));
            }
        }
        rows.sort();
        rows
    }

    async fn collect_ids(stream: SendableRecordBatchStream) -> Vec<i64> {
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column_by_name("id")
                    .unwrap()
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect()
    }

    /// Upsert `ids` (value = 0..n) through the LSM `merge_insert` path.
    async fn lsm_upsert(table: &Table, ids: Vec<i64>) {
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        builder.execute(id_value_reader(ids)).await.unwrap();
    }

    #[tokio::test]
    async fn lsm_read_sees_active_memtable() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await; // base: ids 1,2,3 (value 0,1,2)
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();

        // Insert ids 4,5 into the active memtable (not committed to base).
        lsm_upsert(&table, vec![4, 5]).await;

        // Default read auto-routes through the LSM scanner: base ∪ active memtable.
        let lsm = table.query().execute().await.unwrap();
        let rows = collect_id_value(lsm).await;
        assert_eq!(
            rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );

        // use_lsm(false) bypasses the MemWAL and reads the base table only.
        let base_only = table.query().use_lsm(false).execute().await.unwrap();
        let rows = collect_id_value(base_only).await;
        assert_eq!(
            rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    #[tokio::test]
    async fn query_snapshot_preserves_lsm_read_semantics() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();
        lsm_upsert(&table, vec![4, 5]).await;

        let snapshot = table.query_snapshot().await.unwrap();
        let rows = collect_id_value(snapshot.query().execute().await.unwrap()).await;
        assert_eq!(
            rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
    }

    #[tokio::test]
    async fn query_snapshot_preserves_time_travel_lsm_guard() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();
        lsm_upsert(&table, vec![4]).await;

        let version = table.version().await.unwrap();
        table.checkout(version).await.unwrap();
        let direct_error = table.query().execute().await.err().unwrap();
        assert!(matches!(direct_error, Error::NotSupported { .. }));

        let snapshot = table.query_snapshot().await.unwrap();
        let snapshot_error = snapshot.query().execute().await.err().unwrap();
        assert!(matches!(snapshot_error, Error::NotSupported { .. }));
    }

    #[tokio::test]
    async fn lsm_read_dedup_newest_wins() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await; // base: id 2 -> value 1
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();

        // Upsert ids 2,3,4 with values 0,1,2. id 2 and 3 shadow the base rows.
        lsm_upsert(&table, vec![2, 3, 4]).await;

        let lsm = table.query().execute().await.unwrap();
        let rows = collect_id_value(lsm).await;
        // id 1 from base (value 0); ids 2,3,4 from memtable (values 0,1,2).
        assert_eq!(rows, vec![(1, 0), (2, 0), (3, 1), (4, 2)]);
    }

    #[tokio::test]
    async fn lsm_read_point_lookup_filter() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();
        lsm_upsert(&table, vec![2, 3, 4]).await; // id 2 -> value 0 (shadows base)

        let lsm = table.query().only_if("id = 2").execute().await.unwrap();
        let rows = collect_id_value(lsm).await;
        assert_eq!(rows, vec![(2, 0)]);
    }

    #[tokio::test]
    async fn lsm_read_multi_shard() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::bucket("id", 8))
            .await
            .unwrap();

        // Two single-row upserts that route to (likely) different buckets; each
        // closes the writer so the next opens a fresh shard.
        lsm_upsert(&table, vec![10]).await;
        table.close_lsm_writers().await.unwrap();
        lsm_upsert(&table, vec![11]).await;

        let lsm = table.query().execute().await.unwrap();
        let rows = collect_id_value(lsm).await;
        let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        // Base 1,2,3 + flushed/active shards for 10 and 11.
        assert_eq!(ids, vec![1, 2, 3, 10, 11]);
    }

    #[tokio::test]
    async fn lsm_read_after_close_sees_flushed() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();
        lsm_upsert(&table, vec![4, 5]).await;
        // close flushes the active memtable to an on-disk generation and drops
        // the cached writer; the read must still see those rows via the shard
        // manifest snapshot.
        table.close_lsm_writers().await.unwrap();

        let lsm = table.query().execute().await.unwrap();
        let ids: Vec<i64> = collect_id_value(lsm)
            .await
            .iter()
            .map(|(id, _)| *id)
            .collect();
        assert_eq!(ids, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn lsm_read_without_spec_reads_base() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await; // no LSM write spec

        // With no spec installed there is nothing to route: the default read and
        // an explicit use_lsm(false) both read the base table without error.
        for query in [table.query(), table.query().use_lsm(false)] {
            let rows = collect_id_value(query.execute().await.unwrap()).await;
            assert_eq!(
                rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
                vec![1, 2, 3]
            );
        }
    }

    #[tokio::test]
    async fn lsm_read_unsupported_shape_errors_without_use_lsm_false() {
        let dir = tempdir().unwrap();
        let table = id_value_table(&dir).await;
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();
        lsm_upsert(&table, vec![4]).await;

        // `with_row_id` is a shape the LSM scanner cannot honor. On a MemWAL
        // table the default (auto-routed) read hard-errors rather than silently
        // reading a stale base-only result that would exclude un-compacted row 4.
        let err = table
            .query()
            .with_row_id()
            .execute()
            .await
            .err()
            .expect("unsupported shape on a MemWAL table must error");
        assert!(matches!(err, Error::NotSupported { .. }), "got {err:?}");

        // use_lsm(false) is the escape hatch: it reads the base table only.
        let rows = collect_id_value(
            table
                .query()
                .with_row_id()
                .use_lsm(false)
                .execute()
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(
            rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
    }

    /// A table with `id`/`text` and a primary key, ready for an LSM write spec.
    async fn lsm_text_table(dir: &tempfile::TempDir) -> crate::Table {
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("t", id_text_reader(vec![(1, "alpha")]))
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
    }

    /// Upsert `rows` through the LSM write path.
    async fn upsert_text(table: &crate::Table, rows: Vec<(i64, &str)>) {
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        builder.execute(id_text_reader(rows)).await.unwrap();
    }

    /// A reader of `[id: Int64, text: Utf8]` rows.
    fn id_text_reader(rows: Vec<(i64, &str)>) -> Box<dyn RecordBatchReader + Send> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("text", DataType::Utf8, false),
        ]));
        let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
        let texts: Vec<&str> = rows.iter().map(|(_, t)| *t).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(texts)),
            ],
        )
        .unwrap();
        Box::new(RecordBatchIterator::new(vec![Ok(batch)], schema))
    }

    /// Under the default set, an index created while a writer is open reaches
    /// that writer.
    ///
    /// The default maintains every index the table has, so creating one changes
    /// what the table maintains without anyone touching the spec. A writer left
    /// on its old configs would keep building MemTables the new index does not
    /// cover.
    #[tokio::test]
    async fn lsm_default_set_picks_up_an_index_created_while_writing() {
        use crate::index::Index;
        use lance_index::scalar::FullTextSearchQuery;

        let dir = tempdir().unwrap();
        let table = lsm_text_table(&dir).await;
        // The default set, on a table with no FTS index yet.
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();

        // Opens the writer, whose MemTable carries no FTS index.
        upsert_text(&table, vec![(99, "zebra")]).await;

        // Created with the writer still open and never reopened.
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        upsert_text(&table, vec![(100, "zebra stripes")]).await;

        let query = FullTextSearchQuery::new("stripes".to_string())
            .with_column("text".to_string())
            .unwrap();
        let batches = table
            .query()
            .full_text_search(query)
            .execute()
            .await
            .expect("the created index reached the open writer")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let found: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(found, 1, "the row written after create_index must be found");
    }

    /// A spec is replaced by unsetting it and setting the new one.
    ///
    /// `unset_lsm_write_spec` drains the open writer, so the rows it held
    /// survive the replacement, and the writer the next write opens builds its
    /// MemTables from the new set.
    #[tokio::test]
    async fn lsm_spec_is_replaced_by_unsetting_it_first() {
        use crate::index::Index;
        use lance_index::scalar::FullTextSearchQuery;

        let dir = tempdir().unwrap();
        let table = lsm_text_table(&dir).await;
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        let fts_index = table.list_indices().await.unwrap()[0].name.clone();

        // Installed maintaining nothing, then written to: the writer opens with
        // no FTS index.
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded().with_maintained_indexes(Vec::new()))
            .await
            .unwrap();
        upsert_text(&table, vec![(99, "zebra")]).await;

        // Setting over an installed spec is refused; unset, then set.
        let err = table
            .set_lsm_write_spec(
                LsmWriteSpec::unsharded().with_maintained_indexes(vec![fts_index.clone()]),
            )
            .await
            .expect_err("an installed spec cannot be set over");
        assert!(
            err.to_string().contains("already set"),
            "unexpected error: {err}"
        );
        table.unset_lsm_write_spec().await.unwrap();
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded().with_maintained_indexes(vec![fts_index]))
            .await
            .unwrap();

        // A row written after the replacement is answered by the index the new
        // writer maintains.
        upsert_text(&table, vec![(100, "zebra stripes")]).await;

        let query = FullTextSearchQuery::new("stripes".to_string())
            .with_column("text".to_string())
            .unwrap();
        let batches = table
            .query()
            .full_text_search(query)
            .execute()
            .await
            .expect("the replaced spec maintains the index")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let found: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(
            found, 1,
            "the row written after the replacement must be found"
        );
    }

    /// A row written before the index exists is searchable once it does.
    ///
    /// The MemTable holding it was built without the index. Creating the index
    /// replaces the writer's configs, which seals that MemTable and waits for
    /// its flush, so no resident MemTable is left that cannot answer the read.
    #[tokio::test]
    async fn lsm_a_row_written_before_the_index_is_searchable_after_it() {
        use crate::index::Index;
        use lance_index::scalar::FullTextSearchQuery;

        let dir = tempdir().unwrap();
        let table = lsm_text_table(&dir).await;
        // No index yet, and the default set: maintain whatever the table has.
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();

        // Opens the writer, whose MemTable therefore carries no FTS index.
        upsert_text(&table, vec![(99, "zebra")]).await;

        // Built afterwards: the spec now resolves to it, the MemTable does not.
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();

        let query = FullTextSearchQuery::new("zebra".to_string())
            .with_column("text".to_string())
            .unwrap();
        let batches = table
            .query()
            .full_text_search(query)
            .execute()
            .await
            .expect("no resident MemTable is left that cannot answer")
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let found: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(found, 1, "the row written before the index must be found");
    }

    #[tokio::test]
    async fn lsm_read_full_text_search() {
        use crate::index::Index;
        use lance_index::scalar::FullTextSearchQuery;

        let dir = tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table(
                "t",
                id_text_reader(vec![(1, "alpha"), (2, "beta"), (3, "gamma")]),
            )
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .create_index(&["text"], Index::FTS(Default::default()))
            .execute()
            .await
            .unwrap();
        let fts_index = table.list_indices().await.unwrap()[0].name.clone();
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded().with_maintained_indexes(vec![fts_index]))
            .await
            .unwrap();

        // Insert a row whose term ("zebra") exists in no base row.
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        builder
            .execute(id_text_reader(vec![(99, "zebra")]))
            .await
            .unwrap();

        let search = |term: &str| {
            let q = FullTextSearchQuery::new(term.to_string())
                .with_column("text".to_string())
                .unwrap();
            table.query().full_text_search(q)
        };

        // "zebra" lives only in the active memtable; LSM read finds it.
        let stream = search("zebra").execute().await.unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "LSM FTS must surface the memtable row");

        // A base-only term still matches the base table through the LSM scan.
        let stream = search("alpha").execute().await.unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(rows, 1, "LSM FTS must still see base rows");
    }

    /// A table whose vector index the LSM maintains: base rows filled with
    /// their id, and one memtable row filled with 1000, nearest to `[1000; 8]`.
    /// `registry` replaces the table's memtable kinds.
    async fn lsm_vector_table(
        dir: &std::path::Path,
        registry: Option<lance::dataset::mem_wal::MemIndexRegistry>,
    ) -> Table {
        use crate::index::Index;
        use crate::index::vector::IvfPqIndexBuilder;
        use arrow::array::{FixedSizeListBuilder, Float32Builder};

        const DIM: i32 = 8;
        const N: i64 = 256;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), DIM),
                false,
            ),
        ]));
        let make_batch = |rows: Vec<(i64, f32)>| -> RecordBatch {
            let ids: Vec<i64> = rows.iter().map(|(id, _)| *id).collect();
            let mut vb = FixedSizeListBuilder::new(Float32Builder::new(), DIM);
            for (_, fill) in &rows {
                for _ in 0..DIM {
                    vb.values().append_value(*fill);
                }
                vb.append(true);
            }
            RecordBatch::try_new(
                schema.clone(),
                vec![Arc::new(Int64Array::from(ids)), Arc::new(vb.finish())],
            )
            .unwrap()
        };

        let conn = connect(dir.to_str().unwrap()).execute().await.unwrap();
        let base = make_batch((0..N).map(|i| (i, i as f32)).collect());
        let base_reader: Box<dyn RecordBatchReader + Send> =
            Box::new(RecordBatchIterator::new(vec![Ok(base)], schema.clone()));
        let table = conn.create_table("t", base_reader).execute().await.unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .create_index(
                &["vec"],
                Index::IvfPq(
                    IvfPqIndexBuilder::default()
                        .num_partitions(8)
                        .num_sub_vectors(2),
                ),
            )
            .execute()
            .await
            .unwrap();
        if let Some(registry) = registry {
            table.as_native().unwrap().set_mem_index_registry(registry);
        }
        let vec_index = table.list_indices().await.unwrap()[0].name.clone();
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded().with_maintained_indexes(vec![vec_index]))
            .await
            .unwrap();

        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let insert_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(make_batch(vec![(9999, 1000.0)]))],
            schema.clone(),
        ));
        builder.execute(insert_reader).await.unwrap();
        table
    }

    /// The ids of the nearest row to `[1000; 8]` through the default read.
    async fn nearest_ids(table: &Table) -> Vec<i64> {
        let query = table.query().nearest_to(&[1000.0_f32; 8]).unwrap().limit(1);
        collect_ids(query.execute().await.unwrap()).await
    }

    #[tokio::test]
    async fn lsm_read_vector_search() {
        let dir = tempdir().unwrap();
        let table = lsm_vector_table(dir.path(), None).await;
        assert_eq!(
            nearest_ids(&table).await,
            vec![9999],
            "LSM vector search must rank the memtable row first"
        );

        let base_query = table
            .query()
            .nearest_to(&[0.0_f32; 8])
            .unwrap()
            .only_if("id = 255")
            .limit(1);
        for query in [
            base_query.clone(),
            base_query.clone().nprobes(8).unwrap(),
            base_query.clone().minimum_nprobes(8).unwrap(),
            base_query.clone().maximum_nprobes(Some(8)).unwrap(),
            base_query
                .nprobes(1)
                .unwrap()
                .maximum_nprobes(None)
                .unwrap(),
        ] {
            let ids = collect_ids(query.execute().await.unwrap()).await;
            assert_eq!(ids, vec![255]);
        }
    }

    /// The built-in vector plugin, except that its index answers no search.
    #[derive(Debug)]
    struct AnswersNothing;

    #[derive(Debug)]
    struct AnswersNothingIndex(Arc<dyn lance::dataset::mem_wal::index::MemIndex>);

    fn vector_plugin() -> Arc<dyn lance::dataset::mem_wal::index::MemIndexPlugin> {
        Arc::new(lance::dataset::mem_wal::index::HnswMemIndexPlugin)
    }

    #[async_trait::async_trait]
    impl lance::dataset::mem_wal::index::MemIndexPlugin for AnswersNothing {
        fn name(&self) -> &str {
            "AnswersNothing"
        }
        fn details_message(&self) -> &str {
            "VectorIndexDetails"
        }
        fn flush_index_type(&self) -> lance_index::IndexType {
            vector_plugin().flush_index_type()
        }
        fn training_criteria(&self) -> lance_index::scalar::registry::TrainingCriteria {
            vector_plugin().training_criteria()
        }
        async fn resolve(
            &self,
            ctx: &lance::dataset::mem_wal::index::ResolveContext<'_>,
        ) -> lance_core::Result<lance::dataset::mem_wal::index::ResolvedIndex> {
            vector_plugin().resolve(ctx).await
        }
        fn validate(
            &self,
            ctx: &lance::dataset::mem_wal::index::MemIndexBuildContext<'_>,
        ) -> lance_core::Result<()> {
            vector_plugin().validate(ctx)
        }
        fn create(
            &self,
            ctx: &lance::dataset::mem_wal::index::MemIndexBuildContext<'_>,
        ) -> lance_core::Result<Arc<dyn lance::dataset::mem_wal::index::MemIndex>> {
            Ok(Arc::new(AnswersNothingIndex(vector_plugin().create(ctx)?)))
        }
    }

    #[async_trait::async_trait]
    impl lance::dataset::mem_wal::index::MemIndex for AnswersNothingIndex {
        fn columns(&self) -> &[String] {
            self.0.columns()
        }
        fn can_answer(&self, _query: &dyn lance::dataset::mem_wal::index::MemQuery) -> bool {
            false
        }
        fn insert(&self, batch: &RecordBatch, row_offset: u64) -> lance_core::Result<()> {
            self.0.insert(batch, row_offset)
        }
        fn insert_batches(
            &self,
            batches: &[lance::dataset::mem_wal::write::StoredBatch],
        ) -> lance_core::Result<()> {
            self.0.insert_batches(batches)
        }
        fn resident_bytes(&self) -> usize {
            self.0.resident_bytes()
        }
        fn search(
            &self,
            _query: &dyn lance::dataset::mem_wal::index::MemQuery,
            _ctx: &lance::dataset::mem_wal::index::SearchContext,
        ) -> lance_core::Result<Option<lance::dataset::mem_wal::index::MemMatches>> {
            Ok(None)
        }
        async fn flush(
            &self,
            ctx: &lance::dataset::mem_wal::index::FlushContext<'_>,
        ) -> lance_core::Result<lance::dataset::mem_wal::index::FlushOutcome> {
            self.0.flush(ctx).await
        }
    }

    /// A resident memtable whose vector index answers nothing still serves an
    /// LSM vector search: Lance compares every row instead.
    #[tokio::test]
    async fn lsm_vector_search_compares_every_row_when_no_resident_index_answers() {
        let dir = tempdir().unwrap();
        let mut registry = lance::dataset::mem_wal::MemIndexRegistry::default();
        registry.replace_plugin(Arc::new(AnswersNothing)).unwrap();
        let table = lsm_vector_table(dir.path(), Some(registry)).await;
        assert_eq!(nearest_ids(&table).await, vec![9999]);

        // The second page starts after the memtable row.
        let page = table
            .query()
            .nearest_to(&[1000.0_f32; 8])
            .unwrap()
            .limit(2)
            .offset(1);
        assert_eq!(
            collect_ids(page.execute().await.unwrap()).await,
            vec![255, 254]
        );
    }

    /// Claims the base table's bitmap index and keeps it as a B-tree, whose
    /// flush rows are in the shape the bitmap trainer takes.
    #[derive(Debug)]
    struct BitmapAsBTree;

    fn btree_plugin() -> Arc<dyn lance::dataset::mem_wal::index::MemIndexPlugin> {
        lance::dataset::mem_wal::MemIndexRegistry::default()
            .plugin_for_details_url("/lance.table.BTreeIndexDetails")
            .unwrap()
            .clone()
    }

    #[async_trait::async_trait]
    impl lance::dataset::mem_wal::index::MemIndexPlugin for BitmapAsBTree {
        fn name(&self) -> &str {
            "BitmapAsBTree"
        }
        fn details_message(&self) -> &str {
            "BitmapIndexDetails"
        }
        fn flush_index_type(&self) -> lance_index::IndexType {
            lance_index::IndexType::Bitmap
        }
        fn training_criteria(&self) -> lance_index::scalar::registry::TrainingCriteria {
            btree_plugin().training_criteria()
        }
        fn validate(
            &self,
            ctx: &lance::dataset::mem_wal::index::MemIndexBuildContext<'_>,
        ) -> lance_core::Result<()> {
            btree_plugin().validate(ctx)
        }
        fn create(
            &self,
            ctx: &lance::dataset::mem_wal::index::MemIndexBuildContext<'_>,
        ) -> lance_core::Result<Arc<dyn lance::dataset::mem_wal::index::MemIndex>> {
            btree_plugin().create(ctx)
        }
    }

    /// A kind Lance has no memtable index for is refused when named until the
    /// table's registry maintains it; then it is maintained, flushed with each
    /// generation, and answers searches on the flushed generations.
    #[tokio::test]
    async fn lsm_maintains_an_index_kind_from_the_tables_registry() {
        let dir = tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("t", id_region_reader(vec![(1, "us"), (2, "eu"), (3, "us")]))
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        table
            .create_index(&["region"], crate::index::Index::Bitmap(Default::default()))
            .name("region_bitmap".to_string())
            .execute()
            .await
            .unwrap();
        let spec =
            || LsmWriteSpec::unsharded().with_maintained_indexes(vec!["region_bitmap".to_string()]);

        let refused = table.set_lsm_write_spec(spec()).await.unwrap_err();
        assert!(
            refused.to_string().contains("region_bitmap")
                && refused
                    .to_string()
                    .contains("no registered plugin maintains"),
            "{refused:?}"
        );

        let registry = lance::dataset::mem_wal::MemIndexRegistry::default()
            .with_plugin(Arc::new(BitmapAsBTree))
            .unwrap();
        table.as_native().unwrap().set_mem_index_registry(registry);
        table.set_lsm_write_spec(spec()).await.unwrap();

        // The registry is not stored with the table.
        let other = conn.open_table("t").execute().await.unwrap();
        let mut builder = other.merge_insert(&["id"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let refused = builder
            .execute(id_region_reader(vec![(20, "us")]))
            .await
            .unwrap_err();
        assert!(
            refused
                .to_string()
                .contains("no registered plugin maintains"),
            "{refused:?}"
        );

        let mut builder = table.merge_insert(&["id"]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        builder
            .execute(id_region_reader(vec![
                (10, "fresh"),
                (11, "fresh"),
                (12, "eu"),
            ]))
            .await
            .unwrap();

        let fresh = || async {
            let query = table.query().only_if("region = 'fresh'");
            let mut ids = collect_ids(query.execute().await.unwrap()).await;
            ids.sort_unstable();
            ids
        };
        assert_eq!(fresh().await, vec![10, 11]);
        let (_, _, memtables) = table
            .as_native()
            .unwrap()
            .dataset
            .shard_writer()
            .read_snapshot()
            .await
            .unwrap()
            .expect("the merge opened a writer");
        let active = memtables.expect("the writer keeps memtables").active;
        assert!(
            active
                .index_store
                .index_names()
                .contains(&"region_bitmap".to_string()),
            "the memtable maintains the plugin's index: {:?}",
            active.index_store.index_names()
        );

        table.close_lsm_writers().await.unwrap();
        assert_eq!(fresh().await, vec![10, 11]);
        assert_generations_answer_through_region_bitmap(&table, vec![10, 11]).await;
    }

    /// The generations each shard's manifest publishes.
    async fn published_generations(table: &Table) -> Vec<lance::Dataset> {
        use lance::dataset::mem_wal::ShardManifestStore;

        let uri = table.uri().await.unwrap();
        let (store, base) = lance_io::object_store::ObjectStore::from_uri(&uri)
            .await
            .unwrap();
        let mem_wal = std::path::Path::new(&uri).join("_mem_wal");
        let mut generations = Vec::new();
        for shard in std::fs::read_dir(&mem_wal).unwrap() {
            let Ok(shard_id) = shard.unwrap().file_name().to_string_lossy().parse() else {
                continue;
            };
            let manifest = ShardManifestStore::new(store.clone(), &base, shard_id, 2)
                .latest()
                .await
                .unwrap()
                .expect("a shard directory has a manifest");
            for sstable in manifest.sstables {
                let path = mem_wal.join(shard_id.to_string()).join(&sstable.path);
                generations.push(lance::Dataset::open(path.to_str().unwrap()).await.unwrap());
            }
        }
        generations
    }

    /// Every published generation carries `region_bitmap` as a bitmap index on
    /// `region`, and a search for `region = 'fresh'` on it uses that index.
    async fn assert_generations_answer_through_region_bitmap(table: &Table, fresh: Vec<i64>) {
        use lance::index::DatasetIndexExt;

        let generations = published_generations(table).await;
        assert!(
            !generations.is_empty(),
            "closing the writer flushed a generation"
        );
        let mut found = Vec::new();
        for generation in generations {
            let region = generation.schema().field("region").unwrap().id;
            let indices = generation.load_indices().await.unwrap();
            let index = indices
                .iter()
                .find(|index| index.name == "region_bitmap")
                .expect("the generation carries the index the plugin trained");
            assert_eq!(index.fields, vec![region]);
            assert!(
                index
                    .index_details
                    .as_ref()
                    .is_some_and(|details| details.type_url.ends_with("BitmapIndexDetails")),
                "{:?}",
                index.index_details
            );

            let mut scan = generation.scan();
            scan.filter("region = 'fresh'").unwrap();
            let plan = scan.explain_plan(false).await.unwrap();
            assert!(plan.contains("ScalarIndexQuery"), "{plan}");
            let batch = scan.try_into_batch().await.unwrap();
            found.extend(
                batch["id"]
                    .as_primitive::<Int64Type>()
                    .values()
                    .iter()
                    .copied(),
            );
        }
        found.sort_unstable();
        assert_eq!(found, fresh);
    }

    /// Under the default set, a kind only the registry maintains is picked up
    /// when its index is created after the writer opened, and the table's
    /// clones share the registry.
    #[tokio::test]
    async fn lsm_default_set_maintains_a_registry_kind_created_while_writing() {
        let dir = tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("t", id_region_reader(vec![(1, "us"), (2, "eu")]))
            .execute()
            .await
            .unwrap();
        table.set_unenforced_primary_key(["id"]).await.unwrap();
        let registry = lance::dataset::mem_wal::MemIndexRegistry::default()
            .with_plugin(Arc::new(BitmapAsBTree))
            .unwrap();
        table.as_native().unwrap().set_mem_index_registry(registry);
        let clone = table.clone();
        assert!(
            clone
                .as_native()
                .unwrap()
                .mem_index_registry()
                .plugin_for_details_url("/lance.table.BitmapIndexDetails")
                .is_some(),
            "a clone shares the handle's registry"
        );
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded())
            .await
            .unwrap();

        let upsert = |rows: Vec<(i64, &'static str)>| {
            let table = table.clone();
            async move {
                let mut builder = table.merge_insert(&["id"]);
                builder
                    .when_matched_update_all(None)
                    .when_not_matched_insert_all();
                builder.execute(id_region_reader(rows)).await.unwrap();
            }
        };
        // Opens the writer before the table has a bitmap index.
        upsert(vec![(3, "us")]).await;
        table
            .create_index(&["region"], crate::index::Index::Bitmap(Default::default()))
            .name("region_bitmap".to_string())
            .execute()
            .await
            .unwrap();
        upsert(vec![(10, "fresh"), (11, "fresh")]).await;

        let (_, _, memtables) = table
            .as_native()
            .unwrap()
            .dataset
            .shard_writer()
            .read_snapshot()
            .await
            .unwrap()
            .expect("the merge opened a writer");
        let active = memtables.expect("the writer keeps memtables").active;
        assert!(
            active
                .index_store
                .index_names()
                .contains(&"region_bitmap".to_string()),
            "the open writer picked up the new index: {:?}",
            active.index_store.index_names()
        );

        table.close_lsm_writers().await.unwrap();
        // Only the generation written after the index exists carries it.
        let generations = published_generations(&table).await;
        let mut carrying = 0;
        for generation in &generations {
            use lance::index::DatasetIndexExt;
            if generation
                .load_indices()
                .await
                .unwrap()
                .iter()
                .any(|index| index.name == "region_bitmap")
            {
                carrying += 1;
            }
        }
        assert!(
            carrying >= 1,
            "{} generations, none carry the index",
            generations.len()
        );
    }
}
