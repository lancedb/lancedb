// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatchReader;
use futures::future::Either;
use futures::{FutureExt, TryFutureExt};
use lance::dataset::{
    MergeInsertBuilder as LanceMergeInsertBuilder, WhenMatched, WhenNotMatchedBySource,
};
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

/// A builder used to create and run a merge insert operation
///
/// See [`super::Table::merge_insert`] for more context
#[derive(Debug, Clone)]
pub struct MergeInsertBuilder {
    table: Arc<dyn BaseTable>,
    pub(crate) on: Vec<String>,
    pub(crate) when_matched_update_all: bool,
    pub(crate) when_matched_update_all_filt: Option<String>,
    pub(crate) when_not_matched_insert_all: bool,
    pub(crate) when_not_matched_by_source_delete: bool,
    pub(crate) when_not_matched_by_source_delete_filt: Option<String>,
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
    /// matched rows that satisfy the condtion will be updated.  Any
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
        self.when_matched_update_all_filt = condition;
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
        self.when_not_matched_by_source_delete_filt = filter;
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
    pub async fn execute(self, new_data: Box<dyn RecordBatchReader + Send>) -> Result<MergeResult> {
        self.table.clone().merge_insert(self, new_data).await
    }
}

/// Internal implementation of the merge insert logic
///
/// This logic was moved from NativeTable::merge_insert to keep table.rs clean.
pub(crate) async fn execute_merge_insert(
    table: &NativeTable,
    params: MergeInsertBuilder,
    new_data: Box<dyn RecordBatchReader + Send>,
) -> Result<MergeResult> {
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
    let mut builder = LanceMergeInsertBuilder::try_new(dataset.clone(), params.on)?;
    match (
        params.when_matched_update_all,
        params.when_matched_update_all_filt,
    ) {
        (false, _) => builder.when_matched(WhenMatched::DoNothing),
        (true, None) => builder.when_matched(WhenMatched::UpdateAll),
        (true, Some(filt)) => builder.when_matched(WhenMatched::update_if(&dataset, &filt)?),
    };
    if params.when_not_matched_insert_all {
        builder.when_not_matched(lance::dataset::WhenNotMatched::InsertAll);
    } else {
        builder.when_not_matched(lance::dataset::WhenNotMatched::DoNothing);
    }
    if params.when_not_matched_by_source_delete {
        let behavior = if let Some(filter) = params.when_not_matched_by_source_delete_filt {
            WhenNotMatchedBySource::delete_if(dataset.as_ref(), &filter)?
        } else {
            WhenNotMatchedBySource::Delete
        };
        builder.when_not_matched_by_source(behavior);
    } else {
        builder.when_not_matched_by_source(WhenNotMatchedBySource::Keep);
    }
    builder.use_index(params.use_index);

    let future = if let Some(timeout) = params.timeout {
        let future = builder
            .retry_timeout(timeout)
            .try_build()?
            .execute_reader(new_data);
        Either::Left(tokio::time::timeout(timeout, future).map(|res| match res {
            Ok(Ok((new_dataset, stats))) => Ok((new_dataset, stats)),
            Ok(Err(e)) => Err(e.into()),
            Err(_) => Err(Error::Runtime {
                message: "merge insert timed out".to_string(),
            }),
        }))
    } else {
        let job = builder.try_build()?;
        Either::Right(job.execute_reader(new_data).map_err(|e| e.into()))
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
    use arrow_array::{Int32Array, RecordBatch, RecordBatchIterator, RecordBatchReader};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    use crate::connect;

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
            .set_lsm_write_spec(LsmWriteSpec::unsharded().with_maintained_indexes([fts_index]))
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

    #[tokio::test]
    async fn lsm_read_vector_search() {
        use crate::index::Index;
        use crate::index::vector::IvfPqIndexBuilder;
        use arrow::array::{FixedSizeListBuilder, Float32Builder};
        use arrow::datatypes::Int64Type;

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

        let dir = tempdir().unwrap();
        let conn = connect(dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        // Base rows fill each vector with its own id (0..256); all far from 1000.
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
                        .num_partitions(1)
                        .num_sub_vectors(2),
                ),
            )
            .execute()
            .await
            .unwrap();
        let vec_index = table.list_indices().await.unwrap()[0].name.clone();
        table
            .set_lsm_write_spec(LsmWriteSpec::unsharded().with_maintained_indexes([vec_index]))
            .await
            .unwrap();

        // Insert a vector (filled with 1000) that is nearest to the query.
        let mut builder = table.merge_insert(&[]);
        builder
            .when_matched_update_all(None)
            .when_not_matched_insert_all();
        let insert_reader: Box<dyn RecordBatchReader + Send> = Box::new(RecordBatchIterator::new(
            vec![Ok(make_batch(vec![(9999, 1000.0)]))],
            schema.clone(),
        ));
        builder.execute(insert_reader).await.unwrap();

        // KNN near [1000; DIM]: the default (auto-routed) read surfaces the
        // memtable row.
        let stream = table
            .query()
            .nearest_to(&[1000.0_f32; 8])
            .unwrap()
            .limit(1)
            .execute()
            .await
            .unwrap();
        let batches: Vec<_> = stream.try_collect().await.unwrap();
        let ids: Vec<i64> = batches
            .iter()
            .flat_map(|b| {
                b.column_by_name("id")
                    .unwrap()
                    .as_primitive::<Int64Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(
            ids,
            vec![9999],
            "LSM vector search must rank the memtable row first"
        );
    }
}
