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
