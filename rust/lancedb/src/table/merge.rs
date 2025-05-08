// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{sync::Arc, time::Duration};

use arrow_array::RecordBatchReader;

use crate::Result;

use super::{BaseTable, MergeResult};

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

    /// Executes the merge insert operation
    ///
    /// Returns version and statistics about the merge operation including the number of rows
    /// inserted, updated, and deleted.
    pub async fn execute(self, new_data: Box<dyn RecordBatchReader + Send>) -> Result<MergeResult> {
        self.table.clone().merge_insert(self, new_data).await
    }
}
