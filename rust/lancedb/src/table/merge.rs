// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Arc;

use arrow_array::RecordBatchReader;

use crate::Result;

use super::TableInternal;

/// A builder used to create and run a merge insert operation
///
/// See [`super::Table::merge_insert`] for more context
#[derive(Debug, Clone)]
pub struct MergeInsertBuilder {
    table: Arc<dyn TableInternal>,
    pub(crate) on: Vec<String>,
    pub(crate) when_matched_update_all: bool,
    pub(crate) when_matched_update_all_filt: Option<String>,
    pub(crate) when_not_matched_insert_all: bool,
    pub(crate) when_not_matched_by_source_delete: bool,
    pub(crate) when_not_matched_by_source_delete_filt: Option<String>,
}

impl MergeInsertBuilder {
    pub(super) fn new(table: Arc<dyn TableInternal>, on: Vec<String>) -> Self {
        Self {
            table,
            on,
            when_matched_update_all: false,
            when_matched_update_all_filt: None,
            when_not_matched_insert_all: false,
            when_not_matched_by_source_delete: false,
            when_not_matched_by_source_delete_filt: None,
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

    /// Executes the merge insert operation
    ///
    /// Nothing is returned but the [`super::Table`] is updated
    pub async fn execute(self, new_data: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        self.table.clone().merge_insert(self, new_data).await
    }
}
