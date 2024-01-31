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

use arrow_array::RecordBatchReader;
use lance::dataset;

use crate::TableRef;
use crate::error::{Error, Result};

pub struct MergeInsertBuilder {
    table: TableRef,
    when_matched_update_all: bool,
    when_not_matched_insert_all: bool,
    when_not_matched_by_source_delete: bool,
    when_not_matched_by_source_condition: bool,
}

impl MergeInsertBuilder {
    pub(crate) fn new(table: TableRef) -> Self {
        Self {
            table,
            when_matched_update_all: false,
            when_not_matched_insert_all: false,
            when_not_matched_by_source_delete: false,
            when_not_matched_by_source_condition: false,
        }
    }

    pub fn when_matched_update_all(mut self) -> Self {
        self.when_matched_update_all = true;
        self
    }

    pub fn when_not_matched_insert_all(mut self) -> Self {
        self.when_not_matched_insert_all = true;
        self
    }

    pub fn when_not_matched_by_source_delete(mut self) -> Self {
        self.when_not_matched_by_source_delete = true;
        self
    }

    pub fn when_not_matched_by_source_condition(mut self) -> Self {
        self.when_not_matched_by_source_condition = true;
        self
    }

    pub async fn execute(batches: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Ok(())
    }
}