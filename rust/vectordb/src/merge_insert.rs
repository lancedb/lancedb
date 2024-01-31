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

use std::pin::Pin;
use std::sync::Arc;
use arrow_array::RecordBatchReader;
use lance::dataset::{self, WhenMatched, WhenNotMatched, WhenNotMatchedBySource};
use lance_datafusion::utils::reader_to_stream;

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

    pub async fn execute(
        mut self,
        batches: Box<dyn RecordBatchReader + Send>,
    ) -> Result<()> {
        let native_table = self.table.as_native().unwrap(); // TODO no unwrap
        let ds = native_table.clone_inner_dataset();
        let mut builder = dataset::MergeInsertBuilder::try_new(
                Arc::new(ds),
                vec!["vectors".to_string()],
            )
            .unwrap(); // TODO no unwrap
        
        if self.when_matched_update_all {
            builder.when_matched(WhenMatched::UpdateAll);
        }

        if self.when_not_matched_insert_all {
            builder.when_not_matched(WhenNotMatched::InsertAll);
        }

        if self.when_not_matched_by_source_delete {
            builder.when_not_matched_by_source(WhenNotMatchedBySource::Delete);
        }

        // TODO
        // if self.when_not_matched_by_source_condition {
        //     builder.when_not_matched_by_source(WhenNotMatchedBySource::DeleteIf(()));
        // }

        let job = builder.try_build().unwrap(); // TODO no unwrap
        let batches = reader_to_stream(batches).await.unwrap().0; // TODO no unwrap
        let ds2 = job.execute(batches).await.unwrap(); // TODO no unwrap

        native_table.reset_dataset(ds2.as_ref().clone());


        Ok(())
    }
}