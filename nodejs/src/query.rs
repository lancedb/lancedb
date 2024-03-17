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

use napi::bindgen_prelude::*;
use napi_derive::napi;
use vectordb::query::Query as LanceDBQuery;

use crate::{iterator::RecordBatchIterator, table::Table};

#[napi]
pub struct Query {
    inner: LanceDBQuery,
}

#[napi]
impl Query {
    pub fn new(table: &Table) -> Self {
        Self {
            inner: table.table.query(),
        }
    }

    #[napi]
    pub fn column(&mut self, column: String) {
        self.inner = self.inner.clone().column(&column);
    }

    #[napi]
    pub fn filter(&mut self, filter: String) {
        self.inner = self.inner.clone().filter(filter);
    }

    #[napi]
    pub fn select(&mut self, columns: Vec<String>) {
        self.inner = self.inner.clone().select(&columns);
    }

    #[napi]
    pub fn limit(&mut self, limit: u32) {
        self.inner = self.inner.clone().limit(limit as usize);
    }

    #[napi]
    pub fn prefilter(&mut self, prefilter: bool) {
        self.inner = self.inner.clone().prefilter(prefilter);
    }

    #[napi]
    pub fn nearest_to(&mut self, vector: Float32Array) {
        self.inner = self.inner.clone().nearest_to(&vector);
    }

    #[napi]
    pub fn refine_factor(&mut self, refine_factor: u32) {
        self.inner = self.inner.clone().refine_factor(refine_factor);
    }

    #[napi]
    pub fn nprobes(&mut self, nprobe: u32) {
        self.inner = self.inner.clone().nprobes(nprobe as usize);
    }

    #[napi]
    pub async fn execute_stream(&self) -> napi::Result<RecordBatchIterator> {
        let inner_stream = self.inner.execute_stream().await.map_err(|e| {
            napi::Error::from_reason(format!("Failed to execute query stream: {}", e))
        })?;
        Ok(RecordBatchIterator::new(Box::new(inner_stream)))
    }
}
