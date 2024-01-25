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

use crate::table::Table;

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
    pub fn nearest_to(&mut self, vector: Float32Array) {
        let inn = self.inner.clone().nearest_to(&vector);
        self.inner = inn;
    }

    #[napi]
    pub fn execute_stream(&self) -> napi::Result<()> {
        todo!()
    }
}
