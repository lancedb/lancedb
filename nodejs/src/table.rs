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

use arrow_ipc::writer::FileWriter;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use vectordb::{ipc::ipc_file_to_batches, table::TableRef};

use crate::index::IndexBuilder;
use crate::query::Query;

#[napi]
pub struct Table {
    pub(crate) table: TableRef,
}

#[napi]
impl Table {
    pub(crate) fn new(table: TableRef) -> Self {
        Self { table }
    }

    /// Return Schema as empty Arrow IPC file.
    #[napi]
    pub fn schema(&self) -> napi::Result<Buffer> {
        let mut writer = FileWriter::try_new(vec![], &self.table.schema())
            .map_err(|e| napi::Error::from_reason(format!("Failed to create IPC file: {}", e)))?;
        writer
            .finish()
            .map_err(|e| napi::Error::from_reason(format!("Failed to finish IPC file: {}", e)))?;
        Ok(Buffer::from(writer.into_inner().map_err(|e| {
            napi::Error::from_reason(format!("Failed to get IPC file: {}", e))
        })?))
    }

    #[napi]
    pub async fn add(&self, buf: Buffer) -> napi::Result<()> {
        let batches = ipc_file_to_batches(buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC file: {}", e)))?;
        self.table.add(Box::new(batches), None).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to add batches to table {}: {}",
                self.table, e
            ))
        })
    }

    #[napi]
    pub async fn count_rows(&self, filter: Option<String>) -> napi::Result<usize> {
        self.table.count_rows(filter).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to count rows in table {}: {}",
                self.table, e
            ))
        })
    }

    #[napi]
    pub async fn delete(&self, predicate: String) -> napi::Result<()> {
        self.table.delete(&predicate).await.map_err(|e| {
            napi::Error::from_reason(format!(
                "Failed to delete rows in table {}: predicate={}",
                self.table, e
            ))
        })
    }

    #[napi]
    pub fn create_index(&self) -> IndexBuilder {
        IndexBuilder::new(self.table.as_ref())
    }

    #[napi]
    pub fn query(&self) -> Query {
        Query::new(self)
    }
}
