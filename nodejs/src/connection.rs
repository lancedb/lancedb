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

use napi::bindgen_prelude::*;
use napi_derive::*;

use crate::table::Table;
use vectordb::connection::{Connection as LanceDBConnection, Database};
use vectordb::ipc::ipc_file_to_batches;

#[napi]
pub struct Connection {
    conn: Arc<dyn LanceDBConnection>,
}

#[napi]
impl Connection {
    /// Create a new Connection instance from the given URI.
    #[napi(factory)]
    pub async fn new(uri: String) -> napi::Result<Self> {
        Ok(Self {
            conn: Arc::new(Database::connect(&uri).await.map_err(|e| {
                napi::Error::from_reason(format!("Failed to connect to database: {}", e))
            })?),
        })
    }

    /// List all tables in the dataset.
    #[napi]
    pub async fn table_names(&self) -> napi::Result<Vec<String>> {
        self.conn
            .table_names()
            .await
            .map_err(|e| napi::Error::from_reason(format!("{}", e)))
    }

    /// Create table from a Apache Arrow IPC (file) buffer.
    ///
    /// Parameters:
    /// - name: The name of the table.
    /// - buf: The buffer containing the IPC file.
    ///
    #[napi]
    pub async fn create_table(&self, name: String, buf: Buffer) -> napi::Result<Table> {
        let batches = ipc_file_to_batches(buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC file: {}", e)))?;
        let tbl = self
            .conn
            .create_table(&name, Box::new(batches), None)
            .await
            .map_err(|e| napi::Error::from_reason(format!("{}", e)))?;
        Ok(Table::new(tbl))
    }

    #[napi]
    pub async fn open_table(&self, name: String) -> napi::Result<Table> {
        let tbl = self
            .conn
            .open_table(&name)
            .await
            .map_err(|e| napi::Error::from_reason(format!("{}", e)))?;
        Ok(Table::new(tbl))
    }

    /// Drop table with the name. Or raise an error if the table does not exist.
    #[napi]
    pub async fn drop_table(&self, name: String) -> napi::Result<()> {
        self.conn
            .drop_table(&name)
            .await
            .map_err(|e| napi::Error::from_reason(format!("{}", e)))
    }
}
