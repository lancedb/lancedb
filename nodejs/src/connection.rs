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
use napi_derive::*;

use crate::table::Table;
use crate::ConnectionOptions;
use lancedb::connection::{ConnectBuilder, Connection as LanceDBConnection, CreateTableMode};
use lancedb::ipc::{ipc_file_to_batches, ipc_file_to_schema};

#[napi]
pub struct Connection {
    inner: Option<LanceDBConnection>,
}

impl Connection {
    pub(crate) fn inner_new(inner: LanceDBConnection) -> Self {
        Self { inner: Some(inner) }
    }

    fn get_inner(&self) -> napi::Result<&LanceDBConnection> {
        self.inner
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("Connection is closed"))
    }
}

impl Connection {
    fn parse_create_mode_str(mode: &str) -> napi::Result<CreateTableMode> {
        match mode {
            "create" => Ok(CreateTableMode::Create),
            "overwrite" => Ok(CreateTableMode::Overwrite),
            "exist_ok" => Ok(CreateTableMode::exist_ok(|builder| builder)),
            _ => Err(napi::Error::from_reason(format!("Invalid mode {}", mode))),
        }
    }
}

#[napi]
impl Connection {
    /// Create a new Connection instance from the given URI.
    #[napi(factory)]
    pub async fn new(uri: String, options: ConnectionOptions) -> napi::Result<Self> {
        let mut builder = ConnectBuilder::new(&uri);
        if let Some(api_key) = options.api_key {
            builder = builder.api_key(&api_key);
        }
        if let Some(host_override) = options.host_override {
            builder = builder.host_override(&host_override);
        }
        if let Some(interval) = options.read_consistency_interval {
            builder =
                builder.read_consistency_interval(std::time::Duration::from_secs_f64(interval));
        }
        Ok(Self::inner_new(
            builder
                .execute()
                .await
                .map_err(|e| napi::Error::from_reason(format!("{}", e)))?,
        ))
    }

    #[napi]
    pub fn display(&self) -> napi::Result<String> {
        Ok(self.get_inner()?.to_string())
    }

    #[napi]
    pub fn is_open(&self) -> bool {
        self.inner.is_some()
    }

    #[napi]
    pub fn close(&mut self) {
        self.inner.take();
    }

    /// List all tables in the dataset.
    #[napi]
    pub async fn table_names(
        &self,
        start_after: Option<String>,
        limit: Option<u32>,
    ) -> napi::Result<Vec<String>> {
        let mut op = self.get_inner()?.table_names();
        if let Some(start_after) = start_after {
            op = op.start_after(start_after);
        }
        if let Some(limit) = limit {
            op = op.limit(limit);
        }
        op.execute()
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
    pub async fn create_table(
        &self,
        name: String,
        buf: Buffer,
        mode: String,
    ) -> napi::Result<Table> {
        let batches = ipc_file_to_batches(buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC file: {}", e)))?;
        let mode = Self::parse_create_mode_str(&mode)?;
        let tbl = self
            .get_inner()?
            .create_table(&name, Box::new(batches))
            .mode(mode)
            .execute()
            .await
            .map_err(|e| napi::Error::from_reason(format!("{}", e)))?;
        Ok(Table::new(tbl))
    }

    #[napi]
    pub async fn create_empty_table(
        &self,
        name: String,
        schema_buf: Buffer,
        mode: String,
    ) -> napi::Result<Table> {
        let schema = ipc_file_to_schema(schema_buf.to_vec()).map_err(|e| {
            napi::Error::from_reason(format!("Failed to marshal schema from JS to Rust: {}", e))
        })?;
        let mode = Self::parse_create_mode_str(&mode)?;
        let tbl = self
            .get_inner()?
            .create_empty_table(&name, schema)
            .mode(mode)
            .execute()
            .await
            .map_err(|e| napi::Error::from_reason(format!("{}", e)))?;
        Ok(Table::new(tbl))
    }

    #[napi]
    pub async fn open_table(&self, name: String) -> napi::Result<Table> {
        let tbl = self
            .get_inner()?
            .open_table(&name)
            .execute()
            .await
            .map_err(|e| napi::Error::from_reason(format!("{}", e)))?;
        Ok(Table::new(tbl))
    }

    /// Drop table with the name. Or raise an error if the table does not exist.
    #[napi]
    pub async fn drop_table(&self, name: String) -> napi::Result<()> {
        self.get_inner()?
            .drop_table(&name)
            .await
            .map_err(|e| napi::Error::from_reason(format!("{}", e)))
    }
}
