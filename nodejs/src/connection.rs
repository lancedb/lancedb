// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;

use lancedb::database::CreateTableMode;
use napi::bindgen_prelude::*;
use napi_derive::*;

use crate::error::NapiErrorExt;
use crate::table::Table;
use crate::ConnectionOptions;
use lancedb::connection::{ConnectBuilder, Connection as LanceDBConnection};
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
        if let Some(interval) = options.read_consistency_interval {
            match interval {
                Either::A(seconds) => {
                    builder = builder.read_consistency_interval(Some(
                        std::time::Duration::from_secs_f64(seconds),
                    ));
                }
                Either::B(_) => {
                    builder = builder.read_consistency_interval(None);
                }
            }
        }
        if let Some(storage_options) = options.storage_options {
            for (key, value) in storage_options {
                builder = builder.storage_option(key, value);
            }
        }

        let client_config = options.client_config.unwrap_or_default();
        builder = builder.client_config(client_config.into());

        if let Some(api_key) = options.api_key {
            builder = builder.api_key(&api_key);
        }

        if let Some(region) = options.region {
            builder = builder.region(&region);
        } else {
            builder = builder.region("us-east-1");
        }

        if let Some(host_override) = options.host_override {
            builder = builder.host_override(&host_override);
        }

        Ok(Self::inner_new(builder.execute().await.default_error()?))
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
    #[napi(catch_unwind)]
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
        op.execute().await.default_error()
    }

    /// Create table from a Apache Arrow IPC (file) buffer.
    ///
    /// Parameters:
    /// - name: The name of the table.
    /// - buf: The buffer containing the IPC file.
    ///
    #[napi(catch_unwind)]
    pub async fn create_table(
        &self,
        name: String,
        buf: Buffer,
        mode: String,
        storage_options: Option<HashMap<String, String>>,
    ) -> napi::Result<Table> {
        let batches = ipc_file_to_batches(buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC file: {}", e)))?;
        let mode = Self::parse_create_mode_str(&mode)?;
        let mut builder = self.get_inner()?.create_table(&name, batches).mode(mode);

        if let Some(storage_options) = storage_options {
            for (key, value) in storage_options {
                builder = builder.storage_option(key, value);
            }
        }
        let tbl = builder.execute().await.default_error()?;
        Ok(Table::new(tbl))
    }

    #[napi(catch_unwind)]
    pub async fn create_empty_table(
        &self,
        name: String,
        schema_buf: Buffer,
        mode: String,
        storage_options: Option<HashMap<String, String>>,
    ) -> napi::Result<Table> {
        let schema = ipc_file_to_schema(schema_buf.to_vec()).map_err(|e| {
            napi::Error::from_reason(format!("Failed to marshal schema from JS to Rust: {}", e))
        })?;
        let mode = Self::parse_create_mode_str(&mode)?;
        let mut builder = self
            .get_inner()?
            .create_empty_table(&name, schema)
            .mode(mode);
        if let Some(storage_options) = storage_options {
            for (key, value) in storage_options {
                builder = builder.storage_option(key, value);
            }
        }
        let tbl = builder.execute().await.default_error()?;
        Ok(Table::new(tbl))
    }

    #[napi(catch_unwind)]
    pub async fn open_table(
        &self,
        name: String,
        storage_options: Option<HashMap<String, String>>,
        index_cache_size: Option<u32>,
    ) -> napi::Result<Table> {
        let mut builder = self.get_inner()?.open_table(&name);
        if let Some(storage_options) = storage_options {
            for (key, value) in storage_options {
                builder = builder.storage_option(key, value);
            }
        }
        if let Some(index_cache_size) = index_cache_size {
            builder = builder.index_cache_size(index_cache_size);
        }
        let tbl = builder.execute().await.default_error()?;
        Ok(Table::new(tbl))
    }

    /// Drop table with the name. Or raise an error if the table does not exist.
    #[napi(catch_unwind)]
    pub async fn drop_table(&self, name: String) -> napi::Result<()> {
        self.get_inner()?.drop_table(&name).await.default_error()
    }

    #[napi(catch_unwind)]
    pub async fn drop_all_tables(&self) -> napi::Result<()> {
        self.get_inner()?.drop_all_tables().await.default_error()
    }
}
