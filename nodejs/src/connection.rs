// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::sync::Arc;

use lancedb::database::{CreateTableMode, Database};
use napi::bindgen_prelude::*;
use napi_derive::*;

use crate::error::NapiErrorExt;
use crate::header::JsHeaderProvider;
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

    pub fn database(&self) -> napi::Result<Arc<dyn Database>> {
        Ok(self.get_inner()?.database().clone())
    }
}

#[napi]
impl Connection {
    /// Create a new Connection instance from the given URI.
    #[napi(factory)]
    pub async fn new(
        uri: String,
        options: ConnectionOptions,
        header_provider: Option<&JsHeaderProvider>,
    ) -> napi::Result<Self> {
        let mut builder = ConnectBuilder::new(&uri);
        if let Some(interval) = options.read_consistency_interval {
            builder =
                builder.read_consistency_interval(std::time::Duration::from_secs_f64(interval));
        }
        if let Some(storage_options) = options.storage_options {
            for (key, value) in storage_options {
                builder = builder.storage_option(key, value);
            }
        }

        // Create client config, optionally with header provider
        let client_config = options.client_config.unwrap_or_default();
        let mut rust_config: lancedb::remote::ClientConfig = client_config.into();

        if let Some(provider) = header_provider {
            rust_config.header_provider =
                Some(Arc::new(provider.clone()) as Arc<dyn lancedb::remote::HeaderProvider>);
        }

        builder = builder.client_config(rust_config);

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

        if let Some(session) = options.session {
            builder = builder.session(session.inner.clone());
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
        namespace: Vec<String>,
        start_after: Option<String>,
        limit: Option<u32>,
    ) -> napi::Result<Vec<String>> {
        let mut op = self.get_inner()?.table_names();
        op = op.namespace(namespace);
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
        namespace: Vec<String>,
        storage_options: Option<HashMap<String, String>>,
    ) -> napi::Result<Table> {
        let batches = ipc_file_to_batches(buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC file: {}", e)))?;
        let mode = Self::parse_create_mode_str(&mode)?;
        let mut builder = self.get_inner()?.create_table(&name, batches).mode(mode);

        builder = builder.namespace(namespace);

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
        namespace: Vec<String>,
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

        builder = builder.namespace(namespace);

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
        namespace: Vec<String>,
        storage_options: Option<HashMap<String, String>>,
        index_cache_size: Option<u32>,
    ) -> napi::Result<Table> {
        let mut builder = self.get_inner()?.open_table(&name);

        builder = builder.namespace(namespace);

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

    #[napi(catch_unwind)]
    pub async fn clone_table(
        &self,
        target_table_name: String,
        source_uri: String,
        target_namespace: Vec<String>,
        source_version: Option<i64>,
        source_tag: Option<String>,
        is_shallow: bool,
    ) -> napi::Result<Table> {
        let mut builder = self
            .get_inner()?
            .clone_table(&target_table_name, &source_uri);

        builder = builder.target_namespace(target_namespace);

        if let Some(version) = source_version {
            builder = builder.source_version(version as u64);
        }

        if let Some(tag) = source_tag {
            builder = builder.source_tag(tag);
        }

        builder = builder.is_shallow(is_shallow);

        let tbl = builder.execute().await.default_error()?;
        Ok(Table::new(tbl))
    }

    /// Drop table with the name. Or raise an error if the table does not exist.
    #[napi(catch_unwind)]
    pub async fn drop_table(&self, name: String, namespace: Vec<String>) -> napi::Result<()> {
        self.get_inner()?
            .drop_table(&name, &namespace)
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn drop_all_tables(&self, namespace: Vec<String>) -> napi::Result<()> {
        self.get_inner()?
            .drop_all_tables(&namespace)
            .await
            .default_error()
    }
}
