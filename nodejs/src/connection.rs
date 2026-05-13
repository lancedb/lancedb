// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::collections::HashMap;
use std::sync::Arc;

use lancedb::database::{CreateTableMode, Database};
use napi::bindgen_prelude::*;
use napi_derive::*;

use crate::ConnectNamespaceOptions;
use crate::ConnectionOptions;
use crate::error::NapiErrorExt;
use crate::header::JsHeaderProvider;
use crate::table::Table;
use lancedb::connection::{ConnectBuilder, Connection as LanceDBConnection, connect_namespace};

use lance_namespace::models::{
    CreateNamespaceRequest, DescribeNamespaceRequest, DropNamespaceRequest, ListNamespacesRequest,
};
use lancedb::ipc::{ipc_file_to_batches, ipc_file_to_schema};

#[napi]
pub struct Connection {
    inner: Option<LanceDBConnection>,
}

#[napi(object)]
pub struct DescribeNamespaceResponse {
    pub properties: Option<HashMap<String, String>>,
}

#[napi(object)]
pub struct ListNamespacesResponse {
    pub namespaces: Vec<String>,
    pub page_token: Option<String>,
}

#[napi(object)]
pub struct CreateNamespaceResponse {
    pub properties: Option<HashMap<String, String>>,
    pub transaction_id: Option<String>,
}

#[napi(object)]
pub struct DropNamespaceResponse {
    pub properties: Option<HashMap<String, String>>,
    pub transaction_id: Option<Vec<String>>,
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
        if let Some(manifest_enabled) = options.manifest_enabled {
            builder = builder.manifest_enabled(manifest_enabled);
        }
        if let Some(namespace_client_properties) = options.namespace_client_properties {
            builder = builder.namespace_client_properties(namespace_client_properties);
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

    /// Create a new Connection instance backed by a namespace implementation.
    #[napi(factory)]
    pub async fn new_with_namespace(
        impl_name: String,
        properties: HashMap<String, String>,
        options: ConnectNamespaceOptions,
    ) -> napi::Result<Self> {
        if impl_name.is_empty() {
            return Err(napi::Error::from_reason(
                "implName must be a non-empty string",
            ));
        }

        let mut builder = connect_namespace(&impl_name, properties);
        if let Some(interval) = options.read_consistency_interval {
            builder =
                builder.read_consistency_interval(std::time::Duration::from_secs_f64(interval));
        }
        if let Some(storage_options) = options.storage_options {
            for (key, value) in storage_options {
                builder = builder.storage_option(key, value);
            }
        }
        if let Some(namespace_client_properties) = options.namespace_client_properties {
            builder = builder.namespace_client_properties(namespace_client_properties);
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
        namespace_path: Option<Vec<String>>,
        start_after: Option<String>,
        limit: Option<u32>,
    ) -> napi::Result<Vec<String>> {
        let mut op = self.get_inner()?.table_names();
        op = op.namespace(namespace_path.unwrap_or_default());
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
        namespace_path: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
    ) -> napi::Result<Table> {
        let batches = ipc_file_to_batches(buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Failed to read IPC file: {}", e)))?;
        let mode = Self::parse_create_mode_str(&mode)?;
        let mut builder = self.get_inner()?.create_table(&name, batches).mode(mode);

        builder = builder.namespace(namespace_path.unwrap_or_default());

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
        namespace_path: Option<Vec<String>>,
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

        builder = builder.namespace(namespace_path.unwrap_or_default());

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
        namespace_path: Option<Vec<String>>,
        storage_options: Option<HashMap<String, String>>,
        index_cache_size: Option<u32>,
    ) -> napi::Result<Table> {
        let mut builder = self.get_inner()?.open_table(&name);

        builder = builder.namespace(namespace_path.unwrap_or_default());

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
        target_namespace_path: Option<Vec<String>>,
        source_version: Option<i64>,
        source_tag: Option<String>,
        is_shallow: bool,
    ) -> napi::Result<Table> {
        let mut builder = self
            .get_inner()?
            .clone_table(&target_table_name, &source_uri);

        builder = builder.target_namespace(target_namespace_path.unwrap_or_default());

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
    pub async fn drop_table(
        &self,
        name: String,
        namespace_path: Option<Vec<String>>,
    ) -> napi::Result<()> {
        let ns = namespace_path.unwrap_or_default();
        self.get_inner()?
            .drop_table(&name, &ns)
            .await
            .default_error()
    }

    #[napi(catch_unwind)]
    pub async fn drop_all_tables(&self, namespace_path: Option<Vec<String>>) -> napi::Result<()> {
        let ns = namespace_path.unwrap_or_default();
        self.get_inner()?.drop_all_tables(&ns).await.default_error()
    }

    #[napi(catch_unwind)]
    /// Describe a namespace and return its properties.
    pub async fn describe_namespace(
        &self,
        namespace_path: Vec<String>,
    ) -> napi::Result<DescribeNamespaceResponse> {
        let req = DescribeNamespaceRequest {
            id: Some(namespace_path),
            ..Default::default()
        };
        let resp = self
            .get_inner()?
            .describe_namespace(req)
            .await
            .default_error()?;
        Ok(DescribeNamespaceResponse {
            properties: resp.properties,
        })
    }

    #[napi(catch_unwind)]
    /// List child namespaces under the given namespace path
    pub async fn list_namespaces(
        &self,
        namespace_path: Option<Vec<String>>,
        page_token: Option<String>,
        limit: Option<u32>,
    ) -> napi::Result<ListNamespacesResponse> {
        let req = ListNamespacesRequest {
            id: namespace_path,
            page_token,
            limit: limit.map(|l| l as i32),
            ..Default::default()
        };
        let resp = self
            .get_inner()?
            .list_namespaces(req)
            .await
            .default_error()?;
        Ok(ListNamespacesResponse {
            namespaces: resp.namespaces,
            page_token: resp.page_token,
        })
    }

    #[napi(catch_unwind)]
    /// Create a new namespace with optional properties.
    pub async fn create_namespace(
        &self,
        namespace_path: Vec<String>,
        mode: Option<String>,
        properties: Option<HashMap<String, String>>,
    ) -> napi::Result<CreateNamespaceResponse> {
        let mode_str = mode
            .map(|m| match m.to_lowercase().as_str() {
                "create" => Ok("Create".to_string()),
                "exist_ok" => Ok("ExistOk".to_string()),
                "overwrite" => Ok("Overwrite".to_string()),
                _ => Err(napi::Error::from_reason(format!(
                    "Invalid mode '{}': expected one of 'create', 'exist_ok', 'overwrite'",
                    m
                ))),
            })
            .transpose()?;
        let req = CreateNamespaceRequest {
            id: Some(namespace_path),
            mode: mode_str,
            properties,
            ..Default::default()
        };
        let resp = self
            .get_inner()?
            .create_namespace(req)
            .await
            .default_error()?;
        Ok(CreateNamespaceResponse {
            properties: resp.properties,
            transaction_id: resp.transaction_id,
        })
    }

    #[napi(catch_unwind)]
    /// Drop a namespace.
    pub async fn drop_namespace(
        &self,
        namespace_path: Vec<String>,
        mode: Option<String>,
        behavior: Option<String>,
    ) -> napi::Result<DropNamespaceResponse> {
        let mode_str = mode
            .map(|m| match m.to_lowercase().as_str() {
                "skip" => Ok("Skip".to_string()),
                "fail" => Ok("Fail".to_string()),
                _ => Err(napi::Error::from_reason(format!(
                    "Invalid mode '{}': expected one of 'skip', 'fail'",
                    m
                ))),
            })
            .transpose()?;
        let behavior_str = behavior
            .map(|b| match b.to_lowercase().as_str() {
                "restrict" => Ok("Restrict".to_string()),
                "cascade" => Ok("Cascade".to_string()),
                _ => Err(napi::Error::from_reason(format!(
                    "Invalid behavior '{}': expected one of 'restrict', 'cascade'",
                    b
                ))),
            })
            .transpose()?;
        let req = DropNamespaceRequest {
            id: Some(namespace_path),
            mode: mode_str,
            behavior: behavior_str,
            ..Default::default()
        };
        let resp = self
            .get_inner()?
            .drop_namespace(req)
            .await
            .default_error()?;
        Ok(DropNamespaceResponse {
            properties: resp.properties,
            transaction_id: resp.transaction_id,
        })
    }
}
