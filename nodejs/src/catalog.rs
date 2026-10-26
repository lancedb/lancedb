// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{StreamExt, future::poll_fn};

use lancedb::catalog::{CatalogConnection, CreateDatabaseRequest, DropDatabaseRequest};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::connection::Connection;
use crate::error::NapiErrorExt;
use crate::header::JsHeaderProvider;
use crate::remote::{ClientConfig, OAuthConfig};

#[napi(object)]
pub struct CatalogOptions {
    pub api_key: Option<String>,
    pub client_config: Option<ClientConfig>,
    /// SQL service endpoint inherited by database connections.
    pub sql_host_override: Option<String>,
    pub read_consistency_interval: Option<f64>,
    pub oauth_config: Option<OAuthConfig>,
}

/// A lazy iterator over database names with inspectable pagination state.
#[napi]
pub struct DatabaseNames {
    inner: Mutex<lancedb::catalog::DatabaseNames>,
    next_lock: futures::lock::Mutex<()>,
}

#[napi]
impl DatabaseNames {
    /// Number of names cached without another REST request.
    #[napi]
    pub fn num_page_results(&self) -> u32 {
        self.inner.lock().unwrap().num_page_results() as u32
    }

    /// Continuation token for the next REST request.
    #[napi]
    pub fn page_token(&self) -> Option<String> {
        self.inner.lock().unwrap().page_token().map(str::to_owned)
    }

    /// Fetch the next name, returning None when exhausted.
    #[napi]
    pub async fn next(&self) -> Result<Option<String>> {
        // Serialize advances but allow synchronous inspection between polls.
        let _guard = self.next_lock.lock().await;
        poll_fn(|cx| self.inner.lock().unwrap().poll_next_unpin(cx))
            .await
            .transpose()
            .default_error()
    }
}

#[napi]
pub struct Catalog {
    inner: CatalogConnection,
}

#[napi]
impl Catalog {
    #[napi(factory)]
    pub async fn new(
        endpoint: String,
        options: CatalogOptions,
        header_provider: Option<&JsHeaderProvider>,
    ) -> Result<Self> {
        let mut builder = lancedb::connect_catalog(endpoint);
        if let Some(key) = options.api_key {
            builder = builder.api_key(key);
        }
        let mut config: lancedb::remote::ClientConfig =
            options.client_config.unwrap_or_default().into();
        if let Some(provider) = header_provider {
            config.header_provider = Some(Arc::new(provider.clone()));
        }
        builder = builder.client_config(config);
        if let Some(endpoint) = options.sql_host_override {
            builder = builder.sql_host_override(endpoint);
        }
        if let Some(interval) = options.read_consistency_interval {
            let interval = Duration::try_from_secs_f64(interval).map_err(|err| {
                Error::from_reason(format!("Invalid read consistency interval: {err}"))
            })?;
            builder = builder.read_consistency_interval(interval);
        }
        if let Some(oauth) = options.oauth_config {
            builder = builder.oauth_config(oauth.try_into().default_error()?);
        }
        Ok(Self {
            inner: builder.execute().await.default_error()?,
        })
    }

    #[napi(getter)]
    pub fn uri(&self) -> String {
        self.inner.uri().to_string()
    }

    #[napi]
    pub async fn create_database(
        &self,
        name: String,
        exist_ok: Option<bool>,
    ) -> Result<Connection> {
        self.inner
            .create_database(CreateDatabaseRequest::new(name).exist_ok(exist_ok.unwrap_or(false)))
            .await
            .map(Connection::inner_new)
            .default_error()
    }
    #[napi]
    pub async fn connect_database(&self, name: String) -> Result<Connection> {
        self.inner
            .connect_database(name)
            .await
            .map(Connection::inner_new)
            .default_error()
    }
    #[napi]
    pub async fn drop_database(&self, name: String, ignore_missing: Option<bool>) -> Result<()> {
        self.inner
            .drop_database(
                DropDatabaseRequest::new(name).ignore_missing(ignore_missing.unwrap_or(false)),
            )
            .await
            .default_error()
    }
    #[napi]
    pub fn list_databases(
        &self,
        page_token: Option<String>,
        page_limit: Option<u32>,
    ) -> DatabaseNames {
        DatabaseNames {
            inner: Mutex::new(self.inner.list_databases(page_token, page_limit)),
            next_lock: futures::lock::Mutex::new(()),
        }
    }
}
