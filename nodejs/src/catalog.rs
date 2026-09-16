// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;
use std::time::Duration;

use lancedb::catalog::{
    CatalogConnection, CreateDatabaseRequest, DropDatabaseRequest, ListDatabasesRequest,
};
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
    pub read_consistency_interval: Option<f64>,
    pub oauth_config: Option<OAuthConfig>,
}

#[napi(object)]
pub struct ListDatabasesResponse {
    pub databases: Vec<String>,
    pub page_token: Option<String>,
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
    pub async fn list_databases(
        &self,
        limit: Option<u32>,
        page_token: Option<String>,
    ) -> Result<ListDatabasesResponse> {
        let mut request = ListDatabasesRequest::default();
        request.limit = limit;
        request.page_token = page_token;
        let response = self.inner.list_databases(request).await.default_error()?;
        Ok(ListDatabasesResponse {
            databases: response.databases,
            page_token: response.page_token,
        })
    }
}
