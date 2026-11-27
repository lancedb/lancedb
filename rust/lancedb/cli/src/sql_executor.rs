// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use crate::dml_result::DmlResult;
use anyhow::Result;
use arrow_array::RecordBatch;
use async_trait::async_trait;

#[async_trait]
pub trait SqlQueryExecutor: Send + Sync {
    /// Execute a SQL query and return a structured result.
    async fn execute_query(&mut self, query: &str) -> Result<SqlResult>;

    /// Check if the executor is connected and ready
    async fn is_connected(&self) -> bool;

    /// Get the connection info for display purposes
    fn connection_info(&self) -> Option<String>;

    /// Connect to the server. `server_url` may carry an `http://` or `https://`
    /// scheme; a bare `host:port` is treated as plaintext.
    async fn connect(&mut self, server_url: &str) -> Result<()>;

    /// Set the current database
    fn set_database(&mut self, database: String);
}

/// Result of a SQL execution
#[derive(Debug)]
pub enum SqlResult {
    /// Query returned data
    Query(Vec<RecordBatch>),
    /// DML result (INSERT, DELETE, UPDATE)
    Dml(DmlResult),
    /// DDL statement (CREATE, DROP, etc.) completed
    Success,
}

impl From<Vec<RecordBatch>> for SqlResult {
    fn from(batches: Vec<RecordBatch>) -> Self {
        match batches.first().and_then(DmlResult::try_from_batch) {
            Some(dml) => Self::Dml(dml),
            None => Self::Query(batches),
        }
    }
}

/// SQL execution through the public SDK's asynchronous query lifecycle.
pub struct SdkSqlExecutor {
    connection: Option<lancedb::Connection>,
    server_url: String,
    database: String,
    authentication: crate::server_config::Authentication,
    tls: crate::server_config::TlsOptions,
}

impl SdkSqlExecutor {
    pub fn new_with_authentication(
        server_url: String,
        authentication: crate::server_config::Authentication,
    ) -> Self {
        Self {
            connection: None,
            server_url,
            database: "test".into(),
            authentication,
            tls: Default::default(),
        }
    }

    pub fn new_with_api_key(server_url: String, api_key: String) -> Self {
        Self::new_with_authentication(
            server_url,
            crate::server_config::Authentication::ApiKey(api_key),
        )
    }

    pub fn with_tls(mut self, tls: crate::server_config::TlsOptions) -> Self {
        self.tls = tls;
        self
    }
}

#[async_trait]
impl SqlQueryExecutor for SdkSqlExecutor {
    async fn execute_query(&mut self, query: &str) -> Result<SqlResult> {
        use futures::TryStreamExt;
        let connection = self
            .connection
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Flight SQL client not connected"))?;
        let mut builder = connection.execute_query_async(query);
        if let Ok(namespace) = std::env::var("LANCEDB_NAMESPACE_PATH") {
            builder = builder.default_namespace_path(namespace.split('$'));
        }
        let query = builder.execute().await?;
        let batches: Vec<RecordBatch> = query.reader().await?.try_collect().await?;
        Ok(batches.into())
    }

    async fn is_connected(&self) -> bool {
        self.connection.is_some()
    }

    fn connection_info(&self) -> Option<String> {
        self.connection
            .as_ref()
            .map(|_| format!("Connected to {}", self.server_url))
    }

    async fn connect(&mut self, server_url: &str) -> Result<()> {
        use crate::server_config::{parse_grpc_endpoint, Authentication, DEFAULT_SQL_PORT};
        self.connection = None;
        self.server_url = server_url.to_owned();
        let endpoint = parse_grpc_endpoint(server_url, DEFAULT_SQL_PORT)?.url();
        let mut builder = lancedb::connect(&format!("db://{}", self.database))
            .region("self-hosted")
            .sql_host_override(&endpoint);
        builder = match &self.authentication {
            Authentication::ApiKey(key) => builder.api_key(key),
            _ => builder.api_key(""),
        };
        let mut config =
            crate::lancedb_client_config(&self.tls, &self.authentication).unwrap_or_default();
        // Preserve the CLI's bounded startup probe, including the TLS handshake.
        config.timeout_config.connect_timeout = Some(std::time::Duration::from_secs(5));
        builder = builder.client_config(config);
        let connection = builder.execute().await?;
        connection.check_sql_connection().await?;
        self.connection = Some(connection);
        Ok(())
    }

    fn set_database(&mut self, database: String) {
        if self.database != database {
            self.connection = None;
        }
        self.database = database;
    }
}
