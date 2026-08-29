// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to SQL queries running on a remote database.

use std::sync::Arc;

use arrow_array::RecordBatch;
use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::Result;

/// A point-in-time description of a submitted SQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryDescription {
    /// The stable, opaque identifier returned when the query was submitted.
    pub id: String,
    /// The lifecycle state: `running` or `finished`.
    pub status: String,
    /// Server-reported completion progress, when known.
    pub progress: Option<f64>,
    /// When the server may stop accepting this query's continuation token.
    pub expires_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub(crate) trait QueryHandle: Send + Sync {
    fn id(&self) -> &str;
    async fn describe(&self) -> Result<QueryDescription>;
    async fn result(&self) -> Result<Vec<RecordBatch>>;
    async fn cancel(&self) -> Result<()>;
}

/// A handle to a submitted SQL query.
///
/// The handle can be inspected, awaited for its Arrow result, or cancelled.
/// Dropping it does not cancel the server-side query.
pub struct Query {
    handle: Arc<dyn QueryHandle>,
}

impl std::fmt::Debug for Query {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("Query")
            .field("id", &self.id())
            .finish()
    }
}

impl Query {
    #[cfg(feature = "remote")]
    pub(crate) fn new(handle: impl QueryHandle + 'static) -> Self {
        Self {
            handle: Arc::new(handle),
        }
    }

    /// Return the stable, opaque identifier for this query.
    pub fn id(&self) -> &str {
        self.handle.id()
    }

    /// Get a point-in-time description of the query.
    pub async fn describe(&self) -> Result<QueryDescription> {
        self.handle.describe().await
    }

    /// Wait for the query to finish and collect its Arrow record batches.
    ///
    /// A successfully downloaded result is cached on this handle.
    pub async fn result(&self) -> Result<Vec<RecordBatch>> {
        self.handle.result().await
    }

    /// Request cancellation of the query.
    pub async fn cancel(&self) -> Result<()> {
        self.handle.cancel().await
    }
}
