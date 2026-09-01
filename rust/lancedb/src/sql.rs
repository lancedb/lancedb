// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to SQL queries running on a remote database.

use std::sync::Arc;

use async_trait::async_trait;
use chrono::{DateTime, Utc};

use crate::{Result, arrow::SendableRecordBatchStream};

/// A point-in-time description of a submitted SQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryDescription {
    /// The stable, connection-scoped identifier assigned when the query was submitted.
    pub id: String,
    /// The lifecycle state: `running`, `finished`, `cancelling`, or `cancelled`.
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
    async fn reader(&self) -> Result<SendableRecordBatchStream>;
    async fn cancel(&self) -> Result<()>;
}

/// A handle to a submitted SQL query.
///
/// The handle can be inspected, opened as an Arrow reader, or cancelled.
/// Dropping it does not cancel the server-side query.
/// Identifier lookup is scoped to the connection that submitted the query and
/// is not a durable resume mechanism.
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
    pub(crate) fn new(handle: Arc<dyn QueryHandle>) -> Self {
        Self { handle }
    }

    /// Return the stable, connection-scoped identifier for this query.
    pub fn id(&self) -> &str {
        self.handle.id()
    }

    /// Get a point-in-time description of the query.
    pub async fn describe(&self) -> Result<QueryDescription> {
        self.handle.describe().await
    }

    /// Wait for the initial result stream and return its Arrow record batches.
    ///
    /// The stream can begin yielding partial results before query execution is
    /// complete. It continues polling for newly available result endpoints
    /// until the query finishes and all endpoints have been consumed.
    ///
    /// Results are single-consumer. Calling this method more than once on the
    /// same handle returns an error.
    pub async fn reader(&self) -> Result<SendableRecordBatchStream> {
        self.handle.reader().await
    }

    /// Request cancellation of the query.
    pub async fn cancel(&self) -> Result<()> {
        self.handle.cancel().await
    }
}
