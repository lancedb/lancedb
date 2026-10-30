// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Handles to SQL queries running on a LanceDB Distributed database.

use std::{fmt, sync::Arc};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{Result, arrow::SendableRecordBatchStream};

/// The externally visible lifecycle state of a submitted SQL query.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueryStatus {
    /// The server is still executing the query.
    Running,
    /// The server has made the complete result available.
    ///
    /// Directly streamed queries report `Finished` when their stream reaches
    /// its end. Deferred queries report the server's execution state. Open the
    /// reader directly instead of waiting for this status before reading.
    Finished,
    /// The server accepted cancellation but has not confirmed it yet.
    Cancelling,
    /// The server confirmed cancellation.
    Cancelled,
}

impl fmt::Display for QueryStatus {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Running => "running",
            Self::Finished => "finished",
            Self::Cancelling => "cancelling",
            Self::Cancelled => "cancelled",
        })
    }
}

/// A point-in-time description of a submitted SQL query.
#[derive(Clone, Debug, PartialEq)]
pub struct QueryDescription {
    /// The stable, connection-scoped identifier assigned when the query was submitted.
    pub id: Uuid,
    /// The server-visible lifecycle state.
    pub status: QueryStatus,
    /// Server-reported completion progress, when known. Values are in `[0.0, 1.0]`,
    /// with `1.0` meaning complete.
    pub progress: Option<f64>,
    /// When the server may stop accepting this query's continuation token.
    pub expires_at: Option<DateTime<Utc>>,
}

#[async_trait]
pub(crate) trait QueryHandle: Send + Sync {
    fn id(&self) -> Uuid;
    async fn describe(&self) -> Result<QueryDescription>;
    async fn reader(&self) -> Result<SendableRecordBatchStream>;
    async fn cancel(&self) -> Result<()>;
}

/// A handle to a submitted SQL query.
///
/// The handle can be inspected, opened as an Arrow reader, or cancelled.
/// The server may stream results on the initial call or defer execution and
/// return result tickets. Dropping an unread handle or its reader stops a
/// directly streamed query; deferred execution continues until explicitly
/// cancelled. This applies equally with and without parameters.
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
    pub fn id(&self) -> Uuid {
        self.handle.id()
    }

    /// Get a point-in-time description of the query.
    ///
    /// Directly streamed queries stay [`QueryStatus::Running`] until the result
    /// stream ends. Deferred queries expose the server's execution state. Open
    /// [`Self::reader`] directly instead of waiting for [`QueryStatus::Finished`].
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

#[cfg(test)]
mod tests {
    use super::QueryStatus;

    #[test]
    fn query_status_display_is_stable() {
        assert_eq!(QueryStatus::Running.to_string(), "running");
        assert_eq!(QueryStatus::Finished.to_string(), "finished");
        assert_eq!(QueryStatus::Cancelling.to_string(), "cancelling");
        assert_eq!(QueryStatus::Cancelled.to_string(), "cancelled");
    }
}
