// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! SQL: handles to queries running on a remote database, and the seam an
//! embedder extends the dialect through.
//!
//! The extension seam is behind the default-on `sql` feature. It lets a host
//! add statements to the dialect from outside this crate: a statement brings
//! its own grammar ([`CustomSqlHandler`]), and declares its audit label and
//! the access it needs ([`SqlStatement`]), so the host's authorization and
//! auditing do not have to know each statement by name.

#[cfg(feature = "sql")]
mod dml;
#[cfg(feature = "sql")]
mod observer;
#[cfg(feature = "sql")]
mod parser;
#[cfg(feature = "sql")]
mod statement;

#[cfg(feature = "sql")]
pub use dml::{DmlOperation, DmlResult, dml_result_schema};
#[cfg(feature = "sql")]
pub use observer::{CommittedWrite, DmlEventKind, WriteObserver, observe_write};
#[cfg(feature = "sql")]
pub use parser::route_custom_sql;
#[cfg(feature = "sql")]
pub use statement::{
    AccessRequirement, CreateKind, CustomSqlHandler, DatabaseScope, RelationKind,
    RequirementContext, SqlStatement, StatementRegistry, SystemScope, WriteMode,
};

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
    pub fn id(&self) -> Uuid {
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
