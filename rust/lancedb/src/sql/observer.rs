// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Notification of committed writes.
//!
//! A statement that writes rows often has to tell its host that it did, so
//! that follow-up work can be scheduled. What it should *not* have to know is
//! how the host represents that notification. [`WriteObserver`] is the seam:
//! the statement reports what it wrote, and the host decides what that means
//! -- an event on a bus, a metric, or nothing at all.

use std::sync::Arc;

use async_trait::async_trait;

/// Which DML operation committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlEventKind {
    Insert,
    Update,
    Delete,
}

/// A write that has already been made durable.
#[derive(Debug, Clone)]
pub struct CommittedWrite {
    /// The database holding the table.
    pub database: String,
    /// The schema the statement named the table through.
    pub schema: String,
    /// The table written.
    pub table: String,
    /// The table's storage location, when the statement resolved one.
    pub table_uri: Option<String>,
    /// Which operation committed.
    pub kind: DmlEventKind,
}

/// Notified after a statement's write commits.
///
/// Implementations are best-effort by contract: the write is already durable
/// when this is called, so an observer that fails must not fail the statement.
/// That is why the method cannot report an error.
#[async_trait]
pub trait WriteObserver: Send + Sync {
    async fn write_committed(&self, write: CommittedWrite);
}

/// Report a committed write, if anything is observing.
pub async fn observe_write(
    observer: Option<&Arc<dyn WriteObserver>>,
    database: &str,
    schema: &str,
    table: &str,
    table_uri: Option<String>,
    kind: DmlEventKind,
) {
    let Some(observer) = observer else {
        return;
    };
    observer
        .write_committed(CommittedWrite {
            database: database.to_string(),
            schema: schema.to_string(),
            table: table.to_string(),
            table_uri,
            kind,
        })
        .await;
}
