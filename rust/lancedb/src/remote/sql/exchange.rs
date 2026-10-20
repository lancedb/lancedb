// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! SQL statements sent with parameters.
//!
//! A statement without parameters is polled: `PollFlightInfo` starts it
//! detached from the connection and `DoGet` reads its endpoints. Neither call
//! carries Arrow data, so a statement's values would have to be spelled into
//! its text. A statement with parameters is sent on one `DoExchange` instead:
//! the command and the parameter schema, then one row, and the result rows
//! come back on the same call. The call is the statement's lifetime, so
//! cancelling it -- or dropping the handle before reading -- stops the
//! statement on the server.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex as StdMutex, OnceLock};
use std::time::Instant;

use arrow_array::RecordBatch;
use arrow_flight::FlightDescriptor;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt};
use arrow_schema::SchemaRef;
use prost::Message;
use tokio::sync::{Notify, mpsc};
use uuid::Uuid;

use super::{
    ABANDONED_QUERY_RETENTION, DEFAULT_READ_TIMEOUT, ResultEndpointStream, ResultStartGuard,
    SqlClientInner, TERMINAL_QUERY_RETENTION, resolve_timeout, sql_error, with_overall_timeout,
};
use crate::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
use crate::error::{Error, Result};
use crate::sql::{QueryDescription, QueryHandle, QueryStatus};

#[derive(Clone, Debug, PartialEq, Eq)]
enum Lifecycle {
    Running,
    /// Every row has been received.
    Finished,
    Cancelled,
    Failed(String),
}

/// A failed statement, keeping the message `describe` reports it with -- not
/// `Runtime error: …` again when `describe` wraps it in a runtime error.
fn failure(error: &Error) -> Lifecycle {
    Lifecycle::Failed(match error {
        Error::Runtime { message } => message.clone(),
        other => other.to_string(),
    })
}

/// A statement running on its own `DoExchange` call.
pub(super) struct ExchangeQuery {
    id: Uuid,
    client: Arc<SqlClientInner>,
    /// The call's rows, until a reader takes them or a cancellation drops them.
    response: StdMutex<Option<ResultEndpointStream>>,
    lifecycle: StdMutex<Lifecycle>,
    cancelled: Notify,
    terminal_at: OnceLock<Instant>,
    last_accessed: StdMutex<Instant>,
}

struct PreparedExchange {
    schema: SchemaRef,
    first: Option<RecordBatch>,
    rest: Option<ResultEndpointStream>,
}

/// The call's rows while a reader waits for their first batch.
///
/// A reader whose future is dropped mid-wait -- its caller gave up, or its
/// task was cancelled -- puts them back, so the next reader finds the query as
/// this one did. A query cancelled meanwhile has its rows dropped instead,
/// which ends the call.
struct Borrowed<'a> {
    query: &'a ExchangeQuery,
    rows: Option<ResultEndpointStream>,
}

impl Borrowed<'_> {
    fn get(&mut self) -> &mut ResultEndpointStream {
        self.rows.as_mut().expect("rows are borrowed until kept")
    }

    fn keep(mut self) -> ResultEndpointStream {
        self.rows.take().expect("rows are borrowed until kept")
    }
}

impl Drop for Borrowed<'_> {
    fn drop(&mut self) {
        let Some(rows) = self.rows.take() else {
            return;
        };
        // Checked under the response lock: `cancel` settles first and takes
        // the response after, so rows put back here are never missed by it.
        let mut response = self.query.response.lock().unwrap();
        if *self.query.lifecycle.lock().unwrap() == Lifecycle::Running {
            *response = Some(rows);
        }
    }
}

impl ExchangeQuery {
    /// Send the statement and its parameters, returning once the server has
    /// planned it: a statement it refuses is refused here, as a polled one is
    /// on its first poll.
    pub(super) async fn open(
        id: Uuid,
        client: Arc<SqlClientInner>,
        query: &str,
        parameters: RecordBatch,
        default_namespace_path: &[String],
    ) -> Result<Self> {
        if parameters.num_rows() != 1 {
            return Err(Error::InvalidInput {
                message: format!(
                    "SQL query parameters must be exactly one row, got {}",
                    parameters.num_rows()
                ),
            });
        }
        let request_id = Uuid::new_v4().to_string();
        let read_timeout = resolve_timeout(
            client.client_config.timeout_config.read_timeout,
            "LANCE_CLIENT_READ_TIMEOUT",
            Some(DEFAULT_READ_TIMEOUT),
        )?
        .unwrap();
        let descriptor = FlightDescriptor::new_cmd(
            CommandStatementQuery {
                query: query.to_string(),
                transaction_id: None,
            }
            .as_any()
            .encode_to_vec(),
        );
        // The descriptor rides on the schema message, and the row follows it.
        let request = FlightDataEncoderBuilder::new()
            .with_flight_descriptor(Some(descriptor))
            .with_schema(parameters.schema())
            .build(futures::stream::iter([Ok(parameters)]));
        let mut flight = client
            .client_with_headers(default_namespace_path, &request_id)
            .await?;
        let stream = tokio::time::timeout(read_timeout, flight.do_exchange(request))
            .await
            .map_err(|_| sql_error(&request_id, "SQL query submission timed out"))?
            .map_err(|err| sql_error(&request_id, err))?;
        Ok(Self {
            id,
            client,
            response: StdMutex::new(Some(ResultEndpointStream {
                stream,
                request_id,
                read_timeout,
            })),
            lifecycle: StdMutex::new(Lifecycle::Running),
            cancelled: Notify::new(),
            terminal_at: OnceLock::new(),
            last_accessed: StdMutex::new(Instant::now()),
        })
    }

    pub(super) fn touch(&self) {
        *self.last_accessed.lock().unwrap() = Instant::now();
    }

    pub(super) fn registry_expired(&self, abandoned: bool) -> bool {
        if let Some(finished) = self.terminal_at.get() {
            return finished.elapsed() >= TERMINAL_QUERY_RETENTION;
        }
        abandoned && self.last_accessed.lock().unwrap().elapsed() >= ABANDONED_QUERY_RETENTION
    }

    pub(super) fn describe(&self) -> Result<QueryDescription> {
        self.touch();
        let status = match &*self.lifecycle.lock().unwrap() {
            Lifecycle::Running => QueryStatus::Running,
            Lifecycle::Finished => QueryStatus::Finished,
            Lifecycle::Cancelled => QueryStatus::Cancelled,
            Lifecycle::Failed(message) => {
                return Err(Error::Runtime {
                    message: message.clone(),
                });
            }
        };
        Ok(QueryDescription {
            id: self.id,
            status,
            progress: (status == QueryStatus::Finished).then_some(1.0),
            expires_at: None,
        })
    }

    /// Stop the statement. Idempotent, and a no-op once it has ended.
    pub(super) fn cancel(&self) {
        self.touch();
        let settled = self.settle(Lifecycle::Cancelled);
        // Unread rows are dropped here, which cancels the call; a query that
        // already ended has none. A reader's stream is dropped by its task
        // when it sees the cancellation.
        self.response.lock().unwrap().take();
        if settled {
            self.cancelled.notify_waiters();
        }
    }

    /// Move a running statement to `state`. `true` if this call did.
    fn settle(&self, state: Lifecycle) -> bool {
        let mut lifecycle = self.lifecycle.lock().unwrap();
        if *lifecycle != Lifecycle::Running {
            return false;
        }
        *lifecycle = state;
        drop(lifecycle);
        let _ = self.terminal_at.set(Instant::now());
        true
    }

    fn is_cancelled(&self) -> bool {
        *self.lifecycle.lock().unwrap() == Lifecycle::Cancelled
    }

    fn cancelled_error(&self) -> Error {
        Error::JobCancelled {
            job_id: Some(self.id.to_string()),
        }
    }

    async fn wait_for_cancellation(&self) {
        loop {
            let cancelled = self.cancelled.notified();
            if self.is_cancelled() {
                return;
            }
            cancelled.await;
        }
    }

    /// Take the rows and wait for the first batch, which carries the schema.
    async fn prepare(&self) -> Result<PreparedExchange> {
        let Some(rows) = self.response.lock().unwrap().take() else {
            // Taken by an earlier attempt that failed, or dropped by a cancel.
            return Err(match &*self.lifecycle.lock().unwrap() {
                Lifecycle::Failed(message) => Error::Runtime {
                    message: message.clone(),
                },
                _ => self.cancelled_error(),
            });
        };
        let mut rows = Borrowed {
            query: self,
            rows: Some(rows),
        };
        let first = tokio::select! {
            biased;
            _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
            result = rows.get().next_batch() => result,
        };
        // Read or failed, the rows are this attempt's now: a failed read is
        // not worth putting back.
        let rows = rows.keep();
        let first = first?;
        let schema = first
            .as_ref()
            .map(RecordBatch::schema)
            .or_else(|| rows.stream.schema().cloned())
            .ok_or_else(|| Error::Runtime {
                message: "SQL result stream did not include a schema".to_string(),
            })?;
        if first.is_none() {
            self.settle(Lifecycle::Finished);
        }
        Ok(PreparedExchange {
            schema,
            rest: first.is_some().then_some(rows),
            first,
        })
    }

    async fn run(
        self: Arc<Self>,
        mut prepared: PreparedExchange,
        sender: mpsc::Sender<Result<RecordBatch>>,
    ) -> Result<()> {
        if let Some(batch) = prepared.first.take()
            && !self.send(&sender, batch).await?
        {
            return Ok(());
        }
        let Some(mut rows) = prepared.rest.take() else {
            return Ok(());
        };
        loop {
            let batch = tokio::select! {
                biased;
                // The reader was dropped: the rows go with this task, which
                // ends the call.
                _ = sender.closed() => {
                    self.settle(Lifecycle::Cancelled);
                    return Ok(());
                }
                _ = self.wait_for_cancellation() => return Err(self.cancelled_error()),
                result = rows.next_batch() => result,
            };
            match batch {
                Ok(Some(batch)) => {
                    if !self.send(&sender, batch).await? {
                        return Ok(());
                    }
                }
                Ok(None) => {
                    self.settle(Lifecycle::Finished);
                    return Ok(());
                }
                Err(error) => {
                    self.settle(failure(&error));
                    return Err(error);
                }
            }
        }
    }

    async fn send(
        &self,
        sender: &mpsc::Sender<Result<RecordBatch>>,
        batch: RecordBatch,
    ) -> Result<bool> {
        tokio::select! {
            biased;
            _ = self.wait_for_cancellation() => Err(self.cancelled_error()),
            result = sender.send(Ok(batch)) => {
                if result.is_err() {
                    self.settle(Lifecycle::Cancelled);
                }
                Ok(result.is_ok())
            }
        }
    }
}

pub(super) struct ExchangeQueryHandle {
    query: Arc<ExchangeQuery>,
    result_started: AtomicBool,
}

impl ExchangeQueryHandle {
    pub(super) fn new(query: Arc<ExchangeQuery>) -> Self {
        Self {
            query,
            result_started: AtomicBool::new(false),
        }
    }
}

impl Drop for ExchangeQueryHandle {
    // Rows nobody took are rows nobody will read. Keeping the call open would
    // hold the statement running on the server, stalled on flow control,
    // until the registry forgot it.
    fn drop(&mut self) {
        if self.query.response.lock().unwrap().is_some() {
            self.query.cancel();
        }
    }
}

#[async_trait::async_trait]
impl QueryHandle for ExchangeQueryHandle {
    fn id(&self) -> Uuid {
        self.query.touch();
        self.query.id
    }

    async fn describe(&self) -> Result<QueryDescription> {
        self.query.describe()
    }

    async fn reader(&self) -> Result<SendableRecordBatchStream> {
        let timeout = self.query.client.overall_timeout()?;
        if self
            .result_started
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Err(Error::Runtime {
                message: "SQL query results can only be consumed once".to_string(),
            });
        }
        let result_start = ResultStartGuard::new(&self.result_started);
        self.query.touch();
        let started = Instant::now();
        let prepared =
            match with_overall_timeout(timeout, "SQL query result", self.query.prepare()).await {
                Ok(prepared) => prepared,
                Err(error) => {
                    // A failed read or the timeout ends the query, and with it
                    // the call, including rows a timed-out wait put back.
                    self.query.settle(failure(&error));
                    self.query.response.lock().unwrap().take();
                    return Err(error);
                }
            };
        let remaining_timeout = timeout.map(|timeout| timeout.saturating_sub(started.elapsed()));
        let schema = prepared.schema.clone();
        let (sender, receiver) = mpsc::channel(2);
        let error_sender = sender.clone();
        let query = self.query.clone();
        tokio::spawn(async move {
            let result = with_overall_timeout(
                remaining_timeout,
                "SQL query result",
                query.clone().run(prepared, sender),
            )
            .await;
            if let Err(error) = result {
                query.settle(failure(&error));
                let _ = error_sender.send(Err(error)).await;
            }
        });
        let stream = futures::stream::unfold(receiver, |mut receiver| async move {
            receiver.recv().await.map(|item| (item, receiver))
        });
        result_start.commit();
        Ok(Box::pin(SimpleRecordBatchStream::new(stream, schema)))
    }

    async fn cancel(&self) -> Result<()> {
        self.query.cancel();
        Ok(())
    }
}
