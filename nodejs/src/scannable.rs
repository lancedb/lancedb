// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! NodeJS binding for the [`lancedb::data::scannable::Scannable`] trait.
//!
//! The JS side supplies a `getNextBatch` callback that returns the next Arrow
//! `RecordBatch` encoded as a self-contained Arrow IPC Stream message (schema
//! message + record batch message + EOS marker) wrapped in a `Buffer`, or
//! `null` when the stream is exhausted. The Rust side parses each buffer with
//! `arrow_ipc::reader::StreamReader`, validates the schema on the first batch,
//! and yields decoded `RecordBatch`es as a [`SendableRecordBatchStream`].

use std::io::Cursor;
use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_ipc::reader::StreamReader;
use arrow_schema::SchemaRef;
use futures::stream::once;
use lancedb::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
use lancedb::data::scannable::Scannable as LanceScannable;
use lancedb::ipc::ipc_file_to_schema;
use lancedb::{Error, Result as LanceResult};
use napi::bindgen_prelude::*;
use napi::threadsafe_function::ThreadsafeFunction;
use napi_derive::napi;

/// Threadsafe handle to the JS `getNextBatch` callback. The callback takes no
/// arguments and returns a Promise that resolves to a `Buffer` containing one
/// IPC Stream message, or `null` at end-of-stream.
type GetNextBatchFn = ThreadsafeFunction<(), Promise<Option<Buffer>>, (), Status, false>;

/// A Rust-side view of a JS-constructed `Scannable`.
///
/// Held in JS as the return value of the `Scannable` class constructor. When
/// passed to a consumer that accepts `impl lancedb::data::scannable::Scannable`,
/// the consumer invokes `scan_as_stream()` to pull batches through the JS
/// callback.
#[napi]
pub struct NapiScannable {
    schema: SchemaRef,
    num_rows: Option<usize>,
    rescannable: bool,
    // `ThreadsafeFunction` is not `Clone`; wrap in `Arc` so the stream
    // returned by `scan_as_stream` can own a handle independent of `self`.
    get_next_batch: Arc<GetNextBatchFn>,
    // Tracks whether a scan has already started; used to enforce one-shot
    // semantics on non-rescannable sources.
    scanned: bool,
}

#[napi]
impl NapiScannable {
    /// Construct a new `NapiScannable`.
    ///
    /// - `schema_buf` — Arrow IPC File buffer carrying only the schema (no batches).
    /// - `num_rows` — optional row count hint; not validated against the stream.
    /// - `rescannable` — whether `get_next_batch` may be re-driven after the
    ///   scan completes.
    /// - `get_next_batch` — JS callback that yields the next batch as an Arrow
    ///   IPC Stream message wrapped in a `Buffer`, or `null` at EOF.
    #[napi(constructor)]
    pub fn new(
        schema_buf: Buffer,
        num_rows: Option<i64>,
        rescannable: bool,
        get_next_batch: Function<(), Promise<Option<Buffer>>>,
    ) -> napi::Result<Self> {
        let schema = ipc_file_to_schema(schema_buf.to_vec())
            .map_err(|e| napi::Error::from_reason(format!("Invalid schema buffer: {}", e)))?;
        let num_rows = num_rows
            .map(|n| {
                usize::try_from(n)
                    .map_err(|_| napi::Error::from_reason("num_rows must be non-negative"))
            })
            .transpose()?;
        let get_next_batch = Arc::new(get_next_batch.build_threadsafe_function().build()?);
        Ok(Self {
            schema,
            num_rows,
            rescannable,
            get_next_batch,
            scanned: false,
        })
    }
}

impl std::fmt::Debug for NapiScannable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NapiScannable")
            .field("schema", &self.schema)
            .field("num_rows", &self.num_rows)
            .field("rescannable", &self.rescannable)
            .finish()
    }
}

impl LanceScannable for NapiScannable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let schema = self.schema.clone();

        // One-shot enforcement for non-rescannable sources: return a stream
        // whose first item is an error.
        if self.scanned && !self.rescannable {
            let err_stream = once(async {
                Err(Error::InvalidInput {
                    message: "Scannable has already been consumed (non-rescannable source)"
                        .to_string(),
                })
            });
            return Box::pin(SimpleRecordBatchStream::new(err_stream, schema));
        }
        self.scanned = true;

        let tsfn = Arc::clone(&self.get_next_batch);
        let declared_schema = schema.clone();

        // State threaded through the unfold: (tsfn, next_batch_index,
        // declared_schema, validated_first_schema, errored).
        let initial = State {
            tsfn,
            batch_index: 0,
            declared_schema,
            validated: false,
            errored: false,
        };

        let stream = futures::stream::unfold(initial, |mut state| async move {
            if state.errored {
                return None;
            }

            // Pull the next IPC Stream buffer from JS.
            let buf = match pull_next(&state.tsfn).await {
                Ok(Some(buf)) => buf,
                Ok(None) => return None,
                Err(e) => {
                    state.errored = true;
                    return Some((Err(e), state));
                }
            };

            match decode_one_batch(buf.as_ref(), &state.declared_schema, state.validated) {
                Ok(batch) => {
                    state.validated = true;
                    state.batch_index += 1;
                    Some((Ok(batch), state))
                }
                Err(e) => {
                    let tagged = Error::Runtime {
                        message: format!(
                            "[scannable/rust-bridge] failure at batch index {}: {}",
                            state.batch_index, e
                        ),
                    };
                    state.errored = true;
                    Some((Err(tagged), state))
                }
            }
        });

        Box::pin(SimpleRecordBatchStream::new(stream, schema))
    }

    fn num_rows(&self) -> Option<usize> {
        self.num_rows
    }

    fn rescannable(&self) -> bool {
        self.rescannable
    }
}

struct State {
    tsfn: Arc<GetNextBatchFn>,
    batch_index: usize,
    declared_schema: SchemaRef,
    validated: bool,
    errored: bool,
}

/// Invoke the JS callback and await its Promise. Errors on the JS side surface
/// here as rejected promises and are tunneled back as `lancedb::Error::Runtime`.
async fn pull_next(tsfn: &GetNextBatchFn) -> LanceResult<Option<Buffer>> {
    let promise = tsfn.call_async(()).await.map_err(|e| Error::Runtime {
        message: format!(
            "[scannable/js-factory] napi error status={}, reason={}",
            e.status, e.reason
        ),
    })?;
    promise.await.map_err(|e| Error::Runtime {
        message: format!(
            "[scannable/js-iterator] napi error status={}, reason={}",
            e.status, e.reason
        ),
    })
}

/// Decode one IPC Stream buffer (schema + batch + EOS) into a `RecordBatch`.
/// On the first call for a scan, validates that the stream's schema matches
/// the one declared at construction.
fn decode_one_batch(
    buf: &[u8],
    declared: &SchemaRef,
    already_validated: bool,
) -> LanceResult<RecordBatch> {
    let reader = StreamReader::try_new(Cursor::new(buf), None).map_err(|e| Error::Runtime {
        message: format!("failed to open IPC stream reader: {}", e),
    })?;

    if !already_validated {
        let actual = reader.schema();
        if actual.as_ref() != declared.as_ref() {
            return Err(Error::InvalidInput {
                message: format!(
                    "declared schema does not match stream schema: declared={:?} actual={:?}",
                    declared, actual
                ),
            });
        }
    }

    let mut iter = reader;
    let batch = iter
        .next()
        .ok_or_else(|| Error::Runtime {
            message: "IPC stream contained schema but no record batch".to_string(),
        })?
        .map_err(|e| Error::Runtime {
            message: format!("failed to decode record batch: {}", e),
        })?;
    Ok(batch)
}
