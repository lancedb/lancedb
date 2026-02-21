// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use arrow::{
    datatypes::{Schema, SchemaRef},
    ffi_stream::ArrowArrayStreamReader,
    pyarrow::{FromPyArrow, PyArrowType},
};
use futures::StreamExt;
use lancedb::{
    arrow::{SendableRecordBatchStream, SimpleRecordBatchStream},
    data::scannable::Scannable,
    Error,
};
use pyo3::{types::PyAnyMethods, FromPyObject, Py, PyAny, Python};

/// Adapter that implements Scannable for a Python reader factory callable.
///
/// This holds a Python callable that returns a RecordBatchReader when called.
/// For rescannable sources, the callable can be invoked multiple times to
/// get fresh readers.
pub struct PyScannable {
    /// Python callable that returns a RecordBatchReader
    reader_factory: Py<PyAny>,
    schema: SchemaRef,
    num_rows: Option<usize>,
    rescannable: bool,
}

impl std::fmt::Debug for PyScannable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PyScannable")
            .field("schema", &self.schema)
            .field("num_rows", &self.num_rows)
            .field("rescannable", &self.rescannable)
            .finish()
    }
}

impl Scannable for PyScannable {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let reader: Result<ArrowArrayStreamReader, Error> = {
            Python::attach(|py| {
                let result =
                    self.reader_factory
                        .call0(py)
                        .map_err(|e| lancedb::Error::Runtime {
                            message: format!("Python reader factory failed: {}", e),
                        })?;
                ArrowArrayStreamReader::from_pyarrow_bound(result.bind(py)).map_err(|e| {
                    lancedb::Error::Runtime {
                        message: format!("Failed to create Arrow reader from Python: {}", e),
                    }
                })
            })
        };

        // Reader is blocking but stream is non-blocking, so we need to spawn a task to pull.
        let (tx, rx) = tokio::sync::mpsc::channel(1);

        let join_handle = tokio::task::spawn_blocking(move || {
            let reader = match reader {
                Ok(reader) => reader,
                Err(e) => {
                    let _ = tx.blocking_send(Err(e));
                    return;
                }
            };
            for batch in reader {
                match batch {
                    Ok(batch) => {
                        if tx.blocking_send(Ok(batch)).is_err() {
                            // Receiver dropped, stop processing
                            break;
                        }
                    }
                    Err(source) => {
                        let _ = tx.blocking_send(Err(Error::Arrow { source }));
                        break;
                    }
                }
            }
        });

        let schema = self.schema.clone();
        let stream = futures::stream::unfold(
            (rx, Some(join_handle)),
            |(mut rx, join_handle)| async move {
                match rx.recv().await {
                    Some(Ok(batch)) => Some((Ok(batch), (rx, join_handle))),
                    Some(Err(e)) => Some((Err(e), (rx, join_handle))),
                    None => {
                        // Channel closed. Check if the task panicked — a panic
                        // drops the sender without sending an error, so without
                        // this check we'd silently return a truncated stream.
                        if let Some(handle) = join_handle {
                            if let Err(join_err) = handle.await {
                                return Some((
                                    Err(Error::Runtime {
                                        message: format!("Reader task panicked: {}", join_err),
                                    }),
                                    (rx, None),
                                ));
                            }
                        }
                        None
                    }
                }
            },
        );
        Box::pin(SimpleRecordBatchStream::new(stream.fuse(), schema))
    }

    fn num_rows(&self) -> Option<usize> {
        self.num_rows
    }

    fn rescannable(&self) -> bool {
        self.rescannable
    }
}

impl<'py> FromPyObject<'py> for PyScannable {
    fn extract_bound(ob: &pyo3::Bound<'py, PyAny>) -> pyo3::PyResult<Self> {
        // Convert from Scannable dataclass.
        let schema: PyArrowType<Schema> = ob.getattr("schema")?.extract()?;
        let schema = Arc::new(schema.0);
        let num_rows: Option<usize> = ob.getattr("num_rows")?.extract()?;
        let rescannable: bool = ob.getattr("rescannable")?.extract()?;
        let reader_factory: Py<PyAny> = ob.getattr("reader")?.unbind();

        Ok(Self {
            schema,
            reader_factory,
            num_rows,
            rescannable,
        })
    }
}
