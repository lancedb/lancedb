use arrow::{
    ffi_stream::ArrowArrayStreamReader,
    pyarrow::{FromPyArrow, ToPyArrow},
};
use lancedb::table::{
    AddDataMode, Duration, OptimizeAction, OptimizeOptions, Table as LanceDbTable,
};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    pyclass, pymethods,
    types::{PyDict, PyString},
    Bound, PyAny, PyRef, PyResult, Python,
};
use pyo3_asyncio_0_21::tokio::future_into_py;

use crate::{
    error::PythonErrorExt,
    index::{Index, IndexConfig},
    query::Query,
};

/// Statistics about a compaction operation.
#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct CompactionStats {
    /// The number of fragments removed
    pub fragments_removed: u64,
    /// The number of new, compacted fragments added
    pub fragments_added: u64,
    /// The number of data files removed
    pub files_removed: u64,
    /// The number of new, compacted data files added
    pub files_added: u64,
}

/// Statistics about a cleanup operation
#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct RemovalStats {
    /// The number of bytes removed
    pub bytes_removed: u64,
    /// The number of old versions removed
    pub old_versions_removed: u64,
}

/// Statistics about an optimize operation
#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct OptimizeStats {
    /// Statistics about the compaction operation
    pub compaction: CompactionStats,
    /// Statistics about the removal operation
    pub prune: RemovalStats,
}

#[pyclass]
pub struct Table {
    // We keep a copy of the name to use if the inner table is dropped
    name: String,
    inner: Option<LanceDbTable>,
}

#[pymethods]
impl OptimizeStats {
    pub fn __repr__(&self) -> String {
        format!(
            "OptimizeStats(compaction={:?}, prune={:?})",
            self.compaction, self.prune
        )
    }
}

impl Table {
    pub(crate) fn new(inner: LanceDbTable) -> Self {
        Self {
            name: inner.name().to_string(),
            inner: Some(inner),
        }
    }
}

impl Table {
    fn inner_ref(&self) -> PyResult<&LanceDbTable> {
        self.inner
            .as_ref()
            .ok_or_else(|| PyRuntimeError::new_err(format!("Table {} is closed", self.name)))
    }
}

#[pymethods]
impl Table {
    pub fn name(&self) -> String {
        self.name.clone()
    }

    pub fn is_open(&self) -> bool {
        self.inner.is_some()
    }

    pub fn close(&mut self) {
        self.inner.take();
    }

    pub fn schema(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let schema = inner.schema().await.infer_error()?;
            Python::with_gil(|py| schema.to_pyarrow(py))
        })
    }

    pub fn add<'a>(
        self_: PyRef<'a, Self>,
        data: Bound<'_, PyAny>,
        mode: String,
    ) -> PyResult<Bound<'a, PyAny>> {
        let batches = ArrowArrayStreamReader::from_pyarrow_bound(&data)?;
        let mut op = self_.inner_ref()?.add(batches);
        if mode == "append" {
            op = op.mode(AddDataMode::Append);
        } else if mode == "overwrite" {
            op = op.mode(AddDataMode::Overwrite);
        } else {
            return Err(PyValueError::new_err(format!("Invalid mode: {}", mode)));
        }

        future_into_py(self_.py(), async move {
            op.execute().await.infer_error()?;
            Ok(())
        })
    }

    pub fn delete(self_: PyRef<'_, Self>, condition: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.delete(&condition).await.infer_error()
        })
    }

    pub fn update<'a>(
        self_: PyRef<'a, Self>,
        updates: &PyDict,
        r#where: Option<String>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let mut op = self_.inner_ref()?.update();
        if let Some(only_if) = r#where {
            op = op.only_if(only_if);
        }
        for (column_name, value) in updates.into_iter() {
            let column_name: &PyString = column_name.downcast()?;
            let column_name = column_name.to_str()?.to_string();
            let value: &PyString = value.downcast()?;
            let value = value.to_str()?.to_string();
            op = op.column(column_name, value);
        }
        future_into_py(self_.py(), async move {
            op.execute().await.infer_error()?;
            Ok(())
        })
    }

    pub fn count_rows(
        self_: PyRef<'_, Self>,
        filter: Option<String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.count_rows(filter).await.infer_error()
        })
    }

    pub fn create_index<'a>(
        self_: PyRef<'a, Self>,
        column: String,
        index: Option<&Index>,
        replace: Option<bool>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let index = if let Some(index) = index {
            index.consume()?
        } else {
            lancedb::index::Index::Auto
        };
        let mut op = self_.inner_ref()?.create_index(&[column], index);
        if let Some(replace) = replace {
            op = op.replace(replace);
        }

        future_into_py(self_.py(), async move {
            op.execute().await.infer_error()?;
            Ok(())
        })
    }

    pub fn list_indices(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            Ok(inner
                .list_indices()
                .await
                .infer_error()?
                .into_iter()
                .map(IndexConfig::from)
                .collect::<Vec<_>>())
        })
    }

    pub fn __repr__(&self) -> String {
        match &self.inner {
            None => format!("ClosedTable({})", self.name),
            Some(inner) => inner.to_string(),
        }
    }

    pub fn version(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(
            self_.py(),
            async move { inner.version().await.infer_error() },
        )
    }

    pub fn checkout(self_: PyRef<'_, Self>, version: u64) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.checkout(version).await.infer_error()
        })
    }

    pub fn checkout_latest(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.checkout_latest().await.infer_error()
        })
    }

    pub fn restore(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(
            self_.py(),
            async move { inner.restore().await.infer_error() },
        )
    }

    pub fn query(&self) -> Query {
        Query::new(self.inner_ref().unwrap().query())
    }

    pub fn optimize(
        self_: PyRef<'_, Self>,
        cleanup_since_ms: Option<u64>,
        delete_unverified: Option<bool>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        let older_than = if let Some(ms) = cleanup_since_ms {
            if ms > i64::MAX as u64 {
                return Err(PyValueError::new_err(format!(
                    "cleanup_since_ms must be between {} and -{}",
                    i32::MAX,
                    i32::MAX
                )));
            }
            Duration::try_milliseconds(ms as i64)
        } else {
            None
        };
        future_into_py(self_.py(), async move {
            let compaction_stats = inner
                .optimize(OptimizeAction::Compact {
                    options: lancedb::table::CompactionOptions::default(),
                    remap_options: None,
                })
                .await
                .infer_error()?
                .compaction
                .unwrap();
            let prune_stats = inner
                .optimize(OptimizeAction::Prune {
                    older_than,
                    delete_unverified,
                    error_if_tagged_old_versions: None,
                })
                .await
                .infer_error()?
                .prune
                .unwrap();
            inner
                .optimize(lancedb::table::OptimizeAction::Index(
                    OptimizeOptions::default(),
                ))
                .await
                .infer_error()?;
            Ok(OptimizeStats {
                compaction: CompactionStats {
                    files_added: compaction_stats.files_added as u64,
                    files_removed: compaction_stats.files_removed as u64,
                    fragments_added: compaction_stats.fragments_added as u64,
                    fragments_removed: compaction_stats.fragments_removed as u64,
                },
                prune: RemovalStats {
                    bytes_removed: prune_stats.bytes_removed,
                    old_versions_removed: prune_stats.old_versions,
                },
            })
        })
    }
}
