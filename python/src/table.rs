// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
use arrow::{
    datatypes::DataType,
    ffi_stream::ArrowArrayStreamReader,
    pyarrow::{FromPyArrow, ToPyArrow},
};
use lancedb::table::{
    AddDataMode, ColumnAlteration, Duration, NewColumnTransform, OptimizeAction, OptimizeOptions,
    Table as LanceDbTable,
};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    pyclass, pymethods,
    types::{IntoPyDict, PyAnyMethods, PyDict, PyDictMethods},
    Bound, FromPyObject, PyAny, PyRef, PyResult, Python, ToPyObject,
};
use pyo3_async_runtimes::tokio::future_into_py;

use crate::{
    error::PythonErrorExt,
    index::{extract_index_params, IndexConfig},
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

    /// Returns True if the table is open, False if it is closed.
    pub fn is_open(&self) -> bool {
        self.inner.is_some()
    }

    /// Closes the table, releasing any resources associated with it.
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

    #[pyo3(signature = (updates, r#where=None))]
    pub fn update<'a>(
        self_: PyRef<'a, Self>,
        updates: &Bound<'_, PyDict>,
        r#where: Option<String>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let mut op = self_.inner_ref()?.update();
        if let Some(only_if) = r#where {
            op = op.only_if(only_if);
        }
        for (column_name, value) in updates.into_iter() {
            let column_name: String = column_name.extract()?;
            let value: String = value.extract()?;
            op = op.column(column_name, value);
        }
        future_into_py(self_.py(), async move {
            op.execute().await.infer_error()?;
            Ok(())
        })
    }

    #[pyo3(signature = (filter=None))]
    pub fn count_rows(
        self_: PyRef<'_, Self>,
        filter: Option<String>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.count_rows(filter).await.infer_error()
        })
    }

    #[pyo3(signature = (column, index=None, replace=None))]
    pub fn create_index<'a>(
        self_: PyRef<'a, Self>,
        column: String,
        index: Option<Bound<'_, PyAny>>,
        replace: Option<bool>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let index = extract_index_params(&index)?;
        let mut op = self_.inner_ref()?.create_index(&[column], index);
        if let Some(replace) = replace {
            op = op.replace(replace);
        }

        future_into_py(self_.py(), async move {
            op.execute().await.infer_error()?;
            Ok(())
        })
    }

    pub fn drop_index(self_: PyRef<'_, Self>, index_name: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.drop_index(&index_name).await.infer_error()?;
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

    pub fn index_stats(self_: PyRef<'_, Self>, index_name: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let stats = inner.index_stats(&index_name).await.infer_error()?;
            if let Some(stats) = stats {
                Python::with_gil(|py| {
                    let dict = PyDict::new_bound(py);
                    dict.set_item("num_indexed_rows", stats.num_indexed_rows)?;
                    dict.set_item("num_unindexed_rows", stats.num_unindexed_rows)?;
                    dict.set_item("index_type", stats.index_type.to_string())?;

                    if let Some(distance_type) = stats.distance_type {
                        dict.set_item("distance_type", distance_type.to_string())?;
                    }

                    if let Some(num_indices) = stats.num_indices {
                        dict.set_item("num_indices", num_indices)?;
                    }

                    Ok(Some(dict.to_object(py)))
                })
            } else {
                Ok(None)
            }
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

    pub fn list_versions(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let versions = inner.list_versions().await.infer_error()?;
            let versions_as_dict = Python::with_gil(|py| {
                versions
                    .iter()
                    .map(|v| {
                        let dict = PyDict::new_bound(py);
                        dict.set_item("version", v.version).unwrap();
                        dict.set_item(
                            "timestamp",
                            v.timestamp.timestamp_nanos_opt().unwrap_or_default(),
                        )
                        .unwrap();

                        let tup: Vec<(&String, &String)> = v.metadata.iter().collect();
                        dict.set_item("metadata", tup.into_py_dict_bound(py))
                            .unwrap();
                        dict.to_object(py)
                    })
                    .collect::<Vec<_>>()
            });

            Ok(versions_as_dict)
        })
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

    /// Optimize the on-disk data by compacting and pruning old data, for better performance.
    #[pyo3(signature = (cleanup_since_ms=None, delete_unverified=None))]
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

    pub fn execute_merge_insert<'a>(
        self_: PyRef<'a, Self>,
        data: Bound<'a, PyAny>,
        parameters: MergeInsertParams,
    ) -> PyResult<Bound<'a, PyAny>> {
        let batches: ArrowArrayStreamReader = ArrowArrayStreamReader::from_pyarrow_bound(&data)?;
        let on = parameters.on.iter().map(|s| s.as_str()).collect::<Vec<_>>();
        let mut builder = self_.inner_ref()?.merge_insert(&on);
        if parameters.when_matched_update_all {
            builder.when_matched_update_all(parameters.when_matched_update_all_condition);
        }
        if parameters.when_not_matched_insert_all {
            builder.when_not_matched_insert_all();
        }
        if parameters.when_not_matched_by_source_delete {
            builder
                .when_not_matched_by_source_delete(parameters.when_not_matched_by_source_condition);
        }

        future_into_py(self_.py(), async move {
            builder.execute(Box::new(batches)).await.infer_error()?;
            Ok(())
        })
    }

    pub fn uses_v2_manifest_paths(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner
                .as_native()
                .ok_or_else(|| PyValueError::new_err("This cannot be run on a remote table"))?
                .uses_v2_manifest_paths()
                .await
                .infer_error()
        })
    }

    pub fn migrate_manifest_paths_v2(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner
                .as_native()
                .ok_or_else(|| PyValueError::new_err("This cannot be run on a remote table"))?
                .migrate_manifest_paths_v2()
                .await
                .infer_error()
        })
    }

    pub fn add_columns(
        self_: PyRef<'_, Self>,
        definitions: Vec<(String, String)>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let definitions = NewColumnTransform::SqlExpressions(definitions);

        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.add_columns(definitions, None).await.infer_error()?;
            Ok(())
        })
    }

    pub fn alter_columns<'a>(
        self_: PyRef<'a, Self>,
        alterations: Vec<Bound<PyDict>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let alterations = alterations
            .iter()
            .map(|alteration| {
                let path = alteration
                    .get_item("path")?
                    .ok_or_else(|| PyValueError::new_err("Missing path"))?
                    .extract()?;
                let rename = {
                    // We prefer rename, but support name for backwards compatibility
                    let rename = if let Ok(Some(rename)) = alteration.get_item("rename") {
                        Some(rename)
                    } else {
                        alteration.get_item("name")?
                    };
                    rename.map(|name| name.extract()).transpose()?
                };
                let nullable = alteration
                    .get_item("nullable")?
                    .map(|val| val.extract())
                    .transpose()?;
                let data_type = alteration
                    .get_item("data_type")?
                    .map(|val| DataType::from_pyarrow_bound(&val))
                    .transpose()?;
                Ok(ColumnAlteration {
                    path,
                    rename,
                    nullable,
                    data_type,
                })
            })
            .collect::<PyResult<Vec<_>>>()?;

        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.alter_columns(&alterations).await.infer_error()?;
            Ok(())
        })
    }

    pub fn drop_columns(self_: PyRef<Self>, columns: Vec<String>) -> PyResult<Bound<PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let column_refs = columns.iter().map(String::as_str).collect::<Vec<&str>>();
            inner.drop_columns(&column_refs).await.infer_error()?;
            Ok(())
        })
    }
}

#[derive(FromPyObject)]
#[pyo3(from_item_all)]
pub struct MergeInsertParams {
    on: Vec<String>,
    when_matched_update_all: bool,
    when_matched_update_all_condition: Option<String>,
    when_not_matched_insert_all: bool,
    when_not_matched_by_source_delete: bool,
    when_not_matched_by_source_condition: Option<String>,
}
