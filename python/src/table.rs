// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
use std::{collections::HashMap, sync::Arc};

use crate::{
    error::PythonErrorExt,
    index::{extract_index_params, IndexConfig},
    query::Query,
};
use arrow::{
    datatypes::{DataType, Schema},
    ffi_stream::ArrowArrayStreamReader,
    pyarrow::{FromPyArrow, PyArrowType, ToPyArrow},
};
use lancedb::table::{
    AddDataMode, ColumnAlteration, Duration, NewColumnTransform, OptimizeAction, OptimizeOptions,
    Table as LanceDbTable,
};
use pyo3::{
    exceptions::{PyKeyError, PyRuntimeError, PyValueError},
    pyclass, pymethods,
    types::{IntoPyDict, PyAnyMethods, PyDict, PyDictMethods},
    Bound, FromPyObject, PyAny, PyRef, PyResult, Python,
};
use pyo3_async_runtimes::tokio::future_into_py;

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

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct UpdateResult {
    pub rows_updated: u64,
    pub version: u64,
}

#[pymethods]
impl UpdateResult {
    pub fn __repr__(&self) -> String {
        format!(
            "UpdateResult(rows_updated={}, version={})",
            self.rows_updated, self.version
        )
    }
}

impl From<lancedb::table::UpdateResult> for UpdateResult {
    fn from(result: lancedb::table::UpdateResult) -> Self {
        Self {
            rows_updated: result.rows_updated,
            version: result.version,
        }
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct AddResult {
    pub version: u64,
}

#[pymethods]
impl AddResult {
    pub fn __repr__(&self) -> String {
        format!("AddResult(version={})", self.version)
    }
}

impl From<lancedb::table::AddResult> for AddResult {
    fn from(result: lancedb::table::AddResult) -> Self {
        Self {
            version: result.version,
        }
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct DeleteResult {
    pub version: u64,
}

#[pymethods]
impl DeleteResult {
    pub fn __repr__(&self) -> String {
        format!("DeleteResult(version={})", self.version)
    }
}

impl From<lancedb::table::DeleteResult> for DeleteResult {
    fn from(result: lancedb::table::DeleteResult) -> Self {
        Self {
            version: result.version,
        }
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct MergeResult {
    pub version: u64,
    pub num_updated_rows: u64,
    pub num_inserted_rows: u64,
    pub num_deleted_rows: u64,
}

#[pymethods]
impl MergeResult {
    pub fn __repr__(&self) -> String {
        format!(
            "MergeResult(version={}, num_updated_rows={}, num_inserted_rows={}, num_deleted_rows={})",
            self.version,
            self.num_updated_rows,
            self.num_inserted_rows,
            self.num_deleted_rows
        )
    }
}

impl From<lancedb::table::MergeResult> for MergeResult {
    fn from(result: lancedb::table::MergeResult) -> Self {
        Self {
            version: result.version,
            num_updated_rows: result.num_updated_rows,
            num_inserted_rows: result.num_inserted_rows,
            num_deleted_rows: result.num_deleted_rows,
        }
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct AddColumnsResult {
    pub version: u64,
}

#[pymethods]
impl AddColumnsResult {
    pub fn __repr__(&self) -> String {
        format!("AddColumnsResult(version={})", self.version)
    }
}

impl From<lancedb::table::AddColumnsResult> for AddColumnsResult {
    fn from(result: lancedb::table::AddColumnsResult) -> Self {
        Self {
            version: result.version,
        }
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct AlterColumnsResult {
    pub version: u64,
}

#[pymethods]
impl AlterColumnsResult {
    pub fn __repr__(&self) -> String {
        format!("AlterColumnsResult(version={})", self.version)
    }
}

impl From<lancedb::table::AlterColumnsResult> for AlterColumnsResult {
    fn from(result: lancedb::table::AlterColumnsResult) -> Self {
        Self {
            version: result.version,
        }
    }
}

#[pyclass(get_all)]
#[derive(Clone, Debug)]
pub struct DropColumnsResult {
    pub version: u64,
}

#[pymethods]
impl DropColumnsResult {
    pub fn __repr__(&self) -> String {
        format!("DropColumnsResult(version={})", self.version)
    }
}

impl From<lancedb::table::DropColumnsResult> for DropColumnsResult {
    fn from(result: lancedb::table::DropColumnsResult) -> Self {
        Self {
            version: result.version,
        }
    }
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
            let result = op.execute().await.infer_error()?;
            Ok(AddResult::from(result))
        })
    }

    pub fn delete(self_: PyRef<'_, Self>, condition: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let result = inner.delete(&condition).await.infer_error()?;
            Ok(DeleteResult::from(result))
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
            let result = op.execute().await.infer_error()?;
            Ok(UpdateResult::from(result))
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

    #[pyo3(signature = (column, index=None, replace=None, wait_timeout=None, *, name=None, train=None))]
    pub fn create_index<'a>(
        self_: PyRef<'a, Self>,
        column: String,
        index: Option<Bound<'_, PyAny>>,
        replace: Option<bool>,
        wait_timeout: Option<Bound<'_, PyAny>>,
        name: Option<String>,
        train: Option<bool>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let index = extract_index_params(&index)?;
        let timeout = wait_timeout.map(|t| t.extract::<std::time::Duration>().unwrap());
        let mut op = self_
            .inner_ref()?
            .create_index_with_timeout(&[column], index, timeout);
        if let Some(replace) = replace {
            op = op.replace(replace);
        }
        if let Some(name) = name {
            op = op.name(name);
        }
        if let Some(train) = train {
            op = op.train(train);
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

    pub fn wait_for_index<'a>(
        self_: PyRef<'a, Self>,
        index_names: Vec<String>,
        timeout: Bound<'_, PyAny>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        let timeout = timeout.extract::<std::time::Duration>()?;
        future_into_py(self_.py(), async move {
            let index_refs = index_names
                .iter()
                .map(String::as_str)
                .collect::<Vec<&str>>();
            inner
                .wait_for_index(&index_refs, timeout)
                .await
                .infer_error()?;
            Ok(())
        })
    }

    pub fn prewarm_index(self_: PyRef<'_, Self>, index_name: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.prewarm_index(&index_name).await.infer_error()?;
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
                    let dict = PyDict::new(py);
                    dict.set_item("num_indexed_rows", stats.num_indexed_rows)?;
                    dict.set_item("num_unindexed_rows", stats.num_unindexed_rows)?;
                    dict.set_item("index_type", stats.index_type.to_string())?;

                    if let Some(distance_type) = stats.distance_type {
                        dict.set_item("distance_type", distance_type.to_string())?;
                    }

                    if let Some(num_indices) = stats.num_indices {
                        dict.set_item("num_indices", num_indices)?;
                    }

                    if let Some(loss) = stats.loss {
                        dict.set_item("loss", loss)?;
                    }

                    Ok(Some(dict.unbind()))
                })
            } else {
                Ok(None)
            }
        })
    }

    pub fn stats(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let stats = inner.stats().await.infer_error()?;
            Python::with_gil(|py| {
                let dict = PyDict::new(py);
                dict.set_item("total_bytes", stats.total_bytes)?;
                dict.set_item("num_rows", stats.num_rows)?;
                dict.set_item("num_indices", stats.num_indices)?;

                let fragment_stats = PyDict::new(py);
                fragment_stats.set_item("num_fragments", stats.fragment_stats.num_fragments)?;
                fragment_stats.set_item(
                    "num_small_fragments",
                    stats.fragment_stats.num_small_fragments,
                )?;

                let fragment_lengths = PyDict::new(py);
                fragment_lengths.set_item("min", stats.fragment_stats.lengths.min)?;
                fragment_lengths.set_item("max", stats.fragment_stats.lengths.max)?;
                fragment_lengths.set_item("mean", stats.fragment_stats.lengths.mean)?;
                fragment_lengths.set_item("p25", stats.fragment_stats.lengths.p25)?;
                fragment_lengths.set_item("p50", stats.fragment_stats.lengths.p50)?;
                fragment_lengths.set_item("p75", stats.fragment_stats.lengths.p75)?;
                fragment_lengths.set_item("p99", stats.fragment_stats.lengths.p99)?;

                fragment_stats.set_item("lengths", fragment_lengths)?;
                dict.set_item("fragment_stats", fragment_stats)?;

                Ok(Some(dict.unbind()))
            })
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
                        let dict = PyDict::new(py);
                        dict.set_item("version", v.version).unwrap();
                        dict.set_item(
                            "timestamp",
                            v.timestamp.timestamp_nanos_opt().unwrap_or_default(),
                        )
                        .unwrap();

                        let tup: Vec<(&String, &String)> = v.metadata.iter().collect();
                        dict.set_item("metadata", tup.into_py_dict(py)?).unwrap();
                        Ok(dict.unbind())
                    })
                    .collect::<PyResult<Vec<_>>>()
            });

            versions_as_dict
        })
    }

    pub fn checkout(self_: PyRef<'_, Self>, version: LanceVersion) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        let py = self_.py();
        future_into_py(py, async move {
            match version {
                LanceVersion::Version(version_num) => {
                    inner.checkout(version_num).await.infer_error()
                }
                LanceVersion::Tag(tag) => inner.checkout_tag(&tag).await.infer_error(),
            }
        })
    }

    pub fn checkout_latest(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.checkout_latest().await.infer_error()
        })
    }

    #[pyo3(signature = (version=None))]
    pub fn restore(
        self_: PyRef<'_, Self>,
        version: Option<LanceVersion>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        let py = self_.py();

        future_into_py(py, async move {
            if let Some(version) = version {
                match version {
                    LanceVersion::Version(num) => inner.checkout(num).await.infer_error()?,
                    LanceVersion::Tag(tag) => inner.checkout_tag(&tag).await.infer_error()?,
                }
            }
            inner.restore().await.infer_error()
        })
    }

    pub fn query(&self) -> Query {
        Query::new(self.inner_ref().unwrap().query())
    }

    #[getter]
    pub fn tags(&self) -> PyResult<Tags> {
        Ok(Tags::new(self.inner_ref()?.clone()))
    }

    /// Optimize the on-disk data by compacting and pruning old data, for better performance.
    #[pyo3(signature = (cleanup_since_ms=None, delete_unverified=None, retrain=None))]
    pub fn optimize(
        self_: PyRef<'_, Self>,
        cleanup_since_ms: Option<u64>,
        delete_unverified: Option<bool>,
        retrain: Option<bool>,
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
                .optimize(lancedb::table::OptimizeAction::Index(match retrain {
                    Some(true) => OptimizeOptions::retrain(),
                    _ => OptimizeOptions::default(),
                }))
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
        if let Some(timeout) = parameters.timeout {
            builder.timeout(timeout);
        }

        future_into_py(self_.py(), async move {
            let res = builder.execute(Box::new(batches)).await.infer_error()?;
            Ok(MergeResult::from(res))
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
            let result = inner.add_columns(definitions, None).await.infer_error()?;
            Ok(AddColumnsResult::from(result))
        })
    }

    pub fn add_columns_with_schema(
        self_: PyRef<'_, Self>,
        schema: PyArrowType<Schema>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let arrow_schema = &schema.0;
        let transform = NewColumnTransform::AllNulls(Arc::new(arrow_schema.clone()));

        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let result = inner.add_columns(transform, None).await.infer_error()?;
            Ok(AddColumnsResult::from(result))
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
            let result = inner.alter_columns(&alterations).await.infer_error()?;
            Ok(AlterColumnsResult::from(result))
        })
    }

    pub fn drop_columns(self_: PyRef<Self>, columns: Vec<String>) -> PyResult<Bound<PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let column_refs = columns.iter().map(String::as_str).collect::<Vec<&str>>();
            let result = inner.drop_columns(&column_refs).await.infer_error()?;
            Ok(DropColumnsResult::from(result))
        })
    }

    pub fn replace_field_metadata<'a>(
        self_: PyRef<'a, Self>,
        field_name: String,
        metadata: &Bound<'_, PyDict>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let mut new_metadata = HashMap::<String, String>::new();
        for (column_name, value) in metadata.into_iter() {
            let key: String = column_name.extract()?;
            let value: String = value.extract()?;
            new_metadata.insert(key, value);
        }

        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let native_tbl = inner
                .as_native()
                .ok_or_else(|| PyValueError::new_err("This cannot be run on a remote table"))?;
            let schema = native_tbl.manifest().await.infer_error()?.schema;
            let field = schema
                .field(&field_name)
                .ok_or_else(|| PyKeyError::new_err(format!("Field {} not found", field_name)))?;

            native_tbl
                .replace_field_metadata(vec![(field.id as u32, new_metadata)])
                .await
                .infer_error()?;

            Ok(())
        })
    }
}

#[derive(FromPyObject)]
pub enum LanceVersion {
    Version(u64),
    Tag(String),
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
    timeout: Option<std::time::Duration>,
}

#[pyclass]
pub struct Tags {
    inner: LanceDbTable,
}

impl Tags {
    pub fn new(table: LanceDbTable) -> Self {
        Self { inner: table }
    }
}

#[pymethods]
impl Tags {
    pub fn list(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let tags = inner.tags().await.infer_error()?;
            let res = tags.list().await.infer_error()?;

            Python::with_gil(|py| {
                let py_dict = PyDict::new(py);
                for (key, contents) in res {
                    let value_dict = PyDict::new(py);
                    value_dict.set_item("version", contents.version)?;
                    value_dict.set_item("manifest_size", contents.manifest_size)?;
                    py_dict.set_item(key, value_dict)?;
                }
                Ok(py_dict.unbind())
            })
        })
    }

    pub fn get_version(self_: PyRef<'_, Self>, tag: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let tags = inner.tags().await.infer_error()?;
            let res = tags.get_version(tag.as_str()).await.infer_error()?;
            Ok(res)
        })
    }

    pub fn create(self_: PyRef<Self>, tag: String, version: u64) -> PyResult<Bound<PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let mut tags = inner.tags().await.infer_error()?;
            tags.create(tag.as_str(), version).await.infer_error()?;
            Ok(())
        })
    }

    pub fn delete(self_: PyRef<Self>, tag: String) -> PyResult<Bound<PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let mut tags = inner.tags().await.infer_error()?;
            tags.delete(tag.as_str()).await.infer_error()?;
            Ok(())
        })
    }

    pub fn update(self_: PyRef<Self>, tag: String, version: u64) -> PyResult<Bound<PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let mut tags = inner.tags().await.infer_error()?;
            tags.update(tag.as_str(), version).await.infer_error()?;
            Ok(())
        })
    }
}
