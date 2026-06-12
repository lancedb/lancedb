// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
use std::{collections::HashMap, sync::Arc};

use crate::runtime::future_into_py;
use crate::{
    connection::Connection,
    error::PythonErrorExt,
    expr::PyExpr,
    index::{IndexConfig, extract_index_params},
    query::{Query, TakeQuery},
    table::scannable::PyScannable,
};
use arrow::{
    datatypes::{DataType, Schema},
    ffi_stream::ArrowArrayStreamReader,
    pyarrow::{FromPyArrow, PyArrowType, ToPyArrow},
};
use lancedb::table::{
    AddDataMode, ColumnAlteration, Duration, FieldMetadataUpdate, NewColumnTransform,
    OptimizeAction, OptimizeOptions, Ref, Table as LanceDbTable,
};
use pyo3::{
    Bound, FromPyObject, Py, PyAny, PyRef, PyResult, Python,
    exceptions::{PyRuntimeError, PyValueError},
    pyclass, pymethods,
    types::{IntoPyDict, PyAnyMethods, PyDict, PyDictMethods},
};

mod scannable;

#[derive(FromPyObject)]
enum PredicateArg {
    Expr(PyExpr),
    Sql(String),
}

/// Statistics about a compaction operation.
#[pyclass(get_all, from_py_object)]
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
#[pyclass(get_all, from_py_object)]
#[derive(Clone, Debug)]
pub struct RemovalStats {
    /// The number of bytes removed
    pub bytes_removed: u64,
    /// The number of old versions removed
    pub old_versions_removed: u64,
}

/// Statistics about an optimize operation
#[pyclass(get_all, from_py_object)]
#[derive(Clone, Debug)]
pub struct OptimizeStats {
    /// Statistics about the compaction operation
    pub compaction: CompactionStats,
    /// Statistics about the removal operation
    pub prune: RemovalStats,
}

#[pyclass(get_all, from_py_object)]
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

#[pyclass(get_all, from_py_object)]
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

#[pyclass(get_all, from_py_object)]
#[derive(Clone, Debug)]
pub struct DeleteResult {
    pub num_deleted_rows: u64,
    pub version: u64,
}

#[pymethods]
impl DeleteResult {
    pub fn __repr__(&self) -> String {
        format!(
            "DeleteResult(num_deleted_rows={}, version={})",
            self.num_deleted_rows, self.version
        )
    }
}

impl From<lancedb::table::DeleteResult> for DeleteResult {
    fn from(result: lancedb::table::DeleteResult) -> Self {
        Self {
            num_deleted_rows: result.num_deleted_rows,
            version: result.version,
        }
    }
}

#[pyclass(get_all, from_py_object)]
#[derive(Clone, Debug)]
pub struct MergeResult {
    pub version: u64,
    pub num_updated_rows: u64,
    pub num_inserted_rows: u64,
    pub num_deleted_rows: u64,
    pub num_attempts: u32,
    pub num_rows: u64,
}

#[pymethods]
impl MergeResult {
    pub fn __repr__(&self) -> String {
        format!(
            "MergeResult(version={}, num_updated_rows={}, num_inserted_rows={}, num_deleted_rows={}, num_attempts={}, num_rows={})",
            self.version,
            self.num_updated_rows,
            self.num_inserted_rows,
            self.num_deleted_rows,
            self.num_attempts,
            self.num_rows
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
            num_attempts: result.num_attempts,
            num_rows: result.num_rows,
        }
    }
}

/// Specification selecting Lance's MemWAL LSM-style write path for
/// `merge_insert`.
///
/// Constructed via the `bucket(...)`, `identity(...)`, or `unsharded()`
/// classmethods, then optionally chain `with_maintained_indexes(...)` and
/// `with_writer_config_defaults(...)`.
#[pyclass(from_py_object)]
#[derive(Clone, Debug)]
pub struct LsmWriteSpec {
    inner: lancedb::table::LsmWriteSpec,
}

#[pymethods]
impl LsmWriteSpec {
    /// Hash-bucket sharding by the unenforced primary key column.
    #[staticmethod]
    pub fn bucket(column: String, num_buckets: u32) -> Self {
        Self {
            inner: lancedb::table::LsmWriteSpec::bucket(column, num_buckets),
        }
    }

    /// Identity sharding — shard by the raw value of `column`.
    ///
    /// `column` must be a deterministic function of the unenforced primary
    /// key: every row with a given primary key must always produce the same
    /// `column` value, or upserts of that key can land in different shards
    /// and a stale version can win. Typically `column` is the primary key
    /// itself or a stable attribute of it.
    #[staticmethod]
    pub fn identity(column: String) -> Self {
        Self {
            inner: lancedb::table::LsmWriteSpec::identity(column),
        }
    }

    /// No sharding — every `merge_insert` call writes to a single
    /// MemWAL shard.
    #[staticmethod]
    pub fn unsharded() -> Self {
        Self {
            inner: lancedb::table::LsmWriteSpec::unsharded(),
        }
    }

    /// Replace the list of indexes the MemWAL should keep up to date as
    /// rows are appended. Each name must reference an index that
    /// already exists on the table at the time `set_lsm_write_spec`
    /// is called.
    pub fn with_maintained_indexes(&self, indexes: Vec<String>) -> Self {
        Self {
            inner: self.inner.clone().with_maintained_indexes(indexes),
        }
    }

    /// Replace the default `ShardWriter` configuration recorded in the
    /// MemWAL index, so every writer starts from the same defaults.
    pub fn with_writer_config_defaults(&self, defaults: HashMap<String, String>) -> Self {
        Self {
            inner: self.inner.clone().with_writer_config_defaults(defaults),
        }
    }

    pub fn __repr__(&self) -> String {
        match &self.inner {
            lancedb::table::LsmWriteSpec::Bucket {
                column,
                num_buckets,
                maintained_indexes,
                writer_config_defaults,
            } => format!(
                "LsmWriteSpec.bucket(column={:?}, num_buckets={}, maintained_indexes={:?}, writer_config_defaults={:?})",
                column, num_buckets, maintained_indexes, writer_config_defaults,
            ),
            lancedb::table::LsmWriteSpec::Identity {
                column,
                maintained_indexes,
                writer_config_defaults,
            } => format!(
                "LsmWriteSpec.identity(column={:?}, maintained_indexes={:?}, writer_config_defaults={:?})",
                column, maintained_indexes, writer_config_defaults,
            ),
            lancedb::table::LsmWriteSpec::Unsharded {
                maintained_indexes,
                writer_config_defaults,
            } => format!(
                "LsmWriteSpec.unsharded(maintained_indexes={:?}, writer_config_defaults={:?})",
                maintained_indexes, writer_config_defaults,
            ),
        }
    }

    /// Discriminator string identifying the variant ("bucket", "identity",
    /// or "unsharded").
    #[getter]
    pub fn spec_type(&self) -> &'static str {
        match &self.inner {
            lancedb::table::LsmWriteSpec::Bucket { .. } => "bucket",
            lancedb::table::LsmWriteSpec::Identity { .. } => "identity",
            lancedb::table::LsmWriteSpec::Unsharded { .. } => "unsharded",
        }
    }

    /// Bucket and identity variants: the sharding column. `None` for unsharded.
    #[getter]
    pub fn column(&self) -> Option<String> {
        match &self.inner {
            lancedb::table::LsmWriteSpec::Bucket { column, .. }
            | lancedb::table::LsmWriteSpec::Identity { column, .. } => Some(column.clone()),
            lancedb::table::LsmWriteSpec::Unsharded { .. } => None,
        }
    }

    /// Bucket variant only: the number of buckets.
    #[getter]
    pub fn num_buckets(&self) -> Option<u32> {
        match &self.inner {
            lancedb::table::LsmWriteSpec::Bucket { num_buckets, .. } => Some(*num_buckets),
            _ => None,
        }
    }

    /// Names of indexes the MemWAL should keep up to date during writes.
    #[getter]
    pub fn maintained_indexes(&self) -> Vec<String> {
        self.inner.maintained_indexes().to_vec()
    }

    /// Default `ShardWriter` configuration recorded by this spec.
    #[getter]
    pub fn writer_config_defaults(&self) -> HashMap<String, String> {
        self.inner.writer_config_defaults().clone()
    }
}

impl From<LsmWriteSpec> for lancedb::table::LsmWriteSpec {
    fn from(spec: LsmWriteSpec) -> Self {
        spec.inner
    }
}

#[pyclass(get_all, from_py_object)]
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

#[pyclass(get_all, from_py_object)]
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

#[pyclass(get_all, from_py_object)]
#[derive(Clone, Debug)]
pub struct UpdateFieldMetadataResult {
    pub version: u64,
}

#[pymethods]
impl UpdateFieldMetadataResult {
    pub fn __repr__(&self) -> String {
        format!("UpdateFieldMetadataResult(version={})", self.version)
    }
}

impl From<lancedb::table::UpdateFieldMetadataResult> for UpdateFieldMetadataResult {
    fn from(result: lancedb::table::UpdateFieldMetadataResult) -> Self {
        Self {
            version: result.version,
        }
    }
}

#[pyclass(get_all, from_py_object)]
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
    pub(crate) fn inner_ref(&self) -> PyResult<&LanceDbTable> {
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

    pub fn database(&self) -> PyResult<Connection> {
        let inner = self.inner_ref()?.clone();
        let inner_connection =
            lancedb::Connection::new(inner.database().clone(), inner.embedding_registry().clone());
        Ok(Connection::new(inner_connection))
    }

    pub fn schema(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let schema = inner.schema().await.infer_error()?;
            Python::attach(|py| schema.to_pyarrow(py).map(|obj| obj.unbind()))
        })
    }

    #[pyo3(signature = (data, mode, progress=None))]
    pub fn add<'a>(
        self_: PyRef<'a, Self>,
        data: PyScannable,
        mode: String,
        progress: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let mut op = self_.inner_ref()?.add(data);
        if mode == "append" {
            op = op.mode(AddDataMode::Append);
        } else if mode == "overwrite" {
            op = op.mode(AddDataMode::Overwrite);
        } else {
            return Err(PyValueError::new_err(format!("Invalid mode: {}", mode)));
        }
        if let Some(progress_obj) = progress {
            let is_callable = Python::attach(|py| progress_obj.bind(py).is_callable());
            if is_callable {
                // Callback: call with a dict of progress info.
                op = op.progress(move |p| {
                    Python::attach(|py| {
                        let dict = PyDict::new(py);
                        if let Err(e) = dict
                            .set_item("output_rows", p.output_rows())
                            .and_then(|_| dict.set_item("output_bytes", p.output_bytes()))
                            .and_then(|_| dict.set_item("total_rows", p.total_rows()))
                            .and_then(|_| {
                                dict.set_item("elapsed_seconds", p.elapsed().as_secs_f64())
                            })
                            .and_then(|_| dict.set_item("active_tasks", p.active_tasks()))
                            .and_then(|_| dict.set_item("total_tasks", p.total_tasks()))
                            .and_then(|_| dict.set_item("done", p.done()))
                        {
                            log::warn!("progress dict error: {e}");
                            return;
                        }
                        if let Err(e) = progress_obj.call1(py, (dict,)) {
                            log::warn!("progress callback error: {e}");
                        }
                    });
                });
            } else {
                // tqdm-like: has update() method.
                let mut last_rows: usize = 0;
                let mut total_set = false;
                op = op.progress(move |p| {
                    let current = p.output_rows();
                    let prev = last_rows;
                    last_rows = current;
                    Python::attach(|py| {
                        if let Some(total) = p.total_rows()
                            && !total_set
                        {
                            if let Err(e) = progress_obj.setattr(py, "total", total) {
                                log::warn!("progress setattr error: {e}");
                            }
                            total_set = true;
                        }
                        let delta = current.saturating_sub(prev);
                        if delta > 0 {
                            if let Err(e) = progress_obj.call_method1(py, "update", (delta,)) {
                                log::warn!("progress update error: {e}");
                            }
                            // Show throughput and active workers in tqdm postfix.
                            let elapsed = p.elapsed().as_secs_f64();
                            if elapsed > 0.0 {
                                let mb_per_sec = p.output_bytes() as f64 / elapsed / 1_000_000.0;
                                let postfix = format!(
                                    "{:.1} MB/s | {}/{} workers",
                                    mb_per_sec,
                                    p.active_tasks(),
                                    p.total_tasks()
                                );
                                if let Err(e) =
                                    progress_obj.call_method1(py, "set_postfix_str", (postfix,))
                                {
                                    log::warn!("progress set_postfix_str error: {e}");
                                }
                            }
                        }
                        if p.done() {
                            // Force a final refresh so the bar shows completion.
                            if let Err(e) = progress_obj.call_method0(py, "refresh") {
                                log::warn!("progress refresh error: {e}");
                            }
                        }
                    });
                });
            }
        }

        future_into_py(self_.py(), async move {
            let result = op.execute().await.infer_error()?;
            Ok(AddResult::from(result))
        })
    }

    #[allow(private_interfaces)]
    pub fn delete(self_: PyRef<'_, Self>, condition: PredicateArg) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let result = match &condition {
                PredicateArg::Expr(e) => inner.delete(&e.0).await,
                PredicateArg::Sql(s) => inner.delete(s.as_str()).await,
            }
            .infer_error()?;
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

    pub fn prewarm_data(
        self_: PyRef<'_, Self>,
        columns: Option<Vec<String>>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.prewarm_data(columns).await.infer_error()?;
            Ok(())
        })
    }

    pub fn list_indices(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let indices = inner.list_indices().await.infer_error()?;
            Python::attach(|py| {
                Ok(indices
                    .into_iter()
                    .map(|idx| IndexConfig::from_lancedb(py, idx))
                    .collect::<Vec<_>>())
            })
        })
    }

    pub fn index_stats(self_: PyRef<'_, Self>, index_name: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let stats = inner.index_stats(&index_name).await.infer_error()?;
            if let Some(stats) = stats {
                Python::attach(|py| {
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
            Python::attach(|py| {
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

    pub fn uri(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move { inner.uri().await.infer_error() })
    }

    pub fn initial_storage_options(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            Ok(inner.initial_storage_options().await)
        })
    }

    pub fn latest_storage_options(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.latest_storage_options().await.infer_error()
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
            Python::attach(|py| {
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
            })
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

    pub fn current_branch(&self) -> PyResult<Option<String>> {
        Ok(self.inner_ref()?.current_branch())
    }

    #[getter]
    pub fn branches(&self) -> PyResult<Branches> {
        Ok(Branches::new(self.inner_ref()?.clone()))
    }

    #[pyo3(signature = (offsets))]
    pub fn take_offsets(self_: PyRef<'_, Self>, offsets: Vec<u64>) -> PyResult<TakeQuery> {
        Ok(TakeQuery::new(
            self_.inner_ref()?.clone().take_offsets(offsets),
        ))
    }

    #[pyo3(signature = (row_ids))]
    pub fn take_row_ids(self_: PyRef<'_, Self>, row_ids: Vec<u64>) -> PyResult<TakeQuery> {
        Ok(TakeQuery::new(
            self_.inner_ref()?.clone().take_row_ids(row_ids),
        ))
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
            if let Some(e) = parameters.when_not_matched_by_source_condition_expr {
                builder.when_not_matched_by_source_delete_expr(e.0);
            } else {
                builder.when_not_matched_by_source_delete(
                    parameters.when_not_matched_by_source_condition,
                );
            }
        }
        if let Some(timeout) = parameters.timeout {
            builder.timeout(timeout);
        }
        if let Some(use_index) = parameters.use_index {
            builder.use_index(use_index);
        }
        if let Some(use_lsm_write) = parameters.use_lsm_write {
            builder.use_lsm_write(use_lsm_write);
        }
        if let Some(validate_single_shard) = parameters.validate_single_shard {
            builder.validate_single_shard(validate_single_shard);
        }

        future_into_py(self_.py(), async move {
            let res = builder.execute(Box::new(batches)).await.infer_error()?;
            Ok(MergeResult::from(res))
        })
    }

    pub fn set_unenforced_primary_key<'a>(
        self_: PyRef<'a, Self>,
        columns: Vec<String>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner
                .set_unenforced_primary_key(columns)
                .await
                .infer_error()
        })
    }

    pub fn set_lsm_write_spec<'a>(
        self_: PyRef<'a, Self>,
        spec: LsmWriteSpec,
    ) -> PyResult<Bound<'a, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        let native_spec = lancedb::table::LsmWriteSpec::from(spec);
        future_into_py(self_.py(), async move {
            inner.set_lsm_write_spec(native_spec).await.infer_error()
        })
    }

    pub fn unset_lsm_write_spec(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.unset_lsm_write_spec().await.infer_error()
        })
    }

    pub fn close_lsm_writers(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.close_lsm_writers().await.infer_error()
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

    #[pyo3(signature = (columns, where_clause=None, num_workers=None, max_workers=None))]
    pub fn refresh_column(
        self_: PyRef<'_, Self>,
        columns: Vec<String>,
        where_clause: Option<String>,
        num_workers: Option<u32>,
        max_workers: Option<u32>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner
                .refresh_column(&columns, where_clause, num_workers, max_workers)
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
        // Deprecated: forwards to the update_field_metadata path (replace mode).
        let mut update = FieldMetadataUpdate::new(field_name).replace();
        for (key, value) in metadata.into_iter() {
            update = update.set(key.extract::<String>()?, value.extract::<String>()?);
        }

        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            inner.update_field_metadata(&[update]).await.infer_error()?;
            Ok(())
        })
    }

    pub fn update_field_metadata<'a>(
        self_: PyRef<'a, Self>,
        updates: Vec<Bound<PyDict>>,
    ) -> PyResult<Bound<'a, PyAny>> {
        let updates = updates
            .iter()
            .map(|update| {
                let path: String = update
                    .get_item("path")?
                    .ok_or_else(|| PyValueError::new_err("Missing path"))?
                    .extract()?;
                let mut field_update = FieldMetadataUpdate::new(path);
                if let Some(metadata) = update.get_item("metadata")? {
                    let metadata_dict = metadata.cast::<PyDict>()?;
                    for (key, value) in metadata_dict.iter() {
                        let key: String = key.extract()?;
                        if value.is_none() {
                            field_update = field_update.remove(key);
                        } else {
                            field_update = field_update.set(key, value.extract::<String>()?);
                        }
                    }
                }
                if let Some(replace) = update.get_item("replace")?
                    && replace.extract::<bool>()?
                {
                    field_update = field_update.replace();
                }
                Ok(field_update)
            })
            .collect::<PyResult<Vec<_>>>()?;

        let inner = self_.inner_ref()?.clone();
        future_into_py(self_.py(), async move {
            let result = inner.update_field_metadata(&updates).await.infer_error()?;
            Ok(UpdateFieldMetadataResult::from(result))
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
    when_not_matched_by_source_condition_expr: Option<PyExpr>,
    timeout: Option<std::time::Duration>,
    use_index: Option<bool>,
    use_lsm_write: Option<bool>,
    validate_single_shard: Option<bool>,
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

            Python::attach(|py| {
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

#[pyclass]
pub struct Branches {
    inner: LanceDbTable,
}

impl Branches {
    pub fn new(table: LanceDbTable) -> Self {
        Self { inner: table }
    }
}

#[pymethods]
impl Branches {
    pub fn list(self_: PyRef<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let res = inner.list_branches().await.infer_error()?;
            Python::attach(|py| {
                let py_dict = PyDict::new(py);
                for (name, contents) in res {
                    let value = PyDict::new(py);
                    value.set_item("parent_branch", contents.parent_branch)?;
                    value.set_item("parent_version", contents.parent_version)?;
                    value.set_item("manifest_size", contents.manifest_size)?;
                    py_dict.set_item(name, value)?;
                }
                Ok(py_dict.unbind())
            })
        })
    }

    #[pyo3(signature = (name, from_ref=None, from_version=None))]
    pub fn create(
        self_: PyRef<'_, Self>,
        name: String,
        from_ref: Option<String>,
        from_version: Option<u64>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let from = Ref::Version(from_ref, from_version);
            let table = inner.create_branch(&name, from).await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    #[pyo3(signature = (name, version=None))]
    pub fn checkout(
        self_: PyRef<'_, Self>,
        name: String,
        version: Option<u64>,
    ) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            let table = inner.checkout_branch(&name, version).await.infer_error()?;
            Ok(Table::new(table))
        })
    }

    pub fn delete(self_: PyRef<'_, Self>, name: String) -> PyResult<Bound<'_, PyAny>> {
        let inner = self_.inner.clone();
        future_into_py(self_.py(), async move {
            inner.delete_branch(&name).await.infer_error()?;
            Ok(())
        })
    }
}
