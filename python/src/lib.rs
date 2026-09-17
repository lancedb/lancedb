// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow::RecordBatchStream;
use connection::{Connection, connect, connect_namespace, connect_namespace_client};
use env_logger::Env;
use expr::{PyExpr, expr_col, expr_func, expr_lit};
use index::IndexConfig;
use permutation::{PyAsyncPermutationBuilder, PyPermutationReader};
use pyo3::{
    Bound, PyResult, Python, pyfunction, pymodule,
    types::{PyAnyMethods, PyModule, PyModuleMethods},
    wrap_pyfunction,
};
use query::{FTSQuery, HybridQuery, Query, VectorQuery};
use session::Session;
use table::{
    AddColumnsResult, AddResult, AlterColumnsResult, DeleteResult, DropColumnsResult, FtsToken,
    LsmWriteSpec, MergeResult, PyBlobFile, RefreshColumnResult, RefreshMaterializedViewResult,
    Table, UpdateFieldMetadataResult, UpdateResult,
};

pub mod arrow;
pub mod catalog;
pub mod connection;
pub mod error;
pub mod expr;
pub mod header;
pub mod index;
pub mod job;
pub mod namespace;
pub mod oauth;
pub mod otel;
pub mod permutation;
pub mod query;
pub mod runtime;
pub mod session;
pub mod sql;
pub mod table;
pub mod util;

/// Shut down the shared Tokio runtime (see `runtime::shutdown`).
///
/// Registered below as a Python `atexit` callback rather than called
/// directly: `atexit` runs while the interpreter is still fully valid,
/// which is the coordinated, bounded exit the runtime otherwise never gets.
///
/// Runs the actual wait with the GIL released (`Python::detach`): shutdown
/// blocks the calling thread waiting on the runtime's own worker threads,
/// and if any in-flight task needs the GIL to finish (e.g. one that calls
/// back into Python), holding it here while waiting on that same task would
/// deadlock rather than time out.
#[pyfunction]
fn shutdown_runtime(py: Python<'_>) {
    py.detach(|| runtime::shutdown(std::time::Duration::from_secs(5)));
}

#[pymodule]
pub fn _lancedb(py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let env = Env::new()
        .filter_or("LANCEDB_LOG", "warn")
        .write_style("LANCEDB_LOG_STYLE");
    env_logger::init_from_env(env);
    m.add_class::<Connection>()?;
    m.add_class::<catalog::Catalog>()?;
    m.add_function(wrap_pyfunction!(catalog::connect_catalog, m)?)?;
    m.add_class::<Session>()?;
    m.add_class::<Table>()?;
    m.add_class::<crate::oauth::PyOAuthSession>()?;
    m.add_class::<crate::oauth::PySessionStatus>()?;
    m.add_class::<crate::oauth::PySessionLogout>()?;
    m.add_class::<crate::job::Job>()?;
    m.add_class::<crate::job::JobInfo>()?;
    m.add_class::<crate::job::JobDescription>()?;
    m.add_class::<crate::job::JobFailureInfo>()?;
    m.add_class::<crate::sql::Query>()?;
    m.add_class::<crate::sql::QueryDescription>()?;
    m.add_class::<PyBlobFile>()?;
    m.add_class::<IndexConfig>()?;
    m.add_class::<Query>()?;
    m.add_class::<FTSQuery>()?;
    m.add_class::<HybridQuery>()?;
    m.add_class::<VectorQuery>()?;
    m.add_class::<RecordBatchStream>()?;
    m.add_class::<AddColumnsResult>()?;
    m.add_class::<RefreshColumnResult>()?;
    m.add_class::<RefreshMaterializedViewResult>()?;
    m.add_class::<AlterColumnsResult>()?;
    m.add_class::<UpdateFieldMetadataResult>()?;
    m.add_class::<AddResult>()?;
    m.add_class::<MergeResult>()?;
    m.add_class::<LsmWriteSpec>()?;
    m.add_class::<DeleteResult>()?;
    m.add_class::<DropColumnsResult>()?;
    m.add_class::<UpdateResult>()?;
    m.add_class::<FtsToken>()?;
    m.add_class::<PyAsyncPermutationBuilder>()?;
    m.add_class::<PyPermutationReader>()?;
    m.add_class::<PyExpr>()?;
    // OpenTelemetry metrics bridge
    m.add_class::<otel::PyMetricPoint>()?;
    m.add_class::<otel::PyMetricDescription>()?;
    m.add_function(wrap_pyfunction!(
        otel::register_lancedb_metrics_recorder,
        m
    )?)?;
    m.add_function(wrap_pyfunction!(otel::lancedb_metrics_catalog, m)?)?;
    m.add_function(wrap_pyfunction!(otel::snapshot_lancedb_metrics, m)?)?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(connect_namespace, m)?)?;
    m.add_function(wrap_pyfunction!(connect_namespace_client, m)?)?;
    m.add_function(wrap_pyfunction!(table::tokenize, m)?)?;
    m.add_function(wrap_pyfunction!(permutation::async_permutation_builder, m)?)?;
    m.add_function(wrap_pyfunction!(util::validate_table_name, m)?)?;
    m.add_function(wrap_pyfunction!(query::fts_query_to_json, m)?)?;
    m.add_function(wrap_pyfunction!(expr_col, m)?)?;
    m.add_function(wrap_pyfunction!(expr_lit, m)?)?;
    m.add_function(wrap_pyfunction!(expr_func, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    // Give the shared runtime a coordinated, bounded shutdown at normal
    // process exit -- see `shutdown_runtime` and `runtime::shutdown` for why.
    py.import("atexit")?
        .call_method1("register", (wrap_pyfunction!(shutdown_runtime, m)?,))?;
    Ok(())
}
