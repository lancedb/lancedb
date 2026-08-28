// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow::RecordBatchStream;
use connection::{Connection, connect, connect_namespace, connect_namespace_client};
use env_logger::Env;
use expr::{PyExpr, expr_col, expr_func, expr_lit};
use index::IndexConfig;
use permutation::{PyAsyncPermutationBuilder, PyPermutationReader};
use pyo3::{
    Bound, PyResult, Python, pymodule,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction,
};
#[cfg(not(feature = "remote"))]
use pyo3::{PyAny, pyfunction};

#[cfg(not(feature = "remote"))]
#[pyfunction(name = "sql")]
#[pyo3(signature = (
    query,
    *,
    default_database="lancedb",
    default_namespace_path=None,
    api_key=None,
    region="us-east-1",
    host_override=None,
    flight_sql_uri=None,
    client_config=None,
    storage_options=None,
))]
#[allow(clippy::too_many_arguments)]
fn sql_unavailable(
    query: String,
    default_database: &str,
    default_namespace_path: Option<Bound<'_, PyAny>>,
    api_key: Option<String>,
    region: &str,
    host_override: Option<String>,
    flight_sql_uri: Option<String>,
    client_config: Option<Bound<'_, PyAny>>,
    storage_options: Option<std::collections::HashMap<String, String>>,
) -> PyResult<()> {
    let _ = (
        query,
        default_database,
        default_namespace_path,
        api_key,
        region,
        host_override,
        flight_sql_uri,
        client_config,
        storage_options,
    );
    Err(pyo3::exceptions::PyNotImplementedError::new_err(
        "lancedb.sql requires the remote feature",
    ))
}
use query::{FTSQuery, HybridQuery, Query, VectorQuery};
use session::Session;
use table::{
    AddColumnsResult, AddResult, AlterColumnsResult, DeleteResult, DropColumnsResult, FtsToken,
    LsmWriteSpec, MergeResult, PyBlobFile, RefreshColumnResult, RefreshMaterializedViewResult,
    Table, UpdateFieldMetadataResult, UpdateResult,
};

pub mod arrow;
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
#[cfg(feature = "remote")]
pub mod sql;
pub mod table;
pub mod util;

#[pymodule]
pub fn _lancedb(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let env = Env::new()
        .filter_or("LANCEDB_LOG", "warn")
        .write_style("LANCEDB_LOG_STYLE");
    env_logger::init_from_env(env);
    m.add_class::<Connection>()?;
    m.add_class::<Session>()?;
    m.add_class::<Table>()?;
    m.add_class::<crate::job::Job>()?;
    m.add_class::<crate::job::JobInfo>()?;
    m.add_class::<crate::job::JobDescription>()?;
    m.add_class::<crate::job::JobFailureInfo>()?;
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
    #[cfg(feature = "remote")]
    m.add_function(wrap_pyfunction!(sql::sql, m)?)?;
    #[cfg(not(feature = "remote"))]
    m.add_function(wrap_pyfunction!(sql_unavailable, m)?)?;
    m.add_function(wrap_pyfunction!(table::tokenize, m)?)?;
    m.add_function(wrap_pyfunction!(permutation::async_permutation_builder, m)?)?;
    m.add_function(wrap_pyfunction!(util::validate_table_name, m)?)?;
    m.add_function(wrap_pyfunction!(query::fts_query_to_json, m)?)?;
    m.add_function(wrap_pyfunction!(expr_col, m)?)?;
    m.add_function(wrap_pyfunction!(expr_lit, m)?)?;
    m.add_function(wrap_pyfunction!(expr_func, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
