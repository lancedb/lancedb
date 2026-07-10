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
use query::{FTSQuery, HybridQuery, Query, VectorQuery};
use session::Session;
use table::{
    AddColumnsResult, AddResult, AlterColumnsResult, DeleteResult, DropColumnsResult, LsmWriteSpec,
    MergeResult, PyBlobFile, Table, UpdateFieldMetadataResult, UpdateResult,
};

pub mod arrow;
pub mod connection;
pub mod error;
pub mod expr;
pub mod header;
pub mod index;
pub mod namespace;
pub mod oauth;
pub mod otel;
pub mod permutation;
pub mod query;
pub mod runtime;
pub mod session;
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
    m.add_class::<PyBlobFile>()?;
    m.add_class::<IndexConfig>()?;
    m.add_class::<Query>()?;
    m.add_class::<FTSQuery>()?;
    m.add_class::<HybridQuery>()?;
    m.add_class::<VectorQuery>()?;
    m.add_class::<RecordBatchStream>()?;
    m.add_class::<AddColumnsResult>()?;
    m.add_class::<AlterColumnsResult>()?;
    m.add_class::<UpdateFieldMetadataResult>()?;
    m.add_class::<AddResult>()?;
    m.add_class::<MergeResult>()?;
    m.add_class::<LsmWriteSpec>()?;
    m.add_class::<DeleteResult>()?;
    m.add_class::<DropColumnsResult>()?;
    m.add_class::<UpdateResult>()?;
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
    m.add_function(wrap_pyfunction!(permutation::async_permutation_builder, m)?)?;
    m.add_function(wrap_pyfunction!(util::validate_table_name, m)?)?;
    m.add_function(wrap_pyfunction!(query::fts_query_to_json, m)?)?;
    m.add_function(wrap_pyfunction!(expr_col, m)?)?;
    m.add_function(wrap_pyfunction!(expr_lit, m)?)?;
    m.add_function(wrap_pyfunction!(expr_func, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
