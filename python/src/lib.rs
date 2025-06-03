// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow::RecordBatchStream;
use connection::{connect, Connection};
use env_logger::Env;
use index::IndexConfig;
use pyo3::{
    pymodule,
    types::{PyModule, PyModuleMethods},
    wrap_pyfunction, Bound, PyResult, Python,
};
use query::{FTSQuery, HybridQuery, Query, VectorQuery};
use table::{
    AddColumnsResult, AddResult, AlterColumnsResult, DeleteResult, DropColumnsResult, MergeResult,
    Table, UpdateResult,
};

use crate::query::{PyFullTextQuery, PyFullTextSearchQuery};

pub mod arrow;
pub mod connection;
pub mod error;
pub mod index;
pub mod query;
pub mod table;
pub mod util;

#[pymodule]
pub fn _lancedb(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    let env = Env::new()
        .filter_or("LANCEDB_LOG", "warn")
        .write_style("LANCEDB_LOG_STYLE");
    env_logger::init_from_env(env);
    m.add_class::<Connection>()?;
    m.add_class::<Table>()?;
    m.add_class::<IndexConfig>()?;
    m.add_class::<Query>()?;
    m.add_class::<FTSQuery>()?;
    m.add_class::<HybridQuery>()?;
    m.add_class::<VectorQuery>()?;
    m.add_class::<RecordBatchStream>()?;
    m.add_class::<AddColumnsResult>()?;
    m.add_class::<AlterColumnsResult>()?;
    m.add_class::<AddResult>()?;
    m.add_class::<MergeResult>()?;
    m.add_class::<DeleteResult>()?;
    m.add_class::<DropColumnsResult>()?;
    m.add_class::<UpdateResult>()?;
    m.add_class::<PyFullTextQuery>()?;
    m.add_class::<PyFullTextSearchQuery>()?;
    m.add_function(wrap_pyfunction!(connect, m)?)?;
    m.add_function(wrap_pyfunction!(util::validate_table_name, m)?)?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
