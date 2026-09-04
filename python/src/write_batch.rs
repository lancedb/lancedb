// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Exposes the Rust write-path coercion to the Python bindings.
//!
//! Python used to reimplement all of this (blob structs, JSON labelling, null columns,
//! struct alignment) because `merge_insert` and `merge` hand Lance a reader directly and so
//! never reach the coercion `add` gets. These two functions let those paths run the same
//! code instead of a second copy of it.

use arrow::array::RecordBatch;
use arrow::datatypes::Schema;
use arrow::pyarrow::PyArrowType;
use lancedb::table::datafusion::write_batch;
use pyo3::{PyResult, pyfunction};

use crate::error::PythonErrorExt;

/// The schema that batches take on once [`coerce_batch`] has coerced them.
#[pyfunction]
pub fn write_schema_for(
    input_schema: PyArrowType<Schema>,
    table_schema: PyArrowType<Schema>,
) -> PyResult<PyArrowType<Schema>> {
    let write_schema =
        write_batch::write_schema_for(input_schema.0.into(), &table_schema.0).infer_error()?;
    Ok(PyArrowType((*write_schema).clone()))
}

/// Coerce one batch into `write_schema`, which must have come from [`write_schema_for`].
#[pyfunction]
pub fn coerce_batch(
    batch: PyArrowType<RecordBatch>,
    write_schema: PyArrowType<Schema>,
) -> PyResult<PyArrowType<RecordBatch>> {
    let coerced = write_batch::coerce_batch(batch.0, &write_schema.0).infer_error()?;
    Ok(PyArrowType(coerced))
}
