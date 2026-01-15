// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::{Arc, Mutex};

use crate::{
    arrow::RecordBatchStream, connection::Connection, error::PythonErrorExt, table::Table,
};
use arrow::pyarrow::ToPyArrow;
use lancedb::{
    dataloader::permutation::{
        builder::{PermutationBuilder as LancePermutationBuilder, ShuffleStrategy},
        reader::PermutationReader,
        split::{SplitSizes, SplitStrategy},
    },
    query::Select,
};
use pyo3::{
    exceptions::PyRuntimeError,
    pyclass, pymethods,
    types::{PyAnyMethods, PyDict, PyDictMethods, PyType},
    Bound, PyAny, PyRef, PyRefMut, PyResult, Python,
};
use pyo3_async_runtimes::tokio::future_into_py;

/// Create a permutation builder for the given table
#[pyo3::pyfunction]
pub fn async_permutation_builder(table: Bound<'_, PyAny>) -> PyResult<PyAsyncPermutationBuilder> {
    let table = table.getattr("_inner")?.downcast_into::<Table>()?;
    let inner_table = table.borrow().inner_ref()?.clone();
    let inner_builder = LancePermutationBuilder::new(inner_table);

    Ok(PyAsyncPermutationBuilder {
        state: Arc::new(Mutex::new(PyAsyncPermutationBuilderState {
            builder: Some(inner_builder),
        })),
    })
}

struct PyAsyncPermutationBuilderState {
    builder: Option<LancePermutationBuilder>,
}

#[pyclass(name = "AsyncPermutationBuilder")]
pub struct PyAsyncPermutationBuilder {
    state: Arc<Mutex<PyAsyncPermutationBuilderState>>,
}

impl PyAsyncPermutationBuilder {
    fn modify(
        &self,
        func: impl FnOnce(LancePermutationBuilder) -> LancePermutationBuilder,
    ) -> PyResult<Self> {
        let mut state = self.state.lock().unwrap();
        let builder = state
            .builder
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Builder already consumed"))?;
        state.builder = Some(func(builder));
        Ok(Self {
            state: self.state.clone(),
        })
    }
}

#[pymethods]
impl PyAsyncPermutationBuilder {
    #[pyo3(signature = (database, table_name))]
    pub fn persist(
        slf: PyRefMut<'_, Self>,
        database: Bound<'_, PyAny>,
        table_name: String,
    ) -> PyResult<Self> {
        let conn = if database.hasattr("_conn")? {
            database
                .getattr("_conn")?
                .getattr("_inner")?
                .downcast_into::<Connection>()?
        } else {
            database.getattr("_inner")?.downcast_into::<Connection>()?
        };
        let database = conn.borrow().database()?;
        slf.modify(|builder| builder.persist(database, table_name))
    }

    #[pyo3(signature = (*, ratios=None, counts=None, fixed=None, seed=None, split_names=None))]
    pub fn split_random(
        slf: PyRefMut<'_, Self>,
        ratios: Option<Vec<f64>>,
        counts: Option<Vec<u64>>,
        fixed: Option<u64>,
        seed: Option<u64>,
        split_names: Option<Vec<String>>,
    ) -> PyResult<Self> {
        // Check that exactly one split type is provided
        let split_args_count = [ratios.is_some(), counts.is_some(), fixed.is_some()]
            .iter()
            .filter(|&&x| x)
            .count();

        if split_args_count != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Exactly one of 'ratios', 'counts', or 'fixed' must be provided",
            ));
        }

        let sizes = if let Some(ratios) = ratios {
            SplitSizes::Percentages(ratios)
        } else if let Some(counts) = counts {
            SplitSizes::Counts(counts)
        } else if let Some(fixed) = fixed {
            SplitSizes::Fixed(fixed)
        } else {
            unreachable!("One of the split arguments must be provided");
        };

        slf.modify(|builder| {
            builder.with_split_strategy(SplitStrategy::Random { seed, sizes }, split_names)
        })
    }

    #[pyo3(signature = (columns, split_weights, *, discard_weight=0, split_names=None))]
    pub fn split_hash(
        slf: PyRefMut<'_, Self>,
        columns: Vec<String>,
        split_weights: Vec<u64>,
        discard_weight: u64,
        split_names: Option<Vec<String>>,
    ) -> PyResult<Self> {
        slf.modify(|builder| {
            builder.with_split_strategy(
                SplitStrategy::Hash {
                    columns,
                    split_weights,
                    discard_weight,
                },
                split_names,
            )
        })
    }

    #[pyo3(signature = (*, ratios=None, counts=None, fixed=None, split_names=None))]
    pub fn split_sequential(
        slf: PyRefMut<'_, Self>,
        ratios: Option<Vec<f64>>,
        counts: Option<Vec<u64>>,
        fixed: Option<u64>,
        split_names: Option<Vec<String>>,
    ) -> PyResult<Self> {
        // Check that exactly one split type is provided
        let split_args_count = [ratios.is_some(), counts.is_some(), fixed.is_some()]
            .iter()
            .filter(|&&x| x)
            .count();

        if split_args_count != 1 {
            return Err(pyo3::exceptions::PyValueError::new_err(
                "Exactly one of 'ratios', 'counts', or 'fixed' must be provided",
            ));
        }

        let sizes = if let Some(ratios) = ratios {
            SplitSizes::Percentages(ratios)
        } else if let Some(counts) = counts {
            SplitSizes::Counts(counts)
        } else if let Some(fixed) = fixed {
            SplitSizes::Fixed(fixed)
        } else {
            unreachable!("One of the split arguments must be provided");
        };

        slf.modify(|builder| {
            builder.with_split_strategy(SplitStrategy::Sequential { sizes }, split_names)
        })
    }

    pub fn split_calculated(
        slf: PyRefMut<'_, Self>,
        calculation: String,
        split_names: Option<Vec<String>>,
    ) -> PyResult<Self> {
        slf.modify(|builder| {
            builder.with_split_strategy(SplitStrategy::Calculated { calculation }, split_names)
        })
    }

    pub fn shuffle(
        slf: PyRefMut<'_, Self>,
        seed: Option<u64>,
        clump_size: Option<u64>,
    ) -> PyResult<Self> {
        slf.modify(|builder| {
            builder.with_shuffle_strategy(ShuffleStrategy::Random { seed, clump_size })
        })
    }

    pub fn filter(slf: PyRefMut<'_, Self>, filter: String) -> PyResult<Self> {
        slf.modify(|builder| builder.with_filter(filter))
    }

    pub fn execute(slf: PyRefMut<'_, Self>) -> PyResult<Bound<'_, PyAny>> {
        let mut state = slf.state.lock().unwrap();
        let builder = state
            .builder
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("Builder already consumed"))?;

        future_into_py(slf.py(), async move {
            let table = builder.build().await.infer_error()?;
            Ok(Table::new(table))
        })
    }
}

#[pyclass(name = "PermutationReader")]
pub struct PyPermutationReader {
    reader: Arc<PermutationReader>,
}

impl PyPermutationReader {
    fn from_reader(reader: PermutationReader) -> Self {
        Self {
            reader: Arc::new(reader),
        }
    }

    fn parse_selection(selection: Option<Bound<'_, PyAny>>) -> PyResult<Select> {
        let Some(selection) = selection else {
            return Ok(Select::All);
        };
        let selection = selection.downcast_into::<PyDict>()?;
        let selection = selection
            .iter()
            .map(|(key, value)| {
                let key = key.extract::<String>()?;
                let value = value.extract::<String>()?;
                Ok((key, value))
            })
            .collect::<PyResult<Vec<_>>>()?;
        Ok(Select::dynamic(&selection))
    }
}

#[pymethods]
impl PyPermutationReader {
    #[classmethod]
    pub fn from_tables<'py>(
        cls: &Bound<'py, PyType>,
        base_table: Bound<'py, PyAny>,
        permutation_table: Option<Bound<'py, PyAny>>,
        split: u64,
    ) -> PyResult<Bound<'py, PyAny>> {
        let base_table = base_table.getattr("_inner")?.downcast_into::<Table>()?;
        let permutation_table = permutation_table
            .map(|p| PyResult::Ok(p.getattr("_inner")?.downcast_into::<Table>()?))
            .transpose()?;

        let base_table = base_table.borrow().inner_ref()?.base_table().clone();
        let permutation_table = permutation_table
            .map(|p| PyResult::Ok(p.borrow().inner_ref()?.base_table().clone()))
            .transpose()?;

        future_into_py(cls.py(), async move {
            let reader = if let Some(permutation_table) = permutation_table {
                PermutationReader::try_from_tables(base_table, permutation_table, split)
                    .await
                    .infer_error()?
            } else {
                PermutationReader::identity(base_table).await
            };
            Ok(Self::from_reader(reader))
        })
    }

    #[pyo3(signature = (selection=None))]
    pub fn output_schema<'py>(
        slf: PyRef<'py, Self>,
        selection: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let selection = Self::parse_selection(selection)?;
        let reader = slf.reader.clone();
        future_into_py(slf.py(), async move {
            let schema = reader.output_schema(selection).await.infer_error()?;
            Python::attach(|py| schema.as_ref().to_pyarrow(py).map(|obj| obj.unbind()))
        })
    }

    #[pyo3(signature = ())]
    pub fn count_rows<'py>(slf: PyRef<'py, Self>) -> u64 {
        slf.reader.count_rows()
    }

    #[pyo3(signature = (offset))]
    pub fn with_offset<'py>(slf: PyRef<'py, Self>, offset: u64) -> PyResult<Bound<'py, PyAny>> {
        let reader = slf.reader.as_ref().clone();
        future_into_py(slf.py(), async move {
            let reader = reader.with_offset(offset).await.infer_error()?;
            Ok(Self::from_reader(reader))
        })
    }

    #[pyo3(signature = (limit))]
    pub fn with_limit<'py>(slf: PyRef<'py, Self>, limit: u64) -> PyResult<Bound<'py, PyAny>> {
        let reader = slf.reader.as_ref().clone();
        future_into_py(slf.py(), async move {
            let reader = reader.with_limit(limit).await.infer_error()?;
            Ok(Self::from_reader(reader))
        })
    }

    #[pyo3(signature = (selection=None, *, batch_size=None))]
    pub fn read<'py>(
        slf: PyRef<'py, Self>,
        selection: Option<Bound<'py, PyAny>>,
        batch_size: Option<u32>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let selection = Self::parse_selection(selection)?;
        let reader = slf.reader.clone();
        let batch_size = batch_size.unwrap_or(1024);
        future_into_py(slf.py(), async move {
            use lancedb::query::QueryExecutionOptions;
            let mut execution_options = QueryExecutionOptions::default();
            execution_options.max_batch_length = batch_size;
            let stream = reader
                .read(selection, execution_options)
                .await
                .infer_error()?;
            Ok(RecordBatchStream::new(stream))
        })
    }
}
