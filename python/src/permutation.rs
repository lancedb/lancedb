// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::{Arc, Mutex};

use crate::{error::PythonErrorExt, table::Table};
use lancedb::dataloader::{
    permutation::builder::{PermutationBuilder as LancePermutationBuilder, ShuffleStrategy},
    permutation::split::{SplitSizes, SplitStrategy},
};
use pyo3::{
    exceptions::PyRuntimeError, pyclass, pymethods, types::PyAnyMethods, Bound, PyAny, PyRefMut,
    PyResult,
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
    #[pyo3(signature = (*, ratios=None, counts=None, fixed=None, seed=None))]
    pub fn split_random(
        slf: PyRefMut<'_, Self>,
        ratios: Option<Vec<f64>>,
        counts: Option<Vec<u64>>,
        fixed: Option<u64>,
        seed: Option<u64>,
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

        slf.modify(|builder| builder.with_split_strategy(SplitStrategy::Random { seed, sizes }))
    }

    #[pyo3(signature = (columns, split_weights, *, discard_weight=0))]
    pub fn split_hash(
        slf: PyRefMut<'_, Self>,
        columns: Vec<String>,
        split_weights: Vec<u64>,
        discard_weight: u64,
    ) -> PyResult<Self> {
        slf.modify(|builder| {
            builder.with_split_strategy(SplitStrategy::Hash {
                columns,
                split_weights,
                discard_weight,
            })
        })
    }

    #[pyo3(signature = (*, ratios=None, counts=None, fixed=None))]
    pub fn split_sequential(
        slf: PyRefMut<'_, Self>,
        ratios: Option<Vec<f64>>,
        counts: Option<Vec<u64>>,
        fixed: Option<u64>,
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

        slf.modify(|builder| builder.with_split_strategy(SplitStrategy::Sequential { sizes }))
    }

    pub fn split_calculated(slf: PyRefMut<'_, Self>, calculation: String) -> PyResult<Self> {
        slf.modify(|builder| builder.with_split_strategy(SplitStrategy::Calculated { calculation }))
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
