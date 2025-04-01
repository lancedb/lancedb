// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Mutex;

use lancedb::index::scalar::{BoostQuery, FtsQuery, MatchQuery, MultiMatchQuery, PhraseQuery};
use lancedb::DistanceType;
use pyo3::prelude::{PyAnyMethods, PyDictMethods, PyListMethods};
use pyo3::types::PyDict;
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    pyfunction, PyResult,
};
use pyo3::{Bound, PyAny};

/// A wrapper around a rust builder
///
/// Rust builders are often implemented so that the builder methods
/// consume the builder and return a new one. This is not compatible
/// with the pyo3, which, being garbage collected, cannot easily obtain
/// ownership of an object.
///
/// This wrapper converts the compile-time safety of rust into runtime
/// errors if any attempt to use the builder happens after it is consumed.
pub struct BuilderWrapper<T> {
    name: String,
    inner: Mutex<Option<T>>,
}

impl<T> BuilderWrapper<T> {
    pub fn new(name: impl AsRef<str>, inner: T) -> Self {
        Self {
            name: name.as_ref().to_string(),
            inner: Mutex::new(Some(inner)),
        }
    }

    pub fn consume<O>(&self, mod_fn: impl FnOnce(T) -> O) -> PyResult<O> {
        let mut inner = self.inner.lock().unwrap();
        let inner_builder = inner.take().ok_or_else(|| {
            PyRuntimeError::new_err(format!("{} has already been consumed", self.name))
        })?;
        let result = mod_fn(inner_builder);
        Ok(result)
    }
}

pub fn parse_distance_type(distance_type: impl AsRef<str>) -> PyResult<DistanceType> {
    match distance_type.as_ref().to_lowercase().as_str() {
        "l2" => Ok(DistanceType::L2),
        "cosine" => Ok(DistanceType::Cosine),
        "dot" => Ok(DistanceType::Dot),
        "hamming" => Ok(DistanceType::Hamming),
        _ => Err(PyValueError::new_err(format!(
            "Invalid distance type '{}'.  Must be one of l2, cosine, dot, or hamming",
            distance_type.as_ref()
        ))),
    }
}

#[pyfunction]
pub fn validate_table_name(table_name: &str) -> PyResult<()> {
    lancedb::utils::validate_table_name(table_name)
        .map_err(|e| PyValueError::new_err(e.to_string()))
}

pub fn parse_fts_query(query: &Bound<'_, PyDict>) -> PyResult<FtsQuery> {
    let query_type = query.keys().get_item(0)?.extract::<String>()?;
    let query_value = query
        .get_item(&query_type)?
        .ok_or(PyValueError::new_err(format!(
            "Query type {} not found",
            query_type
        )))?;
    let query_value = query_value.downcast::<PyDict>()?;

    match query_type.as_str() {
        "match" => {
            let column = query_value.keys().get_item(0)?.extract::<String>()?;
            let params = query_value
                .get_item(&column)?
                .ok_or(PyValueError::new_err(format!(
                    "column {} not found",
                    column
                )))?;
            let params = params.downcast::<PyDict>()?;

            let query = params
                .get_item("query")?
                .ok_or(PyValueError::new_err("query not found"))?
                .extract::<String>()?;
            let boost = params
                .get_item("boost")?
                .ok_or(PyValueError::new_err("boost not found"))?
                .extract::<f32>()?;
            let fuzziness = params
                .get_item("fuzziness")?
                .ok_or(PyValueError::new_err("fuzziness not found"))?
                .extract::<Option<u32>>()?;
            let max_expansions = params
                .get_item("max_expansions")?
                .ok_or(PyValueError::new_err("max_expansions not found"))?
                .extract::<usize>()?;

            let query = MatchQuery::new(query)
                .with_column(Some(column))
                .with_boost(boost)
                .with_fuzziness(fuzziness)
                .with_max_expansions(max_expansions);
            Ok(query.into())
        }

        "match_phrase" => {
            let column = query_value.keys().get_item(0)?.extract::<String>()?;
            let query = query_value
                .get_item(&column)?
                .ok_or(PyValueError::new_err(format!(
                    "column {} not found",
                    column
                )))?
                .extract::<String>()?;

            let query = PhraseQuery::new(query).with_column(Some(column));
            Ok(query.into())
        }

        "boost" => {
            let positive: Bound<'_, PyAny> = query_value
                .get_item("positive")?
                .ok_or(PyValueError::new_err("positive not found"))?;
            let positive = positive.downcast::<PyDict>()?;

            let negative = query_value
                .get_item("negative")?
                .ok_or(PyValueError::new_err("negative not found"))?;
            let negative = negative.downcast::<PyDict>()?;

            let negative_boost = query_value
                .get_item("negative_boost")?
                .ok_or(PyValueError::new_err("negative_boost not found"))?
                .extract::<f32>()?;

            let positive_query = parse_fts_query(positive)?;
            let negative_query = parse_fts_query(negative)?;
            let query = BoostQuery::new(positive_query, negative_query, Some(negative_boost));

            Ok(query.into())
        }

        "multi_match" => {
            let query = query_value
                .get_item("query")?
                .ok_or(PyValueError::new_err("query not found"))?
                .extract::<String>()?;

            let columns = query_value
                .get_item("columns")?
                .ok_or(PyValueError::new_err("columns not found"))?
                .extract::<Vec<String>>()?;

            let boost = query_value
                .get_item("boost")?
                .ok_or(PyValueError::new_err("boost not found"))?
                .extract::<Vec<f32>>()?;

            let query =
                MultiMatchQuery::try_new_with_boosts(query, columns, boost).map_err(|e| {
                    PyValueError::new_err(format!("Error creating MultiMatchQuery: {}", e))
                })?;
            Ok(query.into())
        }

        _ => Err(PyValueError::new_err(format!(
            "Unsupported query type: {}",
            query_type
        ))),
    }
}
