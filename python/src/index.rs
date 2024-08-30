// Copyright 2024 Lance Developers.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::Mutex;

use lancedb::{
    index::{scalar::BTreeIndexBuilder, vector::IvfPqIndexBuilder, Index as LanceDbIndex},
    DistanceType,
};
use pyo3::{
    exceptions::{PyRuntimeError, PyValueError},
    pyclass, pymethods, PyResult,
};

#[pyclass]
pub struct Index {
    inner: Mutex<Option<LanceDbIndex>>,
}

impl Index {
    pub fn consume(&self) -> PyResult<LanceDbIndex> {
        self.inner
            .lock()
            .unwrap()
            .take()
            .ok_or_else(|| PyRuntimeError::new_err("cannot use an Index more than once"))
    }
}

#[pymethods]
impl Index {
    #[staticmethod]
    pub fn ivf_pq(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
    ) -> PyResult<Self> {
        let mut ivf_pq_builder = IvfPqIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = match distance_type.as_str() {
                "l2" => Ok(DistanceType::L2),
                "cosine" => Ok(DistanceType::Cosine),
                "dot" => Ok(DistanceType::Dot),
                _ => Err(PyValueError::new_err(format!(
                    "Invalid distance type '{}'.  Must be one of l2, cosine, or dot",
                    distance_type
                ))),
            }?;
            ivf_pq_builder = ivf_pq_builder.distance_type(distance_type);
        }
        if let Some(num_partitions) = num_partitions {
            ivf_pq_builder = ivf_pq_builder.num_partitions(num_partitions);
        }
        if let Some(num_sub_vectors) = num_sub_vectors {
            ivf_pq_builder = ivf_pq_builder.num_sub_vectors(num_sub_vectors);
        }
        if let Some(max_iterations) = max_iterations {
            ivf_pq_builder = ivf_pq_builder.max_iterations(max_iterations);
        }
        if let Some(sample_rate) = sample_rate {
            ivf_pq_builder = ivf_pq_builder.sample_rate(sample_rate);
        }
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::IvfPq(ivf_pq_builder))),
        })
    }

    #[staticmethod]
    pub fn btree() -> PyResult<Self> {
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::BTree(BTreeIndexBuilder::default()))),
        })
    }

    #[staticmethod]
    pub fn bitmap() -> PyResult<Self> {
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::Bitmap(Default::default()))),
        })
    }

    #[staticmethod]
    pub fn label_list() -> PyResult<Self> {
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::LabelList(Default::default()))),
        })
    }

    #[staticmethod]
    pub fn fts() -> PyResult<Self> {
        Ok(Self {
            inner: Mutex::new(Some(LanceDbIndex::FTS(Default::default()))),
        })
    }
}

#[pyclass(get_all)]
/// A description of an index currently configured on a column
pub struct IndexConfig {
    /// The type of the index
    pub index_type: String,
    /// The columns in the index
    ///
    /// Currently this is always a list of size 1.  In the future there may
    /// be more columns to represent composite indices.
    pub columns: Vec<String>,
}

#[pymethods]
impl IndexConfig {
    pub fn __repr__(&self) -> String {
        format!("Index({}, columns={:?})", self.index_type, self.columns)
    }
}

impl From<lancedb::index::IndexConfig> for IndexConfig {
    fn from(value: lancedb::index::IndexConfig) -> Self {
        let index_type = format!("{:?}", value.index_type);
        Self {
            index_type,
            columns: value.columns,
        }
    }
}
