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

use lancedb::index::scalar::{BTreeIndexBuilder, FtsIndexBuilder};
use lancedb::index::vector::IvfPqIndexBuilder;
use lancedb::index::Index as LanceDbIndex;
use napi_derive::napi;

use crate::util::parse_distance_type;

#[napi]
pub struct Index {
    inner: Mutex<Option<LanceDbIndex>>,
}

impl Index {
    pub fn consume(&self) -> napi::Result<LanceDbIndex> {
        self.inner
            .lock()
            .unwrap()
            .take()
            .ok_or(napi::Error::from_reason(
                "attempt to use an index more than once",
            ))
    }
}

#[napi]
impl Index {
    #[napi(factory)]
    pub fn ivf_pq(
        distance_type: Option<String>,
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
    ) -> napi::Result<Self> {
        let mut ivf_pq_builder = IvfPqIndexBuilder::default();
        if let Some(distance_type) = distance_type {
            let distance_type = parse_distance_type(distance_type)?;
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

    #[napi(factory)]
    pub fn btree() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::BTree(BTreeIndexBuilder::default()))),
        }
    }

    #[napi(factory)]
    pub fn bitmap() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::Bitmap(Default::default()))),
        }
    }

    #[napi(factory)]
    pub fn label_list() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::LabelList(Default::default()))),
        }
    }

    #[napi(factory)]
    pub fn fts() -> Self {
        Self {
            inner: Mutex::new(Some(LanceDbIndex::FTS(FtsIndexBuilder::default()))),
        }
    }
}
