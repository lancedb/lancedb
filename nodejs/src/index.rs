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

use lance_linalg::distance::MetricType as LanceMetricType;
use lancedb::index::IndexBuilder as LanceDbIndexBuilder;
use lancedb::Table as LanceDbTable;
use napi_derive::napi;

#[napi]
pub enum IndexType {
    Scalar,
    IvfPq,
}

#[napi]
pub enum MetricType {
    L2,
    Cosine,
    Dot,
}

impl From<MetricType> for LanceMetricType {
    fn from(metric: MetricType) -> Self {
        match metric {
            MetricType::L2 => Self::L2,
            MetricType::Cosine => Self::Cosine,
            MetricType::Dot => Self::Dot,
        }
    }
}

#[napi]
pub struct IndexBuilder {
    inner: Mutex<Option<LanceDbIndexBuilder>>,
}

impl IndexBuilder {
    fn modify(
        &self,
        mod_fn: impl Fn(LanceDbIndexBuilder) -> LanceDbIndexBuilder,
    ) -> napi::Result<()> {
        let mut inner = self.inner.lock().unwrap();
        let inner_builder = inner.take().ok_or_else(|| {
            napi::Error::from_reason("IndexBuilder has already been consumed".to_string())
        })?;
        let inner_builder = mod_fn(inner_builder);
        inner.replace(inner_builder);
        Ok(())
    }
}

#[napi]
impl IndexBuilder {
    pub fn new(tbl: &LanceDbTable) -> Self {
        let inner = tbl.create_index(&[]);
        Self {
            inner: Mutex::new(Some(inner)),
        }
    }

    #[napi]
    pub fn replace(&self, v: bool) -> napi::Result<()> {
        self.modify(|b| b.replace(v))
    }

    #[napi]
    pub fn column(&self, c: String) -> napi::Result<()> {
        self.modify(|b| b.columns(&[c.as_str()]))
    }

    #[napi]
    pub fn name(&self, name: String) -> napi::Result<()> {
        self.modify(|b| b.name(name.as_str()))
    }

    #[napi]
    pub fn ivf_pq(
        &self,
        metric_type: Option<MetricType>,
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        num_bits: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
    ) -> napi::Result<()> {
        self.modify(|b| {
            let mut b = b.ivf_pq();
            if let Some(metric_type) = metric_type {
                b = b.metric_type(metric_type.into());
            }
            if let Some(num_partitions) = num_partitions {
                b = b.num_partitions(num_partitions);
            }
            if let Some(num_sub_vectors) = num_sub_vectors {
                b = b.num_sub_vectors(num_sub_vectors);
            }
            if let Some(num_bits) = num_bits {
                b = b.num_bits(num_bits);
            }
            if let Some(max_iterations) = max_iterations {
                b = b.max_iterations(max_iterations);
            }
            if let Some(sample_rate) = sample_rate {
                b = b.sample_rate(sample_rate);
            }
            b
        })
    }

    #[napi]
    pub fn scalar(&self) -> napi::Result<()> {
        self.modify(|b| b.scalar())
    }

    #[napi]
    pub async fn build(&self) -> napi::Result<()> {
        let inner = self.inner.lock().unwrap().take().ok_or_else(|| {
            napi::Error::from_reason("IndexBuilder has already been consumed".to_string())
        })?;
        inner
            .build()
            .await
            .map_err(|e| napi::Error::from_reason(format!("Failed to build index: {}", e)))?;
        Ok(())
    }
}
