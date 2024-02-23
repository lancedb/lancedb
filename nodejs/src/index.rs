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

use lance_linalg::distance::MetricType as LanceMetricType;
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
    inner: lancedb::index::IndexBuilder,
}

#[napi]
impl IndexBuilder {
    pub fn new(tbl: &dyn lancedb::Table) -> Self {
        let inner = tbl.create_index(&[]);
        Self { inner }
    }

    #[napi]
    pub unsafe fn replace(&mut self, v: bool) {
        self.inner.replace(v);
    }

    #[napi]
    pub unsafe fn column(&mut self, c: String) {
        self.inner.columns(&[c.as_str()]);
    }

    #[napi]
    pub unsafe fn name(&mut self, name: String) {
        self.inner.name(name.as_str());
    }

    #[napi]
    pub unsafe fn ivf_pq(
        &mut self,
        metric_type: Option<MetricType>,
        num_partitions: Option<u32>,
        num_sub_vectors: Option<u32>,
        num_bits: Option<u32>,
        max_iterations: Option<u32>,
        sample_rate: Option<u32>,
    ) {
        self.inner.ivf_pq();
        metric_type.map(|m| self.inner.metric_type(m.into()));
        num_partitions.map(|p| self.inner.num_partitions(p));
        num_sub_vectors.map(|s| self.inner.num_sub_vectors(s));
        num_bits.map(|b| self.inner.num_bits(b));
        max_iterations.map(|i| self.inner.max_iterations(i));
        sample_rate.map(|s| self.inner.sample_rate(s));
    }

    #[napi]
    pub unsafe fn scalar(&mut self) {
        self.inner.scalar();
    }

    #[napi]
    pub async fn build(&self) -> napi::Result<()> {
        self.inner
            .build()
            .await
            .map_err(|e| napi::Error::from_reason(format!("Failed to build index: {}", e)))?;
        Ok(())
    }
}
