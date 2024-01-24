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

pub use lance_linalg::distance::MetricType;

pub mod vector;

use crate::error::Result;

/// Index Parameters.
pub enum IndexParameters {
    Scalar {
        replace: bool,
    },
    IvfPq {
        replace: bool,
        metric_type: Option<MetricType>,
        num_partitions: Option<u64>,
    },
}

/// Builder for Index Parameters.

#[derive(Default)]
pub struct IndexParamsBuilder {
    // General parameters
    /// Index name.
    name: Option<String>,
    /// Replace the existing index.
    replace: bool,

    // Scalar index parameters
    // None

    // IVF_PQ parameters
    metric_type: Option<MetricType>,
    num_partitions: Option<u64>,
    // PQ related
    num_sub_vectors: Option<u32>,
    num_bits: Option<u32>,

    /// The rate to find samples to train kmeans.
    sample_rate: Option<u32>,
    /// Max iteration to train kmeans.
    max_iterations: Option<u32>,
}

impl IndexParamsBuilder {
    pub fn scalar() -> Self {
        IndexParamsBuilder {
            replace: true,
            ..Default::default()
        }
    }

    /// Build an IVF PQ index.
    pub fn ivf_pq() -> Self {
        IndexParamsBuilder {
            replace: true,
            ..Default::default()
        }
    }

    pub fn build(&self) -> Result<IndexParameters> {
        Ok(IndexParameters::Scalar { replace: true })
    }
}
