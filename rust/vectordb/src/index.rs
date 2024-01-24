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

use lance_index::{DatasetIndexExt, IndexType};
pub use lance_linalg::distance::MetricType;

pub mod vector;

use crate::{Error, Result, Table};

/// Index Parameters.
pub enum IndexParams {
    Scalar {
        replace: bool,
    },
    IvfPq {
        replace: bool,
        metric_type: MetricType,
        num_partitions: u64,
        num_sub_vectors: u32,
        num_bits: u32,
        sample_rate: u32,
        max_iterations: u32,
    },
}

/// Builder for Index Parameters.

pub struct IndexBuilder<'a> {
    table: &'a dyn Table,
    columns: Vec<String>,
    // General parameters
    /// Index name.
    name: Option<String>,
    /// Replace the existing index.
    replace: bool,

    index_type: IndexType,

    // Scalar index parameters
    // Nothing to set here.

    // IVF_PQ parameters
    metric_type: MetricType,
    num_partitions: Option<u64>,
    // PQ related
    num_sub_vectors: Option<u32>,
    num_bits: u32,

    /// The rate to find samples to train kmeans.
    sample_rate: u32,
    /// Max iteration to train kmeans.
    max_iterations: u32,
}

impl<'a> IndexBuilder<'a> {
    pub(crate) fn new(table: &'a dyn Table, columns: &[&str]) -> Self {
        IndexBuilder {
            table,
            columns: columns.iter().map(|c| c.to_string()).collect(),
            name: None,
            replace: false,
            index_type: IndexType::Scalar,
            metric_type: MetricType::L2,
            num_partitions: None,
            num_sub_vectors: None,
            num_bits: 8,
            sample_rate: 256,
            max_iterations: 50,
        }
    }

    pub fn scalar(&mut self) -> &mut Self {
        self.index_type = IndexType::Scalar;
        self
    }

    /// Build an IVF PQ index.
    pub fn ivf_pq(&mut self) -> &mut Self {
        self.index_type = IndexType::Vector;
        self
    }

    pub fn replace(&mut self) -> &mut Self {
        self.replace = true;
        self
    }

    pub fn name(&mut self, name: &str) -> &mut Self {
        self.name = Some(name.to_string());
        self
    }

    /// [MetricType](lance_linalg::distance::MetricType) to use to build Vector Index.
    pub fn metric_type(&mut self, metric_type: MetricType) -> &mut Self {
        self.metric_type = metric_type;
        self
    }

    /// Number of IVF partitions.
    pub fn num_partitions(&mut self, num_partitions: u64) -> &mut Self {
        self.num_partitions = Some(num_partitions);
        self
    }

    /// Number of sub-vectors of PQ.
    pub fn num_sub_vectors(&mut self, num_sub_vectors: u32) -> &mut Self {
        self.num_sub_vectors = Some(num_sub_vectors);
        self
    }

    /// Number of bits used for PQ centroids.
    pub fn num_bits(&mut self, num_bits: u32) -> &mut Self {
        self.num_bits = num_bits;
        self
    }

    /// The rate to find samples to train kmeans.
    pub fn sample_rate(&mut self, sample_rate: u32) -> &mut Self {
        self.sample_rate = sample_rate;
        self
    }

    /// Max iteration to train kmeans.
    pub fn max_iterations(&mut self, max_iterations: u32) -> &mut Self {
        self.max_iterations = max_iterations;
        self
    }

    /// Build the parameters.
    pub async fn build(&self) -> Result<()> {
        if self.columns.len() != 1 {
            return Err(Error::Schema {
                message: "Only one column is supported for index".to_string(),
            }
            .into());
        }
        let column = &self.columns[0];
        let schema = self.table.schema();
        let field = schema.field_with_name(&column)?;

        let params = match self.index_type {
            IndexType::Scalar => IndexParams::Scalar {
                replace: self.replace,
            },
            IndexType::Vector => {
                let num_partitions = if let Some(n) = self.num_partitions {
                    n
                } else {
                    suggested_num_partitions(self.table.count_rows().await?)
                };
                let num_sub_vectors: u32 = if let Some(n) = self.num_sub_vectors {
                    n
                } else {
                    match field.data_type() {
                        arrow_schema::DataType::FixedSizeList(_, n) => {
                            Ok::<u32, Error>(suggested_num_sub_vectors(*n as u32))
                        }
                        _ => Err(Error::Schema {
                            message: format!(
                                "Column '{}' is not a FixedSizeList",
                                &self.columns[0]
                            ),
                        }
                        .into()),
                    }?
                };
                IndexParams::IvfPq {
                    replace: self.replace,
                    metric_type: self.metric_type,
                    num_partitions,
                    num_sub_vectors,
                    num_bits: self.num_bits,
                    sample_rate: self.sample_rate,
                    max_iterations: self.max_iterations,
                }
            }
        };

        let tbl = self
            .table
            .as_native()
            .expect("Only native table is supported here");
        let mut dataset = tbl.clone_inner_dataset()?;
        match params {
            IndexParams::Scalar { replace } => {
                self.table
                    .as_native()
                    .unwrap()
                    .create_scalar_index(&column, replace)
                    .await?
            }
            IndexParams::IvfPq {
                replace,
                metric_type,
                num_partitions,
                num_sub_vectors,
                num_bits,
                max_iterations,
                ..
            } => {
                let lance_idx_params = lance::index::vector::VectorIndexParams::ivf_pq(
                    num_partitions as usize,
                    num_bits as u8,
                    num_sub_vectors as usize,
                    false,
                    metric_type,
                    max_iterations as usize,
                );
                dataset
                    .create_index(
                        &[column],
                        IndexType::Vector,
                        None,
                        &lance_idx_params,
                        replace,
                    )
                    .await?;
            }
        }
        tbl.reset_dataset(dataset);
        Ok(())
    }
}

fn suggested_num_partitions(rows: usize) -> u64 {
    let num_partitions = (rows as f64).sqrt() as u64;
    if num_partitions < 1 {
        1
    } else {
        num_partitions
    }
}

fn suggested_num_sub_vectors(dim: u32) -> u32 {
    let num_sub_vectors = dim / 16;
    if num_sub_vectors < 1 {
        1
    } else {
        num_sub_vectors
    }
}
