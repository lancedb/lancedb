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

use std::{cmp::max, sync::Arc};

use lance::index::scalar::ScalarIndexParams;
use lance_index::{DatasetIndexExt, IndexType};
pub use lance_linalg::distance::MetricType;

pub mod vector;

use crate::{utils::default_vector_column, Error, Result, Table};

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

pub struct IndexBuilder {
    table: Arc<dyn Table>,
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
    num_partitions: Option<u32>,
    // PQ related
    num_sub_vectors: Option<u32>,
    num_bits: u32,

    /// The rate to find samples to train kmeans.
    sample_rate: u32,
    /// Max iteration to train kmeans.
    max_iterations: u32,
}

impl IndexBuilder {
    pub(crate) fn new(table: Arc<dyn Table>, columns: &[&str]) -> Self {
        Self {
            table,
            columns: columns.iter().map(|c| c.to_string()).collect(),
            name: None,
            replace: true,
            index_type: IndexType::Scalar,
            metric_type: MetricType::L2,
            num_partitions: None,
            num_sub_vectors: None,
            num_bits: 8,
            sample_rate: 256,
            max_iterations: 50,
        }
    }

    /// Build a Scalar Index.
    ///
    /// Accepted parameters:
    ///  - `replace`: Replace the existing index.
    ///  - `name`: Index name. Default: `None`
    pub fn scalar(&mut self) -> &mut Self {
        self.index_type = IndexType::Scalar;
        self
    }

    /// Build an IVF PQ index.
    ///
    /// Accepted parameters:
    /// - `replace`: Replace the existing index.
    /// - `name`: Index name. Default: `None`
    /// - `metric_type`: [MetricType] to use to build Vector Index.
    /// - `num_partitions`: Number of IVF partitions.
    /// - `num_sub_vectors`: Number of sub-vectors of PQ.
    /// - `num_bits`: Number of bits used for PQ centroids.
    /// - `sample_rate`: The rate to find samples to train kmeans.
    /// - `max_iterations`: Max iteration to train kmeans.
    pub fn ivf_pq(&mut self) -> &mut Self {
        self.index_type = IndexType::Vector;
        self
    }

    /// The columns to build index on.
    pub fn columns(&mut self, cols: &[&str]) -> &mut Self {
        self.columns = cols.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Whether to replace the existing index, default is `true`.
    pub fn replace(&mut self, v: bool) -> &mut Self {
        self.replace = v;
        self
    }

    /// Set the index name.
    pub fn name(&mut self, name: &str) -> &mut Self {
        self.name = Some(name.to_string());
        self
    }

    /// [MetricType] to use to build Vector Index.
    ///
    /// Default value is [MetricType::L2].
    pub fn metric_type(&mut self, metric_type: MetricType) -> &mut Self {
        self.metric_type = metric_type;
        self
    }

    /// Number of IVF partitions.
    pub fn num_partitions(&mut self, num_partitions: u32) -> &mut Self {
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
        let schema = self.table.schema();

        // TODO: simplify this after GH lance#1864.
        let mut index_type = &self.index_type;
        let columns = if self.columns.is_empty() {
            // By default we create vector index.
            index_type = &IndexType::Vector;
            vec![default_vector_column(&schema, None)?]
        } else {
            self.columns.clone()
        };

        if columns.len() != 1 {
            return Err(Error::Schema {
                message: "Only one column is supported for index".to_string(),
            });
        }
        let column = &columns[0];

        let field = schema.field_with_name(column)?;

        let params = match index_type {
            IndexType::Scalar => IndexParams::Scalar {
                replace: self.replace,
            },
            IndexType::Vector => {
                let num_partitions = if let Some(n) = self.num_partitions {
                    n
                } else {
                    suggested_num_partitions(self.table.count_rows(None).await?)
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
                        }),
                    }?
                };
                IndexParams::IvfPq {
                    replace: self.replace,
                    metric_type: self.metric_type,
                    num_partitions: num_partitions as u64,
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
        let mut dataset = tbl.clone_inner_dataset();
        match params {
            IndexParams::Scalar { replace } => {
                dataset
                    .create_index(
                        &[&column],
                        IndexType::Scalar,
                        None,
                        &ScalarIndexParams::default(),
                        replace,
                    )
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

fn suggested_num_partitions(rows: usize) -> u32 {
    let num_partitions = (rows as f64).sqrt() as u32;
    max(1, num_partitions)
}

fn suggested_num_sub_vectors(dim: u32) -> u32 {
    if dim % 16 == 0 {
        // Should be more aggressive than this default.
        dim / 16
    } else if dim % 8 == 0 {
        dim / 8
    } else {
        log::warn!(
            "The dimension of the vector is not divisible by 8 or 16, \
                which may cause performance degradation in PQ"
        );
        1
    }
}
