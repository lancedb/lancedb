// Copyright 2023 Lance Developers.
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

use lance::format::{Index, Manifest};
use lance::index::vector::pq::PQBuildParams;
use lance::index::vector::VectorIndexParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_linalg::distance::MetricType;

pub trait VectorIndexBuilder {
    fn get_column(&self) -> Option<String>;
    fn get_index_name(&self) -> Option<String>;
    fn build(&self) -> VectorIndexParams;

    fn get_replace(&self) -> bool;
}

pub struct IvfPQIndexBuilder {
    column: Option<String>,
    index_name: Option<String>,
    metric_type: Option<MetricType>,
    ivf_params: Option<IvfBuildParams>,
    pq_params: Option<PQBuildParams>,
    replace: bool,
}

impl IvfPQIndexBuilder {
    pub fn new() -> IvfPQIndexBuilder {
        Default::default()
    }
}

impl Default for IvfPQIndexBuilder {
    fn default() -> Self {
        IvfPQIndexBuilder {
            column: None,
            index_name: None,
            metric_type: None,
            ivf_params: None,
            pq_params: None,
            replace: true,
        }
    }
}

impl IvfPQIndexBuilder {
    pub fn column(&mut self, column: String) -> &mut IvfPQIndexBuilder {
        self.column = Some(column);
        self
    }

    pub fn index_name(&mut self, index_name: String) -> &mut IvfPQIndexBuilder {
        self.index_name = Some(index_name);
        self
    }

    pub fn metric_type(&mut self, metric_type: MetricType) -> &mut IvfPQIndexBuilder {
        self.metric_type = Some(metric_type);
        self
    }

    pub fn ivf_params(&mut self, ivf_params: IvfBuildParams) -> &mut IvfPQIndexBuilder {
        self.ivf_params = Some(ivf_params);
        self
    }

    pub fn pq_params(&mut self, pq_params: PQBuildParams) -> &mut IvfPQIndexBuilder {
        self.pq_params = Some(pq_params);
        self
    }

    pub fn replace(&mut self, replace: bool) -> &mut IvfPQIndexBuilder {
        self.replace = replace;
        self
    }
}

impl VectorIndexBuilder for IvfPQIndexBuilder {
    fn get_column(&self) -> Option<String> {
        self.column.clone()
    }

    fn get_index_name(&self) -> Option<String> {
        self.index_name.clone()
    }

    fn build(&self) -> VectorIndexParams {
        let ivf_params = self.ivf_params.clone().unwrap_or_default();
        let pq_params = self.pq_params.clone().unwrap_or_default();

        VectorIndexParams::with_ivf_pq_params(
            self.metric_type.unwrap_or(MetricType::L2),
            ivf_params,
            pq_params,
        )
    }

    fn get_replace(&self) -> bool {
        self.replace
    }
}

pub struct VectorIndex {
    pub columns: Vec<String>,
    pub index_name: String,
    pub index_uuid: String,
}

impl VectorIndex {
    pub fn new_from_format(manifest: &Manifest, index: &Index) -> VectorIndex {
        let fields = index
            .fields
            .iter()
            .map(|i| manifest.schema.fields[*i as usize].name.clone())
            .collect();
        VectorIndex {
            columns: fields,
            index_name: index.name.clone(),
            index_uuid: index.uuid.to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use lance::index::vector::StageParams;
    use lance_index::vector::ivf::IvfBuildParams;
    use lance_index::vector::pq::PQBuildParams;

    use crate::index::vector::{IvfPQIndexBuilder, VectorIndexBuilder};

    #[test]
    fn test_builder_no_params() {
        let index_builder = IvfPQIndexBuilder::new();
        assert!(index_builder.get_column().is_none());
        assert!(index_builder.get_index_name().is_none());

        let index_params = index_builder.build();
        assert_eq!(index_params.stages.len(), 2);
        if let StageParams::Ivf(ivf_params) = index_params.stages.get(0).unwrap() {
            let default = IvfBuildParams::default();
            assert_eq!(ivf_params.num_partitions, default.num_partitions);
            assert_eq!(ivf_params.max_iters, default.max_iters);
        } else {
            panic!("Expected first stage to be ivf")
        }

        if let StageParams::PQ(pq_params) = index_params.stages.get(1).unwrap() {
            assert_eq!(pq_params.use_opq, false);
        } else {
            panic!("Expected second stage to be pq")
        }
    }

    #[test]
    fn test_builder_all_params() {
        let mut index_builder = IvfPQIndexBuilder::new();

        index_builder
            .column("c".to_owned())
            .metric_type(MetricType::Cosine)
            .index_name("index".to_owned());

        assert_eq!(index_builder.column.clone().unwrap(), "c");
        assert_eq!(index_builder.metric_type.unwrap(), MetricType::Cosine);
        assert_eq!(index_builder.index_name.clone().unwrap(), "index");

        let ivf_params = IvfBuildParams::new(500);
        let mut pq_params = PQBuildParams::default();
        pq_params.use_opq = true;
        pq_params.max_iters = 1;
        pq_params.num_bits = 8;
        pq_params.num_sub_vectors = 50;
        pq_params.max_opq_iters = 2;
        index_builder.ivf_params(ivf_params);
        index_builder.pq_params(pq_params);

        let index_params = index_builder.build();
        assert_eq!(index_params.stages.len(), 2);
        if let StageParams::Ivf(ivf_params) = index_params.stages.get(0).unwrap() {
            assert_eq!(ivf_params.num_partitions, 500);
        } else {
            assert!(false, "Expected first stage to be ivf")
        }

        if let StageParams::PQ(pq_params) = index_params.stages.get(1).unwrap() {
            assert_eq!(pq_params.use_opq, true);
            assert_eq!(pq_params.max_iters, 1);
            assert_eq!(pq_params.num_bits, 8);
            assert_eq!(pq_params.num_sub_vectors, 50);
            assert_eq!(pq_params.max_opq_iters, 2);
        } else {
            assert!(false, "Expected second stage to be pq")
        }
    }
}
