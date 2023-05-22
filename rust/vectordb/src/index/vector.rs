use lance::index::vector::ivf::IvfBuildParams;
use lance::index::vector::pq::PQBuildParams;
use lance::index::vector::{MetricType, VectorIndexParams};

pub trait VectorIndexBuilder {
    fn get_column(&self) -> Option<String>;
    fn get_index_name(&self) -> Option<String>;
    fn build(&self) -> VectorIndexParams;
}

pub struct IvfIndexBuilder {
    column: Option<String>,
    index_name: Option<String>,
    metric_type: Option<MetricType>,
    ivf_params: Option<IvfBuildParams>,
    pq_params: Option<PQBuildParams>,
}

impl IvfIndexBuilder {
    pub fn new() -> IvfIndexBuilder {
        IvfIndexBuilder {
            column: None,
            index_name: None,
            metric_type: None,
            ivf_params: None,
            pq_params: None,
        }
    }
}

impl IvfIndexBuilder {
    pub fn column(&mut self, column: String) -> &mut IvfIndexBuilder {
        self.column = Some(column);
        self
    }

    pub fn index_name(&mut self, index_name: String) -> &mut IvfIndexBuilder {
        self.index_name = Some(index_name);
        self
    }

    pub fn metric_type(&mut self, metric_type: MetricType) -> &mut IvfIndexBuilder {
        self.metric_type = Some(metric_type);
        self
    }

    pub fn ivf_params(&mut self, ivf_params: IvfBuildParams) -> &mut IvfIndexBuilder {
        self.ivf_params = Some(ivf_params);
        self
    }

    pub fn pq_params(&mut self, pq_params: PQBuildParams) -> &mut IvfIndexBuilder {
        self.pq_params = Some(pq_params);
        self
    }
}

impl VectorIndexBuilder for IvfIndexBuilder {
    fn get_column(&self) -> Option<String> {
        self.column.clone()
    }

    fn get_index_name(&self) -> Option<String> {
        self.index_name.clone()
    }

    fn build(&self) -> VectorIndexParams {
        let ivf_params = self.ivf_params.clone().unwrap_or(IvfBuildParams::default());
        let pq_params = self.pq_params.clone().unwrap_or(PQBuildParams::default());

        VectorIndexParams::with_ivf_pq_params(pq_params.metric_type, ivf_params, pq_params)
    }
}

#[cfg(test)]
mod tests {
    use lance::index::vector::ivf::IvfBuildParams;
    use lance::index::vector::pq::PQBuildParams;
    use lance::index::vector::{MetricType, StageParams};

    use crate::index::vector::{IvfIndexBuilder, VectorIndexBuilder};

    #[test]
    fn test_builder_no_params() {
        let index_builder = IvfIndexBuilder::new();
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
        let mut index_builder = IvfIndexBuilder::new();

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
        pq_params.metric_type = MetricType::Cosine;
        pq_params.max_opq_iters = 2;
        index_builder.ivf_params(ivf_params);
        index_builder.pq_params(pq_params);

        let index_params = index_builder.build();
        assert_eq!(index_params.stages.len(), 2);
        if let StageParams::Ivf(ivf_params) = index_params.stages.get(0).unwrap() {
            assert_eq!(ivf_params.num_partitions, 500);
        } else {
            panic!("Expected first stage to be ivf")
        }

        if let StageParams::PQ(pq_params) = index_params.stages.get(1).unwrap() {
            assert_eq!(pq_params.use_opq, true);
            assert_eq!(pq_params.max_iters, 1);
            assert_eq!(pq_params.num_bits, 8);
            assert_eq!(pq_params.num_sub_vectors, 50);
            assert_eq!(pq_params.metric_type, MetricType::Cosine);
            assert_eq!(pq_params.max_opq_iters, 2);
        } else {
            panic!("Expected second stage to be pq")
        }
    }
}
