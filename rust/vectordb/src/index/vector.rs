use std::sync::Arc;

use lance::index::vector::ivf::IvfBuildParams;
use lance::index::vector::pq::PQBuildParams;
use lance::index::vector::{MetricType, VectorIndexParams};
use lance::index::{IndexType};

use crate::error::Result;
use crate::table::{Table, VECTOR_COLUMN_NAME};

pub struct VectorIndexBuilder {}

impl VectorIndexBuilder {
    pub fn new() -> Self {
        VectorIndexBuilder {}
    }

    pub fn ivf(&self) -> IvfPQIndexBuilder {
        IvfPQIndexBuilder::new()
    }
}

pub struct IvfPQIndexBuilder {
    column: Option<String>,
    index_name: Option<String>,
    uuid: Option<String>,
    metric_type: Option<MetricType>,
    ivf_params: Option<IvfBuildParams>,
    pq_params: Option<PQBuildParams>,
}

impl IvfPQIndexBuilder {
    pub fn new() -> IvfPQIndexBuilder {
        IvfPQIndexBuilder {
            column: None,
            index_name: None,
            uuid: None,
            metric_type: None,
            ivf_params: None,
            pq_params: None,
        }
    }
}

impl IvfPQIndexBuilder {
    pub fn column(mut self, column: String) -> Self {
        self.column = Some(column);
        self
    }

    pub fn index_name(mut self, index_name: String) -> Self {
        self.index_name = Some(index_name);
        self
    }

    pub fn uuid(mut self, uuid: String) -> Self {
        self.uuid = Some(uuid);
        self
    }

    pub fn metric_type(mut self, metric_type: MetricType) -> Self {
        self.metric_type = Some(metric_type);
        self
    }

    pub fn ivf_params(mut self, ivf_params: IvfBuildParams) -> Self {
        self.ivf_params = Some(ivf_params);
        self
    }

    pub fn pq_params(mut self, pq_params: PQBuildParams) -> Self {
        self.pq_params = Some(pq_params);
        self
    }

    pub async fn execute(self, table: Table) -> Result<Table> {
        let ivf_params = self.ivf_params.unwrap_or(IvfBuildParams::default());
        let pq_params = self.pq_params.unwrap_or(PQBuildParams::default());

        let vector_index_params = VectorIndexParams::ivf_pq(
            ivf_params.num_partitions,
            8,
            pq_params.num_sub_vectors,
            pq_params.use_opq,
            pq_params.metric_type,
            ivf_params.max_iters,
        );

        use lance::index::DatasetIndexExt;

        let dataset = table
            .dataset
            .create_index(
                &[self
                    .column
                    .unwrap_or(VECTOR_COLUMN_NAME.to_string())
                    .as_str()],
                IndexType::Vector,
                self.index_name.clone(),
                &vector_index_params,
            )
            .await?;

        Ok(Table {
            path: table.path,
            name: table.name,
            dataset: Arc::new(dataset),
        })
    }
}
