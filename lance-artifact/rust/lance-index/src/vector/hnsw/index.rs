// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::{
    any::Any,
    fmt::{Debug, Formatter},
    sync::Arc,
};

use arrow_array::{Float32Array, RecordBatch, UInt32Array};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use lance_arrow::RecordBatchExt;
use lance_core::ROW_ID;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, Result, datatypes::Schema};
use lance_file::versions::v1::reader::FileReader as V1FileReader;
use lance_io::traits::Reader;
use lance_linalg::distance::DistanceType;
use lance_table::format::SelfDescribingFileReader;
use roaring::RoaringBitmap;
use serde_json::json;
use tracing::instrument;

use crate::vector::ivf::storage::IvfModel;
use crate::vector::quantizer::QuantizationType;
use crate::vector::v3::subindex::{IvfSubIndex, SubIndexType};
use crate::{
    Index, IndexType,
    vector::{
        Query, VectorIndex,
        graph::NEIGHBORS_FIELD,
        hnsw::{HNSW, HnswMetadata, VECTOR_ID_FIELD},
        ivf::storage::IVF_PARTITION_KEY,
        quantizer::{IvfQuantizationStorage, Quantization, Quantizer},
        storage::VectorStore,
    },
};
use crate::{metrics::MetricsCollector, prefilter::PreFilter};

#[derive(Clone, DeepSizeOf)]
pub struct HNSWIndexOptions {
    pub use_residual: bool,
}

#[derive(Clone, DeepSizeOf)]
pub struct HNSWIndex<Q: Quantization> {
    // Some(T) if the index is loaded, None otherwise
    hnsw: Option<HNSW>,
    storage: Option<Arc<Q::Storage>>,

    // TODO: move these into IVFIndex after the refactor is complete
    partition_storage: IvfQuantizationStorage<Q>,
    partition_metadata: Option<Vec<HnswMetadata>>,

    options: HNSWIndexOptions,
}

impl<Q: Quantization> Debug for HNSWIndex<Q> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.hnsw.fmt(f)
    }
}

impl<Q: Quantization> HNSWIndex<Q> {
    pub async fn try_new(
        reader: Arc<dyn Reader>,
        aux_reader: Arc<dyn Reader>,
        options: HNSWIndexOptions,
    ) -> Result<Self> {
        let reader = V1FileReader::try_new_self_described_from_reader(reader.clone(), None).await?;

        let partition_metadata = match reader.schema().metadata.get(IVF_PARTITION_KEY) {
            Some(json) => {
                let metadata: Vec<HnswMetadata> = serde_json::from_str(json)?;
                Some(metadata)
            }
            None => None,
        };

        let ivf_store = IvfQuantizationStorage::open(aux_reader).await?;
        Ok(Self {
            hnsw: None,
            storage: None,
            partition_storage: ivf_store,
            partition_metadata,
            options,
        })
    }

    pub fn quantizer(&self) -> &Quantizer {
        self.partition_storage.quantizer()
    }

    pub fn metadata(&self) -> HnswMetadata {
        self.partition_metadata.as_ref().unwrap()[0].clone()
    }

    fn get_partition_metadata(&self, partition_id: usize) -> Result<HnswMetadata> {
        match self.partition_metadata {
            Some(ref metadata) => Ok(metadata[partition_id].clone()),
            None => Err(Error::index("No partition metadata found".to_string())),
        }
    }
}

#[async_trait]
impl<Q: Quantization + Send + Sync + 'static> Index for HNSWIndex<Q> {
    /// Cast to [Any].
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Cast to [Index]
    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    /// Retrieve index statistics as a JSON Value
    fn statistics(&self) -> Result<serde_json::Value> {
        Ok(json!({
            "index_type": "HNSW",
            "distance_type": self.partition_storage.distance_type().to_string(),
        }))
    }

    async fn prewarm(&self) -> Result<()> {
        // TODO: HNSW can (and should) support pre-warming
        Ok(())
    }

    /// Get the type of the index
    fn index_type(&self) -> IndexType {
        IndexType::Vector
    }

    /// Read through the index and determine which fragment ids are covered by the index
    ///
    /// This is a kind of slow operation.  It's better to use the fragment_bitmap.  This
    /// only exists for cases where the fragment_bitmap has become corrupted or missing.
    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        unimplemented!()
    }
}

#[async_trait]
impl<Q: Quantization + Send + Sync + 'static> VectorIndex for HNSWIndex<Q> {
    #[instrument(level = "debug", skip_all, name = "HNSWIndex::search")]
    async fn search(
        &self,
        query: &Query,
        pre_filter: Arc<dyn PreFilter>,
        metrics: &dyn MetricsCollector,
    ) -> Result<RecordBatch> {
        let hnsw = self
            .hnsw
            .as_ref()
            .ok_or(Error::index("HNSW index not loaded".to_string()))?;

        let storage = self
            .storage
            .as_ref()
            .ok_or(Error::index("vector storage not loaded".to_string()))?;

        let refine_factor = query.refine_factor.unwrap_or(1) as usize;
        let k = query.k * refine_factor;

        hnsw.search(
            query.key.clone(),
            k,
            query.into(),
            storage.as_ref(),
            pre_filter,
            metrics,
        )
    }

    fn find_partitions(&self, _: &Query) -> Result<(UInt32Array, Float32Array)> {
        unimplemented!("only for IVF")
    }

    fn total_partitions(&self) -> usize {
        1
    }

    async fn search_in_partition(
        &self,
        _: usize,
        _: &Query,
        _: Arc<dyn PreFilter>,
        _: &dyn MetricsCollector,
    ) -> Result<RecordBatch> {
        unimplemented!("only for IVF")
    }

    fn is_loadable(&self) -> bool {
        true
    }

    fn use_residual(&self) -> bool {
        self.options.use_residual
    }

    async fn load(
        &self,
        reader: Arc<dyn Reader>,
        _offset: usize,
        _length: usize,
    ) -> Result<Box<dyn VectorIndex>> {
        let schema = Schema::try_from(&arrow_schema::Schema::new(vec![
            NEIGHBORS_FIELD.clone(),
            VECTOR_ID_FIELD.clone(),
        ]))?;

        let reader = V1FileReader::try_new_from_reader(
            reader.path(),
            reader.clone(),
            None,
            schema,
            0,
            0,
            2,
            None,
        )
        .await?;

        let storage = Arc::new(self.partition_storage.load_partition(0).await?);
        let batch = reader.read_range(0..reader.len(), reader.schema()).await?;
        let hnsw = HNSW::load(batch)?;

        Ok(Box::new(Self {
            hnsw: Some(hnsw),
            storage: Some(storage),
            partition_storage: self.partition_storage.clone(),
            partition_metadata: self.partition_metadata.clone(),
            options: self.options.clone(),
        }))
    }

    async fn load_partition(
        &self,
        reader: Arc<dyn Reader>,
        offset: usize,
        length: usize,
        partition_id: usize,
    ) -> Result<Box<dyn VectorIndex>> {
        let reader = V1FileReader::try_new_self_described_from_reader(reader, None).await?;

        let metadata = self.get_partition_metadata(partition_id)?;
        let storage = Arc::new(self.partition_storage.load_partition(partition_id).await?);
        let batch = reader
            .read_range(offset..offset + length, reader.schema())
            .await?;
        let mut schema = batch.schema_ref().as_ref().clone();
        schema.metadata.insert(
            HNSW::metadata_key().to_owned(),
            serde_json::to_string(&metadata)?,
        );
        let batch = batch.with_schema(schema.into())?;
        let hnsw = HNSW::load(batch)?;

        Ok(Box::new(Self {
            hnsw: Some(hnsw),
            storage: Some(storage),
            partition_storage: self.partition_storage.clone(),
            partition_metadata: self.partition_metadata.clone(),
            options: self.options.clone(),
        }))
    }

    async fn to_batch_stream(&self, with_vector: bool) -> Result<SendableRecordBatchStream> {
        let store = self
            .storage
            .as_ref()
            .ok_or(Error::index("vector storage not loaded".to_string()))?;

        let schema = if with_vector {
            store.schema().clone()
        } else {
            let schema = store.schema();
            let row_id_idx = schema.index_of(ROW_ID)?;
            Arc::new(schema.project(&[row_id_idx])?)
        };

        let batches = store
            .to_batches()?
            .map(|b| {
                let batch = b.project_by_schema(&schema)?;
                Ok(batch)
            })
            .collect::<Vec<_>>();
        let stream = futures::stream::iter(batches);
        let stream = RecordBatchStreamAdapter::new(schema, stream);
        Ok(Box::pin(stream))
    }

    fn num_rows(&self) -> u64 {
        self.hnsw
            .as_ref()
            .map_or(0, |hnsw| hnsw.num_nodes(0) as u64)
    }

    fn row_ids(&self) -> Box<dyn Iterator<Item = &'_ u64> + '_> {
        Box::new(self.storage.as_ref().unwrap().row_ids())
    }

    async fn remap(&mut self, _mapping: &RowAddrRemap) -> Result<()> {
        Err(Error::index(
            "Remapping HNSW in this way not supported".to_string(),
        ))
    }

    fn ivf_model(&self) -> &IvfModel {
        unimplemented!("only for IVF")
    }

    fn quantizer(&self) -> Quantizer {
        self.partition_storage.quantizer().clone()
    }

    fn partition_size(&self, _: usize) -> usize {
        unimplemented!("only for IVF")
    }

    fn sub_index_type(&self) -> (SubIndexType, QuantizationType) {
        (
            SubIndexType::Hnsw,
            self.partition_storage.quantizer().quantization_type(),
        )
    }

    fn metric_type(&self) -> DistanceType {
        self.partition_storage.distance_type()
    }
}
