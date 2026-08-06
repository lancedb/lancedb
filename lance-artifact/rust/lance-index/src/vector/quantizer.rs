// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use core::fmt;
use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::fmt::Debug;
use std::str::FromStr;
use std::sync::Arc;

use arrow::{array::AsArray, compute::concat_batches, datatypes::UInt64Type};
use arrow_array::{Array, ArrayRef, FixedSizeListArray, RecordBatch, UInt32Array, UInt64Array};
use arrow_schema::Field;
use async_trait::async_trait;
use bytes::Bytes;
use lance_arrow::RecordBatchExt;
use lance_core::deepsize::DeepSizeOf;
use lance_core::{Error, ROW_ID, Result};
use lance_file::versions::v1::reader::FileReader as V1FileReader;
use lance_io::traits::Reader;
use lance_linalg::distance::DistanceType;
use lance_table::format::SelfDescribingFileReader;
use serde::{Deserialize, Serialize};

use super::flat::index::{FlatBinQuantizer, FlatQuantizer};
use super::pq::ProductQuantizer;
use super::{ivf::storage::IvfModel, sq::ScalarQuantizer, storage::VectorStore};
use crate::frag_reuse::FragReuseIndex;
use crate::vector::bq::builder::RabitQuantizer;
use crate::{INDEX_METADATA_SCHEMA_KEY, IndexMetadata};

pub trait Quantization:
    Send
    + Sync
    + Clone
    + Debug
    + DeepSizeOf
    + Into<Quantizer>
    + TryFrom<Quantizer, Error = lance_core::Error>
{
    type BuildParams: QuantizerBuildParams + Send + Sync;
    type Metadata: QuantizerMetadata + Send + Sync;
    type Storage: QuantizerStorage<Metadata = Self::Metadata> + Debug;

    fn build(
        data: &dyn Array,
        distance_type: DistanceType,
        params: &Self::BuildParams,
    ) -> Result<Self>;
    fn retrain(&mut self, data: &dyn Array) -> Result<()>;
    fn code_dim(&self) -> usize;
    fn column(&self) -> &'static str;
    fn use_residual(_: DistanceType) -> bool {
        false
    }
    fn quantize(&self, vectors: &dyn Array) -> Result<ArrayRef>;
    fn metadata_key() -> &'static str;
    fn quantization_type() -> QuantizationType;
    fn metadata(&self, _: Option<QuantizationMetadata>) -> Self::Metadata;
    fn from_metadata(metadata: &Self::Metadata, distance_type: DistanceType) -> Result<Quantizer>;
    fn field(&self) -> Field;
    fn extra_fields(&self) -> Vec<Field> {
        vec![]
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuantizationType {
    Flat,
    FlatBin,
    Product,
    Scalar,
    Rabit,
}

impl FromStr for QuantizationType {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "FLAT" => Ok(Self::Flat),
            "FLATBIN" => Ok(Self::FlatBin),
            "PQ" => Ok(Self::Product),
            "SQ" => Ok(Self::Scalar),
            // `Display` writes "RQ"; "RABIT" is accepted for headers written
            // before this variant round-tripped.
            "RQ" | "RABIT" => Ok(Self::Rabit),
            _ => Err(Error::index(format!("Unknown quantization type: {}", s))),
        }
    }
}

impl std::fmt::Display for QuantizationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Flat => write!(f, "FLAT"),
            Self::FlatBin => write!(f, "FLATBIN"),
            Self::Product => write!(f, "PQ"),
            Self::Scalar => write!(f, "SQ"),
            Self::Rabit => write!(f, "RQ"),
        }
    }
}

pub trait QuantizerBuildParams: Send + Sync {
    fn sample_size(&self) -> usize;
    fn use_residual(_: DistanceType) -> bool {
        false
    }
}

impl QuantizerBuildParams for () {
    fn sample_size(&self) -> usize {
        0
    }
}

/// Quantization Method.
///
/// <section class="warning">
/// Internal use only. End-user does not use this directly.
/// </section>
#[derive(Debug, Clone, DeepSizeOf)]
pub enum Quantizer {
    Flat(FlatQuantizer),
    FlatBin(FlatBinQuantizer),
    Product(ProductQuantizer),
    Scalar(ScalarQuantizer),
    Rabit(RabitQuantizer),
}

impl Quantizer {
    pub fn code_dim(&self) -> usize {
        match self {
            Self::Flat(fq) => fq.code_dim(),
            Self::FlatBin(fq) => fq.code_dim(),
            Self::Product(pq) => pq.code_dim(),
            Self::Scalar(sq) => sq.code_dim(),
            Self::Rabit(rq) => rq.code_dim(),
        }
    }

    pub fn column(&self) -> &'static str {
        match self {
            Self::Flat(fq) => fq.column(),
            Self::FlatBin(fq) => fq.column(),
            Self::Product(pq) => pq.column(),
            Self::Scalar(sq) => sq.column(),
            Self::Rabit(rq) => rq.column(),
        }
    }

    pub fn metadata_key(&self) -> &'static str {
        match self {
            Self::Flat(_) => FlatQuantizer::metadata_key(),
            Self::FlatBin(_) => FlatBinQuantizer::metadata_key(),
            Self::Product(_) => ProductQuantizer::metadata_key(),
            Self::Scalar(_) => ScalarQuantizer::metadata_key(),
            Self::Rabit(_) => RabitQuantizer::metadata_key(),
        }
    }

    pub fn quantization_type(&self) -> QuantizationType {
        match self {
            Self::Flat(_) => QuantizationType::Flat,
            Self::FlatBin(_) => QuantizationType::FlatBin,
            Self::Product(_) => QuantizationType::Product,
            Self::Scalar(_) => QuantizationType::Scalar,
            Self::Rabit(_) => QuantizationType::Rabit,
        }
    }

    pub fn metadata(&self, args: Option<QuantizationMetadata>) -> Result<serde_json::Value> {
        let metadata = match self {
            Self::Flat(fq) => serde_json::to_value(fq.metadata(args))?,
            Self::FlatBin(fq) => serde_json::to_value(fq.metadata(args))?,
            Self::Product(pq) => serde_json::to_value(pq.metadata(args))?,
            Self::Scalar(sq) => serde_json::to_value(sq.metadata(args))?,
            Self::Rabit(rq) => serde_json::to_value(rq.metadata(args))?,
        };
        Ok(metadata)
    }
}

impl From<ProductQuantizer> for Quantizer {
    fn from(pq: ProductQuantizer) -> Self {
        Self::Product(pq)
    }
}

impl From<ScalarQuantizer> for Quantizer {
    fn from(sq: ScalarQuantizer) -> Self {
        Self::Scalar(sq)
    }
}

#[derive(Debug, Clone, Default)]
pub struct QuantizationMetadata {
    // For PQ
    pub codebook_position: Option<usize>,
    pub codebook: Option<FixedSizeListArray>,
    pub transposed: bool,
}

#[async_trait]
pub trait QuantizerMetadata:
    fmt::Debug + Clone + Sized + DeepSizeOf + for<'a> Deserialize<'a> + Serialize
{
    // the extra metadata index in global buffer
    fn buffer_index(&self) -> Option<u32> {
        None
    }

    fn set_buffer_index(&mut self, _: u32) {
        // do nothing
    }

    // parse the extra metadata bytes from global buffer,
    // and set the metadata fields
    fn parse_buffer(&mut self, _bytes: Bytes) -> Result<()> {
        Ok(())
    }

    // the metadata that should be stored in global buffer
    fn extra_metadata(&self) -> Result<Option<Bytes>> {
        Ok(None)
    }

    async fn load(reader: &V1FileReader) -> Result<Self>;
}

#[async_trait::async_trait]
pub trait QuantizerStorage: Clone + Sized + DeepSizeOf + VectorStore {
    type Metadata: QuantizerMetadata;

    /// Create a [QuantizerStorage] from a [RecordBatch].
    /// The batch should consist of row IDs and quantized vector.
    fn try_from_batch(
        batch: RecordBatch,
        metadata: &Self::Metadata,
        distance_type: DistanceType,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self>;

    fn metadata(&self) -> &Self::Metadata;

    fn remap(&self, mapping: &RowAddrRemap) -> Result<Self> {
        let batches = self
            .to_batches()?
            .map(|b| {
                let mut indices = Vec::with_capacity(b.num_rows());
                let mut new_row_ids = Vec::with_capacity(b.num_rows());

                let row_ids = b.column(0).as_primitive::<UInt64Type>().values();
                for (i, row_id) in row_ids.iter().enumerate() {
                    match mapping.get(*row_id) {
                        Some(Some(new_id)) => {
                            indices.push(i as u32);
                            new_row_ids.push(new_id);
                        }
                        Some(None) => {}
                        None => {
                            indices.push(i as u32);
                            new_row_ids.push(*row_id);
                        }
                    }
                }

                let indices = UInt32Array::from(indices);
                let new_row_ids = UInt64Array::from(new_row_ids);
                let b = b
                    .take(&indices)?
                    .replace_column_by_name(ROW_ID, Arc::new(new_row_ids))?;
                Ok(b)
            })
            .collect::<Result<Vec<_>>>()?;

        let batch = concat_batches(self.schema(), batches.iter())?;
        Self::try_from_batch(batch, self.metadata(), self.distance_type(), None)
    }

    async fn load_partition(
        reader: &V1FileReader,
        range: std::ops::Range<usize>,
        distance_type: DistanceType,
        metadata: &Self::Metadata,
        frag_reuse_index: Option<Arc<FragReuseIndex>>,
    ) -> Result<Self>;
}

/// Loader to load partitioned [VectorStore] from disk.
pub struct IvfQuantizationStorage<Q: Quantization> {
    reader: V1FileReader,

    distance_type: DistanceType,
    quantizer: Quantizer,
    metadata: Q::Metadata,

    ivf: IvfModel,
}

impl<Q: Quantization> DeepSizeOf for IvfQuantizationStorage<Q> {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.reader.deep_size_of_children(context)
            + self.quantizer.deep_size_of_children(context)
            + self.metadata.deep_size_of_children(context)
            + self.ivf.deep_size_of_children(context)
    }
}

impl<Q: Quantization> Clone for IvfQuantizationStorage<Q> {
    fn clone(&self) -> Self {
        Self {
            reader: self.reader.clone(),
            distance_type: self.distance_type,
            quantizer: self.quantizer.clone(),
            metadata: self.metadata.clone(),
            ivf: self.ivf.clone(),
        }
    }
}

impl<Q: Quantization> IvfQuantizationStorage<Q> {
    /// Open a Loader.
    ///
    ///
    pub async fn open(reader: Arc<dyn Reader>) -> Result<Self> {
        let reader = V1FileReader::try_new_self_described_from_reader(reader, None).await?;
        let schema = reader.schema();

        let metadata_str = schema
            .metadata
            .get(INDEX_METADATA_SCHEMA_KEY)
            .ok_or(Error::index(format!(
                "Reading quantization storage: index key {} not found",
                INDEX_METADATA_SCHEMA_KEY
            )))?;
        let index_metadata: IndexMetadata = serde_json::from_str(metadata_str).map_err(|_| {
            Error::index(format!("Failed to parse index metadata: {}", metadata_str))
        })?;
        let distance_type = DistanceType::try_from(index_metadata.distance_type.as_str())?;

        let ivf_data = IvfModel::load(&reader).await?;

        let metadata = Q::Metadata::load(&reader).await?;
        let quantizer = Q::from_metadata(&metadata, distance_type)?;
        Ok(Self {
            reader,
            distance_type,
            quantizer,
            metadata,
            ivf: ivf_data,
        })
    }

    pub fn distance_type(&self) -> DistanceType {
        self.distance_type
    }

    pub fn quantizer(&self) -> &Quantizer {
        &self.quantizer
    }

    pub fn metadata(&self) -> &Q::Metadata {
        &self.metadata
    }

    /// Get the number of partitions in the storage.
    pub fn num_partitions(&self) -> usize {
        self.ivf.num_partitions()
    }

    /// Load one partition of vector storage.
    ///
    /// # Parameters
    /// - `part_id`, partition id
    ///
    ///
    pub async fn load_partition(&self, part_id: usize) -> Result<Q::Storage> {
        let range = self.ivf.row_range(part_id);
        Q::Storage::load_partition(
            &self.reader,
            range,
            self.distance_type,
            &self.metadata,
            None,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;

    /// `IvfIndexState` persists the quantization type with `Display` and reads
    /// it back with `FromStr`, so the two must agree for every variant.
    #[rstest]
    #[case::flat(QuantizationType::Flat)]
    #[case::flat_bin(QuantizationType::FlatBin)]
    #[case::product(QuantizationType::Product)]
    #[case::scalar(QuantizationType::Scalar)]
    #[case::rabit(QuantizationType::Rabit)]
    fn test_display_from_str_round_trip(#[case] quantization_type: QuantizationType) {
        let encoded = quantization_type.to_string();
        assert_eq!(
            encoded.parse::<QuantizationType>().unwrap(),
            quantization_type,
            "{encoded} did not round-trip"
        );
    }

    #[test]
    fn test_from_str_accepts_legacy_rabit_spelling() {
        assert_eq!(
            "RABIT".parse::<QuantizationType>().unwrap(),
            QuantizationType::Rabit
        );
    }
}
