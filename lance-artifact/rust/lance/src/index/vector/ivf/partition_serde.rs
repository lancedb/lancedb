// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Serialization and zero-copy deserialization for IVF partition cache entries.
//!
//! Each entry is a protobuf header (see `lance-index/protos-cache/cache.proto`, with the
//! distance and rotation types as proto enums) followed by 64-byte-aligned
//! Arrow IPC sections in a fixed, version-keyed order: the sub-index, then any
//! quantizer-specific arrays (PQ codebook, RabitQ Matrix rotation), then the
//! quantizer storage batches. Sections decode zero-copy via [`lance_arrow::ipc`].

use std::sync::Arc;

use arrow_array::{FixedSizeListArray, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use lance_core::cache::{CacheCodecImpl, CacheEntryReader, CacheEntryWriter};
use lance_core::{Error, Result};
use lance_index::vector::bq::RQRotationType;
use lance_index::vector::bq::builder::RabitQuantizer;
use lance_index::vector::bq::storage::{RabitQuantizationMetadata, RabitQueryEstimator};
use lance_index::vector::flat::index::{FlatBinQuantizer, FlatMetadata, FlatQuantizer};
use lance_index::vector::pq::ProductQuantizer;
use lance_index::vector::pq::storage::ProductQuantizationMetadata;
use lance_index::vector::quantizer::{Quantization, QuantizerStorage};
use lance_index::vector::sq::ScalarQuantizer;
use lance_index::vector::storage::VectorStore;
use lance_index::vector::v3::subindex::IvfSubIndex;
use lance_linalg::distance::DistanceType;

use lance_index::cache_pb::{
    DistanceType as PbDistanceType, FlatPartitionHeader, PqPartitionHeader, RabitPartitionHeader,
    RabitQueryEstimator as PbRabitQueryEstimator, RotationType as PbRotationType,
    SqPartitionHeader,
};

use super::v2::PartitionEntry;

/// Returns an erased codec for `PartitionEntry<S, Q>` by matching on the
/// quantizer's [`QuantizationType`]. Returns `None` for quantizer types
/// that don't have a `CacheCodecImpl` implementation.
///
/// Uses enum dispatch rather than trait bounds to avoid propagating
/// `CacheCodecImpl` constraints through the `IVFIndex<S, Q>` type hierarchy.
pub fn partition_entry_codec<S: IvfSubIndex + 'static, Q: Quantization + 'static>()
-> Option<lance_core::cache::CacheCodec> {
    use lance_index::vector::quantizer::QuantizationType;
    match Q::quantization_type() {
        QuantizationType::Product => Some(codec_for::<S, Q, ProductQuantizer>()),
        QuantizationType::Flat => Some(codec_for::<S, Q, FlatQuantizer>()),
        QuantizationType::FlatBin => Some(codec_for::<S, Q, FlatBinQuantizer>()),
        QuantizationType::Scalar => Some(codec_for::<S, Q, ScalarQuantizer>()),
        QuantizationType::Rabit => Some(codec_for::<S, Q, RabitQuantizer>()),
    }
}

type ArcAny = Arc<dyn std::any::Any + Send + Sync>;

fn serialize_partition_entry<S, Concrete>(
    any: &ArcAny,
    writer: &mut CacheEntryWriter<'_>,
) -> lance_core::Result<()>
where
    S: IvfSubIndex + 'static,
    Concrete: Quantization + 'static,
    PartitionEntry<S, Concrete>: CacheCodecImpl,
{
    let concrete = any
        .downcast_ref::<PartitionEntry<S, Concrete>>()
        .expect("quantization_type matched but downcast failed (this is a bug)");
    concrete.serialize(writer)
}

fn deserialize_partition_entry<S, Q, Concrete>(
    reader: &mut CacheEntryReader<'_>,
) -> lance_core::Result<ArcAny>
where
    S: IvfSubIndex + 'static,
    Q: Quantization + 'static,
    Concrete: Quantization + 'static,
    PartitionEntry<S, Concrete>: CacheCodecImpl,
{
    let concrete = PartitionEntry::<S, Concrete>::deserialize(reader)?;
    let any: ArcAny = Arc::new(concrete);
    Ok(any
        .downcast::<PartitionEntry<S, Q>>()
        .expect("quantization_type matched but downcast failed (this is a bug)"))
}

/// Build a CacheCodec for `PartitionEntry<S, Q>` by delegating to the
/// CacheCodecImpl for `PartitionEntry<S, Concrete>`.
///
/// Q and Concrete must be the same type (enforced by the QuantizationType
/// match in the caller). Uses Any-based downcasting to bridge the types.
fn codec_for<
    S: IvfSubIndex + 'static,
    Q: Quantization + 'static,
    Concrete: Quantization + 'static,
>() -> lance_core::cache::CacheCodec
where
    PartitionEntry<S, Concrete>: CacheCodecImpl,
{
    lance_core::cache::CacheCodec::new(
        <PartitionEntry<S, Concrete> as CacheCodecImpl>::TYPE_ID,
        <PartitionEntry<S, Concrete> as CacheCodecImpl>::CURRENT_VERSION,
        serialize_partition_entry::<S, Concrete>,
        deserialize_partition_entry::<S, Q, Concrete>,
    )
}

// ---------------------------------------------------------------------------
// Common helpers
// ---------------------------------------------------------------------------

// Distance and rotation discriminants travel as proto enums in the header;
// these map to/from the in-memory Rust enums.

fn distance_type_to_proto(dt: DistanceType) -> PbDistanceType {
    match dt {
        DistanceType::L2 => PbDistanceType::L2,
        DistanceType::Cosine => PbDistanceType::Cosine,
        DistanceType::Dot => PbDistanceType::Dot,
        DistanceType::Hamming => PbDistanceType::Hamming,
    }
}

fn proto_to_distance_type(dt: PbDistanceType) -> DistanceType {
    match dt {
        PbDistanceType::L2 => DistanceType::L2,
        PbDistanceType::Cosine => DistanceType::Cosine,
        PbDistanceType::Dot => DistanceType::Dot,
        PbDistanceType::Hamming => DistanceType::Hamming,
    }
}

fn rotation_type_to_proto(rt: RQRotationType) -> PbRotationType {
    match rt {
        RQRotationType::Matrix => PbRotationType::Matrix,
        RQRotationType::Fast => PbRotationType::Fast,
    }
}

fn proto_to_rotation_type(rt: PbRotationType) -> RQRotationType {
    match rt {
        PbRotationType::Matrix => RQRotationType::Matrix,
        PbRotationType::Fast => RQRotationType::Fast,
    }
}

fn query_estimator_to_proto(qe: RabitQueryEstimator) -> PbRabitQueryEstimator {
    match qe {
        RabitQueryEstimator::ResidualQuery => PbRabitQueryEstimator::ResidualQuery,
        RabitQueryEstimator::RawQuery => PbRabitQueryEstimator::RawQuery,
    }
}

fn proto_to_query_estimator(qe: PbRabitQueryEstimator) -> RabitQueryEstimator {
    match qe {
        PbRabitQueryEstimator::ResidualQuery => RabitQueryEstimator::ResidualQuery,
        PbRabitQueryEstimator::RawQuery => RabitQueryEstimator::RawQuery,
    }
}

/// Read a storage section expected to hold exactly one batch.
fn read_single_storage_batch(r: &mut CacheEntryReader<'_>) -> Result<RecordBatch> {
    let mut batches = r.read_ipc_batches()?;
    match batches.len() {
        1 => Ok(batches.remove(0)),
        n => Err(Error::io(format!(
            "expected exactly 1 storage batch, got {n}"
        ))),
    }
}

/// Wrap a `FixedSizeListArray` in a single-column `RecordBatch` with the given
/// column name.
fn fsl_to_batch(arr: &FixedSizeListArray, name: &str) -> Result<RecordBatch> {
    let field = Field::new(
        name,
        DataType::FixedSizeList(
            Arc::new(Field::new("item", arr.value_type(), true)),
            arr.value_length(),
        ),
        false,
    );
    let schema = Arc::new(Schema::new(vec![field]));
    Ok(RecordBatch::try_new(schema, vec![Arc::new(arr.clone())])?)
}

/// Extract a `FixedSizeListArray` from the first column of a `RecordBatch`.
fn batch_to_fsl(batch: &RecordBatch) -> Result<FixedSizeListArray> {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<FixedSizeListArray>()
        .cloned()
        .ok_or_else(|| Error::io("column is not FixedSizeListArray".to_string()))
}

fn codebook_to_batch(codebook: &FixedSizeListArray) -> Result<RecordBatch> {
    fsl_to_batch(codebook, "codebook")
}

fn batch_to_codebook(batch: &RecordBatch) -> Result<FixedSizeListArray> {
    batch_to_fsl(batch)
}

// ---------------------------------------------------------------------------
// PQ
// ---------------------------------------------------------------------------

impl<S: IvfSubIndex> CacheCodecImpl for PartitionEntry<S, ProductQuantizer> {
    const TYPE_ID: &'static str = "lance.vector.ivf.PartitionEntry.PQ";
    const CURRENT_VERSION: u32 = 1;

    fn serialize(&self, w: &mut CacheEntryWriter<'_>) -> Result<()> {
        let metadata = self.storage.metadata();
        let distance_type = self.storage.distance_type();

        let codebook = metadata.codebook.as_ref().ok_or_else(|| {
            Error::io("PQ metadata missing codebook during serialization".to_string())
        })?;

        let header = PqPartitionHeader {
            distance_type: distance_type_to_proto(distance_type) as i32,
            nbits: metadata.nbits,
            num_sub_vectors: metadata.num_sub_vectors as u64,
            dimension: metadata.dimension as u64,
            transposed: metadata.transposed,
        };

        w.write_header(&header)?;
        w.write_ipc(&self.index.to_batch()?)?;
        w.write_ipc(&codebook_to_batch(codebook)?)?;
        w.write_ipc_batches(self.storage.to_batches()?)?;

        Ok(())
    }

    fn deserialize(r: &mut CacheEntryReader<'_>) -> Result<Self> {
        let header: PqPartitionHeader = r.read_header()?;
        let distance_type = proto_to_distance_type(header.distance_type());

        let sub_index_batch = r.read_ipc()?;
        let codebook_batch = r.read_ipc()?;
        let storage_batch = read_single_storage_batch(r)?;

        let index = S::load(sub_index_batch)?;
        let codebook = batch_to_codebook(&codebook_batch)?;

        let metadata = ProductQuantizationMetadata {
            codebook_position: 0,
            nbits: header.nbits,
            num_sub_vectors: header.num_sub_vectors as usize,
            dimension: header.dimension as usize,
            codebook: Some(codebook),
            codebook_tensor: Vec::new(),
            transposed: header.transposed,
        };

        let storage = <ProductQuantizer as Quantization>::Storage::try_from_batch(
            storage_batch,
            &metadata,
            distance_type,
            None,
        )?;

        Ok(Self { index, storage })
    }
}

// ---------------------------------------------------------------------------
// Flat (Float32)
// ---------------------------------------------------------------------------

impl<S: IvfSubIndex> CacheCodecImpl for PartitionEntry<S, FlatQuantizer> {
    const TYPE_ID: &'static str = "lance.vector.ivf.PartitionEntry.Flat";
    const CURRENT_VERSION: u32 = 1;

    fn serialize(&self, w: &mut CacheEntryWriter<'_>) -> Result<()> {
        let metadata = self.storage.metadata();
        let header = FlatPartitionHeader {
            distance_type: distance_type_to_proto(self.storage.distance_type()) as i32,
            dim: metadata.dim as u64,
        };

        w.write_header(&header)?;
        w.write_ipc(&self.index.to_batch()?)?;
        w.write_ipc_batches(self.storage.to_batches()?)?;

        Ok(())
    }

    fn deserialize(r: &mut CacheEntryReader<'_>) -> Result<Self> {
        let header: FlatPartitionHeader = r.read_header()?;
        let distance_type = proto_to_distance_type(header.distance_type());

        let sub_index_batch = r.read_ipc()?;
        let storage_batch = read_single_storage_batch(r)?;

        let index = S::load(sub_index_batch)?;
        let metadata = FlatMetadata {
            dim: header.dim as usize,
        };
        let storage = <FlatQuantizer as Quantization>::Storage::try_from_batch(
            storage_batch,
            &metadata,
            distance_type,
            None,
        )?;

        Ok(Self { index, storage })
    }
}

// ---------------------------------------------------------------------------
// Flat (Binary / Hamming)
// ---------------------------------------------------------------------------

impl<S: IvfSubIndex> CacheCodecImpl for PartitionEntry<S, FlatBinQuantizer> {
    const TYPE_ID: &'static str = "lance.vector.ivf.PartitionEntry.FlatBin";
    const CURRENT_VERSION: u32 = 1;

    fn serialize(&self, w: &mut CacheEntryWriter<'_>) -> Result<()> {
        let metadata = self.storage.metadata();
        let header = FlatPartitionHeader {
            distance_type: distance_type_to_proto(self.storage.distance_type()) as i32,
            dim: metadata.dim as u64,
        };

        w.write_header(&header)?;
        w.write_ipc(&self.index.to_batch()?)?;
        w.write_ipc_batches(self.storage.to_batches()?)?;

        Ok(())
    }

    fn deserialize(r: &mut CacheEntryReader<'_>) -> Result<Self> {
        let header: FlatPartitionHeader = r.read_header()?;
        let distance_type = proto_to_distance_type(header.distance_type());

        let sub_index_batch = r.read_ipc()?;
        let storage_batch = read_single_storage_batch(r)?;

        let index = S::load(sub_index_batch)?;
        let metadata = FlatMetadata {
            dim: header.dim as usize,
        };
        let storage = <FlatBinQuantizer as Quantization>::Storage::try_from_batch(
            storage_batch,
            &metadata,
            distance_type,
            None,
        )?;

        Ok(Self { index, storage })
    }
}

// ---------------------------------------------------------------------------
// SQ
// ---------------------------------------------------------------------------

impl<S: IvfSubIndex> CacheCodecImpl for PartitionEntry<S, ScalarQuantizer> {
    const TYPE_ID: &'static str = "lance.vector.ivf.PartitionEntry.SQ";
    const CURRENT_VERSION: u32 = 1;

    fn serialize(&self, w: &mut CacheEntryWriter<'_>) -> Result<()> {
        let metadata = self.storage.metadata();
        let header = SqPartitionHeader {
            distance_type: distance_type_to_proto(self.storage.distance_type()) as i32,
            num_bits: metadata.num_bits as u32,
            dim: metadata.dim as u64,
            bounds_start: metadata.bounds.start,
            bounds_end: metadata.bounds.end,
        };

        w.write_header(&header)?;
        w.write_ipc(&self.index.to_batch()?)?;
        // SQ storage may contain multiple batches; write them all in one section.
        w.write_ipc_batches(self.storage.to_batches()?)?;

        Ok(())
    }

    fn deserialize(r: &mut CacheEntryReader<'_>) -> Result<Self> {
        let header: SqPartitionHeader = r.read_header()?;
        let distance_type = proto_to_distance_type(header.distance_type());

        let sub_index_batch = r.read_ipc()?;
        let storage_batches = r.read_ipc_batches()?;

        let index = S::load(sub_index_batch)?;
        let num_bits = header.num_bits as u16;
        let storage = <ScalarQuantizer as Quantization>::Storage::try_new(
            num_bits,
            distance_type,
            header.bounds_start..header.bounds_end,
            storage_batches,
            None,
        )?;

        Ok(Self { index, storage })
    }
}

// ---------------------------------------------------------------------------
// RabitQ
// ---------------------------------------------------------------------------

impl<S: IvfSubIndex> CacheCodecImpl for PartitionEntry<S, RabitQuantizer> {
    const TYPE_ID: &'static str = "lance.vector.ivf.PartitionEntry.Rabit";
    const CURRENT_VERSION: u32 = 1;

    fn serialize(&self, w: &mut CacheEntryWriter<'_>) -> Result<()> {
        let metadata = self.storage.metadata();
        let header = RabitPartitionHeader {
            distance_type: distance_type_to_proto(self.storage.distance_type()) as i32,
            num_bits: metadata.num_bits as u32,
            code_dim: metadata.code_dim,
            rotation_type: rotation_type_to_proto(metadata.rotation_type) as i32,
            query_estimator: query_estimator_to_proto(metadata.query_estimator) as i32,
            fast_rotation_signs: metadata.fast_rotation_signs.clone(),
        };

        w.write_header(&header)?;
        w.write_ipc(&self.index.to_batch()?)?;

        // Write the rotation matrix IPC section only for Matrix rotation; the
        // Fast rotation case stores its signs compactly in the proto header.
        if metadata.rotation_type == RQRotationType::Matrix {
            let mat = metadata.rotate_mat.as_ref().ok_or_else(|| {
                Error::io(
                    "RabitQ Matrix metadata missing rotate_mat during serialization".to_string(),
                )
            })?;
            w.write_ipc(&fsl_to_batch(mat, "rotate_mat")?)?;
        }

        w.write_ipc_batches(self.storage.to_batches()?)?;

        Ok(())
    }

    fn deserialize(r: &mut CacheEntryReader<'_>) -> Result<Self> {
        let header: RabitPartitionHeader = r.read_header()?;
        let distance_type = proto_to_distance_type(header.distance_type());
        let rotation_type = proto_to_rotation_type(header.rotation_type());

        let sub_index_batch = r.read_ipc()?;

        let rotate_mat = if rotation_type == RQRotationType::Matrix {
            let mat_batch = r.read_ipc()?;
            Some(batch_to_fsl(&mat_batch)?)
        } else {
            None
        };

        let storage_batch = read_single_storage_batch(r)?;

        let index = S::load(sub_index_batch)?;
        // Read the proto enum accessor before moving fields out of `header`.
        let query_estimator = proto_to_query_estimator(header.query_estimator());
        let metadata = RabitQuantizationMetadata {
            rotate_mat,
            rotate_mat_position: None,
            fast_rotation_signs: header.fast_rotation_signs,
            rotation_type,
            code_dim: header.code_dim,
            num_bits: header.num_bits as u8,
            // The storage batch already has packed codes; skip re-packing.
            packed: true,
            query_estimator,
        };
        let storage = <RabitQuantizer as Quantization>::Storage::try_from_batch(
            storage_batch,
            &metadata,
            distance_type,
            None,
        )?;

        Ok(Self { index, storage })
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::cast::AsArray;
    use arrow_array::{
        Float16Array, Float32Array, Float64Array, UInt8Array, UInt64Array,
        types::{Float32Type, UInt8Type},
    };
    use arrow_schema::{DataType, Field, Schema};
    use half::f16;
    use lance_arrow::FixedSizeListArrayExt;
    use lance_index::vector::bq::storage::RABIT_CODE_COLUMN;
    use lance_index::vector::bq::transform::{ADD_FACTORS_COLUMN, SCALE_FACTORS_COLUMN};
    use lance_index::vector::bq::{RQRotationType, builder::RabitQuantizer};
    use lance_index::vector::flat::index::FlatIndex;
    use lance_index::vector::flat::storage::FlatFloatStorage;
    use lance_index::vector::sq::storage::ScalarQuantizationStorage;

    /// Serialize a codec body (no envelope) for tests.
    fn ser_body<T: CacheCodecImpl>(entry: &T) -> Vec<u8> {
        let mut buf = Vec::new();
        entry
            .serialize(&mut CacheEntryWriter::new(&mut buf))
            .unwrap();
        buf
    }

    /// Deserialize a codec body (no envelope) at the current build's version.
    fn de_body<T: CacheCodecImpl>(bytes: Vec<u8>) -> Result<T> {
        let data = bytes::Bytes::from(bytes);
        T::deserialize(&mut CacheEntryReader::new(&data, 0, T::CURRENT_VERSION))
    }

    // ----- PQ helpers -------------------------------------------------------

    fn make_test_codebook(dim: usize, num_sub_vectors: usize) -> FixedSizeListArray {
        let sub_dim = dim / num_sub_vectors;
        let num_centroids = 256;
        let total_values = num_sub_vectors * num_centroids * sub_dim;
        let values: Vec<f32> = (0..total_values).map(|i| i as f32 * 0.01).collect();
        let values_array = Float32Array::from(values);
        FixedSizeListArray::try_new_from_values(values_array, sub_dim as i32).unwrap()
    }

    fn make_test_pq_storage(
        num_rows: usize,
        dim: usize,
        num_sub_vectors: usize,
    ) -> <ProductQuantizer as Quantization>::Storage {
        let codebook = make_test_codebook(dim, num_sub_vectors);
        let row_ids = UInt64Array::from((0..num_rows as u64).collect::<Vec<_>>());
        let pq_codes_flat: Vec<u8> = (0..num_rows * num_sub_vectors)
            .map(|i| (i % 256) as u8)
            .collect();
        let pq_codes = UInt8Array::from(pq_codes_flat);
        let pq_codes_fsl =
            FixedSizeListArray::try_new_from_values(pq_codes, num_sub_vectors as i32).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(lance_core::ROW_ID, DataType::UInt64, false),
            Field::new(
                lance_index::vector::PQ_CODE_COLUMN,
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::UInt8, true)),
                    num_sub_vectors as i32,
                ),
                false,
            ),
        ]));

        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(row_ids), Arc::new(pq_codes_fsl)]).unwrap();

        <ProductQuantizer as Quantization>::Storage::new(
            codebook,
            batch,
            8,
            num_sub_vectors,
            dim,
            DistanceType::L2,
            false,
            None,
        )
        .unwrap()
    }

    // ----- PQ tests ---------------------------------------------------------

    #[test]
    fn test_roundtrip_flat_pq() {
        let dim = 128;
        let num_sub_vectors = 16;
        let num_rows = 100;

        let storage = make_test_pq_storage(num_rows, dim, num_sub_vectors);
        let entry = PartitionEntry::<FlatIndex, ProductQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let serialized = ser_body(&entry);
        let deserialized =
            de_body::<PartitionEntry<FlatIndex, ProductQuantizer>>(serialized).unwrap();

        assert_eq!(entry.storage, deserialized.storage);
    }

    #[test]
    fn test_roundtrip_preserves_distance_type() {
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let dim = 32;
            let num_sub_vectors = 4;
            let codebook = make_test_codebook(dim, num_sub_vectors);
            let row_ids = UInt64Array::from(vec![0u64, 1, 2]);
            let pq_codes = UInt8Array::from(vec![0u8; 3 * num_sub_vectors]);
            let pq_codes_fsl =
                FixedSizeListArray::try_new_from_values(pq_codes, num_sub_vectors as i32).unwrap();

            let schema = Arc::new(Schema::new(vec![
                Field::new(lance_core::ROW_ID, DataType::UInt64, false),
                Field::new(
                    lance_index::vector::PQ_CODE_COLUMN,
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::UInt8, true)),
                        num_sub_vectors as i32,
                    ),
                    false,
                ),
            ]));
            let batch =
                RecordBatch::try_new(schema, vec![Arc::new(row_ids), Arc::new(pq_codes_fsl)])
                    .unwrap();

            let storage = <ProductQuantizer as Quantization>::Storage::new(
                codebook,
                batch,
                8,
                num_sub_vectors,
                dim,
                dt,
                false,
                None,
            )
            .unwrap();

            let entry = PartitionEntry::<FlatIndex, ProductQuantizer> {
                index: FlatIndex::default(),
                storage,
            };

            let bytes = ser_body(&entry);
            let restored = de_body::<PartitionEntry<FlatIndex, ProductQuantizer>>(bytes).unwrap();
            assert_eq!(
                restored.storage.distance_type(),
                entry.storage.distance_type()
            );
        }
    }

    #[test]
    fn test_empty_partition() {
        let dim = 16;
        let num_sub_vectors = 2;
        let storage = make_test_pq_storage(0, dim, num_sub_vectors);
        let entry = PartitionEntry::<FlatIndex, ProductQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let serialized = ser_body(&entry);
        let deserialized =
            de_body::<PartitionEntry<FlatIndex, ProductQuantizer>>(serialized).unwrap();
        assert_eq!(entry.storage, deserialized.storage);
    }

    #[test]
    fn test_truncated_data_errors() {
        // Serialize a valid entry, then truncate the bytes and verify that
        // deserialization fails rather than panicking.
        let storage = make_test_pq_storage(1, 16, 2);
        let entry = PartitionEntry::<FlatIndex, ProductQuantizer> {
            index: FlatIndex::default(),
            storage,
        };
        let mut bytes = ser_body(&entry);
        bytes.truncate(3);
        assert!(de_body::<PartitionEntry<FlatIndex, ProductQuantizer>>(bytes).is_err());
    }

    // ----- Flat helpers -----------------------------------------------------

    fn make_flat_storage(num_rows: usize, dim: usize) -> FlatFloatStorage {
        let values: Vec<f32> = (0..num_rows * dim).map(|i| i as f32 * 0.01).collect();
        let values_array = Float32Array::from(values);
        let vectors = FixedSizeListArray::try_new_from_values(values_array, dim as i32).unwrap();
        FlatFloatStorage::new(vectors, DistanceType::L2)
    }

    fn make_flat_storage_f16(num_rows: usize, dim: usize) -> FlatFloatStorage {
        let values: Vec<f16> = (0..num_rows * dim)
            .map(|i| f16::from_f32(i as f32 * 0.01))
            .collect();
        let values_array = Float16Array::from(values);
        let vectors = FixedSizeListArray::try_new_from_values(values_array, dim as i32).unwrap();
        FlatFloatStorage::new(vectors, DistanceType::L2)
    }

    fn make_flat_storage_f64(num_rows: usize, dim: usize) -> FlatFloatStorage {
        let values: Vec<f64> = (0..num_rows * dim).map(|i| i as f64 * 0.01).collect();
        let values_array = Float64Array::from(values);
        let vectors = FixedSizeListArray::try_new_from_values(values_array, dim as i32).unwrap();
        FlatFloatStorage::new(vectors, DistanceType::L2)
    }

    // ----- Flat tests -------------------------------------------------------

    #[test]
    fn test_roundtrip_flat_flat() {
        let storage = make_flat_storage(50, 64);
        let entry = PartitionEntry::<FlatIndex, FlatQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = ser_body(&entry);
        let restored = de_body::<PartitionEntry<FlatIndex, FlatQuantizer>>(bytes).unwrap();

        assert_eq!(
            restored.storage.metadata().dim,
            entry.storage.metadata().dim
        );
        assert_eq!(
            restored.storage.distance_type(),
            entry.storage.distance_type()
        );
        assert_eq!(restored.storage.len(), entry.storage.len());
        let orig_batch = entry.storage.to_batches().unwrap().next().unwrap();
        let rest_batch = restored.storage.to_batches().unwrap().next().unwrap();
        assert_eq!(orig_batch, rest_batch);
    }

    #[test]
    fn test_flat_distance_types() {
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let values = Float32Array::from(vec![1.0f32; 32]);
            let vectors = FixedSizeListArray::try_new_from_values(values, 32).unwrap();
            let storage = FlatFloatStorage::new(vectors, dt);
            let entry = PartitionEntry::<FlatIndex, FlatQuantizer> {
                index: FlatIndex::default(),
                storage,
            };
            let bytes = ser_body(&entry);
            let restored = de_body::<PartitionEntry<FlatIndex, FlatQuantizer>>(bytes).unwrap();
            assert_eq!(restored.storage.distance_type(), dt);
        }
    }

    #[test]
    fn test_roundtrip_flat_flat_f16() {
        let storage = make_flat_storage_f16(8, 16);
        let entry = PartitionEntry::<FlatIndex, FlatQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = ser_body(&entry);
        let restored = de_body::<PartitionEntry<FlatIndex, FlatQuantizer>>(bytes).unwrap();

        let restored_batch = restored.storage.to_batches().unwrap().next().unwrap();
        let schema = restored_batch.schema();
        let field = schema
            .field_with_name(lance_index::vector::flat::storage::FLAT_COLUMN)
            .unwrap();
        let DataType::FixedSizeList(item, _) = field.data_type() else {
            panic!("flat column should be fixed size list");
        };
        assert_eq!(item.data_type(), &DataType::Float16);
    }

    #[test]
    fn test_roundtrip_flat_flat_f64() {
        let storage = make_flat_storage_f64(8, 16);
        let entry = PartitionEntry::<FlatIndex, FlatQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = ser_body(&entry);
        let restored = de_body::<PartitionEntry<FlatIndex, FlatQuantizer>>(bytes).unwrap();

        let restored_batch = restored.storage.to_batches().unwrap().next().unwrap();
        let schema = restored_batch.schema();
        let field = schema
            .field_with_name(lance_index::vector::flat::storage::FLAT_COLUMN)
            .unwrap();
        let DataType::FixedSizeList(item, _) = field.data_type() else {
            panic!("flat column should be fixed size list");
        };
        assert_eq!(item.data_type(), &DataType::Float64);
    }

    // ----- SQ helpers -------------------------------------------------------

    fn make_sq_storage(
        num_rows: usize,
        dim: usize,
        distance_type: DistanceType,
    ) -> ScalarQuantizationStorage {
        let row_ids = UInt64Array::from_iter_values(0..num_rows as u64);
        let sq_codes_flat: Vec<u8> = (0..num_rows * dim).map(|i| (i % 256) as u8).collect();
        let sq_codes = UInt8Array::from(sq_codes_flat);
        let sq_codes_fsl = FixedSizeListArray::try_new_from_values(sq_codes, dim as i32).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new(lance_core::ROW_ID, DataType::UInt64, false),
            Field::new(
                lance_index::vector::SQ_CODE_COLUMN,
                DataType::FixedSizeList(
                    Arc::new(Field::new("item", DataType::UInt8, true)),
                    dim as i32,
                ),
                false,
            ),
        ]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(row_ids), Arc::new(sq_codes_fsl)]).unwrap();

        ScalarQuantizationStorage::try_new(8, distance_type, -1.0..1.0, [batch], None).unwrap()
    }

    // ----- SQ tests ---------------------------------------------------------

    #[test]
    fn test_roundtrip_flat_sq() {
        let storage = make_sq_storage(100, 64, DistanceType::L2);
        let entry = PartitionEntry::<FlatIndex, ScalarQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = ser_body(&entry);
        let restored = de_body::<PartitionEntry<FlatIndex, ScalarQuantizer>>(bytes).unwrap();

        let m = entry.storage.metadata();
        let rm = restored.storage.metadata();
        assert_eq!(rm.dim, m.dim);
        assert_eq!(rm.num_bits, m.num_bits);
        assert_eq!(rm.bounds, m.bounds);
        assert_eq!(
            restored.storage.distance_type(),
            entry.storage.distance_type()
        );
        assert_eq!(restored.storage.len(), entry.storage.len());

        let orig_ids: Vec<u64> = entry.storage.row_ids().copied().collect();
        let rest_ids: Vec<u64> = restored.storage.row_ids().copied().collect();
        assert_eq!(orig_ids, rest_ids);
    }

    #[test]
    fn test_sq_distance_types() {
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let storage = make_sq_storage(10, 16, dt);
            let entry = PartitionEntry::<FlatIndex, ScalarQuantizer> {
                index: FlatIndex::default(),
                storage,
            };
            let bytes = ser_body(&entry);
            let restored = de_body::<PartitionEntry<FlatIndex, ScalarQuantizer>>(bytes).unwrap();
            assert_eq!(restored.storage.distance_type(), dt);
        }
    }

    #[test]
    fn test_sq_multiple_chunks_no_copy() {
        // Build SQ storage with multiple chunks by appending batches separately.
        let dim = 16usize;
        let make_batch = |start: u64, n: usize| {
            let row_ids = UInt64Array::from_iter_values(start..start + n as u64);
            let codes = UInt8Array::from(vec![0u8; n * dim]);
            let fsl = FixedSizeListArray::try_new_from_values(codes, dim as i32).unwrap();
            let schema = Arc::new(Schema::new(vec![
                Field::new(lance_core::ROW_ID, DataType::UInt64, false),
                Field::new(
                    lance_index::vector::SQ_CODE_COLUMN,
                    DataType::FixedSizeList(
                        Arc::new(Field::new("item", DataType::UInt8, true)),
                        dim as i32,
                    ),
                    false,
                ),
            ]));
            RecordBatch::try_new(schema, vec![Arc::new(row_ids), Arc::new(fsl)]).unwrap()
        };
        // Three chunks with 10 rows each.
        let storage = ScalarQuantizationStorage::try_new(
            8,
            DistanceType::L2,
            -1.0..1.0,
            [make_batch(0, 10), make_batch(10, 10), make_batch(20, 10)],
            None,
        )
        .unwrap();
        assert_eq!(storage.len(), 30);

        let entry = PartitionEntry::<FlatIndex, ScalarQuantizer> {
            index: FlatIndex::default(),
            storage,
        };
        let bytes = ser_body(&entry);
        let restored = de_body::<PartitionEntry<FlatIndex, ScalarQuantizer>>(bytes).unwrap();

        assert_eq!(restored.storage.len(), 30);
        let orig_ids: Vec<u64> = entry.storage.row_ids().copied().collect();
        let rest_ids: Vec<u64> = restored.storage.row_ids().copied().collect();
        assert_eq!(orig_ids, rest_ids);
    }

    // ----- RabitQ helpers ---------------------------------------------------

    fn make_rabit_storage_fast(
        num_rows: usize,
        code_dim: usize,
        distance_type: DistanceType,
    ) -> <RabitQuantizer as Quantization>::Storage {
        make_rabit_storage(
            num_rows,
            code_dim,
            distance_type,
            RQRotationType::Fast,
            RabitQueryEstimator::ResidualQuery,
        )
    }

    fn make_rabit_storage(
        num_rows: usize,
        code_dim: usize,
        distance_type: DistanceType,
        rotation_type: RQRotationType,
        query_estimator: RabitQueryEstimator,
    ) -> <RabitQuantizer as Quantization>::Storage {
        use lance_arrow::FixedSizeListArrayExt;

        let quantizer =
            RabitQuantizer::new_with_rotation::<Float32Type>(1, code_dim as i32, rotation_type);
        let values: Vec<f32> = (0..num_rows * code_dim)
            .map(|i| (i % 100) as f32 / 100.0 - 0.5)
            .collect();
        let values_arr = Float32Array::from(values);
        let vectors = FixedSizeListArray::try_new_from_values(values_arr, code_dim as i32).unwrap();
        let codes = quantizer
            .quantize(&vectors)
            .unwrap()
            .as_fixed_size_list()
            .clone();

        let mut metadata = quantizer.metadata(None);
        metadata.query_estimator = query_estimator;
        let batch = RecordBatch::try_from_iter(vec![
            (
                lance_core::ROW_ID,
                Arc::new(UInt64Array::from_iter_values(0..num_rows as u64))
                    as Arc<dyn arrow_array::Array>,
            ),
            (
                RABIT_CODE_COLUMN,
                Arc::new(codes) as Arc<dyn arrow_array::Array>,
            ),
            (
                ADD_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|i| i as f32 * 0.1),
                )) as Arc<dyn arrow_array::Array>,
            ),
            (
                SCALE_FACTORS_COLUMN,
                Arc::new(Float32Array::from_iter_values(
                    (0..num_rows).map(|i| i as f32 * 0.01 + 0.5),
                )) as Arc<dyn arrow_array::Array>,
            ),
        ])
        .unwrap();

        <RabitQuantizer as Quantization>::Storage::try_from_batch(
            batch,
            &metadata,
            distance_type,
            None,
        )
        .unwrap()
    }

    // ----- RabitQ tests -----------------------------------------------------

    #[test]
    fn test_roundtrip_flat_rabitq_fast() {
        let num_rows = 50;
        let code_dim = 64;
        let storage = make_rabit_storage_fast(num_rows, code_dim, DistanceType::L2);
        let entry = PartitionEntry::<FlatIndex, RabitQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = ser_body(&entry);
        let restored = de_body::<PartitionEntry<FlatIndex, RabitQuantizer>>(bytes).unwrap();

        let m = entry.storage.metadata();
        let rm = restored.storage.metadata();
        assert_eq!(rm.num_bits, m.num_bits);
        assert_eq!(rm.code_dim, m.code_dim);
        assert_eq!(rm.rotation_type, m.rotation_type);
        assert_eq!(rm.query_estimator, m.query_estimator);
        assert_eq!(rm.fast_rotation_signs, m.fast_rotation_signs);
        assert!(rm.packed);
        assert_eq!(
            restored.storage.distance_type(),
            entry.storage.distance_type()
        );
        assert_eq!(restored.storage.len(), entry.storage.len());

        let orig_ids: Vec<u64> = entry.storage.row_ids().copied().collect();
        let rest_ids: Vec<u64> = restored.storage.row_ids().copied().collect();
        assert_eq!(orig_ids, rest_ids);

        let orig_batch = entry.storage.to_batches().unwrap().next().unwrap();
        let rest_batch = restored.storage.to_batches().unwrap().next().unwrap();
        let orig_codes = orig_batch[RABIT_CODE_COLUMN].as_fixed_size_list();
        let rest_codes = rest_batch[RABIT_CODE_COLUMN].as_fixed_size_list();
        assert_eq!(
            orig_codes.values().as_primitive::<UInt8Type>().values(),
            rest_codes.values().as_primitive::<UInt8Type>().values(),
        );
    }

    #[test]
    fn test_rabitq_distance_types() {
        for dt in [DistanceType::L2, DistanceType::Cosine, DistanceType::Dot] {
            let storage = make_rabit_storage_fast(10, 32, dt);
            let entry = PartitionEntry::<FlatIndex, RabitQuantizer> {
                index: FlatIndex::default(),
                storage,
            };
            let bytes = ser_body(&entry);
            let restored = de_body::<PartitionEntry<FlatIndex, RabitQuantizer>>(bytes).unwrap();
            // The codec round-trips the distance type faithfully.
            assert_eq!(
                restored.storage.distance_type(),
                entry.storage.distance_type()
            );
        }
    }

    #[test]
    fn test_roundtrip_rabitq_raw_query_estimator() {
        // The query estimator is a non-default value here; it must survive the
        // round trip so raw-query search keeps working after a cache reload.
        let storage = make_rabit_storage(
            40,
            32,
            DistanceType::L2,
            RQRotationType::Fast,
            RabitQueryEstimator::RawQuery,
        );
        assert_eq!(
            storage.metadata().query_estimator,
            RabitQueryEstimator::RawQuery
        );
        let entry = PartitionEntry::<FlatIndex, RabitQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = ser_body(&entry);
        let restored = de_body::<PartitionEntry<FlatIndex, RabitQuantizer>>(bytes).unwrap();
        assert_eq!(
            restored.storage.metadata().query_estimator,
            RabitQueryEstimator::RawQuery
        );
    }

    /// Matrix rotation writes an extra `rotate_mat` IPC section between the
    /// sub-index and storage sections; exercise that the codec preserves it.
    #[test]
    fn test_roundtrip_flat_rabitq_matrix() {
        let storage = make_rabit_storage(
            40,
            32,
            DistanceType::L2,
            RQRotationType::Matrix,
            RabitQueryEstimator::ResidualQuery,
        );
        let entry = PartitionEntry::<FlatIndex, RabitQuantizer> {
            index: FlatIndex::default(),
            storage,
        };

        let bytes = ser_body(&entry);
        let restored = de_body::<PartitionEntry<FlatIndex, RabitQuantizer>>(bytes).unwrap();

        let m = entry.storage.metadata();
        let rm = restored.storage.metadata();
        assert_eq!(rm.rotation_type, RQRotationType::Matrix);
        assert_eq!(rm.code_dim, m.code_dim);
        assert_eq!(rm.num_bits, m.num_bits);
        // The rotation matrix itself must survive the round trip.
        let orig_mat = m
            .rotate_mat
            .as_ref()
            .expect("matrix rotation has rotate_mat");
        let rest_mat = rm
            .rotate_mat
            .as_ref()
            .expect("restored matrix rotation has rotate_mat");
        assert_eq!(
            orig_mat.values().as_primitive::<Float32Type>().values(),
            rest_mat.values().as_primitive::<Float32Type>().values(),
        );
    }

    /// SQ storage (a multi-batch IPC section) must decode zero-copy through the
    /// full envelope even though the proto header and sub-index section push it
    /// to a non-aligned starting offset.
    #[test]
    fn test_partition_storage_is_zero_copy_through_envelope() {
        use lance_core::cache::CacheCodec;
        const ALIGN: usize = 64;

        let entry = PartitionEntry::<FlatIndex, ScalarQuantizer> {
            index: FlatIndex::default(),
            storage: make_sq_storage(64, 32, DistanceType::L2),
        };
        let codec = CacheCodec::from_impl::<PartitionEntry<FlatIndex, ScalarQuantizer>>();
        let any: Arc<dyn std::any::Any + Send + Sync> = Arc::new(entry);
        let mut buf = Vec::new();
        codec.serialize(&any, &mut buf).unwrap();

        let mut v = vec![0u8; buf.len() + ALIGN];
        let pad = (ALIGN - (v.as_ptr() as usize % ALIGN)) % ALIGN;
        v[pad..pad + buf.len()].copy_from_slice(&buf);
        let data = bytes::Bytes::from(v).slice(pad..pad + buf.len());

        let restored = codec.deserialize(&data).hit().unwrap();
        let restored = restored
            .downcast::<PartitionEntry<FlatIndex, ScalarQuantizer>>()
            .unwrap();

        let base = data.as_ptr() as usize;
        let end = base + data.len();
        let first = restored.storage.to_batches().unwrap().next().unwrap();
        for col in first.columns() {
            for buffer in col.to_data().buffers() {
                let ptr = buffer.as_ptr() as usize;
                assert!(
                    ptr >= base && ptr < end,
                    "storage buffer was realigned out of the input — misaligned IPC section",
                );
            }
        }
    }

    #[test]
    fn test_ivf_index_state_roundtrip() {
        use crate::index::vector::ivf::v2::{
            IvfIndexState, IvfStateEntryBox, empty_rabit_search_cache_cell,
        };
        use lance_index::vector::flat::index::FlatQuantizer;
        use lance_index::vector::ivf::storage::IvfModel;
        use lance_index::vector::quantizer::QuantizationType;
        use lance_index::vector::v3::subindex::SubIndexType;

        // Build a minimal IvfModel (single centroid, dim=2).
        let centroids =
            FixedSizeListArray::try_new_from_values(Float32Array::from(vec![0.0f32, 1.0]), 2)
                .unwrap();
        let ivf = IvfModel::new(centroids, None);

        let state = IvfIndexState::<FlatQuantizer> {
            index_file_path: "my/index.lance".to_string(),
            uuid: "test-uuid-1234".to_string(),
            ivf: ivf.clone(),
            aux_ivf: ivf,
            distance_type: DistanceType::L2,
            sub_index_metadata: vec!["meta1".to_string()],
            metadata: lance_index::vector::flat::index::FlatMetadata { dim: 2 },
            sub_index_type: SubIndexType::Flat,
            quantization_type: QuantizationType::Flat,
            index_file_size: 1024,
            aux_file_size: 512,
            rq_search_cache: empty_rabit_search_cache_cell(),
        };

        let entry = IvfStateEntryBox(Arc::new(state));

        let bytes = ser_body(&entry);
        let restored = de_body::<IvfStateEntryBox>(bytes.clone()).unwrap();

        // Re-serialize the restored entry and compare bytes — a stronger check
        // than field-by-field comparison and avoids needing to downcast.
        let restored_bytes = ser_body(&restored);
        assert_eq!(bytes, restored_bytes);
    }
}
