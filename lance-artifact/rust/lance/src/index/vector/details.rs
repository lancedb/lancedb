// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Serialization and deserialization of [`VectorIndexDetails`] proto messages.
//!
//! This module handles:
//! - Populating `VectorIndexDetails` from build params at index creation time
//! - Deriving a human-readable index type string (e.g., "IVF_PQ") from details
//! - Serializing details as JSON for `describe_indices()`
//! - Inferring details from index files on disk (fallback for legacy indices)

use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;

use lance_file::reader::FileReaderOptions;
use lance_index::pb::VectorIndexDetails;
use lance_index::pb::VectorMetricType;
use lance_index::pb::index::Implementation;
use lance_index::pb::vector_index_details::{Compression, FlatCompression, rabit_quantization};
use lance_index::{INDEX_FILE_NAME, INDEX_METADATA_SCHEMA_KEY, pb};
use lance_io::scheduler::{ScanScheduler, SchedulerConfig};
use lance_io::traits::Reader;
use lance_io::utils::{CachedFileSize, read_last_block, read_version};
use lance_linalg::distance::DistanceType;
use lance_table::format::IndexMetadata;
use serde::Serialize;

use lance_index::vector::bq::{RQBuildParams, RQRotationType};
use lance_index::vector::hnsw::builder::HnswBuildParams;
use lance_index::vector::ivf::IvfBuildParams;
use lance_index::vector::pq::PQBuildParams;
use lance_index::vector::sq::builder::SQBuildParams;

use super::{StageParams, VectorIndexParams};
use crate::dataset::Dataset;
use crate::index::open_index_proto;
use crate::{Error, Result};

// Private structs for JSON serialization of VectorIndexDetails.
// Changes to field names or structure are backwards-incompatible for users
// parsing the JSON output of describe_indices(). See snapshot tests below.

#[derive(Serialize)]
struct VectorDetailsJson {
    metric_type: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    target_partition_size: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    hnsw: Option<HnswDetailsJson>,
    #[serde(skip_serializing_if = "Option::is_none")]
    compression: Option<CompressionDetailsJson>,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    runtime_hints: HashMap<String, String>,
}

#[derive(Serialize)]
struct HnswDetailsJson {
    max_connections: u32,
    construction_ef: u32,
    #[serde(skip_serializing_if = "is_zero")]
    max_level: u32,
}

fn is_zero(v: &u32) -> bool {
    *v == 0
}

#[derive(Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum CompressionDetailsJson {
    Pq {
        num_bits: u32,
        num_sub_vectors: u32,
    },
    Sq {
        num_bits: u32,
    },
    Rq {
        num_bits: u32,
        rotation_type: &'static str,
    },
}

/// Build a `VectorIndexDetails` proto from build params at index creation time.
pub fn vector_index_details(params: &VectorIndexParams) -> prost_types::Any {
    let metric_type = match params.metric_type {
        lance_linalg::distance::DistanceType::L2 => VectorMetricType::L2,
        lance_linalg::distance::DistanceType::Cosine => VectorMetricType::Cosine,
        lance_linalg::distance::DistanceType::Dot => VectorMetricType::Dot,
        lance_linalg::distance::DistanceType::Hamming => VectorMetricType::Hamming,
    };

    let mut target_partition_size = 0u64;
    let mut hnsw_index_config = None;
    let mut compression = None;
    let mut runtime_hints: HashMap<String, String> = params.runtime_hints.clone();

    for stage in &params.stages {
        match stage {
            StageParams::Ivf(ivf) => {
                if let Some(tps) = ivf.target_partition_size {
                    target_partition_size = tps as u64;
                }
                runtime_hints.insert("lance.ivf.max_iters".to_string(), ivf.max_iters.to_string());
                runtime_hints.insert(
                    "lance.ivf.sample_rate".to_string(),
                    ivf.sample_rate.to_string(),
                );
                runtime_hints.insert(
                    "lance.ivf.shuffle_partition_batches".to_string(),
                    ivf.shuffle_partition_batches.to_string(),
                );
                runtime_hints.insert(
                    "lance.ivf.shuffle_partition_concurrency".to_string(),
                    ivf.shuffle_partition_concurrency.to_string(),
                );
            }
            StageParams::Hnsw(hnsw) => {
                hnsw_index_config = Some(hnsw.into());
                let val = match hnsw.prefetch_distance {
                    Some(v) => v.to_string(),
                    None => "none".to_string(),
                };
                runtime_hints.insert("lance.hnsw.prefetch_distance".to_string(), val);
            }
            StageParams::PQ(pq) => {
                compression = Some(Compression::Pq(pq.into()));
                runtime_hints.insert("lance.pq.max_iters".to_string(), pq.max_iters.to_string());
                runtime_hints.insert(
                    "lance.pq.sample_rate".to_string(),
                    pq.sample_rate.to_string(),
                );
                runtime_hints.insert(
                    "lance.pq.kmeans_redos".to_string(),
                    pq.kmeans_redos.to_string(),
                );
            }
            StageParams::SQ(sq) => {
                compression = Some(Compression::Sq(sq.into()));
                runtime_hints.insert(
                    "lance.sq.sample_rate".to_string(),
                    sq.sample_rate.to_string(),
                );
            }
            StageParams::RQ(rq) => {
                compression = Some(Compression::Rq(rq.into()));
            }
        }
    }

    runtime_hints.insert(
        "lance.skip_transpose".to_string(),
        params.skip_transpose.to_string(),
    );

    let compression = compression.or(Some(Compression::Flat(FlatCompression {})));

    let details = VectorIndexDetails {
        metric_type: metric_type.into(),
        target_partition_size,
        hnsw_index_config,
        compression,
        runtime_hints,
    };
    prost_types::Any::from_msg(&details).unwrap()
}

pub fn vector_index_details_default() -> prost_types::Any {
    let details = lance_index::pb::VectorIndexDetails::default();
    prost_types::Any::from_msg(&details).unwrap()
}

/// Apply stored runtime hints from `VectorIndexDetails` back into build params.
///
/// Known `lance.*` keys are parsed and applied to the appropriate stage. Unknown
/// keys (e.g., from other runtimes) are silently ignored. Malformed values are
/// also silently ignored — the stage keeps its existing default.
// TODO: wire into a general `Dataset::rebuild_index` method so users can
// regenerate an index from its stored details (e.g. after file corruption).
#[allow(dead_code)]
pub fn apply_runtime_hints(hints: &HashMap<String, String>, params: &mut VectorIndexParams) {
    fn parse<T: FromStr>(hints: &HashMap<String, String>, key: &str) -> Option<T> {
        hints.get(key)?.parse().ok()
    }

    if let Some(v) = parse::<bool>(hints, "lance.skip_transpose") {
        params.skip_transpose = v;
    }

    for stage in &mut params.stages {
        match stage {
            StageParams::Ivf(ivf) => {
                if let Some(v) = parse(hints, "lance.ivf.max_iters") {
                    ivf.max_iters = v;
                }
                if let Some(v) = parse(hints, "lance.ivf.sample_rate") {
                    ivf.sample_rate = v;
                }
                if let Some(v) = parse(hints, "lance.ivf.shuffle_partition_batches") {
                    ivf.shuffle_partition_batches = v;
                }
                if let Some(v) = parse(hints, "lance.ivf.shuffle_partition_concurrency") {
                    ivf.shuffle_partition_concurrency = v;
                }
            }
            StageParams::Hnsw(hnsw) => {
                if let Some(raw) = hints.get("lance.hnsw.prefetch_distance") {
                    hnsw.prefetch_distance = if raw == "none" {
                        None
                    } else {
                        raw.parse().ok()
                    };
                }
            }
            StageParams::PQ(pq) => {
                if let Some(v) = parse(hints, "lance.pq.max_iters") {
                    pq.max_iters = v;
                }
                if let Some(v) = parse(hints, "lance.pq.sample_rate") {
                    pq.sample_rate = v;
                }
                if let Some(v) = parse(hints, "lance.pq.kmeans_redos") {
                    pq.kmeans_redos = v;
                }
            }
            StageParams::SQ(sq) => {
                if let Some(v) = parse(hints, "lance.sq.sample_rate") {
                    sq.sample_rate = v;
                }
            }
            StageParams::RQ(_) => {}
        }
    }
}

/// Reconstruct `VectorIndexParams` from a stored `VectorIndexDetails` proto.
///
/// Returns `None` for legacy indices (empty details) or if the proto is malformed.
/// Runtime hints are applied on top of the reconstructed spec.
// TODO: wire into a general `Dataset::rebuild_index` method so users can
// regenerate an index from its stored details (e.g. after file corruption).
#[allow(dead_code)]
pub fn vector_params_from_details(details: &prost_types::Any) -> Option<VectorIndexParams> {
    if details.value.is_empty() {
        return None;
    }
    let d = details.to_msg::<VectorIndexDetails>().ok()?;

    let metric = DistanceType::from(VectorMetricType::try_from(d.metric_type).ok()?);

    let mut ivf = IvfBuildParams::default();
    if d.target_partition_size > 0 {
        ivf.target_partition_size = Some(d.target_partition_size as usize);
    }

    let hnsw = d.hnsw_index_config.map(|h| HnswBuildParams {
        m: h.max_connections as usize,
        ef_construction: h.construction_ef as usize,
        max_level: h.max_level as u16,
        ..Default::default()
    });

    let mut params = match (hnsw, d.compression) {
        (None, Some(Compression::Pq(pq))) => VectorIndexParams::with_ivf_pq_params(
            metric,
            ivf,
            PQBuildParams {
                num_bits: pq.num_bits as usize,
                num_sub_vectors: pq.num_sub_vectors as usize,
                ..Default::default()
            },
        ),
        (None, Some(Compression::Sq(sq))) => VectorIndexParams::with_ivf_sq_params(
            metric,
            ivf,
            SQBuildParams {
                num_bits: sq.num_bits as u16,
                ..Default::default()
            },
        ),
        (None, Some(Compression::Rq(rq))) => {
            let rotation_type =
                match rabit_quantization::RotationType::try_from(rq.rotation_type).ok()? {
                    rabit_quantization::RotationType::Matrix => RQRotationType::Matrix,
                    rabit_quantization::RotationType::Fast => RQRotationType::Fast,
                };
            VectorIndexParams::with_ivf_rq_params(
                metric,
                ivf,
                RQBuildParams::with_rotation_type(rq.num_bits as u8, rotation_type),
            )
        }
        (Some(hnsw), Some(Compression::Pq(pq))) => VectorIndexParams::with_ivf_hnsw_pq_params(
            metric,
            ivf,
            hnsw,
            PQBuildParams {
                num_bits: pq.num_bits as usize,
                num_sub_vectors: pq.num_sub_vectors as usize,
                ..Default::default()
            },
        ),
        (Some(hnsw), Some(Compression::Sq(sq))) => VectorIndexParams::with_ivf_hnsw_sq_params(
            metric,
            ivf,
            hnsw,
            SQBuildParams {
                num_bits: sq.num_bits as u16,
                ..Default::default()
            },
        ),
        (Some(hnsw), _) => VectorIndexParams::ivf_hnsw(metric, ivf, hnsw),
        _ => VectorIndexParams::with_ivf_flat_params(metric, ivf),
    };

    apply_runtime_hints(&d.runtime_hints, &mut params);
    Some(params)
}

/// Extract metric type from index metadata without opening the index file.
///
/// For newer indices with populated `VectorIndexDetails`, returns the metric type directly.
/// For legacy indices without details, returns `None` and caller should fall back to opening the index.
///
/// # Arguments
/// * `index` - The index metadata containing serialized VectorIndexDetails
///
/// # Returns
/// * `Some(DistanceType)` if details are present and valid
/// * `None` if details are absent or empty (legacy index without details)
pub fn metric_type_from_index_metadata(index: &IndexMetadata) -> Option<DistanceType> {
    let index_details = index.index_details.as_ref()?;

    // Empty value bytes indicates legacy index that needs to be opened for details
    if index_details.value.is_empty() {
        return None;
    }

    let details = index_details.to_msg::<VectorIndexDetails>().ok()?;

    // Try to convert the metric_type field. This works even if metric_type is 0 (L2),
    // since L2 is a valid metric type.
    let metric_enum = VectorMetricType::try_from(details.metric_type).ok()?;
    Some(DistanceType::from(metric_enum))
}

/// Returns true if the proto value represents a "truly empty" VectorIndexDetails
/// (i.e., a legacy index that was created before we populated this field).
fn is_empty_vector_details(details: &prost_types::Any) -> bool {
    details.value.is_empty()
}

/// Returns true if this is a vector index whose details need to be inferred from disk.
///
/// This covers two legacy cases:
/// - Very old indices (<=0.19.2) where `index_details` is `None` but the indexed
///   field is a vector type
/// - Newer pre-details indices where `index_details` has a VectorIndexDetails
///   type_url but empty value bytes
pub fn needs_vector_details_inference(
    index: &IndexMetadata,
    schema: &lance_core::datatypes::Schema,
) -> bool {
    match &index.index_details {
        Some(d) => d.type_url.ends_with("VectorIndexDetails") && d.value.is_empty(),
        None => index.fields.iter().any(|&field_id| {
            schema
                .field_by_id(field_id)
                .map(|f| matches!(f.data_type(), arrow_schema::DataType::FixedSizeList(_, _)))
                .unwrap_or(false)
        }),
    }
}

/// Infer missing vector index details for all indices that need it.
///
/// Runs inference once per unique index name, concurrently across names.
/// Applies the inferred details back to all matching indices in the slice.
pub async fn infer_missing_vector_details(dataset: &Dataset, indices: &mut [IndexMetadata]) {
    let schema = dataset.schema();
    let needs_inference: HashMap<&str, &IndexMetadata> = indices
        .iter()
        .filter(|idx| needs_vector_details_inference(idx, schema))
        .map(|idx| (idx.name.as_str(), idx))
        .collect();
    if needs_inference.is_empty() {
        return;
    }
    let inferred: HashMap<String, Arc<prost_types::Any>> =
        futures::future::join_all(needs_inference.into_iter().map(
            |(name, representative)| async move {
                let result = infer_vector_index_details(dataset, representative).await;
                (name.to_string(), result)
            },
        ))
        .await
        .into_iter()
        .filter_map(|(name, result)| match result {
            Ok(details) => Some((name, Arc::new(details))),
            Err(err) => {
                tracing::warn!("Could not infer vector index details for {}: {}", name, err);
                None
            }
        })
        .collect();
    for index in indices.iter_mut() {
        if let Some(details) = inferred.get(&index.name) {
            index.index_details = Some(details.clone());
        }
    }
}

/// Derive a human-readable index type string from VectorIndexDetails.
pub fn derive_vector_index_type(details: &prost_types::Any) -> String {
    if is_empty_vector_details(details) {
        return "Vector".to_string();
    }

    let Ok(d) = details.to_msg::<VectorIndexDetails>() else {
        return "Vector".to_string();
    };
    let mut index_type = "IVF_".to_string();
    if d.hnsw_index_config.is_some() {
        index_type.push_str("HNSW_");
    }
    match d.compression {
        None | Some(Compression::Flat(_)) => index_type.push_str("FLAT"),
        Some(Compression::Pq(_)) => index_type.push_str("PQ"),
        Some(Compression::Sq(_)) => index_type.push_str("SQ"),
        Some(Compression::Rq(_)) => index_type.push_str("RQ"),
    }
    index_type
}

/// Serialize VectorIndexDetails as a JSON string.
pub fn vector_details_as_json(details: &prost_types::Any) -> Result<String> {
    if is_empty_vector_details(details) {
        return Ok("{}".to_string());
    }

    let d = details
        .to_msg::<VectorIndexDetails>()
        .map_err(|e| Error::index(format!("Failed to deserialize VectorIndexDetails: {}", e)))?;

    let metric_type = match VectorMetricType::try_from(d.metric_type) {
        Ok(VectorMetricType::L2) => "L2",
        Ok(VectorMetricType::Cosine) => "COSINE",
        Ok(VectorMetricType::Dot) => "DOT",
        Ok(VectorMetricType::Hamming) => "HAMMING",
        Err(_) => "UNKNOWN",
    };

    let hnsw = d.hnsw_index_config.map(|h| HnswDetailsJson {
        max_connections: h.max_connections,
        construction_ef: h.construction_ef,
        max_level: h.max_level,
    });

    let compression = d.compression.and_then(|c| match c {
        Compression::Flat(_) => None,
        Compression::Pq(pq) => Some(CompressionDetailsJson::Pq {
            num_bits: pq.num_bits,
            num_sub_vectors: pq.num_sub_vectors,
        }),
        Compression::Sq(sq) => Some(CompressionDetailsJson::Sq {
            num_bits: sq.num_bits,
        }),
        Compression::Rq(rq) => {
            let rotation_type = match rabit_quantization::RotationType::try_from(rq.rotation_type) {
                Ok(rabit_quantization::RotationType::Matrix) => "matrix",
                _ => "fast",
            };
            Some(CompressionDetailsJson::Rq {
                num_bits: rq.num_bits,
                rotation_type,
            })
        }
    });

    let json = VectorDetailsJson {
        metric_type,
        target_partition_size: if d.target_partition_size > 0 {
            Some(d.target_partition_size)
        } else {
            None
        },
        hnsw,
        compression,
        runtime_hints: d.runtime_hints,
    };

    serde_json::to_string(&json).map_err(|e| Error::index(format!("Failed to serialize: {}", e)))
}

/// Infer VectorIndexDetails from index files on disk.
/// Used as a fallback for legacy indices where the manifest doesn't have populated details.
pub async fn infer_vector_index_details(
    dataset: &Dataset,
    index: &IndexMetadata,
) -> Result<prost_types::Any> {
    let uuid = index.uuid.to_string();
    let index_dir = dataset.indice_files_dir(index)?;
    let file_dir = index_dir.clone().join(uuid.as_str());
    let index_file = file_dir.clone().join(INDEX_FILE_NAME);
    let file_sizes = index.file_size_map();
    let reader: Arc<dyn Reader> = super::open_index_file(
        dataset.object_store.as_ref(),
        &index_file,
        INDEX_FILE_NAME,
        &file_sizes,
    )
    .await?
    .into();

    let tailing_bytes = read_last_block(reader.as_ref()).await?;
    let (major_version, minor_version) = read_version(&tailing_bytes)?;

    match (major_version, minor_version) {
        (0, 1) | (0, 0) => {
            // Legacy v0.1: read pb::Index, extract VectorIndex stages
            let proto = open_index_proto(reader.as_ref()).await?;
            convert_legacy_proto_to_details(&proto)
        }
        _ => {
            // v0.2+/v0.3: read lance file schema metadata
            convert_v3_metadata_to_details(dataset, &file_dir).await
        }
    }
}

fn convert_legacy_proto_to_details(proto: &pb::Index) -> Result<prost_types::Any> {
    use lance_index::pb::VectorIndexDetails;
    use lance_index::pb::vector_index_details::*;
    use pb::vector_index_stage::Stage;

    let Some(Implementation::VectorIndex(vector_index)) = &proto.implementation else {
        return Ok(vector_index_details_default());
    };

    let metric_type = pb::VectorMetricType::try_from(vector_index.metric_type)
        .unwrap_or(pb::VectorMetricType::L2);

    let mut compression: Option<Compression> = None;
    for stage in &vector_index.stages {
        if let Some(Stage::Pq(pq)) = &stage.stage {
            compression = Some(Compression::Pq(ProductQuantization {
                num_bits: pq.num_bits,
                num_sub_vectors: pq.num_sub_vectors,
            }));
        }
    }
    let compression = compression.or(Some(Compression::Flat(FlatCompression {})));

    let details = VectorIndexDetails {
        metric_type: metric_type.into(),
        target_partition_size: 0,
        hnsw_index_config: None,
        compression,
        runtime_hints: Default::default(),
    };
    Ok(prost_types::Any::from_msg(&details).unwrap())
}

async fn convert_v3_metadata_to_details(
    dataset: &Dataset,
    file_dir: &object_store::path::Path,
) -> Result<prost_types::Any> {
    use lance_index::INDEX_AUXILIARY_FILE_NAME;
    use lance_index::pb::vector_index_details::*;
    use lance_index::pb::{HnswParameters, VectorIndexDetails};
    use lance_index::vector::bq::storage::RabitQuantizationMetadata;
    use lance_index::vector::hnsw::HnswMetadata;
    use lance_index::vector::hnsw::builder::HNSW_METADATA_KEY;
    use lance_index::vector::pq::storage::ProductQuantizationMetadata;
    use lance_index::vector::shared::partition_merger::SupportedIvfIndexType;
    use lance_index::vector::sq::storage::ScalarQuantizationMetadata;
    use lance_index::vector::storage::STORAGE_METADATA_KEY;

    let index_file = file_dir.clone().join(INDEX_FILE_NAME);
    let main_reader = open_lance_file(dataset, &index_file).await?;
    let main_meta = &main_reader.schema().metadata;

    // Index type and distance live in the main file's INDEX_METADATA_SCHEMA_KEY.
    let idx_meta: Option<lance_index::IndexMetadata> = main_meta
        .get(INDEX_METADATA_SCHEMA_KEY)
        .map(|s| serde_json::from_str(s))
        .transpose()?;

    let metric_type = idx_meta
        .as_ref()
        .map(|m| match m.distance_type.to_uppercase().as_str() {
            "L2" | "EUCLIDEAN" => VectorMetricType::L2,
            "COSINE" => VectorMetricType::Cosine,
            "DOT" => VectorMetricType::Dot,
            "HAMMING" => VectorMetricType::Hamming,
            _ => VectorMetricType::L2,
        })
        .unwrap_or(VectorMetricType::L2);

    // The index_type string drives both whether HNSW is present and which
    // compression to expect. Falls back to IvfFlat if the metadata is missing
    // or unrecognized.
    let supported_type = idx_meta
        .as_ref()
        .and_then(|m| SupportedIvfIndexType::from_index_type_str(&m.index_type))
        .unwrap_or(SupportedIvfIndexType::IvfFlat);
    let (has_hnsw, compression_kind) = match supported_type {
        SupportedIvfIndexType::IvfFlat => (false, CompressionKind::Flat),
        SupportedIvfIndexType::IvfPq => (false, CompressionKind::Pq),
        SupportedIvfIndexType::IvfSq => (false, CompressionKind::Sq),
        SupportedIvfIndexType::IvfRq => (false, CompressionKind::Rq),
        SupportedIvfIndexType::IvfHnswFlat => (true, CompressionKind::Flat),
        SupportedIvfIndexType::IvfHnswPq => (true, CompressionKind::Pq),
        SupportedIvfIndexType::IvfHnswSq => (true, CompressionKind::Sq),
    };

    let hnsw_index_config = if has_hnsw {
        // HNSW partition metadata is stored as a JSON array of JSON-encoded
        // strings (one per partition), matching how the builder writes
        // `partition_index_metadata: Vec<String>`.
        main_meta
            .get(HNSW_METADATA_KEY)
            .map(|s| serde_json::from_str::<Vec<String>>(s))
            .transpose()?
            .and_then(|entries| entries.into_iter().next())
            .map(|s| serde_json::from_str::<HnswMetadata>(&s))
            .transpose()?
            .map(|hnsw| HnswParameters {
                max_connections: hnsw.params.m as u32,
                construction_ef: hnsw.params.ef_construction as u32,
                max_level: hnsw.params.max_level as u32,
            })
    } else {
        None
    };

    // For quantized indices, the per-quantizer metadata is in the auxiliary
    // file under STORAGE_METADATA_KEY (a JSON-encoded Vec<String>, one entry
    // per partition; all entries currently share the same metadata so we read
    // the first).
    let compression = match compression_kind {
        CompressionKind::Flat => Some(Compression::Flat(FlatCompression {})),
        CompressionKind::Pq | CompressionKind::Sq | CompressionKind::Rq => {
            let aux_file = file_dir.clone().join(INDEX_AUXILIARY_FILE_NAME);
            let aux_reader = open_lance_file(dataset, &aux_file).await?;
            let raw = aux_reader
                .schema()
                .metadata
                .get(STORAGE_METADATA_KEY)
                .ok_or_else(|| {
                    Error::index(format!(
                        "auxiliary file missing {STORAGE_METADATA_KEY} metadata"
                    ))
                })?;
            let entries: Vec<String> = serde_json::from_str(raw)?;
            let first = entries.first().ok_or_else(|| {
                Error::index("auxiliary STORAGE_METADATA_KEY was empty".to_string())
            })?;
            match compression_kind {
                CompressionKind::Pq => {
                    let pq: ProductQuantizationMetadata = serde_json::from_str(first)?;
                    Some(Compression::Pq(ProductQuantization {
                        num_bits: pq.nbits,
                        num_sub_vectors: pq.num_sub_vectors as u32,
                    }))
                }
                CompressionKind::Sq => {
                    let sq: ScalarQuantizationMetadata = serde_json::from_str(first)?;
                    Some(Compression::Sq(ScalarQuantization {
                        num_bits: sq.num_bits as u32,
                    }))
                }
                CompressionKind::Rq => {
                    let rq: RabitQuantizationMetadata = serde_json::from_str(first)?;
                    let rotation_type = match rq.rotation_type {
                        lance_index::vector::bq::RQRotationType::Fast => {
                            rabit_quantization::RotationType::Fast
                        }
                        lance_index::vector::bq::RQRotationType::Matrix => {
                            rabit_quantization::RotationType::Matrix
                        }
                    };
                    Some(Compression::Rq(RabitQuantization {
                        num_bits: rq.num_bits as u32,
                        rotation_type: rotation_type.into(),
                    }))
                }
                CompressionKind::Flat => unreachable!(),
            }
        }
    };

    let details = VectorIndexDetails {
        metric_type: metric_type.into(),
        target_partition_size: 0,
        hnsw_index_config,
        compression,
        runtime_hints: Default::default(),
    };
    Ok(prost_types::Any::from_msg(&details).unwrap())
}

enum CompressionKind {
    Flat,
    Pq,
    Sq,
    Rq,
}

async fn open_lance_file(
    dataset: &Dataset,
    path: &object_store::path::Path,
) -> Result<lance_file::reader::FileReader> {
    let scheduler = ScanScheduler::new(
        dataset.object_store.clone(),
        SchedulerConfig::max_bandwidth(&dataset.object_store),
    );
    let file = scheduler
        .open_file(path, &CachedFileSize::unknown())
        .await?;
    lance_file::reader::FileReader::try_open(
        file,
        None,
        Default::default(),
        &dataset.metadata_cache.file_metadata_cache(path),
        FileReaderOptions::default(),
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use lance_index::pb::vector_index_details::*;
    use lance_index::pb::{HnswParameters, VectorIndexDetails};

    fn make_details(
        metric: VectorMetricType,
        hnsw: Option<HnswParameters>,
        compression: Option<Compression>,
    ) -> prost_types::Any {
        let details = VectorIndexDetails {
            metric_type: metric.into(),
            target_partition_size: 0,
            hnsw_index_config: hnsw,
            compression,
            runtime_hints: Default::default(),
        };
        prost_types::Any::from_msg(&details).unwrap()
    }

    #[test]
    fn test_derive_index_type_without_hnsw() {
        // Note: (None, "IVF_FLAT") is not testable here because a proto with
        // all defaults serializes to empty bytes, which is treated as a legacy index.
        let cases: [(Option<Compression>, &str); 3] = [
            (
                Some(Compression::Pq(ProductQuantization {
                    num_bits: 8,
                    num_sub_vectors: 16,
                })),
                "IVF_PQ",
            ),
            (
                Some(Compression::Sq(ScalarQuantization { num_bits: 8 })),
                "IVF_SQ",
            ),
            (
                Some(Compression::Rq(RabitQuantization {
                    num_bits: 1,
                    rotation_type: 0,
                })),
                "IVF_RQ",
            ),
        ];
        for (compression, expected) in cases {
            let details = make_details(VectorMetricType::L2, None, compression);
            assert_eq!(derive_vector_index_type(&details), expected);
        }
    }

    #[test]
    fn test_derive_index_type_with_hnsw() {
        let hnsw = Some(HnswParameters {
            max_connections: 20,
            construction_ef: 150,
            max_level: 7,
        });
        assert_eq!(
            derive_vector_index_type(&make_details(VectorMetricType::L2, hnsw, None)),
            "IVF_HNSW_FLAT"
        );
        assert_eq!(
            derive_vector_index_type(&make_details(
                VectorMetricType::L2,
                hnsw,
                Some(Compression::Pq(ProductQuantization {
                    num_bits: 8,
                    num_sub_vectors: 16,
                }))
            )),
            "IVF_HNSW_PQ"
        );
        assert_eq!(
            derive_vector_index_type(&make_details(
                VectorMetricType::L2,
                hnsw,
                Some(Compression::Sq(ScalarQuantization { num_bits: 8 }))
            )),
            "IVF_HNSW_SQ"
        );
    }

    #[test]
    fn test_derive_index_type_empty_details() {
        let details = vector_index_details_default();
        assert_eq!(derive_vector_index_type(&details), "Vector");
    }

    // Snapshot tests for JSON serialization. These guard backwards compatibility
    // of the JSON format returned by describe_indices().

    #[test]
    fn test_json_ivf_pq() {
        let details = make_details(
            VectorMetricType::L2,
            None,
            Some(Compression::Pq(ProductQuantization {
                num_bits: 8,
                num_sub_vectors: 16,
            })),
        );
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"L2","compression":{"type":"pq","num_bits":8,"num_sub_vectors":16}}"#
        );
    }

    #[test]
    fn test_json_ivf_hnsw_sq() {
        let details = make_details(
            VectorMetricType::Cosine,
            Some(HnswParameters {
                max_connections: 30,
                construction_ef: 200,
                max_level: 8,
            }),
            Some(Compression::Sq(ScalarQuantization { num_bits: 4 })),
        );
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"COSINE","hnsw":{"max_connections":30,"construction_ef":200,"max_level":8},"compression":{"type":"sq","num_bits":4}}"#
        );
    }

    #[test]
    fn test_json_ivf_rq_with_rotation() {
        let details = make_details(
            VectorMetricType::Dot,
            None,
            Some(Compression::Rq(RabitQuantization {
                num_bits: 1,
                rotation_type: rabit_quantization::RotationType::Matrix as i32,
            })),
        );
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"DOT","compression":{"type":"rq","num_bits":1,"rotation_type":"matrix"}}"#
        );
    }

    #[test]
    fn test_json_ivf_rq_fast_rotation() {
        let details = make_details(
            VectorMetricType::L2,
            None,
            Some(Compression::Rq(RabitQuantization {
                num_bits: 1,
                rotation_type: rabit_quantization::RotationType::Fast as i32,
            })),
        );
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"L2","compression":{"type":"rq","num_bits":1,"rotation_type":"fast"}}"#
        );
    }

    #[test]
    fn test_json_with_target_partition_size() {
        let details = {
            let d = VectorIndexDetails {
                metric_type: VectorMetricType::L2.into(),
                target_partition_size: 5000,
                hnsw_index_config: None,
                compression: None,
                runtime_hints: Default::default(),
            };
            prost_types::Any::from_msg(&d).unwrap()
        };
        assert_eq!(
            vector_details_as_json(&details).unwrap(),
            r#"{"metric_type":"L2","target_partition_size":5000}"#
        );
    }

    #[test]
    fn test_json_empty_details() {
        let details = vector_index_details_default();
        assert_eq!(vector_details_as_json(&details).unwrap(), "{}");
    }

    #[test]
    fn test_metric_type_from_index_metadata_populated() {
        // Test that populated details return the metric type.
        // Note: We add a non-default compression field so the proto doesn't serialize to empty bytes.
        let details = make_details(
            VectorMetricType::L2,
            None,
            Some(Compression::Pq(ProductQuantization {
                num_bits: 8,
                num_sub_vectors: 16,
            })),
        );
        let index_details = Some(std::sync::Arc::new(details));
        let index = IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            fields: vec![0],
            name: "test_index".to_string(),
            dataset_version: 1,
            fragment_bitmap: None,
            index_details,
            index_version: 1,
            created_at: None,
            base_id: None,
            files: None,
        };

        let metric = metric_type_from_index_metadata(&index);
        assert_eq!(metric, Some(DistanceType::L2));
    }

    #[test]
    fn test_metric_type_from_index_metadata_empty() {
        // Test that empty details return None (legacy index)
        let details = vector_index_details_default();
        let index_details = Some(std::sync::Arc::new(details));
        let index = IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            fields: vec![0],
            name: "test_index".to_string(),
            dataset_version: 1,
            fragment_bitmap: None,
            index_details,
            index_version: 1,
            created_at: None,
            base_id: None,
            files: None,
        };

        let metric = metric_type_from_index_metadata(&index);
        assert_eq!(metric, None);
    }

    #[test]
    fn test_metric_type_from_index_metadata_none() {
        // Test that missing details return None
        let index = IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            fields: vec![0],
            name: "test_index".to_string(),
            dataset_version: 1,
            fragment_bitmap: None,
            index_details: None,
            index_version: 1,
            created_at: None,
            base_id: None,
            files: None,
        };

        let metric = metric_type_from_index_metadata(&index);
        assert_eq!(metric, None);
    }

    // Schema with a vector column ("vec", FixedSizeList) and a scalar column
    // ("tag", Utf8). Field ids are assigned by `set_field_id` during conversion,
    // so look them up by name rather than hardcoding.
    fn schema_with_vector_and_scalar() -> lance_core::datatypes::Schema {
        use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
        let arrow = ArrowSchema::new(vec![
            ArrowField::new(
                "vec",
                DataType::FixedSizeList(
                    Arc::new(ArrowField::new("item", DataType::Float32, true)),
                    8,
                ),
                true,
            ),
            ArrowField::new("tag", DataType::Utf8, true),
        ]);
        lance_core::datatypes::Schema::try_from(&arrow).unwrap()
    }

    fn index_over_field(field_id: i32, index_details: Option<prost_types::Any>) -> IndexMetadata {
        IndexMetadata {
            uuid: uuid::Uuid::new_v4(),
            fields: vec![field_id],
            name: "idx".to_string(),
            dataset_version: 1,
            fragment_bitmap: None,
            index_details: index_details.map(Arc::new),
            index_version: 1,
            created_at: None,
            base_id: None,
            files: None,
        }
    }

    #[test]
    fn test_needs_inference_missing_details_on_vector_field() {
        // Oldest legacy case (<=0.19.2): no details, but the indexed field is a
        // vector type => must infer.
        let schema = schema_with_vector_and_scalar();
        let vec_id = schema.field("vec").unwrap().id;
        let index = index_over_field(vec_id, None);
        assert!(needs_vector_details_inference(&index, &schema));
    }

    #[test]
    fn test_needs_inference_missing_details_on_scalar_field() {
        // No details and the indexed field is not a vector type (e.g. an FTS or
        // scalar index) => nothing to infer, so the load_indices fast path may
        // skip the clone/infer/compare.
        let schema = schema_with_vector_and_scalar();
        let tag_id = schema.field("tag").unwrap().id;
        let index = index_over_field(tag_id, None);
        assert!(!needs_vector_details_inference(&index, &schema));
    }

    #[test]
    fn test_needs_inference_empty_vector_details() {
        // Newer pre-details case: a VectorIndexDetails type_url with empty value
        // bytes => must infer.
        let schema = schema_with_vector_and_scalar();
        let vec_id = schema.field("vec").unwrap().id;
        let index = index_over_field(vec_id, Some(vector_index_details_default()));
        assert!(needs_vector_details_inference(&index, &schema));
    }

    #[test]
    fn test_needs_inference_populated_vector_details() {
        // Modern vector index with populated details => no inference needed.
        let schema = schema_with_vector_and_scalar();
        let vec_id = schema.field("vec").unwrap().id;
        let details = make_details(
            VectorMetricType::L2,
            None,
            Some(Compression::Pq(ProductQuantization {
                num_bits: 8,
                num_sub_vectors: 16,
            })),
        );
        let index = index_over_field(vec_id, Some(details));
        assert!(!needs_vector_details_inference(&index, &schema));
    }

    #[test]
    fn test_metric_type_from_index_metadata_all_metrics() {
        // Test all supported metric types.
        // Note: We add a non-default compression field so the proto doesn't serialize to empty bytes.
        let metrics = [
            VectorMetricType::L2,
            VectorMetricType::Cosine,
            VectorMetricType::Dot,
            VectorMetricType::Hamming,
        ];
        let expected = [
            DistanceType::L2,
            DistanceType::Cosine,
            DistanceType::Dot,
            DistanceType::Hamming,
        ];

        for (metric_enum, expected_distance) in metrics.iter().zip(expected.iter()) {
            let details = make_details(
                *metric_enum,
                None,
                Some(Compression::Sq(ScalarQuantization { num_bits: 8 })),
            );
            let index_details = Some(std::sync::Arc::new(details));
            let index = IndexMetadata {
                uuid: uuid::Uuid::new_v4(),
                fields: vec![0],
                name: "test_index".to_string(),
                dataset_version: 1,
                fragment_bitmap: None,
                index_details,
                index_version: 1,
                created_at: None,
                base_id: None,
                files: None,
            };

            let metric = metric_type_from_index_metadata(&index);
            assert_eq!(metric, Some(*expected_distance));
        }
    }

    #[test]
    fn test_runtime_hints_roundtrip() {
        use crate::index::vector::{StageParams, VectorIndexParams};
        use lance_index::vector::ivf::builder::IvfBuildParams;
        use lance_index::vector::pq::builder::PQBuildParams;
        use lance_linalg::distance::DistanceType;

        // Non-default values for IVF and PQ hints
        let params = VectorIndexParams::with_ivf_pq_params(
            DistanceType::L2,
            IvfBuildParams {
                max_iters: 100,
                sample_rate: 512,
                shuffle_partition_batches: 2048,
                shuffle_partition_concurrency: 4,
                ..Default::default()
            },
            PQBuildParams {
                num_sub_vectors: 8,
                num_bits: 8,
                max_iters: 75,
                kmeans_redos: 3,
                sample_rate: 128,
                ..Default::default()
            },
        );

        let any = vector_index_details(&params);
        let details = any.to_msg::<VectorIndexDetails>().unwrap();
        assert_eq!(
            details
                .runtime_hints
                .get("lance.ivf.max_iters")
                .map(|s| s.as_str()),
            Some("100")
        );
        assert_eq!(
            details
                .runtime_hints
                .get("lance.ivf.sample_rate")
                .map(|s| s.as_str()),
            Some("512")
        );
        assert_eq!(
            details
                .runtime_hints
                .get("lance.ivf.shuffle_partition_batches")
                .map(|s| s.as_str()),
            Some("2048")
        );
        assert_eq!(
            details
                .runtime_hints
                .get("lance.ivf.shuffle_partition_concurrency")
                .map(|s| s.as_str()),
            Some("4")
        );
        assert_eq!(
            details
                .runtime_hints
                .get("lance.pq.max_iters")
                .map(|s| s.as_str()),
            Some("75")
        );
        assert_eq!(
            details
                .runtime_hints
                .get("lance.pq.sample_rate")
                .map(|s| s.as_str()),
            Some("128")
        );
        assert_eq!(
            details
                .runtime_hints
                .get("lance.pq.kmeans_redos")
                .map(|s| s.as_str()),
            Some("3")
        );
        // No HNSW stage in this IVF+PQ params, so no prefetch_distance hint.
        assert!(
            !details
                .runtime_hints
                .contains_key("lance.hnsw.prefetch_distance")
        );
        // skip_transpose is recorded even when false.
        assert_eq!(
            details.runtime_hints.get("lance.skip_transpose"),
            Some(&"false".to_string())
        );

        // Roundtrip: apply hints back to a fresh params struct
        let mut restored = VectorIndexParams::with_ivf_pq_params(
            DistanceType::L2,
            IvfBuildParams::default(),
            PQBuildParams {
                num_sub_vectors: 8,
                num_bits: 8,
                ..Default::default()
            },
        );
        apply_runtime_hints(&details.runtime_hints, &mut restored);
        let StageParams::Ivf(ivf) = &restored.stages[0] else {
            panic!()
        };
        assert_eq!(ivf.max_iters, 100);
        assert_eq!(ivf.sample_rate, 512);
        assert_eq!(ivf.shuffle_partition_batches, 2048);
        assert_eq!(ivf.shuffle_partition_concurrency, 4);
        let StageParams::PQ(pq) = &restored.stages[1] else {
            panic!()
        };
        assert_eq!(pq.max_iters, 75);
        assert_eq!(pq.sample_rate, 128);
        assert_eq!(pq.kmeans_redos, 3);
    }

    #[test]
    fn test_runtime_hints_roundtrip_hnsw_sq_skip_transpose() {
        use crate::index::vector::{StageParams, VectorIndexParams};
        use lance_index::vector::hnsw::builder::HnswBuildParams;
        use lance_index::vector::ivf::builder::IvfBuildParams;
        use lance_index::vector::sq::builder::SQBuildParams;
        use lance_linalg::distance::DistanceType;

        // Non-default values for hints that aren't covered by the IVF+PQ test:
        // hnsw.prefetch_distance, sq.sample_rate, and the top-level skip_transpose.
        let hnsw = HnswBuildParams {
            m: 20,
            ef_construction: 150,
            max_level: 6,
            prefetch_distance: Some(4),
        };
        let mut params = VectorIndexParams::with_ivf_hnsw_sq_params(
            DistanceType::L2,
            IvfBuildParams::default(),
            hnsw,
            SQBuildParams {
                num_bits: 8,
                sample_rate: 128,
            },
        );
        params.skip_transpose = true;

        let any = vector_index_details(&params);
        let details = any.to_msg::<VectorIndexDetails>().unwrap();
        assert_eq!(
            details.runtime_hints.get("lance.hnsw.prefetch_distance"),
            Some(&"4".to_string())
        );
        assert_eq!(
            details.runtime_hints.get("lance.sq.sample_rate"),
            Some(&"128".to_string())
        );
        assert_eq!(
            details.runtime_hints.get("lance.skip_transpose"),
            Some(&"true".to_string())
        );

        // Roundtrip back into a fresh params struct
        let mut restored = VectorIndexParams::with_ivf_hnsw_sq_params(
            DistanceType::L2,
            IvfBuildParams::default(),
            HnswBuildParams::default(),
            SQBuildParams::default(),
        );
        assert!(!restored.skip_transpose);
        apply_runtime_hints(&details.runtime_hints, &mut restored);

        assert!(restored.skip_transpose);
        let StageParams::Hnsw(restored_hnsw) = &restored.stages[1] else {
            panic!("expected HNSW stage");
        };
        assert_eq!(restored_hnsw.prefetch_distance, Some(4));
        let StageParams::SQ(restored_sq) = &restored.stages[2] else {
            panic!("expected SQ stage");
        };
        assert_eq!(restored_sq.sample_rate, 128);
    }

    #[test]
    fn test_runtime_hints_prefetch_distance_none_roundtrip() {
        use crate::index::vector::{StageParams, VectorIndexParams};
        use lance_index::vector::hnsw::builder::HnswBuildParams;
        use lance_index::vector::ivf::builder::IvfBuildParams;
        use lance_linalg::distance::DistanceType;

        // prefetch_distance = None is distinguishable from "default Some(2)"
        // via the "none" sentinel — verify it round-trips.
        let hnsw = HnswBuildParams {
            m: 16,
            ef_construction: 100,
            max_level: 5,
            prefetch_distance: None,
        };
        let params = VectorIndexParams::ivf_hnsw(DistanceType::L2, IvfBuildParams::default(), hnsw);

        let any = vector_index_details(&params);
        let details = any.to_msg::<VectorIndexDetails>().unwrap();
        assert_eq!(
            details.runtime_hints.get("lance.hnsw.prefetch_distance"),
            Some(&"none".to_string())
        );

        let mut restored = VectorIndexParams::ivf_hnsw(
            DistanceType::L2,
            IvfBuildParams::default(),
            HnswBuildParams::default(),
        );
        apply_runtime_hints(&details.runtime_hints, &mut restored);
        let StageParams::Hnsw(restored_hnsw) = &restored.stages[1] else {
            panic!("expected HNSW stage");
        };
        assert_eq!(restored_hnsw.prefetch_distance, None);
    }

    #[test]
    fn test_runtime_hints_in_json() {
        use crate::index::vector::VectorIndexParams;
        use lance_index::vector::ivf::builder::IvfBuildParams;
        use lance_index::vector::pq::builder::PQBuildParams;
        use lance_linalg::distance::DistanceType;

        let params = VectorIndexParams::with_ivf_pq_params(
            DistanceType::L2,
            IvfBuildParams {
                max_iters: 100,
                ..Default::default()
            },
            PQBuildParams {
                num_sub_vectors: 8,
                num_bits: 8,
                ..Default::default()
            },
        );
        let any = vector_index_details(&params);
        let json = vector_details_as_json(&any).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["runtime_hints"]["lance.ivf.max_iters"], "100");
    }

    /// Matrix of subindex/quantizer combinations we want to round-trip.
    #[derive(Debug, Clone, Copy)]
    #[allow(clippy::enum_variant_names)]
    enum Combo {
        IvfFlat,
        IvfPq,
        IvfSq,
        IvfRqMatrix,
        IvfRqFast,
        IvfHnswFlat,
        IvfHnswPq,
        IvfHnswSq,
    }

    fn build_roundtrip_params(combo: Combo, metric: DistanceType) -> VectorIndexParams {
        use crate::index::vector::VectorIndexParams;
        use lance_index::vector::bq::{RQBuildParams, RQRotationType};
        use lance_index::vector::hnsw::builder::HnswBuildParams;
        use lance_index::vector::ivf::builder::IvfBuildParams;
        use lance_index::vector::pq::builder::PQBuildParams;
        use lance_index::vector::sq::builder::SQBuildParams;

        // Non-default values so the round-trip actually checks preservation
        // rather than coincidentally matching defaults.
        let ivf = IvfBuildParams {
            max_iters: 100,
            sample_rate: 512,
            target_partition_size: Some(2048),
            shuffle_partition_batches: 4096,
            shuffle_partition_concurrency: 4,
            ..Default::default()
        };
        let hnsw = HnswBuildParams {
            m: 30,
            ef_construction: 200,
            max_level: 5,
            prefetch_distance: Some(2),
        };
        let pq = PQBuildParams {
            num_sub_vectors: 8,
            num_bits: 8,
            max_iters: 75,
            sample_rate: 128,
            kmeans_redos: 3,
            ..Default::default()
        };
        let sq = SQBuildParams {
            num_bits: 8,
            sample_rate: 128,
        };

        match combo {
            Combo::IvfFlat => VectorIndexParams::with_ivf_flat_params(metric, ivf),
            Combo::IvfPq => VectorIndexParams::with_ivf_pq_params(metric, ivf, pq),
            Combo::IvfSq => VectorIndexParams::with_ivf_sq_params(metric, ivf, sq),
            Combo::IvfRqMatrix => VectorIndexParams::with_ivf_rq_params(
                metric,
                ivf,
                RQBuildParams::with_rotation_type(1, RQRotationType::Matrix),
            ),
            Combo::IvfRqFast => VectorIndexParams::with_ivf_rq_params(
                metric,
                ivf,
                RQBuildParams::with_rotation_type(1, RQRotationType::Fast),
            ),
            Combo::IvfHnswFlat => VectorIndexParams::ivf_hnsw(metric, ivf, hnsw),
            Combo::IvfHnswPq => VectorIndexParams::with_ivf_hnsw_pq_params(metric, ivf, hnsw, pq),
            Combo::IvfHnswSq => VectorIndexParams::with_ivf_hnsw_sq_params(metric, ivf, hnsw, sq),
        }
    }

    #[rstest::rstest]
    #[case::ivf_flat(Combo::IvfFlat)]
    #[case::ivf_pq(Combo::IvfPq)]
    #[case::ivf_sq(Combo::IvfSq)]
    #[case::ivf_rq_matrix(Combo::IvfRqMatrix)]
    #[case::ivf_rq_fast(Combo::IvfRqFast)]
    #[case::ivf_hnsw_flat(Combo::IvfHnswFlat)]
    #[case::ivf_hnsw_pq(Combo::IvfHnswPq)]
    #[case::ivf_hnsw_sq(Combo::IvfHnswSq)]
    fn test_vector_index_details_roundtrip(
        #[case] combo: Combo,
        #[values(DistanceType::L2, DistanceType::Cosine)] metric: DistanceType,
    ) {
        use crate::index::vector::StageParams;
        use lance_index::vector::bq::RQRotationType;

        let params = build_roundtrip_params(combo, metric);

        let any = vector_index_details(&params);
        let restored = vector_params_from_details(&any)
            .expect("non-empty details should round-trip to params");

        assert_eq!(restored.metric_type, metric);
        assert_eq!(restored.index_type(), params.index_type());

        let StageParams::Ivf(ivf) = &restored.stages[0] else {
            panic!("first stage should be IVF for combo {:?}", combo);
        };
        assert_eq!(ivf.max_iters, 100);
        assert_eq!(ivf.sample_rate, 512);
        assert_eq!(ivf.target_partition_size, Some(2048));
        assert_eq!(ivf.shuffle_partition_batches, 4096);
        assert_eq!(ivf.shuffle_partition_concurrency, 4);

        match combo {
            Combo::IvfFlat => {
                assert_eq!(restored.stages.len(), 1);
            }
            Combo::IvfPq => {
                let StageParams::PQ(pq) = &restored.stages[1] else {
                    panic!("expected PQ stage");
                };
                assert_eq!(pq.num_sub_vectors, 8);
                assert_eq!(pq.num_bits, 8);
                assert_eq!(pq.max_iters, 75);
                assert_eq!(pq.sample_rate, 128);
                assert_eq!(pq.kmeans_redos, 3);
            }
            Combo::IvfSq => {
                let StageParams::SQ(sq) = &restored.stages[1] else {
                    panic!("expected SQ stage");
                };
                assert_eq!(sq.num_bits, 8);
                assert_eq!(sq.sample_rate, 128);
            }
            Combo::IvfRqMatrix | Combo::IvfRqFast => {
                let StageParams::RQ(rq) = &restored.stages[1] else {
                    panic!("expected RQ stage");
                };
                assert_eq!(rq.num_bits, 1);
                let expected = match combo {
                    Combo::IvfRqMatrix => RQRotationType::Matrix,
                    Combo::IvfRqFast => RQRotationType::Fast,
                    _ => unreachable!(),
                };
                assert_eq!(rq.rotation_type, expected);
            }
            Combo::IvfHnswFlat => {
                let StageParams::Hnsw(hnsw) = &restored.stages[1] else {
                    panic!("expected HNSW stage");
                };
                assert_eq!(hnsw.m, 30);
                assert_eq!(hnsw.ef_construction, 200);
                assert_eq!(hnsw.max_level, 5);
            }
            Combo::IvfHnswPq => {
                let StageParams::Hnsw(hnsw) = &restored.stages[1] else {
                    panic!("expected HNSW stage");
                };
                assert_eq!(hnsw.m, 30);
                assert_eq!(hnsw.ef_construction, 200);
                assert_eq!(hnsw.max_level, 5);
                let StageParams::PQ(pq) = &restored.stages[2] else {
                    panic!("expected PQ stage");
                };
                assert_eq!(pq.num_sub_vectors, 8);
                assert_eq!(pq.num_bits, 8);
            }
            Combo::IvfHnswSq => {
                let StageParams::Hnsw(hnsw) = &restored.stages[1] else {
                    panic!("expected HNSW stage");
                };
                assert_eq!(hnsw.m, 30);
                assert_eq!(hnsw.ef_construction, 200);
                assert_eq!(hnsw.max_level, 5);
                let StageParams::SQ(sq) = &restored.stages[2] else {
                    panic!("expected SQ stage");
                };
                assert_eq!(sq.num_bits, 8);
            }
        }
    }

    #[test]
    fn test_apply_runtime_hints_ignores_unknown_keys() {
        use crate::index::vector::VectorIndexParams;
        use lance_index::vector::ivf::builder::IvfBuildParams;
        use lance_linalg::distance::DistanceType;

        let hints: HashMap<String, String> = [
            ("lancedb.accelerator".to_string(), "cuda".to_string()),
            ("unknown.vendor.key".to_string(), "value".to_string()),
            ("lance.ivf.max_iters".to_string(), "99".to_string()),
        ]
        .into();

        let mut params =
            VectorIndexParams::with_ivf_flat_params(DistanceType::L2, IvfBuildParams::default());
        apply_runtime_hints(&hints, &mut params);

        let StageParams::Ivf(ivf) = &params.stages[0] else {
            panic!()
        };
        assert_eq!(ivf.max_iters, 99);
        // Unknown keys silently ignored — no panic
    }
}
