// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Shared helpers for IVF partition merging and metadata writing.
//!
//! The helpers here are used by both the distributed index merger
//! (`vector::distributed::index_merger`) and the classic IVF index
//! builder in the `lance` crate. They keep writer initialization and
//! IVF / index metadata writing in one place.

use arrow_schema::Schema as ArrowSchema;
use bytes::Bytes;
use lance_core::{Error, Result};
use lance_file::reader::FileReader as V2Reader;
use lance_file::writer::FileWriter;
use lance_linalg::distance::DistanceType;
use prost::Message;

use crate::pb;
use crate::vector::bq::storage::RABIT_METADATA_KEY;
use crate::vector::ivf::storage::{IVF_METADATA_KEY, IvfModel};
use crate::vector::pq::storage::PQ_METADATA_KEY;
use crate::vector::sq::storage::SQ_METADATA_KEY;
use crate::vector::{PQ_CODE_COLUMN, SQ_CODE_COLUMN};
use crate::{INDEX_METADATA_SCHEMA_KEY, IndexMetadata as IndexMetaSchema};

/// Supported vector index types for unified IVF metadata writing.
///
/// This mirrors the vector variants in [`crate::IndexType`] that are
/// used by IVF-based indices. Keeping this here avoids pulling the
/// full `IndexType` dependency into helpers that only need the string
/// representation.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SupportedIvfIndexType {
    IvfFlat,
    IvfPq,
    IvfSq,
    IvfRq,
    IvfHnswFlat,
    IvfHnswPq,
    IvfHnswSq,
}

impl SupportedIvfIndexType {
    /// Get the index type string used in metadata.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::IvfFlat => "IVF_FLAT",
            Self::IvfPq => "IVF_PQ",
            Self::IvfSq => "IVF_SQ",
            Self::IvfRq => "IVF_RQ",
            Self::IvfHnswFlat => "IVF_HNSW_FLAT",
            Self::IvfHnswPq => "IVF_HNSW_PQ",
            Self::IvfHnswSq => "IVF_HNSW_SQ",
        }
    }

    /// Map an index type string (as stored in metadata) to a
    /// [`SupportedIvfIndexType`] if it is one of the IVF variants this
    /// helper understands.
    pub fn from_index_type_str(s: &str) -> Option<Self> {
        match s {
            "IVF_FLAT" => Some(Self::IvfFlat),
            "IVF_PQ" => Some(Self::IvfPq),
            "IVF_SQ" => Some(Self::IvfSq),
            "IVF_RQ" => Some(Self::IvfRq),
            "IVF_HNSW_FLAT" => Some(Self::IvfHnswFlat),
            "IVF_HNSW_PQ" => Some(Self::IvfHnswPq),
            "IVF_HNSW_SQ" => Some(Self::IvfHnswSq),
            _ => None,
        }
    }

    /// Detect index type from reader metadata and schema.
    ///
    /// This is primarily used by the distributed index merger when
    /// consolidating partial auxiliary files.
    pub fn detect_from_reader_and_schema(reader: &V2Reader, schema: &ArrowSchema) -> Result<Self> {
        let has_pq_code_col = schema.fields.iter().any(|f| f.name() == PQ_CODE_COLUMN);
        let has_sq_code_col = schema.fields.iter().any(|f| f.name() == SQ_CODE_COLUMN);
        let has_rq_code_col = schema
            .fields
            .iter()
            .any(|f| f.name() == crate::vector::bq::storage::RABIT_CODE_COLUMN);

        let is_pq = reader
            .metadata()
            .file_schema
            .metadata
            .contains_key(PQ_METADATA_KEY)
            || has_pq_code_col;
        let is_sq = reader
            .metadata()
            .file_schema
            .metadata
            .contains_key(SQ_METADATA_KEY)
            || has_sq_code_col;
        let is_rq = reader
            .metadata()
            .file_schema
            .metadata
            .contains_key(RABIT_METADATA_KEY)
            || has_rq_code_col;

        // Detect HNSW-related columns
        let has_hnsw_vector_id_col = schema.fields.iter().any(|f| f.name() == "__vector_id");
        let has_hnsw_pointer_col = schema.fields.iter().any(|f| f.name() == "__pointer");
        let has_hnsw = has_hnsw_vector_id_col || has_hnsw_pointer_col;

        let index_type = match (has_hnsw, is_pq, is_sq, is_rq) {
            (false, false, false, false) => Self::IvfFlat,
            (false, true, false, false) => Self::IvfPq,
            (false, false, true, false) => Self::IvfSq,
            (false, false, false, true) => Self::IvfRq,
            (true, false, false, false) => Self::IvfHnswFlat,
            (true, true, false, false) => Self::IvfHnswPq,
            (true, false, true, false) => Self::IvfHnswSq,
            _ => {
                return Err(Error::not_supported_source(
                    "Unsupported index type combination detected".into(),
                ));
            }
        };

        Ok(index_type)
    }
}

/// Write unified IVF and index metadata to the writer.
///
/// This writes the IVF model into a global buffer and stores its
/// position under [`IVF_METADATA_KEY`], and attaches a compact
/// [`IndexMetaSchema`] payload under [`INDEX_METADATA_SCHEMA_KEY`].
pub async fn write_unified_ivf_and_index_metadata(
    w: &mut FileWriter,
    ivf_model: &IvfModel,
    dt: DistanceType,
    idx_type: SupportedIvfIndexType,
) -> Result<()> {
    let pb_ivf: pb::Ivf = (ivf_model).try_into()?;
    let pos = w
        .add_global_buffer(Bytes::from(pb_ivf.encode_to_vec()))
        .await?;
    w.add_schema_metadata(IVF_METADATA_KEY, pos.to_string());
    let idx_meta = IndexMetaSchema {
        index_type: idx_type.as_str().to_string(),
        distance_type: dt.to_string(),
    };
    w.add_schema_metadata(INDEX_METADATA_SCHEMA_KEY, serde_json::to_string(&idx_meta)?);
    Ok(())
}
