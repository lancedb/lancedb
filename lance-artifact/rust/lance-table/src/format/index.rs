// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Metadata for index

use std::collections::HashMap;
use std::sync::Arc;

use chrono::{DateTime, Utc};
use futures::StreamExt;
use lance_core::deepsize::DeepSizeOf;
use lance_io::object_store::ObjectStore;
use object_store::path::Path;
use roaring::RoaringBitmap;
use uuid::Uuid;

use super::pb;
use lance_core::cache::{CacheEntryReader, CacheEntryWriter};
use lance_core::{Error, Result};

/// Metadata about a single file within an index segment.
#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct IndexFile {
    /// Path relative to the index directory (e.g., "index.idx", "auxiliary.idx")
    pub path: String,
    /// Size of the file in bytes
    pub size_bytes: u64,
}

/// Index metadata
#[derive(Debug, Clone, PartialEq)]
pub struct IndexMetadata {
    /// Unique ID across all dataset versions.
    pub uuid: Uuid,

    /// Fields to build the index.
    pub fields: Vec<i32>,

    /// Human readable index name
    pub name: String,

    /// The version of the dataset this index was last updated on
    ///
    /// This is set when the index is created (based on the version used to train the index)
    /// This is updated when the index is updated or remapped
    pub dataset_version: u64,

    /// The fragment ids this index covers.
    ///
    /// This may contain fragment ids that no longer exist in the dataset.
    ///
    /// If this is None, then this is unknown.
    pub fragment_bitmap: Option<RoaringBitmap>,

    /// Metadata specific to the index type
    ///
    /// This is an Option because older versions of Lance may not have this defined.  However, it should always
    /// be present in newer versions.
    pub index_details: Option<Arc<prost_types::Any>>,

    /// The index version.
    pub index_version: i32,

    /// Timestamp when the index was created
    ///
    /// This field is optional for backward compatibility. For existing indices created before
    /// this field was added, this will be None.
    pub created_at: Option<DateTime<Utc>>,

    /// The base path index of the index files. Used when the index is imported or referred from another dataset.
    /// Lance uses it as key of the base_paths field in Manifest to determine the actual base path of the index files.
    pub base_id: Option<u32>,

    /// List of files and their sizes for this index segment.
    /// This enables skipping HEAD calls when opening indices and provides
    /// visibility into index storage size via describe_indices().
    /// This is None if the file sizes are unknown. This happens for indices created
    /// before this field was added.
    pub files: Option<Vec<IndexFile>>,
}

impl IndexMetadata {
    pub fn effective_fragment_bitmap(
        &self,
        existing_fragments: &RoaringBitmap,
    ) -> Option<RoaringBitmap> {
        let fragment_bitmap = self.fragment_bitmap.as_ref()?;
        Some(fragment_bitmap & existing_fragments)
    }

    /// Returns a map of relative file paths to their sizes.
    /// Returns an empty map if file information is not available.
    pub fn file_size_map(&self) -> HashMap<String, u64> {
        self.files
            .as_ref()
            .map(|files| {
                files
                    .iter()
                    .map(|f| (f.path.clone(), f.size_bytes))
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Returns the total size of all files in this index segment in bytes.
    /// Returns None if file information is not available.
    pub fn total_size_bytes(&self) -> Option<u64> {
        self.files
            .as_ref()
            .map(|files| files.iter().map(|f| f.size_bytes).sum())
    }

    /// Returns the set of fragments which are part of the fragment bitmap
    /// but no longer in the dataset.
    pub fn deleted_fragment_bitmap(
        &self,
        existing_fragments: &RoaringBitmap,
    ) -> Option<RoaringBitmap> {
        let fragment_bitmap = self.fragment_bitmap.as_ref()?;
        Some(fragment_bitmap - existing_fragments)
    }
}

impl DeepSizeOf for IndexMetadata {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.uuid.as_bytes().deep_size_of_children(context)
            + self.fields.deep_size_of_children(context)
            + self.name.deep_size_of_children(context)
            + self.dataset_version.deep_size_of_children(context)
            + self
                .fragment_bitmap
                .as_ref()
                .map(|fragment_bitmap| fragment_bitmap.serialized_size())
                .unwrap_or(0)
            + self.files.deep_size_of_children(context)
    }
}

impl TryFrom<pb::IndexMetadata> for IndexMetadata {
    type Error = Error;

    fn try_from(proto: pb::IndexMetadata) -> Result<Self> {
        let fragment_bitmap = if proto.fragment_bitmap.is_empty() {
            None
        } else {
            Some(RoaringBitmap::deserialize_from(
                &mut proto.fragment_bitmap.as_slice(),
            )?)
        };

        let files = if proto.files.is_empty() {
            None
        } else {
            Some(
                proto
                    .files
                    .into_iter()
                    .map(|f| IndexFile {
                        path: f.path,
                        size_bytes: f.size_bytes,
                    })
                    .collect(),
            )
        };

        Ok(Self {
            uuid: proto.uuid.as_ref().map(Uuid::try_from).ok_or_else(|| {
                Error::invalid_input("uuid field does not exist in Index metadata".to_string())
            })??,
            name: proto.name,
            fields: proto.fields,
            dataset_version: proto.dataset_version,
            fragment_bitmap,
            index_details: proto.index_details.map(Arc::new),
            index_version: proto.index_version.unwrap_or_default(),
            created_at: proto.created_at.map(|ts| {
                DateTime::from_timestamp_millis(ts as i64)
                    .expect("Invalid timestamp in index metadata")
            }),
            base_id: proto.base_id,
            files,
        })
    }
}

impl From<&IndexMetadata> for pb::IndexMetadata {
    fn from(idx: &IndexMetadata) -> Self {
        let mut fragment_bitmap = Vec::new();
        if let Some(bitmap) = &idx.fragment_bitmap
            && let Err(e) = bitmap.serialize_into(&mut fragment_bitmap)
        {
            // In theory, this should never error. But if we do, just
            // recover gracefully.
            log::error!("Failed to serialize fragment bitmap: {}", e);
            fragment_bitmap.clear();
        }

        let files = idx
            .files
            .as_ref()
            .map(|files| {
                files
                    .iter()
                    .map(|f| pb::IndexFile {
                        path: f.path.clone(),
                        size_bytes: f.size_bytes,
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self {
            uuid: Some((&idx.uuid).into()),
            name: idx.name.clone(),
            fields: idx.fields.clone(),
            dataset_version: idx.dataset_version,
            fragment_bitmap,
            index_details: idx
                .index_details
                .as_ref()
                .map(|details| details.as_ref().clone()),
            index_version: Some(idx.index_version),
            created_at: idx.created_at.map(|dt| dt.timestamp_millis() as u64),
            base_id: idx.base_id,
            files,
        }
    }
}

/// Returns a [`CacheCodec`](lance_core::cache::CacheCodec) for `Vec<IndexMetadata>`.
///
/// Uses `pb::IndexSection` (which wraps `repeated IndexMetadata`) as the wire
/// format, reusing the existing `TryFrom`/`From` conversions.
///
/// Uses [`CacheCodec::new`](lance_core::cache::CacheCodec::new) because the
/// orphan rule prevents `impl CacheCodecImpl for Vec<IndexMetadata>`.
type ArcAny = Arc<dyn std::any::Any + Send + Sync>;

/// Stable type identifier for the `Vec<IndexMetadata>` cache entry.
const INDEX_METADATA_TYPE_ID: &str = "lance.table.IndexMetadataList";
/// Body schema version written by this build.
const INDEX_METADATA_VERSION: u32 = 1;

fn serialize_index_metadata(
    any: &ArcAny,
    writer: &mut CacheEntryWriter<'_>,
) -> lance_core::Result<()> {
    let vec = any
        .downcast_ref::<Vec<IndexMetadata>>()
        .expect("index_metadata_codec: wrong type (this is a bug in the cache layer)");
    let section = pb::IndexSection {
        indices: vec.iter().map(pb::IndexMetadata::from).collect(),
    };
    writer.write_header(&section)
}

fn deserialize_index_metadata(reader: &mut CacheEntryReader<'_>) -> lance_core::Result<ArcAny> {
    let section: pb::IndexSection = reader.read_header()?;
    let indices: Vec<IndexMetadata> = section
        .indices
        .into_iter()
        .map(IndexMetadata::try_from)
        .collect::<lance_core::Result<_>>()?;
    Ok(Arc::new(indices))
}

pub fn index_metadata_codec() -> lance_core::cache::CacheCodec {
    lance_core::cache::CacheCodec::new(
        INDEX_METADATA_TYPE_ID,
        INDEX_METADATA_VERSION,
        serialize_index_metadata,
        deserialize_index_metadata,
    )
}

/// List all files in an index directory with their sizes.
///
/// Returns a list of `IndexFile` structs containing relative paths and sizes.
/// This is used to capture file metadata after index creation/modification.
pub async fn list_index_files_with_sizes(
    object_store: &ObjectStore,
    index_dir: &Path,
) -> Result<Vec<IndexFile>> {
    let mut files = Vec::new();
    let mut stream = object_store.read_dir_all(index_dir, None);
    while let Some(meta) = stream.next().await {
        let meta = meta?;
        // Get relative path by stripping the index_dir prefix
        let relative_path = meta
            .location
            .as_ref()
            .strip_prefix(index_dir.as_ref())
            .map(|s| s.trim_start_matches('/').to_string())
            .unwrap_or_else(|| meta.location.filename().unwrap_or("").to_string());
        files.push(IndexFile {
            path: relative_path,
            size_bytes: meta.size,
        });
    }
    Ok(files)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    /// Demonstrates the pattern a disk-backed cache backend would use:
    /// serialize entries to bytes, store in a key-value map, then
    /// deserialize on retrieval.
    #[test]
    fn test_index_metadata_codec_roundtrip() {
        let codec = index_metadata_codec();

        let original = vec![
            IndexMetadata {
                uuid: Uuid::new_v4(),
                name: "my_index".to_string(),
                fields: vec![0, 1],
                dataset_version: 42,
                fragment_bitmap: Some(RoaringBitmap::from_iter([1, 2, 3])),
                index_details: None,
                index_version: 1,
                created_at: None,
                base_id: None,
                files: Some(vec![IndexFile {
                    path: "index.idx".to_string(),
                    size_bytes: 1024,
                }]),
            },
            IndexMetadata {
                uuid: Uuid::new_v4(),
                name: "second_index".to_string(),
                fields: vec![2],
                dataset_version: 43,
                fragment_bitmap: None,
                index_details: None,
                index_version: 2,
                created_at: None,
                base_id: Some(7),
                files: None,
            },
        ];

        // Simulate a disk-backed store: HashMap<String, Vec<u8>>
        let mut store: HashMap<String, Vec<u8>> = HashMap::new();

        // Serialize into the store
        let key = "dataset/v42/Vec<IndexMetadata>".to_string();
        let mut buf = Vec::new();
        let entry: Arc<dyn std::any::Any + Send + Sync> = Arc::new(original.clone());
        codec.serialize(&entry, &mut buf).unwrap();
        store.insert(key.clone(), buf);

        // Deserialize from the store
        let bytes = store.get(&key).unwrap();
        let recovered = codec
            .deserialize(&bytes::Bytes::copy_from_slice(bytes))
            .hit()
            .expect("entry should decode as a hit");
        let recovered = recovered
            .downcast::<Vec<IndexMetadata>>()
            .expect("downcast should succeed");

        assert_eq!(original.len(), recovered.len());
        for (orig, rec) in original.iter().zip(recovered.iter()) {
            assert_eq!(orig.uuid, rec.uuid);
            assert_eq!(orig.name, rec.name);
            assert_eq!(orig.fields, rec.fields);
            assert_eq!(orig.dataset_version, rec.dataset_version);
            assert_eq!(orig.fragment_bitmap, rec.fragment_bitmap);
            assert_eq!(orig.index_version, rec.index_version);
            assert_eq!(orig.base_id, rec.base_id);
            assert_eq!(orig.files, rec.files);
        }
    }
}
