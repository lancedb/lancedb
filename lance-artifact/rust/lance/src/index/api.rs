// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use lance_index::{IndexParams, IndexType, PrewarmOptions, optimize::OptimizeOptions};
use lance_table::format::IndexMetadata;
use roaring::RoaringBitmap;
use uuid::Uuid;

use crate::{Error, Result};

/// A single physical segment of a logical index.
///
/// Each segment is stored independently and will become one manifest entry when committed.
/// The logical index name is provided separately by the commit API, while physical field and
/// dataset-version provenance travel with the segment.
#[derive(Debug, Clone, PartialEq)]
pub struct IndexSegment {
    /// Unique ID of the physical segment.
    uuid: Uuid,
    /// The fragments covered by this segment.
    fragment_bitmap: RoaringBitmap,
    /// Field IDs whose physical values are encoded in this segment.
    fields: Vec<i32>,
    /// Metadata specific to the index type.
    index_details: Arc<prost_types::Any>,
    /// The on-disk index version for this segment.
    index_version: i32,
    /// Dataset version at which this segment's physical contents were built.
    dataset_version: u64,
}

impl IndexSegment {
    /// Create a fully described segment with its physical build provenance.
    pub fn new<I, F>(
        uuid: Uuid,
        fragment_bitmap: I,
        fields: F,
        index_details: Arc<prost_types::Any>,
        index_version: i32,
        dataset_version: u64,
    ) -> Self
    where
        I: IntoIterator<Item = u32>,
        F: IntoIterator<Item = i32>,
    {
        Self {
            uuid,
            fragment_bitmap: fragment_bitmap.into_iter().collect(),
            fields: fields.into_iter().collect(),
            index_details,
            index_version,
            dataset_version,
        }
    }

    /// Return the UUID of this segment.
    pub fn uuid(&self) -> Uuid {
        self.uuid
    }

    /// Return the fragment coverage of this segment.
    pub fn fragment_bitmap(&self) -> &RoaringBitmap {
        &self.fragment_bitmap
    }

    pub(crate) fn fragment_bitmap_mut(&mut self) -> &mut RoaringBitmap {
        &mut self.fragment_bitmap
    }

    /// Return the field IDs whose values are encoded in this segment.
    pub fn fields(&self) -> &[i32] {
        &self.fields
    }

    /// Return the serialized index details for this segment.
    pub fn index_details(&self) -> &Arc<prost_types::Any> {
        &self.index_details
    }

    /// Return the on-disk index version for this segment.
    pub fn index_version(&self) -> i32 {
        self.index_version
    }

    /// Return the source dataset version for this segment.
    pub fn dataset_version(&self) -> u64 {
        self.dataset_version
    }

    /// Consume the segment and return its component parts.
    pub fn into_parts(
        self,
    ) -> (
        Uuid,
        RoaringBitmap,
        Vec<i32>,
        Arc<prost_types::Any>,
        i32,
        u64,
    ) {
        (
            self.uuid,
            self.fragment_bitmap,
            self.fields,
            self.index_details,
            self.index_version,
            self.dataset_version,
        )
    }
}

/// Convert an existing index segment representation into [`IndexSegment`].
pub trait IntoIndexSegment {
    /// Convert into an index segment.
    fn into_index_segment(self) -> Result<IndexSegment>;
}

impl IntoIndexSegment for IndexSegment {
    fn into_index_segment(self) -> Result<IndexSegment> {
        Ok(self)
    }
}

impl IntoIndexSegment for IndexMetadata {
    fn into_index_segment(self) -> Result<IndexSegment> {
        let fragment_bitmap = self.fragment_bitmap.ok_or_else(|| {
            Error::invalid_input(format!(
                "CreateIndex: segment {} is missing fragment coverage",
                self.uuid
            ))
        })?;
        let index_details = self.index_details.ok_or_else(|| {
            Error::invalid_input(format!(
                "CreateIndex: segment {} is missing index details",
                self.uuid
            ))
        })?;

        Ok(IndexSegment::new(
            self.uuid,
            fragment_bitmap.iter(),
            self.fields,
            index_details,
            self.index_version,
            self.dataset_version,
        ))
    }
}

/// Extends [`crate::Dataset`] with secondary index APIs.
#[async_trait]
pub trait DatasetIndexExt {
    type IndexBuilder<'a>
    where
        Self: 'a;

    /// Create a builder for creating an index on columns.
    ///
    /// This returns a builder that can be configured with additional options
    /// like `name()`, `replace()`, and `train()` before awaiting to execute.
    fn create_index_builder<'a>(
        &'a mut self,
        columns: &'a [&'a str],
        index_type: IndexType,
        params: &'a dyn IndexParams,
    ) -> Self::IndexBuilder<'a>;

    /// Create indices on columns.
    ///
    /// Upon finish, a new dataset version is generated.
    async fn create_index(
        &mut self,
        columns: &[&str],
        index_type: IndexType,
        name: Option<String>,
        params: &dyn IndexParams,
        replace: bool,
    ) -> Result<IndexMetadata>;

    /// Drop indices by name.
    ///
    /// Upon finish, a new dataset version is generated.
    async fn drop_index(&mut self, name: &str) -> Result<()>;

    /// Prewarm an index by name.
    ///
    /// This will load the index into memory and cache it.
    async fn prewarm_index(&self, name: &str) -> Result<()>;

    /// Prewarm an index by name with additional options.
    async fn prewarm_index_with_options(
        &self,
        _name: &str,
        _options: &PrewarmOptions,
    ) -> Result<()> {
        Err(Error::not_supported(
            "prewarm options are not supported by this dataset implementation".to_owned(),
        ))
    }

    /// Prewarm selected physical segments of an index by name.
    async fn prewarm_index_segments(&self, _name: &str, _segment_ids: &[Uuid]) -> Result<()> {
        Err(Error::not_supported(
            "segment-level prewarm is not supported by this dataset implementation".to_owned(),
        ))
    }

    /// Prewarm selected physical segments of an index by name with additional options.
    async fn prewarm_index_segments_with_options(
        &self,
        _name: &str,
        _segment_ids: &[Uuid],
        _options: &PrewarmOptions,
    ) -> Result<()> {
        Err(Error::not_supported(
            "prewarm options are not supported by this dataset implementation".to_owned(),
        ))
    }

    /// Read all indices of this Dataset version.
    ///
    /// The indices are lazy loaded and cached in memory within the `Dataset` instance.
    /// The cache is invalidated when the dataset version (Manifest) is changed.
    async fn load_indices(&self) -> Result<Arc<Vec<IndexMetadata>>>;

    /// Loads all the indices of a given UUID.
    ///
    /// Note that it is possible to have multiple indices with the same UUID,
    /// as they are the deltas of the same index.
    async fn load_index(&self, uuid: &Uuid) -> Result<Option<IndexMetadata>> {
        self.load_indices()
            .await
            .map(|indices| indices.iter().find(|idx| idx.uuid == *uuid).cloned())
    }

    /// Loads a specific index with the given index name.
    ///
    /// Returns `Ok(vec![])` if the index does not exist.
    async fn load_indices_by_name(&self, name: &str) -> Result<Vec<IndexMetadata>> {
        self.load_indices().await.map(|indices| {
            indices
                .iter()
                .filter(|idx| idx.name == name)
                .cloned()
                .collect()
        })
    }

    /// Loads a specific index with the given index name.
    /// This function only works for indices that are unique.
    /// If there are multiple indices sharing the same name, please use [`Self::load_indices_by_name`].
    async fn load_index_by_name(&self, name: &str) -> Result<Option<IndexMetadata>> {
        let indices = self.load_indices_by_name(name).await?;
        if indices.is_empty() {
            Ok(None)
        } else if indices.len() == 1 {
            Ok(Some(indices[0].clone()))
        } else {
            Err(Error::index(format!(
                "Found multiple indices of the same name: {:?}, please use load_indices_by_name",
                indices.iter().map(|idx| &idx.name).collect::<Vec<_>>()
            )))
        }
    }

    /// Describes indexes in a dataset.
    ///
    /// This method should only access the index metadata and should not load the index into memory.
    async fn describe_indices<'a, 'b>(
        &'a self,
        criteria: Option<lance_index::IndexCriteria<'b>>,
    ) -> Result<Vec<Arc<dyn lance_index::IndexDescription>>>;

    /// Loads a specific scalar index using the provided criteria.
    async fn load_scalar_index<'a, 'b>(
        &'a self,
        criteria: lance_index::IndexCriteria<'b>,
    ) -> Result<Option<IndexMetadata>>;

    /// Optimize indices.
    async fn optimize_indices(&mut self, options: &OptimizeOptions) -> Result<()>;

    /// Find an index with the given name and return its serialized statistics.
    async fn index_statistics(&self, index_name: &str) -> Result<String>;

    /// Merge one or more existing uncommitted index segments into a single uncommitted segment.
    async fn merge_existing_index_segments(
        &self,
        source_segments: Vec<IndexMetadata>,
    ) -> Result<IndexMetadata>;

    /// Commit one or more existing physical index segments as a logical index.
    async fn commit_existing_index_segments(
        &mut self,
        index_name: &str,
        column: &str,
        segments: Vec<impl IntoIndexSegment + Send>,
    ) -> Result<()>;

    async fn read_index_partition(
        &self,
        index_name: &str,
        partition_id: usize,
        with_vector: bool,
    ) -> Result<SendableRecordBatchStream>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_index_metadata_conversion_preserves_provenance() {
        let metadata = IndexMetadata {
            uuid: Uuid::new_v4(),
            name: "test".to_string(),
            fields: vec![3, 7],
            dataset_version: 42,
            fragment_bitmap: Some(RoaringBitmap::from_iter([1, 2])),
            index_details: Some(Arc::new(prost_types::Any {
                type_url: "example.IndexDetails".to_string(),
                value: vec![1, 2, 3],
            })),
            index_version: 5,
            created_at: None,
            base_id: None,
            files: None,
        };

        let segment = metadata.into_index_segment().unwrap();
        assert_eq!(segment.fields(), [3, 7]);
        assert_eq!(segment.dataset_version(), 42);
    }
}
