// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use async_trait::async_trait;
use chrono::prelude::*;
use lance_core::deepsize::DeepSizeOf;
use lance_file::datatypes::{Fields, FieldsWithMeta};
use lance_file::version::{ConcreteFileVersion, LanceFileVersion};
use lance_file::versions::v1::{
    encoding::populate_schema_dictionaries, reader::FileReader as V1FileReader,
};
use lance_io::traits::{ProtoStruct, Reader};
use object_store::path::Path;
use prost::Message;
use prost_types::Timestamp;
use std::collections::{BTreeMap, HashMap};
use std::ops::Range;
use std::sync::Arc;

use super::Fragment;
use crate::feature_flags::{FLAG_STABLE_ROW_IDS, has_deprecated_v2_feature_flag};
use crate::format::fragment::DataFileFieldInterner;
use crate::format::pb;
use lance_core::cache::LanceCache;
use lance_core::datatypes::Schema;
use lance_core::{Error, Result};
use lance_io::object_store::{ObjectStore, ObjectStoreRegistry};
use lance_io::utils::read_struct;

/// Manifest of a dataset
///
///  * Schema
///  * Version
///  * Fragments.
///  * Indices.
#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct Manifest {
    /// Dataset schema.
    pub schema: Schema,

    /// Dataset version
    pub version: u64,

    /// Branch name, None if the dataset is the main branch.
    pub branch: Option<String>,

    /// Version of the writer library that wrote this manifest.
    pub writer_version: Option<WriterVersion>,

    /// Fragments, the pieces to build the dataset.
    ///
    /// This list is stored in order, sorted by fragment id.  However, the fragment id
    /// sequence may have gaps.
    pub fragments: Arc<Vec<Fragment>>,

    /// The file position of the version aux data.
    pub version_aux_data: usize,

    /// The file position of the index metadata.
    pub index_section: Option<usize>,

    /// The creation timestamp with nanosecond resolution as 128-bit integer
    pub timestamp_nanos: u128,

    /// An optional string tag for this version
    pub tag: Option<String>,

    /// The reader flags
    pub reader_feature_flags: u64,

    /// The writer flags
    pub writer_feature_flags: u64,

    /// The max fragment id used so far
    /// None means never set, Some(0) means max ID used so far is 0
    pub max_fragment_id: Option<u32>,

    /// The path to the transaction file, relative to the root of the dataset
    pub transaction_file: Option<String>,

    /// The file position of the inline transaction content inside the manifest
    pub transaction_section: Option<usize>,

    /// Precomputed logic offset of each fragment
    /// accelerating the fragment search using offset ranges.
    fragment_offsets: Vec<usize>,

    /// The max row id used so far.
    pub next_row_id: u64,

    /// The storage format of the data files.
    pub data_storage_format: DataStorageFormat,

    /// Table configuration.
    pub config: HashMap<String, String>,

    /// Table metadata.
    ///
    /// This is a key-value map that can be used to store arbitrary metadata
    /// associated with the table. This is different than configuration, which
    /// is used to tell libraries how to read, write, or manage the table.
    pub table_metadata: HashMap<String, String>,

    /* external base paths */
    pub base_paths: HashMap<u32, BasePath>,
}

// We use the most significant bit to indicate that a transaction is detached
pub const DETACHED_VERSION_MASK: u64 = 0x8000_0000_0000_0000;

pub fn is_detached_version(version: u64) -> bool {
    version & DETACHED_VERSION_MASK != 0
}

fn compute_fragment_offsets(fragments: &[Fragment]) -> Vec<usize> {
    fragments
        .iter()
        .map(|f| f.num_rows().unwrap_or_default())
        .chain([0]) // Make the last offset to be the full-length of the dataset.
        .scan(0_usize, |offset, len| {
            let start = *offset;
            *offset += len;
            Some(start)
        })
        .collect()
}

#[derive(Default)]
pub struct ManifestSummary {
    pub total_fragments: u64,
    pub total_data_files: u64,
    pub total_files_size: u64,
    pub total_deletion_files: u64,
    pub total_data_file_rows: u64,
    pub total_deletion_file_rows: u64,
    pub total_rows: u64,
}

impl From<ManifestSummary> for BTreeMap<String, String> {
    fn from(summary: ManifestSummary) -> Self {
        let mut stats_map = Self::new();
        stats_map.insert(
            "total_fragments".to_string(),
            summary.total_fragments.to_string(),
        );
        stats_map.insert(
            "total_data_files".to_string(),
            summary.total_data_files.to_string(),
        );
        stats_map.insert(
            "total_files_size".to_string(),
            summary.total_files_size.to_string(),
        );
        stats_map.insert(
            "total_deletion_files".to_string(),
            summary.total_deletion_files.to_string(),
        );
        stats_map.insert(
            "total_data_file_rows".to_string(),
            summary.total_data_file_rows.to_string(),
        );
        stats_map.insert(
            "total_deletion_file_rows".to_string(),
            summary.total_deletion_file_rows.to_string(),
        );
        stats_map.insert("total_rows".to_string(), summary.total_rows.to_string());
        stats_map
    }
}

impl Manifest {
    pub fn new(
        schema: Schema,
        fragments: Arc<Vec<Fragment>>,
        data_storage_format: DataStorageFormat,
        base_paths: HashMap<u32, BasePath>,
    ) -> Self {
        let fragment_offsets = compute_fragment_offsets(&fragments);

        Self {
            schema,
            version: 1,
            branch: None,
            writer_version: Some(WriterVersion::default()),
            fragments,
            version_aux_data: 0,
            index_section: None,
            timestamp_nanos: 0,
            tag: None,
            reader_feature_flags: 0,
            writer_feature_flags: 0,
            max_fragment_id: None,
            transaction_file: None,
            transaction_section: None,
            fragment_offsets,
            next_row_id: 0,
            data_storage_format,
            config: HashMap::new(),
            table_metadata: HashMap::new(),
            base_paths,
        }
    }

    pub fn new_from_previous(
        previous: &Self,
        schema: Schema,
        fragments: Arc<Vec<Fragment>>,
    ) -> Self {
        let fragment_offsets = compute_fragment_offsets(&fragments);

        Self {
            schema,
            version: previous.version + 1,
            branch: previous.branch.clone(),
            writer_version: Some(WriterVersion::default()),
            fragments,
            version_aux_data: 0,
            index_section: None, // Caller should update index if they want to keep them.
            timestamp_nanos: 0,  // This will be set on commit
            tag: None,
            reader_feature_flags: 0, // These will be set on commit
            writer_feature_flags: 0, // These will be set on commit
            max_fragment_id: previous.max_fragment_id,
            transaction_file: None,
            transaction_section: None,
            fragment_offsets,
            next_row_id: previous.next_row_id,
            data_storage_format: previous.data_storage_format.clone(),
            config: previous.config.clone(),
            table_metadata: previous.table_metadata.clone(),
            base_paths: previous.base_paths.clone(),
        }
    }

    /// Performs a shallow_clone of the manifest entirely in memory without:
    /// - Any persistent storage operations
    /// - Modifications to the original data
    /// - If the shallow clone is for branch, ref_name is the source branch
    pub fn shallow_clone(
        &self,
        ref_name: Option<String>,
        ref_path: String,
        ref_base_id: u32,
        branch_name: Option<String>,
        transaction_file: String,
    ) -> Self {
        let cloned_fragments = self
            .fragments
            .as_ref()
            .iter()
            .map(|fragment| {
                let mut cloned_fragment = fragment.clone();
                for file in &mut cloned_fragment.files {
                    if file.base_id.is_none() {
                        file.base_id = Some(ref_base_id);
                    }
                }

                if let Some(deletion) = &mut cloned_fragment.deletion_file
                    && deletion.base_id.is_none()
                {
                    deletion.base_id = Some(ref_base_id);
                }
                cloned_fragment
            })
            .collect::<Vec<_>>();

        Self {
            schema: self.schema.clone(),
            version: self.version,
            branch: branch_name,
            writer_version: self.writer_version.clone(),
            fragments: Arc::new(cloned_fragments),
            version_aux_data: self.version_aux_data,
            index_section: None, // These will be set on commit
            timestamp_nanos: self.timestamp_nanos,
            tag: None,
            reader_feature_flags: 0, // These will be set on commit
            writer_feature_flags: 0, // These will be set on commit
            max_fragment_id: self.max_fragment_id,
            transaction_file: Some(transaction_file),
            transaction_section: None,
            fragment_offsets: self.fragment_offsets.clone(),
            next_row_id: self.next_row_id,
            data_storage_format: self.data_storage_format.clone(),
            config: self.config.clone(),
            base_paths: {
                let mut base_paths = self.base_paths.clone();
                let base_path = BasePath::new(ref_base_id, ref_path, ref_name, true);
                base_paths.insert(ref_base_id, base_path);
                base_paths
            },
            table_metadata: self.table_metadata.clone(),
        }
    }

    /// Return the `timestamp_nanos` value as a Utc DateTime
    pub fn timestamp(&self) -> DateTime<Utc> {
        let nanos = self.timestamp_nanos % 1_000_000_000;
        let seconds = ((self.timestamp_nanos - nanos) / 1_000_000_000) as i64;
        Utc.from_utc_datetime(
            &DateTime::from_timestamp(seconds, nanos as u32)
                .unwrap_or_default()
                .naive_utc(),
        )
    }

    /// Set the `timestamp_nanos` value from a Utc DateTime
    pub fn set_timestamp(&mut self, nanos: u128) {
        self.timestamp_nanos = nanos;
    }

    /// Get a mutable reference to the config
    pub fn config_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.config
    }

    /// Get a mutable reference to the table metadata
    pub fn table_metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.table_metadata
    }

    /// Get a mutable reference to the schema metadata
    pub fn schema_metadata_mut(&mut self) -> &mut HashMap<String, String> {
        &mut self.schema.metadata
    }

    /// Get a mutable reference to the field metadata for a specific field id
    ///
    /// Returns None if the field does not exist in the schema.
    pub fn field_metadata_mut(&mut self, field_id: i32) -> Option<&mut HashMap<String, String>> {
        self.schema
            .field_by_id_mut(field_id)
            .map(|field| &mut field.metadata)
    }

    /// Set the `config` from an iterator
    #[deprecated(note = "Use config_mut() for direct access to config HashMap")]
    pub fn update_config(&mut self, upsert_values: impl IntoIterator<Item = (String, String)>) {
        self.config.extend(upsert_values);
    }

    /// Delete `config` keys using a slice of keys
    #[deprecated(note = "Use config_mut() for direct access to config HashMap")]
    pub fn delete_config_keys(&mut self, delete_keys: &[&str]) {
        self.config
            .retain(|key, _| !delete_keys.contains(&key.as_str()));
    }

    /// Replaces the schema metadata with the given key-value pairs.
    #[deprecated(note = "Use schema_metadata_mut() for direct access to schema metadata HashMap")]
    pub fn replace_schema_metadata(&mut self, new_metadata: HashMap<String, String>) {
        self.schema.metadata = new_metadata;
    }

    /// Replaces the metadata of the field with the given id with the given key-value pairs.
    ///
    /// If the field does not exist in the schema, this is a no-op.
    #[deprecated(
        note = "Use field_metadata_mut(field_id) for direct access to field metadata HashMap"
    )]
    pub fn replace_field_metadata(
        &mut self,
        field_id: i32,
        new_metadata: HashMap<String, String>,
    ) -> Result<()> {
        if let Some(field) = self.schema.field_by_id_mut(field_id) {
            field.metadata = new_metadata;
            Ok(())
        } else {
            Err(Error::invalid_input(format!(
                "Field with id {} does not exist for replace_field_metadata",
                field_id
            )))
        }
    }

    /// Check the current fragment list and update the high water mark
    pub fn update_max_fragment_id(&mut self) {
        // If there are no fragments, don't update max_fragment_id
        if self.fragments.is_empty() {
            return;
        }

        let max_fragment_id = self
            .fragments
            .iter()
            .map(|f| f.id)
            .max()
            .unwrap() // Safe because we checked fragments is not empty
            .try_into()
            .unwrap();

        match self.max_fragment_id {
            None => {
                // First time being set
                self.max_fragment_id = Some(max_fragment_id);
            }
            Some(current_max) => {
                // Only update if the computed max is greater than current
                // This preserves the high water mark even when fragments are deleted
                if max_fragment_id > current_max {
                    self.max_fragment_id = Some(max_fragment_id);
                }
            }
        }
    }

    /// Return the max fragment id.
    /// Note this does not support recycling of fragment ids.
    ///
    /// This will return None if there are no fragments and max_fragment_id was never set.
    pub fn max_fragment_id(&self) -> Option<u64> {
        if let Some(max_id) = self.max_fragment_id {
            // Return the stored high water mark
            Some(max_id.into())
        } else {
            // Not yet set, compute from fragment list
            self.fragments.iter().map(|f| f.id).max()
        }
    }

    /// Get the max used field id
    ///
    /// This is different than [Schema::max_field_id] because it also considers
    /// the field ids in the data files that have been dropped from the schema.
    pub fn max_field_id(&self) -> i32 {
        let schema_max_id = self.schema.max_field_id().unwrap_or(-1);
        let fragment_max_id = self
            .fragments
            .iter()
            .flat_map(|f| f.files.iter().flat_map(|file| file.fields.iter()))
            .max()
            .copied();
        let fragment_max_id = fragment_max_id.unwrap_or(-1);
        schema_max_id.max(fragment_max_id)
    }

    /// Return the fragments that are newer than the given manifest.
    /// Note this does not support recycling of fragment ids.
    pub fn fragments_since(&self, since: &Self) -> Result<Vec<Fragment>> {
        if since.version >= self.version {
            return Err(Error::invalid_input(format!(
                "fragments_since: given version {} is newer than manifest version {}",
                since.version, self.version
            )));
        }
        let start = since.max_fragment_id();
        Ok(self
            .fragments
            .iter()
            .filter(|&f| start.map(|s| f.id > s).unwrap_or(true))
            .cloned()
            .collect())
    }

    /// Find the fragments that contain the rows, identified by the offset range.
    ///
    /// Note that the offsets are the logical offsets of rows, not row IDs.
    ///
    ///
    /// Parameters
    /// ----------
    /// range: `Range<usize>`
    ///     Offset range
    ///
    /// Returns
    /// -------
    /// Vec<(usize, Fragment)>
    ///    A vector of `(starting_offset_of_fragment, fragment)` pairs.
    ///
    pub fn fragments_by_offset_range(&self, range: Range<usize>) -> Vec<(usize, &Fragment)> {
        let start = range.start;
        let end = range.end;
        let idx = self
            .fragment_offsets
            .binary_search(&start)
            .unwrap_or_else(|idx| idx - 1);

        let mut fragments = vec![];
        for i in idx..self.fragments.len() {
            if self.fragment_offsets[i] >= end
                || self.fragment_offsets[i] + self.fragments[i].num_rows().unwrap_or_default()
                    <= start
            {
                break;
            }
            fragments.push((self.fragment_offsets[i], &self.fragments[i]));
        }

        fragments
    }

    /// Whether the dataset uses stable row ids.
    pub fn uses_stable_row_ids(&self) -> bool {
        self.reader_feature_flags & FLAG_STABLE_ROW_IDS != 0
    }

    /// Creates a serialized copy of the manifest, suitable for IPC or temp storage
    /// and can be used to create a dataset
    pub fn serialized(&self) -> Vec<u8> {
        let pb_manifest: pb::Manifest = self.into();
        pb_manifest.encode_to_vec()
    }

    pub fn should_use_legacy_format(&self) -> bool {
        self.data_storage_format.version == ConcreteFileVersion::V1
    }

    /// Get the summary information of a manifest.
    ///
    /// This function calculates various statistics about the manifest, including:
    /// - total_files_size: Total size of all data files in bytes
    /// - total_fragments: Total number of fragments in the dataset
    /// - total_data_files: Total number of data files across all fragments
    /// - total_deletion_files: Total number of deletion files
    /// - total_data_file_rows: Total number of rows in data files
    /// - total_deletion_file_rows: Total number of deleted rows in deletion files
    /// - total_rows: Total number of rows in the dataset
    pub fn summary(&self) -> ManifestSummary {
        // Calculate total fragments
        let mut summary =
            self.fragments
                .iter()
                .fold(ManifestSummary::default(), |mut summary, f| {
                    // Count data files in the current fragment
                    summary.total_data_files += f.files.len() as u64;
                    // Sum the number of rows for the current fragment (if available)
                    if let Some(num_rows) = f.num_rows() {
                        summary.total_rows += num_rows as u64;
                    }
                    // Sum file sizes for all data files in the current fragment (if available)
                    for data_file in &f.files {
                        if let Some(size_bytes) = data_file.file_size_bytes.get() {
                            summary.total_files_size += size_bytes.get();
                        }
                    }
                    // Check and count if the current fragment has a deletion file
                    if f.deletion_file.is_some() {
                        summary.total_deletion_files += 1;
                    }
                    // Sum the number of deleted rows from the deletion file (if available)
                    if let Some(deletion_file) = &f.deletion_file
                        && let Some(num_deleted) = deletion_file.num_deleted_rows
                    {
                        summary.total_deletion_file_rows += num_deleted as u64;
                    }
                    summary
                });
        summary.total_fragments = self.fragments.len() as u64;
        summary.total_data_file_rows = summary.total_rows + summary.total_deletion_file_rows;

        summary
    }
}

/// Populate dictionary values stored outside a V1 manifest.
///
/// Other exact file versions store their schema dictionaries inline, so this
/// is a no-op for those manifests.
///
/// # Examples
///
/// ```
/// # use lance_core::Result;
/// # use lance_io::traits::Reader;
/// # use lance_table::format::{Manifest, populate_manifest_schema_dictionaries};
/// # async fn hydrate_v1_manifest(
/// #     manifest: &mut Manifest,
/// #     reader: &dyn Reader,
/// # ) -> Result<()> {
/// populate_manifest_schema_dictionaries(manifest, reader).await
/// # }
/// ```
pub async fn populate_manifest_schema_dictionaries(
    manifest: &mut Manifest,
    reader: &dyn Reader,
) -> Result<()> {
    match manifest.data_storage_format.version {
        ConcreteFileVersion::V1 => {
            populate_schema_dictionaries(&mut manifest.schema, reader).await?;
        }
        ConcreteFileVersion::V2_0
        | ConcreteFileVersion::V2_1
        | ConcreteFileVersion::V2_2
        | ConcreteFileVersion::V2_3 => {}
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq)]
pub struct BasePath {
    pub id: u32,
    pub name: Option<String>,
    pub is_dataset_root: bool,
    /// The full URI string (e.g., "s3://bucket/path")
    pub path: String,
}

impl BasePath {
    /// Create a new BasePath
    ///
    /// # Arguments
    ///
    /// * `id` - Unique identifier for this base path
    /// * `path` - Full URI string (e.g., "s3://bucket/path", "/local/path")
    /// * `name` - Optional human-readable name for this base
    /// * `is_dataset_root` - Whether this is the dataset root or a data-only base
    pub fn new(id: u32, path: String, name: Option<String>, is_dataset_root: bool) -> Self {
        Self {
            id,
            name,
            is_dataset_root,
            path,
        }
    }

    /// Extract the object store path from this BasePath's URI.
    ///
    /// This is a synchronous operation that parses the URI without initializing an object store.
    pub fn extract_path(&self, registry: Arc<ObjectStoreRegistry>) -> Result<Path> {
        ObjectStore::extract_path_from_uri(registry, &self.path)
    }
}

impl DeepSizeOf for BasePath {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.name.deep_size_of_children(context)
            + self.path.deep_size_of_children(context) * 2
            + size_of::<bool>()
    }
}

#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct WriterVersion {
    pub library: String,
    pub version: String,
    pub prerelease: Option<String>,
    pub build_metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, DeepSizeOf)]
pub struct DataStorageFormat {
    pub file_format: String,
    pub version: ConcreteFileVersion,
}

const LANCE_FORMAT_NAME: &str = "lance";

impl DataStorageFormat {
    pub fn new(version: ConcreteFileVersion) -> Self {
        Self {
            file_format: LANCE_FORMAT_NAME.to_string(),
            version,
        }
    }

    /// Return the exact file format version persisted by this manifest.
    pub fn lance_file_format(&self) -> ConcreteFileVersion {
        self.version
    }

    // Retained until all selector-based execution APIs migrate to exact versions.
    pub fn lance_file_version(&self) -> Result<LanceFileVersion> {
        Ok(self.version.into())
    }
}

impl Default for DataStorageFormat {
    fn default() -> Self {
        Self::new(ConcreteFileVersion::from(LanceFileVersion::Stable))
    }
}

impl TryFrom<pb::manifest::DataStorageFormat> for DataStorageFormat {
    type Error = Error;

    fn try_from(pb: pb::manifest::DataStorageFormat) -> Result<Self> {
        Ok(Self {
            file_format: pb.file_format,
            version: ConcreteFileVersion::from_manifest_string(&pb.version)?,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VersionPart {
    Major,
    Minor,
    Patch,
}

fn bump_version(version: &mut semver::Version, part: VersionPart) {
    match part {
        VersionPart::Major => {
            version.major += 1;
            version.minor = 0;
            version.patch = 0;
        }
        VersionPart::Minor => {
            version.minor += 1;
            version.patch = 0;
        }
        VersionPart::Patch => {
            version.patch += 1;
        }
    }
}

impl WriterVersion {
    /// Split a version string into clean version (major.minor.patch), prerelease, and build metadata.
    ///
    /// Returns None if the input is not a valid semver string.
    ///
    /// For example:
    /// - "2.0.0-rc.1" -> Some(("2.0.0", Some("rc.1"), None))
    /// - "2.0.0-rc.1+build.123" -> Some(("2.0.0", Some("rc.1"), Some("build.123")))
    /// - "2.0.0+build.123" -> Some(("2.0.0", None, Some("build.123")))
    /// - "not-a-version" -> None
    fn split_version(full_version: &str) -> Option<(String, Option<String>, Option<String>)> {
        let mut parsed = semver::Version::parse(full_version).ok()?;

        let prerelease = if parsed.pre.is_empty() {
            None
        } else {
            Some(parsed.pre.to_string())
        };

        let build_metadata = if parsed.build.is_empty() {
            None
        } else {
            Some(parsed.build.to_string())
        };

        // Remove prerelease and build metadata to get clean version
        parsed.pre = semver::Prerelease::EMPTY;
        parsed.build = semver::BuildMetadata::EMPTY;
        Some((parsed.to_string(), prerelease, build_metadata))
    }

    /// Try to parse the version string as a semver string. Returns None if
    /// not successful.
    #[deprecated(note = "Use `lance_lib_version()` instead")]
    pub fn semver(&self) -> Option<(u32, u32, u32, Option<&str>)> {
        // First split by '-' to separate the version from the pre-release tag
        let (version_part, tag) = if let Some(dash_idx) = self.version.find('-') {
            (
                &self.version[..dash_idx],
                Some(&self.version[dash_idx + 1..]),
            )
        } else {
            (self.version.as_str(), None)
        };

        let mut parts = version_part.split('.');
        let major = parts.next().unwrap_or("0").parse().ok()?;
        let minor = parts.next().unwrap_or("0").parse().ok()?;
        let patch = parts.next().unwrap_or("0").parse().ok()?;

        Some((major, minor, patch, tag))
    }

    /// If the library is "lance", parse the version as semver and return it.
    /// Returns None if the library is not "lance" or the version cannot be parsed as semver.
    ///
    /// This method reconstructs the full semantic version by combining the version field
    /// with the prerelease and build_metadata fields (if present). For example:
    /// - version="2.0.0" + prerelease=Some("rc.1") -> "2.0.0-rc.1"
    /// - version="2.0.0" + prerelease=Some("rc.1") + build_metadata=Some("build.123") -> "2.0.0-rc.1+build.123"
    pub fn lance_lib_version(&self) -> Option<semver::Version> {
        if self.library != "lance" {
            return None;
        }

        let mut version = semver::Version::parse(&self.version).ok()?;

        if let Some(ref prerelease) = self.prerelease {
            version.pre = semver::Prerelease::new(prerelease).ok()?;
        }

        if let Some(ref build_metadata) = self.build_metadata {
            version.build = semver::BuildMetadata::new(build_metadata).ok()?;
        }

        Some(version)
    }

    #[deprecated(
        note = "Use `lance_lib_version()` instead, which safely checks the library field and returns Option"
    )]
    #[allow(deprecated)]
    pub fn semver_or_panic(&self) -> (u32, u32, u32, Option<&str>) {
        self.semver()
            .unwrap_or_else(|| panic!("Invalid writer version: {}", self.version))
    }

    /// Check if this is a Lance library version older than the given major/minor/patch.
    ///
    /// # Panics
    ///
    /// Panics if the library is not "lance" or the version cannot be parsed as semver.
    #[deprecated(note = "Use `lance_lib_version()` and its `older_than` method instead.")]
    pub fn older_than(&self, major: u32, minor: u32, patch: u32) -> bool {
        let version = self
            .lance_lib_version()
            .expect("Not lance library or invalid version");
        let other = semver::Version {
            major: major.into(),
            minor: minor.into(),
            patch: patch.into(),
            pre: semver::Prerelease::EMPTY,
            build: semver::BuildMetadata::EMPTY,
        };
        version < other
    }

    #[deprecated(note = "This is meant for testing and will be made private in future version.")]
    pub fn bump(&self, part: VersionPart, keep_tag: bool) -> Self {
        let mut version = self.lance_lib_version().expect("Should be lance version");
        bump_version(&mut version, part);
        if !keep_tag {
            version.pre = semver::Prerelease::EMPTY;
        }
        let (clean_version, prerelease, build_metadata) = Self::split_version(&version.to_string())
            .expect("Bumped version should be valid semver");
        Self {
            library: self.library.clone(),
            version: clean_version,
            prerelease,
            build_metadata,
        }
    }
}

impl Default for WriterVersion {
    #[cfg(not(test))]
    fn default() -> Self {
        let full_version = env!("CARGO_PKG_VERSION");
        let (version, prerelease, build_metadata) =
            Self::split_version(full_version).expect("CARGO_PKG_VERSION should be valid semver");
        Self {
            library: "lance".to_string(),
            version,
            prerelease,
            build_metadata,
        }
    }

    // Unit tests always run as if they are in the next version.
    #[cfg(test)]
    #[allow(deprecated)]
    fn default() -> Self {
        let full_version = env!("CARGO_PKG_VERSION");
        let (version, prerelease, build_metadata) =
            Self::split_version(full_version).expect("CARGO_PKG_VERSION should be valid semver");
        Self {
            library: "lance".to_string(),
            version,
            prerelease,
            build_metadata,
        }
        .bump(VersionPart::Patch, true)
    }
}

impl ProtoStruct for Manifest {
    type Proto = pb::Manifest;
}

impl From<pb::BasePath> for BasePath {
    fn from(p: pb::BasePath) -> Self {
        Self::new(p.id, p.path, p.name, p.is_dataset_root)
    }
}

impl From<BasePath> for pb::BasePath {
    fn from(p: BasePath) -> Self {
        Self {
            id: p.id,
            name: p.name,
            is_dataset_root: p.is_dataset_root,
            path: p.path,
        }
    }
}

impl TryFrom<pb::Manifest> for Manifest {
    type Error = Error;

    fn try_from(p: pb::Manifest) -> Result<Self> {
        let timestamp_nanos = p.timestamp.map(|ts| {
            let sec = ts.seconds as u128 * 1e9 as u128;
            let nanos = ts.nanos as u128;
            sec + nanos
        });
        // We only use the writer version if it is fully set.
        let writer_version = match p.writer_version {
            Some(pb::manifest::WriterVersion {
                library,
                version,
                prerelease,
                build_metadata,
            }) => Some(WriterVersion {
                library,
                version,
                prerelease,
                build_metadata,
            }),
            _ => None,
        };
        let mut interner = DataFileFieldInterner::default();
        let fragments = Arc::new(
            p.fragments
                .into_iter()
                .map(|f| interner.intern_fragment(f))
                .collect::<Result<Vec<_>>>()?,
        );
        let fragment_offsets = compute_fragment_offsets(fragments.as_slice());
        let fields_with_meta = FieldsWithMeta {
            fields: Fields(p.fields),
            metadata: p.schema_metadata,
        };

        if FLAG_STABLE_ROW_IDS & p.reader_feature_flags != 0
            && !fragments.iter().all(|frag| frag.row_id_meta.is_some())
        {
            return Err(Error::internal("All fragments must have row ids"));
        }

        let data_storage_format = match p.data_format {
            None => {
                if let Some(inferred_version) = Fragment::try_infer_version(fragments.as_ref())? {
                    // If there are fragments, they are a better indicator
                    DataStorageFormat::new(inferred_version)
                } else {
                    // No fragments to inspect, best we can do is look at writer flags
                    if has_deprecated_v2_feature_flag(p.writer_feature_flags) {
                        DataStorageFormat::new(ConcreteFileVersion::from(LanceFileVersion::Stable))
                    } else {
                        DataStorageFormat::new(ConcreteFileVersion::V1)
                    }
                }
            }
            Some(format) => DataStorageFormat::try_from(format)?,
        };

        let schema = Schema::try_from(fields_with_meta)?;

        Ok(Self {
            schema,
            version: p.version,
            branch: p.branch,
            writer_version,
            version_aux_data: p.version_aux_data as usize,
            index_section: p.index_section.map(|i| i as usize),
            timestamp_nanos: timestamp_nanos.unwrap_or(0),
            tag: if p.tag.is_empty() { None } else { Some(p.tag) },
            reader_feature_flags: p.reader_feature_flags,
            writer_feature_flags: p.writer_feature_flags,
            max_fragment_id: p.max_fragment_id,
            fragments,
            transaction_file: if p.transaction_file.is_empty() {
                None
            } else {
                Some(p.transaction_file)
            },
            transaction_section: p.transaction_section.map(|i| i as usize),
            fragment_offsets,
            next_row_id: p.next_row_id,
            data_storage_format,
            config: p.config,
            table_metadata: p.table_metadata,
            base_paths: p
                .base_paths
                .iter()
                .map(|item| (item.id, item.clone().into()))
                .collect(),
        })
    }
}

impl From<&Manifest> for pb::Manifest {
    fn from(m: &Manifest) -> Self {
        let timestamp_nanos = if m.timestamp_nanos == 0 {
            None
        } else {
            let nanos = m.timestamp_nanos % 1e9 as u128;
            let seconds = ((m.timestamp_nanos - nanos) / 1e9 as u128) as i64;
            Some(Timestamp {
                seconds,
                nanos: nanos as i32,
            })
        };
        let fields_with_meta: FieldsWithMeta = (&m.schema).into();
        Self {
            fields: fields_with_meta.fields.0,
            schema_metadata: m
                .schema
                .metadata
                .iter()
                .map(|(k, v)| (k.clone(), v.as_bytes().to_vec()))
                .collect(),
            version: m.version,
            branch: m.branch.clone(),
            writer_version: m
                .writer_version
                .as_ref()
                .map(|wv| pb::manifest::WriterVersion {
                    library: wv.library.clone(),
                    version: wv.version.clone(),
                    prerelease: wv.prerelease.clone(),
                    build_metadata: wv.build_metadata.clone(),
                }),
            fragments: m.fragments.iter().map(pb::DataFragment::from).collect(),
            table_metadata: m.table_metadata.clone(),
            version_aux_data: m.version_aux_data as u64,
            index_section: m.index_section.map(|i| i as u64),
            timestamp: timestamp_nanos,
            tag: m.tag.clone().unwrap_or_default(),
            reader_feature_flags: m.reader_feature_flags,
            writer_feature_flags: m.writer_feature_flags,
            max_fragment_id: m.max_fragment_id,
            transaction_file: m.transaction_file.clone().unwrap_or_default(),
            next_row_id: m.next_row_id,
            data_format: Some(pb::manifest::DataStorageFormat {
                file_format: m.data_storage_format.file_format.clone(),
                version: m
                    .data_storage_format
                    .version
                    .to_manifest_string()
                    .to_string(),
            }),
            config: m.config.clone(),
            base_paths: m
                .base_paths
                .values()
                .map(|base_path| pb::BasePath {
                    id: base_path.id,
                    name: base_path.name.clone(),
                    is_dataset_root: base_path.is_dataset_root,
                    path: base_path.path.clone(),
                })
                .collect(),
            transaction_section: m.transaction_section.map(|i| i as u64),
        }
    }
}

#[async_trait]
pub trait SelfDescribingFileReader {
    /// Open a file reader without any cached schema
    ///
    /// In this case the schema will first need to be loaded
    /// from the file itself.
    ///
    /// When loading files from a dataset it is preferable to use
    /// the fragment reader to avoid this overhead.
    async fn try_new_self_described(
        object_store: &ObjectStore,
        path: &Path,
        cache: Option<&LanceCache>,
    ) -> Result<Self>
    where
        Self: Sized,
    {
        let reader = object_store.open(path).await?;
        Self::try_new_self_described_from_reader(reader.into(), cache).await
    }

    async fn try_new_self_described_from_reader(
        reader: Arc<dyn Reader>,
        cache: Option<&LanceCache>,
    ) -> Result<Self>
    where
        Self: Sized;
}

#[async_trait]
impl SelfDescribingFileReader for V1FileReader {
    async fn try_new_self_described_from_reader(
        reader: Arc<dyn Reader>,
        cache: Option<&LanceCache>,
    ) -> Result<Self> {
        let metadata = Self::read_metadata(reader.as_ref(), cache).await?;
        let manifest_position = metadata.manifest_position.ok_or(Error::internal(format!(
            "Attempt to open file at {} as self-describing but it did not contain a manifest",
            reader.path(),
        )))?;
        let mut manifest: Manifest = read_struct(reader.as_ref(), manifest_position).await?;
        populate_manifest_schema_dictionaries(&mut manifest, reader.as_ref()).await?;
        let schema = manifest.schema;
        let max_field_id = schema.max_field_id().unwrap_or_default();
        Self::try_new_from_reader(
            reader.path(),
            reader.clone(),
            Some(metadata),
            schema,
            0,
            0,
            max_field_id,
            cache,
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use crate::feature_flags::FLAG_USE_V2_FORMAT_DEPRECATED;
    use crate::format::{DataFile, DeletionFile, DeletionFileType};
    use std::num::NonZero;

    use super::*;

    use arrow_schema::{Field as ArrowField, Schema as ArrowSchema};
    use lance_core::datatypes::Field;

    #[test]
    fn old_empty_manifest_recovers_v1_or_current_stable() {
        let old_manifest = pb::Manifest {
            data_format: None,
            ..Default::default()
        };
        let recovered_v1 = Manifest::try_from(old_manifest.clone()).unwrap();
        assert_eq!(
            recovered_v1.data_storage_format.lance_file_format(),
            ConcreteFileVersion::V1
        );

        let recovered_stable = Manifest::try_from(pb::Manifest {
            writer_feature_flags: FLAG_USE_V2_FORMAT_DEPRECATED,
            ..old_manifest
        })
        .unwrap();
        assert_eq!(
            recovered_stable.data_storage_format.lance_file_format(),
            ConcreteFileVersion::from(LanceFileVersion::Stable)
        );
    }

    #[test]
    fn manifest_persistence_rejects_selectors_and_public_aliases() {
        for version in ["stable", "next", "legacy", "0.3"] {
            let manifest = pb::Manifest {
                data_format: Some(pb::manifest::DataStorageFormat {
                    file_format: LANCE_FORMAT_NAME.to_string(),
                    version: version.to_string(),
                }),
                ..Default::default()
            };
            assert!(Manifest::try_from(manifest).is_err(), "accepted {version}");
        }
    }

    #[test]
    fn manifest_codec_writes_canonical_exact_string() {
        let manifest = Manifest::new(
            Schema::default(),
            Arc::new(Vec::new()),
            DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );
        let encoded = pb::Manifest::from(&manifest);
        assert_eq!(encoded.data_format.unwrap().version, "2.0");
    }

    #[test]
    fn missing_format_infers_exact_version_and_rejects_mixed_files() {
        let v2_0 = Fragment::new(0).with_file(
            "v2_0.lance",
            vec![0],
            vec![0],
            ConcreteFileVersion::V2_0,
            None,
        );
        let manifest = Manifest::new(
            Schema::default(),
            Arc::new(vec![v2_0.clone()]),
            DataStorageFormat::new(ConcreteFileVersion::V1),
            HashMap::new(),
        );
        let mut encoded = pb::Manifest::from(&manifest);
        encoded.data_format = None;
        let recovered = Manifest::try_from(encoded).unwrap();
        assert_eq!(
            recovered.data_storage_format.lance_file_format(),
            ConcreteFileVersion::V2_0
        );

        let v2_1 = Fragment::new(1).with_file(
            "v2_1.lance",
            vec![0],
            vec![0],
            ConcreteFileVersion::V2_1,
            None,
        );
        let mixed_manifest = Manifest::new(
            Schema::default(),
            Arc::new(vec![v2_0, v2_1]),
            DataStorageFormat::new(ConcreteFileVersion::V2_0),
            HashMap::new(),
        );
        let mut encoded = pb::Manifest::from(&mixed_manifest);
        encoded.data_format = None;
        let error = Manifest::try_from(encoded).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("All data files must have the same version")
        );
    }

    #[test]
    fn test_writer_version() {
        let wv = WriterVersion::default();
        assert_eq!(wv.library, "lance");

        // Parse the actual cargo version to check if it has a pre-release tag
        let cargo_version = env!("CARGO_PKG_VERSION");
        let expected_tag = if cargo_version.contains('-') {
            Some(cargo_version.split('-').nth(1).unwrap())
        } else {
            None
        };

        // Verify the version field contains only major.minor.patch
        let version_parts: Vec<&str> = wv.version.split('.').collect();
        assert_eq!(
            version_parts.len(),
            3,
            "Version should be major.minor.patch"
        );
        assert!(
            !wv.version.contains('-'),
            "Version field should not contain prerelease"
        );

        // Verify the prerelease field matches the expected tag
        assert_eq!(wv.prerelease.as_deref(), expected_tag);
        // Build metadata should be None for default version
        assert_eq!(wv.build_metadata, None);

        // Verify lance_lib_version() reconstructs the full semver correctly
        let version = wv.lance_lib_version().unwrap();
        assert_eq!(
            version.major,
            env!("CARGO_PKG_VERSION_MAJOR").parse::<u64>().unwrap()
        );
        assert_eq!(
            version.minor,
            env!("CARGO_PKG_VERSION_MINOR").parse::<u64>().unwrap()
        );
        assert_eq!(
            version.patch,
            // Unit tests run against (major,minor,patch + 1)
            env!("CARGO_PKG_VERSION_PATCH").parse::<u64>().unwrap() + 1
        );
        assert_eq!(version.pre.as_str(), expected_tag.unwrap_or(""));

        for part in &[VersionPart::Major, VersionPart::Minor, VersionPart::Patch] {
            let mut bumped_version = version.clone();
            bump_version(&mut bumped_version, *part);
            assert!(version < bumped_version);
        }
    }

    #[test]
    fn test_writer_version_split() {
        // Test splitting version with prerelease
        let (version, prerelease, build_metadata) =
            WriterVersion::split_version("2.0.0-rc.1").unwrap();
        assert_eq!(version, "2.0.0");
        assert_eq!(prerelease, Some("rc.1".to_string()));
        assert_eq!(build_metadata, None);

        // Test splitting version without prerelease
        let (version, prerelease, build_metadata) = WriterVersion::split_version("2.0.0").unwrap();
        assert_eq!(version, "2.0.0");
        assert_eq!(prerelease, None);
        assert_eq!(build_metadata, None);

        // Test splitting version with prerelease and build metadata
        let (version, prerelease, build_metadata) =
            WriterVersion::split_version("2.0.0-rc.1+build.123").unwrap();
        assert_eq!(version, "2.0.0");
        assert_eq!(prerelease, Some("rc.1".to_string()));
        assert_eq!(build_metadata, Some("build.123".to_string()));

        // Test splitting version with only build metadata
        let (version, prerelease, build_metadata) =
            WriterVersion::split_version("2.0.0+build.123").unwrap();
        assert_eq!(version, "2.0.0");
        assert_eq!(prerelease, None);
        assert_eq!(build_metadata, Some("build.123".to_string()));

        // Test with invalid version returns None
        assert!(WriterVersion::split_version("not-a-version").is_none());
    }

    #[test]
    fn test_writer_version_comparison_with_prerelease() {
        let v1 = WriterVersion {
            library: "lance".to_string(),
            version: "2.0.0".to_string(),
            prerelease: Some("rc.1".to_string()),
            build_metadata: None,
        };

        let v2 = WriterVersion {
            library: "lance".to_string(),
            version: "2.0.0".to_string(),
            prerelease: None,
            build_metadata: None,
        };

        let semver1 = v1.lance_lib_version().unwrap();
        let semver2 = v2.lance_lib_version().unwrap();

        // rc.1 should be less than the release version
        assert!(semver1 < semver2);
    }

    #[test]
    fn test_writer_version_with_build_metadata() {
        let v = WriterVersion {
            library: "lance".to_string(),
            version: "2.0.0".to_string(),
            prerelease: Some("rc.1".to_string()),
            build_metadata: Some("build.123".to_string()),
        };

        let semver = v.lance_lib_version().unwrap();
        assert_eq!(semver.to_string(), "2.0.0-rc.1+build.123");
        assert_eq!(semver.major, 2);
        assert_eq!(semver.minor, 0);
        assert_eq!(semver.patch, 0);
        assert_eq!(semver.pre.as_str(), "rc.1");
        assert_eq!(semver.build.as_str(), "build.123");
    }

    #[test]
    fn test_writer_version_non_semver() {
        // Test that Lance library can have non-semver version strings
        let v = WriterVersion {
            library: "lance".to_string(),
            version: "custom-build-v1".to_string(),
            prerelease: None,
            build_metadata: None,
        };

        // lance_lib_version should return None for non-semver
        assert!(v.lance_lib_version().is_none());

        // But the WriterVersion itself should still be valid and usable
        assert_eq!(v.library, "lance");
        assert_eq!(v.version, "custom-build-v1");
    }

    #[test]
    #[allow(deprecated)]
    fn test_older_than_with_prerelease() {
        // Test that older_than correctly handles prerelease
        let v_rc = WriterVersion {
            library: "lance".to_string(),
            version: "2.0.0".to_string(),
            prerelease: Some("rc.1".to_string()),
            build_metadata: None,
        };

        // 2.0.0-rc.1 should be older than 2.0.0
        assert!(v_rc.older_than(2, 0, 0));

        // 2.0.0-rc.1 should be older than 2.0.1
        assert!(v_rc.older_than(2, 0, 1));

        // 2.0.0-rc.1 should not be older than 1.9.9
        assert!(!v_rc.older_than(1, 9, 9));

        let v_release = WriterVersion {
            library: "lance".to_string(),
            version: "2.0.0".to_string(),
            prerelease: None,
            build_metadata: None,
        };

        // 2.0.0 should not be older than 2.0.0
        assert!(!v_release.older_than(2, 0, 0));

        // 2.0.0 should be older than 2.0.1
        assert!(v_release.older_than(2, 0, 1));
    }

    #[test]
    fn test_fragments_by_offset_range() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "a",
            arrow_schema::DataType::Int64,
            false,
        )]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        let fragments = vec![
            Fragment::with_file_legacy(0, "path1", &schema, Some(10)),
            Fragment::with_file_legacy(1, "path2", &schema, Some(15)),
            Fragment::with_file_legacy(2, "path3", &schema, Some(20)),
        ];
        let manifest = Manifest::new(
            schema,
            Arc::new(fragments),
            DataStorageFormat::default(),
            HashMap::new(),
        );

        let actual = manifest.fragments_by_offset_range(0..10);
        assert_eq!(actual.len(), 1);
        assert_eq!(actual[0].0, 0);
        assert_eq!(actual[0].1.id, 0);

        let actual = manifest.fragments_by_offset_range(5..15);
        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0].0, 0);
        assert_eq!(actual[0].1.id, 0);
        assert_eq!(actual[1].0, 10);
        assert_eq!(actual[1].1.id, 1);

        let actual = manifest.fragments_by_offset_range(15..50);
        assert_eq!(actual.len(), 2);
        assert_eq!(actual[0].0, 10);
        assert_eq!(actual[0].1.id, 1);
        assert_eq!(actual[1].0, 25);
        assert_eq!(actual[1].1.id, 2);

        // Out of range
        let actual = manifest.fragments_by_offset_range(45..100);
        assert!(actual.is_empty());

        assert!(manifest.fragments_by_offset_range(200..400).is_empty());
    }

    #[test]
    fn test_max_field_id() {
        // Validate that max field id handles varying field ids by fragment.
        let mut field0 =
            Field::try_from(ArrowField::new("a", arrow_schema::DataType::Int64, false)).unwrap();
        field0.set_id(-1, &mut 0);
        let mut field2 =
            Field::try_from(ArrowField::new("b", arrow_schema::DataType::Int64, false)).unwrap();
        field2.set_id(-1, &mut 2);

        let schema = Schema {
            fields: vec![field0, field2],
            metadata: Default::default(),
        };
        let fragments = vec![
            Fragment {
                id: 0,
                files: vec![DataFile::new_legacy_from_fields(
                    "path1",
                    vec![0, 1, 2],
                    None,
                )],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: None,
                physical_rows: None,
                created_at_version_meta: None,
                last_updated_at_version_meta: None,
            },
            Fragment {
                id: 1,
                files: vec![
                    DataFile::new_legacy_from_fields("path2", vec![0, 1, 43], None),
                    DataFile::new_legacy_from_fields("path3", vec![2], None),
                ],
                overlays: vec![],
                deletion_file: None,
                row_id_meta: None,
                physical_rows: None,
                created_at_version_meta: None,
                last_updated_at_version_meta: None,
            },
        ];

        let manifest = Manifest::new(
            schema,
            Arc::new(fragments),
            DataStorageFormat::default(),
            HashMap::new(),
        );

        assert_eq!(manifest.max_field_id(), 43);
    }

    #[test]
    fn test_config() {
        let arrow_schema = ArrowSchema::new(vec![ArrowField::new(
            "a",
            arrow_schema::DataType::Int64,
            false,
        )]);
        let schema = Schema::try_from(&arrow_schema).unwrap();
        let fragments = vec![
            Fragment::with_file_legacy(0, "path1", &schema, Some(10)),
            Fragment::with_file_legacy(1, "path2", &schema, Some(15)),
            Fragment::with_file_legacy(2, "path3", &schema, Some(20)),
        ];
        let mut manifest = Manifest::new(
            schema,
            Arc::new(fragments),
            DataStorageFormat::default(),
            HashMap::new(),
        );

        let mut config = manifest.config.clone();
        config.insert("lance.test".to_string(), "value".to_string());
        config.insert("other-key".to_string(), "other-value".to_string());

        manifest.config_mut().extend(config.clone());
        assert_eq!(manifest.config, config.clone());

        config.remove("other-key");
        manifest.config_mut().remove("other-key");
        assert_eq!(manifest.config, config);
    }

    #[test]
    fn test_manifest_summary() {
        // Step 1: test empty manifest summary
        let arrow_schema = ArrowSchema::new(vec![
            ArrowField::new("id", arrow_schema::DataType::Int64, false),
            ArrowField::new("name", arrow_schema::DataType::Utf8, true),
        ]);
        let schema = Schema::try_from(&arrow_schema).unwrap();

        let empty_manifest = Manifest::new(
            schema.clone(),
            Arc::new(vec![]),
            DataStorageFormat::default(),
            HashMap::new(),
        );

        let empty_summary = empty_manifest.summary();
        assert_eq!(empty_summary.total_rows, 0);
        assert_eq!(empty_summary.total_files_size, 0);
        assert_eq!(empty_summary.total_fragments, 0);
        assert_eq!(empty_summary.total_data_files, 0);
        assert_eq!(empty_summary.total_deletion_file_rows, 0);
        assert_eq!(empty_summary.total_data_file_rows, 0);
        assert_eq!(empty_summary.total_deletion_files, 0);

        // Step 2: write empty files and verify summary
        let empty_fragments = vec![
            Fragment::with_file_legacy(0, "empty_file1.lance", &schema, Some(0)),
            Fragment::with_file_legacy(1, "empty_file2.lance", &schema, Some(0)),
        ];

        let empty_files_manifest = Manifest::new(
            schema.clone(),
            Arc::new(empty_fragments),
            DataStorageFormat::default(),
            HashMap::new(),
        );

        let empty_files_summary = empty_files_manifest.summary();
        assert_eq!(empty_files_summary.total_rows, 0);
        assert_eq!(empty_files_summary.total_files_size, 0);
        assert_eq!(empty_files_summary.total_fragments, 2);
        assert_eq!(empty_files_summary.total_data_files, 2);
        assert_eq!(empty_files_summary.total_deletion_file_rows, 0);
        assert_eq!(empty_files_summary.total_data_file_rows, 0);
        assert_eq!(empty_files_summary.total_deletion_files, 0);

        // Step 3: write real data and verify summary
        let real_fragments = vec![
            Fragment::with_file_legacy(0, "data_file1.lance", &schema, Some(100)),
            Fragment::with_file_legacy(1, "data_file2.lance", &schema, Some(250)),
            Fragment::with_file_legacy(2, "data_file3.lance", &schema, Some(75)),
        ];

        let real_data_manifest = Manifest::new(
            schema.clone(),
            Arc::new(real_fragments),
            DataStorageFormat::default(),
            HashMap::new(),
        );

        let real_data_summary = real_data_manifest.summary();
        assert_eq!(real_data_summary.total_rows, 425); // 100 + 250 + 75
        assert_eq!(real_data_summary.total_files_size, 0); // Zero for unknown
        assert_eq!(real_data_summary.total_fragments, 3);
        assert_eq!(real_data_summary.total_data_files, 3);
        assert_eq!(real_data_summary.total_deletion_file_rows, 0);
        assert_eq!(real_data_summary.total_data_file_rows, 425);
        assert_eq!(real_data_summary.total_deletion_files, 0);

        // Step 4: write deletion files and verify summary
        let mut fragment_with_deletion = Fragment::new(0)
            .with_file(
                "data_with_deletion.lance",
                vec![0, 1],
                vec![0, 1],
                ConcreteFileVersion::from(LanceFileVersion::Stable),
                NonZero::new(1000),
            )
            .with_physical_rows(50);
        fragment_with_deletion.deletion_file = Some(DeletionFile {
            read_version: 123,
            id: 456,
            file_type: DeletionFileType::Array,
            num_deleted_rows: Some(10),
            base_id: None,
        });

        let manifest_with_deletion = Manifest::new(
            schema,
            Arc::new(vec![fragment_with_deletion]),
            DataStorageFormat::default(),
            HashMap::new(),
        );

        let deletion_summary = manifest_with_deletion.summary();
        assert_eq!(deletion_summary.total_rows, 40); // 50 - 10
        assert_eq!(deletion_summary.total_files_size, 1000);
        assert_eq!(deletion_summary.total_fragments, 1);
        assert_eq!(deletion_summary.total_data_files, 1);
        assert_eq!(deletion_summary.total_deletion_file_rows, 10);
        assert_eq!(deletion_summary.total_data_file_rows, 50);
        assert_eq!(deletion_summary.total_deletion_files, 1);

        //Just verify the transformation is OK
        let stats_map: BTreeMap<String, String> = deletion_summary.into();
        assert_eq!(stats_map.len(), 7)
    }
}
