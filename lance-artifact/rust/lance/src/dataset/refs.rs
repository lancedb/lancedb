// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::Range;

use chrono::{DateTime, Utc};
use futures::stream::{StreamExt, TryStreamExt};
use itertools::Itertools;
use lance_io::object_store::ObjectStore;
use lance_table::io::commit::CommitHandler;
use object_store::path::Path;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::dataset::branch_location::BranchLocation;
use crate::dataset::refs::Ref::{Tag, Version, VersionNumber};
use crate::utils::temporal::utc_now;
use crate::{Error, Result};
use serde::de::DeserializeOwned;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::fmt::Formatter;
use uuid::Uuid;

pub const MAIN_BRANCH: &str = "main";

/// Lance Ref
#[derive(Debug, Clone)]
pub enum Ref {
    // Version number points of the current branch
    VersionNumber(u64),
    // This is a global version identifier present as (branch_name, version_number)
    // if branch_name is None, it points to the main branch
    // if version_number is None, it points to the latest version
    Version(Option<String>, Option<u64>),
    // Tag name points to the global version identifier, could be considered as an alias of specific global version
    Tag(String),
}

impl From<u64> for Ref {
    fn from(reference: u64) -> Self {
        VersionNumber(reference)
    }
}

impl From<&str> for Ref {
    fn from(reference: &str) -> Self {
        Tag(reference.to_string())
    }
}

impl From<(&str, u64)> for Ref {
    fn from(reference: (&str, u64)) -> Self {
        Version(standardize_branch(reference.0), Some(reference.1))
    }
}

impl From<(Option<&str>, Option<u64>)> for Ref {
    fn from(reference: (Option<&str>, Option<u64>)) -> Self {
        Version(reference.0.and_then(standardize_branch), reference.1)
    }
}

impl From<(&str, Option<u64>)> for Ref {
    fn from(reference: (&str, Option<u64>)) -> Self {
        Version(standardize_branch(reference.0), reference.1)
    }
}

impl fmt::Display for Ref {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Version(branch, version_number) => {
                let version_str = version_number
                    .map(|v| v.to_string())
                    .unwrap_or_else(|| "latest".to_string());
                write!(f, "{}:{}", normalize_branch(branch.as_deref()), version_str)
            }
            VersionNumber(version_number) => write!(f, "{}", version_number),
            Tag(tag_name) => write!(f, "{}", tag_name),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Refs {
    pub(crate) object_store: Arc<ObjectStore>,
    pub(crate) commit_handler: Arc<dyn CommitHandler>,
    pub(crate) base_location: BranchLocation,
}

impl Refs {
    pub fn new(
        object_store: Arc<ObjectStore>,
        commit_handler: Arc<dyn CommitHandler>,
        base_location: BranchLocation,
    ) -> Self {
        Self {
            object_store,
            commit_handler,
            base_location,
        }
    }

    pub fn tags(&self) -> Tags<'_> {
        Tags { refs: self }
    }

    pub fn branches(&self) -> Branches<'_> {
        Branches { refs: self }
    }

    pub fn base(&self) -> &Path {
        &self.base_location.path
    }

    pub fn root(&self) -> Result<BranchLocation> {
        self.base_location.find_main()
    }
}

/// Tags operation
#[derive(Debug, Clone)]
pub struct Tags<'a> {
    refs: &'a Refs,
}

/// Branches operation
#[derive(Debug, Clone)]
pub struct Branches<'a> {
    refs: &'a Refs,
}

impl Tags<'_> {
    fn object_store(&self) -> &ObjectStore {
        &self.refs.object_store
    }
}

impl Branches<'_> {
    fn object_store(&self) -> &ObjectStore {
        &self.refs.object_store
    }
}

impl Tags<'_> {
    pub async fn fetch_tags(&self) -> Result<Vec<(String, TagContents)>> {
        let root_location = self.refs.root()?;
        let base_path = base_tags_path(&root_location.path);
        let tag_files = self.object_store().read_dir(base_path).await?;

        let tag_names: Vec<String> = tag_files
            .iter()
            .filter_map(|name| name.strip_suffix(".json"))
            .map(|name| name.to_string())
            .collect_vec();

        let root_path = &root_location.path;
        futures::stream::iter(tag_names)
            .map(|tag_name| async move {
                let contents =
                    TagContents::from_path(&tag_path(root_path, &tag_name), self.object_store())
                        .await?;
                Ok((tag_name, contents))
            })
            .buffer_unordered(10)
            .try_collect()
            .await
    }

    pub async fn list(&self) -> Result<HashMap<String, TagContents>> {
        self.fetch_tags()
            .await
            .map(|tags| tags.into_iter().collect())
    }

    pub async fn list_tags_ordered(
        &self,
        order: Option<Ordering>,
    ) -> Result<Vec<(String, TagContents)>> {
        let mut tags = self.fetch_tags().await?;
        tags.sort_by(|a, b| {
            let desired_ordering = order.unwrap_or(Ordering::Greater);
            let version_ordering = a.1.version.cmp(&b.1.version);
            let version_result = match desired_ordering {
                Ordering::Less => version_ordering,
                _ => version_ordering.reverse(),
            };
            version_result.then_with(|| a.0.cmp(&b.0))
        });
        Ok(tags)
    }

    pub async fn get_version(&self, tag: &str) -> Result<u64> {
        self.get(tag).await.map(|tag| tag.version)
    }

    pub async fn get(&self, tag: &str) -> Result<TagContents> {
        check_valid_tag(tag)?;

        let root_location = self.refs.root()?;
        let tag_file = tag_path(&root_location.path, tag);

        if !self.object_store().exists(&tag_file).await? {
            return Err(Error::RefNotFound {
                message: format!("tag {} does not exist", tag),
            });
        }

        let tag_contents = TagContents::from_path(&tag_file, self.object_store()).await?;
        Ok(tag_contents)
    }

    pub async fn create(&self, tag: &str, reference: impl Into<Ref>) -> Result<()> {
        check_valid_tag(tag)?;
        let root_location = self.refs.root()?;
        let tag_file = tag_path(&root_location.path, tag);

        if self.object_store().exists(&tag_file).await? {
            return Err(Error::RefConflict {
                message: format!("tag {} already exists", tag),
            });
        }
        let now = utc_now();
        let tag_contents = self
            .build_tag_content_by_ref(reference, Some(now), Some(now))
            .await?;

        self.object_store()
            .put(
                &tag_file,
                serde_json::to_string_pretty(&tag_contents)?.as_bytes(),
            )
            .await
            .map(|_| ())
    }

    pub async fn delete(&self, tag: &str) -> Result<()> {
        check_valid_tag(tag)?;

        let root_location = self.refs.root()?;
        let tag_file = tag_path(&root_location.path, tag);

        if !self.object_store().exists(&tag_file).await? {
            return Err(Error::RefNotFound {
                message: format!("tag {} does not exist", tag),
            });
        }

        self.object_store().delete(&tag_file).await
    }

    pub async fn update(&self, tag: &str, reference: impl Into<Ref>) -> Result<()> {
        check_valid_tag(tag)?;

        let root_location = self.refs.root()?;
        let tag_file = tag_path(&root_location.path, tag);
        if !self.object_store().exists(&tag_file).await? {
            return Err(Error::RefNotFound {
                message: format!("tag {} does not exist", tag),
            });
        }
        let mut tag_contents = TagContents::from_path(&tag_file, self.object_store()).await?;
        let updated_reference = self
            .build_tag_content_by_ref(reference, tag_contents.created_at, Some(utc_now()))
            .await?;
        tag_contents.branch = updated_reference.branch;
        tag_contents.version = updated_reference.version;
        tag_contents.created_at = updated_reference.created_at;
        tag_contents.updated_at = updated_reference.updated_at;
        tag_contents.manifest_size = updated_reference.manifest_size;

        self.object_store()
            .put(
                &tag_file,
                serde_json::to_string_pretty(&tag_contents)?.as_bytes(),
            )
            .await
            .map(|_| ())
    }

    pub async fn replace_metadata(
        &self,
        tag: &str,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        check_valid_tag(tag)?;

        let root_location = self.refs.root()?;
        let tag_file = tag_path(&root_location.path, tag);
        if !self.object_store().exists(&tag_file).await? {
            return Err(Error::RefNotFound {
                message: format!("tag {} does not exist", tag),
            });
        }

        let mut tag_contents = TagContents::from_path(&tag_file, self.object_store()).await?;
        tag_contents.metadata = metadata;

        self.object_store()
            .put(
                &tag_file,
                serde_json::to_string_pretty(&tag_contents)?.as_bytes(),
            )
            .await
            .map(|_| ())
    }

    async fn build_tag_content_by_ref(
        &self,
        reference: impl Into<Ref>,
        created_at: Option<DateTime<Utc>>,
        updated_at: Option<DateTime<Utc>>,
    ) -> Result<TagContents> {
        let reference = reference.into();
        let (branch, version_number) = match reference {
            Version(branch, version_number) => (branch, version_number),
            VersionNumber(version_number) => {
                (self.refs.base_location.branch.clone(), Some(version_number))
            }
            Tag(tag_name) => {
                let tag_content = self.get(tag_name.as_str()).await?;
                (tag_content.branch, Some(tag_content.version))
            }
        };

        let branch_location = self.refs.base_location.find_branch(branch.as_deref())?;
        let manifest_file = if let Some(version_number) = version_number {
            self.refs
                .commit_handler
                .resolve_version_location(
                    &branch_location.path,
                    version_number,
                    &self.refs.object_store.inner,
                )
                .await?
        } else {
            self.refs
                .commit_handler
                .resolve_latest_location(&branch_location.path, &self.refs.object_store)
                .await?
        };

        if !self.object_store().exists(&manifest_file.path).await? {
            return Err(Error::VersionNotFound {
                message: format!("version {} does not exist", Version(branch, version_number)),
            });
        }

        let manifest_size = if let Some(size) = manifest_file.size {
            size as usize
        } else {
            self.object_store().size(&manifest_file.path).await? as usize
        };

        let tag_contents = TagContents {
            branch,
            version: manifest_file.version,
            created_at,
            updated_at,
            manifest_size,
            metadata: HashMap::new(),
        };
        Ok(tag_contents)
    }
}

impl Branches<'_> {
    pub(crate) fn is_main_branch(branch: Option<&str>) -> bool {
        branch == Some(MAIN_BRANCH)
    }

    pub async fn fetch(&self) -> Result<Vec<(String, BranchContents)>> {
        let root_location = self.refs.root()?;
        let base_path = base_branches_contents_path(&root_location.path);
        let branch_files = self.object_store().read_dir(base_path).await?;

        let branch_names: Vec<String> = branch_files
            .iter()
            .filter_map(|name| name.strip_suffix(".json"))
            .map(|str| {
                Path::from_url_path(str)
                    .map_err(|e| Error::InvalidRef {
                        message: format!(
                            "Failed to decode branch name: {} due to exception {}",
                            str, e
                        ),
                    })
                    .map(|path| path.to_string())
            })
            .collect::<Result<Vec<_>>>()?;

        let branch_path = &root_location.path;
        futures::stream::iter(branch_names)
            .map(|name| async move {
                let contents = BranchContents::from_path(
                    &branch_contents_path(branch_path, &name),
                    self.object_store(),
                    &name,
                )
                .await?;
                Ok((name, contents))
            })
            .buffer_unordered(10)
            .try_collect()
            .await
    }

    pub async fn list(&self) -> Result<HashMap<String, BranchContents>> {
        self.fetch()
            .await
            .map(|branches| branches.into_iter().collect())
    }

    pub async fn get(&self, branch: &str) -> Result<BranchContents> {
        check_valid_branch(branch)?;

        let root_location = self.refs.root()?;
        let branch_file = branch_contents_path(&root_location.path, branch);

        if !self.object_store().exists(&branch_file).await? {
            return Err(Error::RefNotFound {
                message: format!("branch {} does not exist", branch),
            });
        }

        let branch_contents =
            BranchContents::from_path(&branch_file, self.object_store(), branch).await?;

        Ok(branch_contents)
    }

    pub async fn get_identifier(&self, branch: Option<&str>) -> Result<BranchIdentifier> {
        if let Some(branch_name) = branch {
            let branch_contents = self.get(branch_name).await?;
            Ok(branch_contents.identifier)
        } else {
            Ok(BranchIdentifier::main())
        }
    }

    // Only create branch metadata
    pub(crate) async fn create(
        &self,
        branch_name: &str,
        version_number: u64,
        source_branch: Option<&str>,
    ) -> Result<()> {
        check_valid_branch(branch_name)?;

        let source_branch = source_branch.and_then(standardize_branch);
        let root_location = self.refs.root()?;
        let branch_file = branch_contents_path(&root_location.path, branch_name);
        if self.object_store().exists(&branch_file).await? {
            return Err(Error::RefConflict {
                message: format!("branch {} already exists", branch_name),
            });
        }

        let branch_location = self
            .refs
            .base_location
            .find_branch(source_branch.as_deref())?;
        // Verify the source version exists
        let manifest_file = self
            .refs
            .commit_handler
            .resolve_version_location(
                &branch_location.path,
                version_number,
                &self.refs.object_store.inner,
            )
            .await?;

        if !self.object_store().exists(&manifest_file.path).await? {
            return Err(Error::VersionNotFound {
                message: format!("Manifest file {} does not exist", manifest_file.path),
            });
        };

        let parent_branch_id = if let Some(ref parent_branch) = source_branch {
            let parent_file = branch_contents_path(&root_location.path, parent_branch);
            if self.object_store().exists(&parent_file).await? {
                BranchContents::from_path(&parent_file, self.object_store(), parent_branch)
                    .await?
                    .identifier
            } else {
                return Err(Error::RefNotFound {
                    message: format!("Parent branch {} does not exist", branch_name),
                });
            }
        } else {
            BranchIdentifier::main()
        };

        let branch_contents = BranchContents {
            parent_branch: source_branch,
            identifier: BranchIdentifier::new(&parent_branch_id, version_number),
            parent_version: version_number,
            create_at: chrono::Utc::now().timestamp() as u64,
            manifest_size: if let Some(size) = manifest_file.size {
                size as usize
            } else {
                self.object_store().size(&manifest_file.path).await? as usize
            },
            metadata: HashMap::new(),
        };

        self.object_store()
            .put(
                &branch_file,
                serde_json::to_string_pretty(&branch_contents)?.as_bytes(),
            )
            .await
            .map(|_| ())
    }

    pub async fn replace_metadata(
        &self,
        branch: &str,
        metadata: HashMap<String, String>,
    ) -> Result<()> {
        check_valid_branch(branch)?;

        let root_location = self.refs.root()?;
        let branch_file = branch_contents_path(&root_location.path, branch);
        if !self.object_store().exists(&branch_file).await? {
            return Err(Error::RefNotFound {
                message: format!("branch {} does not exist", branch),
            });
        }

        let mut branch_contents =
            BranchContents::from_path(&branch_file, self.object_store(), branch).await?;
        branch_contents.metadata = metadata;

        self.object_store()
            .put(
                &branch_file,
                serde_json::to_string_pretty(&branch_contents)?.as_bytes(),
            )
            .await
            .map(|_| ())
    }

    /// Delete a branch
    ///
    /// If the `BranchContents` does not exist, it will return an error directly unless `force` is true.
    /// If `force` is true, it will try to delete the branch directories no matter `BranchContents` exists or not.
    pub async fn delete(&self, branch: &str, force: bool) -> Result<()> {
        check_valid_branch(branch)?;

        let all_branches = self.list().await?;
        let branch_id = all_branches
            .get(branch)
            .map(|contents| contents.identifier.clone());
        if let Some(branch_id) = branch_id {
            let referenced_versions = branch_id.collect_referenced_versions(&all_branches);
            if !referenced_versions.is_empty() {
                return Err(Error::RefConflict {
                    message: format!(
                        "Branch {} is referenced by {:?} versions, can not delete",
                        branch, referenced_versions
                    ),
                });
            }
        } else if !force {
            return Err(Error::RefNotFound {
                message: format!("Branch {} does not exist", branch),
            });
        } else {
            log::warn!("BranchContents of {} does not exist", branch);
        }

        let root_location = self.refs.root()?;
        let branch_file = branch_contents_path(&root_location.path, branch);
        if self.object_store().exists(&branch_file).await? {
            self.object_store().delete(&branch_file).await?;
        }

        // Clean up branch directories
        self.cleanup_branch_directories(branch).await
    }

    pub async fn list_ordered(
        &self,
        order: Option<Ordering>,
    ) -> Result<Vec<(String, BranchContents)>> {
        let mut branches = self.fetch().await?;
        branches.sort_by(|a, b| {
            let desired_ordering = order.unwrap_or(Ordering::Greater);
            let version_ordering = a.1.parent_version.cmp(&b.1.parent_version);
            let version_result = match desired_ordering {
                Ordering::Less => version_ordering,
                _ => version_ordering.reverse(),
            };
            version_result.then_with(|| a.0.cmp(&b.0))
        });
        Ok(branches)
    }

    /// Clean up empty parent directories
    async fn cleanup_branch_directories(&self, branch: &str) -> Result<()> {
        let branches = self.list().await?;
        let remaining_branches: Vec<&str> = branches.keys().map(|k| k.as_str()).collect();

        if let Some(delete_path) =
            Self::get_cleanup_path(branch, &remaining_branches, &self.refs.base_location)?
            && let Err(e) = self.refs.object_store.remove_dir_all(delete_path).await
        {
            match &e {
                Error::NotFound { .. } => {
                    log::debug!("Branch directory already deleted");
                }
                _ => return Err(e),
            }
        }
        Ok(())
    }

    fn get_cleanup_path(
        branch: &str,
        remaining_branches: &[&str],
        base_location: &BranchLocation,
    ) -> Result<Option<Path>> {
        let deleted_branch = BranchRelativePath::new(branch);
        let mut related_branches = Vec::new();
        let mut relative_dir = branch.to_string();
        for branch in remaining_branches {
            let branch = BranchRelativePath::new(branch);
            if branch.is_parent(&deleted_branch) || branch.is_child(&deleted_branch) {
                related_branches.push(branch);
            } else if let Some(common_prefix) = deleted_branch.find_common_prefix(&branch) {
                related_branches.push(common_prefix);
            }
        }

        related_branches.sort_by(|a, b| a.segments.len().cmp(&b.segments.len()).reverse());
        if let Some(branch) = related_branches.first() {
            if branch.is_child(&deleted_branch) || branch == &deleted_branch {
                // There are children of the deleted branch, we can't delete any directory for now
                // Example: deleted_branch = "a/b/c", remaining_branches = ["a/b/c/d"], we need to delete nothing
                return Ok(None);
            } else {
                // We pick the longest common directory between the deleted branch and the remaining branches
                // Then delete the first child of this common directory
                // Example: deleted_branch = "a/b/c", remaining_branches = ["a"], we need to delete "a/b"
                relative_dir = format!(
                    "{}/{}",
                    branch.segments.join("/"),
                    deleted_branch.segments[branch.segments.len()]
                );
            }
        } else if !deleted_branch.segments.is_empty() {
            // There are no common directories between the deleted branch and the remaining branches
            // We need to delete the entire directory
            // Example: deleted_branch = "a/b/c", remaining_branches = [], we need to delete "a"
            relative_dir = deleted_branch.segments[0].to_string();
        }

        let absolute_dir = base_location.find_branch(Some(relative_dir.as_str()))?;
        Ok(Some(absolute_dir.path))
    }
}

#[derive(Debug, PartialEq)]
struct BranchRelativePath<'a> {
    segments: Vec<&'a str>,
}

impl<'a> BranchRelativePath<'a> {
    fn new(branch_name: &'a str) -> Self {
        let segments = branch_name.split('/').collect_vec();
        Self { segments }
    }

    fn find_common_prefix(&self, other: &Self) -> Option<Self> {
        let mut common_segments = Vec::new();
        for (i, segment) in self.segments.iter().enumerate() {
            if i >= other.segments.len() || other.segments[i] != *segment {
                break;
            }
            common_segments.push(*segment);
        }
        if !common_segments.is_empty() {
            Some(BranchRelativePath {
                segments: common_segments,
            })
        } else {
            None
        }
    }

    fn is_parent(&self, other: &Self) -> bool {
        if other.segments.len() <= self.segments.len() {
            false
        } else {
            for (i, segment) in self.segments.iter().enumerate() {
                if other.segments[i] != *segment {
                    return false;
                }
            }
            true
        }
    }

    fn is_child(&self, other: &Self) -> bool {
        if other.segments.len() >= self.segments.len() {
            false
        } else {
            for (i, segment) in other.segments.iter().enumerate() {
                if self.segments[i] != *segment {
                    return false;
                }
            }
            true
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagContents {
    pub branch: Option<String>,
    pub version: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub updated_at: Option<DateTime<Utc>>,
    pub manifest_size: usize,
    /// Metadata associated with this tag.
    ///
    /// Missing metadata is deserialized as an empty map.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BranchContents {
    pub parent_branch: Option<String>,
    #[serde(default = "BranchIdentifier::missing_identifier_sentinel")]
    pub identifier: BranchIdentifier,
    pub parent_version: u64,
    pub create_at: u64, // unix timestamp
    pub manifest_size: usize,
    /// Metadata associated with this branch.
    ///
    /// Missing metadata is deserialized as an empty map.
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct BranchIdentifier {
    pub version_mapping: Vec<(u64, String)>,
}

impl BranchIdentifier {
    pub fn new(parent: &Self, parent_version: u64) -> Self {
        let mut version_mapping = parent.version_mapping.clone();
        version_mapping.push((parent_version, Uuid::new_v4().simple().to_string()));
        Self { version_mapping }
    }

    /// Creates a sentinel identifier for legacy branch metadata that lacks an explicit
    /// identifier.
    ///
    /// `BranchContents::from_path` replaces this value with a deterministic synthetic
    /// identifier. Keeping this sentinel stable lets us distinguish missing identifiers from
    /// persisted identifiers without changing this field to `Option<BranchIdentifier>`.
    pub fn missing_identifier_sentinel() -> Self {
        Self {
            version_mapping: vec![(0, Uuid::nil().simple().to_string())],
        }
    }

    fn synthetic_identifier(
        branch_name: &str,
        parent_branch: Option<&str>,
        parent_version: u64,
        create_at: u64,
    ) -> Self {
        let identifier_input = format!(
            "branch_name={branch_name}\nparent_branch={}\nparent_version={parent_version}\ncreate_at={create_at}",
            parent_branch.unwrap_or("")
        );
        Self {
            version_mapping: vec![(
                0,
                Uuid::from_bytes(Self::synthetic_identifier_bytes(
                    identifier_input.as_bytes(),
                ))
                .simple()
                .to_string(),
            )],
        }
    }

    fn synthetic_identifier_bytes(input: &[u8]) -> [u8; 16] {
        // Use fixed, local hashing so legacy fallback identifiers stay deterministic without
        // enabling extra UUID generation features.
        const FNV_OFFSET: u64 = 0xcbf29ce484222325;
        const FNV_PRIME: u64 = 0x100000001b3;

        fn hash_with_seed(input: &[u8], seed: u64) -> u64 {
            input.iter().fold(seed, |hash, byte| {
                (hash ^ u64::from(*byte)).wrapping_mul(FNV_PRIME)
            })
        }

        let first = hash_with_seed(input, FNV_OFFSET);
        let second = hash_with_seed(input, FNV_OFFSET ^ 0x9e3779b97f4a7c15);
        let mut bytes = [0; 16];
        bytes[..8].copy_from_slice(&first.to_be_bytes());
        bytes[8..].copy_from_slice(&second.to_be_bytes());
        bytes
    }

    pub fn main() -> Self {
        Self {
            version_mapping: vec![],
        }
    }

    pub fn parse(identifier: &str) -> Result<Self> {
        let parts: Vec<&str> = identifier.split(':').collect();
        if !parts.len().is_multiple_of(2) {
            return Err(Error::InvalidRef {
                message: format!(
                    "Invalid branch identifier: {}, format should be 'ver1:uuid1:ver2:uuid2:...:final_uuid'",
                    parts.len()
                ),
            });
        }

        let version_mapping = parts
            .chunks_exact(2)
            .map(|chunk| {
                let version = chunk[0].parse::<u64>().map_err(|e| Error::InvalidRef {
                    message: format!("Invalid version number '{}': {}", chunk[0], e),
                })?;
                let uuid = chunk[1].to_string();
                Ok((version, uuid))
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Self { version_mapping })
    }

    pub fn find_referenced_version(&self, referenced_branch: &Self) -> Option<u64> {
        let ref_mapping = &referenced_branch.version_mapping;
        let next_idx = ref_mapping.len();

        (self.version_mapping.len() > next_idx && self.version_mapping[..next_idx] == *ref_mapping)
            .then(|| self.version_mapping[next_idx].0)
            .filter(|&version| version > 0)
    }

    /// Collects all branches that reference this branch, returning (branch_name, version) tuples.
    /// Results are in post-order traversal (deepest branches first).
    pub fn collect_referenced_versions(
        &self,
        branches: &HashMap<String, BranchContents>,
    ) -> Vec<(String, u64)> {
        let mut branch_ids = branches
            .iter()
            .map(|(name, branch)| (branch.identifier.clone(), name.clone()))
            .collect::<Vec<_>>();
        // Sort by BranchIdentifier desc to implement post-order traversal.
        branch_ids.sort_by(|a, b| b.cmp(a));
        branch_ids
            .into_iter()
            .filter_map(|(branch_id, name)| {
                branch_id
                    .find_referenced_version(self)
                    .map(|version| (name, version))
            })
            .collect()
    }
}

pub fn base_tags_path(base_path: &Path) -> Path {
    base_path.clone().join("_refs").join("tags")
}

pub fn base_branches_contents_path(base_path: &Path) -> Path {
    base_path.clone().join("_refs").join("branches")
}

pub fn tag_path(base_path: &Path, branch: &str) -> Path {
    base_tags_path(base_path).join(format!("{}.json", branch))
}

// Note: child will encode '/' to '%2F'
pub fn branch_contents_path(base_path: &Path, branch: &str) -> Path {
    base_branches_contents_path(base_path).join(format!("{}.json", branch))
}

pub(crate) fn normalize_branch(branch: Option<&str>) -> String {
    match branch {
        None => MAIN_BRANCH.to_string(),
        Some(name) => name.to_string(),
    }
}

pub(crate) fn standardize_branch(branch: &str) -> Option<String> {
    match branch {
        MAIN_BRANCH => None,
        name => Some(name.to_string()),
    }
}

async fn from_path<T>(path: &Path, object_store: &ObjectStore) -> Result<T>
where
    T: DeserializeOwned,
{
    let tag_reader = object_store.open(path).await?;
    let tag_bytes = tag_reader
        .get_range(Range {
            start: 0,
            end: tag_reader.size().await?,
        })
        .await?;
    let json_str = String::from_utf8(tag_bytes.to_vec())
        .map_err(|e| Error::corrupt_file(path.clone(), e.to_string()))?;
    Ok(serde_json::from_str(&json_str)?)
}

impl TagContents {
    pub async fn from_path(path: &Path, object_store: &ObjectStore) -> Result<Self> {
        from_path(path, object_store).await
    }
}

impl BranchContents {
    pub async fn from_path(
        path: &Path,
        object_store: &ObjectStore,
        branch_name: &str,
    ) -> Result<Self> {
        let mut contents: Self = from_path(path, object_store).await?;
        if contents.identifier == BranchIdentifier::missing_identifier_sentinel() {
            // Legacy branch files do not store an identifier. Derive a deterministic fallback
            // from stable branch metadata so repeated reads expose the same public
            // branch_identifier.
            contents.identifier = BranchIdentifier::synthetic_identifier(
                branch_name,
                contents.parent_branch.as_deref(),
                contents.parent_version,
                contents.create_at,
            );
        }
        Ok(contents)
    }
}

pub fn check_valid_branch(branch_name: &str) -> Result<()> {
    if branch_name.is_empty() {
        return Err(Error::InvalidRef {
            message: "Branch name cannot be empty".to_string(),
        });
    }

    // Validate if the branch name starts or ends with a '/'
    if branch_name.starts_with('/') || branch_name.ends_with('/') {
        return Err(Error::InvalidRef {
            message: "Branch name cannot start or end with a '/'".to_string(),
        });
    }

    // Validate if there are any consecutive '/' in the branch name
    if branch_name.contains("//") {
        return Err(Error::InvalidRef {
            message: "Branch name cannot contain consecutive '/'".to_string(),
        });
    }

    // Validate if there are any dangerous characters in the branch name
    if branch_name.contains("..") || branch_name.contains('\\') {
        return Err(Error::InvalidRef {
            message: "Branch name cannot contain '..' or '\\'".to_string(),
        });
    }

    for segment in branch_name.split('/') {
        if segment.is_empty() {
            return Err(Error::InvalidRef {
                message: "Branch name cannot have empty segments between '/'".to_string(),
            });
        }
        if !segment
            .chars()
            .all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
        {
            return Err(Error::InvalidRef {
                message: format!(
                    "Branch segment '{}' contains invalid characters. Only alphanumeric, '.', '-', '_' are allowed.",
                    segment
                ),
            });
        }
    }

    if branch_name.ends_with(".lock") {
        return Err(Error::InvalidRef {
            message: "Branch name cannot end with '.lock'".to_string(),
        });
    }

    if branch_name.eq("main") {
        return Err(Error::InvalidRef {
            message: "Branch name cannot be 'main'".to_string(),
        });
    }
    Ok(())
}

pub fn check_valid_tag(s: &str) -> Result<()> {
    if s.is_empty() {
        return Err(Error::InvalidRef {
            message: "Ref cannot be empty".to_string(),
        });
    }

    if !s
        .chars()
        .all(|c| c.is_alphanumeric() || c == '.' || c == '-' || c == '_')
    {
        return Err(Error::InvalidRef {
            message: "Ref characters must be either alphanumeric, '.', '-' or '_'".to_string(),
        });
    }

    if s.starts_with('.') {
        return Err(Error::InvalidRef {
            message: "Ref cannot begin with a dot".to_string(),
        });
    }

    if s.ends_with('.') {
        return Err(Error::InvalidRef {
            message: "Ref cannot end with a dot".to_string(),
        });
    }

    if s.ends_with(".lock") {
        return Err(Error::InvalidRef {
            message: "Ref cannot end with .lock".to_string(),
        });
    }

    if s.contains("..") {
        return Err(Error::InvalidRef {
            message: "Ref cannot have two consecutive dots".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::assert_contains;

    use rstest::rstest;

    #[rstest]
    fn test_ok_ref(
        #[values(
            "ref",
            "ref-with-dashes",
            "ref.extension",
            "ref_with_underscores",
            "v1.2.3-rc4"
        )]
        r: &str,
    ) {
        check_valid_tag(r).unwrap();
    }

    #[rstest]
    fn test_err_ref(
        #[values(
            "",
            "../ref",
            ".ref",
            "/ref",
            "@",
            "deeply/nested/ref",
            "nested//ref",
            "nested/ref",
            "nested\\ref",
            "ref*",
            "ref.lock",
            "ref/",
            "ref?",
            "ref@{ref",
            "ref[",
            "ref^",
            "~/ref",
            "ref.",
            "ref..ref"
        )]
        r: &str,
    ) {
        assert_contains!(
            check_valid_tag(r).err().unwrap().to_string(),
            "Ref is invalid: Ref"
        );
    }

    #[rstest]
    fn test_valid_branch_names(
        #[values(
            "feature/login",
            "bugfix/issue-123",
            "release/v1.2.3",
            "user/someone/my-feature",
            "normal",
            "with-dash",
            "with_underscore",
            "with.dot"
        )]
        branch_name: &str,
    ) {
        assert!(
            check_valid_branch(branch_name).is_ok(),
            "Branch name '{}' should be valid",
            branch_name
        );
    }

    #[rstest]
    fn test_invalid_branch_names(
        #[values(
            "",
            "/start-with-slash",
            "end-with-slash/",
            "have//consecutive-slash",
            "have..dot-dot",
            "have\\backslash",
            "segment/",
            "/segment",
            "segment//empty",
            "name.lock",
            "bad@character",
            "bad segment"
        )]
        branch_name: &str,
    ) {
        assert!(
            check_valid_branch(branch_name).is_err(),
            "Branch name '{}' should be invalid",
            branch_name
        );
    }

    #[test]
    fn test_path_functions() {
        let base_path = Path::from("dataset");

        // Test base_tags_path
        let tags_path = base_tags_path(&base_path);
        assert_eq!(tags_path, Path::from("dataset/_refs/tags"));

        // Test base_branches_path
        let branches_path = base_branches_contents_path(&base_path);
        assert_eq!(branches_path, Path::from("dataset/_refs/branches"));

        // Test tag_path
        let tag_file_path = tag_path(&base_path, "v1.0.0");
        assert_eq!(tag_file_path, Path::from("dataset/_refs/tags/v1.0.0.json"));

        // Test branch_path
        let branch_file_path = branch_contents_path(&base_path, "feature");
        assert_eq!(
            branch_file_path,
            Path::from("dataset/_refs/branches/feature.json")
        );
    }

    #[tokio::test]
    async fn test_refs_from_traits() {
        // Test From<u64> for Ref
        let version_ref: Ref = 42u64.into();
        match version_ref {
            VersionNumber(version_number) => {
                assert_eq!(version_number, 42);
            }
            _ => panic!("Expected Version variant"),
        }

        // Test From<&str> for Ref
        let tag_ref: Ref = "test_tag".into();
        match tag_ref {
            Tag(name) => assert_eq!(name, "test_tag"),
            _ => panic!("Expected Tag variant"),
        }

        // Test From<(&str, u64)> for Ref
        let branch_ref: Ref = ("test_branch", 10u64).into();
        match branch_ref {
            Version(name, version) => {
                assert_eq!(name.unwrap(), "test_branch");
                assert_eq!(version, Some(10));
            }
            _ => panic!("Expected Branch variant"),
        }
    }

    #[tokio::test]
    async fn test_branch_contents_serialization() {
        let branch_contents = BranchContents {
            parent_branch: Some("main".to_string()),
            identifier: BranchIdentifier::missing_identifier_sentinel(),
            parent_version: 42,
            create_at: 1234567890,
            manifest_size: 1024,
            metadata: HashMap::from([("description".to_string(), "production branch".to_string())]),
        };

        // Test serialization
        let json = serde_json::to_string(&branch_contents).unwrap();
        assert!(json.contains("parentBranch"));
        assert!(json.contains("parentVersion"));
        assert!(json.contains("createAt"));
        assert!(json.contains("manifestSize"));
        assert!(json.contains("metadata"));

        // Test deserialization
        let deserialized: BranchContents = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.parent_branch, branch_contents.parent_branch);
        assert_eq!(deserialized.parent_version, branch_contents.parent_version);
        assert_eq!(deserialized.create_at, branch_contents.create_at);
        assert_eq!(deserialized.manifest_size, branch_contents.manifest_size);
        assert_eq!(deserialized.metadata, branch_contents.metadata);

        // Backward compatibility: older serialized content does not include metadata.
        let legacy_json = r#"{"parentBranch":"main","parentVersion":42,"createAt":1234567890,"manifestSize":1024}"#;
        let legacy_deserialized: BranchContents = serde_json::from_str(legacy_json).unwrap();
        assert!(legacy_deserialized.metadata.is_empty());
    }

    #[tokio::test]
    async fn test_branch_synthetic_uuid_is_stable() {
        let legacy_json = r#"{"parentBranch":"main","parentVersion":42,"createAt":1234567890,"manifestSize":1024}"#;
        let store = ObjectStore::memory();
        let base_path = Path::from("dataset");
        let first_path = branch_contents_path(&base_path, "legacy_branch");
        store
            .put(&first_path, legacy_json.as_bytes())
            .await
            .unwrap();
        let second_path = branch_contents_path(&base_path, "legacy_branch_other");
        store
            .put(&second_path, legacy_json.as_bytes())
            .await
            .unwrap();

        let first = BranchContents::from_path(&first_path, &store, "legacy_branch")
            .await
            .unwrap();
        let second = BranchContents::from_path(&first_path, &store, "legacy_branch")
            .await
            .unwrap();
        assert_eq!(first.identifier, second.identifier);
        assert_ne!(
            first.identifier,
            BranchIdentifier::missing_identifier_sentinel()
        );
        assert_eq!(first.identifier.version_mapping[0].1.len(), 32);
        assert!(
            first.identifier.version_mapping[0]
                .1
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() && !ch.is_ascii_uppercase())
        );

        let other = BranchContents::from_path(&second_path, &store, "legacy_branch_other")
            .await
            .unwrap();
        assert_ne!(first.identifier, other.identifier);
    }

    #[tokio::test]
    async fn test_tag_contents_serialization() {
        let tag_contents = TagContents {
            branch: Some("feature".to_string()),
            version: 10,
            created_at: Some(chrono::DateTime::from_timestamp(1_234_567_000, 456_000_000).unwrap()),
            updated_at: Some(chrono::DateTime::from_timestamp(1_234_567_890, 123_000_000).unwrap()),
            manifest_size: 2048,
            metadata: HashMap::from([("channel".to_string(), "release".to_string())]),
        };

        // Test serialization
        let json = serde_json::to_string(&tag_contents).unwrap();
        assert!(json.contains("branch"));
        assert!(json.contains("version"));
        assert!(json.contains("createdAt"));
        assert!(json.contains("updatedAt"));
        assert!(json.contains("manifestSize"));
        assert!(json.contains("metadata"));

        // Test deserialization
        let deserialized: TagContents = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.branch, tag_contents.branch);
        assert_eq!(deserialized.version, tag_contents.version);
        assert_eq!(deserialized.created_at, tag_contents.created_at);
        assert_eq!(deserialized.updated_at, tag_contents.updated_at);
        assert_eq!(deserialized.manifest_size, tag_contents.manifest_size);
        assert_eq!(deserialized.metadata, tag_contents.metadata);

        let tag_contents_without_created_at = TagContents {
            branch: Some("feature".to_string()),
            version: 10,
            created_at: None,
            updated_at: Some(chrono::DateTime::from_timestamp(1_234_567_890, 123_000_000).unwrap()),
            manifest_size: 2048,
            metadata: HashMap::new(),
        };
        let json_without_created_at =
            serde_json::to_string(&tag_contents_without_created_at).unwrap();
        assert!(!json_without_created_at.contains("createdAt"));
        assert!(json_without_created_at.contains("updatedAt"));

        // Backward compatibility: older serialized content does not include timestamps or metadata.
        let legacy_json = r#"{"branch":"feature","version":10,"manifestSize":2048}"#;
        let legacy_deserialized: TagContents = serde_json::from_str(legacy_json).unwrap();
        assert_eq!(legacy_deserialized.created_at, None);
        assert_eq!(legacy_deserialized.updated_at, None);
        assert!(legacy_deserialized.metadata.is_empty());

        let legacy_updated_only_json = r#"{"branch":"feature","version":10,"updatedAt":"2009-02-13T23:31:30.123Z","manifestSize":2048}"#;
        let legacy_updated_only_deserialized: TagContents =
            serde_json::from_str(legacy_updated_only_json).unwrap();
        assert_eq!(legacy_updated_only_deserialized.created_at, None);
        assert_eq!(
            legacy_updated_only_deserialized.updated_at,
            Some(chrono::DateTime::from_timestamp(1_234_567_890, 123_000_000).unwrap())
        );
        assert!(legacy_updated_only_deserialized.metadata.is_empty());
    }

    #[rstest]
    #[case("feature/auth", &["feature/auth/sub"], None)]
    #[case("feature", &["feature/sub1", "feature/sub2"], None)]
    #[case("a/b", &["a/b/c", "b/c/d"], None)]
    #[case("main", &[], Some("main"))]
    #[case("a", &["a"], None)]
    #[case("feature/auth", &["feature/login", "feature/signup"], Some("feature/auth"))]
    #[case("feature/sub", &["feature", "other"], Some("feature/sub"))]
    #[case("very/long/common/prefix/branch1", &["very/long/common/prefix/branch2"], Some("very/long/common/prefix/branch1"))]
    #[case("feature/auth/module", &["feature/other"], Some("feature/auth"))]
    #[case("feature/dev", &["bugfix", "hotfix"], Some("feature"))]
    #[case("branch1", &["dev/branch2", "feature/nathan/branch3", "branch4"], Some("branch1"))]
    fn test_get_cleanup_path(
        #[case] branch_to_delete: &str,
        #[case] remaining_branches: &[&str],
        #[case] expected_relative_cleanup_path: Option<&str>,
    ) {
        let dataset_root_dir = "file:///var/balabala/dataset1".to_string();
        let base_location = BranchLocation {
            path: Path::from(format!("{}/tree/random_branch", dataset_root_dir.as_str())),
            uri: format!("{}/tree/random_branch", dataset_root_dir.as_str()),
            branch: Some("random_branch".to_string()),
        };

        let result =
            Branches::get_cleanup_path(branch_to_delete, remaining_branches, &base_location)
                .unwrap();

        match expected_relative_cleanup_path {
            Some(expected_relative) => {
                assert!(
                    result.is_some(),
                    "Expected cleanup path but got None for branch: {}",
                    branch_to_delete
                );
                let expected_full_path = base_location
                    .find_branch(Some(expected_relative))
                    .unwrap()
                    .path;
                assert_eq!(result.unwrap().as_ref(), expected_full_path.as_ref());
            }
            None => {
                assert!(
                    result.is_none(),
                    "Expected no cleanup but got: {:?} for branch: {}",
                    result,
                    branch_to_delete
                );
            }
        }
    }

    /// Build a reusable mocked BranchContents map mirroring cleanup::lineage_tests::build_lineage_datasets.
    ///
    /// Structure:
    ///    main:v1 ──▶ branch1:v1 ──▶ dev/branch2:v2 ──▶ feature/nathan/branch3:v3
    ///        │
    ///    (main:v2) ──▶ branch4:v2
    ///
    /// Notes:
    /// - The "main" root is virtual (no BranchContents entry).
    /// - Version numbers are representative and monotonically increasing along the chain.
    /// - Tests reuse this builder to ensure consistent lineage and deterministic assertions.
    fn build_mock_branch_contents() -> HashMap<String, BranchContents> {
        fn build(
            parent_name: Option<&str>,
            parent_branch: Option<&BranchContents>,
            parent_ver: u64,
        ) -> BranchContents {
            let parent_branch_id = if let Some(parent_branch) = parent_branch {
                parent_branch.identifier.clone()
            } else {
                BranchIdentifier::main()
            };
            BranchContents {
                parent_branch: parent_name.map(String::from),
                identifier: BranchIdentifier::new(&parent_branch_id, parent_ver),
                parent_version: parent_ver,
                create_at: 0,
                manifest_size: 1,
                metadata: HashMap::new(),
            }
        }
        let mut contents = HashMap::new();
        contents.insert("branch1".to_string(), build(None, None, 1));
        contents.insert(
            "dev/branch2".to_string(),
            build(Some("branch1"), contents.get("branch1"), 2),
        );
        contents.insert(
            "feature/nathan/branch3".to_string(),
            build(Some("dev/branch2"), contents.get("dev/branch2"), 3),
        );
        contents.insert("branch4".to_string(), build(None, None, 5));
        contents
    }

    #[test]
    fn test_collect_children_for_branch3() {
        let all_branches = build_mock_branch_contents();
        let root_id = all_branches
            .get("feature/nathan/branch3")
            .unwrap()
            .identifier
            .clone();
        assert!(
            root_id
                .collect_referenced_versions(&all_branches)
                .is_empty()
        );
    }

    #[test]
    fn test_collect_children_for_branch2() {
        let all_branches = build_mock_branch_contents();
        let root_id = all_branches.get("dev/branch2").unwrap().identifier.clone();
        let children = root_id.collect_referenced_versions(&all_branches);

        assert_eq!(children.len(), 1);
        assert_eq!(children[0].0.as_str(), "feature/nathan/branch3");
        assert_eq!(children[0].1, 3);
    }

    #[test]
    fn test_collect_children_for_branch1() {
        let all_branches = build_mock_branch_contents();
        let root_id = all_branches.get("branch1").unwrap().identifier.clone();
        let children = root_id.collect_referenced_versions(&all_branches);

        assert_eq!(children.len(), 2);
        assert_eq!(children[0].0.as_str(), "feature/nathan/branch3");
        assert_eq!(children[1].0.as_str(), "dev/branch2");
        assert_eq!(children[0].1, 2);
        assert_eq!(children[1].1, 2);
    }

    #[test]
    fn test_collect_children_for_main() {
        let all_branches = build_mock_branch_contents();
        let root_id = BranchIdentifier::main();
        let children = root_id.collect_referenced_versions(&all_branches);

        assert_eq!(children.len(), 4);
        assert_eq!(children[0].0.as_str(), "branch4");
        assert_eq!(children[1].0.as_str(), "feature/nathan/branch3");
        assert_eq!(children[2].0.as_str(), "dev/branch2");
        assert_eq!(children[3].0.as_str(), "branch1");
        assert_eq!(children[0].1, 5);
        assert_eq!(children[1].1, 1);
        assert_eq!(children[2].1, 1);
        assert_eq!(children[3].1, 1);
    }
}
