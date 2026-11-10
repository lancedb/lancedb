// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Central registry of documentation links emitted in SDK warnings and
//! Enterprise-only error messages.
//!
//! Every link appends standard UTM query parameters (`utm_source=lancedb_sdk`,
//! `utm_medium=warning|error`, `utm_campaign=<link id>`,
//! `utm_content=<sdk>-<version>`) before any `#anchor` fragment, and never
//! includes table names, URIs, hostnames, machine IDs, or other user data.

use std::collections::HashSet;
use std::sync::{LazyLock, Mutex, OnceLock};

use lance::Dataset;
use roaring::RoaringBitmap;

/// Base URL for the LanceDB documentation site.
pub const DOCS_BASE_URL: &str = "https://docs.lancedb.com";

/// Fragment size threshold (in physical rows) below which a fragment is counted
/// as a small fragment for compaction warnings and `Table::stats()`.
pub const SMALL_FRAGMENT_ROW_THRESHOLD: usize = 100_000;

/// Minimum number of small fragments (`< 100k` physical rows each) on an opened
/// table before emitting a one-time compaction warning.
pub const SMALL_FRAGMENT_WARN_COUNT_THRESHOLD: usize = 100;

/// Minimum number of unindexed rows on any user index of an opened table before
/// emitting a one-time index-maintenance warning.
pub const UNINDEXED_ROWS_WARN_THRESHOLD: u64 = 1_000_000;

static SDK_UTM_CONTENT: OnceLock<String> = OnceLock::new();

static SMALL_FRAGMENTS_WARNED: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));
static UNINDEXED_ROWS_WARNED: LazyLock<Mutex<HashSet<String>>> =
    LazyLock::new(|| Mutex::new(HashSet::new()));

/// Record which SDK and version are running in this process for `utm_content`.
///
/// Language bindings (`python`, `nodejs`, `cli`) call this once during module
/// initialization. If never called, `utm_content` defaults to
/// `rust-<CARGO_PKG_VERSION>`.
pub fn set_sdk_ident(sdk: &str, version: &str) {
    let _ = SDK_UTM_CONTENT.set(format!("{sdk}-{version}"));
}

/// The `<sdk>-<version>` value used for `utm_content`.
pub fn sdk_utm_content() -> &'static str {
    SDK_UTM_CONTENT
        .get_or_init(|| concat!("rust-", env!("CARGO_PKG_VERSION")).to_string())
        .as_str()
}

/// The `utm_medium` parameter value for a documentation link.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UtmMedium {
    Warning,
    Error,
}

impl UtmMedium {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Warning => "warning",
            Self::Error => "error",
        }
    }
}

/// Every documentation link emitted by the SDK.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DocLink {
    /// Compaction maintenance warning for tables with many small fragments.
    Compaction,
    /// Incremental reindexing maintenance warning for tables with many unindexed rows.
    Reindexing,
    /// SQL execution (Enterprise only).
    Sql,
    /// Server-side job management and event history (Enterprise only).
    Jobs,
    /// Registered Python Functions and Function-backed columns (Enterprise only).
    Functions,
    /// GPU class discovery for Function workers (Enterprise only).
    GpuClasses,
    /// Database Secrets and storage integrations (Enterprise only).
    Secrets,
    /// Per-row Function error store (Enterprise only).
    FunctionErrors,
    /// SQL views (Enterprise only).
    Views,
    /// Materialized views (Enterprise only).
    MaterializedViews,
    /// Branch diff against `main` (Enterprise only).
    BranchDiff,
    /// Branch cherry-pick onto `main` (Enterprise only).
    BranchCherryPick,
    /// Data cache prewarming (Enterprise only).
    PrewarmData,
    /// Deployment-wide authorization administration (Enterprise only).
    Authorization,
}

impl DocLink {
    /// Every defined [`DocLink`] variant, enumerated by CI and unit tests.
    pub const ALL: &'static [Self] = &[
        Self::Compaction,
        Self::Reindexing,
        Self::Sql,
        Self::Jobs,
        Self::Functions,
        Self::GpuClasses,
        Self::Secrets,
        Self::FunctionErrors,
        Self::Views,
        Self::MaterializedViews,
        Self::BranchDiff,
        Self::BranchCherryPick,
        Self::PrewarmData,
        Self::Authorization,
    ];

    /// Look up a [`DocLink`] by its `utm_campaign` identifier.
    pub fn from_campaign(campaign: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|link| link.campaign() == campaign)
    }

    /// The `utm_campaign` identifier for this link.
    pub const fn campaign(self) -> &'static str {
        match self {
            Self::Compaction => "compaction",
            Self::Reindexing => "reindexing",
            Self::Sql => "sql",
            Self::Jobs => "jobs",
            Self::Functions => "functions",
            Self::GpuClasses => "gpu_classes",
            Self::Secrets => "secrets",
            Self::FunctionErrors => "function_errors",
            Self::Views => "views",
            Self::MaterializedViews => "materialized_views",
            Self::BranchDiff => "branch_diff",
            Self::BranchCherryPick => "branch_cherry_pick",
            Self::PrewarmData => "prewarm_data",
            Self::Authorization => "authorization",
        }
    }

    /// The documentation page path (starting with `/`).
    ///
    /// Every path here must return HTTP 200 on the currently published
    /// `https://docs.lancedb.com` edition and also resolve (directly or via a
    /// redirect entry in `docs/web/docs.json`) in the upcoming `docs/web`
    /// edition.
    pub const fn path(self) -> &'static str {
        match self {
            Self::Compaction => "/performance",
            Self::Reindexing => "/indexing/reindexing",
            Self::Sql => "/search/sql",
            Self::BranchDiff | Self::BranchCherryPick => "/tables/branching",
            Self::Jobs
            | Self::Functions
            | Self::GpuClasses
            | Self::Secrets
            | Self::FunctionErrors
            | Self::Views
            | Self::MaterializedViews
            | Self::PrewarmData
            | Self::Authorization => "/basics/embedded-vs-enterprise",
        }
    }

    /// Optional `#anchor` fragment on the target documentation page.
    pub const fn anchor(self) -> Option<&'static str> {
        match self {
            Self::Compaction => Some("compaction-and-cleanup"),
            Self::Reindexing => Some("incremental-reindexing"),
            Self::BranchDiff => Some("diff-a-branch"),
            Self::BranchCherryPick => Some("merge-a-branch"),
            Self::Sql
            | Self::Jobs
            | Self::Functions
            | Self::GpuClasses
            | Self::Secrets
            | Self::FunctionErrors
            | Self::Views
            | Self::MaterializedViews
            | Self::PrewarmData
            | Self::Authorization => None,
        }
    }

    /// The `utm_medium` associated with this link.
    pub const fn medium(self) -> UtmMedium {
        match self {
            Self::Compaction | Self::Reindexing => UtmMedium::Warning,
            Self::Sql
            | Self::Jobs
            | Self::Functions
            | Self::GpuClasses
            | Self::Secrets
            | Self::FunctionErrors
            | Self::Views
            | Self::MaterializedViews
            | Self::BranchDiff
            | Self::BranchCherryPick
            | Self::PrewarmData
            | Self::Authorization => UtmMedium::Error,
        }
    }

    /// Build the full URL with UTM parameters placed before any `#anchor`.
    pub fn url(self) -> String {
        self.url_with_content(sdk_utm_content())
    }

    pub(crate) fn url_with_content(self, utm_content: &str) -> String {
        let path = self.path();
        let medium = self.medium().as_str();
        let campaign = self.campaign();
        match self.anchor() {
            Some(anchor) => format!(
                "{DOCS_BASE_URL}{path}?utm_source=lancedb_sdk&utm_medium={medium}&utm_campaign={campaign}&utm_content={utm_content}#{anchor}"
            ),
            None => format!(
                "{DOCS_BASE_URL}{path}?utm_source=lancedb_sdk&utm_medium={medium}&utm_campaign={campaign}&utm_content={utm_content}"
            ),
        }
    }

    /// Append `. See <url>` (or ` See <url>` if `message` already ends with `.`)
    /// to an error or warning message.
    pub fn append_to(self, message: impl AsRef<str>) -> String {
        let message = message.as_ref().trim_end();
        let url = self.url();
        if message.ends_with('.') {
            format!("{message} See {url}")
        } else {
            format!("{message}. See {url}")
        }
    }
}

fn is_warned(set: &Mutex<HashSet<String>>, table_key: &str) -> bool {
    set.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .contains(table_key)
}

fn mark_warned(set: &Mutex<HashSet<String>>, table_key: &str) -> bool {
    set.lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(table_key.to_string())
}

/// Check already-loaded dataset metadata when a table is opened and emit
/// rate-limited maintenance warnings (at most once per table per process).
pub(crate) async fn check_table_maintenance_warnings(dataset: &Dataset, table_key: &str) {
    if !log::log_enabled!(log::Level::Warn) {
        return;
    }
    if let Some(msg) = check_small_fragments_warning(dataset, table_key) {
        log::warn!("{msg}");
    }
    if let Some(msg) = check_unindexed_rows_warning(dataset, table_key).await {
        log::warn!("{msg}");
    }
}

pub(crate) fn check_small_fragments_warning(dataset: &Dataset, table_key: &str) -> Option<String> {
    if is_warned(&SMALL_FRAGMENTS_WARNED, table_key) {
        return None;
    }
    dataset.manifest().writer_version.as_ref()?;
    let num_small_fragments = dataset
        .manifest()
        .fragments
        .iter()
        .filter(|frag| {
            frag.physical_rows
                .is_some_and(|rows| rows < SMALL_FRAGMENT_ROW_THRESHOLD)
        })
        .count();
    if num_small_fragments >= SMALL_FRAGMENT_WARN_COUNT_THRESHOLD
        && mark_warned(&SMALL_FRAGMENTS_WARNED, table_key)
    {
        Some(format!(
            "Table has {num_small_fragments} small fragments (<100k rows each), \
             which can degrade query performance. Run optimize() to compact fragments. \
             See {}",
            DocLink::Compaction.url()
        ))
    } else {
        None
    }
}

fn segment_supports_optimize_without_io(segment: &lance_table::format::IndexMetadata) -> bool {
    let Some(raw_details) = segment.index_details.as_ref() else {
        return false;
    };
    let details = lance::index::scalar::IndexDetails(raw_details.clone());
    if details.is_vector() {
        return true;
    }
    details
        .get_plugin()
        .is_ok_and(|plugin| plugin.supports_optimization())
}

pub(crate) async fn check_unindexed_rows_warning(
    dataset: &Dataset,
    table_key: &str,
) -> Option<String> {
    if is_warned(&UNINDEXED_ROWS_WARNED, table_key) {
        return None;
    }
    dataset.manifest().index_section?;
    let total_rows: u64 = dataset
        .manifest()
        .fragments
        .iter()
        .filter_map(|frag| frag.num_rows().map(|rows| rows as u64))
        .sum();
    if total_rows < UNINDEXED_ROWS_WARN_THRESHOLD {
        return None;
    }

    let store = dataset.object_store(None).await.ok()?;
    let raw_indices = lance_table::io::manifest::read_manifest_indexes(
        store.as_ref(),
        dataset.manifest_location(),
        dataset.manifest(),
    )
    .await
    .ok()?;
    if raw_indices
        .iter()
        .any(|idx| idx.name == lance_index::frag_reuse::FRAG_REUSE_INDEX_NAME)
    {
        // Remapping fragment coverage after fragment reuse requires opening the
        // fragment-reuse index artifact under `_indices/`. Skip rather than
        // read index files during `open_table()`.
        return None;
    }

    let mut covered_by_index: std::collections::BTreeMap<&str, Option<RoaringBitmap>> =
        std::collections::BTreeMap::new();
    for index in &raw_indices {
        if lance_index::is_system_index(index) {
            continue;
        }
        let Some(bitmap) = index
            .fragment_bitmap
            .as_ref()
            .filter(|_| segment_supports_optimize_without_io(index))
        else {
            covered_by_index.insert(index.name.as_str(), None);
            continue;
        };
        match covered_by_index.get_mut(index.name.as_str()) {
            Some(Some(acc)) => *acc |= bitmap,
            Some(None) => {}
            None => {
                covered_by_index.insert(index.name.as_str(), Some(bitmap.clone()));
            }
        }
    }

    for covered in covered_by_index.values().flatten() {
        let unindexed_rows: u64 = dataset
            .manifest()
            .fragments
            .iter()
            .filter(|frag| !covered.contains(frag.id as u32))
            .filter_map(|frag| frag.num_rows().map(|rows| rows as u64))
            .sum();
        if unindexed_rows >= UNINDEXED_ROWS_WARN_THRESHOLD {
            if mark_warned(&UNINDEXED_ROWS_WARNED, table_key) {
                return Some(format!(
                    "Table has {unindexed_rows} unindexed rows, \
                     which can degrade search performance. Run optimize() to update indexes. \
                     See {}",
                    DocLink::Reindexing.url()
                ));
            }
            return None;
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use std::sync::Arc;

    use arrow_array::{FixedSizeListArray, Float32Array, Int32Array, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use lance::dataset::WriteDestination;
    use lance::dataset::builder::DatasetBuilder;
    use lance::dataset::transaction::Operation;
    use lance_table::format::Fragment;
    use lance_table::io::commit::write_manifest_file_to_path;
    use lance_table::io::manifest::read_manifest_indexes;
    use tempfile::tempdir;

    use crate::connect;
    use crate::database::ExecuteQueryRequest;
    use crate::function::FunctionErrorsRequest;
    use crate::index::Index;
    use crate::index::vector::IvfFlatIndexBuilder;

    use super::*;

    fn strip_mdx_comments(content: &str) -> String {
        let mut out = String::with_capacity(content.len());
        let mut rest = content;
        while let Some(start) = rest.find("{/*") {
            out.push_str(&rest[..start]);
            match rest[start + 3..].find("*/}") {
                Some(end) => {
                    rest = &rest[start + 3 + end + 3..];
                }
                None => return out,
            }
        }
        out.push_str(rest);
        out
    }

    #[test]
    fn test_doc_link_url_format_and_campaign_lookup() {
        for &link in DocLink::ALL {
            assert_eq!(DocLink::from_campaign(link.campaign()), Some(link));
            let url = link.url_with_content("python-0.42.0");
            let (before_hash, fragment) = match url.split_once('#') {
                Some((before, frag)) => (before, Some(frag)),
                None => (url.as_str(), None),
            };
            assert!(
                before_hash.starts_with(&format!(
                    "{DOCS_BASE_URL}{}?utm_source=lancedb_sdk&",
                    link.path()
                )),
                "unexpected URL prefix: {url}"
            );
            assert!(
                before_hash.contains(&format!("utm_medium={}", link.medium().as_str())),
                "missing utm_medium in {url}"
            );
            assert!(
                before_hash.contains(&format!("utm_campaign={}", link.campaign())),
                "missing utm_campaign in {url}"
            );
            assert!(
                before_hash.contains("utm_content=python-0.42.0"),
                "missing utm_content before fragment in {url}"
            );
            assert_eq!(fragment, link.anchor(), "mismatched #anchor in {url}");
        }
    }

    #[test]
    fn test_all_doc_links_resolve_in_repo_docs() {
        let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        let docs_web = manifest_dir.join("../../docs/web");
        let docs_json = std::fs::read_to_string(docs_web.join("docs.json"))
            .expect("docs/web/docs.json must exist");
        let parsed_docs_json: serde_json::Value =
            serde_json::from_str(&docs_json).expect("docs/web/docs.json must be valid JSON");
        let redirect_map: std::collections::HashMap<String, String> = parsed_docs_json
            .get("redirects")
            .and_then(|v| v.as_array())
            .into_iter()
            .flatten()
            .filter_map(|entry| {
                Some((
                    entry.get("source")?.as_str()?.to_string(),
                    entry.get("destination")?.as_str()?.to_string(),
                ))
            })
            .collect();
        let mintignore = std::fs::read_to_string(docs_web.join(".mintignore")).unwrap_or_default();
        let ignored: HashSet<&str> = mintignore
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty() && !line.starts_with('#'))
            .collect();

        for &link in DocLink::ALL {
            if let Some(destination) = redirect_map.get(link.path()) {
                let dest_page = destination
                    .split_once('#')
                    .map_or(destination.as_str(), |(p, _)| p)
                    .trim_start_matches('/');
                let dest_mdx = if dest_page.is_empty() || dest_page == "index" {
                    docs_web.join("index.mdx")
                } else if docs_web.join(format!("{dest_page}.mdx")).is_file() {
                    docs_web.join(format!("{dest_page}.mdx"))
                } else {
                    docs_web.join(format!("{dest_page}/index.mdx"))
                };
                assert!(
                    dest_mdx.is_file(),
                    "DocLink::{link:?} redirect destination '{destination}' does not resolve under docs/web"
                );
                continue;
            }

            let rel_page = link.path().trim_start_matches('/');
            let direct_mdx = format!("{rel_page}.mdx");
            let index_mdx = format!("{rel_page}/index.mdx");
            assert!(
                !ignored.contains(direct_mdx.as_str()) && !ignored.contains(index_mdx.as_str()),
                "DocLink::{link:?} points to a .mintignore'd page: {rel_page}"
            );

            // Mintlify serves MDX pages even when they are intentionally omitted
            // from navigation, so docs.json membership is not required.
            let mdx_path = if docs_web.join(&direct_mdx).is_file() {
                docs_web.join(&direct_mdx)
            } else if docs_web.join(&index_mdx).is_file() {
                docs_web.join(&index_mdx)
            } else {
                panic!("DocLink::{link:?} target page does not exist under docs/web: {rel_page}");
            };

            if let Some(anchor) = link.anchor() {
                let raw_mdx = std::fs::read_to_string(&mdx_path)
                    .unwrap_or_else(|e| panic!("failed to read {}: {e}", mdx_path.display()));
                let active_mdx = strip_mdx_comments(&raw_mdx);
                let heading_anchor = format!("{{#{anchor}}}");
                let span_anchor_double = format!("id=\"{anchor}\"");
                let span_anchor_single = format!("id='{anchor}'");
                assert!(
                    active_mdx.contains(&heading_anchor)
                        || active_mdx.contains(&span_anchor_double)
                        || active_mdx.contains(&span_anchor_single),
                    "DocLink::{link:?} anchor '#{anchor}' not found in {}",
                    mdx_path.display()
                );
            }
        }
    }

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int32, false)]));
        RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from_iter_values(0..10))]).unwrap()
    }

    #[tokio::test]
    async fn test_small_fragments_warning_and_rate_limit() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("small_frags_tbl", make_test_batch())
            .execute()
            .await
            .unwrap();

        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        assert!(
            check_small_fragments_warning(&dataset, "test://small_frags_below").is_none(),
            "1 small fragment must not trigger the compaction warning"
        );

        let existing_frag = dataset.manifest().fragments[0].clone();
        let extra_fragments: Vec<Fragment> = (0..SMALL_FRAGMENT_WARN_COUNT_THRESHOLD)
            .map(|i| {
                let mut frag = existing_frag.clone();
                frag.id = (i + 1) as u64;
                frag.physical_rows = Some(100);
                frag
            })
            .collect();
        let read_version = dataset.version().version;
        let updated = Dataset::commit(
            WriteDestination::Dataset(dataset),
            Operation::Append {
                fragments: extra_fragments,
            },
            Some(read_version),
            None,
            None,
            Arc::new(Default::default()),
            false,
        )
        .await
        .unwrap();

        let warning = check_small_fragments_warning(&updated, "test://small_frags_table_a")
            .expect("101 small fragments should trigger compaction warning");
        assert!(warning.contains(&DocLink::Compaction.url()));
        assert!(!warning.contains("small_frags_table_a"));
        assert!(
            check_small_fragments_warning(&updated, "test://small_frags_table_a").is_none(),
            "same table must be rate-limited to once per process"
        );
        assert!(
            check_small_fragments_warning(&updated, "test://small_frags_table_b").is_some(),
            "a different table must still emit its own warning once"
        );
    }

    #[tokio::test]
    async fn test_unindexed_rows_warning_and_rate_limit() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("unindexed_rows_tbl", make_test_batch())
            .execute()
            .await
            .unwrap();
        table
            .create_index(&["id"], Index::Auto)
            .execute()
            .await
            .unwrap();

        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        assert!(
            check_unindexed_rows_warning(&dataset, "test://unindexed_below")
                .await
                .is_none(),
            "fully indexed table must not trigger the reindexing warning"
        );

        let mut unindexed_frag = dataset.manifest().fragments[0].clone();
        unindexed_frag.id = 99;
        unindexed_frag.physical_rows = Some(UNINDEXED_ROWS_WARN_THRESHOLD as usize);
        let read_version = dataset.version().version;
        let updated = Dataset::commit(
            WriteDestination::Dataset(dataset),
            Operation::Append {
                fragments: vec![unindexed_frag],
            },
            Some(read_version),
            None,
            None,
            Arc::new(Default::default()),
            false,
        )
        .await
        .unwrap();

        let warning = check_unindexed_rows_warning(&updated, "test://unindexed_table_a")
            .await
            .expect("1,000,000 unindexed rows should trigger reindexing warning");
        assert!(warning.contains(&DocLink::Reindexing.url()));
        assert!(!warning.contains("unindexed_table_a"));
        assert!(
            check_unindexed_rows_warning(&updated, "test://unindexed_table_a")
                .await
                .is_none(),
            "same table must be rate-limited to once per process"
        );
        assert!(
            check_unindexed_rows_warning(&updated, "test://unindexed_table_b")
                .await
                .is_some(),
            "a different table must still emit its own warning once"
        );
    }

    #[tokio::test]
    async fn test_unindexed_rows_warning_never_reads_index_or_data_files() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();

        let dim = 4_i32;
        let num_rows = 32_usize;
        let values =
            Float32Array::from_iter_values((0..(num_rows * dim as usize)).map(|v| v as f32));
        let vec_field = Arc::new(Field::new("item", DataType::Float32, true));
        let vec_col = FixedSizeListArray::try_new(vec_field, dim, Arc::new(values), None).unwrap();
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vec",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), dim),
                false,
            ),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from_iter_values(0..num_rows as i32)),
                Arc::new(vec_col),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("vec_no_io_tbl", batch)
            .execute()
            .await
            .unwrap();
        table
            .create_index(
                &["vec"],
                Index::IvfFlat(IvfFlatIndexBuilder::default().num_partitions(1)),
            )
            .execute()
            .await
            .unwrap();

        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        let mut unindexed_frag = dataset.manifest().fragments[0].clone();
        unindexed_frag.id = 99;
        unindexed_frag.physical_rows = Some(UNINDEXED_ROWS_WARN_THRESHOLD as usize);
        let read_version = dataset.version().version;
        let updated = Dataset::commit(
            WriteDestination::Dataset(dataset),
            Operation::Append {
                fragments: vec![unindexed_frag],
            },
            Some(read_version),
            None,
            None,
            Arc::new(Default::default()),
            false,
        )
        .await
        .unwrap();

        let table_uri = tmp_dir
            .path()
            .join("vec_no_io_tbl.lance")
            .to_string_lossy()
            .to_string();

        // 1. Modern vector index: warns using only manifest metadata, with zero
        // `_indices/` or `data/` file reads.
        let reopened = DatasetBuilder::from_uri(&table_uri).load().await.unwrap();
        let store = reopened.object_store(None).await.unwrap();
        let _ = store.io_tracker().incremental_stats();
        let warning = check_unindexed_rows_warning(&reopened, "test://vec_modern_no_io").await;
        assert!(
            warning.is_some(),
            "modern vector index with 1M unindexed rows should trigger warning"
        );
        let stats = store.io_tracker().incremental_stats();
        for req in &stats.requests {
            let path = req.path.as_ref();
            assert!(
                !path.contains("_indices/") && !path.contains("/data/"),
                "unexpected index or data I/O during warning check: {path}"
            );
        }

        // 2. Legacy vector index without `index_details`: skipped cleanly without
        // opening `_indices/<uuid>/index.idx` to infer details.
        let mut indices = read_manifest_indexes(
            store.as_ref(),
            updated.manifest_location(),
            updated.manifest(),
        )
        .await
        .unwrap();
        for idx in &mut indices {
            idx.index_details = None;
        }
        let mut legacy_manifest = (*updated.manifest()).clone();
        write_manifest_file_to_path(
            store.as_ref(),
            &mut legacy_manifest,
            Some(indices),
            &reopened.manifest_location().path,
            None,
        )
        .await
        .unwrap();

        let reopened_legacy = DatasetBuilder::from_uri(&table_uri).load().await.unwrap();
        let legacy_store = reopened_legacy.object_store(None).await.unwrap();
        let _ = legacy_store.io_tracker().incremental_stats();
        let legacy_warning =
            check_unindexed_rows_warning(&reopened_legacy, "test://vec_legacy_no_io").await;
        assert!(
            legacy_warning.is_none(),
            "legacy index missing index_details must be skipped without disk inference"
        );
        let legacy_stats = legacy_store.io_tracker().incremental_stats();
        for req in &legacy_stats.requests {
            let path = req.path.as_ref();
            assert!(
                !path.contains("_indices/") && !path.contains("/data/"),
                "unexpected index or data I/O for legacy index: {path}"
            );
        }
    }

    #[tokio::test]
    async fn test_enterprise_only_errors_contain_utm_doc_links() {
        let tmp_dir = tempdir().unwrap();
        let conn = connect(tmp_dir.path().to_str().unwrap())
            .execute()
            .await
            .unwrap();
        let table = conn
            .create_table("err_tbl", make_test_batch())
            .execute()
            .await
            .unwrap();

        let sql_err = conn
            .database()
            .execute_query_async(ExecuteQueryRequest::new("SELECT 1"))
            .await
            .unwrap_err()
            .to_string();
        assert!(sql_err.contains(&DocLink::Sql.url()), "{sql_err}");

        let job_err = conn.open_job("job-1").await.unwrap_err().to_string();
        assert!(job_err.contains(&DocLink::Jobs.url()), "{job_err}");

        let gpu_err = conn.list_gpu_classes().await.unwrap_err().to_string();
        assert!(gpu_err.contains(&DocLink::GpuClasses.url()), "{gpu_err}");

        let secret_err = conn
            .create_secret("my_secret", "val", &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(secret_err.contains(&DocLink::Secrets.url()), "{secret_err}");

        let view_err = conn
            .create_view("v", "SELECT 1", &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(view_err.contains(&DocLink::Views.url()), "{view_err}");

        let diff_err = table.diff_branch("feature").await.unwrap_err().to_string();
        assert!(diff_err.contains(&DocLink::BranchDiff.url()), "{diff_err}");

        let cp_err = table
            .cherry_pick("feature", false)
            .await
            .unwrap_err()
            .to_string();
        assert!(
            cp_err.contains(&DocLink::BranchCherryPick.url()),
            "{cp_err}"
        );

        let prewarm_err = table.prewarm_data(None).await.unwrap_err().to_string();
        assert!(
            prewarm_err.contains(&DocLink::PrewarmData.url()),
            "{prewarm_err}"
        );

        let fn_err = table
            .function_errors(FunctionErrorsRequest::new())
            .await
            .unwrap_err()
            .to_string();
        assert!(fn_err.contains(&DocLink::FunctionErrors.url()), "{fn_err}");
    }
}
