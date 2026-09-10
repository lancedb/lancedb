// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Source-change detection for computed columns.
//!
//! A refresh fills nulls, so once a value is durable nothing recomputes it and
//! a later write to one of its inputs leaves it stale forever. Two stamps in
//! the column's field metadata close that: the definition it was computed
//! under, and a per-fragment signature of the input storage it was computed
//! from. A refresh compares both with the manifest and recomputes what
//! disagrees. Signatures are read from manifests, never from data.
//!
//! The map is not stored in the manifest: it is one entry per fragment per
//! column, which would dominate the manifest of a large table with many
//! computed columns. It lives in an immutable sidecar object under
//! `_computed/`, named by its content digest, and the field metadata holds
//! only the reference. Sidecars no retained version references are removed
//! by [`prune_sidecars`].

use std::collections::{BTreeMap, HashSet};
use std::ops::Range;

use arrow_array::{Array, UInt64Array};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::transaction::Operation;
use lance_core::ROW_ADDR;
use lance_core::datatypes::{Field as LanceField, Schema as LanceSchema};
use lance_io::object_store::ObjectStore;
use lance_table::format::{DataFile, Fragment};
use object_store::path::Path;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::table::computed_columns::{
    DEFINITION_VERSION_META_KEY, RECORDED_AT_VERSION_META_KEY, SOURCE_SIGNATURE_META_KEY,
};
use crate::{Error, Result};

/// A map recorded further back than this carries nothing through the
/// compactions since: the fragments they produced are recomputed instead.
const MAX_CARRY_FORWARD_VERSIONS: u64 = 1024;

/// `{fragment id -> input signature}`.
pub type SignatureMap = BTreeMap<u32, String>;

/// Every field an input covers, keyed by column path: the field's own id
/// first, then its ancestors', because a packed file records the physical
/// column under an ancestor's id.
pub type InputFields = BTreeMap<Vec<String>, Vec<i32>>;

fn invalid(message: String) -> Error {
    Error::InvalidInput { message }
}

/// FNV-1a over the text, as hex. Stable across processes and versions, which
/// a signature compared against a stored one has to be.
fn short_hash(value: &str) -> String {
    let digest = Sha256::digest(value.as_bytes());
    digest.iter().take(8).map(|b| format!("{b:02x}")).collect()
}

/// Digest of the definition a column is computed under.
pub fn definition_version(definition: &str) -> String {
    short_hash(definition)
}

/// The fields the named input column paths cover, children included. Paths,
/// not ids, pair one schema with another: a rewrite renumbers fields. The key
/// is the path's components, so a field named `a.b` and a nested `a` -> `b`
/// are different columns.
pub fn fields_for_paths(schema: &LanceSchema, paths: &[String]) -> Result<InputFields> {
    fn collect(field: &LanceField, path: Vec<String>, ancestors: &[i32], out: &mut InputFields) {
        let mut ids = vec![field.id];
        ids.extend_from_slice(ancestors);
        for child in &field.children {
            let mut child_path = path.clone();
            child_path.push(child.name.clone());
            collect(child, child_path, &ids, out);
        }
        out.insert(path, ids);
    }
    let mut out = InputFields::new();
    for field_path in paths {
        let parts = lance_core::datatypes::parse_field_path(field_path)?;
        let (root, rest) = parts
            .split_first()
            .ok_or_else(|| invalid("computed column input path is empty".to_string()))?;
        let mut field = schema
            .field(root)
            .ok_or_else(|| invalid(format!("unknown computed column input '{field_path}'")))?;
        let mut ancestors = Vec::new();
        for name in rest {
            ancestors.insert(0, field.id);
            field = field
                .children
                .iter()
                .find(|child| child.name == *name)
                .ok_or_else(|| invalid(format!("unknown computed column input '{field_path}'")))?;
        }
        collect(field, parts, &ancestors, &mut out);
    }
    Ok(out)
}

/// Where one field's values come from in a fragment: the files and physical
/// columns storing it, and the overlays overriding cells of it, newest last
/// with the physical column and the cells each covers. A file stores the
/// field under its own id or, packed, under an ancestor's; `ids` is the
/// field's id followed by its ancestors'. Object identity is by base and
/// path; field ids are left out, since a sibling column's rewrite re-labels
/// them without touching a value.
#[derive(Debug, PartialEq, Eq)]
pub struct InputBasis {
    files: Vec<(Option<u32>, String, i32)>,
    overlays: Vec<(Option<u32>, String, i32, RoaringBitmap, u64)>,
}

pub fn input_basis(metadata: &Fragment, ids: &[i32]) -> Result<InputBasis> {
    let column_of = |file: &DataFile| {
        file.fields
            .iter()
            .position(|id| ids.contains(id))
            .map(|pos| (pos, file.column_indices.get(pos).copied().unwrap_or(-1)))
    };
    let files = metadata
        .files
        .iter()
        .filter_map(|file| {
            column_of(file).map(|(_, column)| (file.base_id, file.path.clone(), column))
        })
        .collect();
    let mut overlays = Vec::new();
    for overlay in &metadata.overlays {
        let Some((pos, column)) = column_of(&overlay.data_file) else {
            continue;
        };
        overlays.push((
            overlay.data_file.base_id,
            overlay.data_file.path.clone(),
            column,
            overlay.coverage_for_field(pos)?.as_ref().clone(),
            overlay.committed_version,
        ));
    }
    Ok(InputBasis { files, overlays })
}

/// Identity of the input data a fragment currently holds: per input field,
/// its storage basis. Deletions are left out: a deleted row is never
/// computed, and the rows that stay keep their values. Physical identity,
/// not content, so a rewrite that preserves values still reads as a change;
/// compaction is followed separately.
pub fn fragment_input_signature(fragment: &Fragment, inputs: &InputFields) -> Result<String> {
    let mut parts = Vec::new();
    for (path, ids) in inputs {
        let basis = input_basis(fragment, ids)?;
        parts.push(format!("{}={basis:?}", path.join(".")));
    }
    Ok(short_hash(&parts.join("|")))
}

fn signature_of(
    dataset: &Dataset,
    fragment_id: u32,
    inputs: &InputFields,
) -> Result<Option<String>> {
    dataset
        .get_fragment(fragment_id as usize)
        .map(|fragment| fragment_input_signature(fragment.metadata(), inputs))
        .transpose()
}

/// Signatures for `fragment_ids` as `dataset` currently holds them.
pub fn signatures_for(
    dataset: &Dataset,
    fragment_ids: &[u32],
    inputs: &InputFields,
) -> Result<SignatureMap> {
    let wanted: HashSet<u32> = fragment_ids.iter().copied().collect();
    dataset
        .get_fragments()
        .iter()
        .filter(|fragment| wanted.contains(&(fragment.id() as u32)))
        .map(|fragment| {
            Ok((
                fragment.id() as u32,
                fragment_input_signature(fragment.metadata(), inputs)?,
            ))
        })
        .collect()
}

fn field_meta(dataset: &Dataset, column: &str, key: &str) -> Option<String> {
    dataset
        .schema()
        .field(column)
        .and_then(|field| field.metadata.get(key))
        .cloned()
}

/// What the column's stored map says. The three cases are distinct and the
/// callers act differently on each: see [`staleness_against`].
#[derive(Debug)]
pub enum StoredSignatures {
    /// No map has ever been written for this column.
    Absent,
    Present(SignatureMap),
    /// A map exists but cannot be read.
    Unreadable,
}

/// Directory of signature sidecars, under the dataset root.
const SIDECAR_DIR: &str = "_computed";
/// Metadata value prefix referencing a sidecar by its content digest.
const SIDECAR_REF: &str = "sidecar:";
const SIDECAR_MAGIC: &[u8; 4] = b"CSIG";
const SIDECAR_FORMAT: u8 = 1;
/// How long an unreferenced sidecar is presumed to be a stamp in flight:
/// lance's own threshold for unverified files, independent of how much
/// version history a cleanup keeps.
const SIDECAR_UNVERIFIED_THRESHOLD_DAYS: i64 = 7;

/// The dataset's root directory: the parent of its versions directory.
/// Rebuilt from the raw parts, since re-encoding them would escape a
/// Windows drive letter's colon.
fn dataset_root(dataset: &Dataset) -> Path {
    let versions = dataset.versions_dir();
    let count = versions.parts().count();
    Path::from_iter(versions.parts().take(count.saturating_sub(1)))
}

fn sidecar_path(dataset: &Dataset, digest: &str) -> Path {
    dataset_root(dataset)
        .join(SIDECAR_DIR)
        .join(format!("{digest}.sig"))
}

/// `CSIG`, format byte, entry count, then one fragment id and 8-byte
/// signature per entry, all little-endian; 12 bytes per fragment.
fn encode_sidecar(map: &SignatureMap) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(9 + map.len() * 12);
    bytes.extend_from_slice(SIDECAR_MAGIC);
    bytes.push(SIDECAR_FORMAT);
    bytes.extend_from_slice(
        &u32::try_from(map.len())
            .map_err(|_| invalid("too many fragments for a signature sidecar".to_string()))?
            .to_le_bytes(),
    );
    for (fragment_id, signature) in map {
        let hash = u64::from_str_radix(signature, 16).map_err(|_| {
            invalid(format!(
                "signature '{signature}' is not a 64-bit hex digest"
            ))
        })?;
        bytes.extend_from_slice(&fragment_id.to_le_bytes());
        bytes.extend_from_slice(&hash.to_le_bytes());
    }
    Ok(bytes)
}

fn decode_sidecar(bytes: &[u8]) -> Result<SignatureMap> {
    let malformed = || invalid("signature sidecar is malformed".to_string());
    if bytes.len() < 9 || &bytes[..4] != SIDECAR_MAGIC || bytes[4] != SIDECAR_FORMAT {
        return Err(malformed());
    }
    let count = u32::from_le_bytes(bytes[5..9].try_into().map_err(|_| malformed())?) as usize;
    let body = &bytes[9..];
    if body.len() != count * 12 {
        return Err(malformed());
    }
    Ok(body
        .chunks_exact(12)
        .map(|entry| {
            let fragment_id = u32::from_le_bytes(entry[..4].try_into().unwrap());
            let hash = u64::from_le_bytes(entry[4..].try_into().unwrap());
            (fragment_id, format!("{hash:016x}"))
        })
        .collect())
}

fn digest_of(bytes: &[u8]) -> String {
    Sha256::digest(bytes)
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect()
}

async fn store(dataset: &Dataset) -> Result<std::sync::Arc<ObjectStore>> {
    Ok(dataset.object_store(None).await?)
}

/// Write `map` as a sidecar and return the metadata value referencing it.
/// The object is named by its digest, so two columns with the same map
/// share one object and a rewrite is idempotent.
async fn write_sidecar(dataset: &Dataset, map: &SignatureMap) -> Result<String> {
    let bytes = encode_sidecar(map)?;
    let digest = digest_of(&bytes);
    store(dataset)
        .await?
        .put(&sidecar_path(dataset, &digest), &bytes)
        .await?;
    Ok(format!("{SIDECAR_REF}{digest}"))
}

async fn read_sidecar(dataset: &Dataset, digest: &str) -> Result<SignatureMap> {
    let bytes = store(dataset)
        .await?
        .read_one_all(&sidecar_path(dataset, digest))
        .await?;
    if digest_of(&bytes) != digest {
        return Err(invalid(format!(
            "signature sidecar {digest} does not match its digest"
        )));
    }
    decode_sidecar(&bytes)
}

/// Remove signature sidecars that no version still present references,
/// the counterpart of lance's version cleanup for `_computed/`, with the
/// same protection for objects still being published: a sidecar is put
/// before the commit that references it, so one younger than
/// [`SIDECAR_UNVERIFIED_THRESHOLD_DAYS`] is left alone unless
/// `delete_unverified`. Returns how many were removed.
pub async fn prune_sidecars(dataset: &Dataset, delete_unverified: bool) -> Result<usize> {
    let store = store(dataset).await?;
    let dir = dataset_root(dataset).join(SIDECAR_DIR);
    let unmodified_since = (!delete_unverified)
        .then(|| chrono::Utc::now() - chrono::Duration::days(SIDECAR_UNVERIFIED_THRESHOLD_DAYS));
    let present: Vec<String> = match store
        .read_dir_all(&dir, unmodified_since)
        .try_collect::<Vec<_>>()
        .await
    {
        Ok(objects) => objects
            .into_iter()
            .filter_map(|object| {
                object
                    .location
                    .filename()
                    .and_then(|name| name.strip_suffix(".sig"))
                    .map(str::to_string)
            })
            .collect(),
        Err(_) => return Ok(0),
    };
    if present.is_empty() {
        return Ok(0);
    }
    let mut referenced = HashSet::new();
    for version in dataset.versions().await? {
        let at = dataset.checkout_version(version.version).await?;
        for field in at.schema().fields_pre_order() {
            if let Some(digest) = field
                .metadata
                .get(SOURCE_SIGNATURE_META_KEY)
                .and_then(|value| value.strip_prefix(SIDECAR_REF))
            {
                referenced.insert(digest.to_string());
            }
        }
    }
    let mut removed = 0;
    for digest in present {
        if !referenced.contains(&digest) {
            store.delete(&sidecar_path(dataset, &digest)).await?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// Read the column's stored map. An unreadable map is a state, not an error:
/// failing here would make the column permanently unrefreshable, and the
/// unknown recomputes like every other unknown here.
pub async fn stored_signatures(dataset: &Dataset, column: &str) -> StoredSignatures {
    let Some(encoded) = field_meta(dataset, column, SOURCE_SIGNATURE_META_KEY) else {
        return StoredSignatures::Absent;
    };
    // Inline JSON is the declaration's empty seed and the pre-sidecar form.
    let read = match encoded.strip_prefix(SIDECAR_REF) {
        Some(digest) => read_sidecar(dataset, digest).await,
        None => serde_json::from_str(&encoded).map_err(|e| invalid(e.to_string())),
    };
    match read {
        Ok(map) => StoredSignatures::Present(map),
        Err(error) => {
            log::warn!(
                "computed column '{column}' source signature map is unreadable ({error}); every fragment will be recomputed"
            );
            StoredSignatures::Unreadable
        }
    }
}

/// What a refresh must recompute beyond the null rows.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StalenessPlan {
    /// The definition changed, so every row is stale whatever its signature.
    pub recompute_all: bool,
    /// Fragments whose inputs moved since they were computed, or that were
    /// never recorded.
    pub dirty: HashSet<u32>,
    /// Fragments a compaction produced from fresh ones, at their current
    /// signature: not dirty, and for the seal to record.
    pub inherited: SignatureMap,
}

impl StalenessPlan {
    pub fn is_dirty(&self, fragment_id: u32) -> bool {
        self.recompute_all || self.dirty.contains(&fragment_id)
    }
}

/// A version of the log since the stamp that bears on carrying freshness
/// forward: an append, whose fragments hold no computed value yet, or a
/// compaction's rewrite groups, consumed and produced fragment ids.
enum Step {
    Append,
    Compaction(Vec<(Vec<u32>, Vec<u32>)>),
}

/// The step committed at `version`. A compaction's transaction records a
/// new fragment before its id is assigned, so produced ids come from the
/// version's manifest, matched by data file. Every other operation is
/// skipped: a row-moving update rewrites the rows it moves, so it does not
/// carry their inputs unchanged.
async fn step_at(dataset: &Dataset, version: u64) -> Result<Option<Step>> {
    let Some(transaction) = dataset.read_transaction_by_version(version).await? else {
        return Ok(None);
    };
    let groups = match &transaction.operation {
        Operation::Append { .. } => return Ok(Some(Step::Append)),
        Operation::Rewrite { groups, .. } => groups,
        _ => return Ok(None),
    };
    let at = dataset.checkout_version(version).await?;
    let by_file: BTreeMap<(Option<u32>, String), u32> = at
        .get_fragments()
        .iter()
        .flat_map(|fragment| {
            let id = fragment.id() as u32;
            fragment
                .metadata()
                .files
                .iter()
                .map(move |file| ((file.base_id, file.path.clone()), id))
        })
        .collect();
    let compactions = groups
        .iter()
        .map(|group| {
            let consumed = group.old_fragments.iter().map(|f| f.id as u32).collect();
            let produced = group
                .new_fragments
                .iter()
                .map(|fragment| {
                    fragment
                        .files
                        .iter()
                        .find_map(|file| by_file.get(&(file.base_id, file.path.clone())).copied())
                        .ok_or_else(|| {
                            invalid(format!(
                                "a fragment added in version {version} is not in that version's manifest"
                            ))
                        })
                })
                .collect::<Result<Vec<u32>>>()?;
            Ok((consumed, produced))
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(Some(Step::Compaction(compactions)))
}

/// Manifests since the stamp, each loaded once.
struct Manifests<'a> {
    dataset: &'a Dataset,
    loaded: BTreeMap<u64, Dataset>,
}

impl Manifests<'_> {
    async fn at(&mut self, version: u64) -> Result<&Dataset> {
        if !self.loaded.contains_key(&version) {
            let manifest = self.dataset.checkout_version(version).await?;
            self.loaded.insert(version, manifest);
        }
        Ok(&self.loaded[&version])
    }
}

/// Whether a fragment consumed by a compaction was created by an append
/// since the stamp and its inputs never moved after: `current` is its
/// signature just before the compaction. LanceDB's writes leave a computed
/// column null (see `ensure_not_written`), but a raw append need not, so
/// the rows it contributed to the product are checked to hold no value
/// (`holds_values_in`) before the product inherits freshness.
async fn appended_untouched(
    manifests: &mut Manifests<'_>,
    appends: &[u64],
    fragment_id: u32,
    current: Option<&String>,
    inputs: &InputFields,
) -> Result<bool> {
    let Some(current) = current else {
        return Ok(false);
    };
    for &version in appends.iter().rev() {
        let Some(born) = signature_of(manifests.at(version).await?, fragment_id, inputs)? else {
            continue;
        };
        if signature_of(manifests.at(version - 1).await?, fragment_id, inputs)?.is_some() {
            // Live before this append: born earlier.
            continue;
        }
        return Ok(&born == current);
    }
    Ok(false)
}

/// Whether `column` holds a value in any of `ranges`, offsets within the
/// fragment. Compaction scans its sources in order, so the rows an appended
/// source contributed sit at known offsets of the product.
async fn holds_values_in(
    dataset: &Dataset,
    fragment_id: u32,
    column: &str,
    ranges: &[Range<u64>],
) -> Result<bool> {
    let Some(fragment) = dataset.get_fragment(fragment_id as usize) else {
        return Ok(false);
    };
    let mut scanner = dataset.scan();
    scanner
        .with_fragments(vec![fragment.metadata().clone()])
        .with_row_address()
        .project(&[column])?
        .filter(&format!(
            "{} IS NOT NULL",
            super::refresh::quote_identifier(column)
        ))?;
    let mut batches = scanner.try_into_stream().await?;
    while let Some(batch) = batches.try_next().await? {
        let addresses = batch
            .column_by_name(ROW_ADDR)
            .and_then(|column| column.as_any().downcast_ref::<UInt64Array>())
            .ok_or_else(|| invalid("row addresses missing from a freshness scan".to_string()))?;
        if addresses
            .iter()
            .flatten()
            .map(|address| address & 0xFFFF_FFFF)
            .any(|offset| ranges.iter().any(|range| range.contains(&offset)))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Compaction copies inputs verbatim, so a fragment it produced from
/// recorded fragments whose inputs had not moved is as fresh as they were,
/// and a fragment an append created since the stamp, untouched after and
/// unfilled in the product, changes nothing (`appended_untouched`).
/// Followed from the version the map was recorded at through every
/// compaction since, so a chain of them carries too. Returns the produced
/// fragments' signatures at production; the caller compares each with the
/// current manifest, which catches anything written to them afterwards.
async fn carried_forward(
    dataset: &Dataset,
    column: &str,
    stored: &SignatureMap,
    inputs: &InputFields,
) -> Result<SignatureMap> {
    let Some(recorded_at) = field_meta(dataset, column, RECORDED_AT_VERSION_META_KEY)
        .and_then(|version| version.parse::<u64>().ok())
    else {
        return Ok(SignatureMap::new());
    };
    let to = dataset.version().version;
    if to <= recorded_at || to - recorded_at > MAX_CARRY_FORWARD_VERSIONS {
        return Ok(SignatureMap::new());
    }
    let mut fresh = stored.clone();
    let mut inherited = SignatureMap::new();
    let mut appends = Vec::new();
    let mut manifests = Manifests {
        dataset,
        loaded: BTreeMap::new(),
    };
    for version in (recorded_at + 1)..=to {
        let compactions = match step_at(dataset, version).await? {
            None => continue,
            Some(Step::Append) => {
                appends.push(version);
                continue;
            }
            Some(Step::Compaction(compactions)) => compactions,
        };
        for (consumed, produced) in compactions {
            // Each source's signature and live rows just before the
            // compaction; live rows place its contribution in the product.
            let mut sources = Vec::with_capacity(consumed.len());
            {
                let before = manifests.at(version - 1).await?;
                for id in &consumed {
                    let live = match before.get_fragment(*id as usize) {
                        Some(fragment) => fragment.count_rows(None).await? as u64,
                        None => 0,
                    };
                    sources.push((*id, signature_of(before, *id, inputs)?, live));
                }
            }
            let mut all_fresh = !consumed.is_empty();
            let mut appended = Vec::new();
            let mut offset = 0u64;
            for (id, current, live) in sources {
                let recorded = matches!(
                    (fresh.get(&id), current.as_ref()),
                    (Some(recorded), Some(current)) if recorded == current
                );
                if !recorded {
                    if appended_untouched(&mut manifests, &appends, id, current.as_ref(), inputs)
                        .await?
                    {
                        appended.push(offset..offset + live);
                    } else {
                        all_fresh = false;
                        break;
                    }
                }
                offset += live;
            }
            if !all_fresh {
                continue;
            }
            let after = manifests.at(version).await?;
            // The products in order, each with its share of the appended
            // rows; a value there was supplied by the append, not computed.
            let mut base = 0u64;
            let mut unfilled = true;
            for id in &produced {
                let rows = match after.get_fragment(*id as usize) {
                    Some(fragment) => fragment.count_rows(None).await? as u64,
                    None => 0,
                };
                let local: Vec<Range<u64>> = appended
                    .iter()
                    .filter(|range| range.start < base + rows && range.end > base)
                    .map(|range| range.start.max(base) - base..range.end.min(base + rows) - base)
                    .collect();
                if !local.is_empty() && holds_values_in(after, *id, column, &local).await? {
                    unfilled = false;
                    break;
                }
                base += rows;
            }
            if !unfilled {
                continue;
            }
            for id in &produced {
                if let Some(signature) = signature_of(after, *id, inputs)? {
                    fresh.insert(*id, signature.clone());
                    inherited.insert(*id, signature);
                }
            }
        }
    }
    Ok(inherited)
}

/// Decide what is stale, from the manifest alone.
///
/// A column with no map at all was declared before signatures existed. It
/// keeps its null-fill behavior until its first stamp enrolls it (see
/// [`record_freshness`]). A map that omits a fragment is authoritative: the
/// fragment's input state is unknown, and unknown recomputes -- unless a
/// compaction of fresh fragments produced it, which is followed. Lineage is
/// read only when a live fragment has no entry, so a plan over a recorded
/// table costs no transaction reads.
pub async fn staleness_against(
    dataset: &Dataset,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
) -> Result<StalenessPlan> {
    let stored = match stored_signatures(dataset, column).await {
        StoredSignatures::Absent => return Ok(StalenessPlan::default()),
        StoredSignatures::Unreadable => {
            return Ok(StalenessPlan {
                recompute_all: true,
                ..Default::default()
            });
        }
        StoredSignatures::Present(stored) => stored,
    };
    let stored_version = field_meta(dataset, column, DEFINITION_VERSION_META_KEY);
    if stored_version.is_some_and(|version| version != definition_version) {
        return Ok(StalenessPlan {
            recompute_all: true,
            ..Default::default()
        });
    }
    let unrecorded = dataset
        .get_fragments()
        .iter()
        .any(|fragment| !stored.contains_key(&(fragment.id() as u32)));
    let mut inherited = if unrecorded {
        carried_forward(dataset, column, &stored, inputs).await?
    } else {
        SignatureMap::new()
    };
    let mut dirty = HashSet::new();
    let mut live = HashSet::new();
    for fragment in dataset.get_fragments() {
        let id = fragment.id() as u32;
        live.insert(id);
        let current = fragment_input_signature(fragment.metadata(), inputs)?;
        if stored.get(&id).or_else(|| inherited.get(&id)) != Some(&current) {
            dirty.insert(id);
        }
    }
    inherited.retain(|id, _| live.contains(id) && !dirty.contains(id));
    Ok(StalenessPlan {
        recompute_all: false,
        dirty,
        inherited,
    })
}

/// What [`record_freshness`] wrote: the table version the stamp landed at,
/// if it wrote one, and the entries recorded and dropped for having moved.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct FreshnessRecord {
    pub version: Option<u64>,
    pub recorded: usize,
    pub moved: usize,
}

/// Record, on `column`, the input state its fragments were computed from.
///
/// `computed` is what this refresh computed in full, signed at the version
/// the values were read from. A column with no map yet was declared before
/// signatures existed; `pinned`, the version the refresh planned against,
/// is then the baseline: every fragment live there is trusted as it stood,
/// the null-fill contract its values were written under. Otherwise the
/// staleness decided on `pinned` supplies what compactions since the last
/// stamp carried forward. Either
/// way an entry is recorded only if `latest` still holds that input state --
/// an input write can rebase under the output commit -- so a fragment whose
/// inputs moved stays unrecorded and is recomputed by the next refresh.
///
/// Written once per refresh, after its data commit.
pub async fn record_freshness(
    latest: &mut Dataset,
    pinned: Option<(&Dataset, &StalenessPlan)>,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
    computed: SignatureMap,
) -> Result<FreshnessRecord> {
    let absent = matches!(
        stored_signatures(latest, column).await,
        StoredSignatures::Absent
    );
    let mut entries = SignatureMap::new();
    if let Some((pinned, staleness)) = pinned {
        if absent {
            let all: Vec<u32> = pinned
                .get_fragments()
                .iter()
                .map(|fragment| fragment.id() as u32)
                .collect();
            entries = signatures_for(pinned, &all, inputs)?;
        } else {
            entries = staleness.inherited.clone();
        }
    }
    entries.extend(computed);
    if entries.is_empty() && !absent {
        return Ok(FreshnessRecord::default());
    }
    let fragments: Vec<u32> = entries.keys().copied().collect();
    let current = signatures_for(latest, &fragments, inputs)?;
    let verified: SignatureMap = entries
        .into_iter()
        .filter(|(fragment_id, signature)| current.get(fragment_id) == Some(signature))
        .collect();
    let recorded = verified.len();
    let version = write_signatures(latest, column, definition_version, verified).await?;
    Ok(FreshnessRecord {
        version: Some(version),
        recorded,
        moved: fragments.len() - recorded,
    })
}

/// Merge `entries` into the column's stored map and stamp the definition and
/// the version the map now describes. Merges rather than replaces: the
/// entries cover only the fragments this refresh wrote, and every fragment it
/// skipped keeps the entry an earlier one left. Entries for fragments no
/// longer in the manifest are dropped, so compaction cannot grow the map
/// without bound. Returns the version the stamp landed at.
pub async fn write_signatures(
    dataset: &mut Dataset,
    column: &str,
    definition_version: &str,
    entries: SignatureMap,
) -> Result<u64> {
    let live: HashSet<u32> = dataset
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect();
    // An unreadable map is discarded rather than merged: nothing in it can be
    // trusted, and its fragments recompute until a later refresh records them.
    let mut merged = match stored_signatures(dataset, column).await {
        StoredSignatures::Present(stored) => stored,
        StoredSignatures::Absent | StoredSignatures::Unreadable => SignatureMap::new(),
    };
    merged.extend(entries);
    merged.retain(|fragment_id, _| live.contains(fragment_id));
    // The sidecar is durable before the commit references it; a failure in
    // between leaves an unreferenced object for `prune_sidecars`.
    let encoded = if merged.is_empty() {
        "{}".to_string()
    } else {
        write_sidecar(dataset, &merged).await?
    };
    let describes = dataset.version().version;
    dataset
        .update_field_metadata()
        .update(
            column,
            [
                (SOURCE_SIGNATURE_META_KEY.to_string(), encoded),
                (
                    DEFINITION_VERSION_META_KEY.to_string(),
                    definition_version.to_string(),
                ),
                (
                    RECORDED_AT_VERSION_META_KEY.to_string(),
                    describes.to_string(),
                ),
            ],
        )?
        .await?;
    Ok(dataset.version().version)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use arrow_array::RecordBatchIterator;
    use arrow_schema::{DataType, Field as ArrowField, Schema as ArrowSchema};
    use lance::dataset::{
        MergeInsertBuilder, MergeInsertWriteMode, NewColumnTransform, WhenMatched, WhenNotMatched,
        WriteMode, WriteParams,
    };
    use lance_file::version::ConcreteFileVersion;
    use lance_table::format::overlay::{DataOverlayFile, OverlayCoverage};

    const COLUMN: &str = "doubled";
    const EXPRESSION: &str = "value * 2";

    /// Two fragments of 50 rows, `id` and `value`, with `doubled` declared
    /// all-null against `value`, tracked from birth.
    async fn table(uri: &str) -> Dataset {
        let batch = arrow_array::record_batch!(
            ("id", Int32, (0..100).collect::<Vec<i32>>()),
            ("value", Int32, (0..100).collect::<Vec<i32>>())
        )
        .unwrap();
        let schema = batch.schema();
        let mut dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            uri,
            Some(WriteParams {
                mode: WriteMode::Create,
                max_rows_per_file: 50,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let mut metadata = std::collections::HashMap::from([
            (
                crate::table::computed_columns::COMPUTED_COLUMN_META_KEY.to_string(),
                "true".to_string(),
            ),
            (
                crate::table::computed_columns::EXPRESSION_META_KEY.to_string(),
                EXPRESSION.to_string(),
            ),
        ]);
        metadata.insert(SOURCE_SIGNATURE_META_KEY.to_string(), "{}".to_string());
        dataset
            .add_columns(
                NewColumnTransform::AllNulls(Arc::new(ArrowSchema::new(vec![
                    ArrowField::new(COLUMN, DataType::Int64, true).with_metadata(metadata),
                ]))),
                None,
                None,
            )
            .await
            .unwrap();
        dataset
    }

    fn inputs(dataset: &Dataset) -> InputFields {
        fields_for_paths(dataset.schema(), &["value".to_string()]).unwrap()
    }

    async fn plan(dataset: &Dataset) -> StalenessPlan {
        staleness_against(
            dataset,
            COLUMN,
            &definition_version(EXPRESSION),
            &inputs(dataset),
        )
        .await
        .unwrap()
    }

    async fn stamp_all(dataset: &mut Dataset) {
        let ids = inputs(dataset);
        let frags: Vec<u32> = dataset
            .get_fragments()
            .iter()
            .map(|f| f.id() as u32)
            .collect();
        let entries = signatures_for(dataset, &frags, &ids).unwrap();
        write_signatures(dataset, COLUMN, &definition_version(EXPRESSION), entries)
            .await
            .unwrap();
    }

    /// Strip the refresh's own keys: a column from before signatures existed.
    async fn make_legacy(dataset: &mut Dataset) {
        let declaration = dataset
            .schema()
            .field(COLUMN)
            .unwrap()
            .metadata
            .iter()
            .filter(|(key, _)| !key.starts_with("computed_refresh."))
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Vec<_>>();
        dataset
            .update_field_metadata()
            .replace(COLUMN, declaration)
            .unwrap()
            .await
            .unwrap();
        assert!(matches!(
            stored_signatures(dataset, COLUMN).await,
            StoredSignatures::Absent
        ));
    }

    /// Rewrite `value` of the row with `id` in place: a partial merge-insert
    /// attaches a new column file to the row's fragment, keeping its id.
    async fn rewrite_value(dataset: &mut Dataset, id: i32) {
        let batch =
            arrow_array::record_batch!(("id", Int32, [id]), ("value", Int32, [1000])).unwrap();
        let schema = batch.schema();
        let mut builder =
            MergeInsertBuilder::try_new(Arc::new(dataset.clone()), vec!["id".to_string()]).unwrap();
        builder
            .when_matched(WhenMatched::UpdateAll)
            .when_not_matched(WhenNotMatched::DoNothing)
            .write_mode(MergeInsertWriteMode::RewriteColumns);
        let (updated, _) = builder
            .try_build()
            .unwrap()
            .execute_reader(RecordBatchIterator::new([Ok(batch)], schema))
            .await
            .unwrap();
        *dataset = (*updated).clone();
    }

    async fn compact(dataset: &mut Dataset) -> u32 {
        lance::dataset::optimize::compact_files(
            dataset,
            lance::dataset::optimize::CompactionOptions {
                target_rows_per_fragment: 1000,
                ..Default::default()
            },
            None,
        )
        .await
        .unwrap();
        dataset.checkout_latest().await.unwrap();
        dataset.get_fragments()[0].id() as u32
    }

    /// `a.b` under `root` and a nested `a` -> `b` are different columns with
    /// two bases; a dotted-string key would fold them into one.
    #[test]
    fn a_dotted_field_name_is_not_a_nested_path() {
        let leaf = |name: &str| ArrowField::new(name, DataType::Int32, true);
        let root = ArrowField::new(
            "root",
            DataType::Struct(
                vec![
                    ArrowField::new("a", DataType::Struct(vec![leaf("b")].into()), true),
                    leaf("a.b"),
                ]
                .into(),
            ),
            true,
        );
        let schema = LanceSchema::try_from(&ArrowSchema::new(vec![root])).unwrap();
        let ids = fields_for_paths(&schema, &["root".to_string()]).unwrap();
        let path = |parts: &[&str]| parts.iter().map(|p| p.to_string()).collect::<Vec<_>>();
        let nested = &ids[&path(&["root", "a", "b"])];
        let dotted = &ids[&path(&["root", "a.b"])];
        assert_ne!(nested[0], dotted[0], "{ids:?}");
        assert_eq!(ids.len(), 4, "{ids:?}");
    }

    /// A packed file records the physical column under an ancestor's id, so a
    /// nested input's basis is found through its ancestors.
    #[test]
    fn a_packed_nested_input_has_a_file_basis() {
        let word_count = ArrowField::new("word_count", DataType::Int32, true);
        let metrics = ArrowField::new("metrics", DataType::Struct(vec![word_count].into()), true);
        let analysis = ArrowField::new("analysis", DataType::Struct(vec![metrics].into()), true);
        let schema = LanceSchema::try_from(&ArrowSchema::new(vec![analysis])).unwrap();
        let ids = fields_for_paths(&schema, &["analysis.metrics.word_count".to_string()]).unwrap();
        let word_count = &ids[&["analysis", "metrics", "word_count"]
            .map(String::from)
            .to_vec()];
        let analysis = schema.field("analysis").unwrap().id;
        assert_eq!(word_count.last(), Some(&analysis), "{word_count:?}");
        let mut fragment = Fragment::new(0);
        fragment.files.push(DataFile::new(
            "packed.lance",
            vec![analysis],
            vec![3],
            ConcreteFileVersion::V2_2,
            None,
            None,
        ));
        let basis = input_basis(&fragment, word_count).unwrap();
        assert_eq!(basis.files, vec![(None, "packed.lance".to_string(), 3)]);
    }

    /// An overlay that stores the input in another physical column of the
    /// same object is a different basis.
    #[test]
    fn an_overlay_column_remap_changes_the_basis() {
        let overlay = |column: i32| {
            let mut fragment = Fragment::new(0);
            fragment.overlays.push(DataOverlayFile {
                data_file: DataFile::new(
                    "overlay.lance",
                    vec![7],
                    vec![column],
                    ConcreteFileVersion::V2_2,
                    None,
                    None,
                ),
                coverage: OverlayCoverage::dense(RoaringBitmap::from_iter([0u32])),
                committed_version: 2,
            });
            input_basis(&fragment, &[7]).unwrap()
        };
        assert_ne!(overlay(0), overlay(1));
        assert_eq!(overlay(0), overlay(0));
    }

    async fn sidecar_names(dataset: &Dataset) -> Vec<String> {
        let mut names = store(dataset)
            .await
            .unwrap()
            .read_dir(dataset_root(dataset).join(SIDECAR_DIR))
            .await
            .unwrap_or_default();
        names.sort();
        names
    }

    fn signature_ref(dataset: &Dataset, column: &str) -> String {
        field_meta(dataset, column, SOURCE_SIGNATURE_META_KEY).unwrap()
    }

    /// The manifest carries only a digest; the map itself is a sidecar the
    /// reader fetches and verifies. The declaration's empty seed stays inline.
    #[tokio::test]
    async fn a_stamp_is_a_sidecar_the_manifest_only_references() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        assert_eq!(signature_ref(&dataset, COLUMN), "{}");
        stamp_all(&mut dataset).await;
        let reference = signature_ref(&dataset, COLUMN);
        let digest = reference.strip_prefix(SIDECAR_REF).unwrap();
        assert_eq!(reference.len(), SIDECAR_REF.len() + 64, "{reference}");
        assert_eq!(sidecar_names(&dataset).await, vec![format!("{digest}.sig")]);
        let StoredSignatures::Present(stored) = stored_signatures(&dataset, COLUMN).await else {
            panic!("sidecar unreadable");
        };
        assert_eq!(
            stored,
            signatures_for(&dataset, &[0, 1], &inputs(&dataset)).unwrap()
        );
    }

    /// Two columns with the same inputs share one sidecar, and a rewrite of
    /// the same map is idempotent.
    #[tokio::test]
    async fn identical_maps_share_one_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        stamp_all(&mut dataset).await;
        let ids = inputs(&dataset);
        let entries = signatures_for(&dataset, &[0, 1], &ids).unwrap();
        write_signatures(
            &mut dataset,
            "value",
            &definition_version(EXPRESSION),
            entries,
        )
        .await
        .unwrap();
        assert_eq!(sidecar_names(&dataset).await.len(), 1);
        assert_eq!(
            signature_ref(&dataset, COLUMN),
            signature_ref(&dataset, "value")
        );
    }

    /// A sidecar that is missing or whose bytes do not match the digest is
    /// unreadable: everything recomputes and the next stamp replaces it.
    #[tokio::test]
    async fn a_missing_or_corrupt_sidecar_recomputes_everything() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let digest = signature_ref(&dataset, COLUMN)
            .strip_prefix(SIDECAR_REF)
            .unwrap()
            .to_string();
        let path = sidecar_path(&dataset, &digest);
        let object_store = store(&dataset).await.unwrap();
        object_store.put(&path, b"CSIG garbage").await.unwrap();
        assert!(plan(&dataset).await.recompute_all);
        object_store.delete(&path).await.unwrap();
        assert!(plan(&dataset).await.recompute_all);
        stamp_all(&mut dataset).await;
        assert_eq!(plan(&dataset).await, StalenessPlan::default());
    }

    /// A map written inline, the pre-sidecar form, is still read.
    #[tokio::test]
    async fn an_inline_map_is_still_read() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        let ids = inputs(&dataset);
        let entries = signatures_for(&dataset, &[0, 1], &ids).unwrap();
        dataset
            .update_field_metadata()
            .update(
                COLUMN,
                [
                    (
                        SOURCE_SIGNATURE_META_KEY.to_string(),
                        serde_json::to_string(&entries).unwrap(),
                    ),
                    (
                        DEFINITION_VERSION_META_KEY.to_string(),
                        definition_version(EXPRESSION),
                    ),
                ],
            )
            .unwrap()
            .await
            .unwrap();
        assert_eq!(plan(&dataset).await, StalenessPlan::default());
    }

    /// Pruning removes only sidecars no version still present references:
    /// an older stamp survives while its version does, and goes with it.
    #[tokio::test]
    async fn pruning_follows_version_retention() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        let ids = inputs(&dataset);
        let first = signatures_for(&dataset, &[0], &ids).unwrap();
        write_signatures(&mut dataset, COLUMN, &definition_version(EXPRESSION), first)
            .await
            .unwrap();
        stamp_all(&mut dataset).await;
        assert_eq!(sidecar_names(&dataset).await.len(), 2);
        // Orphan from a stamp that never committed.
        store(&dataset)
            .await
            .unwrap()
            .put(&sidecar_path(&dataset, "orphan"), b"CSIG")
            .await
            .unwrap();
        assert_eq!(prune_sidecars(&dataset, true).await.unwrap(), 1);
        assert_eq!(sidecar_names(&dataset).await.len(), 2);

        dataset
            .cleanup_old_versions(chrono::Duration::zero(), Some(true), None)
            .await
            .unwrap();
        assert_eq!(prune_sidecars(&dataset, true).await.unwrap(), 1);
        let remaining = sidecar_names(&dataset).await;
        let current = signature_ref(&dataset, COLUMN);
        assert_eq!(
            remaining,
            vec![format!(
                "{}.sig",
                current.strip_prefix(SIDECAR_REF).unwrap()
            )]
        );
    }

    /// The gate's reproducer: a sidecar is put before the commit that
    /// references it, so a prune that interleaves must leave a recent,
    /// as yet unreferenced object alone whatever version retention the
    /// caller chose; only an unverified prune takes it.
    #[tokio::test]
    async fn pruning_does_not_collect_an_in_flight_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        let ids = inputs(&dataset);
        let entries = signatures_for(&dataset, &[0, 1], &ids).unwrap();
        let reference = write_sidecar(&dataset, &entries).await.unwrap();

        let removed = prune_sidecars(&dataset, false).await.unwrap();
        assert_eq!(removed, 0);
        dataset
            .update_field_metadata()
            .update(COLUMN, [(SOURCE_SIGNATURE_META_KEY.to_string(), reference)])
            .unwrap()
            .await
            .unwrap();
        assert!(matches!(
            stored_signatures(&dataset, COLUMN).await,
            StoredSignatures::Present(_)
        ));

        let orphan = write_sidecar(&dataset, &SignatureMap::from([(9, "0".repeat(16))]))
            .await
            .unwrap();
        assert_eq!(
            prune_sidecars(&dataset, false).await.unwrap(),
            0,
            "a recent orphan waits for its window"
        );
        assert_eq!(prune_sidecars(&dataset, true).await.unwrap(), 1);
        assert!(!sidecar_names(&dataset).await.contains(&format!(
            "{}.sig",
            orphan.strip_prefix(SIDECAR_REF).unwrap()
        )));
    }

    /// A freshly declared column is tracked from birth: every fragment is
    /// unrecorded, so every fragment is stale until a refresh records it.
    #[tokio::test]
    async fn a_declared_column_is_stale_until_recorded() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        assert_eq!(plan(&dataset).await.dirty, HashSet::from([0, 1]));
        stamp_all(&mut dataset).await;
        assert_eq!(plan(&dataset).await, StalenessPlan::default());
    }

    /// The signature answers "did my inputs move": a write to any other
    /// column, the computed column included, leaves it alone.
    #[tokio::test]
    async fn an_unrelated_column_rewrite_leaves_the_signature_alone() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        let ids = inputs(&dataset);
        let before = signatures_for(&dataset, &[0, 1], &ids).unwrap();
        dataset
            .add_columns(
                NewColumnTransform::SqlExpressions(vec![("extra".into(), "id * 3".into())]),
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(before, signatures_for(&dataset, &[0, 1], &ids).unwrap());
    }

    /// An in-place write to one fragment's input keeps every fragment id, so
    /// only the signature can notice -- and on that fragment alone.
    #[tokio::test]
    async fn an_in_place_input_change_dirties_only_its_own_fragment() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        rewrite_value(&mut dataset, 60).await;
        let stale = plan(&dataset).await;
        assert_eq!(stale.dirty, HashSet::from([1]), "{stale:?}");
    }

    /// A deleted row is never computed and the rows that stay keep their
    /// values, so a delete dirties nothing.
    #[tokio::test]
    async fn a_delete_dirties_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        dataset.delete("id >= 60 AND id < 70").await.unwrap();
        assert_eq!(plan(&dataset).await, StalenessPlan::default());
    }

    /// A definition change makes every row stale whatever the signatures say.
    #[tokio::test]
    async fn a_definition_change_recomputes_every_row() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let rebound = staleness_against(&dataset, COLUMN, "other", &inputs(&dataset))
            .await
            .unwrap();
        assert!(rebound.recompute_all);
    }

    /// A column declared before signatures existed carries no map, and keeps
    /// null-fill behavior until its first stamp enrolls it -- at the pinned
    /// state, not the latest: an input that moved in between is left out.
    #[tokio::test]
    async fn a_first_stamp_enrolls_an_older_column_as_it_stood() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        make_legacy(&mut dataset).await;
        assert_eq!(plan(&dataset).await, StalenessPlan::default());
        let ids = inputs(&dataset);
        let pinned = dataset.clone();
        let staleness = plan(&pinned).await;
        rewrite_value(&mut dataset, 60).await;
        let record = record_freshness(
            &mut dataset,
            Some((&pinned, &staleness)),
            COLUMN,
            &definition_version(EXPRESSION),
            &ids,
            SignatureMap::new(),
        )
        .await
        .unwrap();
        assert_eq!((record.recorded, record.moved), (1, 1));
        assert_eq!(record.version, Some(dataset.version().version));
        assert_eq!(plan(&dataset).await.dirty, HashSet::from([1]));
    }

    /// Append `count` rows after `first_id`, the computed column null as a
    /// write must leave it; returns the new fragment's id.
    async fn append_rows(dataset: &mut Dataset, first_id: i32, count: i32) -> u32 {
        let batch = arrow_array::record_batch!(
            (
                "id",
                Int32,
                (first_id..first_id + count).collect::<Vec<i32>>()
            ),
            (
                "value",
                Int32,
                (first_id..first_id + count).collect::<Vec<i32>>()
            ),
            ("doubled", Int64, vec![None::<i64>; count as usize])
        )
        .unwrap();
        let schema = batch.schema();
        *dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            dataset.uri(),
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        dataset.get_fragments().last().unwrap().id() as u32
    }

    /// A fragment an append created since the stamp holds no computed value,
    /// so a compaction folding it into recorded fragments produces a fresh
    /// fragment: nothing recomputes, and the null fill covers the new rows.
    #[tokio::test]
    async fn a_compaction_folding_an_untouched_appended_fragment_carries_freshness_forward() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let appended = append_rows(&mut dataset, 100, 10).await;
        assert_eq!(plan(&dataset).await.dirty, HashSet::from([appended]));
        let compacted = compact(&mut dataset).await;
        let stale = plan(&dataset).await;
        assert!(stale.dirty.is_empty(), "{stale:?}");
        assert_eq!(
            stale.inherited.keys().copied().collect::<Vec<u32>>(),
            vec![compacted]
        );
    }

    /// The same fragment with an input rewritten after the append is not
    /// neutral: what it holds may have been computed from the older input.
    #[tokio::test]
    async fn a_compaction_folding_an_appended_fragment_whose_input_moved_recomputes() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        append_rows(&mut dataset, 100, 10).await;
        rewrite_value(&mut dataset, 105).await;
        let compacted = compact(&mut dataset).await;
        let stale = plan(&dataset).await;
        assert_eq!(stale.dirty, HashSet::from([compacted]), "{stale:?}");
        assert!(stale.inherited.is_empty());
    }

    /// A raw append can supply a computed value LanceDB's own writes never
    /// do. The product holds it where the appended rows landed, so the
    /// compaction is not carried: the value is recomputed, not certified.
    #[tokio::test]
    async fn a_compaction_folding_an_appended_fragment_with_values_recomputes() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let batch = arrow_array::record_batch!(
            ("id", Int32, [100, 101]),
            ("value", Int32, [100, 101]),
            ("doubled", Int64, [None, Some(999_i64)])
        )
        .unwrap();
        let schema = batch.schema();
        dataset = Dataset::write(
            RecordBatchIterator::new(vec![Ok(batch)], schema),
            dataset.uri(),
            Some(WriteParams {
                mode: WriteMode::Append,
                ..Default::default()
            }),
        )
        .await
        .unwrap();
        let compacted = compact(&mut dataset).await;
        let stale = plan(&dataset).await;
        assert_eq!(stale.dirty, HashSet::from([compacted]), "{stale:?}");
        assert!(stale.inherited.is_empty());
    }

    /// Compaction copies inputs unchanged: a fragment it produced from
    /// recorded, unmoved fragments is fresh, and the seal records it. One
    /// produced from a fragment whose inputs had moved is not.
    #[tokio::test]
    async fn a_compaction_of_fresh_fragments_carries_freshness_forward() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let compacted = compact(&mut dataset).await;
        let stale = plan(&dataset).await;
        assert!(stale.dirty.is_empty(), "{stale:?}");
        assert_eq!(
            stale.inherited.keys().copied().collect::<Vec<u32>>(),
            vec![compacted]
        );
        let ids = inputs(&dataset);
        let pinned = dataset.clone();
        let record = record_freshness(
            &mut dataset,
            Some((&pinned, &stale)),
            COLUMN,
            &definition_version(EXPRESSION),
            &ids,
            SignatureMap::new(),
        )
        .await
        .unwrap();
        assert_eq!(record.recorded, 1);
        let StoredSignatures::Present(stored) = stored_signatures(&dataset, COLUMN).await else {
            panic!("stamped");
        };
        assert_eq!(
            stored.keys().copied().collect::<Vec<u32>>(),
            vec![compacted]
        );

        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        rewrite_value(&mut dataset, 60).await;
        let compacted = compact(&mut dataset).await;
        let stale = plan(&dataset).await;
        assert_eq!(stale.dirty, HashSet::from([compacted]), "{stale:?}");
        assert!(stale.inherited.is_empty());
    }

    /// A recorded fragment whose signature no longer matches, or a fragment
    /// the map omits without a compaction to explain it, is stale.
    #[tokio::test]
    async fn a_fragment_missing_from_the_stored_map_is_stale() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        let ids = inputs(&dataset);
        let partial = signatures_for(&dataset, &[0], &ids).unwrap();
        write_signatures(
            &mut dataset,
            COLUMN,
            &definition_version(EXPRESSION),
            partial,
        )
        .await
        .unwrap();
        assert_eq!(plan(&dataset).await.dirty, HashSet::from([1]));
    }

    /// An unreadable map recomputes everything rather than failing the
    /// refresh, and the next stamp replaces it.
    #[tokio::test]
    async fn an_unreadable_stored_map_recomputes_rather_than_failing() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        dataset
            .update_field_metadata()
            .update(
                COLUMN,
                [(SOURCE_SIGNATURE_META_KEY.to_string(), "{".to_string())],
            )
            .unwrap()
            .await
            .unwrap();
        assert!(plan(&dataset).await.recompute_all);
        stamp_all(&mut dataset).await;
        assert_eq!(plan(&dataset).await, StalenessPlan::default());
    }
}
