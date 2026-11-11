// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Source-change detection for computed columns.
//!
//! A refresh fills nulls, so once a value is durable nothing recomputes it and
//! a later write to one of its inputs leaves it stale forever. The column's
//! field metadata records the definition it was computed under, a
//! per-fragment signature of the input storage it was computed from, and the
//! rows whose retained values are not current for that signature. A refresh
//! compares the stamps with the manifest and recomputes what disagrees.
//! Signatures are read from manifests, never from data.
//!
//! The map is not stored in the manifest: it is one entry per fragment per
//! column, which would dominate the manifest of a large table with many
//! computed columns. It lives in an immutable sidecar object under
//! `_computed/`, named by its content digest, and the field metadata holds
//! only the reference. The sidecar also carries row-level exceptions and the
//! accepted repair basis, so old readers reject the newer format and
//! conservatively recompute. Sidecars no retained version references are
//! removed by [`prune_sidecars`].

use std::collections::{BTreeMap, HashMap, HashSet};
use std::fmt;
use std::io::{Cursor, Read};
use std::ops::Range;

use arrow_array::{Array, UInt64Array};
use arrow_schema::{DataType, Schema as ArrowSchema};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::refs::Ref;
use lance::dataset::transaction::{Operation, UpdateMode};
use lance_core::ROW_ADDR;
use lance_core::datatypes::{Field as LanceField, Schema as LanceSchema};
use lance_io::object_store::{ObjectStore, uri_to_url};
use lance_table::format::{DataFile, Fragment};
use object_store::path::Path;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use uuid::Uuid;

use crate::table::computed_columns::{
    DEFINITION_VERSION_META_KEY, FUNCTION_BINDINGS_META_KEY, PENDING_FILL_SIGNATURE_META_KEY,
    RECORDED_AT_VERSION_META_KEY, SOURCE_SIGNATURE_META_KEY, field_root, function_bindings,
};
use crate::{Error, Result};

/// A map recorded further back than this carries nothing through the
/// compactions since: the fragments they produced are recomputed instead.
const MAX_CARRY_FORWARD_VERSIONS: u64 = 1024;

/// `{fragment id -> input signature}`.
pub type SignatureMap = BTreeMap<u32, String>;

/// Rows whose retained values are not current for the fragment signature.
///
/// `AllExcept` represents a fragment whose basis changed after a partial
/// refresh: every physical row is dirty except the offsets recorded as clean.
/// It avoids materializing a full bitmap while preserving a partially filled
/// fragment.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DirtyRows {
    /// The listed physical offsets are dirty.
    Rows(RoaringBitmap),
    /// Every physical offset is dirty except the listed clean offsets.
    AllExcept(RoaringBitmap),
}

impl DirtyRows {
    fn mark_dirty(&mut self, rows: &RoaringBitmap) {
        match self {
            Self::Rows(dirty) => *dirty |= rows,
            Self::AllExcept(clean) => *clean -= rows,
        }
    }

    fn mark_clean(&mut self, rows: &RoaringBitmap) {
        match self {
            Self::Rows(dirty) => *dirty -= rows,
            Self::AllExcept(clean) => *clean |= rows,
        }
    }

    fn is_explicitly_empty(&self) -> bool {
        matches!(self, Self::Rows(rows) if rows.is_empty())
    }

    /// Materialize the dirty offsets among `live_rows`.
    pub fn live_dirty_rows(&self, live_rows: &RoaringBitmap) -> RoaringBitmap {
        match self {
            Self::Rows(dirty) => dirty & live_rows,
            Self::AllExcept(clean) => live_rows - clean,
        }
    }
}

/// `{fragment id -> row-level dirty state}`. A missing fragment is clean.
pub type RowStateMap = BTreeMap<u32, DirtyRows>;

/// Physical rows whose latest Function outcome is an unresolved failure.
pub type FailureRows = BTreeMap<u32, RoaringBitmap>;

/// Evidence for rows whose latest Function outcome is known.
///
/// `known_rows` includes both successful and failed observations.
/// `failed_rows` is the failure subset. A missing row is unknown, not clean.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FailureEvidence {
    pub known_rows: FailureRows,
    pub failed_rows: FailureRows,
}

impl FailureEvidence {
    fn is_empty(&self) -> bool {
        self.known_rows.is_empty() && self.failed_rows.is_empty()
    }

    fn validate(&self) -> Result<()> {
        if self.failed_rows.iter().any(|(fragment_id, failed)| {
            !self
                .known_rows
                .get(fragment_id)
                .is_some_and(|known| failed.is_subset(known))
        }) {
            return Err(invalid(
                "failed rows are not covered by the failure evidence update".to_string(),
            ));
        }
        Ok(())
    }

    fn for_fragments(&self, fragments: &HashSet<u32>) -> Self {
        Self {
            known_rows: self
                .known_rows
                .iter()
                .filter(|(fragment_id, _)| fragments.contains(fragment_id))
                .map(|(fragment_id, rows)| (*fragment_id, rows.clone()))
                .collect(),
            failed_rows: self
                .failed_rows
                .iter()
                .filter(|(fragment_id, _)| fragments.contains(fragment_id))
                .map(|(fragment_id, rows)| (*fragment_id, rows.clone()))
                .collect(),
        }
    }
}

/// The single owning value of the freshness sidecar.
///
/// `SignatureMap` remains the public facade for callers that only need input
/// signatures. The row state and accepted basis live here so a reader can
/// never observe one half of a mixed-generation freshness record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct FreshnessState {
    signatures: SignatureMap,
    row_state: RowStateMap,
    /// Physical offsets with an exact current-generation outcome.
    failure_coverage: FailureRows,
    /// The failed subset of `failure_coverage`.
    failed_rows: FailureRows,
    /// Whether the decoded source used the v3 sidecar format that carries
    /// failure evidence. Older formats leave failure rows unclassified.
    failure_evidence_recorded: bool,
    basis: String,
    // A fresh identity for each begin, even when the basis and dirty rows repeat.
    intent_id: Uuid,
}

impl FreshnessState {
    fn normalize(mut self) -> Result<Self> {
        self.row_state.retain(|_, rows| !rows.is_explicitly_empty());
        self.failure_coverage.retain(|_, rows| !rows.is_empty());
        self.failed_rows.retain(|_, rows| !rows.is_empty());
        if self
            .row_state
            .keys()
            .any(|fragment_id| !self.signatures.contains_key(fragment_id))
        {
            return Err(invalid(
                "row state names a fragment without a signature".to_string(),
            ));
        }
        if self
            .failure_coverage
            .keys()
            .chain(self.failed_rows.keys())
            .any(|fragment_id| !self.signatures.contains_key(fragment_id))
        {
            return Err(invalid(
                "failure evidence names a fragment without a signature".to_string(),
            ));
        }
        if self.failed_rows.iter().any(|(fragment_id, failed)| {
            !self
                .failure_coverage
                .get(fragment_id)
                .is_some_and(|covered| failed.is_subset(covered))
        }) {
            return Err(invalid(
                "failed rows are not covered by failure evidence".to_string(),
            ));
        }
        if self
            .signatures
            .values()
            .any(|signature| !is_signature_hex(signature))
        {
            return Err(invalid(
                "freshness signature is not a 64-bit hex digest".to_string(),
            ));
        }
        if self.basis.len() > u16::MAX as usize {
            return Err(invalid("freshness basis exceeds u16 bytes".to_string()));
        }
        Ok(self)
    }

    fn invalidate_failure_evidence(&mut self, fragments: impl IntoIterator<Item = u32>) {
        for fragment_id in fragments {
            self.failure_coverage.remove(&fragment_id);
            self.failed_rows.remove(&fragment_id);
        }
    }

    fn invalidate_failure_rows(&mut self, rows: &BTreeMap<u32, RoaringBitmap>) {
        for (fragment_id, rows) in rows {
            if let Some(covered) = self.failure_coverage.get_mut(fragment_id) {
                *covered -= rows;
            }
            if self
                .failure_coverage
                .get(fragment_id)
                .is_some_and(RoaringBitmap::is_empty)
            {
                self.failure_coverage.remove(fragment_id);
            }
            if let Some(failed) = self.failed_rows.get_mut(fragment_id) {
                *failed -= rows;
            }
            if self
                .failed_rows
                .get(fragment_id)
                .is_some_and(RoaringBitmap::is_empty)
            {
                self.failed_rows.remove(fragment_id);
            }
        }
    }
}

/// Opaque accepted-basis evidence returned by [`begin_row_repair`] and required by
/// [`finish_row_repair`].
///
/// The digest identifies the unified freshness state established before
/// publication, and the selected-row bitmaps bound which rows finalization
/// may change. One accepted wave can finalize disjoint fragments together;
/// later serialized waves preserve clean rows from earlier accepted waves.
/// Each begin persists a fresh intent identity in the sidecar, so repeating
/// the same basis and rows still fences stale non-noop finalization. There is
/// no per-row lease.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RowStateToken {
    pub basis: String,
    pub intent_digest: String,
    pub selected_rows: BTreeMap<u32, RoaringBitmap>,
}

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
/// field's id followed by its ancestors'. Object identity is the resolved
/// location and path (see [`Bases`]); field ids are left out, since a
/// sibling column's rewrite re-labels them without touching a value.
#[derive(Debug, PartialEq, Eq)]
pub struct InputBasis {
    files: Vec<(String, String, i32)>,
    overlays: Vec<(String, String, i32, RoaringBitmap, u64)>,
}

/// Where each storage base's data lives, as the store resolves it. A file
/// signs the same on the table that wrote it and on a branch or shallow
/// clone reading it through a base, while files under different bases
/// stay distinct.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bases {
    own: String,
    registered: HashMap<u32, String>,
}

impl Bases {
    pub fn of(dataset: &Dataset) -> Result<Self> {
        let registered = dataset
            .manifest()
            .base_paths
            .values()
            .map(|base| Ok((base.id, location(&base.path, base.is_dataset_root)?)))
            .collect::<Result<_>>()?;
        Ok(Self {
            own: location(dataset.uri(), true)?,
            registered,
        })
    }

    fn location(&self, base_id: Option<u32>) -> Result<&str> {
        match base_id {
            None => Ok(&self.own),
            Some(id) => self
                .registered
                .get(&id)
                .map(String::as_str)
                .ok_or_else(|| invalid(format!("base path id {id} not found"))),
        }
    }
}

/// `uri` as its store resolves it, with the data directory under a dataset
/// root. The query part only selects a commit handler, so it is left out.
fn location(uri: &str, is_dataset_root: bool) -> Result<String> {
    let url = uri_to_url(uri)?;
    let mut location = url[..url::Position::BeforeQuery]
        .trim_end_matches('/')
        .to_string();
    if is_dataset_root {
        location.push_str("/data");
    }
    Ok(location)
}

pub fn input_basis(bases: &Bases, metadata: &Fragment, ids: &[i32]) -> Result<InputBasis> {
    let column_of = |file: &DataFile| {
        file.fields
            .iter()
            .position(|id| ids.contains(id))
            .map(|pos| (pos, file.column_indices.get(pos).copied().unwrap_or(-1)))
    };
    let mut files = Vec::new();
    for file in &metadata.files {
        if let Some((_, column)) = column_of(file) {
            files.push((
                bases.location(file.base_id)?.to_string(),
                file.path.clone(),
                column,
            ));
        }
    }
    let mut overlays = Vec::new();
    for overlay in &metadata.overlays {
        let Some((pos, column)) = column_of(&overlay.data_file) else {
            continue;
        };
        overlays.push((
            bases.location(overlay.data_file.base_id)?.to_string(),
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
pub fn fragment_input_signature(
    bases: &Bases,
    fragment: &Fragment,
    inputs: &InputFields,
) -> Result<String> {
    let mut parts = Vec::new();
    for (path, ids) in inputs {
        let basis = input_basis(bases, fragment, ids)?;
        parts.push(format!("{}={basis:?}", path.join(".")));
    }
    Ok(short_hash(&parts.join("|")))
}

fn signature_of(
    dataset: &Dataset,
    fragment_id: u32,
    inputs: &InputFields,
) -> Result<Option<String>> {
    let bases = Bases::of(dataset)?;
    dataset
        .get_fragment(fragment_id as usize)
        .map(|fragment| fragment_input_signature(&bases, fragment.metadata(), inputs))
        .transpose()
}

/// Signatures for `fragment_ids` as `dataset` currently holds them.
pub fn signatures_for(
    dataset: &Dataset,
    fragment_ids: &[u32],
    inputs: &InputFields,
) -> Result<SignatureMap> {
    let wanted: HashSet<u32> = fragment_ids.iter().copied().collect();
    let bases = Bases::of(dataset)?;
    dataset
        .get_fragments()
        .iter()
        .filter(|fragment| wanted.contains(&(fragment.id() as u32)))
        .map(|fragment| {
            Ok((
                fragment.id() as u32,
                fragment_input_signature(&bases, fragment.metadata(), inputs)?,
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

/// What the column's stored row-level state says.
#[derive(Debug, PartialEq, Eq)]
pub enum StoredRowStates {
    /// No row-level state has been written; every live value is clean.
    Absent,
    /// Explicit dirty state for the fragments named in the map.
    Present(RowStateMap),
    /// The owning freshness sidecar cannot be read.
    Unreadable,
}

/// Directory of freshness sidecars, under the dataset root.
const SIDECAR_DIR: &str = "_computed";
/// Metadata value prefix referencing a sidecar by its content digest.
const SIDECAR_REF: &str = "sidecar:";
const SIDECAR_MAGIC: &[u8; 4] = b"CSIG";
const SIDECAR_FORMAT_V1: u8 = 1;
const SIDECAR_FORMAT_V2: u8 = 2;
const SIDECAR_FORMAT: u8 = 3;
/// How long an unreferenced sidecar is presumed to be a stamp in flight:
/// lance's own threshold for unverified files, independent of how much
/// version history a cleanup keeps.
const SIDECAR_UNVERIFIED_THRESHOLD_DAYS: i64 = 7;

/// The table's root: one `_computed/` per table, shared by main and its
/// branches, so a branch reads the stamps it was taken with.
fn dataset_root(dataset: &Dataset) -> Result<Path> {
    Ok(dataset.branch_location().find_main()?.path)
}

fn sidecar_path(dataset: &Dataset, digest: &str) -> Result<Path> {
    Ok(dataset_root(dataset)?
        .join(SIDECAR_DIR)
        .join(format!("{digest}.sig")))
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn is_signature_hex(value: &str) -> bool {
    value.len() == 16 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

#[cfg(test)]
fn encode_legacy_sidecar(map: &SignatureMap) -> Result<Vec<u8>> {
    let mut bytes = Vec::with_capacity(9 + map.len() * 12);
    bytes.extend_from_slice(SIDECAR_MAGIC);
    bytes.push(SIDECAR_FORMAT_V1);
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

#[cfg(test)]
fn encode_v2_sidecar(state: &FreshnessState) -> Result<Vec<u8>> {
    let state = state.clone().normalize()?;
    let basis_len = u16::try_from(state.basis.len())
        .map_err(|_| invalid("freshness basis exceeds u16 bytes".to_string()))?;
    let fragment_count = u32::try_from(state.signatures.len())
        .map_err(|_| invalid("too many fragments for a freshness sidecar".to_string()))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SIDECAR_MAGIC);
    bytes.push(SIDECAR_FORMAT_V2);
    bytes.extend_from_slice(state.intent_id.as_bytes());
    bytes.extend_from_slice(&basis_len.to_le_bytes());
    bytes.extend_from_slice(state.basis.as_bytes());
    bytes.extend_from_slice(&fragment_count.to_le_bytes());
    for (fragment_id, signature) in &state.signatures {
        let hash = u64::from_str_radix(signature, 16).map_err(|_| {
            invalid(format!(
                "signature '{signature}' is not a 64-bit hex digest"
            ))
        })?;
        let rows = state
            .row_state
            .get(fragment_id)
            .cloned()
            .unwrap_or_else(|| DirtyRows::Rows(RoaringBitmap::new()));
        let (kind, bitmap) = match rows {
            DirtyRows::Rows(bitmap) => (0u8, bitmap),
            DirtyRows::AllExcept(clean) => (1u8, clean),
        };
        bytes.extend_from_slice(&fragment_id.to_le_bytes());
        bytes.extend_from_slice(&hash.to_le_bytes());
        bytes.push(kind);
        encode_bitmap(&mut bytes, Some(&bitmap))?;
    }
    Ok(bytes)
}

fn encode_bitmap(bytes: &mut Vec<u8>, bitmap: Option<&RoaringBitmap>) -> Result<()> {
    let Some(bitmap) = bitmap.filter(|bitmap| !bitmap.is_empty()) else {
        bytes.extend_from_slice(&0u32.to_le_bytes());
        return Ok(());
    };
    let mut encoded = Vec::with_capacity(bitmap.serialized_size());
    bitmap
        .serialize_into(&mut encoded)
        .map_err(|error| invalid(format!("encoding freshness bitmap: {error}")))?;
    let bitmap_len = u32::try_from(encoded.len())
        .map_err(|_| invalid("freshness bitmap exceeds u32 bytes".to_string()))?;
    bytes.extend_from_slice(&bitmap_len.to_le_bytes());
    bytes.extend_from_slice(&encoded);
    Ok(())
}

fn decode_bitmap(
    cursor: &mut Cursor<&[u8]>,
    malformed: &impl Fn() -> Error,
) -> Result<Option<RoaringBitmap>> {
    let mut bitmap_len = [0u8; 4];
    cursor
        .read_exact(&mut bitmap_len)
        .map_err(|_| malformed())?;
    let bitmap_len = u32::from_le_bytes(bitmap_len) as usize;
    if bitmap_len == 0 {
        return Ok(None);
    }
    let mut encoded = vec![0u8; bitmap_len];
    cursor.read_exact(&mut encoded).map_err(|_| malformed())?;
    let mut bitmap_reader = Cursor::new(encoded.as_slice());
    let bitmap = RoaringBitmap::deserialize_from(&mut bitmap_reader).map_err(|_| malformed())?;
    if bitmap_reader.position() != bitmap_len as u64 {
        return Err(malformed());
    }
    Ok(Some(bitmap))
}

/// `CSIG`, format byte, 16-byte intent UUID, basis, entry count, then for each fragment its id,
/// 8-byte signature, dirty-row mode and serialized bitmap. Format 3 appends
/// failure-coverage and failed-row bitmaps. All integers are little-endian.
/// Format 1 remains readable as clean row state.
fn encode_sidecar(state: &FreshnessState) -> Result<Vec<u8>> {
    let state = state.clone().normalize()?;
    let basis_len = u16::try_from(state.basis.len())
        .map_err(|_| invalid("freshness basis exceeds u16 bytes".to_string()))?;
    let fragment_count = u32::try_from(state.signatures.len())
        .map_err(|_| invalid("too many fragments for a freshness sidecar".to_string()))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SIDECAR_MAGIC);
    bytes.push(SIDECAR_FORMAT);
    bytes.extend_from_slice(state.intent_id.as_bytes());
    bytes.extend_from_slice(&basis_len.to_le_bytes());
    bytes.extend_from_slice(state.basis.as_bytes());
    bytes.extend_from_slice(&fragment_count.to_le_bytes());
    for (fragment_id, signature) in &state.signatures {
        let hash = u64::from_str_radix(signature, 16).map_err(|_| {
            invalid(format!(
                "signature '{signature}' is not a 64-bit hex digest"
            ))
        })?;
        let rows = state
            .row_state
            .get(fragment_id)
            .cloned()
            .unwrap_or_else(|| DirtyRows::Rows(RoaringBitmap::new()));
        let (kind, bitmap) = match rows {
            DirtyRows::Rows(bitmap) => (0u8, bitmap),
            DirtyRows::AllExcept(clean) => (1u8, clean),
        };
        bytes.extend_from_slice(&fragment_id.to_le_bytes());
        bytes.extend_from_slice(&hash.to_le_bytes());
        bytes.push(kind);
        encode_bitmap(&mut bytes, Some(&bitmap))?;
        encode_bitmap(&mut bytes, state.failure_coverage.get(fragment_id))?;
        encode_bitmap(&mut bytes, state.failed_rows.get(fragment_id))?;
    }
    Ok(bytes)
}

fn decode_legacy_sidecar(bytes: &[u8]) -> Result<FreshnessState> {
    let malformed = || invalid("signature sidecar is malformed".to_string());
    if bytes.len() < 9 || &bytes[..4] != SIDECAR_MAGIC || bytes[4] != SIDECAR_FORMAT_V1 {
        return Err(malformed());
    }
    let count = u32::from_le_bytes(bytes[5..9].try_into().map_err(|_| malformed())?) as usize;
    let body = &bytes[9..];
    if body.len() != count * 12 {
        return Err(malformed());
    }
    let signatures = body
        .chunks_exact(12)
        .map(|entry| {
            let fragment_id = u32::from_le_bytes(entry[..4].try_into().unwrap());
            let hash = u64::from_le_bytes(entry[4..].try_into().unwrap());
            (fragment_id, format!("{hash:016x}"))
        })
        .collect();
    Ok(FreshnessState {
        signatures,
        ..Default::default()
    })
}

fn decode_sidecar(bytes: &[u8]) -> Result<FreshnessState> {
    if bytes.len() >= 5 && &bytes[..4] == SIDECAR_MAGIC && bytes[4] == SIDECAR_FORMAT_V1 {
        return decode_legacy_sidecar(bytes);
    }
    let malformed = || invalid("freshness sidecar is malformed".to_string());
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; 4];
    cursor.read_exact(&mut magic).map_err(|_| malformed())?;
    let mut format = [0u8; 1];
    cursor.read_exact(&mut format).map_err(|_| malformed())?;
    if &magic != SIDECAR_MAGIC || !matches!(format[0], SIDECAR_FORMAT_V2 | SIDECAR_FORMAT) {
        return Err(malformed());
    }
    let mut intent_id = [0u8; 16];
    cursor.read_exact(&mut intent_id).map_err(|_| malformed())?;
    let mut basis_len = [0u8; 2];
    cursor.read_exact(&mut basis_len).map_err(|_| malformed())?;
    let mut basis = vec![0u8; u16::from_le_bytes(basis_len) as usize];
    cursor.read_exact(&mut basis).map_err(|_| malformed())?;
    let basis = String::from_utf8(basis).map_err(|_| malformed())?;
    let mut count = [0u8; 4];
    cursor.read_exact(&mut count).map_err(|_| malformed())?;
    let count = u32::from_le_bytes(count) as usize;
    let mut state = FreshnessState {
        basis,
        intent_id: Uuid::from_bytes(intent_id),
        failure_evidence_recorded: format[0] == SIDECAR_FORMAT,
        ..Default::default()
    };
    for _ in 0..count {
        let mut fragment_id = [0u8; 4];
        cursor
            .read_exact(&mut fragment_id)
            .map_err(|_| malformed())?;
        let fragment_id = u32::from_le_bytes(fragment_id);
        let mut signature = [0u8; 8];
        cursor.read_exact(&mut signature).map_err(|_| malformed())?;
        let mut kind = [0u8; 1];
        cursor.read_exact(&mut kind).map_err(|_| malformed())?;
        let bitmap = decode_bitmap(&mut cursor, &malformed)?.unwrap_or_default();
        let dirty = match kind[0] {
            0 => DirtyRows::Rows(bitmap),
            1 => DirtyRows::AllExcept(bitmap),
            _ => return Err(malformed()),
        };
        if state.signatures.contains_key(&fragment_id) {
            return Err(malformed());
        }
        state.signatures.insert(
            fragment_id,
            format!("{:016x}", u64::from_le_bytes(signature)),
        );
        if !dirty.is_explicitly_empty() {
            state.row_state.insert(fragment_id, dirty);
        }
        if format[0] == SIDECAR_FORMAT {
            if let Some(covered) = decode_bitmap(&mut cursor, &malformed)? {
                state.failure_coverage.insert(fragment_id, covered);
            }
            if let Some(failed) = decode_bitmap(&mut cursor, &malformed)? {
                state.failed_rows.insert(fragment_id, failed);
            }
        }
    }
    if cursor.position() != bytes.len() as u64 {
        return Err(malformed());
    }
    state.normalize()
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

/// Write `state` as a sidecar and return the metadata value referencing it.
/// The object is named by its digest, so two columns with the same state
/// share one object and a rewrite is idempotent.
async fn write_sidecar(dataset: &Dataset, state: &FreshnessState) -> Result<String> {
    write_sidecar_bytes(dataset, encode_sidecar(state)?).await
}

async fn write_sidecar_bytes(dataset: &Dataset, bytes: Vec<u8>) -> Result<String> {
    let digest = digest_of(&bytes);
    store(dataset)
        .await?
        .put(&sidecar_path(dataset, &digest)?, &bytes)
        .await?;
    Ok(format!("{SIDECAR_REF}{digest}"))
}

/// A sidecar is read from the table's own directory, then through the bases a
/// shallow clone reads its data from, where the source stamped it. A copy
/// found through a base is put in the table's own directory so it outlives
/// the source's pruning; a failed copy only costs the next lookup.
async fn read_sidecar_bytes(dataset: &Dataset, digest: &str) -> Result<bytes::Bytes> {
    let own = sidecar_path(dataset, digest)?;
    let store = store(dataset).await?;
    let bytes = match store.read_one_all(&own).await {
        Ok(bytes) => bytes,
        Err(error) if error.is_not_found() => {
            let bytes = read_sidecar_through_bases(dataset, digest).await?;
            if let Err(error) = store.put(&own, &bytes).await {
                log::warn!(
                    "computed column freshness sidecar {digest} could not be copied to {own}: {error}"
                );
            }
            bytes
        }
        Err(error) => return Err(error.into()),
    };
    if digest_of(&bytes) != digest {
        return Err(invalid(format!(
            "freshness sidecar {digest} does not match its digest"
        )));
    }
    Ok(bytes)
}

async fn read_sidecar(dataset: &Dataset, digest: &str) -> Result<FreshnessState> {
    decode_sidecar(&read_sidecar_bytes(dataset, digest).await?)
}

async fn read_sidecar_through_bases(dataset: &Dataset, digest: &str) -> Result<bytes::Bytes> {
    let registry = dataset.session().store_registry();
    for base in dataset.manifest().base_paths.values() {
        if !base.is_dataset_root {
            continue;
        }
        let path = base.extract_path(registry.clone())?;
        // A base cloned from a branch is named after it and sits at
        // `<root>/tree/<branch>`; the stamps are at that root.
        let below_root = base
            .name
            .as_deref()
            .map_or(0, |branch| 1 + branch.split('/').count());
        let count = path.parts().count();
        let root = Path::from_iter(path.parts().take(count.saturating_sub(below_root)));
        let location = root.join(SIDECAR_DIR).join(format!("{digest}.sig"));
        match dataset
            .object_store(Some(base.id))
            .await?
            .read_one_all(&location)
            .await
        {
            Ok(bytes) => return Ok(bytes),
            Err(error) if error.is_not_found() => continue,
            Err(error) => return Err(error.into()),
        }
    }
    Err(invalid(format!(
        "freshness sidecar {digest} is in neither the table's directory nor a base's"
    )))
}

/// Remove freshness sidecars that no version still present references,
/// the counterpart of lance's version cleanup for `_computed/`, with the
/// same protection for objects still being published: a sidecar is put
/// before the commit that references it, so one younger than
/// [`SIDECAR_UNVERIFIED_THRESHOLD_DAYS`] is left alone unless
/// `delete_unverified`. Returns how many were removed.
pub async fn prune_sidecars(dataset: &Dataset, delete_unverified: bool) -> Result<usize> {
    // The directory serves main and every branch, so the references are
    // collected across all of them whichever handle prunes.
    let main;
    let dataset = if dataset.manifest().branch.is_some() {
        main = dataset.checkout_version(Ref::Version(None, None)).await?;
        &main
    } else {
        dataset
    };
    let store = store(dataset).await?;
    let dir = dataset_root(dataset)?.join(SIDECAR_DIR);
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
    referenced_digests(dataset, &mut referenced).await?;
    for branch in dataset.list_branches().await?.keys() {
        referenced_digests(&dataset.checkout_branch(branch).await?, &mut referenced).await?;
    }
    let mut removed = 0;
    for digest in present {
        if !referenced.contains(&digest) {
            store.delete(&sidecar_path(dataset, &digest)?).await?;
            removed += 1;
        }
    }
    Ok(removed)
}

/// The sidecars every retained version of `dataset` references.
async fn referenced_digests(dataset: &Dataset, into: &mut HashSet<String>) -> Result<()> {
    for version in dataset.versions().await? {
        let at = dataset.checkout_version(version.version).await?;
        for field in at.schema().fields_pre_order() {
            for key in [SOURCE_SIGNATURE_META_KEY, PENDING_FILL_SIGNATURE_META_KEY] {
                if let Some(digest) = field
                    .metadata
                    .get(key)
                    .and_then(|value| value.strip_prefix(SIDECAR_REF))
                {
                    into.insert(digest.to_string());
                }
            }
        }
    }
    Ok(())
}

enum StoredFreshnessState {
    Absent,
    Present {
        state: FreshnessState,
        digest: Option<String>,
    },
    Unreadable,
}

async fn load_freshness_state(
    dataset: &Dataset,
    column: &str,
) -> Result<Option<(FreshnessState, Option<String>)>> {
    let Some(encoded) = field_meta(dataset, column, SOURCE_SIGNATURE_META_KEY) else {
        return Ok(None);
    };
    // Inline JSON is the declaration's empty seed and the pre-sidecar form.
    let read = match encoded.strip_prefix(SIDECAR_REF) {
        Some(digest) => (
            read_sidecar(dataset, digest).await,
            Some(digest.to_string()),
        ),
        None => (
            serde_json::from_str::<SignatureMap>(&encoded)
                .map(|signatures| FreshnessState {
                    signatures,
                    ..Default::default()
                })
                .map_err(|e| invalid(e.to_string())),
            None,
        ),
    };
    let (state, digest) = read;
    Ok(Some((state?, digest)))
}

async fn stored_freshness_state(dataset: &Dataset, column: &str) -> StoredFreshnessState {
    match load_freshness_state(dataset, column).await {
        Ok(Some((state, digest))) => StoredFreshnessState::Present { state, digest },
        Ok(None) => StoredFreshnessState::Absent,
        Err(error) => {
            log::warn!(
                "computed column '{column}' freshness state is unreadable ({error}); every fragment will be recomputed"
            );
            StoredFreshnessState::Unreadable
        }
    }
}

/// Read the column's stored signature map through the versioned freshness
/// state. An unreadable sidecar remains a distinct fail-closed state.
pub async fn stored_signatures(dataset: &Dataset, column: &str) -> StoredSignatures {
    match stored_freshness_state(dataset, column).await {
        StoredFreshnessState::Absent => StoredSignatures::Absent,
        StoredFreshnessState::Present { state, .. } => StoredSignatures::Present(state.signatures),
        StoredFreshnessState::Unreadable => StoredSignatures::Unreadable,
    }
}

/// Read the explicit row-level dirty state associated with the column's
/// current signature map.
pub async fn stored_row_states(dataset: &Dataset, column: &str) -> StoredRowStates {
    match stored_freshness_state(dataset, column).await {
        StoredFreshnessState::Absent => StoredRowStates::Absent,
        StoredFreshnessState::Present { state, .. } => StoredRowStates::Present(state.row_state),
        StoredFreshnessState::Unreadable => StoredRowStates::Unreadable,
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
    /// Current-signature fragments with only some rows dirty.
    pub row_dirty: RowStateMap,
    /// Retained values are current, but unassigned inserted rows need filling.
    /// These signatures become completed stamps only after a full null-fill.
    #[serde(default)]
    pub pending_fill: SignatureMap,
    /// Fragments a compaction produced from fresh ones, at their current
    /// signature: not dirty, and for the seal to record.
    pub inherited: SignatureMap,
}

impl StalenessPlan {
    pub fn is_dirty(&self, fragment_id: u32) -> bool {
        self.recompute_all || self.dirty.contains(&fragment_id)
    }

    /// Whether any retained row in the fragment is known to be non-current.
    pub fn has_dirty_rows(&self, fragment_id: u32) -> bool {
        !self.recompute_all
            && self
                .row_dirty
                .get(&fragment_id)
                .is_some_and(|rows| !rows.is_explicitly_empty())
    }

    /// Whether the fragment requires any recomputation, whole or row-level.
    pub fn needs_recompute(&self, fragment_id: u32) -> bool {
        self.is_dirty(fragment_id) || self.has_dirty_rows(fragment_id)
    }
}

/// Strict, internally consistent evidence for a Function-column status read.
///
/// Unlike [`staleness_against`], this value never converts an unreadable
/// sidecar into a conservative recompute plan. Failure coverage may be
/// partial: the caller combines it with live-row freshness and value evidence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StrictFreshness {
    /// No freshness certificate is recorded on this dataset version.
    ///
    /// This does not prove that no refresh was ever attempted; older failed
    /// attempts may be untracked. Assignment and value evidence still decide
    /// stale versus never computed.
    Absent,
    /// The sidecar predates failure evidence. Its staleness plan is still
    /// usable; row classification may use freshness, values, and assignment
    /// evidence outside this sidecar.
    Uncertified { staleness: StalenessPlan },
    /// Exact staleness and current-generation failed rows.
    Certified {
        staleness: StalenessPlan,
        failures: FailureEvidence,
    },
}

fn incomplete_certificate(column: &str, reason: impl fmt::Display) -> Error {
    Error::Runtime {
        message: format!(
            "computed column '{column}' has no complete freshness certificate: {reason}"
        ),
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
    let transaction = match dataset.read_transaction_by_version(version).await {
        Ok(Some(transaction)) => transaction,
        // A branch or clone holds no manifest before the version it was
        // taken from; what happened there is unknown, and unknown is stale.
        Ok(None) | Err(lance_core::Error::DatasetNotFound { .. }) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let groups = match &transaction.operation {
        Operation::Append { .. } => return Ok(Some(Step::Append)),
        Operation::Rewrite { groups, .. } => groups,
        _ => return Ok(None),
    };
    let at = dataset.checkout_version(version).await?;
    let bases = Bases::of(&at)?;
    let identity = |file: &DataFile| -> Result<(String, String)> {
        Ok((bases.location(file.base_id)?.to_string(), file.path.clone()))
    };
    let mut by_file = BTreeMap::new();
    for fragment in at.get_fragments() {
        for file in &fragment.metadata().files {
            by_file.insert(identity(file)?, fragment.id() as u32);
        }
    }
    let compactions = groups
        .iter()
        .map(|group| {
            let consumed = group.old_fragments.iter().map(|f| f.id as u32).collect();
            let produced = group
                .new_fragments
                .iter()
                .map(|fragment| {
                    let mut found = None;
                    for file in &fragment.files {
                        if let Some(id) = by_file.get(&identity(file)?) {
                            found = Some(*id);
                            break;
                        }
                    }
                    found.ok_or_else(|| {
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
#[derive(Default)]
struct CarriedForward {
    inherited: SignatureMap,
    dirty_products: HashSet<u32>,
}

async fn carried_forward(
    dataset: &Dataset,
    column: &str,
    stored: &SignatureMap,
    row_state: &RowStateMap,
    inputs: &InputFields,
) -> Result<CarriedForward> {
    let Some(recorded_at) = field_meta(dataset, column, RECORDED_AT_VERSION_META_KEY)
        .and_then(|version| version.parse::<u64>().ok())
    else {
        return Ok(CarriedForward::default());
    };
    let to = dataset.version().version;
    if to <= recorded_at || to - recorded_at > MAX_CARRY_FORWARD_VERSIONS {
        return Ok(CarriedForward::default());
    }
    let mut fresh = stored.clone();
    let mut inherited = SignatureMap::new();
    let mut dirty_products = HashSet::new();
    let mut known_dirty: HashSet<u32> = row_state.keys().copied().collect();
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
            let source_dirty = consumed.iter().any(|id| known_dirty.contains(id));
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
                    if source_dirty {
                        inherited.remove(id);
                        dirty_products.insert(*id);
                        known_dirty.insert(*id);
                    } else {
                        known_dirty.remove(id);
                    }
                }
            }
        }
    }
    Ok(CarriedForward {
        inherited,
        dirty_products,
    })
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
    let stored = match stored_freshness_state(dataset, column).await {
        StoredFreshnessState::Absent => return Ok(StalenessPlan::default()),
        StoredFreshnessState::Unreadable => {
            return Ok(StalenessPlan {
                recompute_all: true,
                ..Default::default()
            });
        }
        StoredFreshnessState::Present { state: stored, .. } => stored,
    };
    staleness_from_state(dataset, column, definition_version, inputs, stored).await
}

async fn staleness_from_state(
    dataset: &Dataset,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
    stored: FreshnessState,
) -> Result<StalenessPlan> {
    let stored_version = field_meta(dataset, column, DEFINITION_VERSION_META_KEY);
    if stored_version.is_some_and(|version| version != definition_version) {
        return Ok(StalenessPlan {
            recompute_all: true,
            ..Default::default()
        });
    }
    let FreshnessState {
        signatures: stored,
        row_state,
        ..
    } = stored;
    let unrecorded = dataset
        .get_fragments()
        .iter()
        .any(|fragment| !stored.contains_key(&(fragment.id() as u32)));
    let carried = if unrecorded {
        carried_forward(dataset, column, &stored, &row_state, inputs).await?
    } else {
        CarriedForward::default()
    };
    let mut inherited = carried.inherited;
    let mut dirty = carried.dirty_products;
    let mut row_dirty = RowStateMap::new();
    let pending = match field_meta(dataset, column, PENDING_FILL_SIGNATURE_META_KEY) {
        Some(reference) if unrecorded => {
            let state = match reference.strip_prefix(SIDECAR_REF) {
                Some(digest) => read_sidecar(dataset, digest).await.ok(),
                None => None,
            };
            state
                .filter(|state| state.row_state.is_empty() && state.basis.is_empty())
                .map(|state| state.signatures)
                .unwrap_or_default()
        }
        _ => SignatureMap::new(),
    };
    let mut pending_fill = SignatureMap::new();
    let mut live = HashSet::new();
    let bases = Bases::of(dataset)?;
    for fragment in dataset.get_fragments() {
        let id = fragment.id() as u32;
        live.insert(id);
        let current = fragment_input_signature(&bases, fragment.metadata(), inputs)?;
        if !stored.contains_key(&id) && !dirty.contains(&id) && pending.get(&id) == Some(&current) {
            pending_fill.insert(id, current);
        } else if stored.get(&id).or_else(|| inherited.get(&id)) != Some(&current) {
            dirty.insert(id);
        } else if let Some(rows) = row_state.get(&id) {
            row_dirty.insert(id, rows.clone());
        }
    }
    inherited.retain(|id, _| live.contains(id) && !dirty.contains(id));
    row_dirty.retain(|id, _| live.contains(id) && !dirty.contains(id));
    Ok(StalenessPlan {
        recompute_all: false,
        dirty,
        row_dirty,
        pending_fill,
        inherited,
    })
}

/// Read exact status evidence without the planner's conservative fallback.
///
/// A missing metadata key returns [`StrictFreshness::Absent`]. A referenced
/// sidecar that cannot be read or decoded propagates the existing error. The
/// returned plan is certified when the sidecar records a definition and can be
/// decoded. Failure coverage is returned only for live fragments whose stored
/// input signature still matches; stale evidence is omitted. The status
/// provider remains responsible for classifying uncovered live rows as fresh,
/// stale, or never computed, and for reporting an error only when a row is
/// genuinely ambiguous.
pub async fn strict_freshness_against(
    dataset: &Dataset,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
) -> Result<StrictFreshness> {
    let Some((stored, _)) = load_freshness_state(dataset, column).await? else {
        return Ok(StrictFreshness::Absent);
    };
    let Some(stored_definition) = field_meta(dataset, column, DEFINITION_VERSION_META_KEY) else {
        return Err(incomplete_certificate(
            column,
            "the recorded definition is missing",
        ));
    };
    let staleness =
        staleness_from_state(dataset, column, definition_version, inputs, stored.clone()).await?;
    if !stored.failure_evidence_recorded {
        return Ok(StrictFreshness::Uncertified { staleness });
    }
    if stored_definition != definition_version {
        return Ok(StrictFreshness::Certified {
            staleness,
            failures: FailureEvidence::default(),
        });
    }
    let mut failures = FailureEvidence::default();
    for fragment in dataset.get_fragments() {
        let fragment_id = fragment.id() as u32;
        let current = signature_of(dataset, fragment_id, inputs)?.ok_or_else(|| {
            incomplete_certificate(column, format!("fragment {fragment_id} is not live"))
        })?;
        if stored.signatures.get(&fragment_id) != Some(&current) {
            // The old generation's failure evidence no longer describes this
            // fragment. Its live rows are stale or never computed, not
            // ambiguously failed.
            continue;
        }
        if let Some(covered) = stored.failure_coverage.get(&fragment_id)
            && !covered.is_empty()
        {
            failures.known_rows.insert(fragment_id, covered.clone());
        }
        if let Some(failed) = stored.failed_rows.get(&fragment_id)
            && !failed.is_empty()
        {
            failures.failed_rows.insert(fragment_id, failed.clone());
        }
    }
    Ok(StrictFreshness::Certified {
        staleness,
        failures,
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
    record_freshness_impl(
        latest,
        pinned,
        column,
        definition_version,
        inputs,
        computed,
        None,
    )
    .await
}

/// Record signatures and an explicit current-generation failure outcome in one
/// immutable sidecar transition.
///
/// `failures.known_rows` must include only physical offsets whose final
/// outcome is known. `failures.failed_rows` is the failed subset. Rows omitted
/// from coverage remain unknown to failure classification; freshness, values,
/// and assignment evidence may still classify them.
pub async fn record_freshness_with_failures(
    latest: &mut Dataset,
    pinned: Option<(&Dataset, &StalenessPlan)>,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
    computed: SignatureMap,
    failures: &FailureEvidence,
) -> Result<FreshnessRecord> {
    record_freshness_impl(
        latest,
        pinned,
        column,
        definition_version,
        inputs,
        computed,
        Some(failures),
    )
    .await
}

async fn record_freshness_impl(
    latest: &mut Dataset,
    pinned: Option<(&Dataset, &StalenessPlan)>,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
    computed: SignatureMap,
    failures: Option<&FailureEvidence>,
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
    if entries.is_empty() && !absent && failures.is_none_or(FailureEvidence::is_empty) {
        return Ok(FreshnessRecord::default());
    }
    let fragments: Vec<u32> = entries.keys().copied().collect();
    let current = signatures_for(latest, &fragments, inputs)?;
    let verified: SignatureMap = entries
        .into_iter()
        .filter(|(fragment_id, signature)| current.get(fragment_id) == Some(signature))
        .collect();
    let recorded = verified.len();
    let verified_fragments: HashSet<u32> = verified.keys().copied().collect();
    let scoped_failures = failures
        .map(|failures| failures.for_fragments(&verified_fragments))
        .filter(|failures| !failures.is_empty());
    let version = write_signatures_impl(
        latest,
        column,
        definition_version,
        Some(inputs),
        verified,
        scoped_failures.as_ref(),
    )
    .await?;
    Ok(FreshnessRecord {
        version: Some(version),
        recorded,
        moved: fragments.len() - recorded,
    })
}

/// The source of a just-committed write whose retained Function values may
/// still be fresh. The fields must describe the actual write, including its
/// join keys; they must not be inferred from a later table snapshot.
pub enum FunctionWrite<'a> {
    /// Assignments executed by Lance's update builder.
    Update { columns: &'a [String] },
    /// The source schema and join columns executed by Lance's merge builder.
    Merge {
        source_schema: &'a ArrowSchema,
        on: &'a [String],
        /// The insertion count from the same committed merge result.
        num_inserted_rows: u64,
    },
}

impl FunctionWrite<'_> {
    fn writes_root(&self, root: &str) -> Result<bool> {
        match self {
            Self::Update { columns } => columns.iter().try_fold(false, |found, column| {
                Ok(found || field_root(column)? == root)
            }),
            Self::Merge { source_schema, .. } => Ok(source_schema.field_with_name(root).is_ok()),
        }
    }

    fn preserves_input(&self, path: &str, schema: &ArrowSchema) -> Result<bool> {
        let parts = lance_core::datatypes::parse_field_path(path)?;
        let root = field_root(path)?;
        if !self.writes_root(&root)? {
            return Ok(true);
        }
        let Self::Merge {
            source_schema, on, ..
        } = self
        else {
            return Ok(false);
        };
        if parts.len() != 1 || !on.contains(&root) {
            return Ok(false);
        }
        let target = schema.field_with_name(&root)?;
        let source = source_schema.field_with_name(&root)?;
        // Join equality must preserve the Function's exact input. In particular,
        // floating-point joins can equate different signed zeros or NaN payloads.
        // Extension types can also define equality over a decoded representation.
        Ok(source.data_type() == target.data_type()
            && !source
                .metadata()
                .contains_key(lance_arrow::ARROW_EXT_NAME_KEY)
            && !target
                .metadata()
                .contains_key(lance_arrow::ARROW_EXT_NAME_KEY)
            && matches!(
                target.data_type(),
                DataType::Boolean
                    | DataType::Int8
                    | DataType::Int16
                    | DataType::Int32
                    | DataType::Int64
                    | DataType::UInt8
                    | DataType::UInt16
                    | DataType::UInt32
                    | DataType::UInt64
                    | DataType::Utf8
                    | DataType::LargeUtf8
                    | DataType::Binary
                    | DataType::LargeBinary
                    | DataType::FixedSizeBinary(_)
            ))
    }
}

/// Preserve Function freshness after an input-preserving update or merge.
///
/// Call this on the exact dataset returned by the write, before any subsequent
/// operation. `write` must describe that write's real assignments or source.
/// The successful transaction's read version supplies the source freshness,
/// including when the write retried. Only fresh source fragments are carried;
/// partial repairs and unknown input equality remain stale. Inserted rows keep
/// NULL outputs and carry a pending-fill signature instead of a completed stamp.
///
/// All siblings are stamped in one version-fenced metadata commit. Failure is
/// an optimization miss: callers must retain the successful data-write result,
/// rather than report a failed write that a client might repeat.
///
/// ```
/// use lance::Dataset;
/// use lancedb::table::freshness::{FunctionWrite, preserve_function_freshness};
/// # async fn after_label_update(dataset: &mut Dataset) {
/// let columns = vec!["label".to_string()];
/// if let Err(error) = preserve_function_freshness(
///     dataset, FunctionWrite::Update { columns: &columns },
/// ).await {
///     log::warn!("freshness preservation skipped: {error}");
/// }
/// # }
/// ```
pub async fn preserve_function_freshness(
    dataset: &mut Dataset,
    write: FunctionWrite<'_>,
) -> Result<()> {
    let schema = ArrowSchema::from(dataset.schema());
    let bindings = function_bindings(&schema)?;
    if bindings.is_empty() {
        return Ok(());
    }
    let committed_version = dataset.version().version;
    let Some(transaction) = dataset
        .read_transaction_by_version(committed_version)
        .await?
    else {
        return Ok(());
    };
    let Operation::Update {
        removed_fragment_ids,
        updated_fragments,
        new_fragments,
        update_mode: Some(UpdateMode::RewriteRows),
        ..
    } = &transaction.operation
    else {
        return Ok(());
    };
    if new_fragments.is_empty() {
        return Ok(());
    }
    let before = dataset.checkout_version(transaction.read_version).await?;
    if before.schema().metadata.get(FUNCTION_BINDINGS_META_KEY)
        != dataset.schema().metadata.get(FUNCTION_BINDINGS_META_KEY)
    {
        return Ok(());
    }
    let before_schema = ArrowSchema::from(before.schema());
    let sources = removed_fragment_ids
        .iter()
        .copied()
        .chain(updated_fragments.iter().map(|fragment| fragment.id))
        .collect::<HashSet<_>>();
    // Commit assigns fragment IDs. Match only this transaction's files, not
    // unrelated appends that may have committed while it was being written.
    let live_fragments = dataset.get_fragments();
    let produced = new_fragments
        .iter()
        .map(|fragment| {
            live_fragments
                .iter()
                .find(|live| live.metadata().files == fragment.files)
                .map(|live| live.id() as u32)
                .ok_or_else(|| {
                    invalid("written Function fragment is missing from its commit".into())
                })
        })
        .collect::<Result<Vec<_>>>()?;
    let live = live_fragments
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect::<HashSet<_>>();
    let mut pending = Vec::new();
    for binding in bindings {
        let paths = binding
            .inputs()
            .iter()
            .map(|input| input.field_path.clone())
            .collect::<Vec<_>>();
        let mut preserved = true;
        for path in &paths {
            preserved &= write.preserves_input(path, &before_schema)?;
        }
        // Even a join-key output can supply a non-NULL value to an insert.
        // Those values were not computed under this binding.
        for name in binding
            .outputs()
            .iter()
            .map(|output| output.output_name.as_str())
            .chain(
                binding
                    .assignment()
                    .map(|assignment| assignment.output_name.as_str()),
            )
        {
            preserved &= !write.writes_root(name)?;
        }
        if !preserved {
            continue;
        }
        let old_inputs = fields_for_paths(before.schema(), &paths)?;
        let new_inputs = fields_for_paths(dataset.schema(), &paths)?;
        for output in binding.outputs() {
            let column = &output.output_name;
            let (Some(old_field), Some(new_field)) = (
                before.schema().field(column),
                dataset.schema().field(column),
            ) else {
                continue;
            };
            if old_field.metadata != new_field.metadata {
                continue;
            }
            let StoredFreshnessState::Present { mut state, .. } =
                stored_freshness_state(&before, column).await
            else {
                continue;
            };
            let stale =
                staleness_against(&before, column, binding.binding_id(), &old_inputs).await?;
            if stale.recompute_all
                || sources.iter().any(|id| {
                    before.get_fragment(*id as usize).is_none() || stale.needs_recompute(*id as u32)
                })
            {
                continue;
            }
            // Advancing recorded_at must also carry unrelated compactions
            // already proven fresh at the write's source version.
            for (id, signature) in stale.inherited {
                if signature_of(dataset, id, &new_inputs)?.as_ref() == Some(&signature) {
                    state.signatures.insert(id, signature);
                }
            }
            let needs_fill = matches!(write, FunctionWrite::Merge { num_inserted_rows, .. } if num_inserted_rows > 0)
                || sources
                    .iter()
                    .any(|id| stale.pending_fill.contains_key(&(*id as u32)));
            let mut pending_fill = stale.pending_fill;
            let produced_signatures = signatures_for(dataset, &produced, &new_inputs)?;
            if needs_fill {
                pending_fill.extend(produced_signatures);
            } else {
                state.signatures.extend(produced_signatures);
            }
            pending_fill.retain(|id, _| live.contains(id));
            state.signatures.retain(|id, _| live.contains(id));
            state.row_state.retain(|id, _| live.contains(id));
            state.basis.clear();
            let reference = write_sidecar(dataset, &state.normalize()?).await?;
            let fill_reference = if pending_fill.is_empty() {
                None
            } else {
                Some(
                    write_sidecar(
                        dataset,
                        &FreshnessState {
                            signatures: pending_fill,
                            ..Default::default()
                        },
                    )
                    .await?,
                )
            };
            pending.push((
                column.clone(),
                binding.binding_id().to_string(),
                reference,
                fill_reference,
            ));
        }
    }
    if pending.is_empty() {
        return Ok(());
    }
    let mut metadata = dataset.update_field_metadata();
    for (column, definition, reference, fill_reference) in pending {
        metadata = metadata.update(
            &column,
            [
                (SOURCE_SIGNATURE_META_KEY.to_string(), Some(reference)),
                (PENDING_FILL_SIGNATURE_META_KEY.to_string(), fill_reference),
                (DEFINITION_VERSION_META_KEY.to_string(), Some(definition)),
                (
                    RECORDED_AT_VERSION_META_KEY.to_string(),
                    Some(committed_version.to_string()),
                ),
            ],
        )?;
    }
    metadata
        .with_expected_latest_version(committed_version)
        .await?;
    Ok(())
}

/// Merge `entries` into the column's stored map and stamp the definition and
/// the version the map now describes. Merges rather than replaces: the
/// entries cover only the fragments this refresh wrote, and every fragment it
/// skipped keeps the entry an earlier one left. Entries for fragments no
/// longer in the manifest are dropped, so compaction cannot grow the map
/// without bound. When the definition changes, only the newly computed entries
/// are trusted. Returns the version the stamp landed at.
pub async fn write_signatures(
    dataset: &mut Dataset,
    column: &str,
    definition_version: &str,
    entries: SignatureMap,
) -> Result<u64> {
    write_signatures_impl(dataset, column, definition_version, None, entries, None).await
}

async fn write_signatures_impl(
    dataset: &mut Dataset,
    column: &str,
    definition_version: &str,
    inputs: Option<&InputFields>,
    entries: SignatureMap,
    failures: Option<&FailureEvidence>,
) -> Result<u64> {
    if let Some(failures) = failures {
        failures.validate()?;
        if inputs.is_none() {
            return Err(invalid(
                "failure evidence requires input fields for signature validation".to_string(),
            ));
        }
    }
    let definition_changed = field_meta(dataset, column, DEFINITION_VERSION_META_KEY)
        .is_some_and(|version| version != definition_version);
    let live: HashSet<u32> = dataset
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect();
    // An unreadable state is discarded rather than merged: nothing in it can
    // be trusted, and its fragments recompute until a later refresh records
    // them.
    let stored = stored_freshness_state(dataset, column).await;
    let state_unreadable = matches!(&stored, StoredFreshnessState::Unreadable);
    let mut state = match stored {
        StoredFreshnessState::Present { state, .. } => state,
        StoredFreshnessState::Absent | StoredFreshnessState::Unreadable => {
            FreshnessState::default()
        }
    };
    if definition_changed {
        state = FreshnessState::default();
    }
    let recomputed: HashSet<u32> = entries.keys().copied().collect();
    let signatures_changed = entries
        .iter()
        .any(|(fragment_id, signature)| state.signatures.get(fragment_id) != Some(signature));
    if state_unreadable
        && !live
            .iter()
            .all(|fragment_id| recomputed.contains(fragment_id))
    {
        return Err(invalid(
            "cannot replace an unreadable freshness state from a partial signature stamp"
                .to_string(),
        ));
    }
    // A signature entry written through this API means the fragment was
    // recomputed in full. Whole callers keep their old semantics while
    // clearing any partial retry state left by a previous publication. The
    // old failure outcome is also unknown unless the failure-aware wrapper
    // supplies it below.
    for fragment_id in &recomputed {
        state.row_state.remove(fragment_id);
    }
    state.invalidate_failure_evidence(recomputed.iter().copied());
    state.signatures.extend(
        entries
            .iter()
            .map(|(fragment_id, signature)| (*fragment_id, signature.clone())),
    );
    state
        .signatures
        .retain(|fragment_id, _| live.contains(fragment_id));
    state
        .row_state
        .retain(|fragment_id, _| live.contains(fragment_id));
    state
        .failure_coverage
        .retain(|fragment_id, _| live.contains(fragment_id));
    state
        .failed_rows
        .retain(|fragment_id, _| live.contains(fragment_id));
    if let Some(failures) = failures {
        apply_failure_evidence(
            dataset,
            inputs.expect("failure evidence requires input fields"),
            &entries,
            &mut state,
            failures,
        )
        .await?;
    }
    if state.row_state.is_empty() || state_unreadable || signatures_changed {
        state.basis.clear();
    }
    Ok(
        write_freshness_state(dataset, column, definition_version, state, false, None)
            .await?
            .version,
    )
}

struct FreshnessWrite {
    version: u64,
    intent_digest: Option<String>,
}

async fn apply_failure_evidence(
    dataset: &Dataset,
    inputs: &InputFields,
    accepted: &SignatureMap,
    state: &mut FreshnessState,
    update: &FailureEvidence,
) -> Result<()> {
    update.validate()?;
    let physical_rows: BTreeMap<u32, u64> = {
        let mut rows = BTreeMap::new();
        for fragment in dataset.get_fragments() {
            rows.insert(fragment.id() as u32, fragment.physical_rows().await? as u64);
        }
        rows
    };
    let mut applied = HashSet::new();
    for (fragment_id, covered) in &update.known_rows {
        let Some(rows) = physical_rows.get(fragment_id) else {
            continue;
        };
        let Some(current) = signature_of(dataset, *fragment_id, inputs)? else {
            continue;
        };
        // Evidence belongs to the attempt that produced this accepted
        // signature. A concurrent attempt may have stamped a newer signature
        // into `state`; that does not make this attempt's rows valid for it.
        if accepted.get(fragment_id) != Some(&current) {
            continue;
        }
        if state.signatures.get(fragment_id) != Some(&current) {
            continue;
        }
        if covered
            .max()
            .is_some_and(|offset| u64::from(offset) >= *rows)
        {
            return Err(invalid(format!(
                "failure evidence for fragment {fragment_id} exceeds its {rows} physical rows"
            )));
        }
        if let Some(failed) = state.failed_rows.get_mut(fragment_id) {
            *failed -= covered;
        }
        let coverage = state.failure_coverage.entry(*fragment_id).or_default();
        *coverage |= covered;
        applied.insert(*fragment_id);
    }
    for (fragment_id, failed) in &update.failed_rows {
        if !applied.contains(fragment_id) {
            continue;
        }
        if !update.known_rows.contains_key(fragment_id) {
            return Err(invalid(format!(
                "failed rows for fragment {fragment_id} have no coverage update"
            )));
        }
        let current = state.failed_rows.entry(*fragment_id).or_default();
        *current |= failed;
    }
    state.failed_rows.retain(|_, rows| !rows.is_empty());
    state.failure_coverage.retain(|_, rows| !rows.is_empty());
    Ok(())
}

async fn write_freshness_state(
    dataset: &mut Dataset,
    column: &str,
    definition_version: &str,
    state: FreshnessState,
    retain_empty_sidecar: bool,
    expected_latest_version: Option<u64>,
) -> Result<FreshnessWrite> {
    let state = state.normalize()?;
    let inline_empty = state.signatures.is_empty()
        && state.row_state.is_empty()
        && state.failure_coverage.is_empty()
        && state.failed_rows.is_empty()
        && state.basis.is_empty()
        && !retain_empty_sidecar;
    // The sidecar is durable before the metadata commit references it.
    // Unreferenced objects are removed by `prune_sidecars`.
    let (signature_ref, intent_digest) = if inline_empty {
        ("{}".to_string(), None)
    } else {
        let reference = write_sidecar(dataset, &state).await?;
        let digest = reference
            .strip_prefix(SIDECAR_REF)
            .expect("sidecar reference has its prefix")
            .to_string();
        (reference, Some(digest))
    };
    let describes = dataset.version().version;
    let mut updates = vec![
        (SOURCE_SIGNATURE_META_KEY.to_string(), Some(signature_ref)),
        (
            DEFINITION_VERSION_META_KEY.to_string(),
            Some(definition_version.to_string()),
        ),
        (
            RECORDED_AT_VERSION_META_KEY.to_string(),
            Some(describes.to_string()),
        ),
    ];
    // Pending-fill proofs vouch for retained outputs under the old definition.
    // A partial publication must not make them valid under its new stamp.
    if field_meta(dataset, column, DEFINITION_VERSION_META_KEY)
        .is_some_and(|version| version != definition_version)
    {
        updates.push((PENDING_FILL_SIGNATURE_META_KEY.to_string(), None));
    }
    let builder = dataset.update_field_metadata().update(column, updates)?;
    match expected_latest_version {
        Some(expected) => builder.with_expected_latest_version(expected).await?,
        None => builder.await?,
    };
    Ok(FreshnessWrite {
        version: dataset.version().version,
        intent_digest,
    })
}

/// One fragment's rows selected by an accepted refresh request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowRepair {
    /// Input signature of the request's pinned source.
    pub signature: String,
    /// Selected physical offsets. They become dirty before publication.
    pub rows: RoaringBitmap,
}

/// Durably mark every selected row dirty before its data publication.
///
/// The returned token binds the subsequent filled-row clear to the accepted
/// input basis and selected rows. This function checks the accepted
/// signatures against the latest manifest and does not retry a concurrent
/// metadata conflict with stale state.
///
/// `basis` identifies the request's accepted input snapshot. It may be reused
/// when retrying the same inputs: each successful begin records a new intent
/// identity independently of that basis.
///
/// Publication and verification belong to the caller. In this example,
/// `publish_values` writes the selected computed values and retains a receipt;
/// `verify_survival` checks that receipt against the latest snapshot, including
/// any intervening Restore. These two helpers are application-defined.
///
/// ```
/// use std::collections::BTreeMap;
/// use lance::Dataset;
/// use lancedb::Result;
/// use lancedb::table::freshness::{
///     begin_row_repair, finish_row_repair, InputFields, RowRepair, RowStateToken,
/// };
/// use roaring::RoaringBitmap;
/// # struct Publication {
/// #     filled: BTreeMap<u32, RoaringBitmap>,
/// #     skipped: BTreeMap<u32, RoaringBitmap>,
/// # }
/// # async fn publish_values(_: &mut Dataset, _: &RowStateToken) -> Result<Publication> {
/// #     unimplemented!()
/// # }
/// # async fn verify_survival(_: &Dataset, _: &Publication) -> Result<()> {
/// #     unimplemented!()
/// # }
/// async fn repair(
///     dataset: &mut Dataset,
///     inputs: &InputFields,
///     selected: &BTreeMap<u32, RowRepair>,
/// ) -> Result<u64> {
///     let token = begin_row_repair(
///         dataset, "embedding", "definition-v1", inputs,
///         "accepted-input-snapshot", selected,
///     ).await?;
///     let publication = publish_values(dataset, &token).await?;
///     dataset.checkout_latest().await?;
///     verify_survival(dataset, &publication).await?;
///     let verified_version = dataset.version().version;
///     finish_row_repair(
///         dataset, "embedding", "definition-v1", inputs, &token,
///         &publication.filled, &publication.skipped, Some(verified_version),
///     ).await
/// }
/// ```
///
/// `verified_version` is the snapshot just checked for publication survival,
/// not the version returned by begin or the original publication. Passing it
/// to [`finish_row_repair`] prevents a later version change from silently
/// rebasing the clean-row update. `None` omits that version fence.
pub async fn begin_row_repair(
    dataset: &mut Dataset,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
    basis: impl Into<String>,
    repairs: &BTreeMap<u32, RowRepair>,
) -> Result<RowStateToken> {
    let basis = basis.into();
    if basis.is_empty() {
        return Err(invalid("row repair basis must not be empty".to_string()));
    }
    if repairs.is_empty() {
        return Err(invalid("row repair has no fragments".to_string()));
    }
    for repair in repairs.values() {
        if !is_signature_hex(&repair.signature) {
            return Err(invalid(
                "row repair signature is not a 64-bit hex digest".to_string(),
            ));
        }
    }
    dataset.checkout_latest().await?;
    let live: HashSet<u32> = dataset
        .get_fragments()
        .iter()
        .map(|fragment| fragment.id() as u32)
        .collect();
    if let Some(missing) = repairs.keys().find(|id| !live.contains(id)) {
        return Err(invalid(format!(
            "row repair names fragment {missing}, which is not live"
        )));
    }

    let stored_version = field_meta(dataset, column, DEFINITION_VERSION_META_KEY);
    let definition_changed = stored_version.is_some_and(|version| version != definition_version);
    let stored = stored_freshness_state(dataset, column).await;
    let was_absent = matches!(&stored, StoredFreshnessState::Absent);
    let state_unreadable = matches!(&stored, StoredFreshnessState::Unreadable);
    let mut freshness = match stored {
        StoredFreshnessState::Present { state, .. } => state,
        StoredFreshnessState::Absent | StoredFreshnessState::Unreadable => {
            FreshnessState::default()
        }
    };

    if was_absent && !state_unreadable {
        freshness.signatures =
            signatures_for(dataset, &live.iter().copied().collect::<Vec<_>>(), inputs)?;
        freshness.row_state.clear();
        freshness.failure_coverage.clear();
        freshness.failed_rows.clear();
        freshness.basis.clear();
    }
    if state_unreadable || definition_changed {
        freshness.signatures =
            signatures_for(dataset, &live.iter().copied().collect::<Vec<_>>(), inputs)?;
        freshness.row_state = live
            .iter()
            .map(|id| (*id, DirtyRows::AllExcept(RoaringBitmap::new())))
            .collect();
        freshness.failure_coverage.clear();
        freshness.failed_rows.clear();
    }

    for (fragment_id, repair) in repairs {
        let current = signature_of(dataset, *fragment_id, inputs)?.ok_or_else(|| {
            invalid(format!(
                "row repair fragment {fragment_id} disappeared from the latest manifest"
            ))
        })?;
        if current != repair.signature {
            return Err(invalid(format!(
                "row repair fragment {fragment_id} input basis changed before dirty intent"
            )));
        }
        let preserve = !state_unreadable
            && !definition_changed
            && freshness.signatures.get(fragment_id) == Some(&current);
        if !preserve {
            freshness.invalidate_failure_evidence([*fragment_id]);
        }
        let dirty = if preserve {
            freshness
                .row_state
                .entry(*fragment_id)
                .or_insert_with(|| DirtyRows::Rows(RoaringBitmap::new()))
        } else {
            freshness
                .row_state
                .insert(*fragment_id, DirtyRows::AllExcept(RoaringBitmap::new()));
            freshness
                .row_state
                .get_mut(fragment_id)
                .expect("row state was just inserted")
        };
        dirty.mark_dirty(&repair.rows);
        freshness.signatures.insert(*fragment_id, current);
    }
    freshness
        .signatures
        .retain(|fragment_id, _| live.contains(fragment_id));
    freshness
        .row_state
        .retain(|fragment_id, _| live.contains(fragment_id));
    freshness
        .failure_coverage
        .retain(|fragment_id, _| live.contains(fragment_id));
    freshness
        .failed_rows
        .retain(|fragment_id, _| live.contains(fragment_id));
    freshness.basis = basis.clone();
    freshness.intent_id = Uuid::new_v4();

    let write =
        write_freshness_state(dataset, column, definition_version, freshness, true, None).await?;
    let intent_digest = write
        .intent_digest
        .ok_or_else(|| invalid("row repair dirty intent produced no sidecar".to_string()))?;
    Ok(RowStateToken {
        basis,
        intent_digest,
        selected_rows: repairs
            .iter()
            .map(|(fragment_id, repair)| (*fragment_id, repair.rows.clone()))
            .collect(),
    })
}

fn apply_row_repair(
    rows: &mut RowStateMap,
    selections: &BTreeMap<u32, RoaringBitmap>,
    filled_rows: &BTreeMap<u32, RoaringBitmap>,
    skipped_rows: &BTreeMap<u32, RoaringBitmap>,
) -> Result<()> {
    if filled_rows
        .keys()
        .chain(skipped_rows.keys())
        .any(|fragment_id| !selections.contains_key(fragment_id))
    {
        return Err(invalid(
            "row repair result names a fragment outside its token".to_string(),
        ));
    }
    for (fragment_id, selected) in selections {
        if filled_rows
            .get(fragment_id)
            .zip(skipped_rows.get(fragment_id))
            .is_some_and(|(filled, skipped)| !(filled & skipped).is_empty())
        {
            return Err(invalid(format!(
                "row repair fragment {fragment_id} marks a row both filled and skipped"
            )));
        }
        if filled_rows
            .get(fragment_id)
            .is_some_and(|filled| !filled.is_subset(selected))
            || skipped_rows
                .get(fragment_id)
                .is_some_and(|skipped| !skipped.is_subset(selected))
        {
            return Err(invalid(format!(
                "row repair fragment {fragment_id} changes a row outside its selected set"
            )));
        }
        let state = rows
            .entry(*fragment_id)
            .or_insert_with(|| DirtyRows::Rows(RoaringBitmap::new()));
        if let Some(filled) = filled_rows.get(fragment_id) {
            state.mark_clean(filled);
        }
        if let Some(skipped) = skipped_rows.get(fragment_id) {
            state.mark_dirty(skipped);
        }
    }
    rows.retain(|_, state| !state.is_explicitly_empty());
    Ok(())
}

/// Clear rows whose publication was accepted and keep every skipped row dirty.
///
/// The token names the accepted input basis and the exact rows it may change.
/// One accepted wave can finalize disjoint fragments together, and later
/// serialized waves preserve clean rows from earlier accepted waves. A newer
/// dirty intent fences a stale non-noop finalization; exact replay of the
/// accepted transition is idempotent. This API deliberately does not invent a
/// second per-row lease.
///
/// `verified_version` is the latest version whose publication survival the
/// caller verified. With `Some`, finalization commits field metadata
/// conditionally from that exact version, so a Restore before or during the
/// commit fails instead of being rebased underneath the verified publication;
/// with `None`, it uses ordinary metadata-commit semantics and provides no
/// version fence.
#[allow(clippy::too_many_arguments)]
pub async fn finish_row_repair(
    dataset: &mut Dataset,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
    token: &RowStateToken,
    filled_rows: &BTreeMap<u32, RoaringBitmap>,
    skipped_rows: &BTreeMap<u32, RoaringBitmap>,
    verified_version: Option<u64>,
) -> Result<u64> {
    finish_row_repair_impl(
        dataset,
        column,
        definition_version,
        inputs,
        token,
        filled_rows,
        skipped_rows,
        None,
        verified_version,
    )
    .await
}

/// Finalize a row repair together with the exact failure evidence observed for
/// those rows.
///
/// `failures.known_rows` must be a subset of the rows assigned either a
/// value or an explicit skipped outcome. This permits a deferral to become
/// known non-failed without turning it into a failure. `failed_rows` must be a
/// subset of `known_rows`. The same token, input-signature and version
/// fences as [`finish_row_repair`] apply.
#[allow(clippy::too_many_arguments)]
pub async fn finish_row_repair_with_failures(
    dataset: &mut Dataset,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
    token: &RowStateToken,
    filled_rows: &BTreeMap<u32, RoaringBitmap>,
    skipped_rows: &BTreeMap<u32, RoaringBitmap>,
    failures: &FailureEvidence,
    verified_version: Option<u64>,
) -> Result<u64> {
    finish_row_repair_impl(
        dataset,
        column,
        definition_version,
        inputs,
        token,
        filled_rows,
        skipped_rows,
        Some(failures),
        verified_version,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn finish_row_repair_impl(
    dataset: &mut Dataset,
    column: &str,
    definition_version: &str,
    inputs: &InputFields,
    token: &RowStateToken,
    filled_rows: &BTreeMap<u32, RoaringBitmap>,
    skipped_rows: &BTreeMap<u32, RoaringBitmap>,
    failures: Option<&FailureEvidence>,
    verified_version: Option<u64>,
) -> Result<u64> {
    if !is_sha256_hex(&token.intent_digest) {
        return Err(invalid("row repair token is malformed".to_string()));
    }
    if filled_rows
        .keys()
        .chain(skipped_rows.keys())
        .any(|fragment_id| !token.selected_rows.contains_key(fragment_id))
    {
        return Err(invalid(
            "row repair result names a fragment outside its token".to_string(),
        ));
    }
    if let Some(failures) = failures {
        failures.validate()?;
        for (fragment_id, covered) in &failures.known_rows {
            let selected = token.selected_rows.get(fragment_id).ok_or_else(|| {
                invalid(format!(
                    "failure evidence names fragment {fragment_id} outside its token"
                ))
            })?;
            let mut known = filled_rows.get(fragment_id).cloned().unwrap_or_default();
            if let Some(skipped) = skipped_rows.get(fragment_id) {
                known |= skipped;
            }
            if !covered.is_subset(selected) || !covered.is_subset(&known) {
                return Err(invalid(format!(
                    "failure evidence for fragment {fragment_id} covers rows without a final outcome"
                )));
            }
        }
        for (fragment_id, failed) in &failures.failed_rows {
            if !failures.known_rows.contains_key(fragment_id)
                || skipped_rows
                    .get(fragment_id)
                    .is_none_or(|skipped| !failed.is_subset(skipped))
            {
                return Err(invalid(format!(
                    "failed rows for fragment {fragment_id} must be an explicit skipped subset"
                )));
            }
        }
    }
    dataset.checkout_latest().await?;
    if verified_version.is_some_and(|version| dataset.version().version != version) {
        return Err(invalid(
            "row repair latest version changed after publication verification".to_string(),
        ));
    }
    let (mut state, current_digest) = match stored_freshness_state(dataset, column).await {
        StoredFreshnessState::Present {
            state,
            digest: Some(digest),
        } => (state, digest),
        StoredFreshnessState::Present { digest: None, .. } => {
            return Err(invalid(
                "row repair token cannot name an inline freshness state".to_string(),
            ));
        }
        StoredFreshnessState::Absent | StoredFreshnessState::Unreadable => {
            return Err(invalid(
                "row repair freshness state disappeared before finalization".to_string(),
            ));
        }
    };
    if state.basis != token.basis {
        return Err(invalid(
            "row repair token does not match the current dirty intent".to_string(),
        ));
    }
    for fragment_id in token.selected_rows.keys() {
        let expected = state.signatures.get(fragment_id).ok_or_else(|| {
            invalid(format!(
                "row repair fragment {fragment_id} lost its signature"
            ))
        })?;
        let current = signature_of(dataset, *fragment_id, inputs)?.ok_or_else(|| {
            invalid(format!(
                "row repair fragment {fragment_id} disappeared before finalization"
            ))
        })?;
        if expected != &current {
            return Err(invalid(format!(
                "row repair fragment {fragment_id} input basis changed before finalization"
            )));
        }
    }
    let before = state.clone();
    state.invalidate_failure_rows(&token.selected_rows);
    apply_row_repair(
        &mut state.row_state,
        &token.selected_rows,
        filled_rows,
        skipped_rows,
    )?;
    if let Some(failures) = failures {
        let accepted = token
            .selected_rows
            .keys()
            .map(|fragment_id| {
                state
                    .signatures
                    .get(fragment_id)
                    .cloned()
                    .map(|signature| (*fragment_id, signature))
            })
            .collect::<Option<SignatureMap>>()
            .ok_or_else(|| {
                invalid("row repair failure evidence lost an accepted signature".to_string())
            })?;
        apply_failure_evidence(dataset, inputs, &accepted, &mut state, failures).await?;
    }
    if state == before {
        return Ok(dataset.version().version);
    }
    if current_digest != token.intent_digest.as_str() {
        return Err(invalid(
            "row repair dirty intent changed before finalization".to_string(),
        ));
    }
    state.basis = token.basis.clone();
    let write = write_freshness_state(
        dataset,
        column,
        definition_version,
        state,
        true,
        verified_version,
    )
    .await?;
    Ok(write.version)
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

    async fn fresh_function_table() -> crate::Table {
        use crate::table::computed_columns::test_support::{stamp_bindings, text_features_binding};

        let batch = arrow_array::record_batch!(
            ("title", Utf8, ["a", "b", "c", "d"]),
            ("body", Utf8, [Some("x"), None, Some("z"), Some("w")]),
            ("search_text", Utf8, ["a x", "b", "c z", "d w"]),
            ("search_token_count", Int64, [2, 1, 2, 2]),
            ("spare", Int32, [0, 0, 0, 0])
        )
        .unwrap();
        let table = crate::connect("memory://")
            .execute()
            .await
            .unwrap()
            .create_table("fresh_functions", batch)
            .write_options(crate::table::WriteOptions {
                lance_write_params: Some(WriteParams {
                    max_rows_per_file: 2,
                    ..Default::default()
                }),
            })
            .execute()
            .await
            .unwrap();
        let binding = text_features_binding("");
        stamp_bindings(&table, std::slice::from_ref(&binding)).await;
        let native = table.as_native().unwrap();
        let mut dataset = native.dataset.get().await.unwrap().as_ref().clone();
        let inputs = fields_for_paths(dataset.schema(), &["title".into(), "body".into()]).unwrap();
        let signatures = signatures_for(&dataset, &[0, 1], &inputs).unwrap();
        for output in binding.outputs() {
            write_signatures(
                &mut dataset,
                &output.output_name,
                binding.binding_id(),
                signatures.clone(),
            )
            .await
            .unwrap();
        }
        native.dataset.update(dataset);
        table
    }

    async fn function_plan(dataset: &Dataset, column: &str) -> StalenessPlan {
        staleness_against(
            dataset,
            column,
            "fb_01K3TEXT",
            &fields_for_paths(dataset.schema(), &["title".into(), "body".into()]).unwrap(),
        )
        .await
        .unwrap()
    }

    #[rstest::rstest]
    #[case::nullable_text("text")]
    #[case::nested_text("nested")]
    #[case::blob("blob")]
    #[tokio::test]
    async fn function_write_preserves_complex_omitted_inputs(
        #[case] kind: &str,
        #[values(false, true)] merge: bool,
    ) {
        use arrow_array::{
            ArrayRef, Int32Array, Int64Array, RecordBatch, StringArray, StructArray,
        };

        use crate::table::computed_columns::test_support::{stamp_bindings, text_features_binding};

        let texts = Arc::new(StringArray::from(vec![
            Some("alpha"),
            None,
            Some(""),
            Some("delta"),
        ]));
        let (body_field, body): (ArrowField, ArrayRef) = match kind {
            "text" => (ArrowField::new("body", DataType::Utf8, true), texts),
            "nested" => {
                let fields = vec![ArrowField::new("text", DataType::Utf8, true)].into();
                let body = StructArray::new(
                    fields,
                    vec![texts],
                    Some(arrow_buffer::NullBuffer::from(vec![
                        true, false, true, true,
                    ])),
                );
                (
                    ArrowField::new("body", body.data_type().clone(), true),
                    Arc::new(body),
                )
            }
            "blob" => {
                let mut builder = lance::blob::BlobArrayBuilder::new(4);
                builder.push_bytes(b"alpha").unwrap();
                builder.push_null().unwrap();
                builder.push_bytes(b"").unwrap();
                builder.push_bytes(b"delta").unwrap();
                (
                    lance::blob::blob_field("body", true),
                    builder.finish().unwrap(),
                )
            }
            _ => unreachable!(),
        };
        let batch = RecordBatch::try_new(
            Arc::new(ArrowSchema::new(vec![
                ArrowField::new("title", DataType::Utf8, false),
                body_field.clone(),
                ArrowField::new("search_text", DataType::Utf8, true),
                ArrowField::new("search_token_count", DataType::Int64, true),
                ArrowField::new("spare", DataType::Int32, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
                body,
                Arc::new(StringArray::from(vec!["alpha", "null", "empty", "delta"])),
                Arc::new(Int64Array::from(vec![5, 0, 0, 5])),
                Arc::new(Int32Array::from(vec![0; 4])),
            ],
        )
        .unwrap();
        let table = crate::connect("memory://")
            .execute()
            .await
            .unwrap()
            .create_table("complex_function_inputs", batch)
            .write_options(crate::table::WriteOptions {
                lance_write_params: Some(WriteParams {
                    max_rows_per_file: 2,
                    data_storage_version: Some(lance_file::version::LanceFileVersion::V2_2),
                    ..Default::default()
                }),
            })
            .execute()
            .await
            .unwrap();
        let mut raw = serde_json::to_value(text_features_binding("")).unwrap();
        let json_field =
            lance_namespace::schema::arrow_schema_to_json(&ArrowSchema::new(vec![body_field]))
                .unwrap()
                .fields
                .remove(0);
        raw["input_schema"]["fields"][1] = serde_json::to_value(&json_field).unwrap();
        raw["inputs"][1]["arrow_type"] = serde_json::Value::String(match kind {
            "text" => "utf8".into(),
            "blob" => crate::function::FUNCTION_BLOB_V2_TYPE.into(),
            _ => serde_json::to_string(&json_field.r#type).unwrap(),
        });
        let binding = serde_json::from_value(raw).unwrap();
        stamp_bindings(&table, std::slice::from_ref(&binding)).await;
        let native = table.as_native().unwrap();
        let mut dataset = native.dataset.get().await.unwrap().as_ref().clone();
        let inputs = fields_for_paths(dataset.schema(), &["title".into(), "body".into()]).unwrap();
        let signatures = signatures_for(&dataset, &[0, 1], &inputs).unwrap();
        for output in binding.outputs() {
            write_signatures(
                &mut dataset,
                &output.output_name,
                binding.binding_id(),
                signatures.clone(),
            )
            .await
            .unwrap();
        }
        native.dataset.update(dataset);
        if merge {
            let source = arrow_array::record_batch!(
                ("title", Utf8, ["a", "b", "c", "d"]),
                ("spare", Int32, [1, 1, 1, 1])
            )
            .unwrap();
            let mut merge = table.merge_insert(&["title"]);
            merge.when_matched_update_all(None);
            merge
                .execute(Box::new(RecordBatchIterator::new(
                    [Ok(source.clone())],
                    source.schema(),
                )))
                .await
                .unwrap();
        } else {
            table.update().column("spare", "1").execute().await.unwrap();
        }
        let dataset = native.dataset.get().await.unwrap();
        for column in ["search_text", "search_token_count"] {
            let plan = function_plan(&dataset, column).await;
            assert!(
                !plan.recompute_all && plan.dirty.is_empty() && plan.pending_fill.is_empty(),
                "{plan:?}"
            );
        }
        let mut scan = dataset.scan();
        scan.with_row_address();
        scan.project(&["title", "search_text", "search_token_count", "body"])
            .unwrap();
        let mut actual = BTreeMap::new();
        for batch in scan
            .try_into_stream()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
        {
            let titles = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let text = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap();
            let lengths = batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                assert!(!text.is_null(row) && !lengths.is_null(row));
                let body = batch.column_by_name("body").unwrap();
                let input = if kind == "blob" {
                    let addresses = batch
                        .column_by_name(ROW_ADDR)
                        .unwrap()
                        .as_any()
                        .downcast_ref::<UInt64Array>()
                        .unwrap();
                    match dataset
                        .take_blobs_by_addresses(&[addresses.value(row)], "body")
                        .await
                        .unwrap()
                        .pop()
                        .unwrap()
                    {
                        Some(file) => Some(file.read().await.unwrap().to_vec()),
                        None => None,
                    }
                } else {
                    let leaf = if kind == "nested" {
                        body.as_any()
                            .downcast_ref::<StructArray>()
                            .unwrap()
                            .column(0)
                    } else {
                        body
                    };
                    let leaf = leaf.as_any().downcast_ref::<StringArray>().unwrap();
                    (!body.is_null(row) && !leaf.is_null(row))
                        .then(|| leaf.value(row).as_bytes().to_vec())
                };
                let expected_input = match titles.value(row) {
                    "a" => Some(b"alpha".to_vec()),
                    "b" => None,
                    "c" => Some(vec![]),
                    "d" => Some(b"delta".to_vec()),
                    _ => unreachable!(),
                };
                assert_eq!(input, expected_input);
                actual.insert(
                    titles.value(row).to_string(),
                    (text.value(row).to_string(), lengths.value(row)),
                );
            }
        }
        assert_eq!(
            actual,
            BTreeMap::from([
                ("a".into(), ("alpha".into(), 5)),
                ("b".into(), ("null".into(), 0)),
                ("c".into(), ("empty".into(), 0)),
                ("d".into(), ("delta".into(), 5)),
            ])
        );
    }

    #[rstest::rstest]
    #[case::label_update(false, 1, false)]
    #[case::label_merge(true, 1, false)]
    #[case::identical_merge(true, 0, false)]
    #[case::mixed_update_insert(true, 1, true)]
    #[tokio::test]
    async fn function_write_preserves_unchanged_inputs(
        #[case] merge: bool,
        #[case] spare: i32,
        #[case] insert: bool,
    ) {
        use crate::query::ExecutableQuery;

        let table = fresh_function_table().await;
        let version = if merge {
            let titles = if insert {
                vec!["a", "c", "e"]
            } else {
                vec!["a", "c"]
            };
            let batch = arrow_array::record_batch!(
                ("title", Utf8, titles.clone()),
                ("spare", Int32, vec![spare; titles.len()])
            )
            .unwrap();
            let mut merge = table.merge_insert(&["title"]);
            merge.when_matched_update_all(None);
            if insert {
                merge.when_not_matched_insert_all();
            }
            merge
                .execute(Box::new(RecordBatchIterator::new(
                    [Ok(batch.clone())],
                    batch.schema(),
                )))
                .await
                .unwrap()
                .version
        } else {
            table
                .update()
                .only_if("title IN ('a', 'c')")
                .column("spare", spare.to_string())
                .execute()
                .await
                .unwrap()
                .version
        };
        assert_eq!(version, table.version().await.unwrap());
        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        for column in ["search_text", "search_token_count"] {
            let stale = function_plan(&dataset, column).await;
            assert!(
                !stale.recompute_all && stale.dirty.is_empty() && stale.row_dirty.is_empty(),
                "{stale:?}"
            );
            assert_eq!(stale.pending_fill.is_empty(), !insert, "{stale:?}");
        }
        let batches = table
            .query()
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let mut values = BTreeMap::new();
        for batch in batches {
            let titles = batch
                .column_by_name("title")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            let outputs = batch
                .column_by_name("search_text")
                .unwrap()
                .as_any()
                .downcast_ref::<arrow_array::StringArray>()
                .unwrap();
            for i in 0..batch.num_rows() {
                values.insert(
                    titles.value(i).to_string(),
                    (!outputs.is_null(i)).then(|| outputs.value(i).to_string()),
                );
            }
        }
        for (title, output) in [("a", "a x"), ("b", "b"), ("c", "c z"), ("d", "d w")] {
            assert_eq!(values[title].as_deref(), Some(output));
        }
        assert_eq!(values.len(), if insert { 5 } else { 4 });
        if insert {
            assert_eq!(values["e"], None, "new rows still need Function evaluation");
            prune_sidecars(&dataset, true).await.unwrap();
            assert!(
                !function_plan(&dataset, "search_text")
                    .await
                    .pending_fill
                    .is_empty()
            );
        }
    }

    #[rstest::rstest]
    #[case::same_definition(false)]
    #[case::changed_definition(true)]
    #[tokio::test]
    async fn partial_refresh_preserves_only_current_definition(#[case] changed: bool) {
        let table = fresh_function_table().await;
        let batch =
            arrow_array::record_batch!(("title", Utf8, ["a", "e"]), ("spare", Int32, [1, 1]))
                .unwrap();
        let mut merge = table.merge_insert(&["title"]);
        merge.when_matched_update_all(None);
        merge.when_not_matched_insert_all();
        merge
            .execute(Box::new(RecordBatchIterator::new(
                [Ok(batch.clone())],
                batch.schema(),
            )))
            .await
            .unwrap();
        let mut dataset = table
            .as_native()
            .unwrap()
            .dataset
            .get()
            .await
            .unwrap()
            .as_ref()
            .clone();
        let inputs = fields_for_paths(dataset.schema(), &["title".into(), "body".into()]).unwrap();
        let before = function_plan(&dataset, "search_text").await;
        assert!(!before.pending_fill.is_empty());
        assert!(before.dirty.is_empty());
        // Refresh only the untouched fragment. Retained values elsewhere
        // still belong to the old definition, even though their inputs match.
        let definition = if changed {
            "new-function-version"
        } else {
            "fb_01K3TEXT"
        };
        let pinned = dataset.clone();
        let stale = staleness_against(&pinned, "search_text", definition, &inputs)
            .await
            .unwrap();
        let computed = signatures_for(&dataset, &[1], &inputs).unwrap();
        assert_eq!(computed.len(), 1);
        record_freshness(
            &mut dataset,
            Some((&pinned, &stale)),
            "search_text",
            definition,
            &inputs,
            computed,
        )
        .await
        .unwrap();
        let after = staleness_against(&dataset, "search_text", definition, &inputs)
            .await
            .unwrap();
        assert!(!after.is_dirty(1), "{after:?}");
        if changed {
            for fragment in dataset.get_fragments().iter().filter(|f| f.id() != 1) {
                assert!(after.is_dirty(fragment.id() as u32), "{after:?}");
            }
            assert!(after.pending_fill.is_empty(), "{after:?}");
        } else {
            assert_eq!(after.pending_fill, before.pending_fill);
            assert!(after.dirty.is_empty(), "{after:?}");
        }
    }

    #[tokio::test]
    async fn function_write_retains_pending_fill_across_another_update() {
        let table = fresh_function_table().await;
        let batch =
            arrow_array::record_batch!(("title", Utf8, ["e"]), ("spare", Int32, [1])).unwrap();
        let mut merge = table.merge_insert(&["title"]);
        merge.when_not_matched_insert_all();
        merge
            .execute(Box::new(RecordBatchIterator::new(
                [Ok(batch.clone())],
                batch.schema(),
            )))
            .await
            .unwrap();
        table
            .update()
            .only_if("title = 'e'")
            .column("spare", "2")
            .execute()
            .await
            .unwrap();
        let native = table.as_native().unwrap();
        let mut dataset = native.dataset.get().await.unwrap().as_ref().clone();
        let stale = function_plan(&dataset, "search_text").await;
        assert!(stale.dirty.is_empty(), "{stale:?}");
        assert_eq!(
            stale.pending_fill.keys().copied().collect::<Vec<_>>(),
            vec![3]
        );

        write_signatures(
            &mut dataset,
            "search_text",
            "fb_01K3TEXT",
            stale.pending_fill,
        )
        .await
        .unwrap();
        assert!(
            function_plan(&dataset, "search_text")
                .await
                .pending_fill
                .is_empty()
        );
        // An older pending-fill reference must not override a later completed
        // stamp or make a genuinely changed input fresh.
        let changed = lance::dataset::UpdateBuilder::new(Arc::new(dataset))
            .update_where("title = 'e'")
            .unwrap()
            .set("body", "'changed'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        assert!(
            function_plan(&changed.new_dataset, "search_text")
                .await
                .is_dirty(4)
        );
    }

    #[rstest::rstest]
    #[case::dirty_source("a", true)]
    #[case::unrelated_dirty_fragment("c", false)]
    #[tokio::test]
    async fn function_write_preserves_pending_repairs(
        #[case] title: &str,
        #[case] dirty_product: bool,
    ) {
        let table = fresh_function_table().await;
        let native = table.as_native().unwrap();
        let mut dataset = native.dataset.get().await.unwrap().as_ref().clone();
        let inputs = fields_for_paths(dataset.schema(), &["title".into(), "body".into()]).unwrap();
        let signature = signatures_for(&dataset, &[0], &inputs).unwrap()[&0].clone();
        begin_row_repair(
            &mut dataset,
            "search_text",
            "fb_01K3TEXT",
            &inputs,
            "pending-refresh",
            &BTreeMap::from([(
                0,
                RowRepair {
                    signature,
                    rows: RoaringBitmap::from_iter([0]),
                },
            )]),
        )
        .await
        .unwrap();
        let prior = stored_row_states(&dataset, "search_text").await;
        native.dataset.update(dataset);
        table
            .update()
            .only_if(format!("title = '{title}'"))
            .column("spare", "1")
            .execute()
            .await
            .unwrap();
        let dataset = native.dataset.get().await.unwrap();
        let stale = function_plan(&dataset, "search_text").await;
        assert_eq!(stale.is_dirty(2), dirty_product, "{stale:?}");
        assert_eq!(stored_row_states(&dataset, "search_text").await, prior);
    }

    #[rstest::rstest]
    #[case::input("body", "'changed'")]
    #[case::output("search_text", "'uncomputed'")]
    #[tokio::test]
    async fn function_write_does_not_certify_changed_values(
        #[case] column: &str,
        #[case] value: &str,
    ) {
        let table = fresh_function_table().await;
        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        let result = lance::dataset::UpdateBuilder::new(dataset)
            .update_where("title = 'a'")
            .unwrap()
            .set(column, value)
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let mut after = result.new_dataset.as_ref().clone();
        let version = after.version().version;
        preserve_function_freshness(
            &mut after,
            FunctionWrite::Update {
                columns: &[column.into()],
            },
        )
        .await
        .unwrap();
        assert_eq!(after.version().version, version);
        assert!(function_plan(&after, "search_text").await.is_dirty(2));
    }

    #[rstest::rstest]
    #[case::input_same_rows("body", "a", true)]
    #[case::input_other_rows("body", "c", false)]
    #[case::output_same_rows("search_text", "a", true)]
    #[case::output_other_rows("search_text", "c", false)]
    #[tokio::test]
    async fn function_write_preserves_prior_invalidation(
        #[case] changed_column: &str,
        #[case] title: &str,
        #[case] dirty_product: bool,
    ) {
        let table = fresh_function_table().await;
        let native = table.as_native().unwrap();
        let result = lance::dataset::UpdateBuilder::new(native.dataset.get().await.unwrap())
            .update_where("title = 'a'")
            .unwrap()
            .set(changed_column, "'changed'")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        assert!(
            function_plan(&result.new_dataset, "search_text")
                .await
                .is_dirty(2)
        );
        native.dataset.update(result.new_dataset.as_ref().clone());

        table
            .update()
            .only_if(format!("title = '{title}'"))
            .column("spare", "1")
            .execute()
            .await
            .unwrap();
        let dataset = native.dataset.get().await.unwrap();
        let stale = function_plan(&dataset, "search_text").await;
        assert_eq!(stale.is_dirty(3), dirty_product, "{stale:?}");
        if !dirty_product {
            assert!(
                stale.is_dirty(2),
                "the earlier invalidation was lost: {stale:?}"
            );
        }
    }

    #[tokio::test]
    async fn function_write_keeps_a_changed_definition_stale() {
        let table = fresh_function_table().await;
        let native = table.as_native().unwrap();
        let mut dataset = native.dataset.get().await.unwrap().as_ref().clone();
        dataset
            .update_field_metadata()
            .update(
                "search_text",
                [(DEFINITION_VERSION_META_KEY, Some("old-function-version"))],
            )
            .unwrap()
            .await
            .unwrap();
        native.dataset.update(dataset);
        table.update().column("spare", "1").execute().await.unwrap();
        assert!(
            function_plan(native.dataset.get().await.unwrap().as_ref(), "search_text")
                .await
                .recompute_all
        );
    }

    #[rstest::rstest]
    #[case::delete(false)]
    #[case::restore(true)]
    #[tokio::test]
    async fn function_write_cannot_stamp_over_a_concurrent_commit(#[case] restore: bool) {
        let table = fresh_function_table().await;
        let dataset = table.as_native().unwrap().dataset.get().await.unwrap();
        let before_version = dataset.version().version;
        let result = lance::dataset::UpdateBuilder::new(dataset)
            .update_where("title = 'a'")
            .unwrap()
            .set("spare", "1")
            .unwrap()
            .build()
            .unwrap()
            .execute()
            .await
            .unwrap();
        let mut staged = result.new_dataset.as_ref().clone();
        let mut concurrent = staged.clone();
        if restore {
            concurrent = concurrent.checkout_version(before_version).await.unwrap();
            concurrent.restore().await.unwrap();
        } else {
            concurrent.delete("title = 'd'").await.unwrap();
        }
        let current_version = concurrent.version().version;
        let before = field_meta(&concurrent, "search_text", SOURCE_SIGNATURE_META_KEY);
        assert!(
            preserve_function_freshness(
                &mut staged,
                FunctionWrite::Update {
                    columns: &["spare".into()]
                }
            )
            .await
            .is_err()
        );
        concurrent.checkout_latest().await.unwrap();
        assert_eq!(concurrent.version().version, current_version);
        assert_eq!(
            field_meta(&concurrent, "search_text", SOURCE_SIGNATURE_META_KEY),
            before
        );
    }

    #[tokio::test]
    async fn function_write_retains_inherited_compaction_freshness() {
        let table = fresh_function_table().await;
        let native = table.as_native().unwrap();
        let mut dataset = native.dataset.get().await.unwrap().as_ref().clone();
        compact(&mut dataset).await;
        assert!(
            !function_plan(&dataset, "search_text")
                .await
                .inherited
                .is_empty()
        );
        native.dataset.update(dataset);
        table
            .update()
            .only_if("title = 'a'")
            .column("spare", "1")
            .execute()
            .await
            .unwrap();
        let dataset = native.dataset.get().await.unwrap();
        let stale = function_plan(&dataset, "search_text").await;
        assert!(stale.dirty.is_empty(), "{stale:?}");
        assert_eq!(dataset.get_fragments().len(), 2);
    }

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

    async fn stamp_v2(
        dataset: &mut Dataset,
        column: &str,
        definition: &str,
        entries: SignatureMap,
    ) {
        let reference = write_sidecar_bytes(
            dataset,
            encode_v2_sidecar(&FreshnessState {
                signatures: entries,
                ..Default::default()
            })
            .unwrap(),
        )
        .await
        .unwrap();
        let describes = dataset.version().version.to_string();
        dataset
            .update_field_metadata()
            .update(
                column,
                [
                    (SOURCE_SIGNATURE_META_KEY.to_string(), reference),
                    (
                        DEFINITION_VERSION_META_KEY.to_string(),
                        definition.to_string(),
                    ),
                    (RECORDED_AT_VERSION_META_KEY.to_string(), describes),
                ],
            )
            .unwrap()
            .await
            .unwrap();
    }

    fn fragment_signature(dataset: &Dataset, fragment_id: u32) -> String {
        signature_of(dataset, fragment_id, &inputs(dataset))
            .unwrap()
            .unwrap()
    }

    fn repair(dataset: &Dataset, fragment_id: u32, rows: RoaringBitmap) -> RowRepair {
        RowRepair {
            signature: fragment_signature(dataset, fragment_id),
            rows,
        }
    }

    #[test]
    fn unified_sidecar_round_trips_partial_clean_exceptions() {
        let state = FreshnessState {
            signatures: SignatureMap::from([
                (3, "0000000000000003".to_string()),
                (7, "0000000000000007".to_string()),
            ]),
            basis: "job-generation".to_string(),
            intent_id: Uuid::from_u128(7),
            row_state: RowStateMap::from([
                (3, DirtyRows::Rows(RoaringBitmap::from_iter([1, 4, 9]))),
                (7, DirtyRows::AllExcept(RoaringBitmap::from_iter([0, 2]))),
            ]),
            failure_coverage: FailureRows::from([
                (3, RoaringBitmap::from_iter([1, 2, 4, 9])),
                (7, RoaringBitmap::from_iter([0, 1, 2, 3])),
            ]),
            failed_rows: FailureRows::from([
                (3, RoaringBitmap::from_iter([1])),
                (7, RoaringBitmap::from_iter([1, 3])),
            ]),
            failure_evidence_recorded: true,
        };
        let encoded = encode_sidecar(&state).unwrap();
        assert_eq!(encoded[4], SIDECAR_FORMAT);
        assert!(
            decode_legacy_sidecar(&encoded).is_err(),
            "an old reader must reject the row-aware format"
        );
        assert_eq!(decode_sidecar(&encoded).unwrap(), state);
    }

    #[test]
    fn legacy_sidecar_is_read_as_clean_rows() {
        let signatures = SignatureMap::from([
            (1, "0000000000000001".to_string()),
            (2, "0000000000000002".to_string()),
        ]);
        let state = decode_sidecar(&encode_legacy_sidecar(&signatures).unwrap()).unwrap();
        assert_eq!(state.signatures, signatures);
        assert!(state.row_state.is_empty());
        assert!(state.failure_coverage.is_empty());
        assert!(state.failed_rows.is_empty());
        assert!(!state.failure_evidence_recorded);
        assert!(state.basis.is_empty());
    }

    #[test]
    fn v2_sidecar_decodes_with_unknown_failure_evidence() {
        let state = FreshnessState {
            signatures: SignatureMap::from([(1, "0000000000000001".to_string())]),
            row_state: RowStateMap::from([(1, DirtyRows::Rows(RoaringBitmap::from_iter([2, 3])))]),
            basis: "legacy-intent".to_string(),
            intent_id: Uuid::from_u128(1),
            ..Default::default()
        };
        let decoded = decode_sidecar(&encode_v2_sidecar(&state).unwrap()).unwrap();
        assert_eq!(decoded, state);
        assert!(decoded.failure_coverage.is_empty());
        assert!(decoded.failed_rows.is_empty());
        assert!(!decoded.failure_evidence_recorded);
    }

    #[tokio::test]
    async fn a_first_partial_write_keeps_untouched_legacy_rows_unknown() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let entries = signatures_for(&dataset, &[0, 1], &ids).unwrap();
        stamp_v2(&mut dataset, COLUMN, &definition, entries).await;

        let StrictFreshness::Uncertified { staleness } =
            strict_freshness_against(&dataset, COLUMN, &definition, &ids)
                .await
                .unwrap()
        else {
            panic!("the v2 sidecar should yield a readable uncertified plan");
        };
        assert_eq!(staleness, StalenessPlan::default());

        let selected = RoaringBitmap::from_iter([1, 2]);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, selected.clone()))]);
        let token = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "legacy-partial",
            &repairs,
        )
        .await
        .unwrap();
        finish_row_repair_with_failures(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &token,
            &BTreeMap::from([(0, RoaringBitmap::from_iter([2]))]),
            &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            &FailureEvidence {
                known_rows: BTreeMap::from([(0, selected)]),
                failed_rows: BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            },
            None,
        )
        .await
        .unwrap();

        let StrictFreshness::Certified {
            staleness,
            failures,
        } = strict_freshness_against(&dataset, COLUMN, &definition, &ids)
            .await
            .unwrap()
        else {
            panic!("partial v3 evidence is still readable");
        };
        assert_eq!(
            staleness.row_dirty.get(&0),
            Some(&DirtyRows::Rows(RoaringBitmap::from_iter([1])))
        );
        assert_eq!(
            failures.known_rows.get(&0),
            Some(&RoaringBitmap::from_iter([1, 2])),
            "only the rows this partial publication observed are covered"
        );
        assert_eq!(
            failures.failed_rows.get(&0),
            Some(&RoaringBitmap::from_iter([1]))
        );
        assert!(
            !failures.known_rows.contains_key(&1),
            "the untouched legacy fragment remains unknown"
        );
    }

    #[tokio::test]
    async fn a_successful_retry_clears_only_its_failed_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        stamp_v2(
            &mut dataset,
            COLUMN,
            &definition,
            signatures_for(&dataset, &[0, 1], &ids).unwrap(),
        )
        .await;

        let first_selected = RoaringBitmap::from_iter([1, 2]);
        let first = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "failed-wave",
            &BTreeMap::from([(0, repair(&dataset, 0, first_selected.clone()))]),
        )
        .await
        .unwrap();
        finish_row_repair_with_failures(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &first,
            &BTreeMap::from([(0, RoaringBitmap::from_iter([2]))]),
            &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            &FailureEvidence {
                known_rows: BTreeMap::from([(0, first_selected)]),
                failed_rows: BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            },
            None,
        )
        .await
        .unwrap();

        let retry_selected = RoaringBitmap::from_iter([1]);
        let retry = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "successful-retry",
            &BTreeMap::from([(0, repair(&dataset, 0, retry_selected.clone()))]),
        )
        .await
        .unwrap();
        finish_row_repair_with_failures(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &retry,
            &BTreeMap::from([(0, retry_selected.clone())]),
            &BTreeMap::new(),
            &FailureEvidence {
                known_rows: BTreeMap::from([(0, retry_selected)]),
                failed_rows: BTreeMap::new(),
            },
            None,
        )
        .await
        .unwrap();

        let StrictFreshness::Certified { failures, .. } =
            strict_freshness_against(&dataset, COLUMN, &definition, &ids)
                .await
                .unwrap()
        else {
            panic!("status should remain readable");
        };
        assert!(!failures.failed_rows.contains_key(&0));
        assert_eq!(
            failures.known_rows.get(&0),
            Some(&RoaringBitmap::from_iter([1, 2]))
        );
    }

    #[tokio::test]
    async fn an_old_attempt_cannot_rewrite_a_newer_signature_evidence() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let old = dataset.clone();
        let old_inputs = ids.clone();
        let old_signature = signature_of(&old, 0, &old_inputs).unwrap().unwrap();

        rewrite_value(&mut dataset, 0).await;
        let new_signature = signature_of(&dataset, 0, &inputs(&dataset))
            .unwrap()
            .unwrap();
        assert_ne!(old_signature, new_signature);

        let current_entries = signatures_for(&dataset, &[0, 1], &inputs(&dataset)).unwrap();
        write_signatures(&mut dataset, COLUMN, &definition, current_entries.clone())
            .await
            .unwrap();
        let StoredFreshnessState::Present {
            state: mut current_state,
            ..
        } = stored_freshness_state(&dataset, COLUMN).await
        else {
            panic!("new attempt state");
        };
        current_state
            .failure_coverage
            .insert(0, RoaringBitmap::from_iter([3, 4]));
        current_state
            .failed_rows
            .insert(0, RoaringBitmap::from_iter([4]));
        let write = write_freshness_state(
            &mut dataset,
            COLUMN,
            &definition,
            current_state.clone(),
            false,
            None,
        )
        .await
        .unwrap();
        assert!(write.version > 0);
        let new_evidence = FailureEvidence {
            known_rows: current_state.failure_coverage.clone(),
            failed_rows: current_state.failed_rows.clone(),
        };

        let record = record_freshness_with_failures(
            &mut dataset,
            None,
            COLUMN,
            &definition,
            &old_inputs,
            SignatureMap::from([(0, old_signature)]),
            &FailureEvidence {
                known_rows: BTreeMap::from([(0, RoaringBitmap::from_iter([1, 2]))]),
                failed_rows: BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            },
        )
        .await
        .unwrap();
        assert_eq!((record.recorded, record.moved), (0, 1));

        let StrictFreshness::Certified { failures, .. } =
            strict_freshness_against(&dataset, COLUMN, &definition, &inputs(&dataset))
                .await
                .unwrap()
        else {
            panic!("current signature evidence should remain readable");
        };
        assert_eq!(failures.known_rows, new_evidence.known_rows);
        assert_eq!(failures.failed_rows, new_evidence.failed_rows);
        assert_eq!(
            failures.failed_rows.get(&0),
            Some(&RoaringBitmap::from_iter([4])),
            "old S1 failure must not replace the S2 failure"
        );
        assert!(!failures.known_rows.contains_key(&1));
    }

    #[test]
    fn failure_evidence_must_be_a_subset_of_coverage() {
        let state = FreshnessState {
            signatures: SignatureMap::from([(3, "0000000000000003".to_string())]),
            failure_coverage: FailureRows::from([(3, RoaringBitmap::from_iter([1]))]),
            failed_rows: FailureRows::from([(3, RoaringBitmap::from_iter([2]))]),
            ..Default::default()
        };
        let error = encode_sidecar(&state).unwrap_err();
        assert!(
            error
                .to_string()
                .contains("failed rows are not covered by failure evidence"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn a_row_repair_clears_filled_rows_and_retains_skipped_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1, 2])))]);
        let token = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-job-generation-a",
            &repairs,
        )
        .await
        .unwrap();

        let before = plan(&dataset).await;
        assert!(before.dirty.is_empty());
        assert_eq!(
            before.row_dirty.get(&0),
            Some(&DirtyRows::Rows(RoaringBitmap::from_iter([1, 2])))
        );

        let version = finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &token,
            &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            &BTreeMap::from([(0, RoaringBitmap::from_iter([2]))]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            stored_row_states(&dataset, COLUMN).await,
            StoredRowStates::Present(RowStateMap::from([(
                0,
                DirtyRows::Rows(RoaringBitmap::from_iter([2])),
            )]))
        );
        assert_eq!(
            plan(&dataset).await.row_dirty.get(&0),
            Some(&DirtyRows::Rows(RoaringBitmap::from_iter([2])))
        );

        // Replaying the accepted transition changes nothing.
        assert_eq!(
            finish_row_repair(
                &mut dataset,
                COLUMN,
                &definition,
                &ids,
                &token,
                &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
                &BTreeMap::from([(0, RoaringBitmap::from_iter([2]))]),
                None,
            )
            .await
            .unwrap(),
            version
        );
    }

    #[tokio::test]
    async fn a_definition_change_preserves_filled_clean_exceptions() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let ids = inputs(&dataset);
        let definition = "definition-generation-b";
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([0])))]);
        let token = begin_row_repair(
            &mut dataset,
            COLUMN,
            definition,
            &ids,
            "accepted-job-generation-b",
            &repairs,
        )
        .await
        .unwrap();
        finish_row_repair(
            &mut dataset,
            COLUMN,
            definition,
            &ids,
            &token,
            &BTreeMap::from([(0, RoaringBitmap::from_iter([0]))]),
            &BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

        let plan = staleness_against(&dataset, COLUMN, definition, &ids)
            .await
            .unwrap();
        assert!(plan.dirty.is_empty());
        assert_eq!(
            plan.row_dirty.get(&0),
            Some(&DirtyRows::AllExcept(RoaringBitmap::from_iter([0])))
        );
        assert_eq!(
            plan.row_dirty.get(&1),
            Some(&DirtyRows::AllExcept(RoaringBitmap::new()))
        );
    }

    #[tokio::test]
    async fn one_token_finishes_disjoint_fragments_without_clearing_other_rows() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([
            (0, repair(&dataset, 0, RoaringBitmap::from_iter([1]))),
            (1, repair(&dataset, 1, RoaringBitmap::from_iter([2]))),
        ]);
        let token = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &repairs,
        )
        .await
        .unwrap();
        finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &token,
            &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            &BTreeMap::from([(1, RoaringBitmap::from_iter([2]))]),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            stored_row_states(&dataset, COLUMN).await,
            StoredRowStates::Present(RowStateMap::from([(
                1,
                DirtyRows::Rows(RoaringBitmap::from_iter([2])),
            )]))
        );
    }

    #[tokio::test]
    async fn a_new_intent_blocks_a_stale_non_noop_finish() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let first_repair =
            BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        let first = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &first_repair,
        )
        .await
        .unwrap();
        let second_repair =
            BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([3])))]);
        begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &second_repair,
        )
        .await
        .unwrap();
        let before = stored_row_states(&dataset, COLUMN).await;
        let error = finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &first,
            &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            &BTreeMap::new(),
            None,
        )
        .await
        .expect_err("a newer dirty intent must fence a non-noop finish");
        assert!(
            error.to_string().contains("dirty intent changed"),
            "{error}"
        );
        assert_eq!(stored_row_states(&dataset, COLUMN).await, before);
    }

    #[tokio::test]
    async fn repeated_row_repair_intent_fences_old_completion() {
        let dir = tempfile::tempdir().unwrap();
        let uri = dir.path().to_str().unwrap();
        let mut dataset = table(uri).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        let selected = BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]);
        let first = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &repairs,
        )
        .await
        .unwrap();
        finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &first,
            &selected,
            &BTreeMap::new(),
            None,
        )
        .await
        .unwrap();

        let second = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &repairs,
        )
        .await
        .unwrap();
        let verified = dataset.version().version;
        finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &second,
            &BTreeMap::new(),
            &selected,
            Some(verified),
        )
        .await
        .unwrap();

        let expected = StoredRowStates::Present(RowStateMap::from([(
            0,
            DirtyRows::Rows(RoaringBitmap::from_iter([1])),
        )]));
        let mut reopened = Dataset::open(uri).await.unwrap();
        assert_eq!(stored_row_states(&reopened, COLUMN).await, expected);
        let verified = reopened.version().version;
        let error = finish_row_repair(
            &mut reopened,
            COLUMN,
            &definition,
            &ids,
            &first,
            &selected,
            &BTreeMap::new(),
            Some(verified),
        )
        .await
        .expect_err("an old completion must not clear a newer skipped repair of the same rows");
        assert!(
            error.to_string().contains("dirty intent changed"),
            "{error}"
        );
        assert_eq!(reopened.version().version, verified);
        let reopened = Dataset::open(uri).await.unwrap();
        assert_eq!(stored_row_states(&reopened, COLUMN).await, expected);
    }

    #[tokio::test]
    async fn recovery_rejects_a_restore_after_publication_verification() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        let token = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &repairs,
        )
        .await
        .unwrap();
        let intent_version = dataset.version().version;
        let mut publication = dataset.clone();
        publication
            .update_field_metadata()
            .update(COLUMN, [("fixture.publication", "1")])
            .unwrap()
            .await
            .unwrap();
        let verified = publication.version().version;

        let mut intent = dataset.checkout_version(intent_version).await.unwrap();
        intent.restore().await.unwrap();
        let before = stored_row_states(&dataset, COLUMN).await;
        let error = finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &token,
            &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            &BTreeMap::new(),
            Some(verified),
        )
        .await
        .expect_err("a restore after verification must fence recovery finalization");

        assert!(
            error
                .to_string()
                .contains("latest version changed after publication verification"),
            "{error}"
        );
        assert_eq!(stored_row_states(&dataset, COLUMN).await, before);
        assert_eq!(
            plan(&dataset).await.row_dirty.get(&0),
            Some(&DirtyRows::Rows(RoaringBitmap::from_iter([1]))),
            "the recovered row must remain retryable"
        );
    }

    #[tokio::test]
    async fn an_all_success_finish_replays_without_a_legacy_sidecar() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        let token = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &repairs,
        )
        .await
        .unwrap();
        let filled = BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]);
        let version = finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &token,
            &filled,
            &BTreeMap::new(),
            None,
        )
        .await
        .unwrap();
        assert_eq!(
            stored_row_states(&dataset, COLUMN).await,
            StoredRowStates::Present(RowStateMap::new()),
            "an accepted all-success transition keeps an empty freshness sidecar for replay"
        );
        assert_eq!(
            finish_row_repair(
                &mut dataset,
                COLUMN,
                &definition,
                &ids,
                &token,
                &filled,
                &BTreeMap::new(),
                None,
            )
            .await
            .unwrap(),
            version
        );

        corrupt_current_sidecar(&dataset).await;
        let error = finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &token,
            &filled,
            &BTreeMap::new(),
            None,
        )
        .await
        .expect_err("legacy absence cannot prove an accepted generation");
        assert!(
            error
                .to_string()
                .contains("disappeared before finalization"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn finish_rejects_rows_outside_the_token_selection() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        let token = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &repairs,
        )
        .await
        .unwrap();
        let before = stored_row_states(&dataset, COLUMN).await;
        let error = finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &token,
            &BTreeMap::from([(0, RoaringBitmap::from_iter([2]))]),
            &BTreeMap::new(),
            None,
        )
        .await
        .expect_err("a token must not clear rows it did not select");
        assert!(
            error
                .to_string()
                .contains("changes a row outside its selected set"),
            "{error}"
        );
        assert_eq!(stored_row_states(&dataset, COLUMN).await, before);
    }

    #[tokio::test]
    async fn unreadable_row_state_keeps_unselected_fragments_dirty() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        let stale = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &repairs,
        )
        .await
        .unwrap();
        finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &stale,
            &BTreeMap::new(),
            &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            None,
        )
        .await
        .unwrap();
        corrupt_current_sidecar(&dataset).await;

        let retry = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([2])))]);
        begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "retry-input-basis",
            &retry,
        )
        .await
        .unwrap();
        let plan = plan(&dataset).await;
        assert_eq!(
            plan.row_dirty.get(&0),
            Some(&DirtyRows::AllExcept(RoaringBitmap::new()))
        );
        assert_eq!(
            plan.row_dirty.get(&1),
            Some(&DirtyRows::AllExcept(RoaringBitmap::new())),
            "unreadable state outside the selected fragment must stay dirty"
        );
    }

    #[tokio::test]
    async fn write_signatures_fails_closed_for_unreadable_row_state() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        let stale = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &repairs,
        )
        .await
        .unwrap();
        finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &stale,
            &BTreeMap::new(),
            &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            None,
        )
        .await
        .unwrap();
        corrupt_current_sidecar(&dataset).await;

        let partial = SignatureMap::from([(0, fragment_signature(&dataset, 0))]);
        let error = write_signatures(&mut dataset, COLUMN, &definition, partial)
            .await
            .expect_err("a partial stamp cannot replace unreadable state");
        assert!(
            error.to_string().contains("partial signature stamp"),
            "{error}"
        );
        assert!(plan(&dataset).await.recompute_all);
    }

    #[tokio::test]
    async fn concurrent_field_metadata_writes_on_one_field_conflict() {
        let dir = tempfile::tempdir().unwrap();
        let dataset = table(dir.path().to_str().unwrap()).await;
        let mut left = dataset.clone();
        let mut right = dataset.clone();
        let left_write = async {
            left.update_field_metadata()
                .update(COLUMN, [("computed_refresh.concurrent_a", "1")])
                .unwrap()
                .await
        };
        let right_write = async {
            right
                .update_field_metadata()
                .update(COLUMN, [("computed_refresh.concurrent_b", "2")])
                .unwrap()
                .await
        };
        let (left_result, right_result) = tokio::join!(left_write, right_write);
        assert_eq!(
            usize::from(left_result.is_ok()) + usize::from(right_result.is_ok()),
            1,
            "same-field metadata writes must conflict: {left_result:?} {right_result:?}"
        );
    }

    #[tokio::test]
    async fn pruning_tracks_unified_freshness_sidecars() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-input-basis",
            &repairs,
        )
        .await
        .unwrap();
        assert_eq!(sidecar_names(&dataset).await.len(), 2);
        assert_eq!(prune_sidecars(&dataset, true).await.unwrap(), 0);

        dataset
            .update_field_metadata()
            .update(
                COLUMN,
                [(SOURCE_SIGNATURE_META_KEY.to_string(), "{}".to_string())],
            )
            .unwrap()
            .await
            .unwrap();
        dataset
            .cleanup_old_versions(chrono::Duration::zero(), Some(true), None)
            .await
            .unwrap();
        assert_eq!(prune_sidecars(&dataset, true).await.unwrap(), 2);
        assert!(sidecar_names(&dataset).await.is_empty());
    }

    #[tokio::test]
    async fn corrupt_unified_freshness_state_fails_closed() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-job-generation-a",
            &repairs,
        )
        .await
        .unwrap();
        corrupt_current_sidecar(&dataset).await;
        assert!(plan(&dataset).await.recompute_all);
    }

    #[tokio::test]
    async fn compaction_from_a_partially_dirty_source_recomputes_the_product() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let definition = definition_version(EXPRESSION);
        let ids = inputs(&dataset);
        let repairs = BTreeMap::from([(0, repair(&dataset, 0, RoaringBitmap::from_iter([1])))]);
        let token = begin_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            "accepted-job-generation-a",
            &repairs,
        )
        .await
        .unwrap();
        finish_row_repair(
            &mut dataset,
            COLUMN,
            &definition,
            &ids,
            &token,
            &BTreeMap::new(),
            &BTreeMap::from([(0, RoaringBitmap::from_iter([1]))]),
            None,
        )
        .await
        .unwrap();
        let product = compact(&mut dataset).await;
        let plan = plan(&dataset).await;
        assert!(plan.dirty.contains(&product), "{plan:?}");
        assert!(!plan.row_dirty.contains_key(&product), "{plan:?}");
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
        let basis = input_basis(&bases("memory://t"), &fragment, word_count).unwrap();
        assert_eq!(
            basis.files,
            vec![("memory://t/data".to_string(), "packed.lance".to_string(), 3)]
        );
    }

    fn bases(root: &str) -> Bases {
        Bases {
            own: location(root, true).unwrap(),
            registered: HashMap::new(),
        }
    }

    /// A file is identified by where its store resolves it: the table that
    /// wrote it and a clone reading it through a base agree, however the
    /// root was spelled, and a different root is a different file.
    #[test]
    fn a_file_signs_by_its_resolved_location() {
        let file = |base_id| {
            let mut fragment = Fragment::new(0);
            fragment.files.push(DataFile::new(
                "a.lance",
                vec![7],
                vec![0],
                ConcreteFileVersion::V2_2,
                None,
                base_id,
            ));
            fragment
        };
        let source = bases("/t/source/");
        let mut clone = bases("/t/clone");
        clone
            .registered
            .insert(1, location("/t/source", true).unwrap());
        let written = input_basis(&source, &file(None), &[7]).unwrap();
        assert_eq!(written, input_basis(&clone, &file(Some(1)), &[7]).unwrap());
        assert_ne!(written, input_basis(&clone, &file(None), &[7]).unwrap());
        assert_ne!(
            written,
            input_basis(&bases("/t/other"), &file(None), &[7]).unwrap()
        );
        assert!(input_basis(&source, &file(Some(1)), &[7]).is_err());
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
            input_basis(&bases("memory://t"), &fragment, &[7]).unwrap()
        };
        assert_ne!(overlay(0), overlay(1));
        assert_eq!(overlay(0), overlay(0));
    }

    async fn sidecar_names(dataset: &Dataset) -> Vec<String> {
        let mut names = store(dataset)
            .await
            .unwrap()
            .read_dir(dataset_root(dataset).unwrap().join(SIDECAR_DIR))
            .await
            .unwrap_or_default();
        names.sort();
        names
    }

    fn signature_ref(dataset: &Dataset, column: &str) -> String {
        field_meta(dataset, column, SOURCE_SIGNATURE_META_KEY).unwrap()
    }

    async fn corrupt_current_sidecar(dataset: &Dataset) {
        let digest = signature_ref(dataset, COLUMN)
            .strip_prefix(SIDECAR_REF)
            .unwrap()
            .to_string();
        store(dataset)
            .await
            .unwrap()
            .put(&sidecar_path(dataset, &digest).unwrap(), b"CSIG garbage")
            .await
            .unwrap();
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
        let path = sidecar_path(&dataset, &digest).unwrap();
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
            .put(&sidecar_path(&dataset, "orphan").unwrap(), b"CSIG")
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
        let reference = write_sidecar(
            &dataset,
            &FreshnessState {
                signatures: entries,
                ..Default::default()
            },
        )
        .await
        .unwrap();

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

        let orphan = write_sidecar(
            &dataset,
            &FreshnessState {
                signatures: SignatureMap::from([(9, "0".repeat(16))]),
                ..Default::default()
            },
        )
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
    async fn names_under(dataset: &Dataset, dir: Path) -> Vec<String> {
        let mut names = store(dataset)
            .await
            .unwrap()
            .read_dir(dir.join(SIDECAR_DIR))
            .await
            .unwrap_or_default();
        names.sort();
        names
    }

    /// A branch starts with main's freshness, and its stamps join the
    /// table's one sidecar directory rather than the branch's tree.
    #[tokio::test]
    async fn a_branch_starts_with_mains_freshness() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let mut branch = dataset
            .create_branch("exp", dataset.version().version, None)
            .await
            .unwrap();
        assert_eq!(plan(&branch).await, StalenessPlan::default());
        rewrite_value(&mut branch, 3).await;
        assert_eq!(plan(&branch).await.dirty, HashSet::from([0]));
        stamp_all(&mut branch).await;
        assert_eq!(plan(&branch).await, StalenessPlan::default());
        assert_eq!(sidecar_names(&dataset).await.len(), 2);
        assert!(
            names_under(&branch, branch.branch_location().path)
                .await
                .is_empty()
        );
        assert_eq!(plan(&dataset).await, StalenessPlan::default());
    }

    /// A branch taken after an append main never stamped: the versions
    /// before the branch are not in its tree, so the walk skips them and
    /// the appended fragment is simply stale.
    #[tokio::test]
    async fn a_branch_walks_only_the_history_it_holds() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let appended = append_rows(&mut dataset, 100, 10).await;
        let mut branch = dataset
            .create_branch("exp", dataset.version().version, None)
            .await
            .unwrap();
        let staleness = plan(&branch).await;
        assert_eq!(staleness.dirty, HashSet::from([appended]));
        assert!(staleness.inherited.is_empty());
        let product = compact(&mut branch).await;
        assert_eq!(plan(&branch).await.dirty, HashSet::from([product]));
    }

    /// A shallow clone reads the map through the base it was cloned from
    /// and keeps its own copy from then on.
    #[tokio::test]
    async fn a_shallow_clone_starts_with_its_sources_freshness() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().join("source").to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let target = dir.path().join("clone");
        let mut clone = dataset
            .shallow_clone(target.to_str().unwrap(), dataset.version().version, None)
            .await
            .unwrap();
        assert_eq!(plan(&clone).await, StalenessPlan::default());
        assert_eq!(sidecar_names(&clone).await, sidecar_names(&dataset).await);
        rewrite_value(&mut clone, 3).await;
        assert_eq!(plan(&clone).await.dirty, HashSet::from([0]));
        stamp_all(&mut clone).await;
        assert_eq!(plan(&clone).await, StalenessPlan::default());
        assert_eq!(plan(&dataset).await, StalenessPlan::default());
    }

    /// A sidecar only a branch references survives a prune from either
    /// handle until the branch is gone.
    #[tokio::test]
    async fn pruning_keeps_what_a_branch_references() {
        let dir = tempfile::tempdir().unwrap();
        let mut dataset = table(dir.path().to_str().unwrap()).await;
        stamp_all(&mut dataset).await;
        let mains = signature_ref(&dataset, COLUMN);
        let mut branch = dataset
            .create_branch("exp", dataset.version().version, None)
            .await
            .unwrap();
        rewrite_value(&mut branch, 3).await;
        stamp_all(&mut branch).await;
        assert_ne!(signature_ref(&branch, COLUMN), mains);
        assert_eq!(sidecar_names(&dataset).await.len(), 2);
        assert_eq!(prune_sidecars(&dataset, true).await.unwrap(), 0);
        assert_eq!(prune_sidecars(&branch, true).await.unwrap(), 0);
        assert_eq!(plan(&branch).await, StalenessPlan::default());
        dataset.delete_branch("exp").await.unwrap();
        assert_eq!(prune_sidecars(&dataset, true).await.unwrap(), 1);
        assert_eq!(
            sidecar_names(&dataset).await,
            vec![format!("{}.sig", mains.strip_prefix(SIDECAR_REF).unwrap())]
        );
    }
}
