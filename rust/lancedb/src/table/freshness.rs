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
use std::io::{Cursor, Read};
use std::ops::Range;

use arrow_array::{Array, UInt64Array};
use futures::TryStreamExt;
use lance::Dataset;
use lance::dataset::refs::Ref;
use lance::dataset::transaction::Operation;
use lance_core::ROW_ADDR;
use lance_core::datatypes::{Field as LanceField, Schema as LanceSchema};
use lance_io::object_store::{ObjectStore, uri_to_url};
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

/// The single owning value of the freshness sidecar.
///
/// `SignatureMap` remains the public facade for callers that only need input
/// signatures. The row state and accepted basis live here so a reader can
/// never observe one half of a mixed-generation freshness record.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct FreshnessState {
    signatures: SignatureMap,
    row_state: RowStateMap,
    basis: String,
}

impl FreshnessState {
    fn normalize(mut self) -> Result<Self> {
        self.row_state.retain(|_, rows| !rows.is_explicitly_empty());
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
}

/// Opaque accepted-basis evidence returned by [`begin_row_repair`] and required by
/// [`finish_row_repair`].
///
/// The digest identifies the unified freshness state established before
/// publication, and the selected-row bitmaps bound which rows finalization
/// may change. The digest is not a per-row lease: overlapping publications
/// are settled by their data-commit witnesses, while disjoint accepted
/// repairs compose.
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
const SIDECAR_FORMAT: u8 = 2;
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

/// `CSIG`, format byte, basis, entry count, then for each fragment its id,
/// 8-byte signature, dirty-row mode and serialized bitmap. All integers are
/// little-endian. Format 1 remains readable as clean row state.
fn encode_sidecar(state: &FreshnessState) -> Result<Vec<u8>> {
    let state = state.clone().normalize()?;
    let basis_len = u16::try_from(state.basis.len())
        .map_err(|_| invalid("freshness basis exceeds u16 bytes".to_string()))?;
    let fragment_count = u32::try_from(state.signatures.len())
        .map_err(|_| invalid("too many fragments for a freshness sidecar".to_string()))?;
    let mut bytes = Vec::new();
    bytes.extend_from_slice(SIDECAR_MAGIC);
    bytes.push(SIDECAR_FORMAT);
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
        let mut encoded = Vec::with_capacity(bitmap.serialized_size());
        bitmap
            .serialize_into(&mut encoded)
            .map_err(|error| invalid(format!("encoding freshness bitmap: {error}")))?;
        let bitmap_len = u32::try_from(encoded.len())
            .map_err(|_| invalid("freshness bitmap exceeds u32 bytes".to_string()))?;
        bytes.extend_from_slice(&fragment_id.to_le_bytes());
        bytes.extend_from_slice(&hash.to_le_bytes());
        bytes.push(kind);
        bytes.extend_from_slice(&bitmap_len.to_le_bytes());
        bytes.extend_from_slice(&encoded);
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
    if &magic != SIDECAR_MAGIC || format[0] != SIDECAR_FORMAT {
        return Err(malformed());
    }
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
        let mut bitmap_len = [0u8; 4];
        cursor
            .read_exact(&mut bitmap_len)
            .map_err(|_| malformed())?;
        let bitmap_len = u32::from_le_bytes(bitmap_len) as usize;
        let mut encoded = vec![0u8; bitmap_len];
        cursor.read_exact(&mut encoded).map_err(|_| malformed())?;
        let mut bitmap_reader = Cursor::new(encoded.as_slice());
        let bitmap =
            RoaringBitmap::deserialize_from(&mut bitmap_reader).map_err(|_| malformed())?;
        if bitmap_reader.position() != bitmap_len as u64 {
            return Err(malformed());
        }
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
            if let Some(digest) = field
                .metadata
                .get(SOURCE_SIGNATURE_META_KEY)
                .and_then(|value| value.strip_prefix(SIDECAR_REF))
            {
                into.insert(digest.to_string());
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

async fn stored_freshness_state(dataset: &Dataset, column: &str) -> StoredFreshnessState {
    let Some(encoded) = field_meta(dataset, column, SOURCE_SIGNATURE_META_KEY) else {
        return StoredFreshnessState::Absent;
    };
    // Inline JSON is the declaration's empty seed and the pre-sidecar form.
    let (read, digest) = match encoded.strip_prefix(SIDECAR_REF) {
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
    match read {
        Ok(state) => StoredFreshnessState::Present { state, digest },
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
    let mut live = HashSet::new();
    let bases = Bases::of(dataset)?;
    for fragment in dataset.get_fragments() {
        let id = fragment.id() as u32;
        live.insert(id);
        let current = fragment_input_signature(&bases, fragment.metadata(), inputs)?;
        if stored.get(&id).or_else(|| inherited.get(&id)) != Some(&current) {
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
    // clearing any partial retry state left by a previous publication.
    for fragment_id in &recomputed {
        state.row_state.remove(fragment_id);
    }
    state.signatures.extend(entries);
    state
        .signatures
        .retain(|fragment_id, _| live.contains(fragment_id));
    state
        .row_state
        .retain(|fragment_id, _| live.contains(fragment_id));
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
    let updates = vec![
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
        freshness.basis.clear();
    }
    if state_unreadable || definition_changed {
        freshness.signatures =
            signatures_for(dataset, &live.iter().copied().collect::<Vec<_>>(), inputs)?;
        freshness.row_state = live
            .iter()
            .map(|id| (*id, DirtyRows::AllExcept(RoaringBitmap::new())))
            .collect();
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
    freshness.basis = basis.clone();

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
/// Finalization applies only that row-scoped delta to the latest state, so
/// disjoint accepted repairs compose. Replaying the same accepted transition
/// is idempotent. Overlapping publications are settled by their data-commit
/// witnesses; this API deliberately does not invent a second per-row lease.
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
    let before = state.row_state.clone();
    apply_row_repair(
        &mut state.row_state,
        &token.selected_rows,
        filled_rows,
        skipped_rows,
    )?;
    if state.row_state == before {
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
            row_state: RowStateMap::from([
                (3, DirtyRows::Rows(RoaringBitmap::from_iter([1, 4, 9]))),
                (7, DirtyRows::AllExcept(RoaringBitmap::from_iter([0, 2]))),
            ]),
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
        assert!(state.basis.is_empty());
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
