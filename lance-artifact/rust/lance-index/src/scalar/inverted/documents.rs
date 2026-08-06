// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Typed document-side state for partitioned FTS indices.
//!
//! Posting lists use dense, partition-local document identifiers.  This module
//! keeps that identity separate from dataset-version row addresses so scoring
//! never has to infer which value a numeric slot represents.

use std::borrow::Cow;
use std::sync::{Arc, OnceLock, Weak};

use arc_swap::ArcSwapWeak;
use arrow::buffer::ScalarBuffer;
use arrow_array::{Array, RecordBatch, UInt32Array, UInt64Array};
use lance_core::cache::{CacheKey, WeakLanceCache};
use lance_core::deepsize::DeepSizeOf;
use lance_core::utils::address::RowAddress;
use lance_core::utils::tokio::spawn_cpu;
use lance_core::{Error, ROW_ID, Result};
use lance_select::{RowAddrMask, RowAddrSelection, RowAddrTreeMap};
use object_store::path::Path;
use roaring::RoaringBitmap;
use tokio::sync::OnceCell;

use crate::scalar::{IndexReader, IndexStore, RowIdRemapper};

use super::index::{
    DocSet, NUM_TOKEN_COL, dequantize_doc_length, doc_index_storage_column,
    document_coordinate_rank, quantize_doc_length,
};

/// Schema metadata key persisted in every modern `docs.lance` partition.
pub(super) const TOTAL_TOKENS_KEY: &str = "total_tokens";

/// Dense, immutable document identity inside one FTS partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(super) struct DocId(u32);

impl DocId {
    pub(crate) fn new(value: u32) -> Self {
        Self(value)
    }

    pub(crate) fn get(self) -> u32 {
        self.0
    }

    fn as_usize(self) -> usize {
        self.0 as usize
    }
}

/// Immutable corpus statistics for one partition.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PartitionStats {
    pub(crate) num_docs: usize,
    pub(crate) total_tokens: u64,
}

/// One partition's immutable `DocId -> row address` column.
///
/// The independently weighed cache entry is the only long-lived owner. The
/// address projection keeps a weak handle and query-local guards upgrade it,
/// so cache eviction releases the column once in-flight queries finish.
#[derive(Debug)]
pub(super) struct CachedDocRowIds {
    pub(crate) row_ids: Arc<UInt64Array>,
}

impl DeepSizeOf for CachedDocRowIds {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        self.row_ids.len() * std::mem::size_of::<u64>()
    }
}

/// Cache key for one partition's [`CachedDocRowIds`].
#[derive(Debug, Clone)]
pub(super) struct DocRowIdsKey {
    pub(crate) partition_id: u64,
}

impl CacheKey for DocRowIdsKey {
    type ValueType = CachedDocRowIds;

    fn key(&self) -> Cow<'_, str> {
        format!("doc-row-ids-{}", self.partition_id).into()
    }

    fn type_name() -> &'static str {
        "DocRowIds"
    }
}

/// Exact document lengths plus the optional quantized scoring representation.
#[derive(Debug)]
pub(super) struct DocLengths {
    values: ScalarBuffer<u32>,
    total_tokens: u64,
    quantized_scoring: bool,
    norms: OnceLock<Box<[u8]>>,
}

impl DeepSizeOf for DocLengths {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.values.deep_size_of_children(context)
            + self
                .norms
                .get()
                .map(|norms| std::mem::size_of_val(norms.as_ref()))
                .unwrap_or(0)
    }
}

impl DocLengths {
    fn try_new(
        values: ScalarBuffer<u32>,
        expected_num_docs: usize,
        persisted_total_tokens: Option<u64>,
        quantized_scoring: bool,
        path: &str,
    ) -> Result<Self> {
        if values.len() != expected_num_docs {
            return Err(corrupt_docs(
                path,
                format!(
                    "{NUM_TOKEN_COL} has {} rows but the file footer reports {expected_num_docs}",
                    values.len()
                ),
            ));
        }
        let total_tokens = values.iter().try_fold(0_u64, |total, &value| {
            total
                .checked_add(u64::from(value))
                .ok_or_else(|| corrupt_docs(path, format!("{NUM_TOKEN_COL} sum overflows u64")))
        })?;
        if let Some(expected) = persisted_total_tokens
            && expected != total_tokens
        {
            return Err(corrupt_docs(
                path,
                format!(
                    "{TOTAL_TOKENS_KEY} metadata is {expected}, but {NUM_TOKEN_COL} sums to {total_tokens}"
                ),
            ));
        }
        Ok(Self {
            values,
            total_tokens,
            quantized_scoring,
            norms: OnceLock::new(),
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub(crate) fn total_tokens(&self) -> u64 {
        self.total_tokens
    }

    #[inline]
    pub(crate) fn exact(&self, doc_id: DocId) -> u32 {
        self.values[doc_id.as_usize()]
    }

    pub(crate) fn scoring_norms(&self) -> Option<&[u8]> {
        if !self.quantized_scoring {
            return None;
        }
        Some(
            self.norms
                .get_or_init(|| {
                    self.values
                        .iter()
                        .map(|&length| quantize_doc_length(length))
                        .collect()
                })
                .as_ref(),
        )
    }

    fn scoring_ready(&self) -> bool {
        !self.quantized_scoring || self.norms.get().is_some()
    }

    #[inline]
    pub(crate) fn scoring(&self, doc_id: DocId) -> u32 {
        match self.scoring_norms() {
            Some(norms) => dequantize_doc_length(norms[doc_id.as_usize()]),
            None => self.exact(doc_id),
        }
    }
}

#[derive(Debug)]
enum AddressValues {
    Shared { len: usize },
    Owned(Arc<Vec<u64>>),
}

impl AddressValues {
    fn len(&self) -> usize {
        match self {
            Self::Shared { len } => *len,
            Self::Owned(values) => values.len(),
        }
    }
}

impl DeepSizeOf for AddressValues {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::Shared { .. } => 0,
            Self::Owned(values) => values.deep_size_of_children(context),
        }
    }
}

/// A compact address-sorted view of the live DocIds in a projection.
///
/// Most newly built partitions already store addresses in DocId order, so the
/// identity variant adds no per-document memory. Remapped or otherwise
/// unsorted projections keep only a u32 permutation instead of duplicating
/// the u64 address column.
#[derive(Debug)]
enum AddressDocIdLookup {
    Identity,
    Sorted(Box<[u32]>),
}

impl DeepSizeOf for AddressDocIdLookup {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::Identity => 0,
            Self::Sorted(doc_ids) => std::mem::size_of_val(doc_ids.as_ref()),
        }
    }
}

impl AddressDocIdLookup {
    fn build(projection: &ResidentAddressProjection) -> Self {
        if projection.projection.live_docs.is_none()
            && (1..projection.len()).all(|index| {
                projection.stored_address(index - 1) <= projection.stored_address(index)
            })
        {
            return Self::Identity;
        }

        let mut doc_ids = match projection.projection.live_docs.as_ref() {
            Some(live_docs) => live_docs.iter().collect::<Vec<_>>(),
            None => (0..projection.len() as u32).collect::<Vec<_>>(),
        };
        doc_ids.sort_unstable_by_key(|&doc_id| projection.stored_address(doc_id as usize));
        Self::Sorted(doc_ids.into_boxed_slice())
    }

    fn len(&self, projection: &ResidentAddressProjection) -> usize {
        match self {
            Self::Identity => projection.len(),
            Self::Sorted(doc_ids) => doc_ids.len(),
        }
    }

    fn doc_id_at(&self, position: usize) -> u32 {
        match self {
            Self::Identity => position as u32,
            Self::Sorted(doc_ids) => doc_ids[position],
        }
    }

    fn address_at(&self, projection: &ResidentAddressProjection, position: usize) -> u64 {
        projection.stored_address(self.doc_id_at(position) as usize)
    }

    fn partition_point(
        &self,
        projection: &ResidentAddressProjection,
        mut predicate: impl FnMut(u64) -> bool,
    ) -> usize {
        let mut left = 0;
        let mut right = self.len(projection);
        while left < right {
            let middle = left + (right - left) / 2;
            if predicate(self.address_at(projection, middle)) {
                left = middle + 1;
            } else {
                right = middle;
            }
        }
        left
    }

    fn insert_address_range(
        &self,
        projection: &ResidentAddressProjection,
        start: u64,
        end: u64,
        selected: &mut RoaringBitmap,
    ) {
        let first = self.partition_point(projection, |address| address < start);
        let after_last = self.partition_point(projection, |address| address <= end);
        for position in first..after_last {
            selected.insert(self.doc_id_at(position));
        }
    }

    fn matching_doc_ids(
        &self,
        projection: &ResidentAddressProjection,
        addresses: &RowAddrTreeMap,
    ) -> RoaringBitmap {
        let mut selected = RoaringBitmap::new();
        for (&fragment_id, selection) in addresses.iter() {
            match selection {
                RowAddrSelection::Full => {
                    let start = u64::from(RowAddress::new_from_parts(fragment_id, 0));
                    let end = u64::from(RowAddress::new_from_parts(fragment_id, u32::MAX));
                    self.insert_address_range(projection, start, end, &mut selected);
                }
                RowAddrSelection::Partial(offsets) => {
                    let mut offsets = offsets.iter();
                    while let Some(range) = offsets.next_range() {
                        let start =
                            u64::from(RowAddress::new_from_parts(fragment_id, *range.start()));
                        let end = u64::from(RowAddress::new_from_parts(fragment_id, *range.end()));
                        self.insert_address_range(projection, start, end, &mut selected);
                    }
                }
            }
        }
        selected
    }

    fn visibility(
        &self,
        projection: &ResidentAddressProjection,
        mask: &RowAddrMask,
    ) -> DocVisibility {
        match mask {
            RowAddrMask::AllowList(allowed) => {
                DocVisibility::Selected(self.matching_doc_ids(projection, allowed))
            }
            RowAddrMask::BlockList(blocked) => {
                let blocked = self.matching_doc_ids(projection, blocked);
                let mut selected = projection.live_doc_ids();
                selected -= &blocked;
                DocVisibility::Selected(selected)
            }
        }
    }
}

/// Addresses projected into the dataset version that opened the index.
#[derive(Debug)]
pub(super) struct VersionAddressProjection {
    addresses: AddressValues,
    /// `None` means every slot is live.  Deleted documents retain their DocId
    /// slot but are absent from this bitmap.
    live_docs: Option<RoaringBitmap>,
    doc_ids_by_address: OnceCell<Arc<AddressDocIdLookup>>,
}

/// A query-scoped projection guard. Shared addresses remain alive only while
/// this guard or their independently weighed cache entry owns the Arrow column.
#[derive(Debug, Clone)]
pub(super) struct ResidentAddressProjection {
    projection: Arc<VersionAddressProjection>,
    addresses: ResidentAddressValues,
}

#[derive(Debug, Clone)]
enum ResidentAddressValues {
    Shared(Arc<UInt64Array>),
    Owned(Arc<Vec<u64>>),
}

impl DeepSizeOf for VersionAddressProjection {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.addresses.deep_size_of_children(context)
            + self
                .live_docs
                .as_ref()
                .map(|live| live.serialized_size())
                .unwrap_or(0)
            + self
                .doc_ids_by_address
                .get()
                .map(|lookup| lookup.deep_size_of_children(context))
                .unwrap_or(0)
    }
}

impl VersionAddressProjection {
    fn try_new(
        raw: &UInt64Array,
        expected_num_docs: usize,
        remapper: Option<&dyn RowIdRemapper>,
        path: &str,
    ) -> Result<Self> {
        if raw.len() != expected_num_docs {
            return Err(corrupt_docs(
                path,
                format!(
                    "{ROW_ID} has {} rows but the file footer reports {expected_num_docs}",
                    raw.len()
                ),
            ));
        }
        if raw.null_count() != 0 {
            return Err(corrupt_docs(path, format!("{ROW_ID} contains null values")));
        }

        let Some(remapper) = remapper else {
            return Ok(Self {
                addresses: AddressValues::Shared { len: raw.len() },
                live_docs: None,
                doc_ids_by_address: OnceCell::new(),
            });
        };

        let mut addresses = Vec::with_capacity(raw.len());
        let mut live_docs = RoaringBitmap::new();
        for (doc_id, &address) in raw.values().iter().enumerate() {
            match remapper.remap_row_id(address) {
                Some(current) => {
                    addresses.push(current);
                    live_docs.insert(doc_id as u32);
                }
                None => {
                    // The value in a dead slot is intentionally meaningless;
                    // callers must consult `live_docs` before reading it.
                    addresses.push(0);
                }
            }
        }
        Ok(Self {
            addresses: AddressValues::Owned(Arc::new(addresses)),
            live_docs: Some(live_docs),
            doc_ids_by_address: OnceCell::new(),
        })
    }

    fn resident(
        self: &Arc<Self>,
        shared_addresses: Option<Arc<UInt64Array>>,
    ) -> Option<ResidentAddressProjection> {
        match &self.addresses {
            AddressValues::Shared { len } => {
                let shared_addresses = shared_addresses?;
                debug_assert_eq!(shared_addresses.len(), *len);
                Some(ResidentAddressProjection {
                    projection: self.clone(),
                    addresses: ResidentAddressValues::Shared(shared_addresses),
                })
            }
            AddressValues::Owned(values) => Some(ResidentAddressProjection {
                projection: self.clone(),
                addresses: ResidentAddressValues::Owned(values.clone()),
            }),
        }
    }
}

impl ResidentAddressProjection {
    fn len(&self) -> usize {
        self.projection.addresses.len()
    }

    fn stored_address(&self, index: usize) -> u64 {
        match &self.addresses {
            ResidentAddressValues::Shared(values) => values.value(index),
            ResidentAddressValues::Owned(values) => values[index],
        }
    }

    pub(super) fn address(&self, doc_id: DocId) -> Option<u64> {
        if self
            .projection
            .live_docs
            .as_ref()
            .is_some_and(|live| !live.contains(doc_id.get()))
        {
            None
        } else {
            Some(self.stored_address(doc_id.as_usize()))
        }
    }

    fn live_doc_ids(&self) -> RoaringBitmap {
        self.projection
            .live_docs
            .clone()
            .unwrap_or_else(|| (0..self.len() as u32).collect())
    }

    async fn doc_ids_by_address(&self) -> Result<Arc<AddressDocIdLookup>> {
        self.projection
            .doc_ids_by_address
            .get_or_try_init(|| {
                let projection = self.clone();
                async move {
                    spawn_cpu(move || Result::Ok(Arc::new(AddressDocIdLookup::build(&projection))))
                        .await
                }
            })
            .await
            .cloned()
    }

    async fn materialize_visibility(self, mask: Arc<RowAddrMask>) -> Result<DocVisibility> {
        let lookup = self.doc_ids_by_address().await?;
        let projection = self;
        spawn_cpu(move || Result::Ok(lookup.visibility(&projection, &mask))).await
    }
}

/// Query-local selection in the partition-local DocId domain.
#[derive(Debug, Clone)]
pub(super) enum DocVisibility {
    All,
    Selected(RoaringBitmap),
    Filtered {
        projection: ResidentAddressProjection,
        mask: Arc<RowAddrMask>,
    },
}

impl DocVisibility {
    pub(crate) fn is_all(&self) -> bool {
        matches!(self, Self::All)
    }

    #[inline]
    pub(crate) fn selected(&self, doc_id: DocId) -> bool {
        match self {
            Self::All => true,
            Self::Selected(selected) => selected.contains(doc_id.get()),
            Self::Filtered { projection, mask } => projection
                .address(doc_id)
                .is_some_and(|address| mask.selected(address)),
        }
    }

    pub(crate) fn len(&self, total_docs: usize) -> usize {
        match self {
            Self::All => total_docs,
            Self::Selected(selected) => selected.len() as usize,
            Self::Filtered { .. } => total_docs,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        matches!(self, Self::Selected(selected) if selected.is_empty())
    }

    pub(crate) fn iter(&self) -> Option<impl Iterator<Item = DocId> + '_> {
        match self {
            Self::Selected(selected) => Some(selected.iter().map(DocId::new)),
            Self::All | Self::Filtered { .. } => None,
        }
    }
}

/// Modern query-side document state for one partition.
pub(super) struct PartitionDocuments {
    store: Arc<dyn IndexStore>,
    path: String,
    partition_id: u64,
    index_cache: WeakLanceCache,
    num_docs: usize,
    coordinate_rank: usize,
    persisted_total_tokens: Option<u64>,
    quantized_scoring: bool,
    remapper: Option<Arc<dyn RowIdRemapper>>,
    lengths: OnceCell<Arc<DocLengths>>,
    projection: OnceCell<Arc<VersionAddressProjection>>,
    shared_addresses: ArcSwapWeak<UInt64Array>,
    prewarm_complete: OnceCell<()>,
}

/// Load-boundary discriminator between the read-only legacy representation and
/// the typed partitioned representation.  Query code dispatches on this enum
/// once; modern scoring never receives a partial legacy [`DocSet`].
#[derive(Debug, Clone)]
pub(super) enum PartitionDocumentStore {
    Legacy(Arc<DocSet>),
    Modern(Arc<PartitionDocuments>),
}

impl DeepSizeOf for PartitionDocumentStore {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::Legacy(docs) => docs.deep_size_of_children(context),
            Self::Modern(docs) => docs.deep_size_of_children(context),
        }
    }
}

impl PartitionDocumentStore {
    pub(crate) fn len(&self) -> usize {
        match self {
            Self::Legacy(docs) => docs.len(),
            Self::Modern(docs) => docs.len(),
        }
    }

    pub(crate) fn coordinate_rank(&self) -> usize {
        match self {
            Self::Legacy(docs) => docs.coordinate_rank(),
            Self::Modern(docs) => docs.coordinate_rank(),
        }
    }

    pub(crate) fn legacy(&self) -> Option<&Arc<DocSet>> {
        match self {
            Self::Legacy(docs) => Some(docs),
            Self::Modern(_) => None,
        }
    }

    pub(crate) fn modern(&self) -> Option<&Arc<PartitionDocuments>> {
        match self {
            Self::Legacy(_) => None,
            Self::Modern(docs) => Some(docs),
        }
    }

    pub(crate) async fn stats(&self) -> Result<PartitionStats> {
        match self {
            Self::Legacy(docs) => Ok(PartitionStats {
                num_docs: docs.len(),
                total_tokens: docs.total_tokens_num(),
            }),
            Self::Modern(docs) => docs.stats().await,
        }
    }

    pub(crate) fn cached_stats(&self) -> Option<PartitionStats> {
        match self {
            Self::Legacy(docs) => Some(PartitionStats {
                num_docs: docs.len(),
                total_tokens: docs.total_tokens_num(),
            }),
            Self::Modern(docs) => docs.cached_stats(),
        }
    }

    pub(crate) async fn prewarm(&self) -> Result<()> {
        match self {
            Self::Legacy(_) => Ok(()),
            Self::Modern(docs) => docs.prewarm().await,
        }
    }

    pub(crate) fn query_ready(&self) -> bool {
        match self {
            Self::Legacy(_) => true,
            Self::Modern(docs) => docs.query_ready(),
        }
    }

    pub(crate) async fn load_build_docset(&self) -> Result<DocSet> {
        match self {
            Self::Legacy(docs) => Ok((**docs).clone()),
            Self::Modern(docs) => docs.load_build_docset().await,
        }
    }
}

impl std::fmt::Debug for PartitionDocuments {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionDocuments")
            .field("path", &self.path)
            .field("num_docs", &self.num_docs)
            .field("coordinate_rank", &self.coordinate_rank)
            .field("persisted_total_tokens", &self.persisted_total_tokens)
            .field("lengths_loaded", &self.lengths.initialized())
            .field("projection_loaded", &self.projection.initialized())
            .finish()
    }
}

impl DeepSizeOf for PartitionDocuments {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        self.lengths
            .get()
            .map(|lengths| lengths.deep_size_of_children(context))
            .unwrap_or(0)
            + self
                .projection
                .get()
                .map(|projection| projection.deep_size_of_children(context))
                .unwrap_or(0)
    }
}

impl PartitionDocuments {
    pub(crate) fn try_new(
        store: Arc<dyn IndexStore>,
        path: String,
        partition_id: u64,
        index_cache: WeakLanceCache,
        reader: &dyn IndexReader,
        remapper: Option<Arc<dyn RowIdRemapper>>,
        quantized_scoring: bool,
    ) -> Result<Self> {
        let num_docs = reader.num_rows();
        if num_docs > u32::MAX as usize {
            return Err(corrupt_docs(
                &path,
                format!("document count {num_docs} exceeds dense DocId capacity"),
            ));
        }
        let persisted_total_tokens = reader
            .schema()
            .metadata
            .get(TOTAL_TOKENS_KEY)
            .map(|value| {
                value.parse::<u64>().map_err(|error| {
                    corrupt_docs(
                        &path,
                        format!("invalid {TOTAL_TOKENS_KEY} metadata value {value:?}: {error}"),
                    )
                })
            })
            .transpose()?;
        let coordinate_rank =
            document_coordinate_rank(&arrow_schema::Schema::from(reader.schema()));
        Ok(Self {
            store,
            path,
            partition_id,
            index_cache,
            num_docs,
            coordinate_rank,
            persisted_total_tokens,
            quantized_scoring,
            remapper,
            lengths: OnceCell::new(),
            projection: OnceCell::new(),
            shared_addresses: ArcSwapWeak::from(Weak::new()),
            prewarm_complete: OnceCell::new(),
        })
    }

    pub(crate) fn len(&self) -> usize {
        self.num_docs
    }

    pub(crate) fn coordinate_rank(&self) -> usize {
        self.coordinate_rank
    }

    #[cfg(test)]
    pub(crate) fn lengths_loaded(&self) -> bool {
        self.lengths.initialized()
    }

    #[cfg(test)]
    pub(crate) fn projection_loaded(&self) -> bool {
        self.projection.initialized()
    }

    #[cfg(test)]
    pub(crate) fn address_buffer_handle(&self) -> Weak<UInt64Array> {
        self.shared_addresses.load_full()
    }

    pub(crate) fn projection_resident(&self) -> bool {
        self.resident_address_projection().is_some()
    }

    pub(crate) fn query_ready(&self) -> bool {
        self.prewarm_complete.initialized()
            && self
                .lengths
                .get()
                .is_some_and(|lengths| lengths.scoring_ready())
            && self
                .projection
                .get()
                .is_some_and(|projection| projection.doc_ids_by_address.initialized())
            && self.projection_resident()
    }

    async fn reader(&self) -> Result<Arc<dyn IndexReader>> {
        self.store.open_index_file(&self.path).await
    }

    async fn row_ids_column(&self) -> Result<Arc<UInt64Array>> {
        let store = self.store.clone();
        let path = self.path.clone();
        let num_docs = self.num_docs;
        let cached = self
            .index_cache
            .get_or_insert_with_key(
                DocRowIdsKey {
                    partition_id: self.partition_id,
                },
                || async move {
                    let reader = store.open_index_file(&path).await?;
                    let batch = reader.read_range(0..num_docs, Some(&[ROW_ID])).await?;
                    let row_ids = required_u64_column(&batch, ROW_ID, &path)?;
                    if row_ids.null_count() != 0 {
                        return Err(corrupt_docs(
                            &path,
                            format!("{ROW_ID} contains null values"),
                        ));
                    }
                    if row_ids.len() != num_docs {
                        return Err(corrupt_docs(
                            &path,
                            format!(
                                "{ROW_ID} has {} rows but the file footer reports {num_docs}",
                                row_ids.len()
                            ),
                        ));
                    }
                    Ok(CachedDocRowIds {
                        row_ids: Arc::new(row_ids.clone()),
                    })
                },
            )
            .await?;
        let row_ids = cached.row_ids.clone();
        self.shared_addresses.store(Arc::downgrade(&row_ids));
        Ok(row_ids)
    }

    fn lengths_from_batch(&self, batch: &RecordBatch) -> Result<Arc<DocLengths>> {
        let column = required_u32_column(batch, NUM_TOKEN_COL, &self.path)?;
        if column.null_count() != 0 {
            return Err(corrupt_docs(
                &self.path,
                format!("{NUM_TOKEN_COL} contains null values"),
            ));
        }
        Ok(Arc::new(DocLengths::try_new(
            column.values().clone(),
            self.num_docs,
            self.persisted_total_tokens,
            self.quantized_scoring,
            &self.path,
        )?))
    }

    pub(crate) async fn stats(&self) -> Result<PartitionStats> {
        let total_tokens = match self.persisted_total_tokens {
            Some(total_tokens) => total_tokens,
            None => self.lengths().await?.total_tokens(),
        };
        Ok(PartitionStats {
            num_docs: self.num_docs,
            total_tokens,
        })
    }

    pub(crate) fn cached_stats(&self) -> Option<PartitionStats> {
        self.persisted_total_tokens
            .or_else(|| self.lengths.get().map(|lengths| lengths.total_tokens()))
            .map(|total_tokens| PartitionStats {
                num_docs: self.num_docs,
                total_tokens,
            })
    }

    pub(crate) async fn lengths(&self) -> Result<Arc<DocLengths>> {
        self.lengths
            .get_or_try_init(|| async {
                let reader = self.reader().await?;
                let batch = reader
                    .read_range(0..self.num_docs, Some(&[NUM_TOKEN_COL]))
                    .await?;
                self.lengths_from_batch(&batch)
            })
            .await
            .cloned()
    }

    /// Return resident lengths without entering the asynchronous singleflight path.
    pub(crate) fn cached_lengths(&self) -> Option<Arc<DocLengths>> {
        self.lengths.get().cloned()
    }

    pub(super) fn resident_address_projection(&self) -> Option<ResidentAddressProjection> {
        let projection = self.projection.get()?.clone();
        let shared_addresses = match &projection.addresses {
            AddressValues::Shared { .. } => self.shared_addresses.load().upgrade(),
            AddressValues::Owned(_) => None,
        };
        projection.resident(shared_addresses)
    }

    pub(crate) async fn address_projection(&self) -> Result<ResidentAddressProjection> {
        if let Some(projection) = self.resident_address_projection() {
            return Ok(projection);
        }

        let row_ids = self.row_ids_column().await?;
        let projection = self
            .projection
            .get_or_try_init(|| async {
                Result::Ok(Arc::new(VersionAddressProjection::try_new(
                    row_ids.as_ref(),
                    self.num_docs,
                    self.remapper.as_deref(),
                    &self.path,
                )?))
            })
            .await
            .cloned()?;
        projection.resident(Some(row_ids)).ok_or_else(|| {
            Error::internal(format!(
                "address projection for {} could not bind its cache-managed ROW_ID column",
                self.path
            ))
        })
    }

    pub(crate) async fn visibility(
        &self,
        mask: Arc<RowAddrMask>,
        materialize_selected: bool,
    ) -> Result<DocVisibility> {
        if let Some(visibility) = self.immediate_visibility(mask.clone(), materialize_selected) {
            return Ok(visibility);
        }

        let projection = self.address_projection().await?;
        if mask.is_select_all() {
            return Ok(DocVisibility::Selected(projection.live_doc_ids()));
        }
        if materialize_selected {
            projection.materialize_visibility(mask).await
        } else {
            Ok(DocVisibility::Filtered { projection, mask })
        }
    }

    /// Resolve visibility without I/O or CPU-pool work when all required state is resident.
    pub(crate) fn immediate_visibility(
        &self,
        mask: Arc<RowAddrMask>,
        materialize_selected: bool,
    ) -> Option<DocVisibility> {
        if mask.max_len() == Some(0) {
            return Some(DocVisibility::Selected(RoaringBitmap::new()));
        }
        if mask.is_select_all() && self.remapper.is_none() {
            return Some(DocVisibility::All);
        }

        let projection = self.resident_address_projection()?;
        if mask.is_select_all() {
            return Some(DocVisibility::Selected(projection.live_doc_ids()));
        }
        if materialize_selected {
            None
        } else {
            Some(DocVisibility::Filtered { projection, mask })
        }
    }

    /// Resolve final global top-k DocIds to current row addresses.
    pub(crate) async fn resolve_addresses(&self, doc_ids: &[DocId]) -> Result<Vec<u64>> {
        if doc_ids.is_empty() {
            return Ok(Vec::new());
        }
        self.validate_doc_ids(doc_ids)?;
        if let Some(projection) = self.resident_address_projection() {
            return self.resolve_projected_addresses(&projection, doc_ids);
        }
        if self.remapper.is_some() {
            let projection = self.address_projection().await?;
            return self.resolve_projected_addresses(&projection, doc_ids);
        }

        let row_ids = self.row_ids_column().await?;
        Ok(doc_ids
            .iter()
            .map(|doc_id| row_ids.value(doc_id.as_usize()))
            .collect())
    }

    /// Resolve final global top-k DocIds to their logical FTS document keys.
    pub(crate) async fn resolve_document_keys(
        &self,
        doc_ids: &[DocId],
    ) -> Result<Vec<(u64, Vec<u32>)>> {
        let row_ids = self.resolve_addresses(doc_ids).await?;
        if self.coordinate_rank == 0 {
            return Ok(row_ids
                .into_iter()
                .map(|row_id| (row_id, Vec::new()))
                .collect());
        }

        let ranges = doc_ids
            .iter()
            .map(|doc_id| {
                let index = doc_id.as_usize();
                index..index + 1
            })
            .collect::<Vec<_>>();
        let coordinate_names = (0..self.coordinate_rank)
            .map(doc_index_storage_column)
            .collect::<Vec<_>>();
        let projection = coordinate_names
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let batch = self
            .reader()
            .await?
            .read_ranges(&ranges, Some(&projection))
            .await?;
        if batch.num_rows() != row_ids.len() {
            return Err(corrupt_docs(
                &self.path,
                format!(
                    "document coordinate projection returned {} rows for {} candidates",
                    batch.num_rows(),
                    row_ids.len()
                ),
            ));
        }
        let coordinate_columns = coordinate_names
            .iter()
            .map(|name| {
                let column = required_u32_column(&batch, name, &self.path)?;
                if column.null_count() != 0 {
                    return Err(corrupt_docs(
                        &self.path,
                        format!("document coordinate column {name} contains null values"),
                    ));
                }
                Ok(column)
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(row_ids
            .into_iter()
            .enumerate()
            .map(|(index, row_id)| {
                (
                    row_id,
                    coordinate_columns
                        .iter()
                        .map(|column| column.value(index))
                        .collect(),
                )
            })
            .collect())
    }

    fn validate_doc_ids(&self, doc_ids: &[DocId]) -> Result<()> {
        for doc_id in doc_ids {
            if doc_id.as_usize() >= self.num_docs {
                return Err(corrupt_docs(
                    &self.path,
                    format!(
                        "candidate DocId {} is outside [0, {})",
                        doc_id.get(),
                        self.num_docs
                    ),
                ));
            }
        }
        Ok(())
    }

    fn resolve_projected_addresses(
        &self,
        projection: &ResidentAddressProjection,
        doc_ids: &[DocId],
    ) -> Result<Vec<u64>> {
        doc_ids
            .iter()
            .map(|&doc_id| {
                projection.address(doc_id).ok_or_else(|| {
                    corrupt_docs(
                        &self.path,
                        format!("candidate DocId {} is not live", doc_id.get()),
                    )
                })
            })
            .collect()
    }

    /// Resolve addresses synchronously when the cache-managed projection buffer
    /// is resident. The returned `None` asks the caller to use the async reload path.
    pub(crate) fn cached_row_addresses(&self, doc_ids: &[DocId]) -> Result<Option<Vec<u64>>> {
        self.validate_doc_ids(doc_ids)?;
        let Some(projection) = self.resident_address_projection() else {
            return Ok(None);
        };
        self.resolve_projected_addresses(&projection, doc_ids)
            .map(Some)
    }

    /// Estimated Arrow payload retained while loading this partition's cached
    /// row-address column.
    /// The estimate is used to cap cross-partition read concurrency; a single
    /// oversized partition is still allowed to make progress.
    pub(crate) fn estimated_address_read_bytes(&self, doc_ids: &[DocId]) -> usize {
        if doc_ids.is_empty() || self.projection_resident() {
            return 0;
        }
        self.num_docs.saturating_mul(std::mem::size_of::<u64>())
    }

    /// Materialize the build-side table for rewrite/update operations.
    pub(crate) async fn load_build_docset(&self) -> Result<DocSet> {
        DocSet::load(self.reader().await?, false, self.remapper.clone()).await
    }

    pub(crate) async fn prewarm(&self) -> Result<()> {
        self.prewarm_complete
            .get_or_try_init(|| async {
                if self.lengths.get().is_none() && self.projection.get().is_none() {
                    let reader = self.reader().await?;
                    let batch = reader
                        .read_range(0..self.num_docs, Some(&[ROW_ID, NUM_TOKEN_COL]))
                        .await?;
                    let lengths = self.lengths_from_batch(&batch)?;
                    let row_ids =
                        Arc::new(required_u64_column(&batch, ROW_ID, &self.path)?.clone());
                    if row_ids.null_count() != 0 {
                        return Err(corrupt_docs(
                            &self.path,
                            format!("{ROW_ID} contains null values"),
                        ));
                    }
                    let projection = Arc::new(VersionAddressProjection::try_new(
                        row_ids.as_ref(),
                        self.num_docs,
                        self.remapper.as_deref(),
                        &self.path,
                    )?);
                    let cached_row_ids = Arc::new(CachedDocRowIds {
                        row_ids: row_ids.clone(),
                    });
                    self.index_cache
                        .insert_with_key(
                            &DocRowIdsKey {
                                partition_id: self.partition_id,
                            },
                            cached_row_ids,
                        )
                        .await;
                    self.shared_addresses.store(Arc::downgrade(&row_ids));

                    // A concurrent single-column request may win either OnceCell.
                    // Awaiting the accessors below joins that initialization without
                    // replacing the already-published value.
                    let _ = self.lengths.set(lengths);
                    let _ = self.projection.set(projection);
                }

                let lengths = self.lengths().await?;
                spawn_cpu(move || {
                    let _ = lengths.scoring_norms();
                    Result::Ok(())
                })
                .await?;
                self.address_projection()
                    .await?
                    .doc_ids_by_address()
                    .await?;
                Result::Ok(())
            })
            .await?;
        if !self.projection_resident() {
            self.address_projection().await?;
        }
        Ok(())
    }
}

fn required_u32_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
    path: &str,
) -> Result<&'a UInt32Array> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| corrupt_docs(path, format!("required column {name} is missing")))?;
    column
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| {
            corrupt_docs(
                path,
                format!(
                    "column {name} has type {}, expected UInt32",
                    column.data_type()
                ),
            )
        })
}

fn required_u64_column<'a>(
    batch: &'a RecordBatch,
    name: &str,
    path: &str,
) -> Result<&'a UInt64Array> {
    let column = batch
        .column_by_name(name)
        .ok_or_else(|| corrupt_docs(path, format!("required column {name} is missing")))?;
    column
        .as_any()
        .downcast_ref::<UInt64Array>()
        .ok_or_else(|| {
            corrupt_docs(
                path,
                format!(
                    "column {name} has type {}, expected UInt64",
                    column.data_type()
                ),
            )
        })
}

fn corrupt_docs(path: &str, message: impl Into<String>) -> Error {
    Error::corrupt_file(Path::from(path), message)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::ops::Range;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use arrow_array::{ArrayRef, RecordBatch};
    use arrow_schema::{DataType, Field, Schema};
    use async_trait::async_trait;
    use lance_core::cache::{LanceCache, QuickCacheBackend};
    use lance_core::utils::tempfile::TempObjDir;
    use lance_io::object_store::ObjectStore;
    use lance_select::RowAddrTreeMap;
    use roaring::RoaringTreemap;
    use tokio::sync::Notify;

    use crate::scalar::lance_format::LanceIndexStore;
    use crate::scalar::{IndexFile, IndexWriter};

    use super::*;

    #[derive(Debug, Default)]
    struct DocumentReadCounts {
        open_calls: AtomicUsize,
        range_calls: AtomicUsize,
        ranges_calls: AtomicUsize,
        rows: AtomicUsize,
        length_rows: AtomicUsize,
        address_rows: AtomicUsize,
    }

    impl DocumentReadCounts {
        fn record(&self, rows: usize, projection: Option<&[&str]>) {
            self.rows.fetch_add(rows, Ordering::Relaxed);
            if projection.is_none_or(|columns| columns.contains(&NUM_TOKEN_COL)) {
                self.length_rows.fetch_add(rows, Ordering::Relaxed);
            }
            if projection.is_none_or(|columns| columns.contains(&ROW_ID)) {
                self.address_rows.fetch_add(rows, Ordering::Relaxed);
            }
        }
    }

    const PAUSE_ONCE: usize = 1;
    const FAIL_ONCE: usize = 2;

    #[derive(Debug, Default)]
    struct ReadFault {
        action: AtomicUsize,
        started: Notify,
    }

    impl ReadFault {
        async fn apply(&self) -> Result<()> {
            match self.action.swap(0, Ordering::AcqRel) {
                PAUSE_ONCE => {
                    self.started.notify_one();
                    std::future::pending::<Result<()>>().await
                }
                FAIL_ONCE => Err(Error::io("injected document read failure")),
                _ => Ok(()),
            }
        }
    }

    struct CountingReader {
        inner: Arc<dyn IndexReader>,
        counts: Arc<DocumentReadCounts>,
        fault: Option<Arc<ReadFault>>,
    }

    #[async_trait]
    impl IndexReader for CountingReader {
        async fn read_record_batch(&self, n: u64, batch_size: u64) -> Result<RecordBatch> {
            self.inner.read_record_batch(n, batch_size).await
        }

        async fn read_global_buffer(&self, index: u32) -> Result<bytes::Bytes> {
            self.inner.read_global_buffer(index).await
        }

        async fn read_range(
            &self,
            range: Range<usize>,
            projection: Option<&[&str]>,
        ) -> Result<RecordBatch> {
            self.counts.range_calls.fetch_add(1, Ordering::Relaxed);
            self.counts.record(range.len(), projection);
            if let Some(fault) = &self.fault {
                fault.apply().await?;
            }
            self.inner.read_range(range, projection).await
        }

        async fn read_ranges(
            &self,
            ranges: &[Range<usize>],
            projection: Option<&[&str]>,
        ) -> Result<RecordBatch> {
            self.counts.ranges_calls.fetch_add(1, Ordering::Relaxed);
            self.counts
                .record(ranges.iter().map(Range::len).sum(), projection);
            if let Some(fault) = &self.fault {
                fault.apply().await?;
            }
            self.inner.read_ranges(ranges, projection).await
        }

        async fn num_batches(&self, batch_size: u64) -> u32 {
            self.inner.num_batches(batch_size).await
        }

        fn num_rows(&self) -> usize {
            self.inner.num_rows()
        }

        fn schema(&self) -> &lance_core::datatypes::Schema {
            self.inner.schema()
        }

        fn file_size_bytes(&self) -> Option<u64> {
            self.inner.file_size_bytes()
        }
    }

    #[derive(Debug)]
    struct CountingStore {
        inner: Arc<dyn IndexStore>,
        target: String,
        counts: Arc<DocumentReadCounts>,
        fault: Option<Arc<ReadFault>>,
    }

    impl DeepSizeOf for CountingStore {
        fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
            self.inner.deep_size_of_children(context)
        }
    }

    #[async_trait]
    impl IndexStore for CountingStore {
        fn as_any(&self) -> &dyn std::any::Any {
            self
        }

        fn clone_arc(&self) -> Arc<dyn IndexStore> {
            Arc::new(Self {
                inner: self.inner.clone(),
                target: self.target.clone(),
                counts: self.counts.clone(),
                fault: self.fault.clone(),
            })
        }

        fn io_parallelism(&self) -> usize {
            self.inner.io_parallelism()
        }

        async fn new_index_file(
            &self,
            name: &str,
            schema: Arc<Schema>,
        ) -> Result<Box<dyn IndexWriter>> {
            self.inner.new_index_file(name, schema).await
        }

        async fn open_index_file(&self, name: &str) -> Result<Arc<dyn IndexReader>> {
            let reader = self.inner.open_index_file(name).await?;
            if name == self.target {
                self.counts.open_calls.fetch_add(1, Ordering::Relaxed);
                Ok(Arc::new(CountingReader {
                    inner: reader,
                    counts: self.counts.clone(),
                    fault: self.fault.clone(),
                }))
            } else {
                Ok(reader)
            }
        }

        fn with_io_priority(&self, io_priority: u64) -> Arc<dyn IndexStore> {
            Arc::new(Self {
                inner: self.inner.with_io_priority(io_priority),
                target: self.target.clone(),
                counts: self.counts.clone(),
                fault: self.fault.clone(),
            })
        }

        async fn copy_index_file(
            &self,
            name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<IndexFile> {
            self.inner.copy_index_file(name, dest_store).await
        }

        async fn copy_index_file_to(
            &self,
            name: &str,
            new_name: &str,
            dest_store: &dyn IndexStore,
        ) -> Result<IndexFile> {
            self.inner
                .copy_index_file_to(name, new_name, dest_store)
                .await
        }

        async fn rename_index_file(&self, name: &str, new_name: &str) -> Result<IndexFile> {
            self.inner.rename_index_file(name, new_name).await
        }

        async fn delete_index_file(&self, name: &str) -> Result<()> {
            self.inner.delete_index_file(name).await
        }

        async fn list_files_with_sizes(&self) -> Result<Vec<IndexFile>> {
            self.inner.list_files_with_sizes().await
        }
    }

    fn test_store() -> (TempObjDir, Arc<LanceIndexStore>, Arc<LanceCache>) {
        let directory = TempObjDir::default();
        let cache = Arc::new(LanceCache::with_capacity(1024 * 1024));
        test_store_with_cache(directory, cache)
    }

    fn eviction_test_store() -> (TempObjDir, Arc<LanceIndexStore>, Arc<LanceCache>) {
        let directory = TempObjDir::default();
        let cache = Arc::new(LanceCache::with_backend(Arc::new(
            QuickCacheBackend::with_capacity(1024 * 1024),
        )));
        test_store_with_cache(directory, cache)
    }

    fn test_store_with_cache(
        directory: TempObjDir,
        cache: Arc<LanceCache>,
    ) -> (TempObjDir, Arc<LanceIndexStore>, Arc<LanceCache>) {
        let store = Arc::new(LanceIndexStore::new(
            ObjectStore::local().into(),
            directory.clone(),
            cache.clone(),
        ));
        (directory, store, cache)
    }

    async fn write_documents(
        store: &dyn IndexStore,
        path: &str,
        addresses: UInt64Array,
        lengths: UInt32Array,
        total_tokens: Option<&str>,
    ) {
        let schema = Arc::new(Schema::new(vec![
            Field::new(ROW_ID, DataType::UInt64, addresses.null_count() != 0),
            Field::new(NUM_TOKEN_COL, DataType::UInt32, lengths.null_count() != 0),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(addresses) as ArrayRef,
                Arc::new(lengths) as ArrayRef,
            ],
        )
        .unwrap();
        let mut writer = store.new_index_file(path, schema).await.unwrap();
        writer.write_record_batch(batch).await.unwrap();
        if let Some(total_tokens) = total_tokens {
            writer
                .finish_with_metadata(HashMap::from([(
                    TOTAL_TOKENS_KEY.to_owned(),
                    total_tokens.to_owned(),
                )]))
                .await
                .unwrap();
        } else {
            writer.finish().await.unwrap();
        }
    }

    async fn open_documents(
        store: Arc<dyn IndexStore>,
        path: &str,
        index_cache: &LanceCache,
        remapper: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<PartitionDocuments> {
        let reader = store.open_index_file(path).await?;
        PartitionDocuments::try_new(
            store,
            path.to_owned(),
            0,
            WeakLanceCache::from(index_cache),
            reader.as_ref(),
            remapper,
            false,
        )
    }

    fn counted_store(
        inner: Arc<dyn IndexStore>,
        target: &str,
    ) -> (Arc<dyn IndexStore>, Arc<DocumentReadCounts>) {
        let counts = Arc::new(DocumentReadCounts::default());
        (
            Arc::new(CountingStore {
                inner,
                target: target.to_owned(),
                counts: counts.clone(),
                fault: None,
            }),
            counts,
        )
    }

    fn faulting_store(
        inner: Arc<dyn IndexStore>,
        target: &str,
        action: usize,
    ) -> (Arc<dyn IndexStore>, Arc<ReadFault>) {
        let fault = Arc::new(ReadFault {
            action: AtomicUsize::new(action),
            started: Notify::new(),
        });
        (
            Arc::new(CountingStore {
                inner,
                target: target.to_owned(),
                counts: Arc::new(DocumentReadCounts::default()),
                fault: Some(fault.clone()),
            }),
            fault,
        )
    }

    #[derive(Debug)]
    struct TestRemapper {
        mapping: HashMap<u64, Option<u64>>,
    }

    impl RowIdRemapper for TestRemapper {
        fn remap_row_id(&self, row_id: u64) -> Option<u64> {
            self.mapping.get(&row_id).copied().unwrap_or(Some(row_id))
        }

        fn remap_row_addrs_tree_map(&self, _: &RowAddrTreeMap) -> RowAddrTreeMap {
            unreachable!("not used by document projection tests")
        }

        fn remap_row_ids_roaring_tree_map(&self, _: &RoaringTreemap) -> RoaringTreemap {
            unreachable!("not used by document projection tests")
        }

        fn remap_row_ids_record_batch(&self, _: RecordBatch, _: usize) -> Result<RecordBatch> {
            unreachable!("not used by document projection tests")
        }
    }

    #[test]
    fn required_document_columns_validate_name_and_type() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            NUM_TOKEN_COL,
            DataType::UInt64,
            false,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt64Array::from(vec![1])) as ArrayRef],
        )
        .unwrap();

        let missing = required_u64_column(&batch, ROW_ID, "docs.lance").unwrap_err();
        assert!(
            missing
                .to_string()
                .contains("required column _rowid is missing")
        );
        let wrong_type = required_u32_column(&batch, NUM_TOKEN_COL, "docs.lance").unwrap_err();
        assert!(wrong_type.to_string().contains("expected UInt32"));
    }

    #[test]
    fn live_documents_use_doc_ids() {
        let projection = Arc::new(VersionAddressProjection {
            addresses: AddressValues::Owned(Arc::new(vec![10, 20, 30])),
            live_docs: Some(RoaringBitmap::from_iter([0, 2])),
            doc_ids_by_address: OnceCell::new(),
        });
        let projection = projection.resident(None).unwrap();
        let selected = projection.live_doc_ids();
        assert_eq!(selected.iter().collect::<Vec<_>>(), vec![0, 2]);
    }

    #[tokio::test]
    async fn remap_preserves_doc_id_slots_and_filters_in_current_address_domain() {
        let raw = UInt64Array::from(vec![10, 20, 30, 40]);
        let remapper = TestRemapper {
            mapping: HashMap::from([(10, Some(100)), (20, None), (30, Some(300))]),
        };
        let projection = Arc::new(
            VersionAddressProjection::try_new(&raw, 4, Some(&remapper), "docs")
                .expect("valid projection"),
        );
        let projection = projection.resident(None).unwrap();

        assert_eq!(projection.address(DocId::new(0)), Some(100));
        assert_eq!(projection.address(DocId::new(1)), None);
        assert_eq!(projection.address(DocId::new(2)), Some(300));
        assert_eq!(projection.address(DocId::new(3)), Some(40));

        let all_live = projection.live_doc_ids();
        assert_eq!(all_live.iter().collect::<Vec<_>>(), vec![0, 2, 3]);

        let allowed = Arc::new(RowAddrMask::from_allowed(RowAddrTreeMap::from_iter([
            100, 40,
        ])));
        let DocVisibility::Selected(selected) = projection
            .clone()
            .materialize_visibility(allowed)
            .await
            .expect("valid allow-list")
        else {
            panic!("allow-list must compile to DocIds")
        };
        assert_eq!(selected.iter().collect::<Vec<_>>(), vec![0, 3]);

        let first_lookup = projection
            .doc_ids_by_address()
            .await
            .expect("cached address lookup");
        let blocked = Arc::new(RowAddrMask::from_block(RowAddrTreeMap::from_iter([300])));
        let DocVisibility::Selected(selected) = projection
            .clone()
            .materialize_visibility(blocked)
            .await
            .expect("valid block-list")
        else {
            panic!("block-list must compile to DocIds")
        };
        assert_eq!(selected.iter().collect::<Vec<_>>(), vec![0, 3]);
        let second_lookup = projection
            .doc_ids_by_address()
            .await
            .expect("cached address lookup");
        assert!(Arc::ptr_eq(&first_lookup, &second_lookup));
    }

    #[tokio::test]
    async fn materialized_visibility_handles_unsorted_duplicate_addresses_and_full_fragments() {
        let row_address = |fragment_id, row_offset| {
            u64::from(RowAddress::new_from_parts(fragment_id, row_offset))
        };
        let projection = Arc::new(VersionAddressProjection {
            addresses: AddressValues::Owned(Arc::new(vec![
                row_address(2, 5),
                row_address(1, 2),
                row_address(1, 2),
                row_address(1, 4),
            ])),
            live_docs: None,
            doc_ids_by_address: OnceCell::new(),
        });
        let projection = projection.resident(None).unwrap();

        let allowed = Arc::new(RowAddrMask::from_allowed(RowAddrTreeMap::from_iter([
            row_address(1, 2),
            row_address(1, 3),
            row_address(1, 4),
        ])));
        let DocVisibility::Selected(selected) = projection
            .clone()
            .materialize_visibility(allowed)
            .await
            .expect("valid allow-list")
        else {
            panic!("allow-list must compile to DocIds")
        };
        assert_eq!(selected.iter().collect::<Vec<_>>(), vec![1, 2, 3]);

        let mut full_fragment = RowAddrTreeMap::new();
        full_fragment.insert_fragment(2);
        let DocVisibility::Selected(selected) = projection
            .materialize_visibility(Arc::new(RowAddrMask::from_allowed(full_fragment)))
            .await
            .expect("valid full-fragment allow-list")
        else {
            panic!("allow-list must compile to DocIds")
        };
        assert_eq!(selected.iter().collect::<Vec<_>>(), vec![0]);
    }

    #[test]
    fn lazy_visibility_projects_only_candidate_doc_ids() {
        let projection = Arc::new(VersionAddressProjection {
            addresses: AddressValues::Owned(Arc::new(vec![10, 20, 30])),
            live_docs: Some(RoaringBitmap::from_iter([0, 2])),
            doc_ids_by_address: OnceCell::new(),
        });
        let resident = projection.resident(None).unwrap();
        let visibility = DocVisibility::Filtered {
            projection: resident,
            mask: Arc::new(RowAddrMask::from_allowed(RowAddrTreeMap::from_iter([
                20, 30,
            ]))),
        };

        assert!(!visibility.selected(DocId::new(0)));
        assert!(!visibility.selected(DocId::new(1)));
        assert!(visibility.selected(DocId::new(2)));
        assert!(!projection.doc_ids_by_address.initialized());
    }

    #[test]
    fn doc_lengths_validate_shape_total_and_memory() {
        let mismatch = DocLengths::try_new(ScalarBuffer::from(vec![2, 3]), 3, None, false, "docs")
            .unwrap_err();
        assert!(mismatch.to_string().contains("2 rows"));

        let mismatch = DocLengths::try_new(
            ScalarBuffer::from(vec![2, 3, 5]),
            3,
            Some(11),
            false,
            "docs",
        )
        .unwrap_err();
        assert!(mismatch.to_string().contains("sums to 10"));

        let lengths =
            DocLengths::try_new(ScalarBuffer::from(vec![2, 3, 5]), 3, Some(10), true, "docs")
                .unwrap();
        let before_norms = lengths.deep_size_of();
        assert_eq!(lengths.total_tokens(), 10);
        assert_eq!(lengths.scoring_norms().unwrap().len(), 3);
        assert_eq!(lengths.deep_size_of() - before_norms, 3);
    }

    #[tokio::test]
    async fn persisted_stats_are_footer_only_and_compatible_with_full_docset_reader() {
        let (_directory, store, cache) = test_store();
        let path = "docs.lance";
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 3, 5]),
            Some("10"),
        )
        .await;

        let reader = store.open_index_file(path).await.unwrap();
        assert_eq!(
            reader.schema().metadata.get(TOTAL_TOKENS_KEY),
            Some(&"10".to_owned())
        );
        let complete = DocSet::load(reader, false, None).await.unwrap();
        assert_eq!(complete.len(), 3);
        assert_eq!(complete.row_id(1), 20);
        assert_eq!(complete.total_tokens_num(), 10);

        let (counting, counts) = counted_store(store, path);
        let documents = open_documents(counting, path, cache.as_ref(), None)
            .await
            .unwrap();
        assert_eq!(
            documents.stats().await.unwrap(),
            PartitionStats {
                num_docs: 3,
                total_tokens: 10,
            }
        );
        assert_eq!(counts.rows.load(Ordering::Relaxed), 0);
        assert!(!documents.lengths_loaded());
        assert!(!documents.projection_loaded());
    }

    #[tokio::test]
    async fn missing_stats_fall_back_once_to_lengths() {
        let (_directory, store, cache) = test_store();
        let path = "docs.lance";
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 3, 5]),
            None,
        )
        .await;
        let (counting, counts) = counted_store(store, path);
        let documents = open_documents(counting, path, cache.as_ref(), None)
            .await
            .unwrap();

        assert_eq!(documents.stats().await.unwrap().total_tokens, 10);
        assert_eq!(documents.stats().await.unwrap().total_tokens, 10);
        assert_eq!(counts.range_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counts.length_rows.load(Ordering::Relaxed), 3);
        assert_eq!(counts.address_rows.load(Ordering::Relaxed), 0);
        assert!(documents.lengths_loaded());
        assert!(!documents.projection_loaded());
    }

    #[tokio::test]
    async fn prewarm_loads_document_columns_and_address_lookup_once() {
        let (_directory, store, cache) = test_store();
        let path = "docs.lance";
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 3, 5]),
            Some("10"),
        )
        .await;
        let (counting, counts) = counted_store(store, path);
        let documents = open_documents(counting, path, cache.as_ref(), None)
            .await
            .unwrap();

        futures::future::join_all((0..8).map(|_| documents.prewarm()))
            .await
            .into_iter()
            .collect::<Result<Vec<_>>>()
            .unwrap();
        let projection = documents.address_projection().await.unwrap();
        let first_lookup = projection.doc_ids_by_address().await.unwrap();
        documents.prewarm().await.unwrap();
        let second_lookup = documents
            .address_projection()
            .await
            .unwrap()
            .doc_ids_by_address()
            .await
            .unwrap();

        assert!(documents.lengths_loaded());
        assert!(documents.projection_loaded());
        assert!(documents.query_ready());
        assert_eq!(documents.cached_lengths().unwrap().total_tokens(), 10);
        assert!(matches!(
            documents.immediate_visibility(Arc::new(RowAddrMask::all_rows()), false),
            Some(DocVisibility::All)
        ));
        assert!(matches!(
            first_lookup.as_ref(),
            AddressDocIdLookup::Identity
        ));
        assert!(Arc::ptr_eq(&first_lookup, &second_lookup));
        assert_eq!(
            documents.cached_row_addresses(&[DocId::new(1)]).unwrap(),
            Some(vec![20])
        );
        assert_eq!(counts.open_calls.load(Ordering::Relaxed), 2);
        assert_eq!(counts.range_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counts.length_rows.load(Ordering::Relaxed), 3);
        assert_eq!(counts.address_rows.load(Ordering::Relaxed), 3);
        assert!(
            cache
                .get_with_key(&DocRowIdsKey { partition_id: 0 })
                .await
                .is_some()
        );
    }

    #[tokio::test]
    async fn filtered_visibility_releases_addresses_after_cache_eviction() {
        let (_directory, store, cache) = eviction_test_store();
        let path = "docs.lance";
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 3, 5]),
            Some("10"),
        )
        .await;
        let (counting, counts) = counted_store(store, path);
        let documents = open_documents(counting, path, cache.as_ref(), None)
            .await
            .unwrap();
        let mask = Arc::new(RowAddrMask::from_allowed(RowAddrTreeMap::from_iter([20])));

        let visibility = documents.visibility(mask.clone(), false).await.unwrap();
        assert!(!visibility.selected(DocId::new(0)));
        assert!(visibility.selected(DocId::new(1)));
        assert!(!visibility.selected(DocId::new(2)));

        let weak_addresses = documents.address_buffer_handle();

        cache.clear().await;
        assert!(weak_addresses.upgrade().is_some());
        assert!(documents.projection_resident());

        drop(visibility);
        assert!(weak_addresses.upgrade().is_none());
        assert!(!documents.projection_resident());

        let reloaded = documents.visibility(mask, false).await.unwrap();
        assert!(reloaded.selected(DocId::new(1)));
        assert_eq!(counts.address_rows.load(Ordering::Relaxed), 6);
    }

    #[tokio::test]
    async fn prewarm_reloads_addresses_after_cache_eviction() {
        let (_directory, store, cache) = eviction_test_store();
        let path = "docs.lance";
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 3, 5]),
            Some("10"),
        )
        .await;
        let (counting, counts) = counted_store(store, path);
        let documents = open_documents(counting, path, cache.as_ref(), None)
            .await
            .unwrap();

        documents.prewarm().await.unwrap();
        assert!(documents.query_ready());
        let weak_addresses = documents.address_buffer_handle();

        cache.clear().await;
        assert!(weak_addresses.upgrade().is_none());
        assert!(documents.projection_loaded());
        assert!(!documents.projection_resident());
        assert!(!documents.query_ready());

        documents.prewarm().await.unwrap();
        assert!(documents.query_ready());
        assert_eq!(
            documents
                .cached_row_addresses(&[DocId::new(2), DocId::new(0)])
                .unwrap(),
            Some(vec![30, 10])
        );
        assert_eq!(counts.length_rows.load(Ordering::Relaxed), 3);
        assert_eq!(counts.address_rows.load(Ordering::Relaxed), 6);
    }

    #[tokio::test]
    async fn prewarm_materializes_quantized_norms_before_becoming_query_ready() {
        let (_directory, store, cache) = test_store();
        let path = "docs.lance";
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 300, 5]),
            Some("307"),
        )
        .await;
        let reader = store.open_index_file(path).await.unwrap();
        let documents = PartitionDocuments::try_new(
            store,
            path.to_owned(),
            0,
            WeakLanceCache::from(cache.as_ref()),
            reader.as_ref(),
            None,
            true,
        )
        .unwrap();

        assert!(!documents.query_ready());
        documents.prewarm().await.unwrap();
        let lengths = documents.lengths().await.unwrap();
        assert!(lengths.scoring_ready());
        assert_eq!(lengths.scoring_norms().unwrap().len(), 3);
        assert!(documents.query_ready());
    }

    #[tokio::test]
    async fn cancelled_or_failed_prewarm_can_retry_without_partial_publication() {
        let (_directory, store, cache) = test_store();
        let path = "docs.lance";
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 3, 5]),
            Some("10"),
        )
        .await;

        let (pausing, pause) = faulting_store(store.clone(), path, PAUSE_ONCE);
        let documents = Arc::new(
            open_documents(pausing, path, cache.as_ref(), None)
                .await
                .unwrap(),
        );
        let task = tokio::spawn({
            let documents = documents.clone();
            async move { documents.prewarm().await }
        });
        tokio::time::timeout(Duration::from_secs(5), pause.started.notified())
            .await
            .expect("prewarm should reach the injected pending read");
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(!documents.lengths_loaded());
        assert!(!documents.projection_loaded());
        assert!(!documents.query_ready());
        documents.prewarm().await.unwrap();
        assert!(documents.query_ready());

        let (failing, _fault) = faulting_store(store, path, FAIL_ONCE);
        let documents = open_documents(failing, path, cache.as_ref(), None)
            .await
            .unwrap();
        let error = documents.prewarm().await.unwrap_err();
        assert!(error.to_string().contains("injected document read failure"));
        assert!(!documents.lengths_loaded());
        assert!(!documents.projection_loaded());
        assert!(!documents.query_ready());
        documents.prewarm().await.unwrap();
        assert!(documents.query_ready());
    }

    #[tokio::test]
    async fn prewarm_reuses_an_already_loaded_document_column() {
        let (_directory, store, cache) = test_store();
        let path = "docs.lance";
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 3, 5]),
            Some("10"),
        )
        .await;
        let (counting, counts) = counted_store(store, path);
        let documents = open_documents(counting, path, cache.as_ref(), None)
            .await
            .unwrap();

        documents.lengths().await.unwrap();
        documents.prewarm().await.unwrap();
        documents.prewarm().await.unwrap();

        assert_eq!(counts.open_calls.load(Ordering::Relaxed), 3);
        assert_eq!(counts.range_calls.load(Ordering::Relaxed), 2);
        assert_eq!(counts.length_rows.load(Ordering::Relaxed), 3);
        assert_eq!(counts.address_rows.load(Ordering::Relaxed), 3);
    }

    #[tokio::test]
    async fn invalid_or_mismatched_stats_are_corruption_not_fallback() {
        let (_directory, store, cache) = test_store();
        write_documents(
            store.as_ref(),
            "invalid.lance",
            UInt64Array::from(vec![10]),
            UInt32Array::from(vec![2]),
            Some("not-a-u64"),
        )
        .await;
        let error = open_documents(store.clone(), "invalid.lance", cache.as_ref(), None)
            .await
            .unwrap_err();
        assert!(error.to_string().contains("invalid total_tokens"));

        write_documents(
            store.as_ref(),
            "mismatch.lance",
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 3, 5]),
            Some("11"),
        )
        .await;
        let (counting, counts) = counted_store(store, "mismatch.lance");
        let documents = open_documents(counting, "mismatch.lance", cache.as_ref(), None)
            .await
            .unwrap();
        assert_eq!(documents.stats().await.unwrap().total_tokens, 11);
        let error = documents.lengths().await.unwrap_err();
        assert!(error.to_string().contains("sums to 10"));
        assert_eq!(counts.length_rows.load(Ordering::Relaxed), 3);
        assert!(!documents.lengths_loaded());
    }

    #[tokio::test]
    async fn final_address_resolution_reuses_the_cached_row_id_column() {
        let (_directory, store, cache) = test_store();
        let path = "docs.lance";
        let num_docs = 600_u64;
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from_iter_values((0..num_docs).map(|id| id + 1_000)),
            UInt32Array::from_iter_values((0..num_docs).map(|_| 1)),
            Some("600"),
        )
        .await;
        let (counting, counts) = counted_store(store, path);
        let documents = open_documents(counting, path, cache.as_ref(), None)
            .await
            .unwrap();

        assert!(documents.resolve_addresses(&[]).await.unwrap().is_empty());
        assert_eq!(counts.rows.load(Ordering::Relaxed), 0);

        let point_ids = [DocId::new(5), DocId::new(6), DocId::new(10), DocId::new(5)];
        assert_eq!(
            documents.estimated_address_read_bytes(&point_ids),
            num_docs as usize * std::mem::size_of::<u64>()
        );
        assert_eq!(
            documents.resolve_addresses(&point_ids).await.unwrap(),
            vec![1005, 1006, 1010, 1005]
        );
        assert_eq!(counts.ranges_calls.load(Ordering::Relaxed), 0);
        assert_eq!(counts.range_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counts.address_rows.load(Ordering::Relaxed), 600);
        assert!(
            cache
                .get_with_key(&DocRowIdsKey { partition_id: 0 })
                .await
                .is_some()
        );

        let bulk_ids = (0..=512).step_by(2).map(DocId::new).collect::<Vec<_>>();
        assert_eq!(
            documents.estimated_address_read_bytes(&bulk_ids),
            num_docs as usize * std::mem::size_of::<u64>()
        );
        let resolved = documents.resolve_addresses(&bulk_ids).await.unwrap();
        assert_eq!(resolved.first(), Some(&1000));
        assert_eq!(resolved.last(), Some(&1512));
        assert_eq!(counts.ranges_calls.load(Ordering::Relaxed), 0);
        assert_eq!(counts.range_calls.load(Ordering::Relaxed), 1);
        assert_eq!(counts.address_rows.load(Ordering::Relaxed), 600);
        assert!(!documents.projection_loaded());
    }

    #[tokio::test]
    async fn final_address_resolution_reloads_after_cache_eviction() {
        let (_directory, store, _cache) = test_store();
        let path = "docs.lance";
        write_documents(
            store.as_ref(),
            path,
            UInt64Array::from(vec![10, 20, 30]),
            UInt32Array::from(vec![2, 3, 5]),
            Some("10"),
        )
        .await;
        let (counting, counts) = counted_store(store, path);
        let no_retention_cache = LanceCache::no_cache();
        let documents = open_documents(counting, path, &no_retention_cache, None)
            .await
            .unwrap();

        for _ in 0..2 {
            assert_eq!(
                documents
                    .resolve_addresses(&[DocId::new(2), DocId::new(0)])
                    .await
                    .unwrap(),
                vec![30, 10]
            );
        }
        assert_eq!(counts.ranges_calls.load(Ordering::Relaxed), 0);
        assert_eq!(counts.range_calls.load(Ordering::Relaxed), 2);
        assert_eq!(counts.address_rows.load(Ordering::Relaxed), 6);
        assert!(!documents.projection_loaded());
    }

    #[tokio::test]
    async fn document_column_nulls_are_reported_as_corruption() {
        let (_directory, store, cache) = test_store();
        write_documents(
            store.as_ref(),
            "null-length.lance",
            UInt64Array::from(vec![Some(10), Some(20)]),
            UInt32Array::from(vec![Some(2), None]),
            Some("2"),
        )
        .await;
        let documents = open_documents(store.clone(), "null-length.lance", cache.as_ref(), None)
            .await
            .unwrap();
        assert!(
            documents
                .lengths()
                .await
                .unwrap_err()
                .to_string()
                .contains("_num_tokens contains null")
        );

        write_documents(
            store.as_ref(),
            "null-address.lance",
            UInt64Array::from(vec![Some(10), None]),
            UInt32Array::from(vec![2, 3]),
            Some("5"),
        )
        .await;
        let documents = open_documents(store, "null-address.lance", cache.as_ref(), None)
            .await
            .unwrap();
        assert!(
            documents
                .resolve_addresses(&[DocId::new(1)])
                .await
                .unwrap_err()
                .to_string()
                .contains("_rowid contains null")
        );
        assert!(
            documents
                .prewarm()
                .await
                .unwrap_err()
                .to_string()
                .contains("_rowid contains null")
        );
        assert!(!documents.lengths_loaded());
        assert!(!documents.projection_loaded());
    }
}
