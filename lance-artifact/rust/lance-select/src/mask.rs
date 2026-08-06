// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashSet;
use std::io::Write;
use std::ops::{Range, RangeBounds, RangeInclusive};
use std::{collections::BTreeMap, io::Read};

use arrow_array::{Array, BinaryArray, GenericBinaryArray};
use arrow_buffer::{Buffer, NullBuffer, OffsetBuffer};
use byteorder::{ReadBytesExt, WriteBytesExt};
use itertools::Itertools;
use lance_core::deepsize::DeepSizeOf;
use roaring::{MultiOps, RoaringBitmap, RoaringTreemap};

use lance_core::cache::{CacheCodecImpl, CacheEntryReader, CacheEntryWriter};
use lance_core::utils::address::RowAddress;
use lance_core::{Error, Result};

mod nullable;

pub use nullable::{NullableRowAddrMask, NullableRowAddrSet};

/// A mask that selects or deselects rows based on an allow-list or block-list.
#[derive(Clone, Debug, DeepSizeOf, PartialEq)]
pub enum RowAddrMask {
    AllowList(RowAddrTreeMap),
    BlockList(RowAddrTreeMap),
}

impl Default for RowAddrMask {
    fn default() -> Self {
        // Empty block list means all rows are allowed
        Self::BlockList(RowAddrTreeMap::new())
    }
}

impl RowAddrMask {
    // Create a mask allowing all rows, this is an alias for [default]
    pub fn all_rows() -> Self {
        Self::default()
    }

    // Create a mask that doesn't allow anything
    pub fn allow_nothing() -> Self {
        Self::AllowList(RowAddrTreeMap::new())
    }

    // Create a mask from an allow list
    pub fn from_allowed(allow_list: RowAddrTreeMap) -> Self {
        Self::AllowList(allow_list)
    }

    // Create a mask from a block list
    pub fn from_block(block_list: RowAddrTreeMap) -> Self {
        Self::BlockList(block_list)
    }

    pub fn block_list(&self) -> Option<&RowAddrTreeMap> {
        match self {
            Self::BlockList(block_list) => Some(block_list),
            _ => None,
        }
    }

    pub fn allow_list(&self) -> Option<&RowAddrTreeMap> {
        match self {
            Self::AllowList(allow_list) => Some(allow_list),
            _ => None,
        }
    }

    /// True if the row_id is selected by the mask, false otherwise
    pub fn selected(&self, row_id: u64) -> bool {
        match self {
            Self::AllowList(allow_list) => allow_list.contains(row_id),
            Self::BlockList(block_list) => !block_list.contains(row_id),
        }
    }

    /// True if every row_id is selected. Lets callers (e.g. the FTS wand
    /// loop) skip per-row mask checks entirely, which in turn lets the
    /// deferred-row_id scoring path skip loading the row_id column.
    pub fn is_select_all(&self) -> bool {
        matches!(self, Self::BlockList(b) if b.is_empty())
    }

    /// Return the indices of the input row ids that were valid
    pub fn selected_indices<'a>(&self, row_ids: impl Iterator<Item = &'a u64> + 'a) -> Vec<u64> {
        row_ids
            .enumerate()
            .filter_map(|(idx, row_id)| {
                if self.selected(*row_id) {
                    Some(idx as u64)
                } else {
                    None
                }
            })
            .collect()
    }

    /// Also block the given addrs
    pub fn also_block(self, block_list: RowAddrTreeMap) -> Self {
        match self {
            Self::AllowList(allow_list) => Self::AllowList(allow_list - block_list),
            Self::BlockList(existing) => Self::BlockList(existing | block_list),
        }
    }

    /// Also allow the given addrs
    pub fn also_allow(self, allow_list: RowAddrTreeMap) -> Self {
        match self {
            Self::AllowList(existing) => Self::AllowList(existing | allow_list),
            Self::BlockList(block_list) => Self::BlockList(block_list - allow_list),
        }
    }

    /// Convert a mask into an arrow array
    ///
    /// A row addr mask is not very arrow-compatible.  We can't make it a batch with
    /// two columns because the block list and allow list will have different lengths.  Also,
    /// there is no Arrow type for compressed bitmaps.
    ///
    /// However, we need to shove it into some kind of Arrow container to pass it along the
    /// datafusion stream.  Perhaps, in the future, we can add row addr masks as first class
    /// types in datafusion, and this can be passed along as a mask / selection vector.
    ///
    /// We serialize this as a variable length binary array with two items.  The first item
    /// is the block list and the second item is the allow list.
    pub fn into_arrow(&self) -> Result<BinaryArray> {
        // NOTE: This serialization format must be stable as it is used in IPC.
        let (block_list, allow_list) = match self {
            Self::AllowList(allow_list) => (None, Some(allow_list)),
            Self::BlockList(block_list) => (Some(block_list), None),
        };

        let block_list_length = block_list
            .as_ref()
            .map(|bl| bl.serialized_size())
            .unwrap_or(0);
        let allow_list_length = allow_list
            .as_ref()
            .map(|al| al.serialized_size())
            .unwrap_or(0);
        let lengths = vec![block_list_length, allow_list_length];
        let offsets = OffsetBuffer::from_lengths(lengths);
        let mut value_bytes = vec![0; block_list_length + allow_list_length];
        let mut validity = vec![false, false];
        if let Some(block_list) = &block_list {
            validity[0] = true;
            block_list.serialize_into(&mut value_bytes[0..])?;
        }
        if let Some(allow_list) = &allow_list {
            validity[1] = true;
            allow_list.serialize_into(&mut value_bytes[block_list_length..])?;
        }
        let values = Buffer::from(value_bytes);
        let nulls = NullBuffer::from(validity);
        Ok(BinaryArray::try_new(offsets, values, Some(nulls))?)
    }

    /// Deserialize a row address mask from Arrow
    pub fn from_arrow(array: &GenericBinaryArray<i32>) -> Result<Self> {
        let block_list = if array.is_null(0) {
            None
        } else {
            Some(RowAddrTreeMap::deserialize_from(array.value(0)))
        }
        .transpose()?;

        let allow_list = if array.is_null(1) {
            None
        } else {
            Some(RowAddrTreeMap::deserialize_from(array.value(1)))
        }
        .transpose()?;

        let res = match (block_list, allow_list) {
            (Some(bl), None) => Self::BlockList(bl),
            (None, Some(al)) => Self::AllowList(al),
            (Some(block), Some(allow)) => Self::AllowList(allow).also_block(block),
            (None, None) => Self::all_rows(),
        };
        Ok(res)
    }

    /// Return the maximum number of row addresses that could be selected by this mask
    ///
    /// Will be None if this is a BlockList (unbounded)
    pub fn max_len(&self) -> Option<u64> {
        match self {
            Self::AllowList(selection) => selection.len(),
            Self::BlockList(_) => None,
        }
    }

    /// Iterate over the row addresses that are selected by the mask
    ///
    /// This is only possible if this is an AllowList and the maps don't contain
    /// any "full fragment" blocks.
    pub fn iter_addrs(&self) -> Option<Box<dyn Iterator<Item = RowAddress> + '_>> {
        match self {
            Self::AllowList(allow_list) => {
                if let Some(allow_iter) = allow_list.row_addrs() {
                    Some(Box::new(allow_iter))
                } else {
                    None
                }
            }
            Self::BlockList(_) => None, // Can't iterate over block list
        }
    }
}

impl std::ops::Not for RowAddrMask {
    type Output = Self;

    fn not(self) -> Self::Output {
        match self {
            Self::AllowList(allow_list) => Self::BlockList(allow_list),
            Self::BlockList(block_list) => Self::AllowList(block_list),
        }
    }
}

impl std::ops::BitAnd for RowAddrMask {
    type Output = Self;

    fn bitand(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::AllowList(a), Self::AllowList(b)) => Self::AllowList(a & b),
            (Self::AllowList(allow), Self::BlockList(block))
            | (Self::BlockList(block), Self::AllowList(allow)) => Self::AllowList(allow - block),
            (Self::BlockList(a), Self::BlockList(b)) => Self::BlockList(a | b),
        }
    }
}

impl std::ops::BitOr for RowAddrMask {
    type Output = Self;

    fn bitor(self, rhs: Self) -> Self::Output {
        match (self, rhs) {
            (Self::AllowList(a), Self::AllowList(b)) => Self::AllowList(a | b),
            (Self::AllowList(allow), Self::BlockList(block))
            | (Self::BlockList(block), Self::AllowList(allow)) => Self::BlockList(block - allow),
            (Self::BlockList(a), Self::BlockList(b)) => Self::BlockList(a & b),
        }
    }
}

/// Common operations over a set of rows (either row ids or row addresses).
///
/// The concrete representation can be address-based (`RowAddrTreeMap`) or
/// id-based (for example a future `RowIdSet`), but the semantics are the same:
/// a set of unique rows.
pub trait RowSetOps: Clone + Sized {
    /// Logical row handle (`u64` for both row ids and row addresses).
    type Row;

    /// Returns true if the set is empty.
    fn is_empty(&self) -> bool;

    /// Returns the number of rows in the set, if it is known.
    ///
    /// Implementations that cannot always compute an exact size (for example
    /// because of "full fragment" markers) should return `None`.
    fn len(&self) -> Option<u64>;

    /// Remove a value from the row set.
    fn remove(&mut self, row: Self::Row) -> bool;

    /// Returns whether this set contains the given row.
    fn contains(&self, row: Self::Row) -> bool;

    /// Returns the union of `other` and init self.
    fn union_all(other: &[&Self]) -> Self;

    /// Builds a row set from an iterator of rows.
    fn from_sorted_iter<I>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = Self::Row>;
}

/// A collection of row addresses.
///
/// Note: For stable row id mode, this may be split into a separate structure in the future.
///
/// These row ids may either be stable-style (where they can be an incrementing
/// u64 sequence) or address style, where they are a fragment id and a row offset.
/// When address style, this supports setting entire fragments as selected,
/// without needing to enumerate all the ids in the fragment.
///
/// This is similar to a [RoaringTreemap] but it is optimized for the case where
/// entire fragments are selected or deselected.
#[derive(Clone, Debug, Default, PartialEq, DeepSizeOf)]
pub struct RowAddrTreeMap {
    /// The contents of the set. If there is a pair (k, Full) then the entire
    /// fragment k is selected. If there is a pair (k, Partial(v)) then the
    /// fragment k has the selected rows in v.
    inner: BTreeMap<u32, RowAddrSelection>,
}

#[derive(Clone, Debug, PartialEq)]
pub enum RowAddrSelection {
    Full,
    Partial(RoaringBitmap),
}

impl DeepSizeOf for RowAddrSelection {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        match self {
            Self::Full => 0,
            Self::Partial(bitmap) => bitmap.serialized_size(),
        }
    }
}

impl RowAddrSelection {
    fn union_all(selections: &[&Self]) -> Self {
        let mut is_full = false;

        let res = Self::Partial(
            selections
                .iter()
                .filter_map(|selection| match selection {
                    Self::Full => {
                        is_full = true;
                        None
                    }
                    Self::Partial(bitmap) => Some(bitmap),
                })
                .union(),
        );

        if is_full { Self::Full } else { res }
    }
}

impl RowSetOps for RowAddrTreeMap {
    type Row = u64;

    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }

    fn len(&self) -> Option<u64> {
        self.inner
            .values()
            .map(|row_addr_selection| match row_addr_selection {
                RowAddrSelection::Full => None,
                RowAddrSelection::Partial(indices) => Some(indices.len()),
            })
            .try_fold(0_u64, |acc, next| next.map(|next| next + acc))
    }

    fn remove(&mut self, row: Self::Row) -> bool {
        let upper = (row >> 32) as u32;
        let lower = row as u32;
        match self.inner.get_mut(&upper) {
            None => false,
            Some(RowAddrSelection::Full) => {
                let mut set = RoaringBitmap::full();
                set.remove(lower);
                self.inner.insert(upper, RowAddrSelection::Partial(set));
                true
            }
            Some(RowAddrSelection::Partial(lower_set)) => {
                let removed = lower_set.remove(lower);
                if lower_set.is_empty() {
                    self.inner.remove(&upper);
                }
                removed
            }
        }
    }

    fn contains(&self, row: Self::Row) -> bool {
        let upper = (row >> 32) as u32;
        let lower = row as u32;
        match self.inner.get(&upper) {
            None => false,
            Some(RowAddrSelection::Full) => true,
            Some(RowAddrSelection::Partial(fragment_set)) => fragment_set.contains(lower),
        }
    }

    fn union_all(other: &[&Self]) -> Self {
        let mut new_map = BTreeMap::new();

        for map in other {
            for (fragment, selection) in &map.inner {
                new_map
                    .entry(fragment)
                    // I hate this allocation, but I can't think of a better way
                    .or_insert_with(|| Vec::with_capacity(other.len()))
                    .push(selection);
            }
        }

        let new_map = new_map
            .into_iter()
            .map(|(&fragment, selections)| (fragment, RowAddrSelection::union_all(&selections)))
            .collect();

        Self { inner: new_map }
    }

    #[track_caller]
    fn from_sorted_iter<I>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = Self::Row>,
    {
        let mut iter = iter.into_iter().peekable();
        let mut inner = BTreeMap::new();

        while let Some(row_id) = iter.peek() {
            let fragment_id = (row_id >> 32) as u32;
            let next_bitmap_iter = iter
                .peeking_take_while(|row_id| (row_id >> 32) as u32 == fragment_id)
                .map(|row_id| row_id as u32);
            let Ok(bitmap) = RoaringBitmap::from_sorted_iter(next_bitmap_iter) else {
                return Err(Error::internal(
                    "RowAddrTreeMap::from_sorted_iter called with non-sorted input",
                ));
            };
            inner.insert(fragment_id, RowAddrSelection::Partial(bitmap));
        }

        Ok(Self { inner })
    }
}

impl RowAddrTreeMap {
    /// Create an empty set
    pub fn new() -> Self {
        Self::default()
    }

    /// An iterator of row addrs
    ///
    /// If there are any "full fragment" items then this can't be calculated and None
    /// is returned
    pub fn row_addrs(&self) -> Option<impl Iterator<Item = RowAddress> + '_> {
        let inner_iters = self
            .inner
            .iter()
            .filter_map(|(frag_id, row_addr_selection)| match row_addr_selection {
                RowAddrSelection::Full => None,
                RowAddrSelection::Partial(bitmap) => Some(
                    bitmap
                        .iter()
                        .map(|row_offset| RowAddress::new_from_parts(*frag_id, row_offset)),
                ),
            })
            .collect::<Vec<_>>();
        if inner_iters.len() != self.inner.len() {
            None
        } else {
            Some(inner_iters.into_iter().flatten())
        }
    }

    /// Insert a single value into the set
    ///
    /// Returns true if the value was not already in the set.
    ///
    /// ```rust
    /// use lance_select::{RowAddrTreeMap, RowSetOps};
    ///
    /// let mut set = RowAddrTreeMap::new();
    /// assert_eq!(set.insert(10), true);
    /// assert_eq!(set.insert(10), false);
    /// assert_eq!(set.contains(10), true);
    /// ```
    pub fn insert(&mut self, value: u64) -> bool {
        let fragment = (value >> 32) as u32;
        let row_addr = value as u32;
        match self.inner.get_mut(&fragment) {
            None => {
                let mut set = RoaringBitmap::new();
                set.insert(row_addr);
                self.inner.insert(fragment, RowAddrSelection::Partial(set));
                true
            }
            Some(RowAddrSelection::Full) => false,
            Some(RowAddrSelection::Partial(set)) => set.insert(row_addr),
        }
    }

    /// Insert a range of values into the set
    pub fn insert_range<R: RangeBounds<u64>>(&mut self, range: R) -> u64 {
        // Separate the start and end into high and low bits.
        let (mut start_high, mut start_low) = match range.start_bound() {
            std::ops::Bound::Included(&start) => ((start >> 32) as u32, start as u32),
            std::ops::Bound::Excluded(&start) => {
                let start = start.saturating_add(1);
                ((start >> 32) as u32, start as u32)
            }
            std::ops::Bound::Unbounded => (0, 0),
        };

        let (end_high, end_low) = match range.end_bound() {
            std::ops::Bound::Included(&end) => ((end >> 32) as u32, end as u32),
            std::ops::Bound::Excluded(&end) => {
                let end = end.saturating_sub(1);
                ((end >> 32) as u32, end as u32)
            }
            std::ops::Bound::Unbounded => (u32::MAX, u32::MAX),
        };

        let mut count = 0;

        while start_high <= end_high {
            let start = start_low;
            let end = if start_high == end_high {
                end_low
            } else {
                u32::MAX
            };
            let fragment = start_high;
            match self.inner.get_mut(&fragment) {
                None => {
                    let mut set = RoaringBitmap::new();
                    count += set.insert_range(start..=end);
                    self.inner.insert(fragment, RowAddrSelection::Partial(set));
                }
                Some(RowAddrSelection::Full) => {}
                Some(RowAddrSelection::Partial(set)) => {
                    count += set.insert_range(start..=end);
                }
            }
            start_high += 1;
            start_low = 0;
        }

        count
    }

    /// Add a bitmap for a single fragment
    pub fn insert_bitmap(&mut self, fragment: u32, bitmap: RoaringBitmap) {
        self.inner
            .insert(fragment, RowAddrSelection::Partial(bitmap));
    }

    /// Add a whole fragment to the set
    pub fn insert_fragment(&mut self, fragment_id: u32) {
        self.inner.insert(fragment_id, RowAddrSelection::Full);
    }

    pub fn get_fragment_bitmap(&self, fragment_id: u32) -> Option<&RoaringBitmap> {
        match self.inner.get(&fragment_id) {
            None => None,
            Some(RowAddrSelection::Full) => None,
            Some(RowAddrSelection::Partial(set)) => Some(set),
        }
    }

    /// Get the selection for a fragment
    pub fn get(&self, fragment_id: &u32) -> Option<&RowAddrSelection> {
        self.inner.get(fragment_id)
    }

    /// Iterate over (fragment_id, selection) pairs
    pub fn iter(&self) -> impl Iterator<Item = (&u32, &RowAddrSelection)> {
        self.inner.iter()
    }

    pub fn retain_fragments(&mut self, frag_ids: impl IntoIterator<Item = u32>) {
        let frag_id_set = frag_ids.into_iter().collect::<HashSet<_>>();
        self.inner
            .retain(|frag_id, _| frag_id_set.contains(frag_id));
    }

    /// Compute the serialized size of the set.
    pub fn serialized_size(&self) -> usize {
        // Starts at 4 because of the u32 num_entries
        let mut size = 4;
        for set in self.inner.values() {
            // Each entry is 8 bytes for the fragment id and the bitmap size
            size += 8;
            if let RowAddrSelection::Partial(set) = set {
                size += set.serialized_size();
            }
        }
        size
    }

    /// Serialize the set into the given buffer
    ///
    /// The serialization format is stable and used for index serialization
    ///
    /// The serialization format is:
    /// * u32: num_entries
    ///
    /// for each entry:
    ///   * u32: fragment_id
    ///   * u32: bitmap size
    ///   * \[u8\]: bitmap
    ///
    /// If bitmap size is zero then the entire fragment is selected.
    pub fn serialize_into<W: Write>(&self, mut writer: W) -> Result<()> {
        writer.write_u32::<byteorder::LittleEndian>(self.inner.len() as u32)?;
        for (fragment, set) in &self.inner {
            writer.write_u32::<byteorder::LittleEndian>(*fragment)?;
            if let RowAddrSelection::Partial(set) = set {
                writer.write_u32::<byteorder::LittleEndian>(set.serialized_size() as u32)?;
                set.serialize_into(&mut writer)?;
            } else {
                writer.write_u32::<byteorder::LittleEndian>(0)?;
            }
        }
        Ok(())
    }

    /// Deserialize the set from the given buffer
    pub fn deserialize_from<R: Read>(mut reader: R) -> Result<Self> {
        let num_entries = reader.read_u32::<byteorder::LittleEndian>()?;
        let mut inner = BTreeMap::new();
        for _ in 0..num_entries {
            let fragment = reader.read_u32::<byteorder::LittleEndian>()?;
            let bitmap_size = reader.read_u32::<byteorder::LittleEndian>()?;
            if bitmap_size == 0 {
                inner.insert(fragment, RowAddrSelection::Full);
            } else {
                let mut buffer = vec![0; bitmap_size as usize];
                reader.read_exact(&mut buffer)?;
                let set = RoaringBitmap::deserialize_from(&buffer[..])?;
                inner.insert(fragment, RowAddrSelection::Partial(set));
            }
        }
        Ok(Self { inner })
    }

    /// Apply a mask to the row addrs
    ///
    /// For AllowList: only keep rows that are in the selection and not null
    /// For BlockList: remove rows that are blocked (not null) and remove nulls
    pub fn mask(&mut self, mask: &RowAddrMask) {
        match mask {
            RowAddrMask::AllowList(allow_list) => {
                *self &= allow_list;
            }
            RowAddrMask::BlockList(block_list) => {
                *self -= block_list;
            }
        }
    }

    /// Convert the set into an iterator of row addrs
    ///
    /// # Safety
    ///
    /// This is unsafe because if any of the inner RowAddrSelection elements
    /// is not a Partial then the iterator will panic because we don't know
    /// the size of the bitmap.
    pub unsafe fn into_addr_iter(self) -> impl Iterator<Item = u64> {
        self.inner
            .into_iter()
            .flat_map(|(fragment, selection)| match selection {
                RowAddrSelection::Full => panic!("Size of full fragment is unknown"),
                RowAddrSelection::Partial(bitmap) => bitmap.into_iter().map(move |val| {
                    let fragment = fragment as u64;
                    let row_offset = val as u64;
                    (fragment << 32) | row_offset
                }),
            })
    }

    /// Iterate the selected row addresses as `(fragment_id, run)` pairs.
    ///
    /// Range-shaped counterpart to [`Self::into_addr_iter`]. A contiguous
    /// run of N selected rows yields one item, not N. Uses roaring's
    /// `Iter::next_range`, which walks each underlying container's runs
    /// rather than its individual bits, so dense ranges cost
    /// O(num_containers) (roughly num_rows / 65536) instead of O(num_rows).
    ///
    /// # Safety
    /// Same contract as [`Self::into_addr_iter`]: panics if any entry is
    /// `Full`, since the fragment size is unknown at this layer.
    pub unsafe fn iter_runs(&self) -> impl Iterator<Item = (u32, RangeInclusive<u32>)> + '_ {
        self.inner
            .iter()
            .flat_map(|(&fragment, selection)| match selection {
                RowAddrSelection::Full => panic!("Size of full fragment is unknown"),
                RowAddrSelection::Partial(bitmap) => {
                    let mut iter = bitmap.iter();
                    std::iter::from_fn(move || iter.next_range().map(|r| (fragment, r)))
                }
            })
    }
}

impl CacheCodecImpl for RowAddrTreeMap {
    const TYPE_ID: &'static str = "lance.RowAddrTreeMap";
    const CURRENT_VERSION: u32 = 1;

    fn serialize(&self, w: &mut CacheEntryWriter<'_>) -> Result<()> {
        // A roaring bitmap has its own stable, portable serialization; it is
        // the whole body, so write it raw rather than length-prefixed.
        self.serialize_into(w.raw_writer())
    }

    fn deserialize(r: &mut CacheEntryReader<'_>) -> Result<Self> {
        Self::deserialize_from(r.body().as_ref())
    }
}

impl std::ops::BitOr<Self> for RowAddrTreeMap {
    type Output = Self;

    fn bitor(mut self, rhs: Self) -> Self::Output {
        self |= rhs;
        self
    }
}

impl std::ops::BitOr<&Self> for RowAddrTreeMap {
    type Output = Self;

    fn bitor(mut self, rhs: &Self) -> Self::Output {
        self |= rhs;
        self
    }
}

impl std::ops::BitOrAssign<Self> for RowAddrTreeMap {
    fn bitor_assign(&mut self, rhs: Self) {
        *self |= &rhs;
    }
}

impl std::ops::BitOrAssign<&Self> for RowAddrTreeMap {
    fn bitor_assign(&mut self, rhs: &Self) {
        for (fragment, rhs_set) in &rhs.inner {
            let lhs_set = self.inner.get_mut(fragment);
            if let Some(lhs_set) = lhs_set {
                match lhs_set {
                    RowAddrSelection::Full => {
                        // If the fragment is already selected then there is nothing to do
                    }
                    RowAddrSelection::Partial(lhs_bitmap) => match rhs_set {
                        RowAddrSelection::Full => {
                            *lhs_set = RowAddrSelection::Full;
                        }
                        RowAddrSelection::Partial(rhs_set) => {
                            *lhs_bitmap |= rhs_set;
                        }
                    },
                }
            } else {
                self.inner.insert(*fragment, rhs_set.clone());
            }
        }
    }
}

impl std::ops::BitAnd<Self> for RowAddrTreeMap {
    type Output = Self;

    fn bitand(mut self, rhs: Self) -> Self::Output {
        self &= &rhs;
        self
    }
}

impl std::ops::BitAnd<&Self> for RowAddrTreeMap {
    type Output = Self;

    fn bitand(mut self, rhs: &Self) -> Self::Output {
        self &= rhs;
        self
    }
}

impl std::ops::BitAndAssign<Self> for RowAddrTreeMap {
    fn bitand_assign(&mut self, rhs: Self) {
        *self &= &rhs;
    }
}

impl std::ops::BitAndAssign<&Self> for RowAddrTreeMap {
    fn bitand_assign(&mut self, rhs: &Self) {
        // Remove fragment that aren't on the RHS
        self.inner
            .retain(|fragment, _| rhs.inner.contains_key(fragment));

        // For fragments that are on the RHS, intersect the bitmaps
        for (fragment, mut lhs_set) in &mut self.inner {
            match (&mut lhs_set, rhs.inner.get(fragment)) {
                (_, None) => {} // Already handled by retain
                (_, Some(RowAddrSelection::Full)) => {
                    // Everything selected on RHS, so can leave LHS untouched.
                }
                (RowAddrSelection::Partial(lhs_set), Some(RowAddrSelection::Partial(rhs_set))) => {
                    *lhs_set &= rhs_set;
                }
                (RowAddrSelection::Full, Some(RowAddrSelection::Partial(rhs_set))) => {
                    *lhs_set = RowAddrSelection::Partial(rhs_set.clone());
                }
            }
        }
        // Some bitmaps might now be empty. If they are, we should remove them.
        self.inner.retain(|_, set| match set {
            RowAddrSelection::Partial(set) => !set.is_empty(),
            RowAddrSelection::Full => true,
        });
    }
}

impl std::ops::Sub<Self> for RowAddrTreeMap {
    type Output = Self;

    fn sub(mut self, rhs: Self) -> Self {
        self -= &rhs;
        self
    }
}

impl std::ops::Sub<&Self> for RowAddrTreeMap {
    type Output = Self;

    fn sub(mut self, rhs: &Self) -> Self {
        self -= rhs;
        self
    }
}

impl std::ops::SubAssign<&Self> for RowAddrTreeMap {
    fn sub_assign(&mut self, rhs: &Self) {
        for (fragment, rhs_set) in &rhs.inner {
            match self.inner.get_mut(fragment) {
                None => {}
                Some(RowAddrSelection::Full) => {
                    // If the fragment is already selected then there is nothing to do
                    match rhs_set {
                        RowAddrSelection::Full => {
                            self.inner.remove(fragment);
                        }
                        RowAddrSelection::Partial(rhs_set) => {
                            // This generally won't be hit.
                            let mut set = RoaringBitmap::full();
                            set -= rhs_set;
                            self.inner.insert(*fragment, RowAddrSelection::Partial(set));
                        }
                    }
                }
                Some(RowAddrSelection::Partial(lhs_set)) => match rhs_set {
                    RowAddrSelection::Full => {
                        self.inner.remove(fragment);
                    }
                    RowAddrSelection::Partial(rhs_set) => {
                        *lhs_set -= rhs_set;
                        if lhs_set.is_empty() {
                            self.inner.remove(fragment);
                        }
                    }
                },
            }
        }
    }
}

impl FromIterator<u64> for RowAddrTreeMap {
    fn from_iter<T: IntoIterator<Item = u64>>(iter: T) -> Self {
        let mut inner = BTreeMap::new();
        for row_addr in iter {
            let upper = (row_addr >> 32) as u32;
            let lower = row_addr as u32;
            match inner.get_mut(&upper) {
                None => {
                    let mut set = RoaringBitmap::new();
                    set.insert(lower);
                    inner.insert(upper, RowAddrSelection::Partial(set));
                }
                Some(RowAddrSelection::Full) => {
                    // If the fragment is already selected then there is nothing to do
                }
                Some(RowAddrSelection::Partial(set)) => {
                    set.insert(lower);
                }
            }
        }
        Self { inner }
    }
}

impl<'a> FromIterator<&'a u64> for RowAddrTreeMap {
    fn from_iter<T: IntoIterator<Item = &'a u64>>(iter: T) -> Self {
        Self::from_iter(iter.into_iter().copied())
    }
}

impl From<Range<u64>> for RowAddrTreeMap {
    fn from(range: Range<u64>) -> Self {
        let mut map = Self::default();
        map.insert_range(range);
        map
    }
}

impl From<RangeInclusive<u64>> for RowAddrTreeMap {
    fn from(range: RangeInclusive<u64>) -> Self {
        let mut map = Self::default();
        map.insert_range(range);
        map
    }
}

impl From<RoaringTreemap> for RowAddrTreeMap {
    fn from(roaring: RoaringTreemap) -> Self {
        let mut inner = BTreeMap::new();
        for (fragment, set) in roaring.bitmaps() {
            inner.insert(fragment, RowAddrSelection::Partial(set.clone()));
        }
        Self { inner }
    }
}

impl Extend<u64> for RowAddrTreeMap {
    fn extend<T: IntoIterator<Item = u64>>(&mut self, iter: T) {
        for row_addr in iter {
            let upper = (row_addr >> 32) as u32;
            let lower = row_addr as u32;
            match self.inner.get_mut(&upper) {
                None => {
                    let mut set = RoaringBitmap::new();
                    set.insert(lower);
                    self.inner.insert(upper, RowAddrSelection::Partial(set));
                }
                Some(RowAddrSelection::Full) => {
                    // If the fragment is already selected then there is nothing to do
                }
                Some(RowAddrSelection::Partial(set)) => {
                    set.insert(lower);
                }
            }
        }
    }
}

impl<'a> Extend<&'a u64> for RowAddrTreeMap {
    fn extend<T: IntoIterator<Item = &'a u64>>(&mut self, iter: T) {
        self.extend(iter.into_iter().copied())
    }
}

// Extending with RowAddrTreeMap is basically a cumulative set union
impl Extend<Self> for RowAddrTreeMap {
    fn extend<T: IntoIterator<Item = Self>>(&mut self, iter: T) {
        for other in iter {
            for (fragment, set) in other.inner {
                match self.inner.get_mut(&fragment) {
                    None => {
                        self.inner.insert(fragment, set);
                    }
                    Some(RowAddrSelection::Full) => {
                        // If the fragment is already selected then there is nothing to do
                    }
                    Some(RowAddrSelection::Partial(lhs_set)) => match set {
                        RowAddrSelection::Full => {
                            self.inner.insert(fragment, RowAddrSelection::Full);
                        }
                        RowAddrSelection::Partial(rhs_set) => {
                            *lhs_set |= rhs_set;
                        }
                    },
                }
            }
        }
    }
}

pub fn bitmap_to_ranges(bitmap: &RoaringBitmap) -> Vec<Range<u64>> {
    let mut ranges = Vec::new();
    let mut iter = bitmap.iter();
    while let Some(r) = iter.next_range() {
        ranges.push(*r.start() as u64..(*r.end() as u64 + 1));
    }
    ranges
}

pub fn ranges_to_bitmap(ranges: &[Range<u64>], sorted: bool) -> RoaringBitmap {
    if ranges.is_empty() {
        return RoaringBitmap::new();
    }
    if sorted {
        let sample_size = ranges.len().min(10);
        let avg_len: u64 = ranges
            .iter()
            .take(sample_size)
            .map(|r| r.end - r.start)
            .sum::<u64>()
            / sample_size as u64;
        // from_sorted_iter appends each value in O(1) but must visit every u32.
        // insert_range bulk-fills containers but does a binary search per call.
        // Crossover is ~6: below that, iterating all values is cheaper.
        if avg_len <= 6 {
            return RoaringBitmap::from_sorted_iter(
                ranges.iter().flat_map(|r| r.start as u32..r.end as u32),
            )
            .unwrap();
        }
    }
    let mut bm = RoaringBitmap::new();
    for r in ranges {
        bm.insert_range(r.start as u32..r.end as u32);
    }
    bm
}

/// A set of stable row ids backed by a 64-bit Roaring bitmap.
///
/// This is a thin wrapper around [`RoaringTreemap`]. It represents a
/// collection of unique row ids and provides the common row-set
/// operations defined by [`RowSetOps`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct RowIdSet {
    inner: RoaringTreemap,
}

impl RowIdSet {
    /// Creates an empty set of row ids.
    pub fn new() -> Self {
        Self::default()
    }
    /// Returns an iterator over the contained row ids in ascending order.
    pub fn iter(&self) -> impl Iterator<Item = u64> + '_ {
        self.inner.iter()
    }
    /// Returns the union of `self` and `other`.
    pub fn union(mut self, other: &Self) -> Self {
        self.inner |= &other.inner;
        self
    }
    /// Returns the set difference `self \\ other`.
    pub fn difference(mut self, other: &Self) -> Self {
        self.inner -= &other.inner;
        self
    }
}

impl RowSetOps for RowIdSet {
    type Row = u64;
    fn is_empty(&self) -> bool {
        self.inner.is_empty()
    }
    fn len(&self) -> Option<u64> {
        Some(self.inner.len())
    }
    fn remove(&mut self, row: Self::Row) -> bool {
        self.inner.remove(row)
    }
    fn contains(&self, row: Self::Row) -> bool {
        self.inner.contains(row)
    }
    fn union_all(other: &[&Self]) -> Self {
        let mut result = other
            .first()
            .map_or(Self::default(), |&first| first.clone());
        for set in other {
            result.inner |= &set.inner;
        }
        result
    }
    #[track_caller]
    fn from_sorted_iter<I>(iter: I) -> Result<Self>
    where
        I: IntoIterator<Item = Self::Row>,
    {
        let mut inner = RoaringTreemap::new();
        let mut last: Option<u64> = None;
        for value in iter {
            if let Some(prev) = last
                && value < prev
            {
                return Err(Error::internal(
                    "RowIdSet::from_sorted_iter called with non-sorted input",
                ));
            }
            inner.insert(value);
            last = Some(value);
        }
        Ok(Self { inner })
    }
}

/// A mask over stable row ids based on an allow-list or block-list.
///
/// The semantics mirror [`RowAddrMask`], but operate on stable
/// row ids instead of physical row addresses.
#[derive(Clone, Debug, PartialEq)]
pub enum RowIdMask {
    /// Only the ids in the set are selected.
    AllowList(RowIdSet),
    /// All ids are selected except those in the set.
    BlockList(RowIdSet),
}

impl Default for RowIdMask {
    fn default() -> Self {
        // Empty block list means all rows are allowed
        Self::BlockList(RowIdSet::default())
    }
}
impl RowIdMask {
    /// Create a mask allowing all rows, this is an alias for [`Default`].
    pub fn all_rows() -> Self {
        Self::default()
    }
    /// Create a mask that doesn't allow any row id.
    pub fn allow_nothing() -> Self {
        Self::AllowList(RowIdSet::default())
    }
    /// Create a mask from an allow list.
    pub fn from_allowed(allow_list: RowIdSet) -> Self {
        Self::AllowList(allow_list)
    }
    /// Create a mask from a block list.
    pub fn from_block(block_list: RowIdSet) -> Self {
        Self::BlockList(block_list)
    }
    /// True if the row id is selected by the mask, false otherwise.
    pub fn selected(&self, row_id: u64) -> bool {
        match self {
            Self::AllowList(allow_list) => allow_list.contains(row_id),
            Self::BlockList(block_list) => !block_list.contains(row_id),
        }
    }
    /// Return the indices of the input row ids that are selected by the mask.
    pub fn selected_indices<'a>(&self, row_ids: impl Iterator<Item = &'a u64> + 'a) -> Vec<u64> {
        row_ids
            .enumerate()
            .filter_map(|(idx, row_id)| {
                if self.selected(*row_id) {
                    Some(idx as u64)
                } else {
                    None
                }
            })
            .collect()
    }
    /// Also block the given ids.
    ///
    /// * `AllowList(a)` -> `AllowList(a \\ block_list)`
    /// * `BlockList(b)` -> `BlockList(b union block_list)`
    pub fn also_block(self, block_list: RowIdSet) -> Self {
        match self {
            Self::AllowList(allow_list) => Self::AllowList(allow_list.difference(&block_list)),
            Self::BlockList(existing) => Self::BlockList(existing.union(&block_list)),
        }
    }
    /// Also allow the given ids.
    ///
    /// * `AllowList(a)` -> `AllowList(a union allow_list)`
    /// * `BlockList(b)` -> `BlockList(b \\ allow_list)`
    pub fn also_allow(self, allow_list: RowIdSet) -> Self {
        match self {
            Self::AllowList(existing) => Self::AllowList(existing.union(&allow_list)),
            Self::BlockList(block_list) => Self::BlockList(block_list.difference(&allow_list)),
        }
    }
    /// Return the maximum number of row ids that could be selected by this mask.
    ///
    /// Will be `None` if this is a `BlockList` (unbounded).
    pub fn max_len(&self) -> Option<u64> {
        match self {
            Self::AllowList(selection) => selection.len(),
            Self::BlockList(_) => None,
        }
    }
    /// Iterate over the row ids that are selected by the mask.
    ///
    /// This is only possible if this is an `AllowList`. For a `BlockList`
    /// the domain of possible row ids is unbounded.
    pub fn iter_ids(&self) -> Option<Box<dyn Iterator<Item = u64> + '_>> {
        match self {
            Self::AllowList(allow_list) => Some(Box::new(allow_list.iter())),
            Self::BlockList(_) => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::{prop_assert, prop_assert_eq};

    fn rows(ids: &[u64]) -> RowAddrTreeMap {
        RowAddrTreeMap::from_iter(ids)
    }

    fn assert_mask_selects(mask: &RowAddrMask, selected: &[u64], not_selected: &[u64]) {
        for &id in selected {
            assert!(mask.selected(id), "Expected row {} to be selected", id);
        }
        for &id in not_selected {
            assert!(!mask.selected(id), "Expected row {} to NOT be selected", id);
        }
    }

    fn selected_in_range(mask: &RowAddrMask, range: std::ops::Range<u64>) -> Vec<u64> {
        range.filter(|val| mask.selected(*val)).collect()
    }

    #[test]
    fn test_row_addr_mask_construction() {
        let full_mask = RowAddrMask::all_rows();
        assert_eq!(full_mask.max_len(), None);
        assert_mask_selects(&full_mask, &[0, 1, 4 << 32 | 3], &[]);
        assert_eq!(full_mask.allow_list(), None);
        assert_eq!(full_mask.block_list(), Some(&RowAddrTreeMap::default()));
        assert!(full_mask.iter_addrs().is_none());

        let empty_mask = RowAddrMask::allow_nothing();
        assert_eq!(empty_mask.max_len(), Some(0));
        assert_mask_selects(&empty_mask, &[], &[0, 1, 4 << 32 | 3]);
        assert_eq!(empty_mask.allow_list(), Some(&RowAddrTreeMap::default()));
        assert_eq!(empty_mask.block_list(), None);
        let iter = empty_mask.iter_addrs();
        assert!(iter.is_some());
        assert_eq!(iter.unwrap().count(), 0);

        let allow_list = RowAddrMask::from_allowed(rows(&[10, 20, 30]));
        assert_eq!(allow_list.max_len(), Some(3));
        assert_mask_selects(&allow_list, &[10, 20, 30], &[0, 15, 25, 40]);
        assert_eq!(allow_list.allow_list(), Some(&rows(&[10, 20, 30])));
        assert_eq!(allow_list.block_list(), None);
        let iter = allow_list.iter_addrs();
        assert!(iter.is_some());
        let ids: Vec<u64> = iter.unwrap().map(|addr| addr.into()).collect();
        assert_eq!(ids, vec![10, 20, 30]);

        let mut full_frag = RowAddrTreeMap::default();
        full_frag.insert_fragment(2);
        let allow_list = RowAddrMask::from_allowed(full_frag);
        assert_eq!(allow_list.max_len(), None);
        assert_mask_selects(&allow_list, &[(2 << 32) + 5], &[(3 << 32) + 5]);
        assert!(allow_list.iter_addrs().is_none());
    }

    #[test]
    fn test_selected_indices() {
        // Allow list
        let mask = RowAddrMask::from_allowed(rows(&[10, 20, 40]));
        assert!(mask.selected_indices(std::iter::empty()).is_empty());
        assert_eq!(mask.selected_indices([25, 20, 14, 10].iter()), &[1, 3]);

        // Block list
        let mask = RowAddrMask::from_block(rows(&[10, 20, 40]));
        assert!(mask.selected_indices(std::iter::empty()).is_empty());
        assert_eq!(mask.selected_indices([25, 20, 14, 10].iter()), &[0, 2]);
    }

    #[test]
    fn test_also_allow() {
        // Allow list
        let mask = RowAddrMask::from_allowed(rows(&[10, 20]));
        let new_mask = mask.also_allow(rows(&[20, 30, 40]));
        assert_eq!(new_mask, RowAddrMask::from_allowed(rows(&[10, 20, 30, 40])));

        // Block list
        let mask = RowAddrMask::from_block(rows(&[10, 20, 30]));
        let new_mask = mask.also_allow(rows(&[20, 40]));
        assert_eq!(new_mask, RowAddrMask::from_block(rows(&[10, 30])));
    }

    #[test]
    fn test_also_block() {
        // Allow list
        let mask = RowAddrMask::from_allowed(rows(&[10, 20, 30]));
        let new_mask = mask.also_block(rows(&[20, 40]));
        assert_eq!(new_mask, RowAddrMask::from_allowed(rows(&[10, 30])));

        // Block list
        let mask = RowAddrMask::from_block(rows(&[10, 20]));
        let new_mask = mask.also_block(rows(&[20, 30, 40]));
        assert_eq!(new_mask, RowAddrMask::from_block(rows(&[10, 20, 30, 40])));
    }

    #[test]
    fn test_iter_ids() {
        // Allow list
        let mask = RowAddrMask::from_allowed(rows(&[10, 20, 30]));
        let expected: Vec<_> = [10, 20, 30].into_iter().map(RowAddress::from).collect();
        assert_eq!(mask.iter_addrs().unwrap().collect::<Vec<_>>(), expected);

        // Allow list with full fragment
        let mut inner = RowAddrTreeMap::default();
        inner.insert_fragment(10);
        let mask = RowAddrMask::from_allowed(inner);
        assert!(mask.iter_addrs().is_none());

        // Block list
        let mask = RowAddrMask::from_block(rows(&[10, 20, 30]));
        assert!(mask.iter_addrs().is_none());
    }

    #[test]
    fn test_row_addr_mask_not() {
        let allow_list = RowAddrMask::from_allowed(rows(&[1, 2, 3]));
        let block_list = !allow_list.clone();
        assert_eq!(block_list, RowAddrMask::from_block(rows(&[1, 2, 3])));
        // Can roundtrip by negating again
        assert_eq!(!block_list, allow_list);
    }

    #[test]
    fn test_ops() {
        let mask = RowAddrMask::default();
        assert_mask_selects(&mask, &[1, 5], &[]);

        let block_list = mask.also_block(rows(&[0, 5, 15]));
        assert_mask_selects(&block_list, &[1], &[5]);

        let allow_list = RowAddrMask::from_allowed(rows(&[0, 2, 5]));
        assert_mask_selects(&allow_list, &[5], &[1]);

        let combined = block_list & allow_list;
        assert_mask_selects(&combined, &[2], &[0, 5]);

        let other = RowAddrMask::from_allowed(rows(&[3]));
        let combined = combined | other;
        assert_mask_selects(&combined, &[2, 3], &[0, 5]);

        let block_list = RowAddrMask::from_block(rows(&[0]));
        let allow_list = RowAddrMask::from_allowed(rows(&[3]));

        let combined = block_list | allow_list;
        assert_mask_selects(&combined, &[1], &[]);
    }

    #[test]
    fn test_logical_and() {
        let allow1 = RowAddrMask::from_allowed(rows(&[0, 1]));
        let block1 = RowAddrMask::from_block(rows(&[1, 2]));
        let allow2 = RowAddrMask::from_allowed(rows(&[1, 2, 3, 4]));
        let block2 = RowAddrMask::from_block(rows(&[3, 4]));

        fn check(lhs: &RowAddrMask, rhs: &RowAddrMask, expected: &[u64]) {
            for mask in [lhs.clone() & rhs.clone(), rhs.clone() & lhs.clone()] {
                assert_eq!(selected_in_range(&mask, 0..10), expected);
            }
        }

        // Allow & Allow
        check(&allow1, &allow1, &[0, 1]);
        check(&allow1, &allow2, &[1]);

        // Block & Block
        check(&block1, &block1, &[0, 3, 4, 5, 6, 7, 8, 9]);
        check(&block1, &block2, &[0, 5, 6, 7, 8, 9]);

        // Allow & Block
        check(&allow1, &block1, &[0]);
        check(&allow1, &block2, &[0, 1]);
        check(&allow2, &block1, &[3, 4]);
        check(&allow2, &block2, &[1, 2]);
    }

    #[test]
    fn test_logical_or() {
        let allow1 = RowAddrMask::from_allowed(rows(&[5, 6, 7, 8, 9]));
        let block1 = RowAddrMask::from_block(rows(&[5, 6]));
        let mixed1 = allow1.clone().also_block(rows(&[5, 6]));
        let allow2 = RowAddrMask::from_allowed(rows(&[2, 3, 4, 5, 6, 7, 8]));
        let block2 = RowAddrMask::from_block(rows(&[4, 5]));
        let mixed2 = allow2.clone().also_block(rows(&[4, 5]));

        fn check(lhs: &RowAddrMask, rhs: &RowAddrMask, expected: &[u64]) {
            for mask in [lhs.clone() | rhs.clone(), rhs.clone() | lhs.clone()] {
                assert_eq!(selected_in_range(&mask, 0..10), expected);
            }
        }

        check(&allow1, &allow1, &[5, 6, 7, 8, 9]);
        check(&block1, &block1, &[0, 1, 2, 3, 4, 7, 8, 9]);
        check(&mixed1, &mixed1, &[7, 8, 9]);
        check(&allow2, &allow2, &[2, 3, 4, 5, 6, 7, 8]);
        check(&block2, &block2, &[0, 1, 2, 3, 6, 7, 8, 9]);
        check(&mixed2, &mixed2, &[2, 3, 6, 7, 8]);

        check(&allow1, &block1, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        check(&allow1, &mixed1, &[5, 6, 7, 8, 9]);
        check(&allow1, &allow2, &[2, 3, 4, 5, 6, 7, 8, 9]);
        check(&allow1, &block2, &[0, 1, 2, 3, 5, 6, 7, 8, 9]);
        check(&allow1, &mixed2, &[2, 3, 5, 6, 7, 8, 9]);
        check(&block1, &mixed1, &[0, 1, 2, 3, 4, 7, 8, 9]);
        check(&block1, &allow2, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        check(&block1, &block2, &[0, 1, 2, 3, 4, 6, 7, 8, 9]);
        check(&block1, &mixed2, &[0, 1, 2, 3, 4, 6, 7, 8, 9]);
        check(&mixed1, &allow2, &[2, 3, 4, 5, 6, 7, 8, 9]);
        check(&mixed1, &block2, &[0, 1, 2, 3, 6, 7, 8, 9]);
        check(&mixed1, &mixed2, &[2, 3, 6, 7, 8, 9]);
        check(&allow2, &block2, &[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
        check(&allow2, &mixed2, &[2, 3, 4, 5, 6, 7, 8]);
        check(&block2, &mixed2, &[0, 1, 2, 3, 6, 7, 8, 9]);
    }

    #[test]
    fn test_deserialize_legacy_format() {
        // Test that we can deserialize the old format where both allow_list
        // and block_list could be present in the serialized form.
        //
        // The old format (before this PR) used a struct with both allow_list and block_list
        // fields. The new format uses an enum. The deserialization code should handle
        // the case where both lists are present by converting to AllowList(allow - block).

        // Create the RowIdTreeMaps and serialize them directly
        let allow = rows(&[1, 2, 3, 4, 5, 10, 15]);
        let block = rows(&[2, 4, 15]);

        // Serialize using the stable RowIdTreeMap serialization format
        let block_bytes = {
            let mut buf = Vec::with_capacity(block.serialized_size());
            block.serialize_into(&mut buf).unwrap();
            buf
        };
        let allow_bytes = {
            let mut buf = Vec::with_capacity(allow.serialized_size());
            allow.serialize_into(&mut buf).unwrap();
            buf
        };

        // Construct a binary array with both values present (simulating old format)
        let old_format_array =
            BinaryArray::from_opt_vec(vec![Some(&block_bytes), Some(&allow_bytes)]);

        // Deserialize - should handle this by creating AllowList(allow - block)
        let deserialized = RowAddrMask::from_arrow(&old_format_array).unwrap();

        // The expected result: AllowList([1, 2, 3, 4, 5, 10, 15] - [2, 4, 15]) = [1, 3, 5, 10]
        assert_mask_selects(&deserialized, &[1, 3, 5, 10], &[2, 4, 15]);
        assert!(
            deserialized.allow_list().is_some(),
            "Should deserialize to AllowList variant"
        );
    }

    #[test]
    fn test_roundtrip_arrow() {
        let row_addrs = rows(&[1, 2, 3, 100, 2000]);

        // Allow list
        let original = RowAddrMask::from_allowed(row_addrs.clone());
        let array = original.into_arrow().unwrap();
        assert_eq!(RowAddrMask::from_arrow(&array).unwrap(), original);

        // Block list
        let original = RowAddrMask::from_block(row_addrs);
        let array = original.into_arrow().unwrap();
        assert_eq!(RowAddrMask::from_arrow(&array).unwrap(), original);
    }

    #[test]
    fn test_deserialize_legacy_empty_lists() {
        // Case 1: Both None (should become all_rows)
        let array = BinaryArray::from_opt_vec(vec![None, None]);
        let mask = RowAddrMask::from_arrow(&array).unwrap();
        assert_mask_selects(&mask, &[0, 100, u64::MAX], &[]);

        // Case 2: Only block list (no allow list)
        let block = rows(&[5, 10]);
        let block_bytes = {
            let mut buf = Vec::with_capacity(block.serialized_size());
            block.serialize_into(&mut buf).unwrap();
            buf
        };
        let array = BinaryArray::from_opt_vec(vec![Some(&block_bytes[..]), None]);
        let mask = RowAddrMask::from_arrow(&array).unwrap();
        assert_mask_selects(&mask, &[0, 15], &[5, 10]);

        // Case 3: Only allow list (no block list)
        let allow = rows(&[5, 10]);
        let allow_bytes = {
            let mut buf = Vec::with_capacity(allow.serialized_size());
            allow.serialize_into(&mut buf).unwrap();
            buf
        };
        let array = BinaryArray::from_opt_vec(vec![None, Some(&allow_bytes[..])]);
        let mask = RowAddrMask::from_arrow(&array).unwrap();
        assert_mask_selects(&mask, &[5, 10], &[0, 15]);
    }

    #[test]
    fn test_map_insert() {
        let mut map = RowAddrTreeMap::default();

        assert!(!map.contains(20));
        assert!(map.insert(20));
        assert!(map.contains(20));
        assert!(!map.insert(20)); // Inserting again should be no-op

        let bitmap = map.get_fragment_bitmap(0);
        assert!(bitmap.is_some());
        let bitmap = bitmap.unwrap();
        assert_eq!(bitmap.len(), 1);

        assert!(map.get_fragment_bitmap(1).is_none());

        map.insert_fragment(0);
        assert!(map.contains(0));
        assert!(!map.insert(0)); // Inserting into full fragment should be no-op
        assert!(map.get_fragment_bitmap(0).is_none());
    }

    #[test]
    fn test_map_insert_range() {
        let ranges = &[
            (0..10),
            (40..500),
            ((u32::MAX as u64 - 10)..(u32::MAX as u64 + 20)),
        ];

        for range in ranges {
            let mut mask = RowAddrTreeMap::default();

            let count = mask.insert_range(range.clone());
            let expected = range.end - range.start;
            assert_eq!(count, expected);

            let count = mask.insert_range(range.clone());
            assert_eq!(count, 0);

            let new_range = range.start + 5..range.end + 5;
            let count = mask.insert_range(new_range.clone());
            assert_eq!(count, 5);
        }

        let mut mask = RowAddrTreeMap::default();
        let count = mask.insert_range(..10);
        assert_eq!(count, 10);
        assert!(mask.contains(0));

        let count = mask.insert_range(20..=24);
        assert_eq!(count, 5);

        mask.insert_fragment(0);
        let count = mask.insert_range(100..200);
        assert_eq!(count, 0);
    }

    #[test]
    fn test_map_remove() {
        let mut mask = RowAddrTreeMap::default();

        assert!(!mask.remove(20));

        mask.insert(20);
        assert!(mask.contains(20));
        assert!(mask.remove(20));
        assert!(!mask.contains(20));

        mask.insert_range(10..=20);
        assert!(mask.contains(15));
        assert!(mask.remove(15));
        assert!(!mask.contains(15));

        // We don't test removing from a full fragment, because that would take
        // a lot of memory.
    }

    #[test]
    fn test_map_mask() {
        let mask = rows(&[0, 1, 2]);
        let mask2 = rows(&[0, 2, 3]);

        let allow_list = RowAddrMask::AllowList(mask2.clone());
        let mut actual = mask.clone();
        actual.mask(&allow_list);
        assert_eq!(actual, rows(&[0, 2]));

        let block_list = RowAddrMask::BlockList(mask2);
        let mut actual = mask;
        actual.mask(&block_list);
        assert_eq!(actual, rows(&[1]));
    }

    #[test]
    #[should_panic(expected = "Size of full fragment is unknown")]
    fn test_map_insert_full_fragment_row() {
        let mut mask = RowAddrTreeMap::default();
        mask.insert_fragment(0);

        unsafe {
            let _ = mask.into_addr_iter().collect::<Vec<u64>>();
        }
    }

    #[test]
    fn test_map_into_addr_iter() {
        let mut mask = RowAddrTreeMap::default();
        mask.insert(0);
        mask.insert(1);
        mask.insert(1 << 32 | 5);
        mask.insert(2 << 32 | 10);

        let expected = vec![0u64, 1, 1 << 32 | 5, 2 << 32 | 10];
        let actual: Vec<u64> = unsafe { mask.into_addr_iter().collect() };
        assert_eq!(actual, expected);
    }

    #[test]
    fn test_map_iter_runs() {
        // Three contiguous regions across two fragments.
        let mut mask = RowAddrTreeMap::default();
        mask.insert_range(0..3);
        mask.insert_range(10..15);
        mask.insert_range((1u64 << 32) + 100..(1u64 << 32) + 103);

        // SAFETY: only Partial entries.
        let runs: Vec<(u32, RangeInclusive<u32>)> = unsafe { mask.iter_runs().collect() };
        assert_eq!(runs, vec![(0, 0..=2), (0, 10..=14), (1, 100..=102)]);
    }

    #[test]
    fn test_map_iter_runs_matches_into_addr_iter() {
        // Confirm iter_runs and into_addr_iter agree on a non-trivial shape.
        let mut mask = RowAddrTreeMap::default();
        mask.insert_range(5..7);
        mask.insert_range(11..12);
        mask.insert_range(20..25);
        mask.insert_range((1u64 << 32)..(1u64 << 32) + 3);

        let from_runs: Vec<u64> = unsafe { mask.iter_runs() }
            .flat_map(|(frag, run)| {
                let frag = u64::from(frag);
                (*run.start()..=*run.end()).map(move |v| (frag << 32) | u64::from(v))
            })
            .collect();
        let from_bits: Vec<u64> = unsafe { mask.clone().into_addr_iter() }.collect();
        assert_eq!(from_runs, from_bits);
    }

    #[test]
    fn test_map_from() {
        let map = RowAddrTreeMap::from(10..12);
        assert!(map.contains(10));
        assert!(map.contains(11));
        assert!(!map.contains(12));
        assert!(!map.contains(3));

        let map = RowAddrTreeMap::from(10..=12);
        assert!(map.contains(10));
        assert!(map.contains(11));
        assert!(map.contains(12));
        assert!(!map.contains(3));
    }

    #[test]
    fn test_map_from_roaring() {
        let bitmap = RoaringTreemap::from_iter(&[0, 1, 1 << 32]);
        let map = RowAddrTreeMap::from(bitmap);
        assert!(map.contains(0) && map.contains(1) && map.contains(1 << 32));
        assert!(!map.contains(2));
    }

    #[test]
    fn test_map_extend() {
        let mut map = RowAddrTreeMap::default();
        map.insert(0);
        map.insert_fragment(1);

        let other_rows = [0, 2, 1 << 32 | 10, 3 << 32 | 5];
        map.extend(other_rows.iter().copied());

        assert!(map.contains(0));
        assert!(map.contains(2));
        assert!(map.contains(1 << 32 | 5));
        assert!(map.contains(1 << 32 | 10));
        assert!(map.contains(3 << 32 | 5));
        assert!(!map.contains(3));
    }

    #[test]
    fn test_map_extend_other_maps() {
        let mut map = RowAddrTreeMap::default();
        map.insert(0);
        map.insert_fragment(1);
        map.insert(4 << 32);

        let mut other_map = rows(&[0, 2, 1 << 32 | 10, 3 << 32 | 5]);
        other_map.insert_fragment(4);
        map.extend(std::iter::once(other_map));

        for id in [
            0,
            2,
            1 << 32 | 5,
            1 << 32 | 10,
            3 << 32 | 5,
            4 << 32,
            4 << 32 | 7,
        ] {
            assert!(map.contains(id), "Expected {} to be contained", id);
        }
        assert!(!map.contains(3));
    }

    proptest::proptest! {
        #[test]
        fn test_map_serialization_roundtrip(
            values in proptest::collection::vec(
                (0..u32::MAX, proptest::option::of(proptest::collection::vec(0..u32::MAX, 0..1000))),
                0..10
            )
        ) {
            let mut mask = RowAddrTreeMap::default();
            for (fragment, rows) in values {
                if let Some(rows) = rows {
                    let bitmap = RoaringBitmap::from_iter(rows);
                    mask.insert_bitmap(fragment, bitmap);
                } else {
                    mask.insert_fragment(fragment);
                }
            }

            let mut data = Vec::new();
            mask.serialize_into(&mut data).unwrap();
            let deserialized = RowAddrTreeMap::deserialize_from(data.as_slice()).unwrap();
            prop_assert_eq!(mask, deserialized);
        }

        #[test]
        fn test_map_intersect(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
            right_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            right_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowAddrTreeMap::default();
            for fragment in left_full_fragments.clone() {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowAddrTreeMap::default();
            for fragment in right_full_fragments.clone() {
                right.insert_fragment(fragment);
            }
            right.extend(right_rows.iter().copied());

            let mut expected = RowAddrTreeMap::default();
            for fragment in &left_full_fragments {
                if right_full_fragments.contains(fragment) {
                    expected.insert_fragment(*fragment);
                }
            }

            let left_in_right = left_rows.iter().filter(|row| {
                right_rows.contains(row)
                    || right_full_fragments.contains(&((*row >> 32) as u32))
            });
            expected.extend(left_in_right);
            let right_in_left = right_rows.iter().filter(|row| {
                left_rows.contains(row)
                    || left_full_fragments.contains(&((*row >> 32) as u32))
            });
            expected.extend(right_in_left);

            let actual = left & right;
            prop_assert_eq!(expected, actual);
        }

        #[test]
        fn test_map_union(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
            right_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            right_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowAddrTreeMap::default();
            for fragment in left_full_fragments.clone() {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowAddrTreeMap::default();
            for fragment in right_full_fragments.clone() {
                right.insert_fragment(fragment);
            }
            right.extend(right_rows.iter().copied());

            let mut expected = RowAddrTreeMap::default();
            for fragment in left_full_fragments {
                expected.insert_fragment(fragment);
            }
            for fragment in right_full_fragments {
                expected.insert_fragment(fragment);
            }

            let combined_rows = left_rows.iter().chain(right_rows.iter());
            expected.extend(combined_rows);

            let actual = left | right;
            for actual_key_val in &actual.inner {
                proptest::prop_assert!(expected.inner.contains_key(actual_key_val.0));
                let expected_val = expected.inner.get(actual_key_val.0).unwrap();
                prop_assert_eq!(
                    actual_key_val.1,
                    expected_val,
                    "error on key {}",
                    actual_key_val.0
                );
            }
            prop_assert_eq!(expected, actual);
        }

        #[test]
        fn test_map_subassign_rows(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
            right_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowAddrTreeMap::default();
            for fragment in left_full_fragments {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowAddrTreeMap::default();
            right.extend(right_rows.iter().copied());

            let mut expected = left.clone();
            for row in right_rows {
                expected.remove(row);
            }

            left -= &right;
            prop_assert_eq!(expected, left);
        }

        #[test]
        fn test_map_subassign_frags(
            left_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            right_full_fragments in proptest::collection::vec(0..u32::MAX, 0..10),
            left_rows in proptest::collection::vec(0..u64::MAX, 0..1000),
        ) {
            let mut left = RowAddrTreeMap::default();
            for fragment in left_full_fragments {
                left.insert_fragment(fragment);
            }
            left.extend(left_rows.iter().copied());

            let mut right = RowAddrTreeMap::default();
            for fragment in right_full_fragments.clone() {
                right.insert_fragment(fragment);
            }

            let mut expected = left.clone();
            for fragment in right_full_fragments {
                expected.inner.remove(&fragment);
            }

            left -= &right;
            prop_assert_eq!(expected, left);
        }

        #[test]
        fn test_from_sorted_iter(
            mut rows in proptest::collection::vec(0..u64::MAX, 0..1000)
        ) {
            rows.sort();
            let num_rows = rows.len();
            let mask = RowAddrTreeMap::from_sorted_iter(rows).unwrap();
            prop_assert_eq!(mask.len(), Some(num_rows as u64));
        }


    }

    #[test]
    fn test_row_addr_selection_deep_size_of() {
        use lance_core::deepsize::DeepSizeOf;

        // Test Full variant - should have minimal size (just the enum discriminant)
        let full = RowAddrSelection::Full;
        let full_size = full.deep_size_of();
        // Full variant has no heap allocations beyond the enum itself
        assert!(full_size < 100); // Small sanity check

        // Test Partial variant - should include bitmap size
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert_range(0..100);
        let partial = RowAddrSelection::Partial(bitmap.clone());
        let partial_size = partial.deep_size_of();
        // Partial variant should be larger due to bitmap
        assert!(partial_size >= bitmap.serialized_size());
    }

    #[test]
    fn test_row_addr_selection_union_all_with_full() {
        let full = RowAddrSelection::Full;
        let partial = RowAddrSelection::Partial(RoaringBitmap::from_iter(&[1, 2, 3]));

        assert!(matches!(
            RowAddrSelection::union_all(&[&full, &partial]),
            RowAddrSelection::Full
        ));

        let partial2 = RowAddrSelection::Partial(RoaringBitmap::from_iter(&[4, 5, 6]));
        let RowAddrSelection::Partial(bitmap) = RowAddrSelection::union_all(&[&partial, &partial2])
        else {
            panic!("Expected Partial");
        };
        assert!(bitmap.contains(1) && bitmap.contains(4));
    }

    #[test]
    fn test_insert_range_unbounded_start() {
        let mut map = RowAddrTreeMap::default();

        // Test exclusive start bound
        let count = map.insert_range((std::ops::Bound::Excluded(5), std::ops::Bound::Included(10)));
        assert_eq!(count, 5); // 6, 7, 8, 9, 10
        assert!(!map.contains(5));
        assert!(map.contains(6));
        assert!(map.contains(10));

        // Test unbounded end
        let mut map2 = RowAddrTreeMap::default();
        let count = map2.insert_range(0..5);
        assert_eq!(count, 5);
        assert!(map2.contains(0));
        assert!(map2.contains(4));
        assert!(!map2.contains(5));
    }

    #[test]
    fn test_remove_from_full_fragment() {
        let mut map = RowAddrTreeMap::default();
        map.insert_fragment(0);

        // Verify it's a full fragment - get_fragment_bitmap returns None for Full
        for id in [0, 100, u32::MAX as u64] {
            assert!(map.contains(id));
        }
        assert!(map.get_fragment_bitmap(0).is_none());

        // Remove a value from the full fragment
        assert!(map.remove(50));

        // Now it should be partial (a full RoaringBitmap minus one value)
        assert!(map.contains(0) && !map.contains(50) && map.contains(100));
        assert!(map.get_fragment_bitmap(0).is_some());
    }

    #[test]
    fn test_retain_fragments() {
        let mut map = RowAddrTreeMap::default();
        map.insert(0); // fragment 0
        map.insert(1 << 32 | 5); // fragment 1
        map.insert(2 << 32 | 10); // fragment 2
        map.insert_fragment(3); // fragment 3

        map.retain_fragments([0, 2]);

        assert!(map.contains(0) && map.contains(2 << 32 | 10));
        assert!(!map.contains(1 << 32 | 5) && !map.contains(3 << 32));
    }

    #[test]
    fn test_bitor_assign_full_fragment() {
        // Test BitOrAssign when LHS has Full and RHS has Partial
        let mut map1 = RowAddrTreeMap::default();
        map1.insert_fragment(0);
        let mut map2 = RowAddrTreeMap::default();
        map2.insert(5);

        map1 |= &map2;
        // Full | Partial = Full
        assert!(map1.contains(0) && map1.contains(5) && map1.contains(100));

        // Test BitOrAssign when LHS has Partial and RHS has Full
        let mut map3 = RowAddrTreeMap::default();
        map3.insert(5);
        let mut map4 = RowAddrTreeMap::default();
        map4.insert_fragment(0);

        map3 |= &map4;
        // Partial | Full = Full
        assert!(map3.contains(0) && map3.contains(5) && map3.contains(100));
    }

    #[test]
    fn test_bitand_assign_full_fragments() {
        // Test BitAndAssign when both have Full for same fragment
        let mut map1 = RowAddrTreeMap::default();
        map1.insert_fragment(0);
        let mut map2 = RowAddrTreeMap::default();
        map2.insert_fragment(0);

        map1 &= &map2;
        // Full & Full = Full
        assert!(map1.contains(0) && map1.contains(100));

        // Test BitAndAssign when LHS Full, RHS Partial
        let mut map3 = RowAddrTreeMap::default();
        map3.insert_fragment(0);
        let mut map4 = RowAddrTreeMap::default();
        map4.insert(5);
        map4.insert(10);

        map3 &= &map4;
        // Full & Partial([5,10]) = Partial([5,10])
        assert!(map3.contains(5) && map3.contains(10));
        assert!(!map3.contains(0) && !map3.contains(100));

        // Test that empty intersection results in removal
        let mut map5 = RowAddrTreeMap::default();
        map5.insert(5);
        let mut map6 = RowAddrTreeMap::default();
        map6.insert(10);

        map5 &= &map6;
        assert!(map5.is_empty());
    }

    #[test]
    fn test_sub_assign_with_full_fragments() {
        // Test SubAssign when LHS is Full and RHS is Partial
        let mut map1 = RowAddrTreeMap::default();
        map1.insert_fragment(0);
        let mut map2 = RowAddrTreeMap::default();
        map2.insert(5);
        map2.insert(10);

        map1 -= &map2;
        // Full - Partial([5,10]) = Full minus those values
        assert!(map1.contains(0) && map1.contains(100));
        assert!(!map1.contains(5) && !map1.contains(10));

        // Test SubAssign when both are Full for same fragment
        let mut map3 = RowAddrTreeMap::default();
        map3.insert_fragment(0);
        let mut map4 = RowAddrTreeMap::default();
        map4.insert_fragment(0);

        map3 -= &map4;
        // Full - Full = empty
        assert!(map3.is_empty());

        // Test SubAssign when LHS is Partial and RHS is Full
        let mut map5 = RowAddrTreeMap::default();
        map5.insert(5);
        map5.insert(10);
        let mut map6 = RowAddrTreeMap::default();
        map6.insert_fragment(0);

        map5 -= &map6;
        // Partial - Full = empty
        assert!(map5.is_empty());
    }

    #[test]
    fn test_from_iterator_with_full_fragment() {
        // Test that inserting into a full fragment is a no-op
        let mut map = RowAddrTreeMap::default();
        map.insert_fragment(0);

        // Extend with values that would go into fragment 0
        map.extend([5u64, 10, 100].iter());

        // Should still be full fragment
        for id in [0, 5, 10, 100, u32::MAX as u64] {
            assert!(map.contains(id));
        }
    }

    #[test]
    fn test_insert_range_excluded_end() {
        // Test excluded end bound (line 391-393)
        let mut map = RowAddrTreeMap::default();
        // Using RangeFrom with small range won't hit the unbounded case
        // Instead test Bound::Excluded for end
        let count = map.insert_range((std::ops::Bound::Included(5), std::ops::Bound::Excluded(10)));
        assert_eq!(count, 5); // 5, 6, 7, 8, 9
        assert!(map.contains(5));
        assert!(map.contains(9));
        assert!(!map.contains(10));
    }

    #[test]
    fn test_bitand_assign_owned() {
        // Test BitAndAssign<Self> (owned, not reference)
        let mut map1 = RowAddrTreeMap::default();
        map1.insert(5);
        map1.insert(10);

        // Using owned rhs (not reference)
        map1 &= rows(&[5, 15]);

        assert!(map1.contains(5));
        assert!(!map1.contains(10) && !map1.contains(15));
    }

    #[test]
    fn test_from_iter_with_full_fragment() {
        // When we collect into RowAddrTreeMap, it should handle duplicates
        let map: RowAddrTreeMap = vec![5u64, 10, 100].into_iter().collect();
        assert!(map.contains(5) && map.contains(10));

        // Test that extending a map with full fragment ignores new values
        let mut map = RowAddrTreeMap::default();
        map.insert_fragment(0);
        for val in [5, 10, 100] {
            map.insert(val); // This should be no-op since fragment is full
        }
        // Still full fragment
        for id in [0, 5, u32::MAX as u64] {
            assert!(map.contains(id));
        }
    }

    // ============================================================================
    // Tests for bitmap_to_ranges / ranges_to_bitmap
    // ============================================================================

    #[test]
    fn test_bitmap_to_ranges_empty() {
        let bm = RoaringBitmap::new();
        assert!(bitmap_to_ranges(&bm).is_empty());
    }

    #[test]
    fn test_bitmap_to_ranges_single() {
        let bm = RoaringBitmap::from_iter([5]);
        assert_eq!(bitmap_to_ranges(&bm), vec![5..6]);
    }

    #[test]
    fn test_bitmap_to_ranges_contiguous() {
        let mut bm = RoaringBitmap::new();
        bm.insert_range(10..20);
        assert_eq!(bitmap_to_ranges(&bm), vec![10..20]);
    }

    #[test]
    fn test_bitmap_to_ranges_multiple() {
        let mut bm = RoaringBitmap::new();
        bm.insert_range(0..3);
        bm.insert_range(10..15);
        bm.insert(100);
        assert_eq!(bitmap_to_ranges(&bm), vec![0..3, 10..15, 100..101]);
    }

    #[test]
    fn test_ranges_to_bitmap_empty() {
        let bm = ranges_to_bitmap(&[], true);
        assert!(bm.is_empty());
    }

    #[test]
    fn test_ranges_to_bitmap_sorted_short_ranges() {
        // avg len = 1, uses from_sorted_iter path
        let ranges = vec![0..1, 5..6, 10..11];
        let bm = ranges_to_bitmap(&ranges, true);
        assert!(bm.contains(0) && bm.contains(5) && bm.contains(10));
        assert_eq!(bm.len(), 3);
    }

    #[test]
    fn test_ranges_to_bitmap_sorted_long_ranges() {
        // avg len = 100, uses insert_range path
        let ranges = vec![0..100, 200..300];
        let bm = ranges_to_bitmap(&ranges, true);
        assert_eq!(bm.len(), 200);
        assert!(bm.contains(0) && bm.contains(99));
        assert!(!bm.contains(100));
        assert!(bm.contains(200) && bm.contains(299));
    }

    #[test]
    fn test_ranges_to_bitmap_unsorted() {
        let ranges = vec![200..300, 0..100];
        let bm = ranges_to_bitmap(&ranges, false);
        assert_eq!(bm.len(), 200);
        assert!(bm.contains(0) && bm.contains(250));
    }

    #[test]
    fn test_bitmap_ranges_roundtrip() {
        let mut original = RoaringBitmap::new();
        original.insert_range(0..50);
        original.insert_range(100..200);
        original.insert(500);
        original.insert_range(1000..1010);

        let ranges = bitmap_to_ranges(&original);
        let reconstructed = ranges_to_bitmap(&ranges, true);
        assert_eq!(original, reconstructed);
    }

    // ============================================================================
    // Tests for RowIdSet
    // ============================================================================

    fn row_ids(ids: &[u64]) -> RowIdSet {
        let mut set = RowIdSet::new();
        for &id in ids {
            set.inner.insert(id);
        }
        set
    }

    #[test]
    fn test_row_id_set_construction() {
        let set = RowIdSet::new();
        assert!(set.is_empty());
        assert_eq!(set.len(), Some(0));

        let set = row_ids(&[10, 20, 30]);
        assert!(!set.is_empty());
        assert_eq!(set.len(), Some(3));
        assert!(set.contains(10));
        assert!(set.contains(20));
        assert!(set.contains(30));
        assert!(!set.contains(15));
    }

    #[test]
    fn test_row_id_set_remove() {
        let mut set = row_ids(&[10, 20, 30]);

        assert!(!set.remove(15)); // Not present
        assert_eq!(set.len(), Some(3));

        assert!(set.remove(20)); // Present
        assert_eq!(set.len(), Some(2));
        assert!(!set.contains(20));
        assert!(set.contains(10));
        assert!(set.contains(30));

        assert!(!set.remove(20)); // Already removed
    }

    #[test]
    fn test_row_id_set_union() {
        let set1 = row_ids(&[10, 20, 30]);
        let set2 = row_ids(&[20, 30, 40]);

        let result = set1.union(&set2);
        assert_eq!(result.len(), Some(4));
        for id in [10, 20, 30, 40] {
            assert!(result.contains(id));
        }
    }

    #[test]
    fn test_row_id_set_difference() {
        let set1 = row_ids(&[10, 20, 30, 40]);
        let set2 = row_ids(&[20, 40]);

        let result = set1.difference(&set2);
        assert_eq!(result.len(), Some(2));
        assert!(result.contains(10));
        assert!(result.contains(30));
        assert!(!result.contains(20));
        assert!(!result.contains(40));
    }

    #[test]
    fn test_row_id_set_union_all() {
        let set1 = row_ids(&[10, 20]);
        let set2 = row_ids(&[20, 30]);
        let set3 = row_ids(&[30, 40]);

        let result = RowIdSet::union_all(&[&set1, &set2, &set3]);
        assert_eq!(result.len(), Some(4));
        for id in [10, 20, 30, 40] {
            assert!(result.contains(id));
        }

        // Empty slice should return empty set
        let result = RowIdSet::union_all(&[]);
        assert!(result.is_empty());
    }

    #[test]
    fn test_row_id_set_iter() {
        let set = row_ids(&[10, 20, 30]);
        let collected: Vec<u64> = set.iter().collect();
        assert_eq!(collected, vec![10, 20, 30]);

        let empty = RowIdSet::new();
        assert_eq!(empty.iter().count(), 0);
    }

    #[test]
    fn test_row_id_set_from_sorted_iter() {
        // Valid sorted input
        let set = RowIdSet::from_sorted_iter([10, 20, 30, 40]).unwrap();
        assert_eq!(set.len(), Some(4));
        for id in [10, 20, 30, 40] {
            assert!(set.contains(id));
        }

        // Empty iterator
        let set = RowIdSet::from_sorted_iter(std::iter::empty()).unwrap();
        assert!(set.is_empty());

        // Single element
        let set = RowIdSet::from_sorted_iter([42]).unwrap();
        assert_eq!(set.len(), Some(1));
        assert!(set.contains(42));
    }

    #[test]
    fn test_row_id_set_from_sorted_iter_unsorted() {
        // Non-sorted input should return error
        let result = RowIdSet::from_sorted_iter([30, 10, 20]);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("non-sorted"));
    }

    #[test]
    fn test_row_id_set_large_values() {
        // Test with large u64 values
        let large_ids = [u64::MAX - 10, u64::MAX - 5, u64::MAX - 1];
        let set = row_ids(&large_ids);

        for &id in &large_ids {
            assert!(set.contains(id));
        }
        assert!(!set.contains(u64::MAX));
        assert_eq!(set.len(), Some(3));
    }

    // ============================================================================
    // Tests for RowIdMask
    // ============================================================================

    fn assert_row_id_mask_selects(mask: &RowIdMask, selected: &[u64], not_selected: &[u64]) {
        for &id in selected {
            assert!(mask.selected(id), "Expected row id {} to be selected", id);
        }
        for &id in not_selected {
            assert!(
                !mask.selected(id),
                "Expected row id {} to NOT be selected",
                id
            );
        }
    }

    #[test]
    fn test_row_id_mask_construction() {
        let full_mask = RowIdMask::all_rows();
        assert_eq!(full_mask.max_len(), None);
        assert_row_id_mask_selects(&full_mask, &[0, 1, 100, u64::MAX - 1], &[]);

        let empty_mask = RowIdMask::allow_nothing();
        assert_eq!(empty_mask.max_len(), Some(0));
        assert_row_id_mask_selects(&empty_mask, &[], &[0, 1, 100]);

        let allow_list = RowIdMask::from_allowed(row_ids(&[10, 20, 30]));
        assert_eq!(allow_list.max_len(), Some(3));
        assert_row_id_mask_selects(&allow_list, &[10, 20, 30], &[0, 15, 25, 40]);

        let block_list = RowIdMask::from_block(row_ids(&[10, 20, 30]));
        assert_eq!(block_list.max_len(), None);
        assert_row_id_mask_selects(&block_list, &[0, 15, 25, 40], &[10, 20, 30]);
    }

    #[test]
    fn test_row_id_mask_selected_indices() {
        // Allow list
        let mask = RowIdMask::from_allowed(row_ids(&[10, 20, 40]));
        assert!(mask.selected_indices(std::iter::empty()).is_empty());
        assert_eq!(mask.selected_indices([25, 20, 14, 10].iter()), &[1, 3]);

        // Block list
        let mask = RowIdMask::from_block(row_ids(&[10, 20, 40]));
        assert!(mask.selected_indices(std::iter::empty()).is_empty());
        assert_eq!(mask.selected_indices([25, 20, 14, 10].iter()), &[0, 2]);
    }

    #[test]
    fn test_row_id_mask_also_allow() {
        // Allow list
        let mask = RowIdMask::from_allowed(row_ids(&[10, 20]));
        let new_mask = mask.also_allow(row_ids(&[20, 30, 40]));
        assert_eq!(
            new_mask,
            RowIdMask::from_allowed(row_ids(&[10, 20, 30, 40]))
        );

        // Block list
        let mask = RowIdMask::from_block(row_ids(&[10, 20, 30]));
        let new_mask = mask.also_allow(row_ids(&[20, 40]));
        assert_eq!(new_mask, RowIdMask::from_block(row_ids(&[10, 30])));
    }

    #[test]
    fn test_row_id_mask_also_block() {
        // Allow list
        let mask = RowIdMask::from_allowed(row_ids(&[10, 20, 30]));
        let new_mask = mask.also_block(row_ids(&[20, 40]));
        assert_eq!(new_mask, RowIdMask::from_allowed(row_ids(&[10, 30])));

        // Block list
        let mask = RowIdMask::from_block(row_ids(&[10, 20]));
        let new_mask = mask.also_block(row_ids(&[20, 30, 40]));
        assert_eq!(new_mask, RowIdMask::from_block(row_ids(&[10, 20, 30, 40])));
    }

    #[test]
    fn test_row_id_mask_iter_ids() {
        // Allow list
        let mask = RowIdMask::from_allowed(row_ids(&[10, 20, 30]));
        let ids: Vec<u64> = mask.iter_ids().unwrap().collect();
        assert_eq!(ids, vec![10, 20, 30]);

        // Empty allow list
        let mask = RowIdMask::allow_nothing();
        let iter = mask.iter_ids();
        assert!(iter.is_some());
        assert_eq!(iter.unwrap().count(), 0);

        // Block list
        let mask = RowIdMask::from_block(row_ids(&[10, 20, 30]));
        assert!(mask.iter_ids().is_none());
    }

    #[test]
    fn test_row_id_mask_default() {
        let mask = RowIdMask::default();
        // Default should be BlockList with empty set (all rows allowed)
        assert_row_id_mask_selects(&mask, &[0, 1, 100, 1000], &[]);
        assert_eq!(mask.max_len(), None);
    }

    #[test]
    fn test_row_id_mask_ops() {
        let mask = RowIdMask::default();
        assert_row_id_mask_selects(&mask, &[1, 5, 100], &[]);

        let block_list = mask.also_block(row_ids(&[0, 5, 15]));
        assert_row_id_mask_selects(&block_list, &[1, 100], &[5]);

        let allow_list = RowIdMask::from_allowed(row_ids(&[0, 2, 5]));
        assert_row_id_mask_selects(&allow_list, &[5], &[1, 100]);
    }

    #[test]
    fn test_row_id_mask_combined_ops() {
        // Test combining allow and block operations
        let mask = RowIdMask::from_allowed(row_ids(&[10, 20, 30, 40, 50]));
        let mask = mask.also_block(row_ids(&[20, 40]));
        assert_row_id_mask_selects(&mask, &[10, 30, 50], &[20, 40]);

        let mask = mask.also_allow(row_ids(&[20, 60]));
        assert_row_id_mask_selects(&mask, &[10, 20, 30, 50, 60], &[40]);
    }

    #[test]
    fn test_row_id_mask_with_large_values() {
        let large_ids = [u64::MAX - 10, u64::MAX - 5, u64::MAX - 1];

        // Allow list with large values
        let mask = RowIdMask::from_allowed(row_ids(&large_ids));
        for &id in &large_ids {
            assert!(mask.selected(id));
        }
        assert!(!mask.selected(u64::MAX));
        assert!(!mask.selected(0));

        // Block list with large values
        let mask = RowIdMask::from_block(row_ids(&large_ids));
        for &id in &large_ids {
            assert!(!mask.selected(id));
        }
        assert!(mask.selected(u64::MAX));
        assert!(mask.selected(0));
    }

    proptest::proptest! {
        #[test]
        fn test_row_id_set_from_sorted_iter_proptest(
            mut row_ids in proptest::collection::vec(0..u64::MAX, 0..1000)
        ) {
            row_ids.sort();
            row_ids.dedup();
            let num_rows = row_ids.len();
            let set = RowIdSet::from_sorted_iter(row_ids.clone()).unwrap();
            prop_assert_eq!(set.len(), Some(num_rows as u64));
            for id in row_ids {
                prop_assert!(set.contains(id));
            }
        }

        #[test]
        fn test_row_id_set_union_proptest(
            ids1 in proptest::collection::vec(0..u64::MAX, 0..500),
            ids2 in proptest::collection::vec(0..u64::MAX, 0..500),
        ) {
            let set1 = row_ids(&ids1);
            let set2 = row_ids(&ids2);

            let result = set1.union(&set2);

            // All ids from both sets should be in result
            for id in ids1.iter().chain(ids2.iter()) {
                prop_assert!(result.contains(*id));
            }

            // Result size should be union size
            let expected_size = ids1.iter().chain(ids2.iter()).collect::<std::collections::HashSet<_>>().len();
            prop_assert_eq!(result.len(), Some(expected_size as u64));
        }

        #[test]
        fn test_row_id_set_difference_proptest(
            ids1 in proptest::collection::vec(0..u64::MAX, 0..500),
            ids2 in proptest::collection::vec(0..u64::MAX, 0..500),
        ) {
            let set1 = row_ids(&ids1);
            let set2 = row_ids(&ids2);

            let result = set1.difference(&set2);

            // Items in ids1 but not in ids2 should be in result
            for id in &ids1 {
                if !ids2.contains(id) {
                    prop_assert!(result.contains(*id));
                } else {
                    prop_assert!(!result.contains(*id));
                }
            }
        }

        #[test]
        fn test_row_id_mask_allow_block_proptest(
            allow_ids in proptest::collection::vec(0..10000u64, 0..100),
            block_ids in proptest::collection::vec(0..10000u64, 0..100),
            test_ids in proptest::collection::vec(0..10000u64, 0..50),
        ) {
            let mask = RowIdMask::from_allowed(row_ids(&allow_ids))
                .also_block(row_ids(&block_ids));

            for id in test_ids {
                let expected = allow_ids.contains(&id) && !block_ids.contains(&id);
                prop_assert_eq!(mask.selected(id), expected);
            }
        }
    }
}
