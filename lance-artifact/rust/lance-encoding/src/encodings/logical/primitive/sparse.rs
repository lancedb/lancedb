// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use super::*;
use arrow_array::new_empty_array;
use arrow_buffer::ArrowNativeType;

fn invalid_enum<T>(
    value: std::result::Result<T, prost::UnknownEnumValue>,
    label: &str,
) -> Result<T> {
    value.map_err(|error| {
        Error::invalid_input_source(
            format!("Sparse structural {label} has an invalid enum value: {error}").into(),
        )
    })
}

pub(super) mod writer;

fn usize_from_u64(value: u64, label: &str) -> Result<usize> {
    usize::try_from(value).map_err(|_| {
        Error::invalid_input_source(
            format!("Sparse structural {label} {value} exceeds usize::MAX").into(),
        )
    })
}

/// Native sparse structural representation used by the 2.3 sparse layout.
///
/// Layers are stored from outer-most to inner-most, matching the order Arrow structural
/// encoders record offsets and validity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseStructuralPlan {
    pub(crate) layers: Vec<SparseStructuralLayerPlan>,
    pub(crate) num_items: u64,
    pub(crate) num_visible_items: u64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparsePositionSet {
    Empty,
    All { len: u64 },
    Range { start: u64, len: u64 },
    Explicit(Vec<u64>),
}

impl SparsePositionSet {
    pub(crate) fn from_positions(
        positions: Vec<u64>,
        domain_len: u64,
        label: &str,
    ) -> Result<Self> {
        if positions.is_empty() {
            return Ok(Self::Empty);
        }
        for window in positions.windows(2) {
            let [previous, current] = window else {
                continue;
            };
            if previous >= current {
                return Err(Error::invalid_input_source(
                    format!("Sparse structural {label} positions must be strictly increasing")
                        .into(),
                ));
            }
        }
        if let Some(position) = positions.iter().find(|position| **position >= domain_len) {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} position {} is outside layer with {} slots",
                    position, domain_len
                )
                .into(),
            ));
        }

        let len = u64::try_from(positions.len()).map_err(|_| {
            Error::invalid_input_source(
                format!("Sparse structural {label} position count exceeds u64::MAX").into(),
            )
        })?;
        let first = positions.first().copied().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} positions are unexpectedly empty").into(),
            )
        })?;
        let last = positions.last().copied().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} positions are unexpectedly empty").into(),
            )
        })?;
        if first == 0 && len == domain_len && domain_len > 0 && last == domain_len - 1 {
            return Ok(Self::All { len: domain_len });
        }
        if last - first + 1 == len {
            return Ok(Self::Range { start: first, len });
        }
        Ok(Self::Explicit(positions))
    }

    pub(crate) fn empty() -> Self {
        Self::Empty
    }

    pub(crate) fn all(len: u64) -> Self {
        if len == 0 {
            Self::Empty
        } else {
            Self::All { len }
        }
    }

    pub(crate) fn range(start: u64, len: u64) -> Self {
        if len == 0 {
            Self::Empty
        } else {
            Self::Range { start, len }
        }
    }

    pub(crate) fn len(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::All { len } | Self::Range { len, .. } => *len,
            Self::Explicit(positions) => positions.len() as u64,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn deep_size(&self) -> usize {
        match self {
            Self::Explicit(positions) => positions.len() * std::mem::size_of::<u64>(),
            Self::Empty | Self::All { .. } | Self::Range { .. } => 0,
        }
    }

    pub(crate) fn materialize(&self) -> Result<Vec<u64>> {
        match self {
            Self::Empty => Ok(Vec::new()),
            Self::All { len } => Self::materialize_range(0, *len),
            Self::Range { start, len } => Self::materialize_range(*start, *len),
            Self::Explicit(positions) => Ok(positions.clone()),
        }
    }

    fn materialize_range(start: u64, len: u64) -> Result<Vec<u64>> {
        let len_usize = usize::try_from(len).map_err(|_| {
            Error::invalid_input_source(
                format!(
                    "Sparse structural position range length {} exceeds usize::MAX",
                    len
                )
                .into(),
            )
        })?;
        let end = start.checked_add(len).ok_or_else(|| {
            Error::invalid_input_source("Sparse structural position range overflows".into())
        })?;
        let mut positions = Vec::with_capacity(len_usize);
        positions.extend(start..end);
        Ok(positions)
    }

    fn contains(&self, position: u64) -> bool {
        match self {
            Self::Empty => false,
            Self::All { len } => position < *len,
            Self::Range { start, len } => {
                position >= *start && position < start.saturating_add(*len)
            }
            Self::Explicit(positions) => positions.binary_search(&position).is_ok(),
        }
    }

    fn is_subset_of(&self, other: &Self, domain_len: u64) -> Result<bool> {
        self.validate_domain(domain_len, "subset")?;
        other.validate_domain(domain_len, "superset")?;
        Ok(match self {
            Self::Empty => true,
            Self::All { .. } => other.len() == domain_len,
            Self::Range { start, len } => {
                let end = start.checked_add(*len).ok_or_else(|| {
                    Error::invalid_input_source("Sparse structural subset range overflows".into())
                })?;
                match other {
                    Self::All { .. } => true,
                    Self::Range {
                        start: other_start,
                        len: other_len,
                    } => {
                        let other_end = other_start.saturating_add(*other_len);
                        *start >= *other_start && end <= other_end
                    }
                    Self::Explicit(positions) => {
                        let first = positions.partition_point(|position| *position < *start);
                        let last = positions.partition_point(|position| *position < end);
                        u64::try_from(last.saturating_sub(first)).ok() == Some(*len)
                    }
                    Self::Empty => false,
                }
            }
            Self::Explicit(positions) => positions.iter().all(|position| other.contains(*position)),
        })
    }

    fn is_disjoint(&self, other: &Self, domain_len: u64) -> Result<bool> {
        self.validate_domain(domain_len, "first disjoint set")?;
        other.validate_domain(domain_len, "second disjoint set")?;
        let (smaller, larger) = if self.len() <= other.len() {
            (self, other)
        } else {
            (other, self)
        };
        Ok(match smaller {
            Self::Empty => true,
            Self::All { .. } => larger.is_empty(),
            Self::Range { start, len } => {
                let end = start.saturating_add(*len);
                match larger {
                    Self::Empty => true,
                    Self::All { .. } => false,
                    Self::Range {
                        start: other_start,
                        len: other_len,
                    } => end <= *other_start || other_start.saturating_add(*other_len) <= *start,
                    Self::Explicit(positions) => {
                        let index = positions.partition_point(|position| *position < *start);
                        positions.get(index).is_none_or(|position| *position >= end)
                    }
                }
            }
            Self::Explicit(positions) => {
                positions.iter().all(|position| !larger.contains(*position))
            }
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SparseValidityMeaning {
    NullPositions,
    ValidPositions,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseValiditySet {
    pub(crate) meaning: SparseValidityMeaning,
    pub(crate) positions: SparsePositionSet,
}

impl SparseValiditySet {
    pub(crate) fn deep_size(&self) -> usize {
        self.positions.deep_size()
    }

    fn contains_only_valid_positions(
        &self,
        positions: &SparsePositionSet,
        num_slots: u64,
    ) -> Result<bool> {
        match self.meaning {
            SparseValidityMeaning::NullPositions => {
                positions.is_disjoint(&self.positions, num_slots)
            }
            SparseValidityMeaning::ValidPositions => {
                positions.is_subset_of(&self.positions, num_slots)
            }
        }
    }

    fn append_to(&self, validity: &mut BooleanBufferBuilder, num_slots: u64) -> Result<()> {
        self.positions.validate_domain(num_slots, "validity")?;
        let num_slots_usize = usize_from_u64(num_slots, "validity slot count")?;
        match (self.meaning, &self.positions) {
            (SparseValidityMeaning::NullPositions, SparsePositionSet::Empty) => {
                validity.append_n(num_slots_usize, true);
            }
            (SparseValidityMeaning::ValidPositions, SparsePositionSet::Empty) => {
                validity.append_n(num_slots_usize, false);
            }
            (SparseValidityMeaning::NullPositions, SparsePositionSet::All { .. }) => {
                validity.append_n(num_slots_usize, false);
            }
            (SparseValidityMeaning::ValidPositions, SparsePositionSet::All { .. }) => {
                validity.append_n(num_slots_usize, true);
            }
            (meaning, SparsePositionSet::Range { start, len }) => {
                let range_end = start.checked_add(*len).ok_or_else(|| {
                    Error::invalid_input_source("Sparse structural validity range overflows".into())
                })?;
                let default_valid = matches!(meaning, SparseValidityMeaning::NullPositions);
                let range_valid = !default_valid;
                validity.append_n(
                    usize_from_u64(*start, "validity range start")?,
                    default_valid,
                );
                validity.append_n(usize_from_u64(*len, "validity range length")?, range_valid);
                validity.append_n(
                    usize_from_u64(num_slots - range_end, "validity range tail")?,
                    default_valid,
                );
            }
            (_, SparsePositionSet::Explicit(_)) => {
                let mut cursor = SparseValidityCursor::new(self, num_slots, "validity")?;
                for slot in 0..num_slots {
                    validity.append(cursor.is_valid(slot)?);
                }
                cursor.finish()?;
            }
        }
        Ok(())
    }
}

impl SparsePositionSet {
    fn validate_domain(&self, domain_len: u64, label: &str) -> Result<()> {
        match self {
            Self::Empty => {}
            Self::All { len } => {
                if *len != domain_len {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} all set length {} does not match domain {}",
                            len, domain_len
                        )
                        .into(),
                    ));
                }
            }
            Self::Range { start, len } => {
                let end = start.checked_add(*len).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse structural {label} range overflows").into(),
                    )
                })?;
                if end > domain_len {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} range {}..{} is outside domain {}",
                            start, end, domain_len
                        )
                        .into(),
                    ));
                }
            }
            Self::Explicit(positions) => {
                for window in positions.windows(2) {
                    let [previous, current] = window else {
                        continue;
                    };
                    if previous >= current {
                        return Err(Error::invalid_input_source(
                            format!(
                                "Sparse structural {label} positions must be strictly increasing"
                            )
                            .into(),
                        ));
                    }
                }
                if let Some(position) = positions.last()
                    && *position >= domain_len
                {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} position {} is outside layer with {} slots",
                            position, domain_len
                        )
                        .into(),
                    ));
                }
            }
        }
        Ok(())
    }
}

struct SparsePositionSetCursor<'a> {
    set: &'a SparsePositionSet,
    explicit: Option<std::iter::Peekable<std::slice::Iter<'a, u64>>>,
}

impl<'a> SparsePositionSetCursor<'a> {
    fn new(set: &'a SparsePositionSet, domain_len: u64, label: &str) -> Result<Self> {
        set.validate_domain(domain_len, label)?;
        let explicit = match set {
            SparsePositionSet::Explicit(positions) => Some(positions.iter().peekable()),
            _ => None,
        };
        Ok(Self { set, explicit })
    }

    fn contains(&mut self, slot: u64) -> Result<bool> {
        Ok(match self.set {
            SparsePositionSet::Empty => false,
            SparsePositionSet::All { .. } => true,
            SparsePositionSet::Range { start, len } => {
                slot >= *start && slot < start.saturating_add(*len)
            }
            SparsePositionSet::Explicit(_) => {
                let iter = self.explicit.as_mut().ok_or_else(|| {
                    Error::internal("Sparse structural explicit cursor is missing".to_string())
                })?;
                if let Some(position) = iter.peek()
                    && **position < slot
                {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural explicit position {} was skipped before slot {}",
                            **position, slot
                        )
                        .into(),
                    ));
                }
                if iter.peek().is_some_and(|position| **position == slot) {
                    iter.next();
                    true
                } else {
                    false
                }
            }
        })
    }

    fn finish(&mut self) -> Result<()> {
        if let Some(iter) = self.explicit.as_mut()
            && let Some(position) = iter.next()
        {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural explicit position {} was not consumed",
                    position
                )
                .into(),
            ));
        }
        Ok(())
    }
}

struct SparseValidityCursor<'a> {
    meaning: SparseValidityMeaning,
    positions: SparsePositionSetCursor<'a>,
}

impl<'a> SparseValidityCursor<'a> {
    fn new(validity: &'a SparseValiditySet, domain_len: u64, label: &str) -> Result<Self> {
        Ok(Self {
            meaning: validity.meaning,
            positions: SparsePositionSetCursor::new(&validity.positions, domain_len, label)?,
        })
    }

    fn is_valid(&mut self, slot: u64) -> Result<bool> {
        let is_stored = self.positions.contains(slot)?;
        Ok(match self.meaning {
            SparseValidityMeaning::NullPositions => !is_stored,
            SparseValidityMeaning::ValidPositions => is_stored,
        })
    }

    fn finish(&mut self) -> Result<()> {
        self.positions.finish()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparseCountSet {
    Empty,
    Constant {
        value: u64,
        len: u64,
    },
    Explicit {
        counts: Arc<[u64]>,
        offsets: Arc<[u64]>,
    },
}

impl SparseCountSet {
    pub(crate) fn from_counts(counts: Vec<u64>) -> Result<Self> {
        if counts.is_empty() {
            return Ok(Self::Empty);
        }
        if let Some(first) = counts.first().copied()
            && counts.iter().all(|count| *count == first)
        {
            return Ok(Self::Constant {
                value: first,
                len: counts.len() as u64,
            });
        }
        let offsets = offsets_from_counts(&counts)?;
        Ok(Self::Explicit {
            counts: counts.into(),
            offsets: offsets.into(),
        })
    }

    pub(crate) fn constant(value: u64, len: u64) -> Self {
        if len == 0 {
            Self::Empty
        } else {
            Self::Constant { value, len }
        }
    }

    pub(crate) fn len(&self) -> u64 {
        match self {
            Self::Empty => 0,
            Self::Constant { len, .. } => *len,
            Self::Explicit { counts, .. } => counts.len() as u64,
        }
    }

    pub(crate) fn deep_size(&self) -> usize {
        match self {
            Self::Explicit { counts, offsets } => {
                counts.len() * std::mem::size_of::<u64>()
                    + offsets.len() * std::mem::size_of::<u64>()
            }
            Self::Empty | Self::Constant { .. } => 0,
        }
    }

    pub(crate) fn materialize(&self) -> Result<Vec<u64>> {
        match self {
            Self::Empty => Ok(Vec::new()),
            Self::Constant { value, len } => {
                let len = usize::try_from(*len).map_err(|_| {
                    Error::invalid_input_source(
                        "Sparse structural count set length exceeds usize::MAX".into(),
                    )
                })?;
                Ok(vec![*value; len])
            }
            Self::Explicit { counts, .. } => Ok(counts.to_vec()),
        }
    }

    pub(crate) fn sum(&self) -> Result<u64> {
        match self {
            Self::Empty => Ok(0),
            Self::Constant { value, len } => value.checked_mul(*len).ok_or_else(|| {
                Error::invalid_input_source(
                    format!(
                        "Sparse structural constant count sum overflows: value={}, len={}",
                        value, len
                    )
                    .into(),
                )
            }),
            Self::Explicit { offsets, .. } => offsets.last().copied().ok_or_else(|| {
                Error::invalid_input_source(
                    "Sparse structural explicit count offsets are empty".into(),
                )
            }),
        }
    }

    fn validate_positive(&self) -> Result<()> {
        let has_zero = match self {
            Self::Empty => false,
            Self::Constant { value, .. } => *value == 0,
            Self::Explicit { counts, .. } => counts.contains(&0),
        };
        if has_zero {
            return Err(Error::invalid_input_source(
                "Sparse structural non-empty list count is zero".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SparseStructuralLayerPlan {
    Validity {
        num_slots: u64,
        validity: SparseValiditySet,
    },
    List {
        num_slots: u64,
        num_child_slots: u64,
        non_empty_positions: SparsePositionSet,
        counts: SparseCountSet,
        validity: SparseValiditySet,
    },
    FixedSizeList {
        num_slots: u64,
        dimension: u64,
        validity: SparseValiditySet,
    },
}

impl SparseStructuralPlan {
    fn expected_num_items(
        layers: &[SparseStructuralLayerPlan],
        num_visible_items: u64,
    ) -> Result<u64> {
        layers.iter().try_fold(num_visible_items, |items, layer| {
            let additional = match layer {
                SparseStructuralLayerPlan::List {
                    num_slots,
                    non_empty_positions,
                    ..
                } => num_slots
                    .checked_sub(non_empty_positions.len())
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse structural list has more non-empty positions than slots".into(),
                        )
                    })?,
                SparseStructuralLayerPlan::Validity { .. }
                | SparseStructuralLayerPlan::FixedSizeList { .. } => 0,
            };
            items.checked_add(additional).ok_or_else(|| {
                Error::invalid_input_source("Sparse structural item count overflows".into())
            })
        })
    }

    fn validate(&self, row_domain: u64) -> Result<()> {
        usize_from_u64(self.num_visible_items, "visible item count")?;
        let expected_num_items = Self::expected_num_items(&self.layers, self.num_visible_items)?;
        if self.num_items != expected_num_items {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural item count {} does not match the {} items implied by its layers",
                    self.num_items, expected_num_items
                )
                .into(),
            ));
        }
        let mut expected_slots = row_domain;
        for (layer_index, layer) in self.layers.iter().enumerate() {
            let (num_slots, num_child_slots, validity) = match layer {
                SparseStructuralLayerPlan::Validity {
                    num_slots,
                    validity,
                } => (*num_slots, *num_slots, validity),
                SparseStructuralLayerPlan::List {
                    num_slots,
                    num_child_slots,
                    non_empty_positions,
                    counts,
                    validity,
                } => {
                    counts.validate_positive()?;
                    if non_empty_positions.len() != counts.len() {
                        return Err(Error::invalid_input_source(
                            format!(
                                "Sparse structural list layer {} has {} non-empty positions but {} counts",
                                layer_index,
                                non_empty_positions.len(),
                                counts.len()
                            )
                            .into(),
                        ));
                    }
                    if counts.sum()? != *num_child_slots {
                        return Err(Error::invalid_input_source(
                            format!(
                                "Sparse structural list layer {} count sum does not match {} child slots",
                                layer_index, num_child_slots
                            )
                            .into(),
                        ));
                    }
                    if !validity.contains_only_valid_positions(non_empty_positions, *num_slots)? {
                        return Err(Error::invalid_input_source(
                            format!(
                                "Sparse structural list layer {} contains a non-empty null slot",
                                layer_index
                            )
                            .into(),
                        ));
                    }
                    (*num_slots, *num_child_slots, validity)
                }
                SparseStructuralLayerPlan::FixedSizeList {
                    num_slots,
                    dimension,
                    validity,
                } => {
                    if *dimension == 0 {
                        return Err(Error::invalid_input_source(
                            format!(
                                "Sparse structural fixed-size-list layer {} has dimension zero",
                                layer_index
                            )
                            .into(),
                        ));
                    }
                    let num_child_slots = num_slots.checked_mul(*dimension).ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse structural fixed-size-list child domain overflows".into(),
                        )
                    })?;
                    (*num_slots, num_child_slots, validity)
                }
            };
            usize_from_u64(num_slots, "layer slot count")?;
            usize_from_u64(num_child_slots, "layer child slot count")?;
            if num_slots != expected_slots {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse structural layer {} has {} slots, expected {}",
                        layer_index, num_slots, expected_slots
                    )
                    .into(),
                ));
            }
            validity.positions.validate_domain(num_slots, "validity")?;
            expected_slots = num_child_slots;
        }
        if expected_slots != self.num_visible_items {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural terminal domain has {} slots, expected {} visible items",
                    expected_slots, self.num_visible_items
                )
                .into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug)]
pub struct SparseStructuralUnraveler {
    layers: Vec<SparseStructuralLayerPlan>,
    next_layer: usize,
    pending_fixed_size_list: bool,
}

impl SparseStructuralUnraveler {
    pub(crate) fn new(plan: SparseStructuralPlan) -> Self {
        let next_layer = plan.layers.len();
        Self {
            layers: plan.layers,
            next_layer,
            pending_fixed_size_list: false,
        }
    }

    fn current_layer(&self) -> Option<&SparseStructuralLayerPlan> {
        self.next_layer
            .checked_sub(1)
            .and_then(|idx| self.layers.get(idx))
    }

    fn consume_current_layer(&mut self) -> Result<()> {
        self.next_layer = self.next_layer.checked_sub(1).ok_or_else(|| {
            Error::invalid_input_source(
                "Sparse structural metadata has fewer layers than the Arrow schema".into(),
            )
        })?;
        Ok(())
    }

    pub(crate) fn ensure_exhausted(&self) -> Result<()> {
        if self.pending_fixed_size_list {
            return Err(Error::invalid_input_source(
                "Sparse structural metadata has an unconsumed fixed-size-list layer".into(),
            ));
        }
        if self.next_layer != 0 {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural metadata has {} unconsumed layer(s)",
                    self.next_layer
                )
                .into(),
            ));
        }
        Ok(())
    }

    pub(crate) fn is_all_valid(&self) -> bool {
        match self.current_layer() {
            Some(SparseStructuralLayerPlan::Validity {
                num_slots,
                validity,
            })
            | Some(SparseStructuralLayerPlan::FixedSizeList {
                num_slots,
                validity,
                ..
            })
            | Some(SparseStructuralLayerPlan::List {
                num_slots,
                validity,
                ..
            }) => match validity.meaning {
                SparseValidityMeaning::NullPositions => validity.positions.is_empty(),
                SparseValidityMeaning::ValidPositions => validity.positions.len() == *num_slots,
            },
            None => true,
        }
    }

    pub(crate) fn max_lists(&self) -> Result<usize> {
        match self.current_layer() {
            Some(SparseStructuralLayerPlan::List { num_slots, .. }) => {
                usize_from_u64(*num_slots, "list slot count")
            }
            _ => Ok(0),
        }
    }

    pub(crate) fn skip_validity(&mut self) -> Result<()> {
        match self.current_layer() {
            Some(SparseStructuralLayerPlan::Validity { .. }) => {
                if self.pending_fixed_size_list {
                    return Err(Error::invalid_input_source(
                        "Sparse fixed-size-list schema does not match a validity layer".into(),
                    ));
                }
                self.consume_current_layer()?;
            }
            Some(SparseStructuralLayerPlan::FixedSizeList { .. }) => {
                if !self.pending_fixed_size_list {
                    return Err(Error::invalid_input_source(
                        "Sparse fixed-size-list layer does not match the Arrow schema".into(),
                    ));
                }
                self.pending_fixed_size_list = false;
                self.consume_current_layer()?;
            }
            None => {
                return Err(Error::invalid_input_source(
                    "Sparse structural metadata has fewer layers than the Arrow schema".into(),
                ));
            }
            Some(SparseStructuralLayerPlan::List { .. }) => {
                return Err(Error::invalid_input_source(
                    "Sparse structural list layer does not match an Arrow validity layer".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn unravel_validity(&mut self, validity: &mut BooleanBufferBuilder) -> Result<()> {
        match self.current_layer() {
            Some(SparseStructuralLayerPlan::Validity {
                num_slots,
                validity: layer_validity,
            }) => {
                if self.pending_fixed_size_list {
                    return Err(Error::invalid_input_source(
                        "Sparse fixed-size-list schema does not match a validity layer".into(),
                    ));
                }
                layer_validity.append_to(validity, *num_slots)?;
                self.consume_current_layer()?;
            }
            Some(SparseStructuralLayerPlan::FixedSizeList {
                num_slots,
                validity: layer_validity,
                ..
            }) => {
                if !self.pending_fixed_size_list {
                    return Err(Error::invalid_input_source(
                        "Sparse fixed-size-list layer does not match the Arrow schema".into(),
                    ));
                }
                layer_validity.append_to(validity, *num_slots)?;
                self.pending_fixed_size_list = false;
                self.consume_current_layer()?;
            }
            None => {
                return Err(Error::invalid_input_source(
                    "Sparse structural metadata has fewer layers than the Arrow schema".into(),
                ));
            }
            Some(SparseStructuralLayerPlan::List { .. }) => {
                return Err(Error::invalid_input_source(
                    "Sparse structural list layer does not match an Arrow validity layer".into(),
                ));
            }
        }
        Ok(())
    }

    pub(crate) fn decimate(&mut self, dimension: usize) -> Result<()> {
        if self.pending_fixed_size_list {
            return Err(Error::invalid_input_source(
                "Sparse fixed-size-list layer was decimated more than once".into(),
            ));
        }
        let Some(SparseStructuralLayerPlan::FixedSizeList {
            dimension: actual_dimension,
            ..
        }) = self.current_layer()
        else {
            return Err(Error::invalid_input_source(
                "Sparse structural layer does not match an Arrow fixed-size-list layer".into(),
            ));
        };
        if usize_from_u64(*actual_dimension, "fixed-size-list dimension")? != dimension {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural fixed-size-list dimension {} does not match Arrow dimension {}",
                    actual_dimension, dimension
                )
                .into(),
            ));
        }
        self.pending_fixed_size_list = true;
        Ok(())
    }

    fn to_offset<T: ArrowNativeType>(value: u64) -> Result<T> {
        let value = usize::try_from(value).map_err(|_| {
            Error::invalid_input_source(
                format!("Sparse structural offset {} exceeds usize::MAX", value).into(),
            )
        })?;
        T::from_usize(value).ok_or_else(|| {
            Error::invalid_input_source(
                "Sparse structural offset does not fit the Arrow offset type".into(),
            )
        })
    }

    pub(crate) fn unravel_offsets<T: ArrowNativeType>(
        &mut self,
        offsets: &mut Vec<T>,
        validity: Option<&mut BooleanBufferBuilder>,
    ) -> Result<()> {
        let Some(SparseStructuralLayerPlan::List {
            num_slots,
            num_child_slots,
            non_empty_positions,
            counts,
            validity: layer_validity,
            ..
        }) = self.current_layer()
        else {
            return Err(Error::invalid_input_source(
                "Sparse structural layer does not match an Arrow list layer".into(),
            ));
        };
        if self.pending_fixed_size_list {
            return Err(Error::invalid_input_source(
                "Sparse fixed-size-list schema does not match an Arrow list layer".into(),
            ));
        }

        if non_empty_positions.len() != counts.len() {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural list has {} non-empty positions but {} counts",
                    non_empty_positions.len(),
                    counts.len()
                )
                .into(),
            ));
        }
        let actual_child_slots = counts.sum()?;
        if actual_child_slots != *num_child_slots {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural list count sum {} does not match child slots {}",
                    actual_child_slots, num_child_slots
                )
                .into(),
            ));
        }

        let mut current_offset = offsets
            .last()
            .map(|offset| offset.as_usize() as u64)
            .unwrap_or(0);
        if offsets.is_empty() {
            offsets.push(Self::to_offset(current_offset)?);
        }

        if non_empty_positions.is_empty() {
            if let Some(validity) = validity {
                layer_validity.append_to(validity, *num_slots)?;
            }
            let offset = Self::to_offset(current_offset)?;
            let new_len = offsets
                .len()
                .checked_add(usize_from_u64(*num_slots, "list slot count")?)
                .ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse structural list offset length overflows usize".into(),
                    )
                })?;
            offsets.resize(new_len, offset);
            self.consume_current_layer()?;
            return Ok(());
        }

        let non_empty_positions = non_empty_positions.materialize()?;
        let counts = counts.materialize()?;
        let mut non_empty_iter = non_empty_positions
            .iter()
            .copied()
            .zip(counts.iter().copied())
            .peekable();
        let mut validity_cursor =
            SparseValidityCursor::new(layer_validity, *num_slots, "list validity")?;
        let mut validity = validity;
        for slot in 0..*num_slots {
            let is_valid = validity_cursor.is_valid(slot)?;
            if let Some(validity) = validity.as_mut() {
                validity.append(is_valid);
            }

            if non_empty_iter
                .peek()
                .is_some_and(|(non_empty_pos, _)| *non_empty_pos == slot)
            {
                let (_, count) = non_empty_iter.next().ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse structural list position is missing its child count".into(),
                    )
                })?;
                if !is_valid {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural list slot {} is both invalid and non-empty",
                            slot
                        )
                        .into(),
                    ));
                }
                current_offset = current_offset.checked_add(count).ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse structural list offsets overflow u64".into(),
                    )
                })?;
            }
            offsets.push(Self::to_offset(current_offset)?);
        }
        validity_cursor.finish()?;
        if let Some((extra_pos, _)) = non_empty_iter.next() {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural non-empty position {} is outside layer with {} slots",
                    extra_pos, num_slots
                )
                .into(),
            ));
        }

        self.consume_current_layer()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
enum SparsePositionSetDecoder {
    Empty,
    All {
        len: u64,
    },
    Range {
        start: u64,
        len: u64,
    },
    Explicit {
        decompressor: Arc<dyn BlockDecompressor>,
        encoding: CompressiveEncoding,
        count: u64,
        domain_len: u64,
    },
}

#[derive(Debug, Clone)]
enum SparseCountSetDecoder {
    Empty,
    Constant {
        value: u64,
        len: u64,
    },
    Explicit {
        decompressor: Arc<dyn BlockDecompressor>,
        encoding: CompressiveEncoding,
        count: u64,
    },
}

#[derive(Debug, Clone)]
enum SparseLayerDecompressors {
    Validity {
        num_slots: u64,
        validity: SparseValiditySetDecoder,
    },
    List {
        num_slots: u64,
        num_child_slots: u64,
        non_empty_positions: SparsePositionSetDecoder,
        counts: SparseCountSetDecoder,
        validity: SparseValiditySetDecoder,
    },
    FixedSizeList {
        num_slots: u64,
        num_child_slots: u64,
        dimension: u64,
        validity: SparseValiditySetDecoder,
    },
}

#[derive(Debug, Clone)]
struct SparseValiditySetDecoder {
    meaning: SparseValidityMeaning,
    positions: SparsePositionSetDecoder,
}

#[derive(Debug)]
struct SparseStructuralCacheableState {
    chunk_meta: Vec<ChunkMeta>,
    chunk_value_offsets: Arc<[u64]>,
    plan: SparseStructuralPlan,
    row_domain: u64,
}

impl DeepSizeOf for SparseStructuralCacheableState {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        let structural_size = self
            .plan
            .layers
            .iter()
            .map(|layer| match layer {
                SparseStructuralLayerPlan::Validity { validity, .. } => validity.deep_size(),
                SparseStructuralLayerPlan::List {
                    non_empty_positions,
                    counts,
                    validity,
                    ..
                } => non_empty_positions.deep_size() + counts.deep_size() + validity.deep_size(),
                SparseStructuralLayerPlan::FixedSizeList { validity, .. } => validity.deep_size(),
            })
            .sum::<usize>();
        self.chunk_meta.len() * std::mem::size_of::<ChunkMeta>()
            + self.chunk_value_offsets.len() * std::mem::size_of::<u64>()
            + structural_size
    }
}

impl CachedPageData for SparseStructuralCacheableState {
    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self
    }
}

#[derive(Debug)]
pub(super) struct SparseStructuralScheduler {
    buffer_offsets_and_sizes: Vec<(u64, u64)>,
    priority: u64,
    row_domain: u64,
    row_scale: u64,
    num_items: u64,
    num_visible_items: u64,
    num_buffers: u64,
    value_encoding: CompressiveEncoding,
    value_decompressor: Arc<dyn MiniBlockDecompressor>,
    layer_decompressors: Vec<SparseLayerDecompressors>,
    data_type: DataType,
    page_meta: Option<Arc<SparseStructuralCacheableState>>,
    has_large_chunk: bool,
}

impl SparseStructuralScheduler {
    fn require_layer(
        layer: &pb21::SparseStructuralLayer,
    ) -> Result<&pb21::sparse_structural_layer::Layer> {
        layer.layer.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                "Sparse structural layer is missing its layer variant".into(),
            )
        })
    }

    fn layer_num_slots(layer: &pb21::SparseStructuralLayer) -> Result<u64> {
        Ok(match Self::require_layer(layer)? {
            pb21::sparse_structural_layer::Layer::Validity(layer) => layer.num_slots,
            pb21::sparse_structural_layer::Layer::List(layer) => layer.num_slots,
            pb21::sparse_structural_layer::Layer::FixedSizeList(layer) => layer.num_slots,
        })
    }

    fn layer_num_child_slots(layer: &pb21::SparseStructuralLayer) -> Result<u64> {
        Ok(match Self::require_layer(layer)? {
            pb21::sparse_structural_layer::Layer::Validity(layer) => layer.num_slots,
            pb21::sparse_structural_layer::Layer::List(layer) => layer.num_child_slots,
            pb21::sparse_structural_layer::Layer::FixedSizeList(layer) => layer
                .num_slots
                .checked_mul(layer.dimension)
                .ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural fixed-size-list child slot count overflows: slots={}, dimension={}",
                            layer.num_slots, layer.dimension
                        )
                        .into(),
                    )
                })?,
        })
    }

    pub(super) fn try_new(
        buffer_offsets_and_sizes: &[(u64, u64)],
        priority: u64,
        encoded_row_domain: u64,
        data_type: DataType,
        layout: &pb21::SparseLayout,
        decompressors: &dyn DecompressionStrategy,
    ) -> Result<Self> {
        let value_compression = layout.value_compression.as_ref().ok_or_else(|| {
            Error::invalid_input_source("Sparse layout is missing value compression".into())
        })?;
        let value_buffer_count = Self::validate_value_encoding(value_compression)?;
        if layout.num_buffers != value_buffer_count {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout declares {} value buffers, but its compression descriptor requires {}",
                    layout.num_buffers, value_buffer_count
                )
                .into(),
            ));
        }
        let row_domain = match layout.structural_layers.first() {
            Some(layer) => Self::layer_num_slots(layer)?,
            None => encoded_row_domain,
        };
        Self::validate_domain_chain(
            &layout.structural_layers,
            row_domain,
            layout.num_items,
            layout.num_visible_items,
        )?;
        let expected_buffers = 2 + Self::structural_buffer_count(&layout.structural_layers)?;
        if buffer_offsets_and_sizes.len() != expected_buffers {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout has {} buffers, expected {}",
                    buffer_offsets_and_sizes.len(),
                    expected_buffers
                )
                .into(),
            ));
        }
        Self::validate_page_buffers(buffer_offsets_and_sizes, layout.num_visible_items)?;
        let row_scale =
            layout
                .structural_layers
                .iter()
                .try_fold(1_u64, |scale, layer| -> Result<u64> {
                    match Self::require_layer(layer)? {
                        pb21::sparse_structural_layer::Layer::FixedSizeList(layer) => {
                            scale.checked_mul(layer.dimension).ok_or_else(|| {
                                Error::invalid_input_source(
                                    "Sparse structural fixed-size-list row scale overflows".into(),
                                )
                            })
                        }
                        pb21::sparse_structural_layer::Layer::Validity(_)
                        | pb21::sparse_structural_layer::Layer::List(_) => Ok(scale),
                    }
                })?;
        let expected_encoded_row_domain = row_domain.checked_mul(row_scale).ok_or_else(|| {
            Error::invalid_input_source("Sparse structural encoded row domain overflows".into())
        })?;
        if encoded_row_domain != expected_encoded_row_domain {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural encoded row domain {} does not match outer domain {} * fixed-size-list scale {}",
                    encoded_row_domain, row_domain, row_scale
                )
                .into(),
            ));
        }
        let value_decompressor = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decompressors.create_miniblock_decompressor(value_compression, decompressors)
        }))
        .map_err(|_| {
            Error::invalid_input_source(
                "Sparse value compression descriptor caused decompressor construction to panic"
                    .into(),
            )
        })?
        .map_err(|error| {
            Error::invalid_input_source(
                format!("Sparse value decompressor construction failed: {error}").into(),
            )
        })?;
        let layer_decompressors = layout
            .structural_layers
            .iter()
            .map(|layer| Self::layer_decompressors(layer, decompressors))
            .collect::<Result<Vec<_>>>()?;

        Ok(Self {
            buffer_offsets_and_sizes: buffer_offsets_and_sizes.to_vec(),
            priority,
            row_domain,
            row_scale,
            num_items: layout.num_items,
            num_visible_items: layout.num_visible_items,
            num_buffers: layout.num_buffers,
            value_encoding: value_compression.clone(),
            value_decompressor: value_decompressor.into(),
            layer_decompressors,
            data_type,
            page_meta: None,
            has_large_chunk: layout.has_large_chunk,
        })
    }

    fn validate_compression<'a>(
        compression: &'a CompressiveEncoding,
        label: &str,
    ) -> Result<&'a CompressiveEncoding> {
        compression.compression.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} is missing compression details").into(),
            )
        })?;
        Ok(compression)
    }

    fn validate_buffer_compression(
        compression: Option<&pb21::BufferCompression>,
        label: &str,
    ) -> Result<()> {
        let Some(compression) = compression else {
            return Ok(());
        };
        match invalid_enum(
            pb21::CompressionScheme::try_from(compression.scheme),
            "compression scheme",
        )? {
            pb21::CompressionScheme::CompressionAlgorithmUnspecified => {
                Err(Error::invalid_input_source(
                    format!("Sparse structural {label} buffer compression is unspecified").into(),
                ))
            }
            pb21::CompressionScheme::CompressionAlgorithmLz4
            | pb21::CompressionScheme::CompressionAlgorithmZstd => Ok(()),
        }
    }

    fn encoding_contains_general(root: &CompressiveEncoding) -> bool {
        use pb21::compressive_encoding::Compression;

        let mut stack = vec![root];
        while let Some(encoding) = stack.pop() {
            let Some(compression) = encoding.compression.as_ref() else {
                continue;
            };
            match compression {
                Compression::General(_) => return true,
                Compression::Variable(variable) => {
                    stack.extend(variable.offsets.as_deref());
                }
                Compression::OutOfLineBitpacking(bitpacking) => {
                    stack.extend(bitpacking.values.as_deref());
                }
                Compression::Fsst(fsst) => stack.extend(fsst.values.as_deref()),
                Compression::Dictionary(dictionary) => {
                    stack.extend(dictionary.indices.as_deref());
                    stack.extend(dictionary.items.as_deref());
                }
                Compression::Rle(rle) => {
                    stack.extend(rle.values.as_deref());
                    stack.extend(rle.run_lengths.as_deref());
                }
                Compression::ByteStreamSplit(split) => {
                    stack.extend(split.values.as_deref());
                }
                Compression::FixedSizeList(fsl) => stack.extend(fsl.values.as_deref()),
                Compression::PackedStruct(packed) => stack.extend(packed.values.as_deref()),
                Compression::VariablePackedStruct(packed) => {
                    stack.extend(
                        packed
                            .fields
                            .iter()
                            .filter_map(|field| field.value.as_ref()),
                    );
                }
                Compression::Flat(_)
                | Compression::Constant(_)
                | Compression::InlineBitpacking(_) => {}
            }
        }
        false
    }

    fn require_encoding<'a>(
        encoding: &'a Option<Box<CompressiveEncoding>>,
        label: &str,
    ) -> Result<&'a CompressiveEncoding> {
        encoding.as_deref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} encoding is required").into(),
            )
        })
    }

    fn validate_flat(flat: &pb21::Flat, label: &str) -> Result<()> {
        if flat.bits_per_value == 0 {
            return Err(Error::invalid_input_source(
                format!("Sparse structural {label} flat bit width is zero").into(),
            ));
        }
        if flat.data.is_some() {
            return Err(Error::invalid_input_source(
                format!("Sparse structural {label} uses unsupported leaf buffer compression")
                    .into(),
            ));
        }
        Ok(())
    }

    fn validate_value_encoding(encoding: &CompressiveEncoding) -> Result<u64> {
        use pb21::compressive_encoding::Compression;

        let compression = encoding.compression.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                "Sparse value compression is missing compression details".into(),
            )
        })?;
        match compression {
            Compression::Flat(flat) => {
                Self::validate_flat(flat, "value")?;
                Ok(1)
            }
            Compression::InlineBitpacking(bitpacking) => {
                if !matches!(bitpacking.uncompressed_bits_per_value, 8 | 16 | 32 | 64) {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse inline bitpacking width {} is not supported",
                            bitpacking.uncompressed_bits_per_value
                        )
                        .into(),
                    ));
                }
                if bitpacking.values.is_some() {
                    return Err(Error::invalid_input_source(
                        "Sparse inline bitpacking uses unsupported leaf buffer compression".into(),
                    ));
                }
                Ok(1)
            }
            Compression::Variable(variable) => {
                let offsets = variable.offsets.as_deref().ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse variable compression is missing offsets".into(),
                    )
                })?;
                let Some(Compression::Flat(offsets)) = offsets.compression.as_ref() else {
                    return Err(Error::invalid_input_source(
                        "Sparse variable offsets must use flat compression".into(),
                    ));
                };
                Self::validate_flat(offsets, "variable offsets")?;
                if !matches!(offsets.bits_per_value, 32 | 64) {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse variable offset width {} is not supported",
                            offsets.bits_per_value
                        )
                        .into(),
                    ));
                }
                if variable.values.is_some() {
                    return Err(Error::invalid_input_source(
                        "Sparse variable values use unsupported leaf buffer compression".into(),
                    ));
                }
                Ok(1)
            }
            Compression::Fsst(fsst) => {
                if fsst.symbol_table.is_empty() {
                    return Err(Error::invalid_input_source(
                        "Sparse FSST compression has an empty symbol table".into(),
                    ));
                }
                let values = Self::require_encoding(&fsst.values, "FSST values")?;
                if !matches!(values.compression.as_ref(), Some(Compression::Variable(_))) {
                    return Err(Error::invalid_input_source(
                        "Sparse FSST values must use variable compression".into(),
                    ));
                }
                Self::validate_value_encoding(values)
            }
            Compression::ByteStreamSplit(split) => {
                let values = Self::require_encoding(&split.values, "byte-stream-split values")?;
                let Some(Compression::Flat(flat)) = values.compression.as_ref() else {
                    return Err(Error::invalid_input_source(
                        "Sparse byte-stream-split values must use flat compression".into(),
                    ));
                };
                Self::validate_flat(flat, "byte-stream-split values")?;
                if !matches!(flat.bits_per_value, 32 | 64) {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse byte-stream-split width {} is not supported",
                            flat.bits_per_value
                        )
                        .into(),
                    ));
                }
                Ok(1)
            }
            Compression::FixedSizeList(fsl) => Self::validate_fsl_value_encoding(fsl),
            Compression::PackedStruct(packed) => Self::validate_packed_value_encoding(packed),
            Compression::Rle(rle) => {
                let values = Self::require_encoding(&rle.values, "RLE values")?;
                let lengths = Self::require_encoding(&rle.run_lengths, "RLE run lengths")?;
                Self::validate_block_encoding(values, "RLE values")?;
                Self::validate_block_encoding(lengths, "RLE run lengths")?;
                Ok(2)
            }
            Compression::General(general) => {
                let compression = general.compression.as_ref().ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse general compression is missing its buffer compression".into(),
                    )
                })?;
                Self::validate_buffer_compression(Some(compression), "general")?;
                Self::validate_value_encoding(Self::require_encoding(
                    &general.values,
                    "general values",
                )?)
            }
            Compression::Constant(_)
            | Compression::OutOfLineBitpacking(_)
            | Compression::Dictionary(_)
            | Compression::VariablePackedStruct(_) => Err(Error::invalid_input_source(
                "Sparse value compression uses an unsupported mini-block encoding".into(),
            )),
        }
    }

    fn validate_fsl_value_encoding(fsl: &pb21::FixedSizeList) -> Result<u64> {
        use pb21::compressive_encoding::Compression;

        if fsl.items_per_value == 0 {
            return Err(Error::invalid_input_source(
                "Sparse fixed-size-list value compression has dimension zero".into(),
            ));
        }
        let values = Self::require_encoding(&fsl.values, "fixed-size-list values")?;
        let child_buffers = match values.compression.as_ref() {
            Some(Compression::Flat(flat)) => {
                Self::validate_flat(flat, "fixed-size-list values")?;
                1_u64
            }
            Some(Compression::FixedSizeList(inner)) => Self::validate_fsl_value_encoding(inner)?,
            _ => {
                return Err(Error::invalid_input_source(
                    "Sparse fixed-size-list values must use fixed-size-list or flat compression"
                        .into(),
                ));
            }
        };
        child_buffers
            .checked_add(u64::from(fsl.has_validity))
            .ok_or_else(|| {
                Error::invalid_input_source(
                    "Sparse fixed-size-list value buffer count overflows".into(),
                )
            })
    }

    fn validate_packed_value_encoding(packed: &pb21::PackedStruct) -> Result<u64> {
        use pb21::compressive_encoding::Compression;

        if packed.bits_per_value.is_empty()
            || packed
                .bits_per_value
                .iter()
                .any(|bits| *bits == 0 || !bits.is_multiple_of(8))
        {
            return Err(Error::invalid_input_source(
                "Sparse packed-struct widths must be non-empty positive byte widths".into(),
            ));
        }
        let values = Self::require_encoding(&packed.values, "packed-struct values")?;
        let Some(Compression::Flat(flat)) = values.compression.as_ref() else {
            return Err(Error::invalid_input_source(
                "Sparse packed-struct values must use flat compression".into(),
            ));
        };
        Self::validate_flat(flat, "packed-struct values")?;
        let total_bits = packed.bits_per_value.iter().try_fold(0_u64, |sum, bits| {
            sum.checked_add(*bits).ok_or_else(|| {
                Error::invalid_input_source("Sparse packed-struct bit width sum overflows".into())
            })
        })?;
        if total_bits != flat.bits_per_value {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse packed-struct child widths sum to {}, but values use {} bits",
                    total_bits, flat.bits_per_value
                )
                .into(),
            ));
        }
        Ok(1)
    }

    fn validate_block_encoding(encoding: &CompressiveEncoding, label: &str) -> Result<()> {
        use pb21::compressive_encoding::Compression;

        match encoding.compression.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} is missing compression details").into(),
            )
        })? {
            Compression::Flat(flat) => Self::validate_flat(flat, label),
            Compression::InlineBitpacking(bitpacking) => {
                if !matches!(bitpacking.uncompressed_bits_per_value, 8 | 16 | 32 | 64) {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} inline bitpacking width {} is unsupported",
                            bitpacking.uncompressed_bits_per_value
                        )
                        .into(),
                    ));
                }
                if bitpacking.values.is_some() {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} uses unsupported leaf buffer compression"
                        )
                        .into(),
                    ));
                }
                Ok(())
            }
            Compression::OutOfLineBitpacking(bitpacking) => {
                if !matches!(bitpacking.uncompressed_bits_per_value, 8 | 16 | 32 | 64) {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} out-of-line bitpacking width {} is unsupported",
                            bitpacking.uncompressed_bits_per_value
                        )
                        .into(),
                    ));
                }
                let values = Self::require_encoding(&bitpacking.values, label)?;
                let Some(Compression::Flat(flat)) = values.compression.as_ref() else {
                    return Err(Error::invalid_input_source(
                        format!("Sparse structural {label} bitpacked values must be flat").into(),
                    ));
                };
                Self::validate_flat(flat, label)
            }
            Compression::Constant(constant) => {
                if constant
                    .value
                    .as_ref()
                    .is_some_and(|value| value.len() != 8)
                {
                    return Err(Error::invalid_input_source(
                        format!("Sparse structural {label} constant must be 64 bits").into(),
                    ));
                }
                Ok(())
            }
            Compression::General(general) => {
                let compression = general.compression.as_ref().ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse structural {label} general compression is missing config")
                            .into(),
                    )
                })?;
                Self::validate_buffer_compression(Some(compression), label)?;
                Self::validate_block_encoding(
                    Self::require_encoding(&general.values, label)?,
                    label,
                )
            }
            Compression::Rle(rle) => {
                Self::validate_block_encoding(
                    Self::require_encoding(&rle.values, "RLE values")?,
                    "RLE values",
                )?;
                Self::validate_block_encoding(
                    Self::require_encoding(&rle.run_lengths, "RLE run lengths")?,
                    "RLE run lengths",
                )
            }
            _ => Err(Error::invalid_input_source(
                format!("Sparse structural {label} uses an unsupported block encoding").into(),
            )),
        }
    }

    fn validate_domain_chain(
        layers: &[pb21::SparseStructuralLayer],
        row_domain: u64,
        num_items: u64,
        num_visible_items: u64,
    ) -> Result<()> {
        let expected_num_items =
            layers
                .iter()
                .try_fold(num_visible_items, |items, layer| -> Result<u64> {
                    let additional = match Self::require_layer(layer)? {
                        pb21::sparse_structural_layer::Layer::List(layer) => {
                            let positions = Self::require_position_set(
                                &layer.non_empty_positions,
                                "list non-empty",
                            )?;
                            let num_non_empty = Self::position_cardinality(
                                positions,
                                layer.num_slots,
                                "list non-empty positions",
                            )?;
                            layer.num_slots.checked_sub(num_non_empty).ok_or_else(|| {
                                Error::invalid_input_source(
                                "Sparse structural list has more non-empty positions than slots"
                                    .into(),
                            )
                            })?
                        }
                        pb21::sparse_structural_layer::Layer::Validity(_)
                        | pb21::sparse_structural_layer::Layer::FixedSizeList(_) => 0,
                    };
                    items.checked_add(additional).ok_or_else(|| {
                        Error::invalid_input_source("Sparse structural item count overflows".into())
                    })
                })?;
        if num_items != expected_num_items {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout has {} structural items, but its layers imply {}",
                    num_items, expected_num_items
                )
                .into(),
            ));
        }
        let mut expected_slots = row_domain;
        for (layer_index, layer) in layers.iter().enumerate() {
            let num_slots = Self::layer_num_slots(layer)?;
            let num_child_slots = Self::layer_num_child_slots(layer)?;
            usize_from_u64(num_slots, "layer slot count")?;
            usize_from_u64(num_child_slots, "layer child slot count")?;
            if num_slots != expected_slots {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse structural layer {} has {} slots, expected {} from the outer domain",
                        layer_index, num_slots, expected_slots
                    )
                    .into(),
                ));
            }
            expected_slots = num_child_slots;
        }
        if expected_slots != num_visible_items {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural terminal domain has {} slots, expected {} visible items",
                    expected_slots, num_visible_items
                )
                .into(),
            ));
        }
        Ok(())
    }

    fn metadata_buffer(&self) -> Result<(u64, u64)> {
        self.buffer_offsets_and_sizes
            .first()
            .copied()
            .ok_or_else(|| {
                Error::invalid_input_source("Sparse layout is missing metadata buffer".into())
            })
    }

    fn value_buffer(&self) -> Result<(u64, u64)> {
        self.buffer_offsets_and_sizes
            .get(1)
            .copied()
            .ok_or_else(|| {
                Error::invalid_input_source("Sparse layout is missing value buffer".into())
            })
    }

    fn checked_buffer_range(position: u64, size: u64, label: &str) -> Result<Range<u64>> {
        let end = position.checked_add(size).ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} buffer range overflows").into(),
            )
        })?;
        Ok(position..end)
    }

    fn validate_page_buffers(
        buffer_offsets_and_sizes: &[(u64, u64)],
        num_visible_items: u64,
    ) -> Result<()> {
        let (_, metadata_size) = buffer_offsets_and_sizes.first().copied().ok_or_else(|| {
            Error::invalid_input_source("Sparse layout is missing metadata buffer".into())
        })?;
        if !metadata_size.is_multiple_of(8) {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout metadata buffer has {metadata_size} bytes, which is not a multiple of 8"
                )
                .into(),
            ));
        }
        let chunk_count = metadata_size / 8;
        if num_visible_items == 0 {
            if chunk_count != 0 {
                return Err(Error::invalid_input_source(
                    "Sparse layout with no visible items must have empty chunk metadata".into(),
                ));
            }
        } else {
            let min_chunks =
                num_visible_items.div_ceil(miniblock::MAX_CONFIGURABLE_MINIBLOCK_VALUES);
            if chunk_count < min_chunks || chunk_count > num_visible_items {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse layout metadata declares {chunk_count} chunks for {num_visible_items} visible items, expected {min_chunks}..={num_visible_items}"
                    )
                    .into(),
                ));
            }
        }

        let (_, value_size) = buffer_offsets_and_sizes.get(1).copied().ok_or_else(|| {
            Error::invalid_input_source("Sparse layout is missing value buffer".into())
        })?;
        if (num_visible_items == 0) != (value_size == 0) {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout value buffer has {value_size} bytes for {num_visible_items} visible items"
                )
                .into(),
            ));
        }

        for (position, size) in buffer_offsets_and_sizes.iter().skip(2) {
            Self::checked_buffer_range(*position, *size, "structural")?;
        }

        for (position, size) in buffer_offsets_and_sizes.iter().take(2) {
            Self::checked_buffer_range(*position, *size, "page")?;
        }
        Ok(())
    }

    fn require_position_set<'a>(
        set: &'a Option<pb21::SparsePositionSet>,
        label: &str,
    ) -> Result<&'a pb21::SparsePositionSet> {
        set.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} position set is required").into(),
            )
        })
    }

    fn require_count_set<'a>(
        set: &'a Option<pb21::SparseCountSet>,
        label: &str,
    ) -> Result<&'a pb21::SparseCountSet> {
        set.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} count set is required").into(),
            )
        })
    }

    fn require_validity_set<'a>(
        set: &'a Option<pb21::SparseValiditySet>,
        label: &str,
    ) -> Result<&'a pb21::SparseValiditySet> {
        set.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} validity set is required").into(),
            )
        })
    }

    fn validity_meaning(
        validity_set: &pb21::SparseValiditySet,
        label: &str,
    ) -> Result<SparseValidityMeaning> {
        match invalid_enum(
            pb21::sparse_validity_set::Meaning::try_from(validity_set.meaning),
            "validity meaning",
        )? {
            pb21::sparse_validity_set::Meaning::SparseValidityUnspecified => {
                Err(Error::invalid_input_source(
                    format!("Sparse structural {label} meaning is unspecified").into(),
                ))
            }
            pb21::sparse_validity_set::Meaning::SparseValidityNullPositions => {
                Ok(SparseValidityMeaning::NullPositions)
            }
            pb21::sparse_validity_set::Meaning::SparseValidityValidPositions => {
                Ok(SparseValidityMeaning::ValidPositions)
            }
        }
    }

    fn validity_buffer_count(
        validity_set: &pb21::SparseValiditySet,
        domain_len: u64,
        label: &str,
    ) -> Result<(usize, SparseValidityMeaning, u64)> {
        let meaning = Self::validity_meaning(validity_set, label)?;
        let position_set = validity_set.positions.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} positions are required").into(),
            )
        })?;
        let cardinality = Self::position_cardinality(position_set, domain_len, label)?;
        let buffer_count = Self::position_buffer_count(position_set, domain_len, label)?;
        Ok((buffer_count, meaning, cardinality))
    }

    fn position_cardinality(
        position_set: &pb21::SparsePositionSet,
        domain_len: u64,
        label: &str,
    ) -> Result<u64> {
        let positions = position_set.positions.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} position set is missing its variant").into(),
            )
        })?;
        let cardinality = position_set.num_positions;
        if cardinality > domain_len {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} cardinality {} exceeds domain {}",
                    cardinality, domain_len
                )
                .into(),
            ));
        }
        match positions {
            pb21::sparse_position_set::Positions::Empty(_) => {
                if cardinality != 0 {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} empty set has cardinality {}",
                            cardinality
                        )
                        .into(),
                    ));
                }
            }
            pb21::sparse_position_set::Positions::All(_) => {
                if domain_len == 0 || cardinality != domain_len {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} all set has cardinality {}, expected {}",
                            cardinality, domain_len
                        )
                        .into(),
                    ));
                }
            }
            pb21::sparse_position_set::Positions::Range(range) => {
                let end = range.start.checked_add(range.length).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse structural {label} range overflows").into(),
                    )
                })?;
                if range.length == 0 || range.length != cardinality || end > domain_len {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} range {}..{} does not match cardinality {} in domain {}",
                            range.start, end, cardinality, domain_len
                        )
                        .into(),
                    ));
                }
            }
            pb21::sparse_position_set::Positions::Explicit(compression) => {
                if cardinality == 0 {
                    return Err(Error::invalid_input_source(
                        format!("Sparse structural {label} has compression but no values").into(),
                    ));
                }
                Self::validate_compression(compression, label)?;
            }
        }
        Ok(cardinality)
    }

    fn position_buffer_count(
        position_set: &pb21::SparsePositionSet,
        domain_len: u64,
        label: &str,
    ) -> Result<usize> {
        Self::position_cardinality(position_set, domain_len, label)?;
        let positions = position_set.positions.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} position set is missing its variant").into(),
            )
        })?;
        Ok(usize::from(matches!(
            positions,
            pb21::sparse_position_set::Positions::Explicit(_)
        )))
    }

    fn count_buffer_count(
        count_set: &pb21::SparseCountSet,
        cardinality: u64,
        label: &str,
    ) -> Result<usize> {
        let counts = count_set.counts.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} count set is missing its variant").into(),
            )
        })?;
        match counts {
            pb21::sparse_count_set::Counts::Empty(_) => {
                if cardinality != 0 {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} empty count set has cardinality {}",
                            cardinality
                        )
                        .into(),
                    ));
                }
                Ok(0)
            }
            pb21::sparse_count_set::Counts::Constant(constant) => {
                if cardinality == 0 {
                    return Err(Error::invalid_input_source(
                        format!("Sparse structural {label} constant count has no values").into(),
                    ));
                }
                if constant.value == 0 {
                    return Err(Error::invalid_input_source(
                        format!("Sparse structural {label} constant count is zero").into(),
                    ));
                }
                Ok(0)
            }
            pb21::sparse_count_set::Counts::Explicit(compression) => {
                if cardinality == 0 {
                    return Err(Error::invalid_input_source(
                        format!("Sparse structural {label} has compression but no values").into(),
                    ));
                }
                Self::validate_compression(compression, label)?;
                Ok(1)
            }
        }
    }

    fn count_set_child_slots(
        count_set: &pb21::SparseCountSet,
        cardinality: u64,
        label: &str,
    ) -> Result<Option<u64>> {
        let counts = count_set.counts.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} count set is missing its variant").into(),
            )
        })?;
        match counts {
            pb21::sparse_count_set::Counts::Empty(_) => Ok(Some(0)),
            pb21::sparse_count_set::Counts::Constant(constant) => constant
                .value
                .checked_mul(cardinality)
                .map(Some)
                .ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} constant count sum overflows: value={}, len={}",
                            constant.value, cardinality
                        )
                        .into(),
                    )
                }),
            pb21::sparse_count_set::Counts::Explicit(_) => Ok(None),
        }
    }

    fn create_position_decompressor(
        compression: &CompressiveEncoding,
        label: &str,
        decompressors: &dyn DecompressionStrategy,
    ) -> Result<Arc<dyn BlockDecompressor>> {
        let compression = Self::validate_compression(compression, label)?;
        Self::validate_block_encoding(compression, label)?;
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decompressors.create_block_decompressor(compression)
        }))
        .map_err(|_| {
            Error::invalid_input_source(
                format!(
                    "Sparse structural {label} descriptor caused decompressor construction to panic"
                )
                .into(),
            )
        })?
        .map(Arc::from)
        .map_err(|error| {
            Error::invalid_input_source(
                format!("Sparse structural {label} decompressor construction failed: {error}")
                    .into(),
            )
        })
    }

    fn position_set_decoder(
        position_set: &pb21::SparsePositionSet,
        domain_len: u64,
        label: &str,
        decompressors: &dyn DecompressionStrategy,
    ) -> Result<(SparsePositionSetDecoder, u64)> {
        let cardinality = Self::position_cardinality(position_set, domain_len, label)?;
        let positions = position_set.positions.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} position set is missing its variant").into(),
            )
        })?;
        Ok((
            match positions {
                pb21::sparse_position_set::Positions::Empty(_) => SparsePositionSetDecoder::Empty,
                pb21::sparse_position_set::Positions::All(_) => {
                    SparsePositionSetDecoder::All { len: domain_len }
                }
                pb21::sparse_position_set::Positions::Range(range) => {
                    SparsePositionSetDecoder::Range {
                        start: range.start,
                        len: range.length,
                    }
                }
                pb21::sparse_position_set::Positions::Explicit(compression) => {
                    SparsePositionSetDecoder::Explicit {
                        decompressor: Self::create_position_decompressor(
                            compression,
                            label,
                            decompressors,
                        )?,
                        encoding: compression.clone(),
                        count: cardinality,
                        domain_len,
                    }
                }
            },
            cardinality,
        ))
    }

    fn validity_set_decoder(
        validity_set: &pb21::SparseValiditySet,
        domain_len: u64,
        label: &str,
        decompressors: &dyn DecompressionStrategy,
    ) -> Result<(SparseValiditySetDecoder, u64)> {
        let meaning = Self::validity_meaning(validity_set, label)?;
        let position_set = validity_set.positions.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} positions are required").into(),
            )
        })?;
        let (positions, cardinality) =
            Self::position_set_decoder(position_set, domain_len, label, decompressors)?;
        Ok((SparseValiditySetDecoder { meaning, positions }, cardinality))
    }

    fn num_valid_slots(
        meaning: SparseValidityMeaning,
        cardinality: u64,
        num_slots: u64,
        label: &str,
    ) -> Result<u64> {
        match meaning {
            SparseValidityMeaning::NullPositions => {
                num_slots.checked_sub(cardinality).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} null cardinality {} exceeds slots {}",
                            cardinality, num_slots
                        )
                        .into(),
                    )
                })
            }
            SparseValidityMeaning::ValidPositions => Ok(cardinality),
        }
    }

    fn count_set_decoder(
        count_set: &pb21::SparseCountSet,
        cardinality: u64,
        label: &str,
        decompressors: &dyn DecompressionStrategy,
    ) -> Result<SparseCountSetDecoder> {
        Self::count_buffer_count(count_set, cardinality, label)?;
        let counts = count_set.counts.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} count set is missing its variant").into(),
            )
        })?;
        Ok(match counts {
            pb21::sparse_count_set::Counts::Empty(_) => SparseCountSetDecoder::Empty,
            pb21::sparse_count_set::Counts::Constant(constant) => SparseCountSetDecoder::Constant {
                value: constant.value,
                len: cardinality,
            },
            pb21::sparse_count_set::Counts::Explicit(compression) => {
                SparseCountSetDecoder::Explicit {
                    decompressor: Self::create_position_decompressor(
                        compression,
                        label,
                        decompressors,
                    )?,
                    encoding: compression.clone(),
                    count: cardinality,
                }
            }
        })
    }

    fn add_buffer_count(count: &mut usize, additional: usize) -> Result<()> {
        *count = count.checked_add(additional).ok_or_else(|| {
            Error::invalid_input_source("Sparse structural buffer count overflows".into())
        })?;
        Ok(())
    }

    fn structural_buffer_count(layers: &[pb21::SparseStructuralLayer]) -> Result<usize> {
        layers
            .iter()
            .try_fold(0_usize, |mut count, layer| -> Result<usize> {
                match Self::require_layer(layer)? {
                    pb21::sparse_structural_layer::Layer::Validity(layer) => {
                        let (validity_buffers, _, _) = Self::validity_buffer_count(
                            Self::require_validity_set(&layer.validity, "validity")?,
                            layer.num_slots,
                            "validity positions",
                        )?;
                        Self::add_buffer_count(&mut count, validity_buffers)?;
                    }
                    pb21::sparse_structural_layer::Layer::List(layer) => {
                        let non_empty_positions = Self::require_position_set(
                            &layer.non_empty_positions,
                            "list non-empty",
                        )?;
                        let num_non_empty = Self::position_cardinality(
                            non_empty_positions,
                            layer.num_slots,
                            "list non-empty positions",
                        )?;
                        let non_empty_buffers = Self::position_buffer_count(
                            non_empty_positions,
                            layer.num_slots,
                            "list non-empty positions",
                        )?;
                        Self::add_buffer_count(&mut count, non_empty_buffers)?;
                        let (validity_buffers, validity_meaning, validity_cardinality) =
                            Self::validity_buffer_count(
                                Self::require_validity_set(&layer.validity, "list")?,
                                layer.num_slots,
                                "list validity positions",
                            )?;
                        Self::add_buffer_count(&mut count, validity_buffers)?;
                        let num_valid_slots = Self::num_valid_slots(
                            validity_meaning,
                            validity_cardinality,
                            layer.num_slots,
                            "list validity",
                        )?;
                        if num_non_empty > num_valid_slots {
                            return Err(Error::invalid_input_source(
                                format!(
                                    "Sparse structural list has {} non-empty slots but only {} valid slots",
                                    num_non_empty, num_valid_slots
                                )
                                .into(),
                            ));
                        }
                        let counts = Self::require_count_set(&layer.counts, "list counts")?;
                        let count_buffers =
                            Self::count_buffer_count(counts, num_non_empty, "list counts")?;
                        Self::add_buffer_count(&mut count, count_buffers)?;
                        if let Some(child_slots) =
                            Self::count_set_child_slots(counts, num_non_empty, "list counts")?
                            && child_slots != layer.num_child_slots
                        {
                            return Err(Error::invalid_input_source(
                                format!(
                                    "Sparse structural list count sum {} does not match child slots {}",
                                    child_slots, layer.num_child_slots
                                )
                                .into(),
                            ));
                        }
                    }
                    pb21::sparse_structural_layer::Layer::FixedSizeList(layer) => {
                        if layer.dimension == 0 {
                            return Err(Error::invalid_input_source(
                                "Sparse structural fixed-size-list dimension is zero".into(),
                            ));
                        }
                        layer.num_slots.checked_mul(layer.dimension).ok_or_else(|| {
                            Error::invalid_input_source(
                                format!(
                                    "Sparse structural fixed-size-list child slot count overflows: slots={}, dimension={}",
                                    layer.num_slots, layer.dimension
                                )
                                .into(),
                            )
                        })?;
                        let (validity_buffers, _, _) = Self::validity_buffer_count(
                            Self::require_validity_set(&layer.validity, "fixed-size-list")?,
                            layer.num_slots,
                            "fixed-size-list validity positions",
                        )?;
                        Self::add_buffer_count(&mut count, validity_buffers)?;
                    }
                }
                Ok(count)
            })
    }

    fn layer_decompressors(
        layer: &pb21::SparseStructuralLayer,
        decompressors: &dyn DecompressionStrategy,
    ) -> Result<SparseLayerDecompressors> {
        Ok(match Self::require_layer(layer)? {
            pb21::sparse_structural_layer::Layer::Validity(layer) => {
                let (validity, _) = Self::validity_set_decoder(
                    Self::require_validity_set(&layer.validity, "validity")?,
                    layer.num_slots,
                    "validity positions",
                    decompressors,
                )?;
                SparseLayerDecompressors::Validity {
                    num_slots: layer.num_slots,
                    validity,
                }
            }
            pb21::sparse_structural_layer::Layer::List(layer) => {
                let non_empty_positions =
                    Self::require_position_set(&layer.non_empty_positions, "list non-empty")?;
                let (non_empty_positions, num_non_empty) = Self::position_set_decoder(
                    non_empty_positions,
                    layer.num_slots,
                    "list non-empty positions",
                    decompressors,
                )?;
                let counts = Self::count_set_decoder(
                    Self::require_count_set(&layer.counts, "list counts")?,
                    num_non_empty,
                    "list counts",
                    decompressors,
                )?;
                let (validity, _) = Self::validity_set_decoder(
                    Self::require_validity_set(&layer.validity, "list")?,
                    layer.num_slots,
                    "list validity positions",
                    decompressors,
                )?;
                SparseLayerDecompressors::List {
                    num_slots: layer.num_slots,
                    num_child_slots: layer.num_child_slots,
                    non_empty_positions,
                    counts,
                    validity,
                }
            }
            pb21::sparse_structural_layer::Layer::FixedSizeList(layer) => {
                let (validity, _) = Self::validity_set_decoder(
                    Self::require_validity_set(&layer.validity, "fixed-size-list")?,
                    layer.num_slots,
                    "fixed-size-list validity positions",
                    decompressors,
                )?;
                SparseLayerDecompressors::FixedSizeList {
                    num_slots: layer.num_slots,
                    num_child_slots: layer.num_slots.checked_mul(layer.dimension).ok_or_else(
                        || {
                            Error::invalid_input_source(
                                "Sparse structural fixed-size-list child slot count overflows"
                                    .into(),
                            )
                        },
                    )?,
                    dimension: layer.dimension,
                    validity,
                }
            }
        })
    }

    fn parse_chunk_meta(&self, meta_bytes: Bytes) -> Result<Vec<ChunkMeta>> {
        if !meta_bytes.len().is_multiple_of(8) {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout metadata length {} is not a multiple of 8",
                    meta_bytes.len()
                )
                .into(),
            ));
        }

        let (value_buf_position, value_buf_size) = self.value_buffer()?;
        let value_buf_end = value_buf_position
            .checked_add(value_buf_size)
            .ok_or_else(|| {
                Error::invalid_input_source("Sparse layout value buffer range overflows".into())
            })?;
        let mut rows_counter = 0_u64;
        let mut offset_bytes = value_buf_position;
        let mut chunk_meta = Vec::with_capacity(meta_bytes.len() / 8);
        for chunk in meta_bytes.chunks_exact(8) {
            let entry: [u8; 8] = chunk.try_into().map_err(|_| {
                Error::invalid_input_source(
                    "Sparse layout chunk metadata entry is not 8 bytes".into(),
                )
            })?;
            let divided_bytes_minus_one = u32::from_le_bytes(
                entry
                    .get(..4)
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse layout chunk byte-size field is malformed".into(),
                        )
                    })?,
            );
            let num_values = u64::from(u32::from_le_bytes(
                entry
                    .get(4..)
                    .and_then(|bytes| bytes.try_into().ok())
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse layout chunk value-count field is malformed".into(),
                        )
                    })?,
            ));
            if num_values == 0 {
                return Err(Error::invalid_input_source(
                    "Sparse layout contains an empty value chunk".into(),
                ));
            }
            if num_values > miniblock::MAX_CONFIGURABLE_MINIBLOCK_VALUES {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse layout value chunk has {} values, exceeding the mini-block limit {}",
                        num_values,
                        miniblock::MAX_CONFIGURABLE_MINIBLOCK_VALUES
                    )
                    .into(),
                ));
            }
            let num_bytes = u64::from(divided_bytes_minus_one)
                .checked_add(1)
                .and_then(|units| units.checked_mul(MINIBLOCK_ALIGNMENT as u64))
                .ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse layout value chunk byte size overflows".into(),
                    )
                })?;
            rows_counter = rows_counter.checked_add(num_values).ok_or_else(|| {
                Error::invalid_input_source("Sparse layout visible item count overflows".into())
            })?;
            chunk_meta.push(ChunkMeta {
                num_values,
                chunk_size_bytes: num_bytes,
                offset_bytes,
            });
            offset_bytes = offset_bytes.checked_add(num_bytes).ok_or_else(|| {
                Error::invalid_input_source("Sparse layout value chunk byte range overflows".into())
            })?;
        }
        if rows_counter != self.num_visible_items {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout visible item count mismatch: metadata has {}, layout has {}",
                    rows_counter, self.num_visible_items
                )
                .into(),
            ));
        }
        if offset_bytes != value_buf_end {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout chunk metadata describes {} value bytes, but value buffer has {} bytes",
                    offset_bytes - value_buf_position,
                    value_buf_size
                )
                .into(),
            ));
        }
        Ok(chunk_meta)
    }

    fn validate_general_buffer_header(
        general: &pb21::General,
        data: &[u8],
        label: &str,
    ) -> Result<()> {
        let compression = general.compression.as_ref().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} general compression is missing config").into(),
            )
        })?;
        Self::validate_buffer_compression(Some(compression), label)?;
        let values = Self::require_encoding(&general.values, label)?;
        if Self::encoding_contains_general(values) {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} contains nested general compression, which is unsupported"
                )
                .into(),
            ));
        }

        let scheme = invalid_enum(
            pb21::CompressionScheme::try_from(compression.scheme),
            "compression scheme",
        )?;
        match scheme {
            pb21::CompressionScheme::CompressionAlgorithmLz4 => {
                data.get(..4).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} LZ4 buffer is missing its length prefix"
                        )
                        .into(),
                    )
                })?;
            }
            pb21::CompressionScheme::CompressionAlgorithmZstd => {
                data.get(..8).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural {label} Zstd buffer is missing its length prefix"
                        )
                        .into(),
                    )
                })?;
            }
            pb21::CompressionScheme::CompressionAlgorithmUnspecified => {
                return Err(Error::invalid_input_source(
                    format!("Sparse structural {label} general compression scheme is unspecified")
                        .into(),
                ));
            }
        }
        Ok(())
    }

    fn validate_general_child_buffer(
        encoding: &CompressiveEncoding,
        data: &[u8],
        label: &str,
    ) -> Result<()> {
        if let Some(pb21::compressive_encoding::Compression::General(general)) =
            encoding.compression.as_ref()
        {
            Self::validate_general_buffer_header(general, data, label)?;
        }
        Ok(())
    }

    fn validate_structural_buffer_headers(
        encoding: &CompressiveEncoding,
        data: &[u8],
        label: &str,
    ) -> Result<()> {
        use pb21::compressive_encoding::Compression;

        match encoding.compression.as_ref() {
            Some(Compression::General(general)) => {
                Self::validate_general_buffer_header(general, data, label)
            }
            Some(Compression::Rle(rle)) => {
                let values_size = u64::from_le_bytes(
                    data.get(..8)
                        .ok_or_else(|| {
                            Error::invalid_input_source(
                                format!(
                                    "Sparse structural {label} RLE buffer is missing its header"
                                )
                                .into(),
                            )
                        })?
                        .try_into()
                        .map_err(|_| {
                            Error::invalid_input_source(
                                format!("Sparse structural {label} RLE header is malformed").into(),
                            )
                        })?,
                );
                let values_size = usize_from_u64(values_size, "RLE values buffer size")?;
                let values_end = 8_usize.checked_add(values_size).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse structural {label} RLE values range overflows").into(),
                    )
                })?;
                let values_data = data.get(8..values_end).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse structural {label} RLE values buffer is truncated").into(),
                    )
                })?;
                let lengths_data = data.get(values_end..).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse structural {label} RLE run-length buffer is missing")
                            .into(),
                    )
                })?;
                Self::validate_general_child_buffer(
                    Self::require_encoding(&rle.values, "RLE values")?,
                    values_data,
                    "RLE values",
                )?;
                Self::validate_general_child_buffer(
                    Self::require_encoding(&rle.run_lengths, "RLE run lengths")?,
                    lengths_data,
                    "RLE run lengths",
                )
            }
            _ => Ok(()),
        }
    }

    fn decode_u64_values(
        decompressor: &dyn BlockDecompressor,
        encoding: &CompressiveEncoding,
        data: Bytes,
        num_values: u64,
        label: &str,
    ) -> Result<Vec<u64>> {
        Self::validate_structural_buffer_headers(encoding, &data, label)?;
        let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            decompressor.decompress(LanceBuffer::from_bytes(data, 1), num_values)
        }))
        .map_err(|_| {
            Error::invalid_input_source(
                format!("Sparse structural {label} decompression panicked").into(),
            )
        })?
        .map_err(|error| {
            Error::invalid_input_source(
                format!("Sparse structural {label} decompression failed: {error}").into(),
            )
        })?;
        let fixed = decoded.as_fixed_width().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} did not decode to fixed width data").into(),
            )
        })?;
        if fixed.bits_per_value != 64 {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} decoded to {} bits per value, expected 64",
                    fixed.bits_per_value
                )
                .into(),
            ));
        }
        if fixed.num_values != num_values {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} decoded {} values, expected {}",
                    fixed.num_values, num_values
                )
                .into(),
            ));
        }
        let num_values_usize = usize::try_from(num_values).map_err(|_| {
            Error::invalid_input_source(
                format!("Sparse structural {label} value count exceeds usize::MAX").into(),
            )
        })?;
        let expected_len = num_values_usize
            .checked_mul(std::mem::size_of::<u64>())
            .ok_or_else(|| {
                Error::invalid_input_source(
                    format!("Sparse structural {label} decoded byte length overflows").into(),
                )
            })?;
        if fixed.data.len() != expected_len {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} decoded {} bytes, expected {}",
                    fixed.data.len(),
                    expected_len
                )
                .into(),
            ));
        }
        let values = fixed.data.borrow_to_typed_slice::<u64>();
        if values.len() != num_values_usize {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} decoded {} u64 values, expected {}",
                    values.len(),
                    num_values
                )
                .into(),
            ));
        }
        Ok(values.to_vec())
    }

    fn decode_explicit_positions(
        decompressor: &Arc<dyn BlockDecompressor>,
        encoding: &CompressiveEncoding,
        data: Bytes,
        num_positions: u64,
        num_slots: u64,
        label: &str,
    ) -> Result<SparsePositionSet> {
        let deltas =
            Self::decode_u64_values(decompressor.as_ref(), encoding, data, num_positions, label)?;
        let mut positions = Vec::with_capacity(deltas.len());
        let mut current = 0_u64;
        for (idx, delta) in deltas.into_iter().enumerate() {
            if idx == 0 {
                current = delta;
            } else {
                if delta == 0 {
                    return Err(Error::invalid_input_source(
                        format!("Sparse structural {label} positions must be strictly increasing")
                            .into(),
                    ));
                }
                current = current.checked_add(delta).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse structural {label} position overflow").into(),
                    )
                })?;
            }
            if current >= num_slots {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse structural {label} position {} is outside layer with {} slots",
                        current, num_slots
                    )
                    .into(),
                ));
            }
            positions.push(current);
        }
        SparsePositionSet::from_positions(positions, num_slots, label)
    }

    fn decode_position_set(
        decoder: &SparsePositionSetDecoder,
        buffers: &mut impl Iterator<Item = Bytes>,
        label: &str,
    ) -> Result<SparsePositionSet> {
        match decoder {
            SparsePositionSetDecoder::Empty => Ok(SparsePositionSet::empty()),
            SparsePositionSetDecoder::All { len } => Ok(SparsePositionSet::all(*len)),
            SparsePositionSetDecoder::Range { start, len } => {
                Ok(SparsePositionSet::range(*start, *len))
            }
            SparsePositionSetDecoder::Explicit {
                decompressor,
                encoding,
                count,
                domain_len,
            } => Self::decode_explicit_positions(
                decompressor,
                encoding,
                Self::next_structural_buffer(buffers, label)?,
                *count,
                *domain_len,
                label,
            ),
        }
    }

    fn decode_validity_set(
        decoder: &SparseValiditySetDecoder,
        buffers: &mut impl Iterator<Item = Bytes>,
        label: &str,
    ) -> Result<SparseValiditySet> {
        Ok(SparseValiditySet {
            meaning: decoder.meaning,
            positions: Self::decode_position_set(&decoder.positions, buffers, label)?,
        })
    }

    fn decode_count_set(
        decoder: &SparseCountSetDecoder,
        buffers: &mut impl Iterator<Item = Bytes>,
        label: &str,
    ) -> Result<SparseCountSet> {
        match decoder {
            SparseCountSetDecoder::Empty => Ok(SparseCountSet::Empty),
            SparseCountSetDecoder::Constant { value, len } => {
                Ok(SparseCountSet::constant(*value, *len))
            }
            SparseCountSetDecoder::Explicit {
                decompressor,
                encoding,
                count,
            } => {
                let counts = Self::decode_u64_values(
                    decompressor.as_ref(),
                    encoding,
                    Self::next_structural_buffer(buffers, label)?,
                    *count,
                    label,
                )?;
                SparseCountSet::from_counts(counts)
            }
        }
    }

    fn next_structural_buffer(
        buffers: &mut impl Iterator<Item = Bytes>,
        label: &str,
    ) -> Result<Bytes> {
        buffers.next().ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} is missing its buffer").into(),
            )
        })
    }

    fn decode_layer(
        layer: &SparseLayerDecompressors,
        buffers: &mut impl Iterator<Item = Bytes>,
    ) -> Result<SparseStructuralLayerPlan> {
        Ok(match layer {
            SparseLayerDecompressors::Validity {
                num_slots,
                validity,
            } => SparseStructuralLayerPlan::Validity {
                num_slots: *num_slots,
                validity: Self::decode_validity_set(validity, buffers, "validity positions")?,
            },
            SparseLayerDecompressors::List {
                num_slots,
                num_child_slots,
                non_empty_positions,
                counts,
                validity,
            } => {
                let non_empty_positions = Self::decode_position_set(
                    non_empty_positions,
                    buffers,
                    "list non-empty positions",
                )?;
                let counts = Self::decode_count_set(counts, buffers, "list counts")?;
                let validity =
                    Self::decode_validity_set(validity, buffers, "list validity positions")?;
                let actual_child_slots = counts.sum()?;
                if actual_child_slots != *num_child_slots {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural list count sum {} does not match declared child slots {}",
                            actual_child_slots, num_child_slots
                        )
                        .into(),
                    ));
                }
                SparseStructuralLayerPlan::List {
                    num_slots: *num_slots,
                    num_child_slots: *num_child_slots,
                    non_empty_positions,
                    counts,
                    validity,
                }
            }
            SparseLayerDecompressors::FixedSizeList {
                num_slots,
                num_child_slots,
                dimension,
                validity,
            } => {
                let expected_child_slots = num_slots.checked_mul(*dimension).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural fixed-size-list child slot count overflows: slots={}, dimension={}",
                            num_slots, dimension
                        )
                        .into(),
                    )
                })?;
                if expected_child_slots != *num_child_slots {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural fixed-size-list child slot count {} does not match slots {} * dimension {}",
                            num_child_slots, num_slots, dimension
                        )
                        .into(),
                    ));
                }
                SparseStructuralLayerPlan::FixedSizeList {
                    num_slots: *num_slots,
                    dimension: *dimension,
                    validity: Self::decode_validity_set(
                        validity,
                        buffers,
                        "fixed-size-list validity positions",
                    )?,
                }
            }
        })
    }

    fn lookup_value_chunks(&self, chunk_indices: &[usize]) -> Result<Vec<LoadedChunk>> {
        let page_meta = self.page_meta.as_ref().ok_or_else(|| {
            Error::internal("Sparse page scheduler has not been initialized".to_string())
        })?;
        chunk_indices
            .iter()
            .map(|&chunk_idx| {
                let chunk_meta = page_meta.chunk_meta.get(chunk_idx).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse layout missing value chunk metadata for chunk {chunk_idx}")
                            .into(),
                    )
                })?;
                let bytes_start = chunk_meta.offset_bytes;
                let bytes_end = bytes_start
                    .checked_add(chunk_meta.chunk_size_bytes)
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            format!(
                                "Sparse layout value chunk {} byte range overflows",
                                chunk_idx
                            )
                            .into(),
                        )
                    })?;
                Ok(LoadedChunk {
                    byte_range: bytes_start..bytes_end,
                    items_in_chunk: chunk_meta.num_values,
                    chunk_idx,
                    data: LanceBuffer::empty(),
                })
            })
            .collect()
    }

    fn value_chunk_index(chunk_value_offsets: &[u64], value: u64) -> Result<usize> {
        let total_values = chunk_value_offsets.last().copied().ok_or_else(|| {
            Error::invalid_input_source("Sparse layout has no value chunk offsets".into())
        })?;
        if chunk_value_offsets.len() < 2 || value >= total_values {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout value index {} is outside {} visible items",
                    value, total_values
                )
                .into(),
            ));
        }
        chunk_value_offsets
            .partition_point(|&offset| offset <= value)
            .checked_sub(1)
            .ok_or_else(|| {
                Error::invalid_input_source(
                    format!("Sparse layout value index {value} is before the first chunk").into(),
                )
            })
    }

    fn value_chunk_range(
        chunk_value_offsets: &[u64],
        value_range: Range<u64>,
    ) -> Result<Range<usize>> {
        if value_range.is_empty() {
            return Ok(0..0);
        }
        let total_values = chunk_value_offsets.last().copied().ok_or_else(|| {
            Error::invalid_input_source("Sparse layout has no value chunk offsets".into())
        })?;
        if value_range.start > value_range.end || value_range.end > total_values {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse layout value range {}..{} is outside {} visible items",
                    value_range.start, value_range.end, total_values
                )
                .into(),
            ));
        }
        let start = Self::value_chunk_index(chunk_value_offsets, value_range.start)?;
        let end = chunk_value_offsets
            .partition_point(|&offset| offset < value_range.end)
            .max(start + 1);
        Ok(start..end)
    }
}

impl StructuralPageScheduler for SparseStructuralScheduler {
    fn initialize<'a>(
        &'a mut self,
        io: &Arc<dyn EncodingsIo>,
    ) -> BoxFuture<'a, Result<Arc<dyn CachedPageData>>> {
        let (meta_buf_position, meta_buf_size) = match self.metadata_buffer() {
            Ok(buffer) => buffer,
            Err(err) => return std::future::ready(Err(err)).boxed(),
        };
        let required_ranges = match (|| -> Result<Vec<Range<u64>>> {
            let mut required_ranges = Vec::new();
            required_ranges.push(Self::checked_buffer_range(
                meta_buf_position,
                meta_buf_size,
                "metadata",
            )?);
            for (position, size) in self.buffer_offsets_and_sizes.iter().skip(2) {
                required_ranges.push(Self::checked_buffer_range(*position, *size, "structural")?);
            }
            Ok(required_ranges)
        })() {
            Ok(ranges) => ranges,
            Err(err) => return std::future::ready(Err(err)).boxed(),
        };
        let io_req = io.submit_request(required_ranges, 0);

        async move {
            let mut buffers = io_req.await?.into_iter();
            let meta_bytes = buffers.next().ok_or_else(|| {
                Error::invalid_input_source("Sparse layout is missing chunk metadata buffer".into())
            })?;

            let chunk_meta = self.parse_chunk_meta(meta_bytes)?;
            let mut chunk_value_offsets = Vec::with_capacity(chunk_meta.len() + 1);
            let mut value_offset = 0_u64;
            chunk_value_offsets.push(value_offset);
            for chunk in &chunk_meta {
                value_offset = value_offset.checked_add(chunk.num_values).ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse layout visible item offset overflows".into(),
                    )
                })?;
                chunk_value_offsets.push(value_offset);
            }

            let layers = self
                .layer_decompressors
                .iter()
                .map(|layer| Self::decode_layer(layer, &mut buffers))
                .collect::<Result<Vec<_>>>()?;
            let plan = SparseStructuralPlan {
                layers,
                num_items: self.num_items,
                num_visible_items: self.num_visible_items,
            };
            plan.validate(self.row_domain)?;
            if buffers.next().is_some() {
                return Err(Error::invalid_input_source(
                    "Sparse layout has unused structural buffers".into(),
                ));
            }

            let page_meta = Arc::new(SparseStructuralCacheableState {
                chunk_meta,
                chunk_value_offsets: chunk_value_offsets.into(),
                plan,
                row_domain: self.row_domain,
            });
            self.page_meta = Some(page_meta.clone());
            Ok(page_meta as Arc<dyn CachedPageData>)
        }
        .boxed()
    }

    fn load(&mut self, data: &Arc<dyn CachedPageData>) {
        self.page_meta = data
            .clone()
            .as_arc_any()
            .downcast::<SparseStructuralCacheableState>()
            .ok();
    }

    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>> {
        let page_meta = self.page_meta.as_ref().ok_or_else(|| {
            Error::internal("Sparse page scheduler has not been initialized".to_string())
        })?;
        let encoded_row_domain = page_meta
            .row_domain
            .checked_mul(self.row_scale)
            .ok_or_else(|| {
                Error::invalid_input_source("Sparse structural encoded row domain overflows".into())
            })?;
        let num_rows = validate_slice_ranges(ranges, encoded_row_domain, "row")?;
        let ranges = ranges
            .iter()
            .map(|range| {
                if !range.start.is_multiple_of(self.row_scale)
                    || !range.end.is_multiple_of(self.row_scale)
                {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural encoded row range {}..{} is not aligned to fixed-size-list scale {}",
                            range.start, range.end, self.row_scale
                        )
                        .into(),
                    ));
                }
                Ok((range.start / self.row_scale)..(range.end / self.row_scale))
            })
            .collect::<Result<Vec<_>>>()?;

        let mut chunks_needed = Vec::new();
        let selection = slice_sparse_plan(&page_meta.plan, &ranges, page_meta.row_domain)?;
        for value_range in &selection.leaf_ranges {
            chunks_needed.extend(Self::value_chunk_range(
                &page_meta.chunk_value_offsets,
                value_range.clone(),
            )?);
        }
        chunks_needed.sort_unstable();
        chunks_needed.dedup();

        let mut loaded_chunks = self.lookup_value_chunks(&chunks_needed)?;
        let chunk_ranges = loaded_chunks
            .iter()
            .map(|chunk| chunk.byte_range.clone())
            .collect::<Vec<_>>();
        let loaded_chunk_data = io.submit_request(chunk_ranges, self.priority);
        let ranges = VecDeque::from(ranges);
        let value_decompressor = self.value_decompressor.clone();
        let value_encoding = self.value_encoding.clone();
        let data_type = self.data_type.clone();
        let page_meta = page_meta.clone();
        let num_buffers = self.num_buffers;
        let has_large_chunk = self.has_large_chunk;
        let row_scale = self.row_scale;

        let res = async move {
            let loaded_chunk_data = loaded_chunk_data.await?;
            for (loaded_chunk, chunk_data) in loaded_chunks.iter_mut().zip(loaded_chunk_data) {
                loaded_chunk.data = LanceBuffer::from_bytes(chunk_data, 1);
            }

            Ok(Box::new(SparseStructuralDecoder {
                value_decompressor,
                value_encoding,
                data_type,
                page_meta,
                loaded_chunks: Arc::new(loaded_chunks),
                ranges,
                offset_in_current_range: 0,
                num_rows,
                row_scale,
                num_buffers,
                has_large_chunk,
            }) as Box<dyn StructuralPageDecoder>)
        }
        .boxed();
        Ok(vec![PageLoadTask {
            decoder_fut: res,
            num_rows,
        }])
    }
}

#[derive(Debug)]
struct SparseStructuralDecoder {
    value_decompressor: Arc<dyn MiniBlockDecompressor>,
    value_encoding: CompressiveEncoding,
    data_type: DataType,
    page_meta: Arc<SparseStructuralCacheableState>,
    loaded_chunks: Arc<Vec<LoadedChunk>>,
    ranges: VecDeque<Range<u64>>,
    offset_in_current_range: u64,
    num_rows: u64,
    row_scale: u64,
    num_buffers: u64,
    has_large_chunk: bool,
}

impl SparseStructuralDecoder {
    fn drain_ranges(&mut self, mut rows_desired: u64) -> Result<Vec<Range<u64>>> {
        if !rows_desired.is_multiple_of(self.row_scale) {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse page decoder drain of {} encoded rows is not aligned to fixed-size-list scale {}",
                    rows_desired, self.row_scale
                )
                .into(),
            ));
        }
        rows_desired /= self.row_scale;
        let mut ranges = Vec::new();
        while rows_desired > 0 {
            let range = self.ranges.front().ok_or_else(|| {
                Error::invalid_input_source(
                    "Sparse page decoder was asked to drain more rows than were scheduled".into(),
                )
            })?;
            let start = range
                .start
                .checked_add(self.offset_in_current_range)
                .ok_or_else(|| {
                    Error::invalid_input_source("Sparse page decoder row offset overflows".into())
                })?;
            let rows_available = range.end.checked_sub(start).ok_or_else(|| {
                Error::invalid_input_source(
                    "Sparse page decoder range offset exceeds the scheduled range".into(),
                )
            })?;
            let rows_to_take = rows_available.min(rows_desired);
            let end = start.checked_add(rows_to_take).ok_or_else(|| {
                Error::invalid_input_source("Sparse page decoder row range overflows".into())
            })?;
            ranges.push(start..end);
            rows_desired -= rows_to_take;
            self.offset_in_current_range += rows_to_take;
            if self.offset_in_current_range == range.end - range.start {
                self.offset_in_current_range = 0;
                self.ranges.pop_front();
            }
        }
        Ok(ranges)
    }
}

impl StructuralPageDecoder for SparseStructuralDecoder {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn DecodePageTask>> {
        Ok(Box::new(DecodeSparseStructuralTask {
            row_ranges: self.drain_ranges(num_rows)?,
            value_decompressor: self.value_decompressor.clone(),
            value_encoding: self.value_encoding.clone(),
            data_type: self.data_type.clone(),
            page_meta: self.page_meta.clone(),
            loaded_chunks: self.loaded_chunks.clone(),
            num_buffers: self.num_buffers,
            has_large_chunk: self.has_large_chunk,
        }))
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

#[derive(Debug)]
struct DecodeSparseStructuralTask {
    row_ranges: Vec<Range<u64>>,
    value_decompressor: Arc<dyn MiniBlockDecompressor>,
    value_encoding: CompressiveEncoding,
    data_type: DataType,
    page_meta: Arc<SparseStructuralCacheableState>,
    loaded_chunks: Arc<Vec<LoadedChunk>>,
    num_buffers: u64,
    has_large_chunk: bool,
}

impl DecodeSparseStructuralTask {
    fn read_chunk_size(
        buf: &[u8],
        offset: &mut usize,
        width: usize,
        chunk_idx: usize,
    ) -> Result<u32> {
        let end = offset.checked_add(width).ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural value chunk {chunk_idx} size header overflows").into(),
            )
        })?;
        let bytes = buf.get(*offset..end).ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural value chunk {chunk_idx} has a truncated size header")
                    .into(),
            )
        })?;
        let size = match width {
            2 => u32::from(u16::from_le_bytes(bytes.try_into().map_err(|_| {
                Error::invalid_input_source(
                    format!("Sparse structural value chunk {chunk_idx} has a malformed u16 size")
                        .into(),
                )
            })?)),
            4 => u32::from_le_bytes(bytes.try_into().map_err(|_| {
                Error::invalid_input_source(
                    format!("Sparse structural value chunk {chunk_idx} has a malformed u32 size")
                        .into(),
                )
            })?),
            _ => {
                return Err(Error::internal(format!(
                    "Unsupported sparse value chunk size width {width}"
                )));
            }
        };
        *offset = end;
        Ok(size)
    }

    fn expected_fixed_bytes(num_values: u64, bits_per_value: u64, label: &str) -> Result<usize> {
        let bits = num_values.checked_mul(bits_per_value).ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} decoded bit length overflows").into(),
            )
        })?;
        usize_from_u64(bits.div_ceil(8), label)
    }

    fn validate_fixed_buffer(
        buffer: &LanceBuffer,
        num_values: u64,
        bits_per_value: u64,
        label: &str,
    ) -> Result<()> {
        let expected = Self::expected_fixed_bytes(num_values, bits_per_value, label)?;
        if buffer.len() != expected {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} buffer has {} bytes, expected {}",
                    buffer.len(),
                    expected
                )
                .into(),
            ));
        }
        Ok(())
    }

    fn validate_variable_buffer(
        buffer: &LanceBuffer,
        num_values: u64,
        bits_per_offset: u64,
    ) -> Result<()> {
        let width = usize_from_u64(bits_per_offset / 8, "variable offset width")?;
        let offset_count = num_values.checked_add(1).ok_or_else(|| {
            Error::invalid_input_source("Sparse variable offset count overflows".into())
        })?;
        let table_len = usize_from_u64(offset_count, "variable offset count")?
            .checked_mul(width)
            .ok_or_else(|| {
                Error::invalid_input_source("Sparse variable offset table size overflows".into())
            })?;
        if buffer.len() < table_len {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse variable buffer has {} bytes, smaller than its {}-byte offset table",
                    buffer.len(),
                    table_len
                )
                .into(),
            ));
        }
        let mut previous = None;
        for index in 0..usize_from_u64(offset_count, "variable offset count")? {
            let start = index.checked_mul(width).ok_or_else(|| {
                Error::invalid_input_source("Sparse variable offset index overflows".into())
            })?;
            let end = start.checked_add(width).ok_or_else(|| {
                Error::invalid_input_source("Sparse variable offset range overflows".into())
            })?;
            let bytes = buffer.as_ref().get(start..end).ok_or_else(|| {
                Error::invalid_input_source("Sparse variable offset table is truncated".into())
            })?;
            let offset = match width {
                4 => u64::from(u32::from_le_bytes(bytes.try_into().map_err(|_| {
                    Error::invalid_input_source("Sparse variable u32 offset is malformed".into())
                })?)),
                8 => u64::from_le_bytes(bytes.try_into().map_err(|_| {
                    Error::invalid_input_source("Sparse variable u64 offset is malformed".into())
                })?),
                _ => {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse variable offset width {} is unsupported",
                            bits_per_offset
                        )
                        .into(),
                    ));
                }
            };
            if offset < table_len as u64 || offset > buffer.len() as u64 {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse variable offset {} is outside payload range {}..{}",
                        offset,
                        table_len,
                        buffer.len()
                    )
                    .into(),
                ));
            }
            if previous.is_some_and(|previous| offset < previous) {
                return Err(Error::invalid_input_source(
                    "Sparse variable offsets are not monotonically increasing".into(),
                ));
            }
            previous = Some(offset);
        }
        Ok(())
    }

    fn validate_fsl_buffers(
        fsl: &pb21::FixedSizeList,
        buffers: &[LanceBuffer],
        num_values: u64,
        buffer_index: &mut usize,
    ) -> Result<()> {
        use pb21::compressive_encoding::Compression;

        let child_values = num_values.checked_mul(fsl.items_per_value).ok_or_else(|| {
            Error::invalid_input_source("Sparse fixed-size-list value count overflows".into())
        })?;
        if fsl.has_validity {
            let validity = buffers.get(*buffer_index).ok_or_else(|| {
                Error::invalid_input_source(
                    "Sparse fixed-size-list value validity buffer is missing".into(),
                )
            })?;
            Self::validate_fixed_buffer(validity, child_values, 1, "fixed-size-list validity")?;
            *buffer_index = buffer_index.checked_add(1).ok_or_else(|| {
                Error::invalid_input_source("Sparse fixed-size-list buffer index overflows".into())
            })?;
        }
        let values = fsl.values.as_deref().ok_or_else(|| {
            Error::invalid_input_source("Sparse fixed-size-list value encoding is missing".into())
        })?;
        match values.compression.as_ref() {
            Some(Compression::FixedSizeList(inner)) => {
                Self::validate_fsl_buffers(inner, buffers, child_values, buffer_index)
            }
            Some(Compression::Flat(flat)) => {
                let values = buffers.get(*buffer_index).ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse fixed-size-list leaf value buffer is missing".into(),
                    )
                })?;
                Self::validate_fixed_buffer(
                    values,
                    child_values,
                    flat.bits_per_value,
                    "fixed-size-list leaf values",
                )?;
                *buffer_index = buffer_index.checked_add(1).ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse fixed-size-list buffer index overflows".into(),
                    )
                })?;
                Ok(())
            }
            _ => Err(Error::invalid_input_source(
                "Sparse fixed-size-list value encoding is malformed".into(),
            )),
        }
    }

    fn validate_value_buffers(&self, buffers: &[LanceBuffer], num_values: u64) -> Result<()> {
        use pb21::compressive_encoding::Compression;

        let compression = self.value_encoding.compression.as_ref().ok_or_else(|| {
            Error::invalid_input_source("Sparse value compression is missing".into())
        })?;
        match compression {
            Compression::Flat(flat) => Self::validate_fixed_buffer(
                buffers.first().ok_or_else(|| {
                    Error::invalid_input_source("Sparse flat value buffer is missing".into())
                })?,
                num_values,
                flat.bits_per_value,
                "flat values",
            ),
            Compression::InlineBitpacking(bitpacking) => {
                let buffer = buffers.first().ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse inline-bitpacked value buffer is missing".into(),
                    )
                })?;
                if num_values > 1024 {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse inline-bitpacked chunk has {} values, exceeding 1024",
                            num_values
                        )
                        .into(),
                    ));
                }
                let word_bytes = usize_from_u64(
                    bitpacking.uncompressed_bits_per_value / 8,
                    "inline bitpacking word width",
                )?;
                let header = buffer.as_ref().get(..word_bytes).ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse inline-bitpacked buffer is missing its bit-width header".into(),
                    )
                })?;
                let bit_width =
                    header
                        .iter()
                        .enumerate()
                        .try_fold(0_u64, |value, (idx, byte)| {
                            let shift = u32::try_from(idx.checked_mul(8).ok_or_else(|| {
                                Error::invalid_input_source(
                                    "Sparse inline bit-width shift overflows".into(),
                                )
                            })?)
                            .map_err(|_| {
                                Error::invalid_input_source(
                                    "Sparse inline bit-width shift exceeds u32".into(),
                                )
                            })?;
                            Ok::<_, Error>(value | (u64::from(*byte) << shift))
                        })?;
                if bit_width > bitpacking.uncompressed_bits_per_value {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse inline bit width {} exceeds uncompressed width {}",
                            bit_width, bitpacking.uncompressed_bits_per_value
                        )
                        .into(),
                    ));
                }
                let payload_bytes = usize_from_u64(
                    bit_width.checked_mul(1024).ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse inline-bitpacked payload size overflows".into(),
                        )
                    })? / 8,
                    "inline-bitpacked payload size",
                )?;
                let expected = word_bytes.checked_add(payload_bytes).ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse inline-bitpacked buffer size overflows".into(),
                    )
                })?;
                if buffer.len() != expected {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse inline-bitpacked buffer has {} bytes, expected {}",
                            buffer.len(),
                            expected
                        )
                        .into(),
                    ));
                }
                Ok(())
            }
            Compression::Variable(variable) => {
                let offsets = variable
                    .offsets
                    .as_deref()
                    .and_then(|encoding| encoding.compression.as_ref())
                    .and_then(|compression| match compression {
                        Compression::Flat(flat) => Some(flat),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse variable offset encoding is malformed".into(),
                        )
                    })?;
                Self::validate_variable_buffer(
                    buffers.first().ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse variable value buffer is missing".into(),
                        )
                    })?,
                    num_values,
                    offsets.bits_per_value,
                )
            }
            Compression::Fsst(fsst) => {
                let variable = fsst
                    .values
                    .as_deref()
                    .and_then(|encoding| encoding.compression.as_ref())
                    .and_then(|compression| match compression {
                        Compression::Variable(variable) => Some(variable),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse FSST value encoding is malformed".into(),
                        )
                    })?;
                let offsets = variable
                    .offsets
                    .as_deref()
                    .and_then(|encoding| encoding.compression.as_ref())
                    .and_then(|compression| match compression {
                        Compression::Flat(flat) => Some(flat),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse FSST offset encoding is malformed".into(),
                        )
                    })?;
                Self::validate_variable_buffer(
                    buffers.first().ok_or_else(|| {
                        Error::invalid_input_source("Sparse FSST value buffer is missing".into())
                    })?,
                    num_values,
                    offsets.bits_per_value,
                )
            }
            Compression::ByteStreamSplit(split) => {
                let bits = split
                    .values
                    .as_deref()
                    .and_then(|encoding| encoding.compression.as_ref())
                    .and_then(|compression| match compression {
                        Compression::Flat(flat) => Some(flat.bits_per_value),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse byte-stream-split encoding is malformed".into(),
                        )
                    })?;
                Self::validate_fixed_buffer(
                    buffers.first().ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse byte-stream-split buffer is missing".into(),
                        )
                    })?,
                    num_values,
                    bits,
                    "byte-stream-split values",
                )
            }
            Compression::FixedSizeList(fsl) => {
                let mut buffer_index = 0;
                Self::validate_fsl_buffers(fsl, buffers, num_values, &mut buffer_index)?;
                if buffer_index != buffers.len() {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse fixed-size-list descriptor consumed {} of {} buffers",
                            buffer_index,
                            buffers.len()
                        )
                        .into(),
                    ));
                }
                Ok(())
            }
            Compression::PackedStruct(packed) => {
                let bits = packed.bits_per_value.iter().try_fold(0_u64, |sum, bits| {
                    sum.checked_add(*bits).ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse packed-struct bit width sum overflows".into(),
                        )
                    })
                })?;
                Self::validate_fixed_buffer(
                    buffers.first().ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse packed-struct value buffer is missing".into(),
                        )
                    })?,
                    num_values,
                    bits,
                    "packed-struct values",
                )
            }
            Compression::Rle(rle) => {
                if buffers.len() != 2 {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse RLE value chunk has {} buffers, expected 2",
                            buffers.len()
                        )
                        .into(),
                    ));
                }
                SparseStructuralScheduler::validate_general_child_buffer(
                    SparseStructuralScheduler::require_encoding(&rle.values, "RLE values")?,
                    buffers
                        .first()
                        .ok_or_else(|| {
                            Error::invalid_input_source(
                                "Sparse RLE value buffer is missing after count validation".into(),
                            )
                        })?
                        .as_ref(),
                    "value chunk RLE values",
                )?;
                SparseStructuralScheduler::validate_general_child_buffer(
                    SparseStructuralScheduler::require_encoding(
                        &rle.run_lengths,
                        "RLE run lengths",
                    )?,
                    buffers
                        .get(1)
                        .ok_or_else(|| {
                            Error::invalid_input_source(
                                "Sparse RLE run-length buffer is missing after count validation"
                                    .into(),
                            )
                        })?
                        .as_ref(),
                    "value chunk RLE run lengths",
                )
            }
            Compression::General(general) => {
                let buffer = buffers.first().ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse general-compressed value chunk is missing its first buffer".into(),
                    )
                })?;
                SparseStructuralScheduler::validate_general_buffer_header(
                    general,
                    buffer.as_ref(),
                    "value chunk",
                )
            }
            _ => Err(Error::invalid_input_source(
                "Sparse value chunk uses an unsupported compression descriptor".into(),
            )),
        }
    }

    fn loaded_chunk(&self, chunk_idx: usize) -> Result<&LoadedChunk> {
        let index = self
            .loaded_chunks
            .binary_search_by_key(&chunk_idx, |chunk| chunk.chunk_idx)
            .map_err(|_| {
                Error::internal(format!(
                    "Sparse structural decode missing loaded value chunk {}",
                    chunk_idx
                ))
            })?;
        self.loaded_chunks.get(index).ok_or_else(|| {
            Error::internal(format!(
                "Sparse structural loaded chunk index {} is missing",
                index
            ))
        })
    }

    fn decode_value_chunk(&self, chunk: &LoadedChunk) -> Result<DataBlock> {
        let buf = &chunk.data;
        if buf.len() < 2 {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural value chunk {} is too small for its header: {} bytes",
                    chunk.chunk_idx,
                    buf.len()
                )
                .into(),
            ));
        }
        let num_levels = u16::from_le_bytes(
            buf.as_ref()
                .get(..2)
                .and_then(|bytes| bytes.try_into().ok())
                .ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural value chunk {} has a malformed level header",
                            chunk.chunk_idx
                        )
                        .into(),
                    )
                })?,
        );
        let mut offset: usize = 2;
        if num_levels != 0 {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural value chunk unexpectedly contains {} rep/def levels",
                    num_levels
                )
                .into(),
            ));
        }

        let size_width = if self.has_large_chunk { 4 } else { 2 };
        let num_buffers = usize::try_from(self.num_buffers).map_err(|_| {
            Error::invalid_input_source(
                format!(
                    "Sparse structural value chunk has too many buffers: {}",
                    self.num_buffers
                )
                .into(),
            )
        })?;
        let sizes_len = num_buffers.checked_mul(size_width).ok_or_else(|| {
            Error::invalid_input_source(
                "Sparse structural value chunk buffer-size header overflows".into(),
            )
        })?;
        let header_len = offset.checked_add(sizes_len).ok_or_else(|| {
            Error::invalid_input_source(
                "Sparse structural value chunk header length overflows".into(),
            )
        })?;
        if buf.len() < header_len {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural value chunk {} is too small for {} buffer sizes: {} bytes",
                    chunk.chunk_idx,
                    self.num_buffers,
                    buf.len()
                )
                .into(),
            ));
        }
        let buffer_sizes = (0..num_buffers)
            .map(|_| Self::read_chunk_size(buf, &mut offset, size_width, chunk.chunk_idx))
            .collect::<Result<Vec<_>>>()?;

        offset = offset
            .checked_add(pad_bytes::<MINIBLOCK_ALIGNMENT>(offset))
            .ok_or_else(|| {
                Error::invalid_input_source(
                    format!(
                        "Sparse structural value chunk {} padded header overflows",
                        chunk.chunk_idx
                    )
                    .into(),
                )
            })?;
        if offset > buf.len() {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural value chunk {} is missing padding after its header",
                    chunk.chunk_idx
                )
                .into(),
            ));
        }
        let buffers = buffer_sizes
            .into_iter()
            .map(|buf_size| {
                let buf_size = buf_size as usize;
                let end = offset.checked_add(buf_size).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural value chunk {} buffer size overflows",
                            chunk.chunk_idx
                        )
                        .into(),
                    )
                })?;
                if end > buf.len() {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural value chunk {} buffer extends past chunk end",
                            chunk.chunk_idx
                        )
                        .into(),
                    ));
                }
                let buffer = buf.slice_with_length(offset, buf_size);
                offset = end;
                offset = offset
                    .checked_add(pad_bytes::<MINIBLOCK_ALIGNMENT>(offset))
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            format!(
                                "Sparse structural value chunk {} padded buffer range overflows",
                                chunk.chunk_idx
                            )
                            .into(),
                        )
                    })?;
                if offset > buf.len() {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural value chunk {} padding extends past chunk end",
                            chunk.chunk_idx
                        )
                        .into(),
                    ));
                }
                Ok(buffer)
            })
            .collect::<Result<Vec<_>>>()?;

        if offset != buf.len() {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural value chunk {} consumed {} of {} bytes",
                    chunk.chunk_idx,
                    offset,
                    buf.len()
                )
                .into(),
            ));
        }

        self.validate_value_buffers(&buffers, chunk.items_in_chunk)?;

        let decoded = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.value_decompressor
                .decompress(buffers, chunk.items_in_chunk)
        }))
        .map_err(|_| {
            Error::invalid_input_source(
                format!(
                    "Sparse structural value chunk {} decompression panicked",
                    chunk.chunk_idx
                )
                .into(),
            )
        })?
        .map_err(|error| {
            Error::invalid_input_source(
                format!(
                    "Sparse structural value chunk {} decompression failed: {error}",
                    chunk.chunk_idx
                )
                .into(),
            )
        })?;
        if decoded.num_values() != chunk.items_in_chunk {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural value chunk {} decoded {} values, expected {}",
                    chunk.chunk_idx,
                    decoded.num_values(),
                    chunk.items_in_chunk
                )
                .into(),
            ));
        }
        Ok(decoded)
    }

    fn append_value_range(
        &self,
        value_range: Range<u64>,
        data_builder: &mut DataBlockBuilder,
        chunk_cache: &mut Option<(usize, DataBlock)>,
    ) -> Result<()> {
        let mut value_start = value_range.start;
        while value_start < value_range.end {
            let chunk_idx = SparseStructuralScheduler::value_chunk_index(
                &self.page_meta.chunk_value_offsets,
                value_start,
            )?;
            let chunk_value_start = *self
                .page_meta
                .chunk_value_offsets
                .get(chunk_idx)
                .ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse value chunk {} has no start offset", chunk_idx).into(),
                    )
                })?;
            let chunk_value_end = *self
                .page_meta
                .chunk_value_offsets
                .get(chunk_idx.checked_add(1).ok_or_else(|| {
                    Error::invalid_input_source("Sparse value chunk index overflows".into())
                })?)
                .ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse value chunk {} has no end offset", chunk_idx).into(),
                    )
                })?;
            let take_end = value_range.end.min(chunk_value_end);
            if value_start < chunk_value_start || take_end <= value_start {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse value range {}..{} does not make progress in chunk {} covering {}..{}",
                        value_range.start,
                        value_range.end,
                        chunk_idx,
                        chunk_value_start,
                        chunk_value_end
                    )
                    .into(),
                ));
            }

            if !matches!(chunk_cache, Some((cached_idx, _)) if *cached_idx == chunk_idx) {
                let chunk = self.loaded_chunk(chunk_idx)?;
                *chunk_cache = Some((chunk_idx, self.decode_value_chunk(chunk)?));
            }
            let values = &chunk_cache
                .as_ref()
                .ok_or_else(|| Error::internal("Sparse structural chunk cache is empty"))?
                .1;
            data_builder.append(
                values,
                value_start - chunk_value_start..take_end - chunk_value_start,
            )?;
            value_start = take_end;
        }
        Ok(())
    }

    fn decode_checked(self) -> Result<DecodedPage> {
        let selection = slice_sparse_plan(
            &self.page_meta.plan,
            &self.row_ranges,
            self.page_meta.row_domain,
        )?;
        let estimated_size_bytes = self
            .loaded_chunks
            .iter()
            .map(|chunk| chunk.data.len())
            .try_fold(0_usize, |total, len| total.checked_add(len))
            .and_then(|total| total.checked_mul(2))
            .ok_or_else(|| {
                Error::invalid_input_source(
                    "Sparse structural decode size estimate overflows".into(),
                )
            })?;
        let mut data_builder = DataBlockBuilder::with_capacity_estimate(
            u64::try_from(estimated_size_bytes).map_err(|_| {
                Error::invalid_input_source("Sparse structural decode size exceeds u64::MAX".into())
            })?,
        );
        let mut chunk_cache: Option<(usize, DataBlock)> = None;
        let mut appended_values = false;
        for value_range in &selection.leaf_ranges {
            self.append_value_range(value_range.clone(), &mut data_builder, &mut chunk_cache)?;
            appended_values = true;
        }

        let data = if appended_values {
            data_builder.finish()
        } else {
            DataBlock::from_array(new_empty_array(&self.data_type))
        };
        let unraveler = RepDefUnraveler::new_sparse(selection.plan);
        Ok(DecodedPage {
            data,
            repdef: unraveler,
        })
    }
}

impl DecodePageTask for DecodeSparseStructuralTask {
    fn decode(self: Box<Self>) -> Result<DecodedPage> {
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (*self).decode_checked()))
            .map_err(|_| {
                Error::invalid_input_source(
                    "Sparse structural page decoding panicked on malformed input".into(),
                )
            })?
    }
}

struct SparseStructuralSelection {
    plan: SparseStructuralPlan,
    leaf_ranges: Vec<Range<u64>>,
}

struct SparsePositionSelection {
    positions: SparsePositionSet,
    ordinal_ranges: Vec<Range<u64>>,
}

fn validate_slice_ranges(ranges: &[Range<u64>], domain_len: u64, label: &str) -> Result<u64> {
    let mut total = 0_u64;
    for range in ranges {
        if range.start > range.end || range.end > domain_len {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} slice {}..{} is outside domain {}",
                    range.start, range.end, domain_len
                )
                .into(),
            ));
        }
        total = total.checked_add(range.end - range.start).ok_or_else(|| {
            Error::invalid_input_source(
                format!("Sparse structural {label} slice length overflows").into(),
            )
        })?;
    }
    Ok(total)
}

fn push_coalesced_range(ranges: &mut Vec<Range<u64>>, range: Range<u64>) {
    if range.is_empty() {
        return;
    }
    if let Some(last) = ranges.last_mut()
        && last.end == range.start
    {
        last.end = range.end;
        return;
    }
    ranges.push(range);
}

fn position_segments_to_set(
    segments: Vec<Range<u64>>,
    output_domain: u64,
    label: &str,
) -> Result<SparsePositionSet> {
    let segments = coalesce_ranges(segments);
    if segments.is_empty() {
        return Ok(SparsePositionSet::empty());
    }
    if segments.len() == 1 {
        let segment = segments.first().ok_or_else(|| {
            Error::internal("Sparse structural segment unexpectedly missing".to_string())
        })?;
        if segment.start == 0 && segment.end == output_domain {
            return Ok(SparsePositionSet::all(output_domain));
        }
        return Ok(SparsePositionSet::range(
            segment.start,
            segment.end - segment.start,
        ));
    }

    let total_len = segments
        .iter()
        .map(|range| range.end - range.start)
        .try_fold(0_u64, |sum, len| {
            sum.checked_add(len).ok_or_else(|| {
                Error::invalid_input_source(
                    format!("Sparse structural {label} segment length overflows").into(),
                )
            })
        })?;
    let total_len = usize::try_from(total_len).map_err(|_| {
        Error::invalid_input_source(
            format!("Sparse structural {label} segment length exceeds usize::MAX").into(),
        )
    })?;
    let mut positions = Vec::with_capacity(total_len);
    for segment in segments {
        positions.extend(segment);
    }
    SparsePositionSet::from_positions(positions, output_domain, label)
}

fn select_position_set(
    positions: &SparsePositionSet,
    ranges: &[Range<u64>],
    domain_len: u64,
    label: &str,
) -> Result<SparsePositionSelection> {
    let output_domain = validate_slice_ranges(ranges, domain_len, label)?;
    let mut segments = Vec::new();
    let mut ordinal_ranges = Vec::new();
    let mut output_base = 0_u64;

    match positions {
        SparsePositionSet::Empty => {}
        SparsePositionSet::All { len } => {
            if *len != domain_len {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse structural {label} all set length {} does not match domain {}",
                        len, domain_len
                    )
                    .into(),
                ));
            }
            for range in ranges {
                let range_len = range.end - range.start;
                let output_end = output_base.checked_add(range_len).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!("Sparse structural {label} output domain overflows").into(),
                    )
                })?;
                push_coalesced_range(&mut segments, output_base..output_end);
                push_coalesced_range(&mut ordinal_ranges, range.clone());
                output_base = output_end;
            }
        }
        SparsePositionSet::Range { start, len } => {
            let source_end = start.checked_add(*len).ok_or_else(|| {
                Error::invalid_input_source(
                    format!("Sparse structural {label} range overflows").into(),
                )
            })?;
            if source_end > domain_len {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse structural {label} range {}..{} is outside domain {}",
                        start, source_end, domain_len
                    )
                    .into(),
                ));
            }
            for range in ranges {
                let intersect_start = range.start.max(*start);
                let intersect_end = range.end.min(source_end);
                if intersect_start < intersect_end {
                    let out_start = output_base
                        .checked_add(intersect_start - range.start)
                        .ok_or_else(|| {
                            Error::invalid_input_source(
                                format!("Sparse structural {label} output position overflows")
                                    .into(),
                            )
                        })?;
                    let out_end = output_base
                        .checked_add(intersect_end - range.start)
                        .ok_or_else(|| {
                            Error::invalid_input_source(
                                format!("Sparse structural {label} output position overflows")
                                    .into(),
                            )
                        })?;
                    push_coalesced_range(&mut segments, out_start..out_end);
                    push_coalesced_range(
                        &mut ordinal_ranges,
                        intersect_start - *start..intersect_end - *start,
                    );
                }
                output_base = output_base
                    .checked_add(range.end - range.start)
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            format!("Sparse structural {label} output domain overflows").into(),
                        )
                    })?;
            }
        }
        SparsePositionSet::Explicit(source_positions) => {
            let mut out_positions = Vec::new();
            for range in ranges {
                let idx_start =
                    source_positions.partition_point(|position| *position < range.start);
                let idx_end = source_positions.partition_point(|position| *position < range.end);
                if idx_start < idx_end {
                    let selected_positions =
                        source_positions.get(idx_start..idx_end).ok_or_else(|| {
                            Error::invalid_input_source(
                                format!(
                                    "Sparse structural {label} explicit position slice is invalid"
                                )
                                .into(),
                            )
                        })?;
                    for position in selected_positions {
                        out_positions.push(
                            output_base
                                .checked_add(*position - range.start)
                                .ok_or_else(|| {
                                    Error::invalid_input_source(
                                        format!(
                                            "Sparse structural {label} output position overflows"
                                        )
                                        .into(),
                                    )
                                })?,
                        );
                    }
                    push_coalesced_range(&mut ordinal_ranges, idx_start as u64..idx_end as u64);
                }
                output_base = output_base
                    .checked_add(range.end - range.start)
                    .ok_or_else(|| {
                        Error::invalid_input_source(
                            format!("Sparse structural {label} output domain overflows").into(),
                        )
                    })?;
            }
            let positions = SparsePositionSet::from_positions(out_positions, output_domain, label)?;
            return Ok(SparsePositionSelection {
                positions,
                ordinal_ranges,
            });
        }
    }

    Ok(SparsePositionSelection {
        positions: position_segments_to_set(segments, output_domain, label)?,
        ordinal_ranges,
    })
}

fn select_validity_set(
    validity: &SparseValiditySet,
    ranges: &[Range<u64>],
    domain_len: u64,
    label: &str,
) -> Result<SparseValiditySet> {
    Ok(SparseValiditySet {
        meaning: validity.meaning,
        positions: select_position_set(&validity.positions, ranges, domain_len, label)?.positions,
    })
}

fn offsets_from_counts(counts: &[u64]) -> Result<Vec<u64>> {
    let mut offsets = Vec::with_capacity(counts.len() + 1);
    let mut offset = 0_u64;
    offsets.push(offset);
    for count in counts {
        offset = offset.checked_add(*count).ok_or_else(|| {
            Error::invalid_input_source("Sparse structural list count offsets overflow".into())
        })?;
        offsets.push(offset);
    }
    Ok(offsets)
}

fn coalesce_ranges(ranges: Vec<Range<u64>>) -> Vec<Range<u64>> {
    let mut coalesced: Vec<Range<u64>> = Vec::with_capacity(ranges.len());
    for range in ranges {
        if range.is_empty() {
            continue;
        }
        if let Some(last) = coalesced.last_mut()
            && last.end == range.start
        {
            last.end = range.end;
            continue;
        }
        coalesced.push(range);
    }
    coalesced
}

fn slice_list_layer(
    num_slots: u64,
    num_child_slots: u64,
    non_empty_positions: &SparsePositionSet,
    counts: &SparseCountSet,
    validity: &SparseValiditySet,
    ranges: &[Range<u64>],
) -> Result<(SparseStructuralLayerPlan, Vec<Range<u64>>)> {
    if non_empty_positions.len() != counts.len() {
        return Err(Error::invalid_input_source(
            "Sparse structural list has mismatched non-empty positions and counts".into(),
        ));
    }
    let count_sum = counts.sum()?;
    if count_sum != num_child_slots {
        return Err(Error::invalid_input_source(
            format!(
                "Sparse structural list count sum {} does not match child slots {}",
                count_sum, num_child_slots
            )
            .into(),
        ));
    }

    let non_empty_selection =
        select_position_set(non_empty_positions, ranges, num_slots, "list non-empty")?;
    let out_counts = select_count_set(
        counts,
        &non_empty_selection.ordinal_ranges,
        non_empty_selection.positions.len(),
    )?;
    let child_ranges =
        child_ranges_from_counts(counts, num_child_slots, &non_empty_selection.ordinal_ranges)?;
    let out_validity = select_validity_set(validity, ranges, num_slots, "list validity")?;
    let out_num_slots = validate_slice_ranges(ranges, num_slots, "list")?;
    let out_num_child_slots = out_counts.sum()?;
    Ok((
        SparseStructuralLayerPlan::List {
            num_slots: out_num_slots,
            num_child_slots: out_num_child_slots,
            non_empty_positions: non_empty_selection.positions,
            counts: out_counts,
            validity: out_validity,
        },
        child_ranges,
    ))
}

fn select_count_set(
    counts: &SparseCountSet,
    ordinal_ranges: &[Range<u64>],
    selected_len: u64,
) -> Result<SparseCountSet> {
    match counts {
        SparseCountSet::Empty => {
            if selected_len != 0 {
                return Err(Error::invalid_input_source(
                    "Sparse structural selected non-empty positions but counts are empty".into(),
                ));
            }
            Ok(SparseCountSet::Empty)
        }
        SparseCountSet::Constant { value, len } => {
            validate_ordinal_ranges(ordinal_ranges, *len, "constant list counts")?;
            Ok(SparseCountSet::constant(*value, selected_len))
        }
        SparseCountSet::Explicit {
            counts: source_counts,
            ..
        } => {
            validate_ordinal_ranges(
                ordinal_ranges,
                source_counts.len() as u64,
                "explicit list counts",
            )?;
            let selected_len = usize::try_from(selected_len).map_err(|_| {
                Error::invalid_input_source(
                    "Sparse structural selected count length exceeds usize::MAX".into(),
                )
            })?;
            let mut out_counts = Vec::with_capacity(selected_len);
            for range in ordinal_ranges {
                let start = usize::try_from(range.start).map_err(|_| {
                    Error::invalid_input_source(
                        "Sparse structural count ordinal exceeds usize::MAX".into(),
                    )
                })?;
                let end = usize::try_from(range.end).map_err(|_| {
                    Error::invalid_input_source(
                        "Sparse structural count ordinal exceeds usize::MAX".into(),
                    )
                })?;
                out_counts.extend_from_slice(source_counts.get(start..end).ok_or_else(|| {
                    Error::invalid_input_source(
                        "Sparse structural explicit count slice is invalid".into(),
                    )
                })?);
            }
            SparseCountSet::from_counts(out_counts)
        }
    }
}

fn validate_ordinal_ranges(ranges: &[Range<u64>], len: u64, label: &str) -> Result<()> {
    for range in ranges {
        if range.start > range.end || range.end > len {
            return Err(Error::invalid_input_source(
                format!(
                    "Sparse structural {label} ordinal range {}..{} is outside {} values",
                    range.start, range.end, len
                )
                .into(),
            ));
        }
    }
    Ok(())
}

fn child_ranges_from_counts(
    counts: &SparseCountSet,
    num_child_slots: u64,
    ordinal_ranges: &[Range<u64>],
) -> Result<Vec<Range<u64>>> {
    if ordinal_ranges.is_empty() {
        return Ok(Vec::new());
    }
    let child_ranges = match counts {
        SparseCountSet::Empty => {
            return Err(Error::invalid_input_source(
                "Sparse structural selected non-empty positions but counts are empty".into(),
            ));
        }
        SparseCountSet::Constant { value, len } => {
            let expected_child_slots = value.checked_mul(*len).ok_or_else(|| {
                Error::invalid_input_source(
                    "Sparse structural list constant count sum overflows child slots".into(),
                )
            })?;
            if expected_child_slots != num_child_slots {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse structural list constant count sum {} does not match child slots {}",
                        expected_child_slots, num_child_slots
                    )
                    .into(),
                ));
            }
            validate_ordinal_ranges(ordinal_ranges, *len, "constant list counts")?;
            ordinal_ranges
                .iter()
                .map(|range| {
                    let start = range.start.checked_mul(*value).ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse structural list child range start overflows".into(),
                        )
                    })?;
                    let end = range.end.checked_mul(*value).ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse structural list child range end overflows".into(),
                        )
                    })?;
                    Ok(start..end)
                })
                .collect::<Result<Vec<_>>>()?
        }
        SparseCountSet::Explicit {
            counts,
            offsets: value_offsets,
        } => {
            let last_offset = *value_offsets.last().ok_or_else(|| {
                Error::invalid_input_source(
                    "Sparse structural list count offsets are unexpectedly empty".into(),
                )
            })?;
            if last_offset != num_child_slots {
                return Err(Error::invalid_input_source(
                    format!(
                        "Sparse structural list count sum {} does not match child slots {}",
                        last_offset, num_child_slots
                    )
                    .into(),
                ));
            }
            validate_ordinal_ranges(ordinal_ranges, counts.len() as u64, "explicit list counts")?;
            ordinal_ranges
                .iter()
                .map(|range| {
                    let start = usize::try_from(range.start).map_err(|_| {
                        Error::invalid_input_source(
                            "Sparse structural count ordinal exceeds usize::MAX".into(),
                        )
                    })?;
                    let end = usize::try_from(range.end).map_err(|_| {
                        Error::invalid_input_source(
                            "Sparse structural count ordinal exceeds usize::MAX".into(),
                        )
                    })?;
                    let start_offset = *value_offsets.get(start).ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse structural list start offset is missing".into(),
                        )
                    })?;
                    let end_offset = *value_offsets.get(end).ok_or_else(|| {
                        Error::invalid_input_source(
                            "Sparse structural list end offset is missing".into(),
                        )
                    })?;
                    Ok(start_offset..end_offset)
                })
                .collect::<Result<Vec<_>>>()?
        }
    };
    Ok(coalesce_ranges(child_ranges))
}

fn slice_sparse_plan(
    plan: &SparseStructuralPlan,
    row_ranges: &[Range<u64>],
    row_domain: u64,
) -> Result<SparseStructuralSelection> {
    plan.validate(row_domain)?;
    let mut selected_ranges = row_ranges.to_vec();
    let mut selected_domain = row_domain;
    let mut sliced_layers = Vec::with_capacity(plan.layers.len());

    for layer in &plan.layers {
        match layer {
            SparseStructuralLayerPlan::Validity {
                num_slots,
                validity,
            } => {
                if selected_domain != *num_slots {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural validity slice domain {} does not match {} slots",
                            selected_domain, num_slots
                        )
                        .into(),
                    ));
                }
                let out_num_slots =
                    validate_slice_ranges(&selected_ranges, *num_slots, "validity")?;
                sliced_layers.push(SparseStructuralLayerPlan::Validity {
                    num_slots: out_num_slots,
                    validity: select_validity_set(
                        validity,
                        &selected_ranges,
                        *num_slots,
                        "validity",
                    )?,
                });
            }
            SparseStructuralLayerPlan::List {
                num_slots,
                num_child_slots,
                non_empty_positions,
                counts,
                validity,
                ..
            } => {
                if selected_domain != *num_slots {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural list slice domain {} does not match {} slots",
                            selected_domain, num_slots
                        )
                        .into(),
                    ));
                }
                let (sliced_layer, child_ranges) = slice_list_layer(
                    *num_slots,
                    *num_child_slots,
                    non_empty_positions,
                    counts,
                    validity,
                    &selected_ranges,
                )?;
                sliced_layers.push(sliced_layer);
                selected_ranges = child_ranges;
                selected_domain = *num_child_slots;
            }
            SparseStructuralLayerPlan::FixedSizeList {
                num_slots,
                dimension,
                validity,
            } => {
                if selected_domain != *num_slots {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Sparse structural fixed-size-list slice domain {} does not match {} slots",
                            selected_domain, num_slots
                        )
                        .into(),
                    ));
                }
                let num_child_slots = num_slots.checked_mul(*dimension).ok_or_else(|| {
                    Error::invalid_input_source(
                        format!(
                            "Sparse structural fixed-size-list child slot count overflows: slots={}, dimension={}",
                            num_slots, dimension
                        )
                        .into(),
                    )
                })?;
                let out_num_slots =
                    validate_slice_ranges(&selected_ranges, *num_slots, "fixed-size-list")?;
                let child_ranges = selected_ranges
                    .iter()
                    .map(|range| {
                        let start = range.start.checked_mul(*dimension).ok_or_else(|| {
                            Error::invalid_input_source(
                                "Sparse structural fixed-size-list child range start overflows"
                                    .into(),
                            )
                        })?;
                        let end = range.end.checked_mul(*dimension).ok_or_else(|| {
                            Error::invalid_input_source(
                                "Sparse structural fixed-size-list child range end overflows"
                                    .into(),
                            )
                        })?;
                        Ok(start..end)
                    })
                    .collect::<Result<Vec<_>>>()?;
                sliced_layers.push(SparseStructuralLayerPlan::FixedSizeList {
                    num_slots: out_num_slots,
                    dimension: *dimension,
                    validity: select_validity_set(
                        validity,
                        &selected_ranges,
                        *num_slots,
                        "fixed-size-list validity",
                    )?,
                });
                selected_ranges = child_ranges;
                selected_domain = num_child_slots;
            }
        }
    }

    if selected_domain != plan.num_visible_items {
        return Err(Error::invalid_input_source(
            format!(
                "Sparse structural selected terminal domain {} does not match {} visible items",
                selected_domain, plan.num_visible_items
            )
            .into(),
        ));
    }
    let num_visible_items =
        validate_slice_ranges(&selected_ranges, plan.num_visible_items, "visible value")?;
    let num_items = SparseStructuralPlan::expected_num_items(&sliced_layers, num_visible_items)?;
    Ok(SparseStructuralSelection {
        plan: SparseStructuralPlan {
            layers: sliced_layers,
            num_items,
            num_visible_items,
        },
        leaf_ranges: coalesce_ranges(selected_ranges),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::{
        compression::DefaultDecompressionStrategy,
        encodings::physical::block::{CompressionConfig, CompressionScheme},
        testing::SimulatedScheduler,
    };

    use super::*;

    fn position_set(
        positions: pb21::sparse_position_set::Positions,
        num_positions: u64,
    ) -> Option<pb21::SparsePositionSet> {
        Some(pb21::SparsePositionSet {
            positions: Some(positions),
            num_positions,
        })
    }

    fn position_empty() -> Option<pb21::SparsePositionSet> {
        position_set(
            pb21::sparse_position_set::Positions::Empty(pb21::SparsePositionEmpty {}),
            0,
        )
    }

    fn position_all(num_positions: u64) -> Option<pb21::SparsePositionSet> {
        position_set(
            pb21::sparse_position_set::Positions::All(pb21::SparsePositionAll {}),
            num_positions,
        )
    }

    fn position_explicit(num_positions: u64) -> Option<pb21::SparsePositionSet> {
        position_set(
            pb21::sparse_position_set::Positions::Explicit(ProtobufUtils21::flat(64, None)),
            num_positions,
        )
    }

    fn general_lz4(values: CompressiveEncoding) -> CompressiveEncoding {
        ProtobufUtils21::wrapped(CompressionConfig::new(CompressionScheme::Lz4, None), values)
            .unwrap()
    }

    fn validity(
        meaning: pb21::sparse_validity_set::Meaning,
        positions: Option<pb21::SparsePositionSet>,
    ) -> Option<pb21::SparseValiditySet> {
        Some(pb21::SparseValiditySet {
            meaning: meaning as i32,
            positions,
        })
    }

    fn null_positions(
        positions: Option<pb21::SparsePositionSet>,
    ) -> Option<pb21::SparseValiditySet> {
        validity(
            pb21::sparse_validity_set::Meaning::SparseValidityNullPositions,
            positions,
        )
    }

    fn count_empty() -> Option<pb21::SparseCountSet> {
        Some(pb21::SparseCountSet {
            counts: Some(pb21::sparse_count_set::Counts::Empty(
                pb21::SparseCountEmpty {},
            )),
        })
    }

    fn count_constant(value: u64) -> Option<pb21::SparseCountSet> {
        Some(pb21::SparseCountSet {
            counts: Some(pb21::sparse_count_set::Counts::Constant(
                pb21::SparseCountConstant { value },
            )),
        })
    }

    fn sparse_layout() -> pb21::SparseLayout {
        pb21::SparseLayout {
            value_compression: Some(ProtobufUtils21::flat(32, None)),
            num_buffers: 1,
            num_items: 1,
            num_visible_items: 1,
            has_large_chunk: false,
            structural_layers: Vec::new(),
        }
    }

    fn validity_layer(
        num_slots: u64,
        validity: Option<pb21::SparseValiditySet>,
    ) -> pb21::SparseStructuralLayer {
        pb21::SparseStructuralLayer {
            layer: Some(pb21::sparse_structural_layer::Layer::Validity(
                pb21::SparseValidityLayer {
                    num_slots,
                    validity,
                },
            )),
        }
    }

    fn list_layer(
        num_slots: u64,
        num_child_slots: u64,
        non_empty_positions: Option<pb21::SparsePositionSet>,
        counts: Option<pb21::SparseCountSet>,
        validity: Option<pb21::SparseValiditySet>,
    ) -> pb21::SparseStructuralLayer {
        pb21::SparseStructuralLayer {
            layer: Some(pb21::sparse_structural_layer::Layer::List(
                pb21::SparseListLayer {
                    num_slots,
                    num_child_slots,
                    non_empty_positions,
                    counts,
                    validity,
                },
            )),
        }
    }

    fn fixed_size_list_layer(
        num_slots: u64,
        dimension: u64,
        validity: Option<pb21::SparseValiditySet>,
    ) -> pb21::SparseStructuralLayer {
        pb21::SparseStructuralLayer {
            layer: Some(pb21::sparse_structural_layer::Layer::FixedSizeList(
                pb21::SparseFixedSizeListLayer {
                    num_slots,
                    dimension,
                    validity,
                },
            )),
        }
    }

    fn assert_invalid_input_contains(err: Error, expected: &str) {
        let message = err.to_string();
        assert!(
            matches!(&err, Error::InvalidInput { .. }),
            "expected InvalidInput, got {err:?}"
        );
        assert!(
            message.contains(expected),
            "expected error to contain {expected:?}, got {message}"
        );
    }

    #[test]
    fn rejects_missing_layer_variants_and_item_count_mismatches() {
        let decompressors = DefaultDecompressionStrategy::default();

        let mut layout = sparse_layout();
        layout
            .structural_layers
            .push(pb21::SparseStructuralLayer { layer: None });
        let err = SparseStructuralScheduler::try_new(
            &[(0, 0), (0, 0)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap_err();
        assert_invalid_input_contains(err, "missing its layer variant");

        let mut layout = sparse_layout();
        layout.num_items = 2;
        layout
            .structural_layers
            .push(validity_layer(1, null_positions(position_empty())));
        let err = SparseStructuralScheduler::try_new(
            &[(0, 0), (0, 0)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap_err();
        assert_invalid_input_contains(err, "layers imply 1");
    }

    #[test]
    fn accepts_large_structural_descriptors() {
        let explicit_values = 8_u64 * 1024 * 1024 + 1;
        let metadata_size = explicit_values * 8;
        let value_position = metadata_size;
        let value_size = explicit_values * 16;
        let structural_position = value_position + value_size;
        let structural_size = explicit_values * std::mem::size_of::<u64>() as u64;
        let mut layout = sparse_layout();
        layout.num_items = explicit_values;
        layout.num_visible_items = explicit_values;
        layout.structural_layers.push(validity_layer(
            explicit_values,
            null_positions(position_explicit(explicit_values)),
        ));

        SparseStructuralScheduler::try_new(
            &[
                (0, metadata_size),
                (value_position, value_size),
                (structural_position, structural_size),
            ],
            0,
            explicit_values,
            DataType::Int32,
            &layout,
            &DefaultDecompressionStrategy::default(),
        )
        .unwrap();
    }

    #[test]
    fn accepts_deep_supported_value_encodings() {
        let encoding = (0..300).fold(ProtobufUtils21::flat(32, None), |values, _| {
            ProtobufUtils21::fsl(1, false, values)
        });

        assert_eq!(
            SparseStructuralScheduler::validate_value_encoding(&encoding).unwrap(),
            1
        );
    }

    fn null_set(positions: SparsePositionSet) -> SparseValiditySet {
        SparseValiditySet {
            meaning: SparseValidityMeaning::NullPositions,
            positions,
        }
    }

    fn valid_set(positions: SparsePositionSet) -> SparseValiditySet {
        SparseValiditySet {
            meaning: SparseValidityMeaning::ValidPositions,
            positions,
        }
    }

    #[test]
    fn semantic_position_and_count_sets_project_without_materializing_ranges() {
        let ranges = [1..4];
        assert_eq!(
            select_position_set(&SparsePositionSet::Empty, &ranges, 5, "empty")
                .unwrap()
                .positions,
            SparsePositionSet::Empty
        );
        assert_eq!(
            select_position_set(&SparsePositionSet::all(5), &ranges, 5, "all")
                .unwrap()
                .positions,
            SparsePositionSet::all(3)
        );
        assert_eq!(
            select_position_set(&SparsePositionSet::range(1, 3), &ranges, 5, "range")
                .unwrap()
                .positions,
            SparsePositionSet::all(3)
        );
        assert_eq!(
            select_position_set(
                &SparsePositionSet::Explicit(vec![0, 2, 4]),
                &[0..1, 4..5],
                5,
                "explicit",
            )
            .unwrap()
            .positions,
            SparsePositionSet::all(2)
        );

        assert_eq!(
            select_count_set(&SparseCountSet::Empty, &[], 0).unwrap(),
            SparseCountSet::Empty
        );
        assert_eq!(
            select_count_set(&SparseCountSet::constant(2, 3), &[0..1, 2..3], 2,).unwrap(),
            SparseCountSet::constant(2, 2)
        );
        assert_eq!(
            select_count_set(
                &SparseCountSet::from_counts(vec![1, 2, 3]).unwrap(),
                &[0..1, 2..3],
                2,
            )
            .unwrap(),
            SparseCountSet::from_counts(vec![1, 3]).unwrap()
        );
    }

    #[test]
    fn validity_polarities_rebuild_the_same_arrow_domain() {
        let mut null_builder = BooleanBufferBuilder::new(4);
        null_set(SparsePositionSet::Explicit(vec![1, 3]))
            .append_to(&mut null_builder, 4)
            .unwrap();
        assert_eq!(
            null_builder.finish().iter().collect::<Vec<_>>(),
            vec![true, false, true, false]
        );

        let mut valid_builder = BooleanBufferBuilder::new(4);
        valid_set(SparsePositionSet::range(1, 2))
            .append_to(&mut valid_builder, 4)
            .unwrap();
        assert_eq!(
            valid_builder.finish().iter().collect::<Vec<_>>(),
            vec![false, true, true, false]
        );
    }

    #[test]
    fn schema_layer_mismatches_are_invalid_input() {
        let mut missing = SparseStructuralUnraveler::new(SparseStructuralPlan {
            layers: Vec::new(),
            num_items: 1,
            num_visible_items: 1,
        });
        let mut validity = BooleanBufferBuilder::new(1);
        let err = missing.unravel_validity(&mut validity).unwrap_err();
        assert_invalid_input_contains(err, "fewer layers than the Arrow schema");

        let mut fixed_size_list = SparseStructuralUnraveler::new(SparseStructuralPlan {
            layers: vec![SparseStructuralLayerPlan::FixedSizeList {
                num_slots: 2,
                dimension: 2,
                validity: null_set(SparsePositionSet::empty()),
            }],
            num_items: 4,
            num_visible_items: 4,
        });
        let mut validity = BooleanBufferBuilder::new(2);
        let err = fixed_size_list.unravel_validity(&mut validity).unwrap_err();
        assert_invalid_input_contains(err, "does not match the Arrow schema");

        let extra = SparseStructuralUnraveler::new(SparseStructuralPlan {
            layers: vec![SparseStructuralLayerPlan::Validity {
                num_slots: 1,
                validity: null_set(SparsePositionSet::empty()),
            }],
            num_items: 1,
            num_visible_items: 1,
        });
        let err = extra.ensure_exhausted().unwrap_err();
        assert_invalid_input_contains(err, "1 unconsumed layer");
    }

    fn nested_plan() -> SparseStructuralPlan {
        SparseStructuralPlan {
            layers: vec![
                SparseStructuralLayerPlan::Validity {
                    num_slots: 6,
                    validity: null_set(SparsePositionSet::Explicit(vec![1, 4])),
                },
                SparseStructuralLayerPlan::List {
                    num_slots: 6,
                    num_child_slots: 5,
                    non_empty_positions: SparsePositionSet::Explicit(vec![0, 2, 5]),
                    counts: SparseCountSet::from_counts(vec![2, 1, 2]).unwrap(),
                    validity: null_set(SparsePositionSet::Explicit(vec![1, 4])),
                },
                SparseStructuralLayerPlan::FixedSizeList {
                    num_slots: 5,
                    dimension: 2,
                    validity: valid_set(SparsePositionSet::range(1, 3)),
                },
            ],
            num_items: 13,
            num_visible_items: 10,
        }
    }

    #[test]
    fn discontiguous_projection_preserves_outer_to_inner_order() {
        let selection = slice_sparse_plan(&nested_plan(), &[5..6, 0..1], 6).unwrap();
        assert_eq!(selection.leaf_ranges, vec![6..10, 0..4]);
        assert_eq!(selection.plan.num_visible_items, 8);
        selection.plan.validate(2).unwrap();

        let SparseStructuralLayerPlan::List {
            non_empty_positions,
            counts,
            ..
        } = &selection.plan.layers[1]
        else {
            panic!("expected projected list layer");
        };
        assert_eq!(*non_empty_positions, SparsePositionSet::all(2));
        assert_eq!(*counts, SparseCountSet::constant(2, 2));
    }

    #[test]
    fn no_value_projection_keeps_empty_and_null_list_structure() {
        let selection = slice_sparse_plan(&nested_plan(), &[1..2, 3..4], 6).unwrap();
        assert!(selection.leaf_ranges.is_empty());
        assert_eq!(selection.plan.num_visible_items, 0);
        selection.plan.validate(2).unwrap();

        let SparseStructuralLayerPlan::List {
            num_slots,
            num_child_slots,
            non_empty_positions,
            counts,
            validity,
        } = &selection.plan.layers[1]
        else {
            panic!("expected projected list layer");
        };
        assert_eq!((*num_slots, *num_child_slots), (2, 0));
        assert_eq!(*non_empty_positions, SparsePositionSet::Empty);
        assert_eq!(*counts, SparseCountSet::Empty);
        assert_eq!(*validity, null_set(SparsePositionSet::range(0, 1)));
    }

    #[test]
    fn rejects_missing_value_compression_and_buffer_count_mismatch() {
        let decompressors = DefaultDecompressionStrategy::default();
        let mut layout = sparse_layout();
        layout.value_compression = None;
        let err = SparseStructuralScheduler::try_new(
            &[(0, 0), (0, 0)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap_err();
        assert_invalid_input_contains(err, "missing value compression");

        let mut layout = sparse_layout();
        layout.num_buffers = 2;
        let err = SparseStructuralScheduler::try_new(
            &[(0, 0), (0, 0)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap_err();
        assert_invalid_input_contains(err, "declares 2 value buffers");
    }

    #[test]
    fn rejects_inconsistent_chunk_count_before_io() {
        let decompressors = DefaultDecompressionStrategy::default();

        let layout = sparse_layout();
        let err = SparseStructuralScheduler::try_new(
            &[(0, 16), (16, 8)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap_err();
        assert_invalid_input_contains(err, "declares 2 chunks for 1 visible items");
    }

    #[cfg(feature = "lz4")]
    #[test]
    fn accepts_large_general_decompression_headers() {
        let encoding = general_lz4(ProtobufUtils21::flat(64, None));
        let Some(pb21::compressive_encoding::Compression::General(general)) =
            encoding.compression.as_ref()
        else {
            panic!("expected General compression");
        };
        let declared_size = 65_u32 * 1024 * 1024;

        SparseStructuralScheduler::validate_general_buffer_header(
            general,
            &declared_size.to_le_bytes(),
            "test",
        )
        .unwrap();
    }

    #[cfg(feature = "lz4")]
    #[tokio::test]
    async fn rejects_malformed_general_structural_buffers_as_invalid_input() {
        let mut layout = sparse_layout();
        layout.structural_layers.push(validity_layer(
            1,
            null_positions(position_set(
                pb21::sparse_position_set::Positions::Explicit(general_lz4(ProtobufUtils21::flat(
                    64, None,
                ))),
                1,
            )),
        ));
        let decompressors = DefaultDecompressionStrategy::default();

        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 8), (8, 8), (16, 8)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&1_u32.to_le_bytes());
        data.extend_from_slice(&[0; 8]);
        data.extend_from_slice(&8_u32.to_le_bytes());
        data.extend_from_slice(&[0xff; 4]);
        let io: Arc<dyn EncodingsIo> = Arc::new(SimulatedScheduler::new(Bytes::from(data)));
        let Err(err) = scheduler.initialize(&io).await else {
            panic!("expected malformed General buffer to be rejected");
        };
        assert_invalid_input_contains(err, "decompression failed");
    }

    #[cfg(feature = "lz4")]
    #[tokio::test]
    async fn rejects_malformed_general_value_buffer() {
        let mut layout = sparse_layout();
        layout.value_compression = Some(general_lz4(ProtobufUtils21::flat(32, None)));
        let decompressors = DefaultDecompressionStrategy::default();
        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 8), (8, 8)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&1_u32.to_le_bytes());
        data.extend_from_slice(&0_u16.to_le_bytes());
        data.extend_from_slice(&0_u16.to_le_bytes());
        data.extend_from_slice(&[0; 4]);
        let io: Arc<dyn EncodingsIo> = Arc::new(SimulatedScheduler::new(Bytes::from(data)));
        scheduler.initialize(&io).await.unwrap();
        let mut page_tasks = scheduler.schedule_ranges(&[0..1], &io).unwrap();
        let mut decoder = page_tasks.pop().unwrap().decoder_fut.await.unwrap();
        let Err(err) = decoder.drain(1).unwrap().decode() else {
            panic!("expected malformed General value buffer to be rejected");
        };
        assert_invalid_input_contains(err, "missing its length prefix");
    }

    #[test]
    fn rejects_layer_domain_and_fixed_size_list_mismatches() {
        let decompressors = DefaultDecompressionStrategy::default();
        let mut layout = sparse_layout();
        layout.num_items = 2;
        layout.num_visible_items = 2;
        layout
            .structural_layers
            .push(validity_layer(1, null_positions(position_empty())));
        layout
            .structural_layers
            .push(validity_layer(2, null_positions(position_empty())));
        let err = SparseStructuralScheduler::try_new(
            &[(0, 0), (0, 0)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap_err();
        assert_invalid_input_contains(err, "layer 1 has 2 slots, expected 1");

        let mut layout = sparse_layout();
        layout.num_items = 4;
        layout.num_visible_items = 4;
        layout.structural_layers.push(fixed_size_list_layer(
            2,
            3,
            null_positions(position_empty()),
        ));
        let err = SparseStructuralScheduler::try_new(
            &[(0, 0), (0, 0)],
            0,
            2,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap_err();
        assert_invalid_input_contains(err, "terminal domain has 6 slots");
    }

    #[test]
    fn rejects_invalid_validity_and_list_count_semantics() {
        let decompressors = DefaultDecompressionStrategy::default();
        let mut layout = sparse_layout();
        layout.structural_layers.push(validity_layer(
            1,
            validity(
                pb21::sparse_validity_set::Meaning::SparseValidityUnspecified,
                position_empty(),
            ),
        ));
        let err = SparseStructuralScheduler::try_new(
            &[(0, 0), (0, 0)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap_err();
        assert_invalid_input_contains(err, "meaning is unspecified");

        let mut layout = sparse_layout();
        layout.num_items = 3;
        layout.num_visible_items = 3;
        layout.structural_layers.push(list_layer(
            2,
            3,
            position_all(2),
            count_constant(2),
            null_positions(position_empty()),
        ));
        let err = SparseStructuralScheduler::try_new(
            &[(0, 0), (0, 0)],
            0,
            2,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap_err();
        assert_invalid_input_contains(err, "count sum 4 does not match child slots 3");
    }

    #[tokio::test]
    async fn rejects_unordered_explicit_positions_after_decompression() {
        let mut layout = sparse_layout();
        layout.num_items = 4;
        layout.num_visible_items = 4;
        layout
            .structural_layers
            .push(validity_layer(4, null_positions(position_explicit(2))));
        let decompressors = DefaultDecompressionStrategy::default();
        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 8), (8, 8), (16, 16)],
            0,
            4,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap();

        let mut data = Vec::new();
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&4_u32.to_le_bytes());
        data.extend_from_slice(&[0; 8]);
        data.extend_from_slice(&3_u64.to_le_bytes());
        data.extend_from_slice(&0_u64.to_le_bytes());
        let io: Arc<dyn EncodingsIo> = Arc::new(SimulatedScheduler::new(Bytes::from(data)));

        let Err(err) = scheduler.initialize(&io).await else {
            panic!("expected unordered sparse positions to be rejected");
        };
        assert_invalid_input_contains(err, "positions must be strictly increasing");
    }

    #[tokio::test]
    async fn rejects_chunk_metadata_value_and_byte_sum_mismatches() {
        let layout = sparse_layout();
        let decompressors = DefaultDecompressionStrategy::default();
        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 8), (8, 8)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&1_u32.to_le_bytes());
        data.extend_from_slice(&1_u32.to_le_bytes());
        data.extend_from_slice(&[0; 8]);
        let io: Arc<dyn EncodingsIo> = Arc::new(SimulatedScheduler::new(Bytes::from(data)));
        let Err(err) = scheduler.initialize(&io).await else {
            panic!("expected chunk byte sum mismatch");
        };
        assert_invalid_input_contains(err, "describes 16 value bytes");

        let mut layout = sparse_layout();
        layout.num_items = 2;
        layout.num_visible_items = 2;
        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 8), (8, 8)],
            0,
            2,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&1_u32.to_le_bytes());
        data.extend_from_slice(&[0; 8]);
        let io: Arc<dyn EncodingsIo> = Arc::new(SimulatedScheduler::new(Bytes::from(data)));
        let Err(err) = scheduler.initialize(&io).await else {
            panic!("expected chunk value sum mismatch");
        };
        assert_invalid_input_contains(err, "metadata has 1, layout has 2");
    }

    #[tokio::test]
    async fn rejects_malformed_value_chunk_before_decompression() {
        let layout = sparse_layout();
        let decompressors = DefaultDecompressionStrategy::default();
        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 8), (8, 8)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&1_u32.to_le_bytes());
        data.extend_from_slice(&[0; 8]);
        let io: Arc<dyn EncodingsIo> = Arc::new(SimulatedScheduler::new(Bytes::from(data)));
        scheduler.initialize(&io).await.unwrap();
        let mut page_tasks = scheduler.schedule_ranges(&[0..1], &io).unwrap();
        let page_task = page_tasks.pop().unwrap();
        let mut decoder = page_task.decoder_fut.await.unwrap();
        let decode_task = decoder.drain(1).unwrap();
        let Err(err) = decode_task.decode() else {
            panic!("expected malformed value chunk to be rejected");
        };
        assert_invalid_input_contains(err, "flat values buffer has 0 bytes, expected 4");
    }

    #[tokio::test]
    async fn accepts_value_chunks_larger_than_64_mib() {
        let chunk_size = 65_u64 * 1024 * 1024;
        let layout = sparse_layout();
        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 8), (8, chunk_size)],
            0,
            1,
            DataType::Int32,
            &layout,
            &DefaultDecompressionStrategy::default(),
        )
        .unwrap();
        let words_minus_one = u32::try_from(chunk_size / MINIBLOCK_ALIGNMENT as u64 - 1).unwrap();
        let mut metadata = Vec::new();
        metadata.extend_from_slice(&words_minus_one.to_le_bytes());
        metadata.extend_from_slice(&1_u32.to_le_bytes());
        let io: Arc<dyn EncodingsIo> = Arc::new(SimulatedScheduler::new(Bytes::from(metadata)));

        scheduler.initialize(&io).await.unwrap();
    }

    #[tokio::test]
    async fn rejects_value_chunks_above_the_miniblock_limit() {
        let num_values = miniblock::MAX_CONFIGURABLE_MINIBLOCK_VALUES + 1;
        let mut layout = sparse_layout();
        layout.num_items = num_values;
        layout.num_visible_items = num_values;
        let decompressors = DefaultDecompressionStrategy::default();
        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 16), (16, 16)],
            0,
            num_values,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap();
        let mut data = Vec::new();
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&(num_values as u32).to_le_bytes());
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&0_u32.to_le_bytes());
        data.extend_from_slice(&[0; 16]);
        let io: Arc<dyn EncodingsIo> = Arc::new(SimulatedScheduler::new(Bytes::from(data)));

        let Err(err) = scheduler.initialize(&io).await else {
            panic!("expected oversized value chunk to be rejected");
        };
        assert_invalid_input_contains(err, "exceeding the mini-block limit");
    }

    #[derive(Debug, Clone)]
    struct RecordingIo {
        data: Bytes,
        calls: Arc<Mutex<Vec<Vec<Range<u64>>>>>,
    }

    impl RecordingIo {
        fn new(data: Bytes) -> Self {
            Self {
                data,
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl EncodingsIo for RecordingIo {
        fn submit_request(
            &self,
            ranges: Vec<Range<u64>>,
            _priority: u64,
        ) -> BoxFuture<'static, Result<Vec<Bytes>>> {
            self.calls.lock().unwrap().push(ranges.clone());
            let data = self.data.clone();
            async move {
                ranges
                    .into_iter()
                    .map(|range| {
                        let start = usize_from_u64(range.start, "test range start")?;
                        let end = usize_from_u64(range.end, "test range end")?;
                        if start > end || end > data.len() {
                            return Err(Error::invalid_input_source(
                                "Test I/O range is outside fixture data".into(),
                            ));
                        }
                        Ok(data.slice(start..end))
                    })
                    .collect()
            }
            .boxed()
        }
    }

    #[tokio::test]
    async fn selective_read_requests_only_intersecting_value_chunk() {
        let mut layout = sparse_layout();
        layout.num_items = 4;
        layout.num_visible_items = 4;
        let decompressors = DefaultDecompressionStrategy::default();
        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 16), (16, 32)],
            0,
            4,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap();

        let mut data = Vec::new();
        for _ in 0..2 {
            data.extend_from_slice(&1_u32.to_le_bytes());
            data.extend_from_slice(&2_u32.to_le_bytes());
        }
        for values in [[10_i32, 20], [30, 40]] {
            data.extend_from_slice(&0_u16.to_le_bytes());
            data.extend_from_slice(&8_u16.to_le_bytes());
            data.extend_from_slice(&[0; 4]);
            for value in values {
                data.extend_from_slice(&value.to_le_bytes());
            }
        }

        let io = Arc::new(RecordingIo::new(Bytes::from(data)));
        let trait_io: Arc<dyn EncodingsIo> = io.clone();
        scheduler.initialize(&trait_io).await.unwrap();
        let mut page_tasks = scheduler.schedule_ranges(&[2..3], &trait_io).unwrap();
        let page_task = page_tasks.pop().unwrap();
        let mut decoder = page_task.decoder_fut.await.unwrap();
        let decoded = decoder.drain(1).unwrap().decode().unwrap();
        assert_eq!(decoded.data.num_values(), 1);

        let calls = io.calls.lock().unwrap();
        assert_eq!(calls.as_slice(), &[vec![0..16], vec![32..48]]);
    }

    #[tokio::test]
    async fn empty_leaf_selection_rebuilds_offsets_without_value_io() {
        let mut layout = sparse_layout();
        layout.num_visible_items = 0;
        layout.structural_layers.push(list_layer(
            1,
            0,
            position_empty(),
            count_empty(),
            null_positions(position_empty()),
        ));
        let decompressors = DefaultDecompressionStrategy::default();
        let mut scheduler = SparseStructuralScheduler::try_new(
            &[(0, 0), (0, 0)],
            0,
            1,
            DataType::Int32,
            &layout,
            &decompressors,
        )
        .unwrap();
        let io = Arc::new(RecordingIo::new(Bytes::new()));
        let trait_io: Arc<dyn EncodingsIo> = io.clone();
        scheduler.initialize(&trait_io).await.unwrap();
        let mut page_tasks = scheduler.schedule_ranges(&[0..1], &trait_io).unwrap();
        let page_task = page_tasks.pop().unwrap();
        let mut decoder = page_task.decoder_fut.await.unwrap();
        let decoded = decoder.drain(1).unwrap().decode().unwrap();
        assert_eq!(decoded.data.num_values(), 0);

        let mut repdef = CompositeRepDefUnraveler::new(vec![decoded.repdef]);
        let (offsets, validity) = repdef.unravel_offsets::<i32>().unwrap();
        assert_eq!(offsets.as_ref(), &[0, 0]);
        assert!(validity.is_none());

        let calls = io.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert!(calls[1].is_empty(), "value payload must not be requested");
    }
}
