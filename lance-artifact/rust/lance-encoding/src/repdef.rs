// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for rep-def levels
//!
//! Repetition and definition levels are a way to encode multipile validity / offsets arrays
//! into a single buffer.  They are a form of "zipping" buffers together that takes advantage
//! of the fact that, if the outermost array is invalid, then the validity of the inner items
//! is irrelevant.
//!
//! Note: the concept of repetition & definition levels comes from the Dremel paper and has
//! been implemented in Apache Parquet.  However, the implementation here is not necessarily
//! compatible with Parquet.  For example, we use 0 to represent the "inner-most" item and
//! Parquet uses 0 to represent the "outer-most" item.
//!
//! # Repetition Levels
//!
//! With repetition levels we convert a sparse array of offsets into a dense array of levels.
//! These levels are marked non-zero whenever a new list begins.  In other words, given the
//! list array with 3 rows [{<0,1>, <>, <2>}, {<3>}, {}], [], [{<4>}] we would have three
//! offsets arrays:
//!
//! Outer-most ([]): [0, 3, 3, 4]
//! Middle     ({}): [0, 3, 4, 4, 5]
//! Inner      (<>): [0, 2, 2, 3, 4, 5]
//! Values         : [0, 1, 2, 3, 4]
//!
//! We can convert these into repetition levels as follows:
//!
//! | Values | Repetition |
//! | ------ | ---------- |
//! |      0 |          3 | // Start of outer-most list
//! |      1 |          0 | // Continues inner-most list (no new lists)
//! |      - |          1 | // Start of new inner-most list (empty list)
//! |      2 |          1 | // Start of new inner-most list
//! |      3 |          2 | // Start of new middle list
//! |      - |          2 | // Start of new inner-most list (empty list)
//! |      - |          3 | // Start of new outer-most list (empty list)
//! |      4 |          0 | // Start of new outer-most list
//!
//! Note: We actually have MORE repetition levels than values.  This is because the repetition
//! levels need to be able to represent empty lists.
//!
//! # Definition Levels
//!
//! Definition levels are simpler.  We can think of them as zipping together various validity bitmaps
//! (from different levels of nesting) into a single buffer.  For example, we could zip the arrays
//! [1, 1, 0, 0] and [1, 0, 1, 0] into [11, 10, 01, 00].  However, 00 and 01 are redundant.  If the
//! outer level is null then the validity of the inner levels is irrelevant.  To save space we instead
//! encode a "level" which is the "depth" of the null.  Let's look at a more complete example:
//!
//! Array: [{"middle": {"inner": 1]}}, NULL, {"middle": NULL}, {"middle": {"inner": NULL}}]
//!
//! In Arrow we would have the following validity arrays:
//! Outer validity : 1, 0, 1, 1
//! Middle validity: 1, ?, 0, 1
//! Inner validity : 1, ?, ?, 0
//! Values         : 1, ?, ?, ?
//!
//! The ? values are undefined in the Arrow format.  We can convert these into definition levels as follows:
//!
//! | Values | Definition |
//! | ------ | ---------- |
//! |      1 |          0 | // Valid at all levels
//! |      - |          3 | // Null at outer level
//! |      - |          2 | // Null at middle level
//! |      - |          1 | // Null at inner level
//!
//! # Compression
//!
//! Note that we only need 2 bits of definition levels to represent 3 levels of nesting.  Definition
//! levels are always more compact than the input validity arrays.  However, compressed levels are not
//! necessarily more compact than the compressed validity arrays.
//!
//! Repetition levels are more complex.  If there are very large lists then a sparse array of offsets
//! (which has one element per list) might be more compact than a dense array of repetition levels
//! (which has one element per list value, possibly even more if there are empty lists).
//!
//! However, both repetition levels and definition levels are typically very compressible with RLE.
//!
//! However, in Lance we don't always take advantage of that compression because we want to be able
//! to zip rep-def levels together with our values.  This gives us fewer IOPS when accessing row values.
//!
//! # Utilities in this Module
//!
//! - `RepDefBuilder` - Extracts validity and offset information from Arrow arrays.  We use this as we
//!   shred the incoming data into primitive leaf arrays.  We don't immediately convert into rep-def because
//!   we need to share and cheaply clone the builder when we have structs (each struct child shares some parent
//!   validity / offset information)  The `serialize` method is called once all data has been received to create
//!   the final rep-def levels.
//!
//! - `SerializerContext` - This is an internal utility that helps with serializing rep-def levels.
//!
//! - `CompositeRepDefUnraveler` - This structure is used to reverse the process.  It starts with a set of
//!   rep-def levels and then uses the `unravel_validity` and `unravel_offsets` methods to produce validity
//!   buffers and offset buffers.  It is "composite" because we may be combining sets of rep-def buffers from
//!   multiple locations (e.g. multiple blocks in a mini-block encoded file).
//!
//! - `RepDefSlicer` - This is a utility that helps with slicing rep-def buffers.  These buffers are "kind of"
//!   transparent (maps 1:1 with the values in the array) but not exactly because of the special (empty/null) lists.
//!   The slicer helps with this issue and is used when slicing a rep-def buffer into mini-blocks.
//!
//! - `build_control_word_iterator` - This takes in rep-def levels and returns an iterator that returns byte-padded
//!   "control words" which are used when creating full-zip encoded data.
//!
//! - `ControlWordParser` - This parser can parse the control words returned by `build_control_word_iterator` and is
//!   used when decoding full-zip encoded data.

use std::{
    iter::{Copied, Zip},
    ops::Range,
    sync::Arc,
};

use arrow_array::OffsetSizeTrait;
use arrow_buffer::{
    ArrowNativeType, BooleanBuffer, BooleanBufferBuilder, NullBuffer, OffsetBuffer, ScalarBuffer,
};
use lance_core::{Error, Result, utils::bit::log_2_ceil};

use crate::{
    buffer::LanceBuffer,
    encodings::logical::primitive::sparse::{SparseStructuralPlan, SparseStructuralUnraveler},
};

pub type LevelBuffer = Vec<u16>;

/// A top-level-row range whose dense rep/def stream fits one mini-block page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MiniBlockRepDefSplit {
    /// Top-level row offset, relative to the original unsplit page.
    pub(crate) row_start: u64,
    /// Number of top-level rows in this split.
    pub(crate) num_rows: u64,
    /// Rep/def level range, relative to the original unsplit page.
    pub(crate) level_range: Range<usize>,
    /// Visible value offset, relative to the original unsplit page.
    pub(crate) value_start: u64,
    /// Number of visible values in this split.
    pub(crate) num_values: u64,
}

/// Dense mini-block rep/def budget result for one accumulated page.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MiniBlockRepDefBudget {
    /// The dense rep/def stream fits one mini-block structural page.
    WithinBudget,
    /// The dense rep/def stream fits after splitting on top-level row boundaries.
    RequiresPageSplit(Vec<MiniBlockRepDefSplit>),
    /// A single top-level row has this many rep/def levels and exceeds the budget.
    SingleRowOverBudget(u64),
}

// As we build def levels we add this to special values to indicate that they
// are special so that we can skip over them when processing lower levels.
//
// We assume 16 bits is good enough for rep-def levels.  This _would_ give
// us 65536 levels of struct nesting and list nesting.  However, we cut that
// in half for SPECIAL_THRESHOLD because we use the top bit to indicate if an
// item is a special value (null list / empty list) during construction.
//
// We subtract this off at the end of construction to get the actual definition
// levels.
const SPECIAL_THRESHOLD: u16 = u16::MAX / 2;

/// Represents information that we extract from a list array as we are
/// encoding
#[derive(Clone, Debug)]
struct OffsetDesc {
    offsets: Arc<[i64]>,
    validity: Option<BooleanBuffer>,
    has_empty_lists: bool,
    num_values: usize,
    num_specials: usize,
}

/// Represents validity information that we extract from non-list arrays (that
/// have nulls) as we are encoding
#[derive(Clone, Debug)]
struct ValidityDesc {
    validity: Option<BooleanBuffer>,
    num_values: usize,
}

/// Represents validity information that we extract from FSL arrays.  This is
/// just validity (no offsets) but we also record the dimension of the FSL array
/// as that will impact the next layer
#[derive(Clone, Debug)]
struct FslDesc {
    validity: Option<BooleanBuffer>,
    dimension: usize,
    num_values: usize,
}

// As we build up rep/def from arrow arrays we record a
// series of RawRepDef objects.  Each one corresponds to layer
// in the array structure
#[derive(Clone, Debug)]
enum RawRepDef {
    Offsets(OffsetDesc),
    Validity(ValidityDesc),
    Fsl(FslDesc),
}

/// A normalized Arrow structural layer shared by dense and sparse serializers.
#[derive(Clone, Copy, Debug)]
pub(crate) enum NormalizedStructuralLayer<'a> {
    List {
        offsets: &'a [i64],
        validity: Option<&'a BooleanBuffer>,
        num_slots: usize,
    },
    Validity {
        validity: Option<&'a BooleanBuffer>,
        num_slots: usize,
    },
    FixedSizeList {
        validity: Option<&'a BooleanBuffer>,
        dimension: usize,
        num_slots: usize,
    },
}

/// Structural layers concatenated across input batches exactly once.
///
/// Dense rep/def serialization and sparse metadata planning both consume this
/// representation so the Arrow nesting is not independently reconstructed.
#[derive(Debug)]
pub(crate) struct NormalizedStructuralPlan {
    layers: Vec<RawRepDef>,
    dense_all_valid: bool,
}

impl NormalizedStructuralPlan {
    pub(crate) fn layers(&self) -> impl ExactSizeIterator<Item = NormalizedStructuralLayer<'_>> {
        self.layers.iter().map(|layer| match layer {
            RawRepDef::Offsets(OffsetDesc {
                offsets,
                validity,
                num_values,
                ..
            }) => NormalizedStructuralLayer::List {
                offsets,
                validity: validity.as_ref(),
                num_slots: *num_values,
            },
            RawRepDef::Validity(ValidityDesc {
                validity,
                num_values,
            }) => NormalizedStructuralLayer::Validity {
                validity: validity.as_ref(),
                num_slots: *num_values,
            },
            RawRepDef::Fsl(FslDesc {
                validity,
                dimension,
                num_values,
            }) => NormalizedStructuralLayer::FixedSizeList {
                validity: validity.as_ref(),
                dimension: *dimension,
                num_slots: *num_values,
            },
        })
    }

    fn to_serializer(&self) -> (SerializerContext, Option<u64>) {
        if self.dense_all_valid {
            let def_meaning = self
                .layers
                .iter()
                .map(|_| DefinitionInterpretation::AllValidItem)
                .collect::<Vec<_>>();
            return (
                SerializerContext {
                    def_meaning,
                    rep_levels: LevelBuffer::default(),
                    spare_rep: LevelBuffer::default(),
                    def_levels: LevelBuffer::default(),
                    spare_def: LevelBuffer::default(),
                    current_rep: 0,
                    current_def: 0,
                    current_len: 0,
                    current_num_specials: 0,
                    has_fsl: false,
                },
                None,
            );
        }

        let total_len = self.layers.last().map_or(0, RawRepDef::num_values)
            + self
                .layers
                .iter()
                .map(RawRepDef::num_specials)
                .sum::<usize>();
        let max_rep = self.layers.iter().map(RawRepDef::max_rep).sum::<u16>();
        let max_def = self.layers.iter().map(RawRepDef::max_def).sum::<u16>();
        let bits_per_rep = if max_rep > 0 {
            u64::from(u16::BITS - max_rep.leading_zeros())
        } else {
            0
        };
        let bits_per_def = if max_def > 0 {
            u64::from(u16::BITS - max_def.leading_zeros())
        } else {
            0
        };
        let bits_per_level =
            (bits_per_rep + bits_per_def > 0).then_some(bits_per_rep + bits_per_def);

        let num_layers = self.layers.len();
        let mut context = SerializerContext::new(total_len, num_layers, max_rep, max_def);
        for layer in &self.layers {
            match layer {
                RawRepDef::Validity(def) => context.record_validity(def),
                RawRepDef::Offsets(rep) => context.record_offsets(rep),
                RawRepDef::Fsl(fsl) => context.record_fsl(fsl),
            }
        }
        (context, bits_per_level)
    }

    pub(crate) fn serialize(&self) -> SerializedRepDefs {
        self.to_serializer().0.build()
    }

    pub(crate) fn serialize_with_miniblock_repdef_budget(
        &self,
        max_levels_for_bits: impl FnOnce(u64) -> u64,
        num_rows: u64,
        num_values: u64,
    ) -> Result<(SerializedRepDefs, MiniBlockRepDefBudget)> {
        let (context, bits_per_level) = self.to_serializer();
        context.build_with_miniblock_repdef_budget(
            bits_per_level.map(max_levels_for_bits),
            num_rows,
            num_values,
        )
    }
}

impl RawRepDef {
    // Are there any nulls in this layer
    fn has_nulls(&self) -> bool {
        match self {
            Self::Offsets(OffsetDesc { validity, .. }) => validity.is_some(),
            Self::Validity(ValidityDesc { validity, .. }) => validity.is_some(),
            Self::Fsl(FslDesc { validity, .. }) => validity.is_some(),
        }
    }

    // How many values are in this layer
    fn num_values(&self) -> usize {
        match self {
            Self::Offsets(OffsetDesc { num_values, .. }) => *num_values,
            Self::Validity(ValidityDesc { num_values, .. }) => *num_values,
            Self::Fsl(FslDesc { num_values, .. }) => *num_values,
        }
    }

    /// How many empty/null lists are in this layer
    fn num_specials(&self) -> usize {
        match self {
            Self::Offsets(OffsetDesc { num_specials, .. }) => *num_specials,
            _ => 0,
        }
    }

    /// How many definition levels do we need for this layer
    fn max_def(&self) -> u16 {
        match self {
            Self::Offsets(OffsetDesc {
                has_empty_lists,
                validity,
                ..
            }) => {
                let mut max_def = 0;
                if *has_empty_lists {
                    max_def += 1;
                }
                if validity.is_some() {
                    max_def += 1;
                }
                max_def
            }
            Self::Validity(ValidityDesc { validity: None, .. }) => 0,
            Self::Validity(ValidityDesc { .. }) => 1,
            Self::Fsl(FslDesc { validity: None, .. }) => 0,
            Self::Fsl(FslDesc { .. }) => 1,
        }
    }

    /// How many repetition levels do we need for this layer
    fn max_rep(&self) -> u16 {
        match self {
            Self::Offsets(_) => 1,
            _ => 0,
        }
    }
}

/// Represents repetition and definition levels that have been
/// serialized into a pair of (optional) level buffers
#[derive(Debug)]
pub struct SerializedRepDefs {
    /// The repetition levels, one per item
    ///
    /// If None, there are no lists
    pub repetition_levels: Option<Arc<[u16]>>,
    /// The definition levels, one per item
    ///
    /// If None, there are no nulls
    pub definition_levels: Option<Arc<[u16]>>,
    /// The meaning of each definition level
    pub def_meaning: Vec<DefinitionInterpretation>,
    /// The maximum level that is "visible" from the lowest level
    ///
    /// This is the last level before we encounter a list level of some kind.  Once we've
    /// hit a list level then nulls in any level beyond do not map to actual items.
    ///
    /// This is None if there are no lists
    pub max_visible_level: Option<u16>,
    has_fsl: bool,
}

impl SerializedRepDefs {
    fn max_visible_level(def_meaning: &[DefinitionInterpretation]) -> Option<u16> {
        let first_list = def_meaning.iter().position(|level| level.is_list());
        first_list.map(|first_list| {
            def_meaning
                .iter()
                .map(|level| level.num_def_levels())
                .take(first_list)
                .sum::<u16>()
        })
    }

    pub fn new(
        repetition_levels: Option<LevelBuffer>,
        definition_levels: Option<LevelBuffer>,
        def_meaning: Vec<DefinitionInterpretation>,
    ) -> Self {
        Self::new_with_fixed_size_list_levels(
            repetition_levels,
            definition_levels,
            def_meaning,
            false,
        )
    }

    pub(crate) fn new_with_fixed_size_list_levels(
        repetition_levels: Option<LevelBuffer>,
        definition_levels: Option<LevelBuffer>,
        def_meaning: Vec<DefinitionInterpretation>,
        has_fsl: bool,
    ) -> Self {
        let max_visible_level = Self::max_visible_level(&def_meaning);
        Self {
            repetition_levels: repetition_levels.map(Arc::from),
            definition_levels: definition_levels.map(Arc::from),
            def_meaning,
            max_visible_level,
            has_fsl,
        }
    }

    /// Creates an empty SerializedRepDefs (no repetition, all valid)
    pub fn empty(def_meaning: Vec<DefinitionInterpretation>) -> Self {
        Self {
            repetition_levels: None,
            definition_levels: None,
            def_meaning,
            max_visible_level: None,
            has_fsl: false,
        }
    }

    pub fn rep_slicer(&self) -> Option<RepDefSlicer<'_>> {
        self.repetition_levels
            .as_ref()
            .map(|rep| RepDefSlicer::new(self, rep.clone()))
    }

    pub fn def_slicer(&self) -> Option<RepDefSlicer<'_>> {
        self.definition_levels
            .as_ref()
            .map(|def| RepDefSlicer::new(self, def.clone()))
    }

    pub(crate) fn has_fixed_size_list_levels(&self) -> bool {
        self.has_fsl
    }
}

/// Slices a level buffer into pieces
///
/// This is needed to handle the fact that a level buffer may have more
/// levels than values due to special (empty/null) lists.
///
/// As a result, a call to `slice_next(10)` may return 10 levels or it may
/// return more than 10 levels if any special values are encountered.
#[derive(Debug)]
pub struct RepDefSlicer<'a> {
    repdef: &'a SerializedRepDefs,
    to_slice: LanceBuffer,
    current: usize,
}

// TODO: All of this logic will need some changing when we compress rep/def levels.
impl<'a> RepDefSlicer<'a> {
    fn new(repdef: &'a SerializedRepDefs, levels: Arc<[u16]>) -> Self {
        Self {
            repdef,
            to_slice: LanceBuffer::reinterpret_slice(levels),
            current: 0,
        }
    }

    pub fn num_levels(&self) -> usize {
        self.to_slice.len() / 2
    }

    pub fn num_levels_remaining(&self) -> usize {
        self.num_levels() - self.current
    }

    pub fn all_levels(&self) -> &LanceBuffer {
        &self.to_slice
    }

    /// Returns the rest of the levels not yet sliced
    ///
    /// This must be called instead of `slice_next` on the final iteration.
    /// This is because anytime we slice there may be empty/null lists on the
    /// boundary that are "free" and the current behavior in `slice_next` is to
    /// leave them for the next call.
    ///
    /// `slice_rest` will slice all remaining levels and return them.
    pub fn slice_rest(&mut self) -> LanceBuffer {
        let start = self.current;
        let remaining = self.num_levels_remaining();
        self.current = self.num_levels();
        self.to_slice.slice_with_length(start * 2, remaining * 2)
    }

    /// Returns enough levels to satisfy the next `num_values` values
    pub fn slice_next(&mut self, num_values: usize) -> LanceBuffer {
        let start = self.current;
        let Some(max_visible_level) = self.repdef.max_visible_level else {
            // No lists, should be 1:1 mapping from levels to values
            self.current = start + num_values;
            return self.to_slice.slice_with_length(start * 2, num_values * 2);
        };
        if let Some(def) = self.repdef.definition_levels.as_ref() {
            // There are lists and there are def levels.  That means there may be
            // more rep/def levels than values.  We need to scan the def levels to figure
            // out which items are "invisible" and skip over them
            let mut def_itr = def[start..].iter();
            let mut num_taken = 0;
            let mut num_passed = 0;
            while num_taken < num_values {
                let def_level = *def_itr.next().unwrap();
                if def_level <= max_visible_level {
                    num_taken += 1;
                }
                num_passed += 1;
            }
            self.current = start + num_passed;
            self.to_slice.slice_with_length(start * 2, num_passed * 2)
        } else {
            // No def levels, should be 1:1 mapping from levels to values
            self.current = start + num_values;
            self.to_slice.slice_with_length(start * 2, num_values * 2)
        }
    }
}

/// This tells us how an array handles definition.  Given a stack of
/// these and a nested array and a set of definition levels we can calculate
/// how we should interpret the definition levels.
///
/// For example, if the interpretation is [AllValidItem, NullableItem] then
/// a 0 means "valid item" and a 1 means "null struct".  If the interpretation
/// is [NullableItem, NullableItem] then a 0 means "valid item" and a 1 means
/// "null item" and a 2 means "null struct".
///
/// Lists are tricky because we might use up to two definition levels for a
/// single layer of list nesting because we need one value to indicate "empty list"
/// and another value to indicate "null list".
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum DefinitionInterpretation {
    AllValidItem,
    AllValidList,
    NullableItem,
    NullableList,
    EmptyableList,
    NullableAndEmptyableList,
}

impl DefinitionInterpretation {
    /// How many definition levels do we need for this layer
    pub fn num_def_levels(&self) -> u16 {
        match self {
            Self::AllValidItem => 0,
            Self::AllValidList => 0,
            Self::NullableItem => 1,
            Self::NullableList => 1,
            Self::EmptyableList => 1,
            Self::NullableAndEmptyableList => 2,
        }
    }

    /// Does this layer have nulls?
    pub fn is_all_valid(&self) -> bool {
        matches!(
            self,
            Self::AllValidItem | Self::AllValidList | Self::EmptyableList
        )
    }

    /// Does this layer represent a list?
    pub fn is_list(&self) -> bool {
        matches!(
            self,
            Self::AllValidList
                | Self::NullableList
                | Self::EmptyableList
                | Self::NullableAndEmptyableList
        )
    }
}

/// The RepDefBuilder is used to collect offsets & validity buffers
/// from arrow structures.  Once we have those we use the SerializerContext
/// to build the actual repetition and definition levels.
///
/// We know ahead of time how many rep/def levels we will need (number of items
/// in inner-most array + the number of empty/null lists in any parent arrays).
///
/// As a result we try and avoid any re-allocations by pre-allocating the buffers
/// up front.  We allocate two copies of each buffer which allows us to avoid unsafe
/// code caused by reading and writing to the same buffer (also, it's unavoidable
/// because there are times we need to write 'faster' than we read)
#[derive(Debug)]
struct SerializerContext {
    // This is built from outer-to-inner and then reversed at the end
    def_meaning: Vec<DefinitionInterpretation>,
    rep_levels: LevelBuffer,
    spare_rep: LevelBuffer,
    def_levels: LevelBuffer,
    spare_def: LevelBuffer,
    current_rep: u16,
    current_def: u16,
    current_len: usize,
    current_num_specials: usize,
    has_fsl: bool,
}

impl SerializerContext {
    fn new(len: usize, num_layers: usize, max_rep: u16, max_def: u16) -> Self {
        let def_meaning = Vec::with_capacity(num_layers);
        Self {
            rep_levels: if max_rep > 0 {
                vec![0; len]
            } else {
                LevelBuffer::default()
            },
            spare_rep: if max_rep > 0 {
                vec![0; len]
            } else {
                LevelBuffer::default()
            },
            def_levels: if max_def > 0 {
                vec![0; len]
            } else {
                LevelBuffer::default()
            },
            spare_def: if max_def > 0 {
                vec![0; len]
            } else {
                LevelBuffer::default()
            },
            def_meaning,
            current_rep: max_rep,
            current_def: max_def,
            current_len: 0,
            current_num_specials: 0,
            has_fsl: false,
        }
    }

    fn checkout_def(&mut self, meaning: DefinitionInterpretation) -> u16 {
        let def = self.current_def;
        self.current_def -= meaning.num_def_levels();
        self.def_meaning.push(meaning);
        def
    }

    fn record_offsets(&mut self, offset_desc: &OffsetDesc) {
        let rep_level = self.current_rep;
        let (null_list_level, empty_list_level) =
            match (offset_desc.validity.is_some(), offset_desc.has_empty_lists) {
                (true, true) => {
                    let level =
                        self.checkout_def(DefinitionInterpretation::NullableAndEmptyableList);
                    (level - 1, level)
                }
                (true, false) => (self.checkout_def(DefinitionInterpretation::NullableList), 0),
                (false, true) => (
                    0,
                    self.checkout_def(DefinitionInterpretation::EmptyableList),
                ),
                (false, false) => {
                    self.checkout_def(DefinitionInterpretation::AllValidList);
                    (0, 0)
                }
            };
        self.current_rep -= 1;

        if let Some(validity) = &offset_desc.validity {
            self.do_record_validity(validity, null_list_level);
        }

        // We write into the spare buffers and read from the active buffers
        // and then swap at the end.  This way we don't write over what we
        // are reading.

        let mut new_len = 0;
        let expected_len = offset_desc.num_values + self.current_num_specials;
        if expected_len == 0 {
            // Offsets [0] mean no list values, so no levels.
            self.current_len = 0;
            return;
        }
        assert!(self.rep_levels.len() >= expected_len - 1);
        if self.def_levels.is_empty() {
            let mut write_itr = self.spare_rep.iter_mut();
            let mut read_iter = self.rep_levels.iter().copied();
            for w in offset_desc.offsets.windows(2) {
                let len = w[1] - w[0];
                // len can't be 0 because then we'd have def levels
                assert!(len > 0);
                let rep = read_iter.next().unwrap();
                let list_level = if rep == 0 { rep_level } else { rep };
                *write_itr.next().unwrap() = list_level;

                for _ in 1..len {
                    *write_itr.next().unwrap() = 0;
                }
                new_len += len as usize;
            }
            std::mem::swap(&mut self.rep_levels, &mut self.spare_rep);
        } else {
            assert!(self.def_levels.len() >= expected_len - 1);
            let mut def_write_itr = self.spare_def.iter_mut();
            let mut rep_write_itr = self.spare_rep.iter_mut();
            let mut rep_read_itr = self.rep_levels.iter().copied();
            let mut def_read_itr = self.def_levels.iter().copied();
            let specials_to_pass = self.current_num_specials;
            let mut specials_passed = 0;

            for w in offset_desc.offsets.windows(2) {
                let mut def = def_read_itr.next().unwrap();
                // Copy over any higher-level special values in place
                while def > SPECIAL_THRESHOLD {
                    *def_write_itr.next().unwrap() = def;
                    *rep_write_itr.next().unwrap() = rep_read_itr.next().unwrap();
                    def = def_read_itr.next().unwrap();
                    new_len += 1;
                    specials_passed += 1;
                }

                let len = w[1] - w[0];
                let rep = rep_read_itr.next().unwrap();

                // If the rep_level is 0 then we are the first list level
                // otherwise we are starting a higher level list so keep
                // existing rep level
                let list_level = if rep == 0 { rep_level } else { rep };

                if def == 0 && len > 0 {
                    // New valid list, write a rep level and then add new 0/0 items
                    *def_write_itr.next().unwrap() = 0;
                    *rep_write_itr.next().unwrap() = list_level;

                    for _ in 1..len {
                        *def_write_itr.next().unwrap() = 0;
                        *rep_write_itr.next().unwrap() = 0;
                    }

                    new_len += len as usize;
                } else if def == 0 {
                    // Empty list, insert new special
                    *def_write_itr.next().unwrap() = empty_list_level + SPECIAL_THRESHOLD;
                    *rep_write_itr.next().unwrap() = list_level;
                    new_len += 1;
                } else {
                    // Either the list is null or one of its struct parents
                    // is null.  Promote it to a special value.
                    *def_write_itr.next().unwrap() = def + SPECIAL_THRESHOLD;
                    *rep_write_itr.next().unwrap() = list_level;
                    new_len += 1;
                }
            }

            // If we have any special values at the end, we need to copy them over
            while specials_passed < specials_to_pass {
                *def_write_itr.next().unwrap() = def_read_itr.next().unwrap();
                *rep_write_itr.next().unwrap() = rep_read_itr.next().unwrap();
                new_len += 1;
                specials_passed += 1;
            }
            std::mem::swap(&mut self.def_levels, &mut self.spare_def);
            std::mem::swap(&mut self.rep_levels, &mut self.spare_rep);
        }

        self.current_len = new_len;
        self.current_num_specials += offset_desc.num_specials;
    }

    fn do_record_validity(&mut self, validity: &BooleanBuffer, null_level: u16) {
        assert!(self.def_levels.len() >= validity.len() + self.current_num_specials);
        debug_assert!(
            self.current_len == 0 || self.current_len == validity.len() + self.current_num_specials
        );
        self.current_len = validity.len();

        let mut def_read_itr = self.def_levels.iter().copied();
        let mut def_write_itr = self.spare_def.iter_mut();

        let specials_to_pass = self.current_num_specials;
        let mut specials_passed = 0;

        for incoming_validity in validity.iter() {
            let mut def = def_read_itr.next().unwrap();
            while def > SPECIAL_THRESHOLD {
                *def_write_itr.next().unwrap() = def;
                def = def_read_itr.next().unwrap();
                specials_passed += 1;
            }
            if def == 0 && !incoming_validity {
                *def_write_itr.next().unwrap() = null_level;
            } else {
                *def_write_itr.next().unwrap() = def;
            }
        }

        while specials_passed < specials_to_pass {
            *def_write_itr.next().unwrap() = def_read_itr.next().unwrap();
            specials_passed += 1;
        }

        std::mem::swap(&mut self.def_levels, &mut self.spare_def);
    }

    fn multiply_levels(&mut self, multiplier: usize) {
        let old_len = self.current_len;
        // All non-special values will be broadcasted by the multiplier.  Special values are copied as-is.
        self.current_len =
            (self.current_len - self.current_num_specials) * multiplier + self.current_num_specials;

        if self.rep_levels.is_empty() && self.def_levels.is_empty() {
            // All valid with no rep/def levels, nothing to do
            return;
        } else if self.rep_levels.is_empty() {
            assert!(self.def_levels.len() >= self.current_len);
            // No rep levels, just multiply the def levels
            let mut def_read_itr = self.def_levels.iter().copied();
            let mut def_write_itr = self.spare_def.iter_mut();
            for _ in 0..old_len {
                let mut def = def_read_itr.next().unwrap();
                while def > SPECIAL_THRESHOLD {
                    *def_write_itr.next().unwrap() = def;
                    def = def_read_itr.next().unwrap();
                }
                for _ in 0..multiplier {
                    *def_write_itr.next().unwrap() = def;
                }
            }
        } else if self.def_levels.is_empty() {
            assert!(self.rep_levels.len() >= self.current_len);
            // No def levels, just multiply the rep levels
            let mut rep_read_itr = self.rep_levels.iter().copied();
            let mut rep_write_itr = self.spare_rep.iter_mut();
            for _ in 0..old_len {
                let rep = rep_read_itr.next().unwrap();
                for _ in 0..multiplier {
                    *rep_write_itr.next().unwrap() = rep;
                }
            }
        } else {
            assert!(self.rep_levels.len() >= self.current_len);
            assert!(self.def_levels.len() >= self.current_len);
            let mut rep_read_itr = self.rep_levels.iter().copied();
            let mut def_read_itr = self.def_levels.iter().copied();
            let mut rep_write_itr = self.spare_rep.iter_mut();
            let mut def_write_itr = self.spare_def.iter_mut();
            for _ in 0..old_len {
                let mut def = def_read_itr.next().unwrap();
                while def > SPECIAL_THRESHOLD {
                    *def_write_itr.next().unwrap() = def;
                    *rep_write_itr.next().unwrap() = rep_read_itr.next().unwrap();
                    def = def_read_itr.next().unwrap();
                }
                let rep = rep_read_itr.next().unwrap();
                for _ in 0..multiplier {
                    *def_write_itr.next().unwrap() = def;
                    *rep_write_itr.next().unwrap() = rep;
                }
            }
        }
        std::mem::swap(&mut self.def_levels, &mut self.spare_def);
        std::mem::swap(&mut self.rep_levels, &mut self.spare_rep);
    }

    fn record_validity_buf(&mut self, validity: &Option<BooleanBuffer>) {
        if let Some(validity) = validity {
            let def_level = self.checkout_def(DefinitionInterpretation::NullableItem);
            self.do_record_validity(validity, def_level);
        } else {
            self.checkout_def(DefinitionInterpretation::AllValidItem);
        }
    }

    fn record_validity(&mut self, validity_desc: &ValidityDesc) {
        self.record_validity_buf(&validity_desc.validity)
    }

    fn record_fsl(&mut self, fsl_desc: &FslDesc) {
        self.has_fsl = true;
        self.record_validity_buf(&fsl_desc.validity);
        self.multiply_levels(fsl_desc.dimension);
    }

    fn normalize_specials(&mut self) {
        for def in self.def_levels.iter_mut() {
            if *def > SPECIAL_THRESHOLD {
                *def -= SPECIAL_THRESHOLD;
            }
        }
    }

    fn normalize_specials_and_plan_splits(
        &mut self,
        def_meaning: &[DefinitionInterpretation],
        max_levels_per_page: Option<u64>,
        num_rows: u64,
        num_values: u64,
    ) -> Result<MiniBlockRepDefBudget> {
        // Extremely sparse lists can have many rep/def levels for very few
        // visible leaf values.  If this ratio becomes too skewed then a
        // mini-block rep/def chunk can exceed its packed metadata budget even
        // though the value buffers are small.  We detect that case while
        // normalizing special def levels and split on top-level row boundaries
        // so each emitted dense mini-block page stays within the budget.
        if self.def_levels.is_empty() {
            return Ok(MiniBlockRepDefBudget::WithinBudget);
        }

        if self.rep_levels.is_empty() {
            self.normalize_specials();
            return Ok(MiniBlockRepDefBudget::WithinBudget);
        }

        if self.rep_levels.len() != self.def_levels.len() {
            return Err(Error::internal(format!(
                "Cannot plan structural page splits with mismatched rep/def lengths: rep={}, def={}",
                self.rep_levels.len(),
                self.def_levels.len()
            )));
        }

        let Some(max_levels_per_page) = max_levels_per_page else {
            self.normalize_specials();
            return Ok(MiniBlockRepDefBudget::WithinBudget);
        };

        if num_values == 0 {
            self.normalize_specials();
            return Ok(MiniBlockRepDefBudget::WithinBudget);
        }

        let max_schema_rep = def_meaning.iter().filter(|level| level.is_list()).count() as u16;
        let max_visible_level = SerializedRepDefs::max_visible_level(def_meaning);
        let should_plan = !self.has_fsl && max_schema_rep > 0 && max_visible_level.is_some();

        if !should_plan {
            self.normalize_specials();
            return Ok(MiniBlockRepDefBudget::WithinBudget);
        }

        let max_visible_level = max_visible_level.unwrap();
        let mut splits = Vec::new();
        let mut counted_rows = 0u64;
        let mut counted_values = 0u64;
        let mut saw_structural_overhead = false;
        let mut single_row_over_budget_levels = None;

        let mut current_row_level_start = None;
        let mut current_row_num_values = 0u64;

        let mut current_page_row_start = 0u64;
        let mut current_page_num_rows = 0u64;
        let mut current_page_level_start = 0usize;
        let mut current_page_level_end = 0usize;
        let mut current_page_value_start = 0u64;
        let mut current_page_num_values = 0u64;
        let mut current_page_num_levels = 0u64;
        let mut current_page_has_structural_overhead = false;

        let mut finish_row =
            |row_level_start: usize, row_level_end: usize, row_num_values: u64| -> Result<()> {
                let row_num_levels = (row_level_end - row_level_start) as u64;
                let row_has_structural_overhead = row_num_levels > row_num_values;
                saw_structural_overhead |= row_has_structural_overhead;

                if row_has_structural_overhead && row_num_levels > max_levels_per_page {
                    single_row_over_budget_levels = Some(row_num_levels);
                }

                if current_page_num_rows > 0
                    && (current_page_has_structural_overhead || row_has_structural_overhead)
                    && current_page_num_levels + row_num_levels > max_levels_per_page
                {
                    splits.push(MiniBlockRepDefSplit {
                        row_start: current_page_row_start,
                        num_rows: current_page_num_rows,
                        level_range: current_page_level_start..current_page_level_end,
                        value_start: current_page_value_start,
                        num_values: current_page_num_values,
                    });
                    current_page_row_start = counted_rows;
                    current_page_num_rows = 0;
                    current_page_level_start = row_level_start;
                    current_page_value_start = counted_values;
                    current_page_num_values = 0;
                    current_page_num_levels = 0;
                    current_page_has_structural_overhead = false;
                }

                if current_page_num_rows == 0 {
                    current_page_level_start = row_level_start;
                }
                current_page_num_rows += 1;
                current_page_level_end = row_level_end;
                current_page_num_values += row_num_values;
                current_page_num_levels += row_num_levels;
                current_page_has_structural_overhead |= row_has_structural_overhead;
                counted_rows += 1;
                counted_values += row_num_values;
                Ok(())
            };

        for (idx, (rep_level, def_level)) in self
            .rep_levels
            .iter()
            .copied()
            .zip(self.def_levels.iter_mut())
            .enumerate()
        {
            if *def_level > SPECIAL_THRESHOLD {
                *def_level -= SPECIAL_THRESHOLD;
            }

            if rep_level == max_schema_rep {
                if let Some(level_start) = current_row_level_start {
                    finish_row(level_start, idx, current_row_num_values)?;
                    current_row_num_values = 0;
                } else if idx != 0 {
                    return Err(Error::internal(format!(
                        "Cannot plan structural page splits: first top-level row starts at level {}, expected 0",
                        idx
                    )));
                }
                current_row_level_start = Some(idx);
            }

            if current_row_level_start.is_none() {
                return Err(Error::internal(
                    "Cannot plan structural page splits: found levels before the first top-level row start",
                ));
            }
            if *def_level <= max_visible_level {
                current_row_num_values += 1;
            }
        }

        let Some(level_start) = current_row_level_start else {
            return Err(Error::internal(
                "Cannot plan structural page splits: found no top-level row starts",
            ));
        };
        finish_row(level_start, self.rep_levels.len(), current_row_num_values)?;

        if counted_rows != num_rows {
            return Err(Error::internal(format!(
                "Cannot plan structural page splits: expected {} top-level row starts, found {}",
                num_rows, counted_rows
            )));
        }
        if counted_values != num_values {
            return Err(Error::internal(format!(
                "Cannot plan structural page splits: counted {} visible values, expected {}",
                counted_values, num_values
            )));
        }
        if !saw_structural_overhead {
            return Ok(MiniBlockRepDefBudget::WithinBudget);
        }
        if let Some(row_num_levels) = single_row_over_budget_levels {
            return Ok(MiniBlockRepDefBudget::SingleRowOverBudget(row_num_levels));
        }

        if current_page_num_rows > 0 {
            splits.push(MiniBlockRepDefSplit {
                row_start: current_page_row_start,
                num_rows: current_page_num_rows,
                level_range: current_page_level_start..current_page_level_end,
                value_start: current_page_value_start,
                num_values: current_page_num_values,
            });
        }

        if splits.len() > 1 {
            Ok(MiniBlockRepDefBudget::RequiresPageSplit(splits))
        } else {
            Ok(MiniBlockRepDefBudget::WithinBudget)
        }
    }

    fn build(mut self) -> SerializedRepDefs {
        if self.current_len == 0 {
            return SerializedRepDefs::new_with_fixed_size_list_levels(
                None,
                None,
                self.def_meaning,
                self.has_fsl,
            );
        }

        self.normalize_specials();

        let definition_levels = if self.def_levels.is_empty() {
            None
        } else {
            Some(self.def_levels)
        };
        let repetition_levels = if self.rep_levels.is_empty() {
            None
        } else {
            Some(self.rep_levels)
        };

        // Need to reverse the def meaning since we build rep / def levels in reverse
        let def_meaning = self.def_meaning.into_iter().rev().collect::<Vec<_>>();

        SerializedRepDefs::new_with_fixed_size_list_levels(
            repetition_levels,
            definition_levels,
            def_meaning,
            self.has_fsl,
        )
    }

    fn build_with_miniblock_repdef_budget(
        mut self,
        max_levels_per_page: Option<u64>,
        num_rows: u64,
        num_values: u64,
    ) -> Result<(SerializedRepDefs, MiniBlockRepDefBudget)> {
        if self.current_len == 0 {
            return Ok((
                SerializedRepDefs::new_with_fixed_size_list_levels(
                    None,
                    None,
                    self.def_meaning,
                    self.has_fsl,
                ),
                MiniBlockRepDefBudget::WithinBudget,
            ));
        }

        // Need to reverse the def meaning since we build rep / def levels in reverse
        let def_meaning = std::mem::take(&mut self.def_meaning)
            .into_iter()
            .rev()
            .collect::<Vec<_>>();
        let budget = self.normalize_specials_and_plan_splits(
            &def_meaning,
            max_levels_per_page,
            num_rows,
            num_values,
        )?;

        let definition_levels = if self.def_levels.is_empty() {
            None
        } else {
            Some(self.def_levels)
        };
        let repetition_levels = if self.rep_levels.is_empty() {
            None
        } else {
            Some(self.rep_levels)
        };

        Ok((
            SerializedRepDefs::new_with_fixed_size_list_levels(
                repetition_levels,
                definition_levels,
                def_meaning,
                self.has_fsl,
            ),
            budget,
        ))
    }
}

/// A structure used to collect validity buffers and offsets from arrow
/// arrays and eventually create repetition and definition levels
///
/// As we are encoding the structural encoders are given this struct and
/// will record the arrow information into it.  Once we hit a leaf node we
/// serialize the data into rep/def levels and write these into the page.
#[derive(Clone, Default, Debug)]
pub struct RepDefBuilder {
    // The rep/def info we have collected so far
    repdefs: Vec<RawRepDef>,
    // The current length, can get larger as we traverse lists (e.g. an
    // array might have 5 lists which results in 50 items)
    //
    // Starts uninitialized until we see the first rep/def item
    len: Option<usize>,
}

impl RepDefBuilder {
    fn check_validity_len(&mut self, incoming_len: usize) {
        if let Some(len) = self.len {
            assert_eq!(incoming_len, len);
        } else {
            // First validity buffer we've seen
            self.len = Some(incoming_len);
        }
    }

    fn num_layers(&self) -> usize {
        self.repdefs.len()
    }

    /// The builder is "empty" if there is no repetition and no nulls.  In this case we don't need
    /// to store anything to disk (except the description)
    pub fn is_empty(&self) -> bool {
        self.repdefs
            .iter()
            .all(|r| matches!(r, RawRepDef::Validity(ValidityDesc { validity: None, .. })))
    }

    /// Returns true if there is only a single layer of definition
    pub fn is_simple_validity(&self) -> bool {
        self.repdefs.len() == 1 && matches!(self.repdefs[0], RawRepDef::Validity(_))
    }

    /// Registers a nullable validity bitmap
    pub fn add_validity_bitmap(&mut self, validity: NullBuffer) {
        self.check_validity_len(validity.len());
        if validity.null_count() == 0 {
            self.add_no_null(validity.len());
            return;
        }
        self.repdefs.push(RawRepDef::Validity(ValidityDesc {
            num_values: validity.len(),
            validity: Some(validity.into_inner()),
        }));
    }

    /// Registers an all-valid validity layer
    pub fn add_no_null(&mut self, len: usize) {
        self.check_validity_len(len);
        self.repdefs.push(RawRepDef::Validity(ValidityDesc {
            validity: None,
            num_values: len,
        }));
    }

    pub fn add_fsl(&mut self, validity: Option<NullBuffer>, dimension: usize, num_values: usize) {
        if let Some(len) = self.len {
            assert_eq!(num_values, len);
        }
        self.len = Some(num_values * dimension);
        debug_assert!(validity.is_none() || validity.as_ref().unwrap().len() == num_values);
        self.repdefs.push(RawRepDef::Fsl(FslDesc {
            num_values,
            validity: validity.map(|v| v.into_inner()),
            dimension,
        }))
    }

    fn check_offset_len(&mut self, offsets: &[i64]) {
        if let Some(len) = self.len {
            assert!(offsets.len() == len + 1);
        }
        self.len = Some(offsets[offsets.len() - 1] as usize);
    }

    fn do_add_offsets(
        &mut self,
        lengths: impl Iterator<Item = i64>,
        validity: Option<NullBuffer>,
        capacity: usize,
    ) -> bool {
        let mut num_specials = 0;
        let mut has_empty_lists = false;
        let mut has_garbage_values = false;
        let mut last_off: i64 = 0;

        let mut normalized_offsets = Vec::with_capacity(capacity);
        normalized_offsets.push(0);

        if let Some(ref validity) = validity {
            for (len, is_valid) in lengths.zip(validity.iter()) {
                match (is_valid, len == 0) {
                    (false, is_empty) => {
                        num_specials += 1;
                        has_garbage_values |= !is_empty;
                    }
                    (true, true) => {
                        num_specials += 1;
                        has_empty_lists = true;
                    }
                    _ => {
                        last_off += len;
                    }
                }
                normalized_offsets.push(last_off);
            }
        } else {
            for len in lengths {
                if len == 0 {
                    num_specials += 1;
                    has_empty_lists = true;
                }
                last_off += len;
                normalized_offsets.push(last_off);
            }
        }

        self.check_offset_len(&normalized_offsets);
        self.repdefs.push(RawRepDef::Offsets(OffsetDesc {
            num_values: normalized_offsets.len() - 1,
            offsets: normalized_offsets.into(),
            validity: validity.map(|v| v.into_inner()),
            has_empty_lists,
            num_specials: num_specials as usize,
        }));

        has_garbage_values
    }

    /// Adds a layer of offsets
    ///
    /// Offsets are casted to a common type (i64) and also normalized.  Null lists are
    /// always represented by a zero-length (identical) pair of offsets and so the caller
    /// should filter out any garbage items before encoding them.  To assist with this the
    /// method will return true if any non-empty null lists were found.
    pub fn add_offsets<O: OffsetSizeTrait>(
        &mut self,
        offsets: OffsetBuffer<O>,
        validity: Option<NullBuffer>,
    ) -> bool {
        let inner = offsets.into_inner();
        let buffer_len = inner.len();

        if O::IS_LARGE {
            let i64_buff = ScalarBuffer::<i64>::new(inner.into_inner(), 0, buffer_len);
            let lengths = i64_buff.windows(2).map(|off| off[1] - off[0]);
            self.do_add_offsets(lengths, validity, buffer_len)
        } else {
            let i32_buff = ScalarBuffer::<i32>::new(inner.into_inner(), 0, buffer_len);
            let lengths = i32_buff.windows(2).map(|off| (off[1] - off[0]) as i64);
            self.do_add_offsets(lengths, validity, buffer_len)
        }
    }

    // When we are encoding data it arrives in batches.  For each batch we create a RepDefBuilder and collect the
    // various validity buffers and offset buffers from that batch.  Once we have enough batches to write a page we
    // need to take this collection of RepDefBuilders and concatenate them and then serialize them into rep/def levels.
    //
    // TODO: In the future, we may concatenate and serialize at the same time?
    //
    // This method takes care of the concatenation part.  First we collect all of layer 0 from each builder, then we
    // call this method.  Then we collect all of layer 1 from each builder and call this method.  And so on.
    //
    // That means this method should get a collection of `RawRepDef` where each item is the same kind (all validity or
    // all offsets) though the nullability / lengths may be different in each layer.
    fn concat_layers<'a>(
        layers: impl Iterator<Item = &'a RawRepDef>,
        num_layers: usize,
    ) -> RawRepDef {
        enum LayerKind {
            Validity,
            Fsl,
            Offsets,
        }

        // We make two passes through the layers.  The first determines if we need to pay the cost of allocating
        // buffers.  The second pass actually adds the values.
        let mut collected = Vec::with_capacity(num_layers);
        let mut has_nulls = false;
        let mut layer_kind = LayerKind::Validity;
        let mut total_num_specials = 0;
        let mut all_dimension = 0;
        let mut all_has_empty_lists = false;
        let mut all_num_values = 0;
        for layer in layers {
            has_nulls |= layer.has_nulls();
            match layer {
                RawRepDef::Validity(_) => {
                    layer_kind = LayerKind::Validity;
                }
                RawRepDef::Offsets(OffsetDesc {
                    num_specials,
                    has_empty_lists,
                    ..
                }) => {
                    all_has_empty_lists |= *has_empty_lists;
                    layer_kind = LayerKind::Offsets;
                    total_num_specials += num_specials;
                }
                RawRepDef::Fsl(FslDesc { dimension, .. }) => {
                    layer_kind = LayerKind::Fsl;
                    all_dimension = *dimension;
                }
            }
            collected.push(layer);
            all_num_values += layer.num_values();
        }

        // Shortcut if there are no nulls
        if !has_nulls {
            match layer_kind {
                LayerKind::Validity => {
                    return RawRepDef::Validity(ValidityDesc {
                        validity: None,
                        num_values: all_num_values,
                    });
                }
                LayerKind::Fsl => {
                    return RawRepDef::Fsl(FslDesc {
                        validity: None,
                        num_values: all_num_values,
                        dimension: all_dimension,
                    });
                }
                LayerKind::Offsets => {}
            }
        }

        // Only allocate if needed
        let mut validity_builder = if has_nulls {
            BooleanBufferBuilder::new(all_num_values)
        } else {
            BooleanBufferBuilder::new(0)
        };
        let mut all_offsets = if matches!(layer_kind, LayerKind::Offsets) {
            let mut all_offsets = Vec::with_capacity(all_num_values);
            all_offsets.push(0);
            all_offsets
        } else {
            Vec::new()
        };

        for layer in collected {
            match layer {
                RawRepDef::Validity(ValidityDesc {
                    validity: Some(validity),
                    ..
                }) => {
                    validity_builder.append_buffer(validity);
                }
                RawRepDef::Validity(ValidityDesc {
                    validity: None,
                    num_values,
                }) => {
                    validity_builder.append_n(*num_values, true);
                }
                RawRepDef::Fsl(FslDesc {
                    validity,
                    num_values,
                    ..
                }) => {
                    if let Some(validity) = validity {
                        validity_builder.append_buffer(validity);
                    } else {
                        validity_builder.append_n(*num_values, true);
                    }
                }
                RawRepDef::Offsets(OffsetDesc {
                    offsets,
                    validity: Some(validity),
                    has_empty_lists,
                    ..
                }) => {
                    all_has_empty_lists |= has_empty_lists;
                    validity_builder.append_buffer(validity);
                    let last = *all_offsets.last().unwrap();
                    all_offsets.extend(offsets.iter().skip(1).map(|off| *off + last));
                }
                RawRepDef::Offsets(OffsetDesc {
                    offsets,
                    validity: None,
                    has_empty_lists,
                    num_values,
                    ..
                }) => {
                    all_has_empty_lists |= has_empty_lists;
                    if has_nulls {
                        validity_builder.append_n(*num_values, true);
                    }
                    let last = *all_offsets.last().unwrap();
                    all_offsets.extend(offsets.iter().skip(1).map(|off| *off + last));
                }
            }
        }
        let validity = if has_nulls {
            Some(validity_builder.finish())
        } else {
            None
        };
        match layer_kind {
            LayerKind::Fsl => RawRepDef::Fsl(FslDesc {
                validity,
                num_values: all_num_values,
                dimension: all_dimension,
            }),
            LayerKind::Validity => RawRepDef::Validity(ValidityDesc {
                validity,
                num_values: all_num_values,
            }),
            LayerKind::Offsets => RawRepDef::Offsets(OffsetDesc {
                offsets: all_offsets.into(),
                validity,
                has_empty_lists: all_has_empty_lists,
                num_values: all_num_values,
                num_specials: total_num_specials,
            }),
        }
    }

    /// Converts the validity / offsets buffers that have been gathered so far
    /// into repetition and definition levels
    pub fn serialize(builders: Vec<Self>) -> SerializedRepDefs {
        Self::normalize(builders).serialize()
    }

    pub(crate) fn normalize(builders: Vec<Self>) -> NormalizedStructuralPlan {
        assert!(!builders.is_empty());
        let num_layers = builders[0].num_layers();
        debug_assert!(
            builders
                .iter()
                .all(|builder| builder.num_layers() == num_layers)
        );
        let layers = (0..num_layers)
            .map(|layer_index| {
                Self::concat_layers(
                    builders.iter().map(|b| &b.repdefs[layer_index]),
                    builders.len(),
                )
            })
            .collect::<Vec<_>>();
        NormalizedStructuralPlan {
            layers,
            dense_all_valid: builders.iter().all(Self::is_empty),
        }
    }
}

/// Starts with serialized repetition and definition levels and unravels
/// them into validity buffers and offsets buffers
///
/// This is used during decoding to create the necessary arrow structures
#[derive(Debug)]
pub struct RepDefUnraveler {
    sparse: Option<SparseStructuralUnraveler>,
    rep_levels: Option<LevelBuffer>,
    def_levels: Option<LevelBuffer>,
    // Maps from definition level to the rep level at which that definition level is visible
    levels_to_rep: Vec<u16>,
    def_meaning: Arc<[DefinitionInterpretation]>,
    // Current definition level to compare to.
    current_def_cmp: u16,
    // Current rep level, determines which specials we can see
    current_rep_cmp: u16,
    // Current layer index, 0 means inner-most layer and it counts up from there.  Used to index
    // into special_defs
    current_layer: usize,
    // Number of items in the inner-most layer (needed if the definition levels are not present)
    num_items: u64,
}

impl RepDefUnraveler {
    /// Creates a new unraveler from serialized repetition and definition information
    pub fn new(
        rep_levels: Option<LevelBuffer>,
        def_levels: Option<LevelBuffer>,
        def_meaning: Arc<[DefinitionInterpretation]>,
        num_items: u64,
    ) -> Self {
        let mut levels_to_rep = Vec::with_capacity(def_meaning.len());
        let mut rep_counter = 0;
        // Level=0 is always visible and means valid item
        levels_to_rep.push(0);
        for meaning in def_meaning.as_ref() {
            match meaning {
                DefinitionInterpretation::AllValidItem | DefinitionInterpretation::AllValidList => {
                    // There is no corresponding level, so nothing to put in levels_to_rep
                }
                DefinitionInterpretation::NullableItem => {
                    // Some null structs are not visible at inner rep levels in cases like LIST<STRUCT<LIST<...>>>
                    levels_to_rep.push(rep_counter);
                }
                DefinitionInterpretation::NullableList => {
                    rep_counter += 1;
                    levels_to_rep.push(rep_counter);
                }
                DefinitionInterpretation::EmptyableList => {
                    rep_counter += 1;
                    levels_to_rep.push(rep_counter);
                }
                DefinitionInterpretation::NullableAndEmptyableList => {
                    rep_counter += 1;
                    levels_to_rep.push(rep_counter);
                    levels_to_rep.push(rep_counter);
                }
            }
        }
        Self {
            sparse: None,
            rep_levels,
            def_levels,
            current_def_cmp: 0,
            current_rep_cmp: 0,
            levels_to_rep,
            current_layer: 0,
            def_meaning,
            num_items,
        }
    }

    pub(crate) fn new_sparse(plan: SparseStructuralPlan) -> Self {
        Self {
            sparse: Some(SparseStructuralUnraveler::new(plan)),
            rep_levels: None,
            def_levels: None,
            levels_to_rep: Vec::new(),
            def_meaning: Arc::new([]),
            current_def_cmp: 0,
            current_rep_cmp: 0,
            current_layer: 0,
            num_items: 0,
        }
    }

    fn ensure_exhausted(&self) -> Result<()> {
        if let Some(sparse) = &self.sparse {
            sparse.ensure_exhausted()?;
        }
        Ok(())
    }

    fn is_sparse(&self) -> bool {
        self.sparse.is_some()
    }

    pub fn is_all_valid(&self) -> bool {
        if let Some(sparse) = &self.sparse {
            return sparse.is_all_valid();
        }
        self.def_levels.is_none() || self.def_meaning[self.current_layer].is_all_valid()
    }

    /// If the current level is a repetition layer then this returns the number of lists
    /// at this level.
    ///
    /// This is not valid to call when the current level is a struct/primitive layer because
    /// in some cases there may be no rep or def information to know this.
    pub fn max_lists(&self) -> Result<usize> {
        if let Some(sparse) = &self.sparse {
            return sparse.max_lists();
        }
        debug_assert!(
            self.def_meaning[self.current_layer] != DefinitionInterpretation::NullableItem
        );
        Ok(self
            .rep_levels
            .as_ref()
            // Worst case every rep item is max_rep and a new list
            .map(|levels| levels.len())
            .unwrap_or(0))
    }

    /// Unravels a layer of offsets from the unraveler into the given offset width
    ///
    /// When decoding a list the caller should first unravel the offsets and then
    /// unravel the validity (this is the opposite order used during encoding)
    pub fn unravel_offsets<T: ArrowNativeType>(
        &mut self,
        offsets: &mut Vec<T>,
        validity: Option<&mut BooleanBufferBuilder>,
    ) -> Result<()> {
        if let Some(sparse) = self.sparse.as_mut() {
            return sparse.unravel_offsets(offsets, validity);
        }
        let rep_levels = self
            .rep_levels
            .as_mut()
            .expect("Expected repetition level but data didn't contain repetition");
        let valid_level = self.current_def_cmp;
        let (null_level, empty_level) = match self.def_meaning[self.current_layer] {
            DefinitionInterpretation::NullableList => {
                self.current_def_cmp += 1;
                (valid_level + 1, 0)
            }
            DefinitionInterpretation::EmptyableList => {
                self.current_def_cmp += 1;
                (0, valid_level + 1)
            }
            DefinitionInterpretation::NullableAndEmptyableList => {
                self.current_def_cmp += 2;
                (valid_level + 1, valid_level + 2)
            }
            DefinitionInterpretation::AllValidList => (0, 0),
            _ => unreachable!(),
        };
        self.current_layer += 1;

        // This is the highest def level that is still visible.  Once we hit a list then
        // we stop looking because any null / empty list (or list masked by a higher level
        // null) will not be visible
        let mut max_level = null_level.max(empty_level).max(valid_level);
        // Anything higher than this (but less than max_level) is a null struct masking our
        // list.  We will materialize this is a null list.
        let upper_null = max_level;
        for level in self.def_meaning[self.current_layer..].iter() {
            match level {
                DefinitionInterpretation::NullableItem => {
                    max_level += 1;
                }
                DefinitionInterpretation::AllValidItem => {}
                _ => {
                    break;
                }
            }
        }

        let mut curlen: usize = offsets.last().map(|o| o.as_usize()).unwrap_or(0);

        // If offsets is empty this is a no-op.  If offsets is not empty that means we already
        // added a set of offsets.  For example, we might have added [0, 3, 5] (2 lists).  Now
        // say we want to add [0, 1, 4] (2 lists).  We should get [0, 3, 5, 6, 9] (4 lists).  If
        // we don't pop here we get [0, 3, 5, 5, 6, 9] which is wrong.
        //
        // Or, to think about it another way, if every unraveler adds the starting 0 and the trailing
        // length then we have N + unravelers.len() values instead of N + 1.
        offsets.pop();

        let to_offset = |val: usize| {
            T::from_usize(val)
            .ok_or_else(|| Error::invalid_input("A single batch had more than i32::MAX values and so a large container type is required"))
        };
        self.current_rep_cmp += 1;
        if let Some(def_levels) = &mut self.def_levels {
            assert!(rep_levels.len() == def_levels.len());
            // It's possible validity is None even if we have def levels.  For example, we might have
            // empty lists (which require def levels) but no nulls.
            let mut push_validity: Box<dyn FnMut(bool)> = if let Some(validity) = validity {
                Box::new(|is_valid| validity.append(is_valid))
            } else {
                Box::new(|_| {})
            };
            // This is a strange access pattern.  We are iterating over the rep/def levels and
            // at the same time writing the rep/def levels.  This means we need both a mutable
            // and immutable reference to the rep/def levels.
            let mut read_idx = 0;
            let mut write_idx = 0;
            while read_idx < rep_levels.len() {
                // SAFETY: We assert that rep_levels and def_levels have the same
                // len and read_idx and write_idx can never go past the end.
                unsafe {
                    let rep_val = *rep_levels.get_unchecked(read_idx);
                    if rep_val != 0 {
                        let def_val = *def_levels.get_unchecked(read_idx);
                        // Copy over
                        *rep_levels.get_unchecked_mut(write_idx) = rep_val - 1;
                        *def_levels.get_unchecked_mut(write_idx) = def_val;
                        write_idx += 1;

                        if def_val == 0 {
                            // This is a valid list
                            offsets.push(to_offset(curlen)?);
                            curlen += 1;
                            push_validity(true);
                        } else if def_val > max_level {
                            // This is not visible at this rep level, do not add to offsets, but keep in repdef
                        } else if def_val == null_level || def_val > upper_null {
                            // This is a null list (or a list masked by a null struct)
                            offsets.push(to_offset(curlen)?);
                            push_validity(false);
                        } else if def_val == empty_level {
                            // This is an empty list
                            offsets.push(to_offset(curlen)?);
                            push_validity(true);
                        } else {
                            // New valid list starting with null item
                            offsets.push(to_offset(curlen)?);
                            curlen += 1;
                            push_validity(true);
                        }
                    } else {
                        curlen += 1;
                    }
                    read_idx += 1;
                }
            }
            offsets.push(to_offset(curlen)?);
            rep_levels.truncate(write_idx);
            def_levels.truncate(write_idx);
            Ok(())
        } else {
            // SAFETY: See above loop
            let mut read_idx = 0;
            let mut write_idx = 0;
            let old_offsets_len = offsets.len();
            while read_idx < rep_levels.len() {
                // SAFETY: read_idx / write_idx cannot go past rep_levels.len()
                unsafe {
                    let rep_val = *rep_levels.get_unchecked(read_idx);
                    if rep_val != 0 {
                        // Finish the current list
                        offsets.push(to_offset(curlen)?);
                        *rep_levels.get_unchecked_mut(write_idx) = rep_val - 1;
                        write_idx += 1;
                    }
                    curlen += 1;
                    read_idx += 1;
                }
            }
            let num_new_lists = offsets.len() - old_offsets_len;
            offsets.push(to_offset(curlen)?);
            // Truncate to the number of lists THIS unraveler produced (write_idx),
            // not `offsets.len() - 1` — the latter includes offsets contributed by
            // earlier unravelers in a multi-page read, which would leave too many
            // rep levels for the next (outer) layer and over-count its lists.
            rep_levels.truncate(write_idx);
            if let Some(validity) = validity {
                // Even though we don't have validity it is possible another unraveler did and so we need
                // to push all valids
                validity.append_n(num_new_lists, true);
            }
            Ok(())
        }
    }

    pub fn skip_validity(&mut self) -> Result<()> {
        if let Some(sparse) = self.sparse.as_mut() {
            return sparse.skip_validity();
        }
        debug_assert!(self.is_all_valid());
        self.current_layer += 1;
        Ok(())
    }

    /// Unravels a layer of validity from the definition levels
    pub fn unravel_validity(&mut self, validity: &mut BooleanBufferBuilder) -> Result<()> {
        if let Some(sparse) = self.sparse.as_mut() {
            return sparse.unravel_validity(validity);
        }
        let meaning = self.def_meaning[self.current_layer];
        if meaning == DefinitionInterpretation::AllValidItem || self.def_levels.is_none() {
            self.current_layer += 1;
            validity.append_n(self.num_items as usize, true);
            return Ok(());
        }

        self.current_layer += 1;
        let def_levels = &self.def_levels.as_ref().unwrap();

        let current_def_cmp = self.current_def_cmp;
        self.current_def_cmp += 1;

        for is_valid in def_levels.iter().filter_map(|&level| {
            if self.levels_to_rep[level as usize] <= self.current_rep_cmp {
                Some(level <= current_def_cmp)
            } else {
                None
            }
        }) {
            validity.append(is_valid);
        }
        Ok(())
    }

    pub fn decimate(&mut self, dimension: usize) -> Result<()> {
        if let Some(sparse) = self.sparse.as_mut() {
            return sparse.decimate(dimension);
        }
        if self.rep_levels.is_some() {
            // If we need to support this then I think we need to walk through the rep def levels to find
            // the spots at which we keep.  E.g. if we have:
            //  rep: 1 0 0 1 0 1 0 0 0 1 0 0
            //  def: 1 1 1 0 1 0 1 1 0 1 1 0
            //  dimension: 2
            //
            // The output should be:
            //  rep: 1 0 0 1 0 0 0
            //  def: 1 1 1 0 1 1 0
            //
            // Maybe there's some special logic for empty/null lists?  I'll save the headache for future me.
            todo!("Not yet supported FSL<...List<...>>");
        }
        let Some(def_levels) = self.def_levels.as_mut() else {
            return Ok(());
        };
        let mut read_idx = 0;
        let mut write_idx = 0;
        while read_idx < def_levels.len() {
            unsafe {
                *def_levels.get_unchecked_mut(write_idx) = *def_levels.get_unchecked(read_idx);
            }
            write_idx += 1;
            read_idx += dimension;
        }
        def_levels.truncate(write_idx);
        Ok(())
    }
}

/// As we decode we may extract rep/def information from multiple pages (or multiple
/// chunks within a page).
///
/// For each chunk we create an unraveler.  Each unraveler can have a completely different
/// interpretation (e.g. one page might contain null items but no null structs and the next
/// page might have null structs but no null items).
///
/// Concatenating these unravelers would be tricky and expensive so instead we have a
/// composite unraveler which unravels across multiple unravelers.
///
/// Note: this class should be used even if there is only one page / unraveler.  This is
/// because the `RepDefUnraveler`'s API is more complex (it's meant to be called by this
/// class)
#[derive(Debug)]
pub struct CompositeRepDefUnraveler {
    unravelers: Vec<RepDefUnraveler>,
    comparisons: Vec<Self>,
}

impl CompositeRepDefUnraveler {
    pub fn new(unravelers: Vec<RepDefUnraveler>) -> Self {
        Self {
            unravelers,
            comparisons: Vec::new(),
        }
    }

    pub(crate) fn add_compatibility_check(&mut self, other: Self) {
        self.comparisons.push(other);
    }

    pub(crate) fn has_sparse(&self) -> bool {
        self.unravelers.iter().any(RepDefUnraveler::is_sparse)
            || self.comparisons.iter().any(Self::has_sparse)
    }

    pub(crate) fn ensure_exhausted(&self) -> Result<()> {
        for unraveler in &self.unravelers {
            unraveler.ensure_exhausted()?;
        }
        for comparison in &self.comparisons {
            comparison.ensure_exhausted()?;
        }
        Ok(())
    }

    fn null_buffers_equal(
        left: &Option<NullBuffer>,
        right: &Option<NullBuffer>,
        expected_len: usize,
    ) -> bool {
        match (left, right) {
            (None, None) => true,
            (Some(left), Some(right)) => {
                left.len() == expected_len
                    && right.len() == expected_len
                    && left.iter().eq(right.iter())
            }
            (None, Some(right)) => right.len() == expected_len && right.null_count() == 0,
            (Some(left), None) => left.len() == expected_len && left.null_count() == 0,
        }
    }

    fn decimate(&mut self, dimension: usize) -> Result<()> {
        for unraveler in &mut self.unravelers {
            unraveler.decimate(dimension)?;
        }
        for comparison in &mut self.comparisons {
            comparison.decimate(dimension)?;
        }
        Ok(())
    }

    /// Unravels a layer of validity
    ///
    /// Returns None if there are no null items in this layer
    pub fn unravel_validity(&mut self, num_values: usize) -> Result<Option<NullBuffer>> {
        let is_all_valid = self
            .unravelers
            .iter()
            .all(|unraveler| unraveler.is_all_valid());

        let validity = if is_all_valid {
            for unraveler in self.unravelers.iter_mut() {
                unraveler.skip_validity()?;
            }
            None
        } else {
            let mut validity = BooleanBufferBuilder::new(num_values);
            for unraveler in self.unravelers.iter_mut() {
                unraveler.unravel_validity(&mut validity)?;
            }
            Some(NullBuffer::new(validity.finish()))
        };
        for comparison in &mut self.comparisons {
            let other = comparison.unravel_validity(num_values)?;
            if !Self::null_buffers_equal(&validity, &other, num_values) {
                return Err(Error::invalid_input_source(
                    format!(
                        "Structural sibling fields have incompatible validity metadata for {num_values} values"
                    )
                    .into(),
                ));
            }
        }
        Ok(validity)
    }

    pub fn unravel_fsl_validity(
        &mut self,
        num_values: usize,
        dimension: usize,
    ) -> Result<Option<NullBuffer>> {
        self.decimate(dimension)?;
        self.unravel_validity(num_values)
    }

    /// Unravels a layer of offsets (and the validity for that layer)
    pub fn unravel_offsets<T: ArrowNativeType>(
        &mut self,
    ) -> Result<(OffsetBuffer<T>, Option<NullBuffer>)> {
        let mut is_all_valid = true;
        let mut max_num_lists: usize = 0;
        for unraveler in self.unravelers.iter() {
            is_all_valid &= unraveler.is_all_valid();
            max_num_lists = max_num_lists
                .checked_add(unraveler.max_lists()?)
                .ok_or_else(|| {
                    Error::invalid_input_source(
                        "Combined repetition/definition list count exceeds usize::MAX".into(),
                    )
                })?;
        }

        let mut validity = if is_all_valid {
            None
        } else {
            // Note: This is probably an over-estimate and potentially even an under-estimate.  We only know
            // right now how many items we have and not how many rows.  (TODO: Shouldn't we know the # of rows?)
            Some(BooleanBufferBuilder::new(max_num_lists))
        };

        let mut offsets = Vec::with_capacity(max_num_lists + 1);

        for unraveler in self.unravelers.iter_mut() {
            unraveler.unravel_offsets(&mut offsets, validity.as_mut())?;
        }

        let offsets = OffsetBuffer::new(ScalarBuffer::from(offsets));
        let validity = validity.map(|mut v| NullBuffer::new(v.finish()));
        for comparison in &mut self.comparisons {
            let (other_offsets, other_validity) = comparison.unravel_offsets::<T>()?;
            if offsets.as_ref() != other_offsets.as_ref()
                || !Self::null_buffers_equal(
                    &validity,
                    &other_validity,
                    offsets.len().saturating_sub(1),
                )
            {
                return Err(Error::invalid_input_source(
                    format!(
                        "Structural sibling fields have incompatible list metadata for {} slots",
                        offsets.len().saturating_sub(1)
                    )
                    .into(),
                ));
            }
        }

        Ok((offsets, validity))
    }
}

/// A [`ControlWordIterator`] when there are both repetition and definition levels
///
/// The iterator will put the repetition level in the upper bits and the definition
/// level in the lower bits.  The number of bits used for each level is determined
/// by the width of the repetition and definition levels.
#[derive(Debug)]
pub struct BinaryControlWordIterator<I: Iterator<Item = (u16, u16)>, W> {
    repdef: I,
    def_width: usize,
    max_rep: u16,
    max_visible_def: u16,
    rep_mask: u16,
    def_mask: u16,
    bits_rep: u8,
    bits_def: u8,
    phantom: std::marker::PhantomData<W>,
}

impl<I: Iterator<Item = (u16, u16)>> BinaryControlWordIterator<I, u8> {
    fn append_next(&mut self, buf: &mut Vec<u8>) -> Option<ControlWordDesc> {
        let next = self.repdef.next()?;
        let control_word: u8 =
            (((next.0 & self.rep_mask) as u8) << self.def_width) + ((next.1 & self.def_mask) as u8);
        buf.push(control_word);
        let is_new_row = next.0 == self.max_rep;
        let is_visible = next.1 <= self.max_visible_def;
        let is_valid_item = next.1 == 0;
        Some(ControlWordDesc {
            is_new_row,
            is_visible,
            is_valid_item,
        })
    }
}

impl<I: Iterator<Item = (u16, u16)>> BinaryControlWordIterator<I, u16> {
    fn append_next(&mut self, buf: &mut Vec<u8>) -> Option<ControlWordDesc> {
        let next = self.repdef.next()?;
        let control_word: u16 =
            ((next.0 & self.rep_mask) << self.def_width) + (next.1 & self.def_mask);
        let control_word = control_word.to_le_bytes();
        buf.push(control_word[0]);
        buf.push(control_word[1]);
        let is_new_row = next.0 == self.max_rep;
        let is_visible = next.1 <= self.max_visible_def;
        let is_valid_item = next.1 == 0;
        Some(ControlWordDesc {
            is_new_row,
            is_visible,
            is_valid_item,
        })
    }
}

impl<I: Iterator<Item = (u16, u16)>> BinaryControlWordIterator<I, u32> {
    fn append_next(&mut self, buf: &mut Vec<u8>) -> Option<ControlWordDesc> {
        let next = self.repdef.next()?;
        let control_word: u32 = (((next.0 & self.rep_mask) as u32) << self.def_width)
            + ((next.1 & self.def_mask) as u32);
        let control_word = control_word.to_le_bytes();
        buf.push(control_word[0]);
        buf.push(control_word[1]);
        buf.push(control_word[2]);
        buf.push(control_word[3]);
        let is_new_row = next.0 == self.max_rep;
        let is_visible = next.1 <= self.max_visible_def;
        let is_valid_item = next.1 == 0;
        Some(ControlWordDesc {
            is_new_row,
            is_visible,
            is_valid_item,
        })
    }
}

/// A [`ControlWordIterator`] when there are only definition levels or only repetition levels
#[derive(Debug)]
pub struct UnaryControlWordIterator<I: Iterator<Item = u16>, W> {
    repdef: I,
    level_mask: u16,
    bits_rep: u8,
    bits_def: u8,
    max_rep: u16,
    phantom: std::marker::PhantomData<W>,
}

impl<I: Iterator<Item = u16>> UnaryControlWordIterator<I, u8> {
    fn append_next(&mut self, buf: &mut Vec<u8>) -> Option<ControlWordDesc> {
        let next = self.repdef.next()?;
        buf.push((next & self.level_mask) as u8);
        let is_new_row = self.max_rep == 0 || next == self.max_rep;
        let is_valid_item = next == 0 || self.bits_def == 0;
        Some(ControlWordDesc {
            is_new_row,
            // Either there is no rep, in which case there are no invisible items
            // or there is no def, in which case there are no invisible items
            is_visible: true,
            is_valid_item,
        })
    }
}

impl<I: Iterator<Item = u16>> UnaryControlWordIterator<I, u16> {
    fn append_next(&mut self, buf: &mut Vec<u8>) -> Option<ControlWordDesc> {
        let next = self.repdef.next().unwrap() & self.level_mask;
        let control_word = next.to_le_bytes();
        buf.push(control_word[0]);
        buf.push(control_word[1]);
        let is_new_row = self.max_rep == 0 || next == self.max_rep;
        let is_valid_item = next == 0 || self.bits_def == 0;
        Some(ControlWordDesc {
            is_new_row,
            is_visible: true,
            is_valid_item,
        })
    }
}

impl<I: Iterator<Item = u16>> UnaryControlWordIterator<I, u32> {
    fn append_next(&mut self, buf: &mut Vec<u8>) -> Option<ControlWordDesc> {
        let next = self.repdef.next()?;
        let next = (next & self.level_mask) as u32;
        let control_word = next.to_le_bytes();
        buf.push(control_word[0]);
        buf.push(control_word[1]);
        buf.push(control_word[2]);
        buf.push(control_word[3]);
        let is_new_row = self.max_rep == 0 || next as u16 == self.max_rep;
        let is_valid_item = next == 0 || self.bits_def == 0;
        Some(ControlWordDesc {
            is_new_row,
            is_visible: true,
            is_valid_item,
        })
    }
}

/// A [`ControlWordIterator`] when there are no repetition or definition levels
#[derive(Debug)]
pub struct NilaryControlWordIterator {
    len: usize,
    idx: usize,
}

impl NilaryControlWordIterator {
    fn append_next(&mut self) -> Option<ControlWordDesc> {
        if self.idx == self.len {
            None
        } else {
            self.idx += 1;
            Some(ControlWordDesc {
                is_new_row: true,
                is_visible: true,
                is_valid_item: true,
            })
        }
    }
}

/// Helper function to get a bit mask of the given width
fn get_mask(width: u16) -> u16 {
    (1 << width) - 1
}

// We're really going out of our way to avoid boxing here but this will be called on a per-value basis
// so it is in the critical path.
type SpecificBinaryControlWordIterator<'a, T> = BinaryControlWordIterator<
    Zip<Copied<std::slice::Iter<'a, u16>>, Copied<std::slice::Iter<'a, u16>>>,
    T,
>;

/// An iterator that generates control words from repetition and definition levels
///
/// "Control word" is just a fancy term for a single u8/u16/u32 that contains both
/// the repetition and definition in it.
///
/// In the large majority of case we only need a single byte to represent both the
/// repetition and definition levels.  However, if there is deep nesting then we may
/// need two bytes.  In the worst case we need 4 bytes though this suggests hundreds of
/// levels of nesting which seems unlikely to encounter in practice.
#[derive(Debug)]
pub enum ControlWordIterator<'a> {
    Binary8(SpecificBinaryControlWordIterator<'a, u8>),
    Binary16(SpecificBinaryControlWordIterator<'a, u16>),
    Binary32(SpecificBinaryControlWordIterator<'a, u32>),
    Unary8(UnaryControlWordIterator<Copied<std::slice::Iter<'a, u16>>, u8>),
    Unary16(UnaryControlWordIterator<Copied<std::slice::Iter<'a, u16>>, u16>),
    Unary32(UnaryControlWordIterator<Copied<std::slice::Iter<'a, u16>>, u32>),
    Nilary(NilaryControlWordIterator),
}

/// Describes the properties of a control word
#[derive(Debug)]
pub struct ControlWordDesc {
    pub is_new_row: bool,
    pub is_visible: bool,
    pub is_valid_item: bool,
}

impl ControlWordIterator<'_> {
    /// Appends the next control word to the buffer
    ///
    /// Returns true if this is the start of a new item (i.e. the repetition level is maxed out)
    pub fn append_next(&mut self, buf: &mut Vec<u8>) -> Option<ControlWordDesc> {
        match self {
            Self::Binary8(iter) => iter.append_next(buf),
            Self::Binary16(iter) => iter.append_next(buf),
            Self::Binary32(iter) => iter.append_next(buf),
            Self::Unary8(iter) => iter.append_next(buf),
            Self::Unary16(iter) => iter.append_next(buf),
            Self::Unary32(iter) => iter.append_next(buf),
            Self::Nilary(iter) => iter.append_next(),
        }
    }

    /// Return true if the control word iterator has repetition levels
    pub fn has_repetition(&self) -> bool {
        match self {
            Self::Binary8(_) | Self::Binary16(_) | Self::Binary32(_) => true,
            Self::Unary8(iter) => iter.bits_rep > 0,
            Self::Unary16(iter) => iter.bits_rep > 0,
            Self::Unary32(iter) => iter.bits_rep > 0,
            Self::Nilary(_) => false,
        }
    }

    /// Returns the number of bytes per control word
    pub fn bytes_per_word(&self) -> usize {
        match self {
            Self::Binary8(_) => 1,
            Self::Binary16(_) => 2,
            Self::Binary32(_) => 4,
            Self::Unary8(_) => 1,
            Self::Unary16(_) => 2,
            Self::Unary32(_) => 4,
            Self::Nilary(_) => 0,
        }
    }

    /// Returns the number of bits used for the repetition level
    pub fn bits_rep(&self) -> u8 {
        match self {
            Self::Binary8(iter) => iter.bits_rep,
            Self::Binary16(iter) => iter.bits_rep,
            Self::Binary32(iter) => iter.bits_rep,
            Self::Unary8(iter) => iter.bits_rep,
            Self::Unary16(iter) => iter.bits_rep,
            Self::Unary32(iter) => iter.bits_rep,
            Self::Nilary(_) => 0,
        }
    }

    /// Returns the number of bits used for the definition level
    pub fn bits_def(&self) -> u8 {
        match self {
            Self::Binary8(iter) => iter.bits_def,
            Self::Binary16(iter) => iter.bits_def,
            Self::Binary32(iter) => iter.bits_def,
            Self::Unary8(iter) => iter.bits_def,
            Self::Unary16(iter) => iter.bits_def,
            Self::Unary32(iter) => iter.bits_def,
            Self::Nilary(_) => 0,
        }
    }
}

/// Builds a [`ControlWordIterator`] from repetition and definition levels
/// by first calculating the width needed and then creating the iterator
/// with the appropriate width
pub fn build_control_word_iterator<'a>(
    rep: Option<&'a [u16]>,
    max_rep: u16,
    def: Option<&'a [u16]>,
    max_def: u16,
    max_visible_def: u16,
    len: usize,
) -> ControlWordIterator<'a> {
    let rep_width = if max_rep == 0 {
        0
    } else {
        log_2_ceil(max_rep as u32) as u16
    };
    let rep_mask = if max_rep == 0 { 0 } else { get_mask(rep_width) };
    let def_width = if max_def == 0 {
        0
    } else {
        log_2_ceil(max_def as u32) as u16
    };
    let def_mask = if max_def == 0 { 0 } else { get_mask(def_width) };
    let total_width = rep_width + def_width;
    match (rep, def) {
        (Some(rep), Some(def)) => {
            let iter = rep.iter().copied().zip(def.iter().copied());
            let def_width = def_width as usize;
            if total_width <= 8 {
                ControlWordIterator::Binary8(BinaryControlWordIterator {
                    repdef: iter,
                    rep_mask,
                    def_mask,
                    def_width,
                    max_rep,
                    max_visible_def,
                    bits_rep: rep_width as u8,
                    bits_def: def_width as u8,
                    phantom: std::marker::PhantomData,
                })
            } else if total_width <= 16 {
                ControlWordIterator::Binary16(BinaryControlWordIterator {
                    repdef: iter,
                    rep_mask,
                    def_mask,
                    def_width,
                    max_rep,
                    max_visible_def,
                    bits_rep: rep_width as u8,
                    bits_def: def_width as u8,
                    phantom: std::marker::PhantomData,
                })
            } else {
                ControlWordIterator::Binary32(BinaryControlWordIterator {
                    repdef: iter,
                    rep_mask,
                    def_mask,
                    def_width,
                    max_rep,
                    max_visible_def,
                    bits_rep: rep_width as u8,
                    bits_def: def_width as u8,
                    phantom: std::marker::PhantomData,
                })
            }
        }
        (Some(lev), None) => {
            let iter = lev.iter().copied();
            if total_width <= 8 {
                ControlWordIterator::Unary8(UnaryControlWordIterator {
                    repdef: iter,
                    level_mask: rep_mask,
                    bits_rep: total_width as u8,
                    bits_def: 0,
                    max_rep,
                    phantom: std::marker::PhantomData,
                })
            } else if total_width <= 16 {
                ControlWordIterator::Unary16(UnaryControlWordIterator {
                    repdef: iter,
                    level_mask: rep_mask,
                    bits_rep: total_width as u8,
                    bits_def: 0,
                    max_rep,
                    phantom: std::marker::PhantomData,
                })
            } else {
                ControlWordIterator::Unary32(UnaryControlWordIterator {
                    repdef: iter,
                    level_mask: rep_mask,
                    bits_rep: total_width as u8,
                    bits_def: 0,
                    max_rep,
                    phantom: std::marker::PhantomData,
                })
            }
        }
        (None, Some(lev)) => {
            let iter = lev.iter().copied();
            if total_width <= 8 {
                ControlWordIterator::Unary8(UnaryControlWordIterator {
                    repdef: iter,
                    level_mask: def_mask,
                    bits_rep: 0,
                    bits_def: total_width as u8,
                    max_rep: 0,
                    phantom: std::marker::PhantomData,
                })
            } else if total_width <= 16 {
                ControlWordIterator::Unary16(UnaryControlWordIterator {
                    repdef: iter,
                    level_mask: def_mask,
                    bits_rep: 0,
                    bits_def: total_width as u8,
                    max_rep: 0,
                    phantom: std::marker::PhantomData,
                })
            } else {
                ControlWordIterator::Unary32(UnaryControlWordIterator {
                    repdef: iter,
                    level_mask: def_mask,
                    bits_rep: 0,
                    bits_def: total_width as u8,
                    max_rep: 0,
                    phantom: std::marker::PhantomData,
                })
            }
        }
        (None, None) => ControlWordIterator::Nilary(NilaryControlWordIterator { len, idx: 0 }),
    }
}

/// A parser to unwrap control words into repetition and definition levels
///
/// This is the inverse of the [`ControlWordIterator`].
#[derive(Copy, Clone, Debug)]
pub enum ControlWordParser {
    // First item is the bits to shift, second is the mask to apply (the mask can be
    // calculated from the bits to shift but we don't want to calculate it each time)
    BOTH8(u8, u32),
    BOTH16(u8, u32),
    BOTH32(u8, u32),
    REP8,
    REP16,
    REP32,
    DEF8,
    DEF16,
    DEF32,
    NIL,
}

impl ControlWordParser {
    fn parse_both<const WORD_SIZE: u8>(
        src: &[u8],
        dst_rep: &mut Vec<u16>,
        dst_def: &mut Vec<u16>,
        bits_to_shift: u8,
        mask_to_apply: u32,
    ) {
        match WORD_SIZE {
            1 => {
                let word = src[0];
                let rep = word >> bits_to_shift;
                let def = word & (mask_to_apply as u8);
                dst_rep.push(rep as u16);
                dst_def.push(def as u16);
            }
            2 => {
                let word = u16::from_le_bytes([src[0], src[1]]);
                let rep = word >> bits_to_shift;
                let def = word & mask_to_apply as u16;
                dst_rep.push(rep);
                dst_def.push(def);
            }
            4 => {
                let word = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
                let rep = word >> bits_to_shift;
                let def = word & mask_to_apply;
                dst_rep.push(rep as u16);
                dst_def.push(def as u16);
            }
            _ => unreachable!(),
        }
    }

    fn parse_desc_both<const WORD_SIZE: u8>(
        src: &[u8],
        bits_to_shift: u8,
        mask_to_apply: u32,
        max_rep: u16,
        max_visible_def: u16,
    ) -> ControlWordDesc {
        match WORD_SIZE {
            1 => {
                let word = src[0];
                let rep = word >> bits_to_shift;
                let def = word & (mask_to_apply as u8);
                let is_visible = def as u16 <= max_visible_def;
                let is_new_row = rep as u16 == max_rep;
                let is_valid_item = def == 0;
                ControlWordDesc {
                    is_visible,
                    is_new_row,
                    is_valid_item,
                }
            }
            2 => {
                let word = u16::from_le_bytes([src[0], src[1]]);
                let rep = word >> bits_to_shift;
                let def = word & mask_to_apply as u16;
                let is_visible = def <= max_visible_def;
                let is_new_row = rep == max_rep;
                let is_valid_item = def == 0;
                ControlWordDesc {
                    is_visible,
                    is_new_row,
                    is_valid_item,
                }
            }
            4 => {
                let word = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
                let rep = word >> bits_to_shift;
                let def = word & mask_to_apply;
                let is_visible = def as u16 <= max_visible_def;
                let is_new_row = rep as u16 == max_rep;
                let is_valid_item = def == 0;
                ControlWordDesc {
                    is_visible,
                    is_new_row,
                    is_valid_item,
                }
            }
            _ => unreachable!(),
        }
    }

    fn parse_one<const WORD_SIZE: u8>(src: &[u8], dst: &mut Vec<u16>) {
        match WORD_SIZE {
            1 => {
                let word = src[0];
                dst.push(word as u16);
            }
            2 => {
                let word = u16::from_le_bytes([src[0], src[1]]);
                dst.push(word);
            }
            4 => {
                let word = u32::from_le_bytes([src[0], src[1], src[2], src[3]]);
                dst.push(word as u16);
            }
            _ => unreachable!(),
        }
    }

    fn parse_rep_desc_one<const WORD_SIZE: u8>(src: &[u8], max_rep: u16) -> ControlWordDesc {
        match WORD_SIZE {
            1 => ControlWordDesc {
                is_new_row: src[0] as u16 == max_rep,
                is_visible: true,
                is_valid_item: true,
            },
            2 => ControlWordDesc {
                is_new_row: u16::from_le_bytes([src[0], src[1]]) == max_rep,
                is_visible: true,
                is_valid_item: true,
            },
            4 => ControlWordDesc {
                is_new_row: u32::from_le_bytes([src[0], src[1], src[2], src[3]]) as u16 == max_rep,
                is_visible: true,
                is_valid_item: true,
            },
            _ => unreachable!(),
        }
    }

    fn parse_def_desc_one<const WORD_SIZE: u8>(src: &[u8]) -> ControlWordDesc {
        match WORD_SIZE {
            1 => ControlWordDesc {
                is_new_row: true,
                is_visible: true,
                is_valid_item: src[0] == 0,
            },
            2 => ControlWordDesc {
                is_new_row: true,
                is_visible: true,
                is_valid_item: u16::from_le_bytes([src[0], src[1]]) == 0,
            },
            4 => ControlWordDesc {
                is_new_row: true,
                is_visible: true,
                is_valid_item: u32::from_le_bytes([src[0], src[1], src[2], src[3]]) as u16 == 0,
            },
            _ => unreachable!(),
        }
    }

    /// Returns the number of bytes per control word
    pub fn bytes_per_word(&self) -> usize {
        match self {
            Self::BOTH8(..) => 1,
            Self::BOTH16(..) => 2,
            Self::BOTH32(..) => 4,
            Self::REP8 => 1,
            Self::REP16 => 2,
            Self::REP32 => 4,
            Self::DEF8 => 1,
            Self::DEF16 => 2,
            Self::DEF32 => 4,
            Self::NIL => 0,
        }
    }

    /// Appends the next control word to the rep & def buffers
    ///
    /// `src` should be pointing at the first byte (little endian) of the control word
    ///
    /// `dst_rep` and `dst_def` are the buffers to append the rep and def levels to.
    /// They will not be appended to if not needed.
    pub fn parse(&self, src: &[u8], dst_rep: &mut Vec<u16>, dst_def: &mut Vec<u16>) {
        match self {
            Self::BOTH8(bits_to_shift, mask_to_apply) => {
                Self::parse_both::<1>(src, dst_rep, dst_def, *bits_to_shift, *mask_to_apply)
            }
            Self::BOTH16(bits_to_shift, mask_to_apply) => {
                Self::parse_both::<2>(src, dst_rep, dst_def, *bits_to_shift, *mask_to_apply)
            }
            Self::BOTH32(bits_to_shift, mask_to_apply) => {
                Self::parse_both::<4>(src, dst_rep, dst_def, *bits_to_shift, *mask_to_apply)
            }
            Self::REP8 => Self::parse_one::<1>(src, dst_rep),
            Self::REP16 => Self::parse_one::<2>(src, dst_rep),
            Self::REP32 => Self::parse_one::<4>(src, dst_rep),
            Self::DEF8 => Self::parse_one::<1>(src, dst_def),
            Self::DEF16 => Self::parse_one::<2>(src, dst_def),
            Self::DEF32 => Self::parse_one::<4>(src, dst_def),
            Self::NIL => {}
        }
    }

    /// Return true if the control words contain repetition information
    pub fn has_rep(&self) -> bool {
        match self {
            Self::BOTH8(..)
            | Self::BOTH16(..)
            | Self::BOTH32(..)
            | Self::REP8
            | Self::REP16
            | Self::REP32 => true,
            Self::DEF8 | Self::DEF16 | Self::DEF32 | Self::NIL => false,
        }
    }

    /// Temporarily parses the control word to inspect its properties but does not append to any buffers
    pub fn parse_desc(&self, src: &[u8], max_rep: u16, max_visible_def: u16) -> ControlWordDesc {
        match self {
            Self::BOTH8(bits_to_shift, mask_to_apply) => Self::parse_desc_both::<1>(
                src,
                *bits_to_shift,
                *mask_to_apply,
                max_rep,
                max_visible_def,
            ),
            Self::BOTH16(bits_to_shift, mask_to_apply) => Self::parse_desc_both::<2>(
                src,
                *bits_to_shift,
                *mask_to_apply,
                max_rep,
                max_visible_def,
            ),
            Self::BOTH32(bits_to_shift, mask_to_apply) => Self::parse_desc_both::<4>(
                src,
                *bits_to_shift,
                *mask_to_apply,
                max_rep,
                max_visible_def,
            ),
            Self::REP8 => Self::parse_rep_desc_one::<1>(src, max_rep),
            Self::REP16 => Self::parse_rep_desc_one::<2>(src, max_rep),
            Self::REP32 => Self::parse_rep_desc_one::<4>(src, max_rep),
            Self::DEF8 => Self::parse_def_desc_one::<1>(src),
            Self::DEF16 => Self::parse_def_desc_one::<2>(src),
            Self::DEF32 => Self::parse_def_desc_one::<4>(src),
            Self::NIL => ControlWordDesc {
                is_new_row: true,
                is_valid_item: true,
                is_visible: true,
            },
        }
    }

    /// Creates a new parser from the number of bits used for the repetition and definition levels
    pub fn new(bits_rep: u8, bits_def: u8) -> Self {
        let total_bits = bits_rep + bits_def;

        enum WordSize {
            One,
            Two,
            Four,
        }

        let word_size = if total_bits <= 8 {
            WordSize::One
        } else if total_bits <= 16 {
            WordSize::Two
        } else {
            WordSize::Four
        };

        match (bits_rep > 0, bits_def > 0, word_size) {
            (false, false, _) => Self::NIL,
            (false, true, WordSize::One) => Self::DEF8,
            (false, true, WordSize::Two) => Self::DEF16,
            (false, true, WordSize::Four) => Self::DEF32,
            (true, false, WordSize::One) => Self::REP8,
            (true, false, WordSize::Two) => Self::REP16,
            (true, false, WordSize::Four) => Self::REP32,
            (true, true, WordSize::One) => Self::BOTH8(bits_def, get_mask(bits_def as u16) as u32),
            (true, true, WordSize::Two) => Self::BOTH16(bits_def, get_mask(bits_def as u16) as u32),
            (true, true, WordSize::Four) => {
                Self::BOTH32(bits_def, get_mask(bits_def as u16) as u32)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};

    use crate::encodings::logical::primitive::sparse::{
        SparsePositionSet, SparseStructuralLayerPlan, SparseStructuralPlan, SparseValidityMeaning,
        SparseValiditySet,
    };
    use crate::repdef::{
        CompositeRepDefUnraveler, DefinitionInterpretation, RepDefUnraveler, SerializedRepDefs,
    };

    use super::RepDefBuilder;

    fn validity(values: &[bool]) -> NullBuffer {
        NullBuffer::from_iter(values.iter().copied())
    }

    fn offsets_32(values: &[i32]) -> OffsetBuffer<i32> {
        OffsetBuffer::<i32>::new(ScalarBuffer::from_iter(values.iter().copied()))
    }

    fn offsets_64(values: &[i64]) -> OffsetBuffer<i64> {
        OffsetBuffer::<i64>::new(ScalarBuffer::from_iter(values.iter().copied()))
    }

    #[test]
    fn sparse_sibling_validity_mismatch_is_invalid_input() {
        let sparse = |positions| {
            RepDefUnraveler::new_sparse(SparseStructuralPlan {
                layers: vec![SparseStructuralLayerPlan::Validity {
                    num_slots: 2,
                    validity: SparseValiditySet {
                        meaning: SparseValidityMeaning::NullPositions,
                        positions,
                    },
                }],
                num_items: 2,
                num_visible_items: 2,
            })
        };
        let mut repdef = CompositeRepDefUnraveler::new(vec![sparse(SparsePositionSet::Empty)]);
        repdef.add_compatibility_check(CompositeRepDefUnraveler::new(vec![sparse(
            SparsePositionSet::Explicit(vec![0]),
        )]));

        let err = repdef.unravel_validity(2).unwrap_err();
        assert!(matches!(err, lance_core::Error::InvalidInput { .. }));
        assert!(err.to_string().contains("incompatible validity metadata"));
    }

    #[test]
    fn test_repdef_empty_offsets() {
        // Empty offsets should serialize without panicking.
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(offsets_32(&[0]), None);
        let repdefs = RepDefBuilder::serialize(vec![builder]);
        assert!(repdefs.repetition_levels.is_none());
        assert!(repdefs.definition_levels.is_none());
    }

    #[test]
    fn test_repdef_basic() {
        // Basic case, rep & def
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_64(&[0, 2, 2, 5]),
            Some(validity(&[true, false, true])),
        );
        builder.add_offsets(
            offsets_64(&[0, 1, 3, 5, 5, 9]),
            Some(validity(&[true, true, true, false, true])),
        );
        builder.add_validity_bitmap(validity(&[
            true, true, true, false, false, false, true, true, false,
        ]));

        let repdefs = RepDefBuilder::serialize(vec![builder]);
        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!(vec![0, 0, 0, 3, 1, 1, 2, 1, 0, 0, 1], *def);
        assert_eq!(vec![2, 1, 0, 2, 2, 0, 1, 1, 0, 0, 0], *rep);

        // [[I], [I, I]], NULL, [[NULL, NULL], NULL, [NULL, I, I, NULL]]

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            Some(rep.as_ref().to_vec()),
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            9,
        )]);

        // Note: validity doesn't exactly round-trip because repdef normalizes some of the
        // redundant validity values
        assert_eq!(
            unraveler.unravel_validity(9).unwrap(),
            Some(validity(&[
                true, true, true, false, false, false, true, true, false
            ]))
        );
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 1, 3, 5, 5, 9]).inner());
        assert_eq!(val, Some(validity(&[true, true, true, false, true])));
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 2, 2, 5]).inner());
        assert_eq!(val, Some(validity(&[true, false, true])));
    }

    #[test]
    fn test_repdef_simple_null_empty_list() {
        let check = |repdefs: SerializedRepDefs, last_def: DefinitionInterpretation| {
            let rep = repdefs.repetition_levels.unwrap();
            let def = repdefs.definition_levels.unwrap();

            assert_eq!([1, 0, 1, 1, 0, 0], *rep);
            assert_eq!([0, 0, 2, 0, 1, 0], *def);
            assert_eq!(
                vec![DefinitionInterpretation::NullableItem, last_def,],
                repdefs.def_meaning
            );
        };

        // Null list and empty list should be serialized mostly the same

        // Null case
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_32(&[0, 2, 2, 5]),
            Some(validity(&[true, false, true])),
        );
        builder.add_validity_bitmap(validity(&[true, true, true, false, true]));

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        check(repdefs, DefinitionInterpretation::NullableList);

        // Empty case
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(offsets_32(&[0, 2, 2, 5]), None);
        builder.add_validity_bitmap(validity(&[true, true, true, false, true]));

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        check(repdefs, DefinitionInterpretation::EmptyableList);
    }

    #[test]
    fn test_repdef_empty_list_at_end() {
        // Regresses a failure we encountered when the last item was an empty list
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(offsets_32(&[0, 2, 5, 5]), None);
        builder.add_validity_bitmap(validity(&[true, true, true, false, true]));

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([1, 0, 1, 0, 0, 1], *rep);
        assert_eq!([0, 0, 0, 1, 0, 2], *def);
        assert_eq!(
            vec![
                DefinitionInterpretation::NullableItem,
                DefinitionInterpretation::EmptyableList,
            ],
            repdefs.def_meaning
        );
    }

    #[test]
    fn test_repdef_abnormal_nulls() {
        // List nulls are allowed to have non-empty offsets and garbage values
        // and the add_offsets call should normalize this
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_32(&[0, 2, 5, 8]),
            Some(validity(&[true, false, true])),
        );
        // Note: we pass 5 here and not 8.  If add_offsets tells us there is garbage nulls they
        // should be removed before continuing
        builder.add_no_null(5);

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([1, 0, 1, 1, 0, 0], *rep);
        assert_eq!([0, 0, 1, 0, 0, 0], *def);

        assert_eq!(
            vec![
                DefinitionInterpretation::AllValidItem,
                DefinitionInterpretation::NullableList,
            ],
            repdefs.def_meaning
        );
    }

    #[test]
    fn test_repdef_fsl() {
        let mut builder = RepDefBuilder::default();
        builder.add_fsl(Some(validity(&[true, false])), 2, 2);
        builder.add_fsl(None, 2, 4);
        builder.add_validity_bitmap(validity(&[
            true, false, true, false, true, false, true, false,
        ]));

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        assert_eq!(
            vec![
                DefinitionInterpretation::NullableItem,
                DefinitionInterpretation::AllValidItem,
                DefinitionInterpretation::NullableItem
            ],
            repdefs.def_meaning
        );

        assert!(repdefs.repetition_levels.is_none());

        let def = repdefs.definition_levels.unwrap();

        assert_eq!([0, 1, 0, 1, 2, 2, 2, 2], *def);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            None,
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            8,
        )]);

        assert_eq!(
            unraveler.unravel_validity(8).unwrap(),
            Some(validity(&[
                true, false, true, false, false, false, false, false
            ]))
        );
        assert_eq!(unraveler.unravel_fsl_validity(4, 2).unwrap(), None);
        assert_eq!(
            unraveler.unravel_fsl_validity(2, 2).unwrap(),
            Some(validity(&[true, false]))
        );
    }

    #[test]
    fn test_repdef_fsl_allvalid_item() {
        let mut builder = RepDefBuilder::default();
        builder.add_fsl(Some(validity(&[true, false])), 2, 2);
        builder.add_fsl(None, 2, 4);
        builder.add_no_null(8);

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        assert_eq!(
            vec![
                DefinitionInterpretation::AllValidItem,
                DefinitionInterpretation::AllValidItem,
                DefinitionInterpretation::NullableItem
            ],
            repdefs.def_meaning
        );

        assert!(repdefs.repetition_levels.is_none());

        let def = repdefs.definition_levels.unwrap();

        assert_eq!([0, 0, 0, 0, 1, 1, 1, 1], *def);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            None,
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            8,
        )]);

        assert_eq!(unraveler.unravel_validity(8).unwrap(), None);
        assert_eq!(unraveler.unravel_fsl_validity(4, 2).unwrap(), None);
        assert_eq!(
            unraveler.unravel_fsl_validity(2, 2).unwrap(),
            Some(validity(&[true, false]))
        );
    }

    #[test]
    fn test_repdef_sliced_offsets() {
        // Sliced lists may have offsets that don't start with zero.  The
        // add_offsets call needs to normalize these to operate correctly.
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_32(&[5, 7, 7, 10]),
            Some(validity(&[true, false, true])),
        );
        builder.add_no_null(5);

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([1, 0, 1, 1, 0, 0], *rep);
        assert_eq!([0, 0, 1, 0, 0, 0], *def);

        assert_eq!(
            vec![
                DefinitionInterpretation::AllValidItem,
                DefinitionInterpretation::NullableList,
            ],
            repdefs.def_meaning
        );
    }

    #[test]
    fn test_repdef_complex_null_empty() {
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_32(&[0, 4, 4, 4, 6]),
            Some(validity(&[true, false, true, true])),
        );
        builder.add_offsets(
            offsets_32(&[0, 1, 1, 2, 2, 2, 3]),
            Some(validity(&[true, false, true, false, true, true])),
        );
        builder.add_no_null(3);

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([2, 1, 1, 1, 2, 2, 2, 1], *rep);
        assert_eq!([0, 1, 0, 1, 3, 4, 2, 0], *def);
    }

    #[test]
    fn test_repdef_empty_list_no_null() {
        // Tests when we have some empty lists but no null lists.  This case
        // caused some bugs because we have definition but no nulls
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(offsets_32(&[0, 4, 4, 4, 6]), None);
        builder.add_no_null(6);

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([1, 0, 0, 0, 1, 1, 1, 0], *rep);
        assert_eq!([0, 0, 0, 0, 1, 1, 0, 0], *def);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            Some(rep.as_ref().to_vec()),
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            8,
        )]);

        assert_eq!(unraveler.unravel_validity(6).unwrap(), None);
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 4, 4, 4, 6]).inner());
        assert_eq!(val, None);
    }

    #[test]
    fn test_repdef_all_valid() {
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(offsets_64(&[0, 2, 3, 5]), None);
        builder.add_offsets(offsets_64(&[0, 1, 3, 5, 7, 9]), None);
        builder.add_no_null(9);

        let repdefs = RepDefBuilder::serialize(vec![builder]);
        let rep = repdefs.repetition_levels.unwrap();
        assert!(repdefs.definition_levels.is_none());

        assert_eq!([2, 1, 0, 2, 0, 2, 0, 1, 0], *rep);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            Some(rep.as_ref().to_vec()),
            None,
            repdefs.def_meaning.into(),
            9,
        )]);

        assert_eq!(unraveler.unravel_validity(9).unwrap(), None);
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 1, 3, 5, 7, 9]).inner());
        assert_eq!(val, None);
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 2, 3, 5]).inner());
        assert_eq!(val, None);
    }

    #[test]
    fn test_repdef_nested_list_multibatch_matches_single() {
        // Single builder: List<List<i32>>, 3 rows.
        //   outer [0,2,3,5] -> rows have 2,1,2 inner lists
        //   inner [0,1,3,5,7,9] -> 5 inner lists, lengths 1,2,2,2,2 (9 leaf)
        let mut single = RepDefBuilder::default();
        single.add_offsets(offsets_64(&[0, 2, 3, 5]), None);
        single.add_offsets(offsets_64(&[0, 1, 3, 5, 7, 9]), None);
        single.add_no_null(9);
        let single_rep = RepDefBuilder::serialize(vec![single])
            .repetition_levels
            .unwrap();

        // Same logical data split into two batches:
        //   batch0 = rows 0,1 : outer [0,2,3], inner [0,1,3,5] (3 inner, 5 leaf)
        //   batch1 = row 2    : outer [0,2],   inner [0,2,4]   (2 inner, 4 leaf)
        let mut b0 = RepDefBuilder::default();
        b0.add_offsets(offsets_64(&[0, 2, 3]), None);
        b0.add_offsets(offsets_64(&[0, 1, 3, 5]), None);
        b0.add_no_null(5);
        let mut b1 = RepDefBuilder::default();
        b1.add_offsets(offsets_64(&[0, 2]), None);
        b1.add_offsets(offsets_64(&[0, 2, 4]), None);
        b1.add_no_null(4);
        let multi_rep = RepDefBuilder::serialize(vec![b0, b1])
            .repetition_levels
            .unwrap();

        assert_eq!(
            *single_rep, *multi_rep,
            "multi-batch nested-list rep levels must equal single-batch"
        );
    }

    #[test]
    fn test_only_empty_lists() {
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(offsets_32(&[0, 4, 4, 4, 6]), None);
        builder.add_no_null(6);

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([1, 0, 0, 0, 1, 1, 1, 0], *rep);
        assert_eq!([0, 0, 0, 0, 1, 1, 0, 0], *def);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            Some(rep.as_ref().to_vec()),
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            8,
        )]);

        assert_eq!(unraveler.unravel_validity(6).unwrap(), None);
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 4, 4, 4, 6]).inner());
        assert_eq!(val, None);
    }

    #[test]
    fn test_only_null_lists() {
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_32(&[0, 4, 4, 4, 6]),
            Some(validity(&[true, false, false, true])),
        );
        builder.add_no_null(6);

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([1, 0, 0, 0, 1, 1, 1, 0], *rep);
        assert_eq!([0, 0, 0, 0, 1, 1, 0, 0], *def);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            Some(rep.as_ref().to_vec()),
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            8,
        )]);

        assert_eq!(unraveler.unravel_validity(6).unwrap(), None);
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 4, 4, 4, 6]).inner());
        assert_eq!(val, Some(validity(&[true, false, false, true])));
    }

    #[test]
    fn test_null_and_empty_lists() {
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_32(&[0, 4, 4, 4, 6]),
            Some(validity(&[true, false, true, true])),
        );
        builder.add_no_null(6);

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([1, 0, 0, 0, 1, 1, 1, 0], *rep);
        assert_eq!([0, 0, 0, 0, 1, 2, 0, 0], *def);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            Some(rep.as_ref().to_vec()),
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            8,
        )]);

        assert_eq!(unraveler.unravel_validity(6).unwrap(), None);
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 4, 4, 4, 6]).inner());
        assert_eq!(val, Some(validity(&[true, false, true, true])));
    }

    #[test]
    fn test_repdef_null_struct_valid_list() {
        // This regresses a bug

        let rep = vec![1, 0, 0, 0];
        let def = vec![2, 0, 2, 2];
        // AllValidList<NullableStruct<NullableItem>>
        let def_meaning = vec![
            DefinitionInterpretation::NullableItem,
            DefinitionInterpretation::NullableItem,
            DefinitionInterpretation::AllValidList,
        ];
        let num_items = 4;

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            Some(rep),
            Some(def),
            def_meaning.into(),
            num_items,
        )]);

        assert_eq!(
            unraveler.unravel_validity(4).unwrap(),
            Some(validity(&[false, true, false, false]))
        );
        assert_eq!(
            unraveler.unravel_validity(4).unwrap(),
            Some(validity(&[false, true, false, false]))
        );
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 4]).inner());
        assert_eq!(val, None);
    }

    #[test]
    fn test_repdef_no_rep() {
        let mut builder = RepDefBuilder::default();
        builder.add_no_null(5);
        builder.add_validity_bitmap(validity(&[false, false, true, true, true]));
        builder.add_validity_bitmap(validity(&[false, true, true, true, false]));

        let repdefs = RepDefBuilder::serialize(vec![builder]);
        assert!(repdefs.repetition_levels.is_none());
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([2, 2, 0, 0, 1], *def);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            None,
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            5,
        )]);

        assert_eq!(
            unraveler.unravel_validity(5).unwrap(),
            Some(validity(&[false, false, true, true, false]))
        );
        assert_eq!(
            unraveler.unravel_validity(5).unwrap(),
            Some(validity(&[false, false, true, true, true]))
        );
        assert_eq!(unraveler.unravel_validity(5).unwrap(), None);
    }

    #[test]
    fn test_composite_unravel() {
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_64(&[0, 2, 2, 5]),
            Some(validity(&[true, false, true])),
        );
        builder.add_no_null(5);
        let repdef1 = RepDefBuilder::serialize(vec![builder]);

        let mut builder = RepDefBuilder::default();
        builder.add_offsets(offsets_64(&[0, 1, 3, 5, 7, 9]), None);
        builder.add_no_null(9);
        let repdef2 = RepDefBuilder::serialize(vec![builder]);

        let rep1 = repdef1.repetition_levels.clone().unwrap();
        let def1 = repdef1.definition_levels.clone().unwrap();
        let rep2 = repdef2.repetition_levels.clone().unwrap();
        assert!(repdef2.definition_levels.is_none());

        assert_eq!([1, 0, 1, 1, 0, 0], *rep1);
        assert_eq!([0, 0, 1, 0, 0, 0], *def1);
        assert_eq!([1, 1, 0, 1, 0, 1, 0, 1, 0], *rep2);

        let unravel1 = RepDefUnraveler::new(
            repdef1.repetition_levels.map(|l| l.to_vec()),
            repdef1.definition_levels.map(|l| l.to_vec()),
            repdef1.def_meaning.into(),
            5,
        );
        let unravel2 = RepDefUnraveler::new(
            repdef2.repetition_levels.map(|l| l.to_vec()),
            repdef2.definition_levels.map(|l| l.to_vec()),
            repdef2.def_meaning.into(),
            9,
        );

        let mut unraveler = CompositeRepDefUnraveler::new(vec![unravel1, unravel2]);

        assert!(unraveler.unravel_validity(9).unwrap().is_none());
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(
            off.inner(),
            offsets_32(&[0, 2, 2, 5, 6, 8, 10, 12, 14]).inner()
        );
        assert_eq!(
            val,
            Some(validity(&[true, false, true, true, true, true, true, true]))
        );
    }

    #[test]
    fn test_repdef_multiple_builders() {
        // Basic case, rep & def
        let mut builder1 = RepDefBuilder::default();
        builder1.add_offsets(offsets_64(&[0, 2]), None);
        builder1.add_offsets(offsets_64(&[0, 1, 3]), None);
        builder1.add_validity_bitmap(validity(&[true, true, true]));

        let mut builder2 = RepDefBuilder::default();
        builder2.add_offsets(offsets_64(&[0, 0, 3]), Some(validity(&[false, true])));
        builder2.add_offsets(
            offsets_64(&[0, 2, 2, 6]),
            Some(validity(&[true, false, true])),
        );
        builder2.add_validity_bitmap(validity(&[false, false, false, true, true, false]));

        let repdefs = RepDefBuilder::serialize(vec![builder1, builder2]);

        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([2, 1, 0, 2, 2, 0, 1, 1, 0, 0, 0], *rep);
        assert_eq!([0, 0, 0, 3, 1, 1, 2, 1, 0, 0, 1], *def);
    }

    #[test]
    fn test_all_valid_validity_bitmap_serializes_as_no_null() {
        let mut from_bitmap = RepDefBuilder::default();
        from_bitmap.add_validity_bitmap(validity(&[true, true, true, true]));

        let mut from_no_null = RepDefBuilder::default();
        from_no_null.add_no_null(4);

        let from_bitmap = RepDefBuilder::serialize(vec![from_bitmap]);
        let from_no_null = RepDefBuilder::serialize(vec![from_no_null]);

        assert!(from_bitmap.repetition_levels.is_none());
        assert!(from_bitmap.definition_levels.is_none());
        assert_eq!(from_bitmap.def_meaning, from_no_null.def_meaning);
        assert_eq!(
            from_bitmap.max_visible_level,
            from_no_null.max_visible_level
        );
    }

    #[test]
    fn test_slicer() {
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_64(&[0, 2, 2, 30, 30]),
            Some(validity(&[true, false, true, true])),
        );
        builder.add_no_null(30);

        let repdefs = RepDefBuilder::serialize(vec![builder]);

        let mut rep_slicer = repdefs.rep_slicer().unwrap();

        // First 5 items include a null list so we get 6 levels (12 bytes)
        assert_eq!(rep_slicer.slice_next(5).len(), 12);
        // Next 20 are all plain
        assert_eq!(rep_slicer.slice_next(20).len(), 40);
        // Last 5 include an empty list so we get 6 levels (12 bytes)
        assert_eq!(rep_slicer.slice_rest().len(), 12);

        let mut def_slicer = repdefs.rep_slicer().unwrap();

        // First 5 items include a null list so we get 6 levels (12 bytes)
        assert_eq!(def_slicer.slice_next(5).len(), 12);
        // Next 20 are all plain
        assert_eq!(def_slicer.slice_next(20).len(), 40);
        // Last 5 include an empty list so we get 6 levels (12 bytes)
        assert_eq!(def_slicer.slice_rest().len(), 12);
    }

    #[test]
    fn test_control_words() {
        // Convert to control words, verify expected, convert back, verify same as original
        fn check(
            rep: &[u16],
            def: &[u16],
            expected_values: Vec<u8>,
            expected_bytes_per_word: usize,
            expected_bits_rep: u8,
            expected_bits_def: u8,
        ) {
            let num_vals = rep.len().max(def.len());
            let max_rep = rep.iter().max().copied().unwrap_or(0);
            let max_def = def.iter().max().copied().unwrap_or(0);

            let in_rep = if rep.is_empty() { None } else { Some(rep) };
            let in_def = if def.is_empty() { None } else { Some(def) };

            let mut iter = super::build_control_word_iterator(
                in_rep,
                max_rep,
                in_def,
                max_def,
                max_def + 1,
                expected_values.len(),
            );
            assert_eq!(iter.bytes_per_word(), expected_bytes_per_word);
            assert_eq!(iter.bits_rep(), expected_bits_rep);
            assert_eq!(iter.bits_def(), expected_bits_def);
            let mut cw_vec = Vec::with_capacity(num_vals * iter.bytes_per_word());

            for _ in 0..num_vals {
                iter.append_next(&mut cw_vec);
            }
            assert!(iter.append_next(&mut cw_vec).is_none());

            assert_eq!(expected_values, cw_vec);

            let parser = super::ControlWordParser::new(expected_bits_rep, expected_bits_def);

            let mut rep_out = Vec::with_capacity(num_vals);
            let mut def_out = Vec::with_capacity(num_vals);

            if expected_bytes_per_word > 0 {
                for slice in cw_vec.chunks_exact(expected_bytes_per_word) {
                    parser.parse(slice, &mut rep_out, &mut def_out);
                }
            }

            assert_eq!(rep, rep_out.as_slice());
            assert_eq!(def, def_out.as_slice());
        }

        // Each will need 4 bits and so we should get 1-byte control words
        let rep = &[0_u16, 7, 3, 2, 9, 8, 12, 5];
        let def = &[5_u16, 3, 1, 2, 12, 15, 0, 2];
        let expected = vec![
            0b00000101, // 0, 5
            0b01110011, // 7, 3
            0b00110001, // 3, 1
            0b00100010, // 2, 2
            0b10011100, // 9, 12
            0b10001111, // 8, 15
            0b11000000, // 12, 0
            0b01010010, // 5, 2
        ];
        check(rep, def, expected, 1, 4, 4);

        // Now we need 5 bits for def so we get 2-byte control words
        let rep = &[0_u16, 7, 3, 2, 9, 8, 12, 5];
        let def = &[5_u16, 3, 1, 2, 12, 22, 0, 2];
        let expected = vec![
            0b00000101, 0b00000000, // 0, 5
            0b11100011, 0b00000000, // 7, 3
            0b01100001, 0b00000000, // 3, 1
            0b01000010, 0b00000000, // 2, 2
            0b00101100, 0b00000001, // 9, 12
            0b00010110, 0b00000001, // 8, 22
            0b10000000, 0b00000001, // 12, 0
            0b10100010, 0b00000000, // 5, 2
        ];
        check(rep, def, expected, 2, 4, 5);

        // Just rep, 4 bits so 1 byte each
        let levels = &[0_u16, 7, 3, 2, 9, 8, 12, 5];
        let expected = vec![
            0b00000000, // 0
            0b00000111, // 7
            0b00000011, // 3
            0b00000010, // 2
            0b00001001, // 9
            0b00001000, // 8
            0b00001100, // 12
            0b00000101, // 5
        ];
        check(levels, &[], expected.clone(), 1, 4, 0);

        // Just def
        check(&[], levels, expected, 1, 0, 4);

        // No rep, no def, no bytes
        check(&[], &[], Vec::default(), 0, 0, 0);
    }

    #[test]
    fn test_control_words_rep_index() {
        fn check(
            rep: &[u16],
            def: &[u16],
            expected_new_rows: Vec<bool>,
            expected_is_visible: Vec<bool>,
        ) {
            let num_vals = rep.len().max(def.len());
            let max_rep = rep.iter().max().copied().unwrap_or(0);
            let max_def = def.iter().max().copied().unwrap_or(0);

            let in_rep = if rep.is_empty() { None } else { Some(rep) };
            let in_def = if def.is_empty() { None } else { Some(def) };

            let mut iter = super::build_control_word_iterator(
                in_rep,
                max_rep,
                in_def,
                max_def,
                /*max_visible_def=*/ 2,
                expected_new_rows.len(),
            );

            let mut cw_vec = Vec::with_capacity(num_vals * iter.bytes_per_word());
            let mut expected_new_rows = expected_new_rows.iter().copied();
            let mut expected_is_visible = expected_is_visible.iter().copied();
            for _ in 0..expected_new_rows.len() {
                let word_desc = iter.append_next(&mut cw_vec).unwrap();
                assert_eq!(word_desc.is_new_row, expected_new_rows.next().unwrap());
                assert_eq!(word_desc.is_visible, expected_is_visible.next().unwrap());
            }
            assert!(iter.append_next(&mut cw_vec).is_none());
        }

        // 2 means new list
        let rep = &[2_u16, 1, 0, 2, 2, 0, 1, 1, 0, 2, 0];
        // These values don't matter for this test
        let def = &[0_u16, 0, 0, 3, 1, 1, 2, 1, 0, 0, 1];

        // Rep & def
        check(
            rep,
            def,
            vec![
                true, false, false, true, true, false, false, false, false, true, false,
            ],
            vec![
                true, true, true, false, true, true, true, true, true, true, true,
            ],
        );
        // Rep only
        check(
            rep,
            &[],
            vec![
                true, false, false, true, true, false, false, false, false, true, false,
            ],
            vec![true; 11],
        );
        // No repetition
        check(
            &[],
            def,
            vec![
                true, true, true, true, true, true, true, true, true, true, true,
            ],
            vec![true; 11],
        );
        // No repetition, no definition
        check(
            &[],
            &[],
            vec![
                true, true, true, true, true, true, true, true, true, true, true,
            ],
            vec![true; 11],
        );
    }

    #[test]
    fn regress_empty_list_case() {
        // This regresses a case where we had 3 null lists inside a struct
        let mut builder = RepDefBuilder::default();
        builder.add_validity_bitmap(validity(&[true, false, true]));
        builder.add_offsets(
            offsets_32(&[0, 0, 0, 0]),
            Some(validity(&[false, false, false])),
        );
        builder.add_no_null(0);

        let repdefs = RepDefBuilder::serialize(vec![builder]);
        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([1, 1, 1], *rep);
        assert_eq!([1, 2, 1], *def);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            Some(rep.as_ref().to_vec()),
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            0,
        )]);

        assert_eq!(unraveler.unravel_validity(0).unwrap(), None);
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 0, 0, 0]).inner());
        assert_eq!(val, Some(validity(&[false, false, false])));
        let val = unraveler.unravel_validity(3).unwrap().unwrap();
        assert_eq!(val.inner(), validity(&[true, false, true]).inner());
    }

    #[test]
    fn regress_list_ends_null_case() {
        let mut builder = RepDefBuilder::default();
        builder.add_offsets(
            offsets_64(&[0, 1, 2, 2]),
            Some(validity(&[true, true, false])),
        );
        builder.add_offsets(offsets_64(&[0, 1, 1]), Some(validity(&[true, false])));
        builder.add_no_null(1);

        let repdefs = RepDefBuilder::serialize(vec![builder]);
        let rep = repdefs.repetition_levels.unwrap();
        let def = repdefs.definition_levels.unwrap();

        assert_eq!([2, 2, 2], *rep);
        assert_eq!([0, 1, 2], *def);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![RepDefUnraveler::new(
            Some(rep.as_ref().to_vec()),
            Some(def.as_ref().to_vec()),
            repdefs.def_meaning.into(),
            1,
        )]);

        assert_eq!(unraveler.unravel_validity(1).unwrap(), None);
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 1, 1]).inner());
        assert_eq!(val, Some(validity(&[true, false])));
        let (off, val) = unraveler.unravel_offsets::<i32>().unwrap();
        assert_eq!(off.inner(), offsets_32(&[0, 1, 2, 2]).inner());
        assert_eq!(val, Some(validity(&[true, true, false])));
    }

    #[test]
    fn test_mixed_unraveler() {
        // This tests cases where the validity is different between two different pages
        // because one page has nulls and the other doesn't.

        // Simple case with one layer of validity and no repetition
        let mut unraveler = CompositeRepDefUnraveler::new(vec![
            RepDefUnraveler::new(
                None,
                Some(vec![0, 1, 0, 1]),
                vec![DefinitionInterpretation::NullableItem].into(),
                4,
            ),
            RepDefUnraveler::new(
                None,
                None,
                vec![DefinitionInterpretation::AllValidItem].into(),
                4,
            ),
        ]);

        assert_eq!(
            unraveler.unravel_validity(8).unwrap(),
            Some(validity(&[
                true, false, true, false, true, true, true, true
            ]))
        );

        // More complex case with two layers of validity and repetition
        let def1 = Some(vec![0, 1, 2]);
        let rep1 = Some(vec![1, 0, 1]);

        let def2 = Some(vec![1, 0, 0]);
        let rep2 = Some(vec![1, 1, 0]);

        let mut unraveler = CompositeRepDefUnraveler::new(vec![
            RepDefUnraveler::new(
                rep1,
                def1,
                vec![
                    DefinitionInterpretation::NullableItem,
                    DefinitionInterpretation::EmptyableList,
                ]
                .into(),
                2,
            ),
            RepDefUnraveler::new(
                rep2,
                def2,
                vec![
                    DefinitionInterpretation::AllValidItem,
                    DefinitionInterpretation::NullableList,
                ]
                .into(),
                2,
            ),
        ]);

        assert_eq!(
            unraveler.unravel_validity(4).unwrap(),
            Some(validity(&[true, false, true, true]))
        );
        assert_eq!(
            unraveler.unravel_offsets::<i32>().unwrap(),
            (
                offsets_32(&[0, 2, 2, 2, 4]),
                Some(validity(&[true, true, false, true]))
            )
        );
    }

    #[test]
    fn test_mixed_unraveler_nullable_without_def_levels() {
        // A page can keep nullable layer metadata even when all definition levels are 0
        // and no definition buffer needs to be materialized. This should decode as all-valid.
        let mut unraveler = CompositeRepDefUnraveler::new(vec![
            RepDefUnraveler::new(
                None,
                Some(vec![0, 1, 0, 1]),
                vec![DefinitionInterpretation::NullableItem].into(),
                4,
            ),
            RepDefUnraveler::new(
                None,
                None,
                vec![DefinitionInterpretation::NullableItem].into(),
                4,
            ),
        ]);

        assert_eq!(
            unraveler.unravel_validity(8).unwrap(),
            Some(validity(&[
                true, false, true, false, true, true, true, true
            ]))
        );
    }
}
