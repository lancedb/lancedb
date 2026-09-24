// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    any::Any,
    collections::{HashMap, VecDeque},
    env,
    fmt::Debug,
    iter,
    ops::Range,
    sync::Arc,
    vec,
};

use crate::{
    constants::{
        STRUCTURAL_ENCODING_FULLZIP, STRUCTURAL_ENCODING_META_KEY, STRUCTURAL_ENCODING_MINIBLOCK,
        STRUCTURAL_ENCODING_SPARSE,
    },
    data::DictionaryDataBlock,
    encodings::logical::primitive::blob::{BlobDescriptionPageScheduler, BlobPageScheduler},
    format::{
        ProtobufUtils21,
        pb21::{self, CompressiveEncoding, PageLayout, compressive_encoding::Compression},
    },
};
use arrow_array::{
    Array, ArrayRef, PrimitiveArray, cast::AsArray, make_array, new_null_array, types::UInt64Type,
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, NullBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field as ArrowField};
use bytes::Bytes;
use futures::{FutureExt, TryStreamExt, future::BoxFuture, stream::FuturesOrdered};
use itertools::Itertools;
use lance_arrow::DataTypeExt;
use lance_arrow::deepcopy::deep_copy_nulls;
use lance_core::{
    cache::{CacheKey, CacheKeySchema, Context, DeepSizeOf, KeyBuilder, LanceCache},
    error::{Error, LanceOptionExt},
    utils::bit::pad_bytes,
};
use log::{debug, trace};

use crate::encodings::logical::primitive::miniblock::MiniBlockChunk;
use crate::encodings::physical::rle::{RleDecompressor, RleRuns};
use crate::utils::bytepack::ByteUnpacker;
use crate::{
    compression::{
        BlockDecompressor, CompressionStrategy, DecompressionStrategy, MiniBlockDecompressor,
        compress_required_block, create_rle_decompressor,
    },
    data::{AllNullDataBlock, DataBlock, VariableWidthBlock},
    decoder::{IO_REQUEST_BATCH_BYTES, RequestBatch},
    utils::bytepack::BytepackedIntegerEncoder,
};
use crate::{
    compression::{FixedPerValueDecompressor, VariablePerValueDecompressor},
    encodings::logical::primitive::fullzip::PerValueDataBlock,
};
use crate::{
    encodings::logical::primitive::miniblock::{MiniBlockCompressed, MiniBlockCompressionContext},
    statistics::{ComputeStat, GetStat, Stat},
};
use crate::{
    repdef::{
        CompositeRepDefUnraveler, ControlWordIterator, ControlWordParser, DefinitionInterpretation,
        MiniBlockRepDefBudget, NormalizedStructuralPlan, RepDefSlicer, SerializedRepDefs,
        build_control_word_iterator, max_visible_level,
    },
    utils::accumulation::AccumulationQueue,
};
use lance_core::{Result, datatypes::Field, utils::tokio::spawn_cpu};

use crate::constants::{
    COMPRESSION_LEVEL_META_KEY, COMPRESSION_META_KEY, DICT_DIVISOR_META_KEY,
    DICT_SIZE_RATIO_META_KEY, DICT_VALUES_COMPRESSION_ENV_VAR,
    DICT_VALUES_COMPRESSION_LEVEL_ENV_VAR, DICT_VALUES_COMPRESSION_LEVEL_META_KEY,
    DICT_VALUES_COMPRESSION_META_KEY,
};
use crate::{
    EncodingsIo,
    buffer::LanceBuffer,
    data::{BlockInfo, DataBlockBuilder, FixedWidthDataBlock},
    decoder::{
        ColumnInfo, DecodePageTask, DecodedArray, DecodedPage, FilterExpression, LoadedPageShard,
        MessageType, PageEncoding, PageInfo, ScheduledScanLine, SchedulerContext,
        StructuralDecodeArrayTask, StructuralFieldDecoder, StructuralFieldScheduler,
        StructuralPageDecoder, StructuralSchedulingJob, UnloadedPageShard,
    },
    encoder::{
        EncodeTask, EncodedColumn, EncodedPage, EncodingOptions, FieldEncoder, OutOfLineBuffers,
    },
    repdef::{LevelBuffer, RepDefBuilder, RepDefUnraveler},
};

pub mod blob;
mod chunk_index;
pub mod constant;
pub mod dict;
pub mod fullzip;
mod layout;
pub mod miniblock;
pub(crate) mod sparse;

use chunk_index::{ItemCounts, MiniBlockChunkIndex, PrefixSums, RowMapping, parse_nested_rep};

const FILL_BYTE: u8 = 0xFE;
const DEFAULT_DICT_DIVISOR: u64 = 2;
const DEFAULT_DICT_MAX_CARDINALITY: u64 = 100_000;
const DEFAULT_DICT_SIZE_RATIO: f64 = 0.8;
const DEFAULT_DICT_VALUES_COMPRESSION: &str = "lz4";

/// Largest level count a direct legacy u16-value/U8-run RLE frame can prove.
///
/// Mini-block level payload sizes are u16. After the eight-byte frame header, each run needs a
/// two-byte value and a one-byte length, and each U8 run can represent at most 255 levels.
const MAX_LEGACY_RLE_LEVELS: u64 = ((u16::MAX as u64 - std::mem::size_of::<u64>() as u64)
    / (std::mem::size_of::<u16>() as u64 + std::mem::size_of::<u8>() as u64))
    * u8::MAX as u64;

struct PageLoadTask {
    decoder_fut: BoxFuture<'static, Result<Box<dyn StructuralPageDecoder>>>,
    num_rows: u64,
}

/// The ordered initialization-buffer slots for one page.
///
/// Optional slots preserve the semantic position of absent buffers so range
/// declaration and buffer consumption share one representation.
#[derive(Debug)]
struct PageInitialization {
    buffer_ranges: Vec<Option<Range<u64>>>,
}

impl PageInitialization {
    fn new(buffer_ranges: impl IntoIterator<Item = Option<Range<u64>>>) -> Self {
        Self {
            buffer_ranges: buffer_ranges.into_iter().collect(),
        }
    }

    fn required_ranges(&self) -> impl Iterator<Item = &Range<u64>> {
        self.buffer_ranges.iter().flatten()
    }

    fn num_required_ranges(&self) -> usize {
        self.required_ranges().count()
    }

    fn with_buffers(self, buffers: Vec<Bytes>) -> Result<PageInitializationBuffers> {
        let expected = self.num_required_ranges();
        if buffers.len() != expected {
            return Err(Error::invalid_input_source(
                format!(
                    "Page initialization received {} buffers for {expected} declared ranges",
                    buffers.len()
                )
                .into(),
            ));
        }

        let mut buffers = buffers.into_iter();
        let mut buffer_slots = Vec::with_capacity(self.buffer_ranges.len());
        for range in self.buffer_ranges {
            if range.is_some() {
                let buffer = buffers.next().ok_or_else(|| {
                    Error::internal("Page initialization buffer was not assigned")
                })?;
                buffer_slots.push(Some(buffer));
            } else {
                buffer_slots.push(None);
            }
        }
        debug_assert!(
            buffers.next().is_none(),
            "validated page initialization buffer count must be exhausted"
        );
        Ok(PageInitializationBuffers { buffer_slots })
    }
}

#[derive(Debug)]
struct PageInitializationBuffers {
    buffer_slots: Vec<Option<Bytes>>,
}

impl PageInitializationBuffers {
    fn try_into_array<const N: usize>(self, layout: &str) -> Result<[Option<Bytes>; N]> {
        let actual = self.buffer_slots.len();
        self.buffer_slots.try_into().map_err(|_| {
            Error::internal(format!(
                "{layout} page initialization declared {actual} buffer slots, expected {N}"
            ))
        })
    }

    fn into_required(self, layout: &str) -> Result<Vec<Bytes>> {
        self.buffer_slots
            .into_iter()
            .enumerate()
            .map(|(index, buffer)| {
                buffer.ok_or_else(|| {
                    Error::internal(format!(
                        "{layout} page initialization buffer slot {index} is absent"
                    ))
                })
            })
            .collect()
    }
}

/// A trait for figuring out how to schedule the data within
/// a single page.
trait StructuralPageScheduler: std::fmt::Debug + Send {
    /// Whether this page requires initialized metadata before scheduling.
    fn needs_initialization(&self) -> bool {
        true
    }

    /// Describes the file byte ranges this page must read to initialize its
    /// metadata (chunk metadata, dictionary, repetition index, ...).
    ///
    /// The field scheduler sorts the combined I/O request by file offset and
    /// restores buffers to the declared slots afterward.
    fn init_layout(&self) -> Result<PageInitialization>;

    /// Returns initialized state from the bytes declared by [`Self::init_layout`].
    ///
    /// The caller attaches the returned state with [`Self::try_load`].
    fn init_from_buffers<'a>(
        &'a mut self,
        buffers: PageInitializationBuffers,
        io: &Arc<dyn EncodingsIo>,
    ) -> BoxFuture<'a, Result<Arc<dyn CachedPageData>>>;

    /// Tries to attach metadata from a previous initialization.
    fn try_load(&mut self, data: &Arc<dyn CachedPageData>) -> Result<()>;

    // Identifies the requested decode shape (e.g. blob descriptor struct vs
    // raw bytes). Blob columns can produce multiple page scheduler variants
    // for the same physical column depending on the target field's data type,
    // and the cached page state types differ per variant. The stable view is
    // mixed into the cache key so different variants do not collide.
    fn cache_view(&self) -> PageCacheView {
        PageCacheView::Native
    }

    /// Schedules the read of the given ranges in the page
    ///
    /// The read may be split into multiple "shards" if the page is extremely large.
    /// Each shard maps to one or more rows and can be decoded independently.
    ///
    /// Note: this sharding is for splitting up very large pages into smaller reads to
    /// avoid buffering too much data in memory.  It is not related to the batch size or
    /// compute units in any way.
    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>>;
}

/// Metadata describing the decoded size of a mini-block
#[derive(Debug)]
struct ChunkMeta {
    num_values: u64,
    chunk_size_bytes: u64,
    offset_bytes: u64,
}

/// A mini-block chunk that has been decoded and decompressed
#[derive(Debug, Clone)]
struct DecodedMiniBlockChunk {
    rep: Option<ScalarBuffer<u16>>,
    def: Option<ScalarBuffer<u16>>,
    values: DataBlock,
}

/// A task to decode a one or more mini-blocks of data into an output batch
///
/// Note: Two batches might share the same mini-block of data.  When this happens
/// then each batch gets a copy of the block and each batch decodes the block independently.
///
/// This means we have duplicated work but it is necessary to avoid having to synchronize
/// the decoding of the block. (TODO: test this theory)
#[derive(Debug)]
struct DecodeMiniBlockTask {
    rep_decompressor: Option<Arc<dyn BlockDecompressor>>,
    def_decompressor: Option<Arc<dyn BlockDecompressor>>,
    value_decompressor: Arc<dyn MiniBlockDecompressor>,
    dictionary_data: Option<Arc<DataBlock>>,
    def_meaning: Arc<[DefinitionInterpretation]>,
    num_buffers: u64,
    max_visible_level: u16,
    instructions: Vec<(ChunkDrainInstructions, LoadedChunk)>,
    has_large_chunk: bool,
}

impl DecodeMiniBlockTask {
    fn decoded_size_bytes(&self) -> Option<u64> {
        if self.rep_decompressor.is_some() || self.def_decompressor.is_some() {
            return None;
        }
        let num_values = self
            .instructions
            .iter()
            .try_fold(0_u64, |total, (instruction, _)| {
                total.checked_add(instruction.rows_to_take)
            })?;
        self.value_decompressor.decoded_size_bytes(num_values)
    }

    fn resolve_num_levels(
        rep_decompressor: Option<&dyn BlockDecompressor>,
        rep_levels: Option<&LanceBuffer>,
        def_decompressor: Option<&dyn BlockDecompressor>,
        def_levels: Option<&LanceBuffer>,
        declared_num_levels: u16,
    ) -> Result<u64> {
        let rep_num_levels = match (rep_decompressor, rep_levels) {
            (Some(decompressor), Some(levels)) => decompressor.infer_num_values(levels)?,
            (None, None) => None,
            _ => {
                return Err(Error::invalid_input_source(
                    "miniblock repetition codec and payload presence disagree".into(),
                ));
            }
        };
        let def_num_levels = match (def_decompressor, def_levels) {
            (Some(decompressor), Some(levels)) => decompressor.infer_num_values(levels)?,
            (None, None) => None,
            _ => {
                return Err(Error::invalid_input_source(
                    "miniblock definition codec and payload presence disagree".into(),
                ));
            }
        };

        let inferred_num_levels = match (rep_num_levels, def_num_levels) {
            (Some(rep_num_levels), Some(def_num_levels)) if rep_num_levels != def_num_levels => {
                return Err(Error::invalid_input_source(
                    format!(
                        "miniblock structural streams disagree on the level count: repetition inferred {rep_num_levels}, definition inferred {def_num_levels}"
                    )
                    .into(),
                ));
            }
            (Some(num_levels), _) | (_, Some(num_levels)) => num_levels,
            (None, None) => return Ok(u64::from(declared_num_levels)),
        };

        let declared_num_levels = u64::from(declared_num_levels);
        if inferred_num_levels == declared_num_levels {
            return Ok(inferred_num_levels);
        }
        let u16_modulus = u64::from(u16::MAX) + 1;
        if inferred_num_levels <= declared_num_levels
            || inferred_num_levels % u16_modulus != declared_num_levels
        {
            return Err(Error::invalid_input_source(
                format!(
                    "miniblock payload proves {inferred_num_levels} levels but the header declared {declared_num_levels}; the counts are not congruent modulo 65536"
                )
                .into(),
            ));
        }
        if inferred_num_levels > MAX_LEGACY_RLE_LEVELS {
            return Err(Error::invalid_input_source(
                format!(
                    "miniblock payload proves {inferred_num_levels} levels, exceeding the legacy RLE payload bound of {MAX_LEGACY_RLE_LEVELS}"
                )
                .into(),
            ));
        }
        Ok(inferred_num_levels)
    }

    fn decode_levels(
        decompressor: &dyn BlockDecompressor,
        levels: LanceBuffer,
        expected_num_levels: u64,
    ) -> Result<ScalarBuffer<u16>> {
        let levels = decompressor.decompress(Some(levels), expected_num_levels)?;
        let levels = levels.as_fixed_width().ok_or_else(|| {
            Error::invalid_input_source(
                "miniblock levels did not decode to fixed-width data".into(),
            )
        })?;
        if levels.num_values != expected_num_levels {
            return Err(Error::invalid_input_source(
                format!(
                    "miniblock levels decoded {} values, expected {expected_num_levels}",
                    levels.num_values
                )
                .into(),
            ));
        }
        if levels.bits_per_value != 16 {
            return Err(Error::invalid_input_source(
                format!(
                    "miniblock levels decoded {} bits per value, expected 16",
                    levels.bits_per_value
                )
                .into(),
            ));
        }
        let expected_bytes = expected_num_levels.checked_mul(2).ok_or_else(|| {
            Error::invalid_input_source(
                format!("miniblock level byte count overflowed for {expected_num_levels} levels")
                    .into(),
            )
        })?;
        if levels.data.len() as u64 != expected_bytes {
            return Err(Error::invalid_input_source(
                format!(
                    "miniblock levels decoded {} bytes, expected {expected_bytes}",
                    levels.data.len()
                )
                .into(),
            ));
        }
        Ok(levels.data.borrow_to_typed_slice::<u16>())
    }

    // We are building a LevelBuffer (levels) and want to copy into it `total_len`
    // values from `level_buf` starting at `offset`.
    //
    // We need to handle both the case where `levels` is None (no nulls encountered
    // yet) and the case where `level_buf` is None (the input we are copying from has
    // no nulls)
    fn extend_levels(
        range: Range<u64>,
        levels: &mut Option<LevelBuffer>,
        level_buf: &Option<impl AsRef<[u16]>>,
        dest_offset: usize,
    ) {
        if let Some(level_buf) = level_buf {
            if levels.is_none() {
                // This is the first non-empty def buf we've hit, fill in the past
                // with 0 (valid)
                let mut new_levels_vec =
                    LevelBuffer::with_capacity(dest_offset + (range.end - range.start) as usize);
                new_levels_vec.extend(iter::repeat_n(0, dest_offset));
                *levels = Some(new_levels_vec);
            }
            levels.as_mut().unwrap().extend(
                level_buf.as_ref()[range.start as usize..range.end as usize]
                    .iter()
                    .copied(),
            );
        } else if let Some(levels) = levels {
            let num_values = (range.end - range.start) as usize;
            // This is an all-valid level_buf but we had nulls earlier and so we
            // need to materialize it
            levels.extend(iter::repeat_n(0, num_values));
        }
    }

    /// Maps a range of rows to a range of items and a range of levels
    ///
    /// If there is no repetition information this just returns the range as-is.
    ///
    /// If there is repetition information then we need to do some work to figure out what
    /// range of items corresponds to the requested range of rows.
    ///
    /// For example, if the data is [[1, 2, 3], [4, 5], [6, 7]] and the range is 1..2 (i.e. just row
    /// 1) then the user actually wants items 3..5.  In the above case the rep levels would be:
    ///
    /// Idx: 0 1 2 3 4 5 6
    /// Rep: 1 0 0 1 0 1 0
    ///
    /// So the start (1) maps to the second 1 (idx=3) and the end (2) maps to the third 1 (idx=5)
    ///
    /// If there are invisible items then we don't count them when calculating the range of items we
    /// are interested in but we do count them when calculating the range of levels we are interested
    /// in.  As a result we have to return both the item range (first return value) and the level range
    /// (second return value).
    ///
    /// For example, if the data is [[1, 2, 3], [4, 5], NULL, [6, 7, 8]] and the range is 2..4 then the
    /// user wants items 5..8 but they want levels 5..9.  In the above case the rep/def levels would be:
    ///
    /// Idx: 0 1 2 3 4 5 6 7 8
    /// Rep: 1 0 0 1 0 1 1 0 0
    /// Def: 0 0 0 0 0 1 0 0 0
    /// Itm: 1 2 3 4 5 6 7 8
    ///
    /// Finally, we have to contend with the fact that chunks may or may not start with a "preamble" of
    /// trailing values that finish up a list from the previous chunk.  In this case the first item does
    /// not start at max_rep because it is a continuation of the previous chunk.  For our purposes we do
    /// not consider this a "row" and so the range 0..1 will refer to the first row AFTER the preamble.
    ///
    /// We have a separate parameter (`preamble_action`) to control whether we want the preamble or not.
    ///
    /// Note that the "trailer" is considered a "row" and if we want it we should include it in the range.
    fn map_range(
        range: Range<u64>,
        rep: Option<&impl AsRef<[u16]>>,
        def: Option<&impl AsRef<[u16]>>,
        max_rep: u16,
        max_visible_def: u16,
        // The total number of items (not rows) in the chunk.  This is not quite the same as
        // rep.len() / def.len() because it doesn't count invisible items
        total_items: u64,
        preamble_action: PreambleAction,
    ) -> (Range<u64>, Range<u64>) {
        if let Some(rep) = rep {
            let mut rep = rep.as_ref();
            // If there is a preamble and we need to skip it then do that first.  The work is the same
            // whether there is def information or not
            let mut items_in_preamble = 0_u64;
            let first_row_start = match preamble_action {
                PreambleAction::Skip | PreambleAction::Take => {
                    let first_row_start = if let Some(def) = def.as_ref() {
                        let mut first_row_start = None;
                        for (idx, (rep, def)) in rep.iter().zip(def.as_ref()).enumerate() {
                            if *rep == max_rep {
                                first_row_start = Some(idx as u64);
                                break;
                            }
                            if *def <= max_visible_def {
                                items_in_preamble += 1;
                            }
                        }
                        first_row_start
                    } else {
                        let first_row_start =
                            rep.iter().position(|&r| r == max_rep).map(|r| r as u64);
                        items_in_preamble = first_row_start.unwrap_or(rep.len() as u64);
                        first_row_start
                    };
                    // It is possible for a chunk to be entirely partial values but if it is then it
                    // should never show up as a preamble to skip
                    if first_row_start.is_none() {
                        assert!(preamble_action == PreambleAction::Take);
                        return (0..total_items, 0..rep.len() as u64);
                    }
                    let first_row_start = first_row_start.unwrap();
                    rep = &rep[first_row_start as usize..];
                    first_row_start
                }
                PreambleAction::Absent => {
                    debug_assert!(rep[0] == max_rep);
                    0
                }
            };

            // We hit this case when all we needed was the preamble
            if range.start == range.end {
                debug_assert!(preamble_action == PreambleAction::Take);
                debug_assert!(items_in_preamble <= total_items);
                return (0..items_in_preamble, 0..first_row_start);
            }
            assert!(range.start < range.end);

            let mut rows_seen = 0;
            let mut new_start = 0;
            let mut new_levels_start = 0;

            if let Some(def) = def {
                let def = &def.as_ref()[first_row_start as usize..];

                // range.start == 0 always maps to 0 (even with invis items), otherwise we need to walk
                let mut lead_invis_seen = 0;

                if range.start > 0 {
                    if def[0] > max_visible_def {
                        lead_invis_seen += 1;
                    }
                    for (idx, (rep, def)) in rep.iter().zip(def).skip(1).enumerate() {
                        if *rep == max_rep {
                            rows_seen += 1;
                            if rows_seen == range.start {
                                new_start = idx as u64 + 1 - lead_invis_seen;
                                new_levels_start = idx as u64 + 1;
                                break;
                            }
                        }
                        if *def > max_visible_def {
                            lead_invis_seen += 1;
                        }
                    }
                }

                rows_seen += 1;

                let mut new_end = u64::MAX;
                let mut new_levels_end = rep.len() as u64;
                let new_start_is_visible = def[new_levels_start as usize] <= max_visible_def;
                let mut tail_invis_seen = if new_start_is_visible { 0 } else { 1 };
                for (idx, (rep, def)) in rep[(new_levels_start + 1) as usize..]
                    .iter()
                    .zip(&def[(new_levels_start + 1) as usize..])
                    .enumerate()
                {
                    if *rep == max_rep {
                        rows_seen += 1;
                        if rows_seen == range.end + 1 {
                            new_end = idx as u64 + new_start + 1 - tail_invis_seen;
                            new_levels_end = idx as u64 + new_levels_start + 1;
                            break;
                        }
                    }
                    if *def > max_visible_def {
                        tail_invis_seen += 1;
                    }
                }

                if new_end == u64::MAX {
                    new_levels_end = rep.len() as u64;
                    let total_invis_seen = lead_invis_seen + tail_invis_seen;
                    new_end = rep.len() as u64 - total_invis_seen;
                }

                assert_ne!(new_end, u64::MAX);

                // Adjust for any skipped preamble
                if preamble_action == PreambleAction::Skip {
                    new_start += items_in_preamble;
                    new_end += items_in_preamble;
                    new_levels_start += first_row_start;
                    new_levels_end += first_row_start;
                } else if preamble_action == PreambleAction::Take {
                    debug_assert_eq!(new_start, 0);
                    debug_assert_eq!(new_levels_start, 0);
                    new_end += items_in_preamble;
                    new_levels_end += first_row_start;
                }

                debug_assert!(new_end <= total_items);
                (new_start..new_end, new_levels_start..new_levels_end)
            } else {
                // Easy case, there are no invisible items, so we don't need to check for them
                // The items range and levels range will be the same.  We do still need to walk
                // the rep levels to find the row boundaries

                // range.start == 0 always maps to 0, otherwise we need to walk
                if range.start > 0 {
                    for (idx, rep) in rep.iter().skip(1).enumerate() {
                        if *rep == max_rep {
                            rows_seen += 1;
                            if rows_seen == range.start {
                                new_start = idx as u64 + 1;
                                break;
                            }
                        }
                    }
                }
                let mut new_end = rep.len() as u64;
                // range.end == max_items always maps to rep.len(), otherwise we need to walk
                if range.end < total_items {
                    for (idx, rep) in rep[(new_start + 1) as usize..].iter().enumerate() {
                        if *rep == max_rep {
                            rows_seen += 1;
                            if rows_seen == range.end {
                                new_end = idx as u64 + new_start + 1;
                                break;
                            }
                        }
                    }
                }

                // Adjust for any skipped preamble
                if preamble_action == PreambleAction::Skip {
                    new_start += first_row_start;
                    new_end += first_row_start;
                } else if preamble_action == PreambleAction::Take {
                    debug_assert_eq!(new_start, 0);
                    new_end += first_row_start;
                }

                debug_assert!(new_end <= total_items);
                (new_start..new_end, new_start..new_end)
            }
        } else {
            // No repetition info, easy case, just use the range as-is and the item
            // and level ranges are the same
            (range.clone(), range)
        }
    }

    // read `num_buffers` buffer sizes from `buf` starting at `offset`
    fn read_buffer_sizes<const LARGE: bool>(
        buf: &[u8],
        offset: &mut usize,
        num_buffers: u64,
    ) -> Vec<u32> {
        let read_size = if LARGE { 4 } else { 2 };
        (0..num_buffers)
            .map(|_| {
                let bytes = &buf[*offset..*offset + read_size];
                let size = if LARGE {
                    u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
                } else {
                    // the buffer size is read from u16 but is stored as u32 after decoding for consistency
                    u16::from_le_bytes([bytes[0], bytes[1]]) as u32
                };
                *offset += read_size;
                size
            })
            .collect()
    }

    // Unserialize a miniblock into a collection of vectors
    fn decode_miniblock_chunk(
        &self,
        buf: &LanceBuffer,
        items_in_chunk: u64,
    ) -> Result<DecodedMiniBlockChunk> {
        let mut offset = 0;
        let num_levels = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
        offset += 2;

        let rep_size = if self.rep_decompressor.is_some() {
            let rep_size = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
            offset += 2;
            Some(rep_size)
        } else {
            None
        };
        let def_size = if self.def_decompressor.is_some() {
            let def_size = u16::from_le_bytes([buf[offset], buf[offset + 1]]);
            offset += 2;
            Some(def_size)
        } else {
            None
        };

        let buffer_sizes = if self.has_large_chunk {
            Self::read_buffer_sizes::<true>(buf, &mut offset, self.num_buffers)
        } else {
            Self::read_buffer_sizes::<false>(buf, &mut offset, self.num_buffers)
        };

        offset += pad_bytes::<MINIBLOCK_ALIGNMENT>(offset);

        let rep = rep_size.map(|rep_size| {
            let rep = buf.slice_with_length(offset, rep_size as usize);
            offset += rep_size as usize;
            offset += pad_bytes::<MINIBLOCK_ALIGNMENT>(offset);
            rep
        });

        let def = def_size.map(|def_size| {
            let def = buf.slice_with_length(offset, def_size as usize);
            offset += def_size as usize;
            offset += pad_bytes::<MINIBLOCK_ALIGNMENT>(offset);
            def
        });

        let num_levels = Self::resolve_num_levels(
            self.rep_decompressor.as_deref(),
            rep.as_ref(),
            self.def_decompressor.as_deref(),
            def.as_ref(),
            num_levels,
        )?;

        let buffers = buffer_sizes
            .into_iter()
            .map(|buf_size| {
                let buf = buf.slice_with_length(offset, buf_size as usize);
                offset += buf_size as usize;
                offset += pad_bytes::<MINIBLOCK_ALIGNMENT>(offset);
                buf
            })
            .collect::<Vec<_>>();

        let values = self
            .value_decompressor
            .decompress(buffers, items_in_chunk)?;

        let rep = rep
            .map(|rep| {
                Self::decode_levels(
                    self.rep_decompressor.as_ref().unwrap().as_ref(),
                    rep,
                    num_levels,
                )
            })
            .transpose()?;
        let def = def
            .map(|def| {
                Self::decode_levels(
                    self.def_decompressor.as_ref().unwrap().as_ref(),
                    def,
                    num_levels,
                )
            })
            .transpose()?;

        if let (Some(rep), Some(def)) = (&rep, &def)
            && rep.len() != def.len()
        {
            return Err(Error::invalid_input_source(
                format!(
                    "miniblock structural streams decoded different level counts: repetition {}, definition {}",
                    rep.len(),
                    def.len()
                )
                .into(),
            ));
        }

        Ok(DecodedMiniBlockChunk { rep, def, values })
    }
}

impl DecodePageTask for DecodeMiniBlockTask {
    fn decode(self: Box<Self>) -> Result<DecodedPage> {
        // First, we create output buffers for the rep and def and data
        let mut repbuf: Option<LevelBuffer> = None;
        let mut defbuf: Option<LevelBuffer> = None;

        let max_rep = self.def_meaning.iter().filter(|l| l.is_list()).count() as u16;

        let estimated_size_bytes = self.decoded_size_bytes().unwrap_or_else(|| {
            // Variable-width and rep/def encoded output sizes are not known before decoding.
            self.instructions
                .iter()
                .map(|(_, chunk)| chunk.data.len() as u64)
                .sum::<u64>()
                * 2
        });
        let mut data_builder = DataBlockBuilder::with_capacity_estimate(estimated_size_bytes);

        // We need to keep track of the offset into repbuf/defbuf that we are building up
        let mut level_offset = 0;

        // Pre-compute caching needs for each chunk by checking if the next chunk is the same
        let needs_caching: Vec<bool> = self
            .instructions
            .windows(2)
            .map(|w| w[0].1.chunk_idx == w[1].1.chunk_idx)
            .chain(std::iter::once(false)) // the last one never needs caching
            .collect();

        // Cache for storing decoded chunks when beneficial
        let mut chunk_cache: Option<(usize, DecodedMiniBlockChunk)> = None;

        // Now we iterate through each instruction and process it
        for (idx, (instructions, chunk)) in self.instructions.iter().enumerate() {
            let should_cache_this_chunk = needs_caching[idx];

            let decoded_chunk = match &chunk_cache {
                Some((cached_chunk_idx, cached_chunk)) if *cached_chunk_idx == chunk.chunk_idx => {
                    // Clone only when we have a cache hit (much cheaper than decoding)
                    cached_chunk.clone()
                }
                _ => {
                    // Cache miss, need to decode
                    let decoded = self.decode_miniblock_chunk(&chunk.data, chunk.items_in_chunk)?;

                    // Only update cache if this chunk will benefit the next access
                    if should_cache_this_chunk {
                        chunk_cache = Some((chunk.chunk_idx, decoded.clone()));
                    }
                    decoded
                }
            };

            let DecodedMiniBlockChunk { rep, def, values } = decoded_chunk;

            // Our instructions tell us which rows we want to take from this chunk
            let row_range_start =
                instructions.rows_to_skip + instructions.chunk_instructions.rows_to_skip;
            let row_range_end = row_range_start + instructions.rows_to_take;

            // We use the rep info to map the row range to an item range / levels range
            let (item_range, level_range) = Self::map_range(
                row_range_start..row_range_end,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                self.max_visible_level,
                chunk.items_in_chunk,
                instructions.preamble_action,
            );
            if item_range.end - item_range.start > chunk.items_in_chunk {
                return Err(lance_core::Error::internal(format!(
                    "Item range {:?} is greater than chunk items in chunk {:?}",
                    item_range, chunk.items_in_chunk
                )));
            }

            // Now we append the data to the output buffers
            Self::extend_levels(level_range.clone(), &mut repbuf, &rep, level_offset);
            Self::extend_levels(level_range.clone(), &mut defbuf, &def, level_offset);
            level_offset += (level_range.end - level_range.start) as usize;
            data_builder.append(&values, item_range)?;
        }

        let mut data = data_builder.finish();

        let unraveler =
            RepDefUnraveler::new(repbuf, defbuf, self.def_meaning.clone(), data.num_values());

        if let Some(dictionary) = &self.dictionary_data {
            // Don't decode here, that happens later (if needed)
            let DataBlock::FixedWidth(indices) = data else {
                return Err(lance_core::Error::internal(format!(
                    "Expected FixedWidth DataBlock for dictionary indices, got {:?}",
                    data
                )));
            };
            data = DataBlock::Dictionary(DictionaryDataBlock::from_parts(
                indices,
                dictionary.as_ref().clone(),
            ));
        }

        Ok(DecodedPage {
            data,
            repdef: unraveler,
        })
    }
}

/// A chunk that has been loaded by the miniblock scheduler (but not
/// yet decoded)
#[derive(Debug)]
struct LoadedChunk {
    data: LanceBuffer,
    items_in_chunk: u64,
    byte_range: Range<u64>,
    chunk_idx: usize,
}

impl Clone for LoadedChunk {
    fn clone(&self) -> Self {
        Self {
            // Safe as we always create borrowed buffers here
            data: self.data.clone(),
            items_in_chunk: self.items_in_chunk,
            byte_range: self.byte_range.clone(),
            chunk_idx: self.chunk_idx,
        }
    }
}

/// Decodes mini-block formatted data.  See [`PrimitiveStructuralEncoder`] for more
/// details on the different layouts.
#[derive(Debug)]
struct MiniBlockDecoder {
    rep_decompressor: Option<Arc<dyn BlockDecompressor>>,
    def_decompressor: Option<Arc<dyn BlockDecompressor>>,
    value_decompressor: Arc<dyn MiniBlockDecompressor>,
    def_meaning: Arc<[DefinitionInterpretation]>,
    loaded_chunks: VecDeque<LoadedChunk>,
    instructions: VecDeque<ChunkInstructions>,
    offset_in_current_chunk: u64,
    num_rows: u64,
    num_buffers: u64,
    dictionary: Option<Arc<DataBlock>>,
    has_large_chunk: bool,
}

/// See [`MiniBlockScheduler`] for more details on the scheduling and decoding
/// process for miniblock encoded data.
impl StructuralPageDecoder for MiniBlockDecoder {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn DecodePageTask>> {
        let mut items_desired = num_rows;
        let mut need_preamble = false;
        let mut skip_in_chunk = self.offset_in_current_chunk;
        let mut drain_instructions = Vec::new();
        while items_desired > 0 || need_preamble {
            let (instructions, consumed) = self
                .instructions
                .front()
                .unwrap()
                .drain_from_instruction(&mut items_desired, &mut need_preamble, &mut skip_in_chunk);

            while self.loaded_chunks.front().unwrap().chunk_idx
                != instructions.chunk_instructions.chunk_idx
            {
                self.loaded_chunks.pop_front();
            }
            drain_instructions.push((instructions, self.loaded_chunks.front().unwrap().clone()));
            if consumed {
                self.instructions.pop_front();
            }
        }
        // We can throw away need_preamble here because it must be false.  If it were true it would mean
        // we were still in the middle of loading rows.  We do need to latch skip_in_chunk though.
        self.offset_in_current_chunk = skip_in_chunk;

        let max_visible_level = max_visible_level(&self.def_meaning);

        Ok(Box::new(DecodeMiniBlockTask {
            instructions: drain_instructions,
            def_decompressor: self.def_decompressor.clone(),
            rep_decompressor: self.rep_decompressor.clone(),
            value_decompressor: self.value_decompressor.clone(),
            dictionary_data: self.dictionary.clone(),
            def_meaning: self.def_meaning.clone(),
            num_buffers: self.num_buffers,
            max_visible_level,
            has_large_chunk: self.has_large_chunk,
        }))
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

/// How a complex-all-null page's rep/def level buffer is compressed on disk.
/// Captured at scheduler construction so `initialize` can keep RLE levels in run
/// form instead of expanding them.
#[derive(Debug, Clone)]
pub(crate) enum LevelCodec {
    /// Raw little-endian u16 levels (no block compression).
    Uncompressed,
    /// RLE-compressed levels; the validated physical runs select their cached representation.
    Rle(Arc<RleDecompressor>),
    /// Any other block compression; decoded eagerly into [`LazyLevels::Dense`]
    /// (these encodings don't expand, so laziness buys nothing).
    Block(Arc<dyn BlockDecompressor>),
}

impl LevelCodec {
    fn try_new(
        encoding: Option<&CompressiveEncoding>,
        decompression_strategy: &dyn DecompressionStrategy,
    ) -> Result<Self> {
        match encoding {
            None => Ok(Self::Uncompressed),
            Some(encoding) => match encoding.compression.as_ref() {
                Some(Compression::Rle(rle)) => Ok(Self::Rle(Arc::new(create_rle_decompressor(
                    rle,
                    decompression_strategy,
                )?))),
                _ => Ok(Self::Block(Arc::from(
                    decompression_strategy.create_block_decompressor(encoding)?,
                ))),
            },
        }
    }
}

#[derive(Debug)]
enum RunEnds {
    U16(Box<[u16]>),
    U32(Box<[u32]>),
    U64(Box<[u64]>),
}

impl RunEnds {
    fn width_for(num_values: usize) -> usize {
        if u16::try_from(num_values).is_ok() {
            std::mem::size_of::<u16>()
        } else if u32::try_from(num_values).is_ok() {
            std::mem::size_of::<u32>()
        } else {
            std::mem::size_of::<u64>()
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::U16(ends) => ends.len(),
            Self::U32(ends) => ends.len(),
            Self::U64(ends) => ends.len(),
        }
    }

    fn get(&self, run: usize) -> usize {
        match self {
            Self::U16(ends) => ends[run] as usize,
            Self::U32(ends) => ends[run] as usize,
            Self::U64(ends) => ends[run] as usize,
        }
    }

    fn partition_point(&self, logical_index: usize) -> usize {
        match self {
            Self::U16(ends) => ends.partition_point(|&end| end as usize <= logical_index),
            Self::U32(ends) => ends.partition_point(|&end| end as usize <= logical_index),
            Self::U64(ends) => ends.partition_point(|&end| end as usize <= logical_index),
        }
    }

    fn deep_size(&self) -> usize {
        match self {
            Self::U16(ends) => std::mem::size_of_val(ends.as_ref()),
            Self::U32(ends) => std::mem::size_of_val(ends.as_ref()),
            Self::U64(ends) => std::mem::size_of_val(ends.as_ref()),
        }
    }
}

enum RunEndsBuilder {
    U16(Vec<u16>),
    U32(Vec<u32>),
    U64(Vec<u64>),
}

impl RunEndsBuilder {
    fn with_capacity(num_values: usize, capacity: usize) -> Self {
        if u16::try_from(num_values).is_ok() {
            Self::U16(Vec::with_capacity(capacity))
        } else if u32::try_from(num_values).is_ok() {
            Self::U32(Vec::with_capacity(capacity))
        } else {
            Self::U64(Vec::with_capacity(capacity))
        }
    }

    fn push(&mut self, end: usize) -> Result<()> {
        match self {
            Self::U16(ends) => ends.push(
                u16::try_from(end)
                    .map_err(|_| Error::internal(format!("Run end {end} does not fit in u16")))?,
            ),
            Self::U32(ends) => ends.push(
                u32::try_from(end)
                    .map_err(|_| Error::internal(format!("Run end {end} does not fit in u32")))?,
            ),
            Self::U64(ends) => ends.push(end as u64),
        }
        Ok(())
    }

    fn set_last(&mut self, end: usize) -> Result<()> {
        match self {
            Self::U16(ends) => {
                let last = ends.last_mut().ok_or_else(|| {
                    Error::internal("Cannot extend an empty coalesced run buffer")
                })?;
                *last = u16::try_from(end)
                    .map_err(|_| Error::internal(format!("Run end {end} does not fit in u16")))?;
            }
            Self::U32(ends) => {
                let last = ends.last_mut().ok_or_else(|| {
                    Error::internal("Cannot extend an empty coalesced run buffer")
                })?;
                *last = u32::try_from(end)
                    .map_err(|_| Error::internal(format!("Run end {end} does not fit in u32")))?;
            }
            Self::U64(ends) => {
                let last = ends.last_mut().ok_or_else(|| {
                    Error::internal("Cannot extend an empty coalesced run buffer")
                })?;
                *last = end as u64;
            }
        }
        Ok(())
    }

    fn finish(self) -> RunEnds {
        match self {
            Self::U16(ends) => RunEnds::U16(ends.into_boxed_slice()),
            Self::U32(ends) => RunEnds::U32(ends.into_boxed_slice()),
            Self::U64(ends) => RunEnds::U64(ends.into_boxed_slice()),
        }
    }
}

#[derive(Debug)]
enum RunStorage {
    Physical(RleRuns),
    Coalesced { values: Box<[u16]>, ends: RunEnds },
}

impl RunStorage {
    fn len(&self) -> usize {
        match self {
            Self::Physical(runs) => runs.num_values(),
            Self::Coalesced { ends, .. } => ends.get(ends.len() - 1),
        }
    }

    fn num_runs(&self) -> usize {
        match self {
            Self::Physical(runs) => runs.num_runs(),
            Self::Coalesced { values, .. } => values.len(),
        }
    }

    fn value(&self, run: usize) -> u16 {
        match self {
            Self::Physical(runs) => runs.value(run),
            Self::Coalesced { values, .. } => values[run],
        }
    }

    fn first_value_above(&self, max: u16) -> Option<(usize, u16)> {
        (0..self.num_runs()).find_map(|run| {
            let value = self.value(run);
            (value > max).then_some((run, value))
        })
    }

    fn seek(&self, position: &mut RunPosition, logical_index: usize) {
        if logical_index >= self.len() {
            *position = RunPosition {
                run: self.num_runs(),
                start: self.len(),
                end: self.len(),
            };
            return;
        }

        match self {
            Self::Physical(runs) => {
                if position.run >= runs.num_runs()
                    || position.end == 0
                    || logical_index < position.start
                {
                    *position = RunPosition {
                        run: 0,
                        start: 0,
                        end: runs.length(0),
                    };
                }
                while position.end <= logical_index {
                    self.advance(position);
                }
            }
            Self::Coalesced { ends, .. } => {
                if logical_index < position.start || logical_index >= position.end {
                    let run = ends.partition_point(logical_index);
                    *position = RunPosition {
                        run,
                        start: if run == 0 { 0 } else { ends.get(run - 1) },
                        end: ends.get(run),
                    };
                }
            }
        }
    }

    fn advance(&self, position: &mut RunPosition) {
        let next_run = position.run + 1;
        if next_run >= self.num_runs() {
            *position = RunPosition {
                run: self.num_runs(),
                start: self.len(),
                end: self.len(),
            };
            return;
        }

        let start = position.end;
        position.run = next_run;
        position.start = start;
        position.end = match self {
            Self::Physical(runs) => start + runs.length(next_run),
            Self::Coalesced { ends, .. } => ends.get(next_run),
        };
    }

    fn deep_size(&self) -> usize {
        match self {
            Self::Physical(runs) => runs.deep_size(),
            Self::Coalesced { values, ends } => {
                std::mem::size_of_val(values.as_ref()) + ends.deep_size()
            }
        }
    }
}

/// Rep/def levels for a complex-all-null page.
///
/// RLE pages retain the smallest of their validated physical runs, coalesced
/// runs, and dense values. The decoder materializes only the per-drain slices
/// it touches.
#[derive(Debug, Clone)]
enum LazyLevels {
    Dense(ScalarBuffer<u16>),
    Runs(Arc<RunStorage>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LevelPlan {
    Physical,
    Coalesced,
    Dense,
}

#[derive(Debug, Default, Clone, Copy)]
struct RunPosition {
    run: usize,
    start: usize,
    end: usize,
}

/// Monotonic forward cursor into a [`LazyLevels`] sequence.
///
/// Drains seek to strictly increasing rows, so each [`LazyLevels::seek_row_start`]
/// resumes from the last position instead of rescanning — every run is visited at
/// most once per page while locating and counting level ranges.
#[derive(Debug, Default, Clone, Copy)]
struct LevelCursor {
    /// Logical level index where the current row begins.
    level: usize,
    /// Row index at `level` (the number of `max_rep` occurrences before it).
    row: u64,
    /// Run containing `level`. Unused for [`LazyLevels::Dense`].
    run: RunPosition,
}

impl LazyLevels {
    fn from_rle_runs(runs: RleRuns) -> Result<Self> {
        let plan = Self::select_plan(&runs);
        match plan {
            LevelPlan::Physical => Ok(Self::Runs(Arc::new(RunStorage::Physical(
                runs.into_owned(),
            )))),
            LevelPlan::Coalesced => Self::build_coalesced(runs),
            LevelPlan::Dense => Self::build_dense(runs),
        }
    }

    /// Minimize retained payload bytes first, then expected traversal work.
    /// If both are equal, keep the physical runs and avoid another allocation.
    fn select_plan(runs: &RleRuns) -> LevelPlan {
        if runs.num_values() == 0 {
            return LevelPlan::Dense;
        }

        let run_storage_size = std::mem::size_of::<RunStorage>() as u128;
        let physical_size = run_storage_size + runs.owned_size() as u128;
        let coalesced_size = run_storage_size
            + (runs.coalesced_runs() as u128)
                * (std::mem::size_of::<u16>() + RunEnds::width_for(runs.num_values())) as u128;
        let dense_size = (runs.num_values() as u128) * std::mem::size_of::<u16>() as u128;
        [
            (physical_size, runs.num_runs(), 0usize, LevelPlan::Physical),
            (
                coalesced_size,
                runs.coalesced_runs(),
                1usize,
                LevelPlan::Coalesced,
            ),
            (dense_size, runs.num_values(), 2usize, LevelPlan::Dense),
        ]
        .into_iter()
        .min_by_key(|(size, traversal, priority, _)| (*size, *traversal, *priority))
        .map(|(_, _, _, plan)| plan)
        .unwrap_or(LevelPlan::Dense)
    }

    fn build_coalesced(runs: RleRuns) -> Result<Self> {
        let mut values = Vec::with_capacity(runs.coalesced_runs());
        let mut ends = RunEndsBuilder::with_capacity(runs.num_values(), runs.coalesced_runs());
        let mut logical_end = 0usize;
        for (value, length) in runs.iter() {
            logical_end = logical_end
                .checked_add(length)
                .ok_or_else(|| Error::internal("Validated RLE run length sum overflowed usize"))?;
            if values.last().copied() == Some(value) {
                ends.set_last(logical_end)?;
            } else {
                values.push(value);
                ends.push(logical_end)?;
            }
        }
        Ok(Self::Runs(Arc::new(RunStorage::Coalesced {
            values: values.into_boxed_slice(),
            ends: ends.finish(),
        })))
    }

    fn build_dense(runs: RleRuns) -> Result<Self> {
        let mut values = Vec::new();
        values.try_reserve_exact(runs.num_values()).map_err(|_| {
            Error::internal(format!(
                "Cannot allocate {} dense repetition/definition levels",
                runs.num_values()
            ))
        })?;
        for (value, length) in runs.iter() {
            values.resize(values.len() + length, value);
        }
        Ok(Self::Dense(ScalarBuffer::from(values)))
    }

    fn len(&self) -> usize {
        match self {
            Self::Dense(buf) => buf.len(),
            Self::Runs(runs) => runs.len(),
        }
    }

    fn validate_max(&self, level_type: &str, max: u16) -> Result<()> {
        let invalid = match self {
            Self::Dense(levels) => levels
                .iter()
                .enumerate()
                .find_map(|(index, &value)| (value > max).then_some(("index", index, value))),
            Self::Runs(runs) => runs
                .first_value_above(max)
                .map(|(run, value)| ("run", run, value)),
        };
        if let Some((position_type, position, value)) = invalid {
            return Err(Error::invalid_input_source(
                format!(
                    "Invalid {level_type} level {value} at {position_type} {position}: maximum is {max}"
                )
                .into(),
            ));
        }
        Ok(())
    }

    /// Advance `cursor` to the start of row `target_row`, returning that row's
    /// starting level index.
    ///
    /// Rows begin at `max_rep` positions, so this finds the `target_row`-th one.
    /// `target_row` must be `>= cursor.row`: the cursor only moves forward, which
    /// is what keeps a full page decode O(runs) rather than O(rows).
    fn seek_row_start(
        &self,
        cursor: &mut LevelCursor,
        target_row: u64,
        max_rep: u16,
    ) -> Result<usize> {
        let mut need = target_row.checked_sub(cursor.row).ok_or_else(|| {
            Error::internal(format!(
                "Complex all-null row ranges are not sorted: target row {target_row} follows {}",
                cursor.row
            ))
        })?;
        if need == 0 {
            return Ok(cursor.level);
        }
        match self {
            Self::Dense(buf) => {
                let mut level = cursor.level;
                while need > 0 {
                    if level >= buf.len() {
                        return Err(Error::internal(
                            "Invalid complex all-null layout: repetition buffer too short",
                        ));
                    }
                    if buf[level] != max_rep {
                        return Err(Error::internal(
                            "Invalid complex all-null layout: row did not start at max repetition level",
                        ));
                    }
                    level += 1;
                    while level < buf.len() && buf[level] != max_rep {
                        level += 1;
                    }
                    need -= 1;
                }
                cursor.level = level;
                cursor.row = target_row;
                Ok(level)
            }
            Self::Runs(runs) => {
                let mut level = cursor.level;
                let mut run = cursor.run;
                runs.seek(&mut run, level);
                while need > 0 {
                    if run.run >= runs.num_runs() {
                        return Err(Error::internal(
                            "Invalid complex all-null layout: repetition buffer too short",
                        ));
                    }
                    if runs.value(run.run) != max_rep {
                        return Err(Error::internal(
                            "Invalid complex all-null layout: row did not start at max repetition level",
                        ));
                    }
                    let avail = (run.end - level) as u64;
                    if need < avail {
                        // Target lands inside this max-rep run.
                        level += need as usize;
                        need = 0;
                    } else {
                        // Consume every row start in this run, then skip the
                        // trailing non-max-rep runs to reach the next row start.
                        need -= avail;
                        runs.advance(&mut run);
                        while run.run < runs.num_runs() && runs.value(run.run) != max_rep {
                            runs.advance(&mut run);
                        }
                        level = if run.run < runs.num_runs() {
                            run.start
                        } else {
                            self.len()
                        };
                    }
                }
                cursor.level = level;
                cursor.row = target_row;
                cursor.run = run;
                Ok(level)
            }
        }
    }

    /// Count of levels in `range` that are `<= max`, resuming from `*run_cursor`
    /// and leaving it on the last run that overlaps `range`.
    ///
    /// Successive calls must pass ascending, non-overlapping ranges (`range.start
    /// >=` the previous `range.end`) so runs are swept at most once per page.
    fn count_le_cursor(
        &self,
        run_cursor: &mut RunPosition,
        range: Range<usize>,
        max: u16,
    ) -> (u64, RunPosition) {
        if range.is_empty() {
            return (0, *run_cursor);
        }
        match self {
            Self::Dense(buf) => (
                buf[range].iter().filter(|&&d| d <= max).count() as u64,
                RunPosition::default(),
            ),
            Self::Runs(runs) => {
                // Advance to the first run overlapping the range.
                runs.seek(run_cursor, range.start);
                let start = *run_cursor;
                let mut count = 0u64;
                let mut current = *run_cursor;
                while current.run < runs.num_runs() && current.start < range.end {
                    if runs.value(current.run) <= max {
                        let lo = current.start.max(range.start);
                        let hi = current.end.min(range.end);
                        count += (hi - lo) as u64;
                    }
                    if current.end >= range.end {
                        break;
                    }
                    runs.advance(&mut current);
                }
                // Resume the next (ascending) range from the last overlapping run;
                // `current` remains valid because `range` is non-empty.
                *run_cursor = current;
                (count, start)
            }
        }
    }

    fn extend_into(&self, range: Range<usize>, run: RunPosition, out: &mut Vec<u16>) {
        if range.is_empty() {
            return;
        }
        match self {
            Self::Dense(buf) => out.extend_from_slice(&buf[range]),
            Self::Runs(runs) => {
                let mut current = run;
                runs.seek(&mut current, range.start);
                while current.run < runs.num_runs() && current.start < range.end {
                    let lo = current.start.max(range.start);
                    let hi = current.end.min(range.end);
                    if hi > lo {
                        out.resize(out.len() + (hi - lo), runs.value(current.run));
                    }
                    runs.advance(&mut current);
                }
            }
        }
    }

    #[cfg(test)]
    fn deep_size(&self) -> usize {
        self.deep_size_of_children(&mut Context::new())
    }
}

impl DeepSizeOf for LazyLevels {
    fn deep_size_of_children(&self, ctx: &mut Context) -> usize {
        match self {
            Self::Dense(buf) => buf.deep_size_of_children(ctx),
            Self::Runs(runs) => {
                let pointer = Arc::as_ptr(runs) as *const () as usize;
                if ctx.mark_seen(pointer) {
                    std::mem::size_of_val(runs.as_ref()) + runs.deep_size()
                } else {
                    0
                }
            }
        }
    }
}

fn validate_complex_all_null_levels(
    rep: &Option<LazyLevels>,
    def: &Option<LazyLevels>,
    max_rep: u16,
    max_def: u16,
) -> Result<()> {
    if let Some(rep) = rep {
        rep.validate_max("repetition", max_rep)?;
    }
    if let Some(def) = def {
        def.validate_max("definition", max_def)?;
    }
    if let (Some(rep), Some(def)) = (rep, def)
        && rep.len() != def.len()
    {
        return Err(Error::invalid_input_source(
            format!(
                "Mismatched complex all-null level counts: repetition has {}, definition has {}",
                rep.len(),
                def.len()
            )
            .into(),
        ));
    }
    Ok(())
}

fn expected_level_bytes(num_values: u64, level_type: &str) -> Result<usize> {
    usize::try_from(num_values)
        .ok()
        .and_then(|num_values| num_values.checked_mul(std::mem::size_of::<u16>()))
        .ok_or_else(|| {
            Error::invalid_input_source(
                format!("{level_type} level count {num_values} does not fit in memory").into(),
            )
        })
}

fn dense_levels_from_block(
    decompressed: DataBlock,
    num_values: u64,
    level_type: &str,
) -> Result<LazyLevels> {
    let DataBlock::FixedWidth(block) = decompressed else {
        return Err(Error::invalid_input_source(
            format!("Expected fixed-width data block for {level_type} levels").into(),
        ));
    };
    if block.num_values != num_values {
        return Err(Error::invalid_input_source(
            format!(
                "Unexpected {level_type} level count after decompression: expected {num_values}, got {}",
                block.num_values
            )
            .into(),
        ));
    }
    if block.bits_per_value != 16 {
        return Err(Error::invalid_input_source(
            format!(
                "Unexpected {level_type} level bit width after decompression: expected 16, got {}",
                block.bits_per_value
            )
            .into(),
        ));
    }
    let expected_bytes = expected_level_bytes(num_values, level_type)?;
    if block.data.len() != expected_bytes {
        return Err(Error::invalid_input_source(
            format!(
                "Unexpected decompressed {level_type} level size: expected {expected_bytes} bytes for {num_values} values, got {}",
                block.data.len()
            )
            .into(),
        ));
    }
    Ok(LazyLevels::Dense(block.data.borrow_to_typed_slice::<u16>()))
}

#[derive(Debug)]
struct CachedComplexAllNullState {
    rep: Option<LazyLevels>,
    def: Option<LazyLevels>,
}

impl DeepSizeOf for CachedComplexAllNullState {
    fn deep_size_of_children(&self, ctx: &mut Context) -> usize {
        self.rep.deep_size_of_children(ctx) + self.def.deep_size_of_children(ctx)
    }
}

impl CachedPageData for CachedComplexAllNullState {
    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self
    }
}

/// A scheduler for all-null data that has repetition and definition levels
///
/// We still need to do some I/O in this case because we need to figure out what kind of null we
/// are dealing with (null list, null struct, what level null struct, etc.)
///
/// TODO: Right now we just load the entire rep/def at initialization time and cache it.  This is a touch
/// RAM aggressive and maybe we want something more lazy in the future.  On the other hand, it's simple
/// and fast so...maybe not :)
#[derive(Debug)]
pub struct ComplexAllNullScheduler {
    // Set from protobuf
    buffer_offsets_and_sizes: Arc<[(u64, u64)]>,
    def_meaning: Arc<[DefinitionInterpretation]>,
    repdef: Option<Arc<CachedComplexAllNullState>>,
    max_rep: u16,
    max_def: u16,
    max_visible_level: u16,
    rep_codec: LevelCodec,
    def_codec: LevelCodec,
    num_rep_values: u64,
    num_def_values: u64,
}

impl ComplexAllNullScheduler {
    pub(crate) fn new(
        buffer_offsets_and_sizes: Arc<[(u64, u64)]>,
        def_meaning: Arc<[DefinitionInterpretation]>,
        rep_codec: LevelCodec,
        def_codec: LevelCodec,
        num_rep_values: u64,
        num_def_values: u64,
    ) -> Self {
        let max_rep = def_meaning.iter().filter(|l| l.is_list()).count() as u16;
        let max_def = def_meaning
            .iter()
            .map(|meaning| meaning.num_def_levels())
            .sum::<u16>();
        let max_visible_level = max_visible_level(&def_meaning);
        Self {
            buffer_offsets_and_sizes,
            def_meaning,
            repdef: None,
            max_rep,
            max_def,
            max_visible_level,
            rep_codec,
            def_codec,
            num_rep_values,
            num_def_values,
        }
    }
}

impl StructuralPageScheduler for ComplexAllNullScheduler {
    fn init_layout(&self) -> Result<PageInitialization> {
        // Fully load the rep & def buffers, as needed
        let (rep_pos, rep_size) = self.buffer_offsets_and_sizes[0];
        let (def_pos, def_size) = self.buffer_offsets_and_sizes[1];

        Ok(PageInitialization::new([
            (rep_size > 0).then_some(rep_pos..rep_pos + rep_size),
            (def_size > 0).then_some(def_pos..def_pos + def_size),
        ]))
    }

    fn init_from_buffers<'a>(
        &'a mut self,
        buffers: PageInitializationBuffers,
        _io: &Arc<dyn EncodingsIo>,
    ) -> BoxFuture<'a, Result<Arc<dyn CachedPageData>>> {
        let rep_codec = self.rep_codec.clone();
        let def_codec = self.def_codec.clone();
        let num_rep_values = self.num_rep_values;
        let num_def_values = self.num_def_values;
        let max_rep = self.max_rep;
        let max_def = self.max_def;

        async move {
            let [rep_bytes, def_bytes] = buffers.try_into_array("complex all-null")?;

            // RLE levels select the smallest validated cache representation;
            // everything else expands eagerly to `LazyLevels::Dense`.
            let build_levels = |compressed_bytes: Bytes,
                                codec: &LevelCodec,
                                num_values: u64,
                                level_type: &str|
             -> Result<LazyLevels> {
                match codec {
                    LevelCodec::Uncompressed => {
                        if num_values == 0 {
                            if !compressed_bytes
                                .len()
                                .is_multiple_of(std::mem::size_of::<u16>())
                            {
                                return Err(Error::invalid_input_source(
                                    format!(
                                        "Unexpected uncompressed {level_type} level size: {} bytes is not divisible by {}",
                                        compressed_bytes.len(),
                                        std::mem::size_of::<u16>()
                                    )
                                    .into(),
                                ));
                            }
                        } else {
                            let expected_bytes = expected_level_bytes(num_values, level_type)?;
                            if compressed_bytes.len() != expected_bytes {
                                return Err(Error::invalid_input_source(
                                    format!(
                                        "Unexpected uncompressed {level_type} level size: expected {expected_bytes} bytes for {num_values} values, got {}",
                                        compressed_bytes.len()
                                    )
                                    .into(),
                                ));
                            }
                        }
                        let buffer = LanceBuffer::from_bytes(compressed_bytes, 2);
                        Ok(LazyLevels::Dense(buffer.borrow_to_typed_slice::<u16>()))
                    }
                    LevelCodec::Rle(decompressor) => {
                        let frame = LanceBuffer::from_bytes(compressed_bytes, 1);
                        let runs = decompressor.decode_u16_runs(frame, num_values)?;
                        LazyLevels::from_rle_runs(runs)
                    }
                    LevelCodec::Block(decompressor) => {
                        let frame = LanceBuffer::from_bytes(compressed_bytes, 1);
                        let decompressed = decompressor.decompress(Some(frame), num_values)?;
                        dense_levels_from_block(decompressed, num_values, level_type)
                    }
                }
            };

            let rep = if let Some(rep) = rep_bytes {
                Some(build_levels(rep, &rep_codec, num_rep_values, "repetition")?)
            } else {
                None
            };

            let def = if let Some(def) = def_bytes {
                Some(build_levels(def, &def_codec, num_def_values, "definition")?)
            } else {
                None
            };

            validate_complex_all_null_levels(&rep, &def, max_rep, max_def)?;
            let repdef = Arc::new(CachedComplexAllNullState { rep, def });

            Ok(repdef as Arc<dyn CachedPageData>)
        }
        .boxed()
    }

    fn try_load(&mut self, data: &Arc<dyn CachedPageData>) -> Result<()> {
        self.repdef = Some(data.clone().as_arc_any().downcast().map_err(|_| {
            Error::invalid_input_source(
                "Cached complex all-null page data has an unexpected type".into(),
            )
        })?);
        Ok(())
    }

    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        _io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>> {
        let ranges = VecDeque::from_iter(ranges.iter().cloned());
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        let decoder = Box::new(ComplexAllNullPageDecoder {
            ranges,
            rep: self.repdef.as_ref().unwrap().rep.clone(),
            def: self.repdef.as_ref().unwrap().def.clone(),
            num_rows,
            def_meaning: self.def_meaning.clone(),
            max_rep: self.max_rep,
            max_visible_level: self.max_visible_level,
            rep_cursor: LevelCursor::default(),
            def_run_cursor: RunPosition::default(),
        }) as Box<dyn StructuralPageDecoder>;
        let page_load_task = PageLoadTask {
            decoder_fut: std::future::ready(Ok(decoder)).boxed(),
            num_rows,
        };
        Ok(vec![page_load_task])
    }
}

#[derive(Debug)]
pub struct ComplexAllNullPageDecoder {
    ranges: VecDeque<Range<u64>>,
    rep: Option<LazyLevels>,
    def: Option<LazyLevels>,
    num_rows: u64,
    def_meaning: Arc<[DefinitionInterpretation]>,
    max_rep: u16,
    max_visible_level: u16,
    /// Monotonic cursor into `rep` tracking the current row's level start.
    rep_cursor: LevelCursor,
    /// Monotonic run cursor into `def` for `count_le_cursor`.
    def_run_cursor: RunPosition,
}

impl ComplexAllNullPageDecoder {
    fn drain_ranges(&mut self, num_rows: u64) -> Vec<Range<u64>> {
        let mut rows_desired = num_rows;
        let mut ranges = Vec::with_capacity(self.ranges.len());
        while rows_desired > 0 {
            let front = self.ranges.front_mut().unwrap();
            let avail = front.end - front.start;
            if avail > rows_desired {
                ranges.push(front.start..front.start + rows_desired);
                front.start += rows_desired;
                rows_desired = 0;
            } else {
                ranges.push(self.ranges.pop_front().unwrap());
                rows_desired -= avail;
            }
        }
        ranges
    }

    /// Level index at which row `target_row` starts, advancing the monotonic
    /// repetition cursor. Callers must request non-decreasing `target_row`.
    fn seek_row_start(&mut self, target_row: u64) -> Result<usize> {
        match &self.rep {
            Some(rep) => rep.seek_row_start(&mut self.rep_cursor, target_row, self.max_rep),
            None => {
                // Without repetition every level is its own row.
                self.rep_cursor.row = target_row;
                self.rep_cursor.level = target_row as usize;
                Ok(target_row as usize)
            }
        }
    }

    /// Number of visible items in the level range `levels` (definition levels
    /// `<= max_visible_level`), advancing the monotonic definition cursor.
    fn count_visible(&mut self, levels: Range<usize>) -> Result<(u64, RunPosition)> {
        match &self.def {
            Some(def) => {
                if levels.end > def.len() {
                    return Err(Error::internal(
                        "Invalid complex all-null layout: definition buffer too short",
                    ));
                }
                Ok(def.count_le_cursor(&mut self.def_run_cursor, levels, self.max_visible_level))
            }
            None => Ok(((levels.end - levels.start) as u64, RunPosition::default())),
        }
    }
}

impl StructuralPageDecoder for ComplexAllNullPageDecoder {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn DecodePageTask>> {
        let drained_ranges = self.drain_ranges(num_rows);
        let mut level_slices: Vec<LevelSlice> = Vec::with_capacity(drained_ranges.len());
        let mut visible_items_total = 0;

        // Each row range is one contiguous level slice `[start_row_level,
        // end_row_level)`, so we seek both boundaries and count its visibility at
        // once rather than per row. The cursors only move forward, so locating and
        // counting all requested ranges visits each intervening run at most once.
        for range in drained_ranges {
            let level_start = self.seek_row_start(range.start)?;
            let rep_run = self.rep_cursor.run;
            let level_end = self.seek_row_start(range.end)?;
            let (visible_items, def_run) = self.count_visible(level_start..level_end)?;
            visible_items_total += visible_items;
            if let Some(last) = level_slices.last_mut()
                && last.range.end == level_start
            {
                last.range.end = level_end;
            } else {
                level_slices.push(LevelSlice {
                    range: level_start..level_end,
                    rep_run,
                    def_run,
                });
            }
        }

        Ok(Box::new(DecodeComplexAllNullTask {
            level_slices,
            visible_items_total,
            rep: self.rep.clone(),
            def: self.def.clone(),
            def_meaning: self.def_meaning.clone(),
            max_visible_level: self.max_visible_level,
        }))
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

/// We use `level_slices` to slice into `rep` and `def` and create rep/def buffers
/// for the null data.
#[derive(Debug, Clone)]
struct LevelSlice {
    range: Range<usize>,
    rep_run: RunPosition,
    def_run: RunPosition,
}

#[derive(Clone, Copy)]
enum LevelKind {
    Repetition,
    Definition,
}

impl LevelSlice {
    fn run(&self, kind: LevelKind) -> RunPosition {
        match kind {
            LevelKind::Repetition => self.rep_run,
            LevelKind::Definition => self.def_run,
        }
    }
}

#[derive(Debug)]
pub struct DecodeComplexAllNullTask {
    level_slices: Vec<LevelSlice>,
    visible_items_total: u64,
    rep: Option<LazyLevels>,
    def: Option<LazyLevels>,
    def_meaning: Arc<[DefinitionInterpretation]>,
    max_visible_level: u16,
}

impl DecodeComplexAllNullTask {
    fn decode_level(&self, levels: &Option<LazyLevels>, kind: LevelKind) -> Option<Vec<u16>> {
        levels.as_ref().map(|levels| {
            let num_levels = self
                .level_slices
                .iter()
                .map(|slice| slice.range.end - slice.range.start)
                .sum();
            let mut referenced_levels = Vec::with_capacity(num_levels);
            for slice in &self.level_slices {
                levels.extend_into(slice.range.clone(), slice.run(kind), &mut referenced_levels);
            }
            referenced_levels
        })
    }
}

impl DecodePageTask for DecodeComplexAllNullTask {
    fn decode(self: Box<Self>) -> Result<DecodedPage> {
        let rep = self.decode_level(&self.rep, LevelKind::Repetition);
        let def = self.decode_level(&self.def, LevelKind::Definition);

        // If there are definition levels there may be empty / null lists which are not visible
        // in the items array.  We need to account for that here to figure out how many values
        // should be in the items array.
        let num_values = if let Some(def) = &def {
            def.iter().filter(|&d| *d <= self.max_visible_level).count() as u64
        } else {
            self.visible_items_total
        };

        let data = DataBlock::AllNull(AllNullDataBlock { num_values });
        let unraveler = RepDefUnraveler::new(rep, def, self.def_meaning, num_values);
        Ok(DecodedPage {
            data,
            repdef: unraveler,
        })
    }
}

/// A scheduler for simple all-null data
///
/// "simple" all-null data is data that is all null and only has a single level of definition and
/// no repetition.  We don't need to read any data at all in this case.
#[derive(Debug, Default)]
pub struct SimpleAllNullScheduler {}

impl StructuralPageScheduler for SimpleAllNullScheduler {
    fn needs_initialization(&self) -> bool {
        false
    }

    fn init_layout(&self) -> Result<PageInitialization> {
        Ok(PageInitialization::new([]))
    }

    fn init_from_buffers<'a>(
        &'a mut self,
        _buffers: PageInitializationBuffers,
        _io: &Arc<dyn EncodingsIo>,
    ) -> BoxFuture<'a, Result<Arc<dyn CachedPageData>>> {
        std::future::ready(Ok(Arc::new(NoCachedPageData) as Arc<dyn CachedPageData>)).boxed()
    }

    fn try_load(&mut self, _cache: &Arc<dyn CachedPageData>) -> Result<()> {
        Ok(())
    }

    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        _io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>> {
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        let decoder =
            Box::new(SimpleAllNullPageDecoder { num_rows }) as Box<dyn StructuralPageDecoder>;
        let page_load_task = PageLoadTask {
            decoder_fut: std::future::ready(Ok(decoder)).boxed(),
            num_rows,
        };
        Ok(vec![page_load_task])
    }
}

/// A page decode task for all-null data without any
/// repetition and only a single level of definition
#[derive(Debug)]
struct SimpleAllNullDecodePageTask {
    num_values: u64,
}
impl DecodePageTask for SimpleAllNullDecodePageTask {
    fn decode(self: Box<Self>) -> Result<DecodedPage> {
        let unraveler = RepDefUnraveler::new(
            None,
            Some(vec![1; self.num_values as usize]),
            Arc::new([DefinitionInterpretation::NullableItem]),
            self.num_values,
        );
        Ok(DecodedPage {
            data: DataBlock::AllNull(AllNullDataBlock {
                num_values: self.num_values,
            }),
            repdef: unraveler,
        })
    }
}

#[derive(Debug)]
pub struct SimpleAllNullPageDecoder {
    num_rows: u64,
}

impl StructuralPageDecoder for SimpleAllNullPageDecoder {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn DecodePageTask>> {
        Ok(Box::new(SimpleAllNullDecodePageTask {
            num_values: num_rows,
        }))
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

#[derive(Debug, Clone)]
struct MiniBlockSchedulerDictionary {
    // These come from the protobuf
    dictionary_decompressor: Arc<dyn BlockDecompressor>,
    dictionary_buf_position_and_size: (u64, u64),
    dictionary_data_alignment: u64,
    num_dictionary_items: u64,
}

/// State that is loaded once and cached for future lookups
#[derive(Debug)]
struct MiniBlockCacheableState {
    /// Compact per-chunk index (byte ranges + row/item mapping) for the page
    chunk_index: MiniBlockChunkIndex,
    /// The dictionary for the page, if any
    dictionary: Option<Arc<DataBlock>>,
}

impl DeepSizeOf for MiniBlockCacheableState {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.chunk_index.deep_size_of_children(context)
            + self
                .dictionary
                .as_ref()
                .map(|dict| dict.data_size() as usize)
                .unwrap_or(0)
    }
}

impl CachedPageData for MiniBlockCacheableState {
    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self
    }
}

/// A scheduler for a page that has been encoded with the mini-block layout
///
/// Scheduling mini-block encoded data is simple in concept and somewhat complex
/// in practice.
///
/// First, during initialization, we load the chunk metadata, the repetition index,
/// and the dictionary (these last two may not be present)
///
/// Then, during scheduling, we use the user's requested row ranges and the repetition
/// index to determine which chunks we need and which rows we need from those chunks.
///
/// For example, if the repetition index is: [50, 3], [50, 0], [10, 0] and the range
/// from the user is 40..60 then we need to:
///
///  - Read the first chunk and skip the first 40 rows, then read 10 full rows, and
///    then read 3 items for the 11th row of our range.
///  - Read the second chunk and read the remaining items in our 11th row and then read
///    the remaining 9 full rows.
///
/// Then, if we are going to decode that in batches of 5, we need to make decode tasks.
/// The first two decode tasks will just need the first chunk.  The third decode task will
/// need the first chunk (for the trailer which has the 11th row in our range) and the second
/// chunk.  The final decode task will just need the second chunk.
///
/// The above prose descriptions are what are represented by `ChunkInstructions` and
/// `ChunkDrainInstructions`.
#[derive(Debug)]
pub struct MiniBlockScheduler {
    // These come from the protobuf
    buffer_offsets_and_sizes: Vec<(u64, u64)>,
    priority: u64,
    items_in_page: u64,
    repetition_index_depth: u16,
    num_buffers: u64,
    rep_decompressor: Option<Arc<dyn BlockDecompressor>>,
    def_decompressor: Option<Arc<dyn BlockDecompressor>>,
    value_decompressor: Arc<dyn MiniBlockDecompressor>,
    def_meaning: Arc<[DefinitionInterpretation]>,
    dictionary: Option<MiniBlockSchedulerDictionary>,
    // This is set after initialization
    page_meta: Option<Arc<MiniBlockCacheableState>>,
    has_large_chunk: bool,
}

impl MiniBlockScheduler {
    fn try_new(
        buffer_offsets_and_sizes: &[(u64, u64)],
        priority: u64,
        items_in_page: u64,
        layout: &pb21::MiniBlockLayout,
        decompressors: &dyn DecompressionStrategy,
    ) -> Result<Self> {
        let rep_decompressor = layout
            .rep_compression
            .as_ref()
            .map(|rep_compression| {
                decompressors
                    .create_block_decompressor(rep_compression)
                    .map(Arc::from)
            })
            .transpose()?;
        let def_decompressor = layout
            .def_compression
            .as_ref()
            .map(|def_compression| {
                decompressors
                    .create_block_decompressor(def_compression)
                    .map(Arc::from)
            })
            .transpose()?;
        let def_meaning = layout
            .layers
            .iter()
            .map(|l| ProtobufUtils21::repdef_layer_to_def_interp(*l))
            .collect::<Vec<_>>();
        let value_decompressor = decompressors.create_miniblock_decompressor(
            layout.value_compression.as_ref().unwrap(),
            decompressors,
        )?;

        let dictionary = if let Some(dictionary_encoding) = layout.dictionary.as_ref() {
            let num_dictionary_items = layout.num_dictionary_items;
            let dictionary_decompressor = decompressors
                .create_block_decompressor(dictionary_encoding)?
                .into();
            let dictionary_data_alignment = match dictionary_encoding.compression.as_ref().unwrap()
            {
                Compression::Variable(_) => 4,
                Compression::Flat(_) => 16,
                Compression::General(_) => 1,
                Compression::InlineBitpacking(_) | Compression::OutOfLineBitpacking(_) => {
                    crate::encoder::MIN_PAGE_BUFFER_ALIGNMENT
                }
                _ => {
                    return Err(Error::invalid_input_source(
                        format!(
                            "Unsupported mini-block dictionary encoding: {:?}",
                            dictionary_encoding.compression.as_ref().unwrap()
                        )
                        .into(),
                    ));
                }
            };
            Some(MiniBlockSchedulerDictionary {
                dictionary_decompressor,
                dictionary_buf_position_and_size: buffer_offsets_and_sizes[2],
                dictionary_data_alignment,
                num_dictionary_items,
            })
        } else {
            None
        };

        Ok(Self {
            buffer_offsets_and_sizes: buffer_offsets_and_sizes.to_vec(),
            rep_decompressor,
            def_decompressor,
            value_decompressor: value_decompressor.into(),
            repetition_index_depth: layout.repetition_index_depth as u16,
            num_buffers: layout.num_buffers,
            priority,
            items_in_page,
            dictionary,
            def_meaning: def_meaning.into(),
            page_meta: None,
            has_large_chunk: layout.has_large_chunk,
        })
    }

    fn lookup_chunks(&self, chunk_indices: &[usize]) -> Vec<LoadedChunk> {
        let chunk_index = &self.page_meta.as_ref().unwrap().chunk_index;
        chunk_indices
            .iter()
            .map(|&chunk_idx| LoadedChunk {
                byte_range: chunk_index.byte_range(chunk_idx),
                items_in_chunk: chunk_index.items_in_chunk(chunk_idx),
                chunk_idx,
                data: LanceBuffer::empty(),
            })
            .collect()
    }
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
enum PreambleAction {
    Take,
    Skip,
    Absent,
}

// When we schedule a chunk we use the repetition index (or, if none exists, just the # of items
// in each chunk) to map a user requested range into a set of ChunkInstruction objects which tell
// us how exactly to read from the chunk.
//
// Examples:
//
// | Chunk 0     | Chunk 1   | Chunk 2   | Chunk 3 |
// | xxxxyyyyzzz | zzzzzzzzz | zzzzzzzzz | aaabbcc |
//
// Full read (0..6)
//
// Chunk 0: (several rows, ends with trailer)
//   preamble: absent
//   rows_to_skip: 0
//   rows_to_take: 3 (x, y, z)
//   take_trailer: true
//
// Chunk 1: (all preamble, ends with trailer)
//   preamble: take
//   rows_to_skip: 0
//   rows_to_take: 0
//   take_trailer: true
//
// Chunk 2: (all preamble, no trailer)
//   preamble: take
//   rows_to_skip: 0
//   rows_to_take: 0
//   take_trailer: false
//
// Chunk 3: (several rows, no trailer or preamble)
//   preamble: absent
//   rows_to_skip: 0
//   rows_to_take: 3 (a, b, c)
//   take_trailer: false
#[derive(Clone, Debug, PartialEq, Eq)]
struct ChunkInstructions {
    // The index of the chunk to read
    chunk_idx: usize,
    // A "preamble" is when a chunk begins with a continuation of the previous chunk's list.  If there
    // is no repetition index there is never a preamble.
    //
    // It's possible for a chunk to be entirely premable.  For example, if there is a really large list
    // that spans several chunks.
    preamble: PreambleAction,
    // How many complete rows (not including the preamble or trailer) to skip
    //
    // If this is non-zero then premable must not be Take
    rows_to_skip: u64,
    // How many rows to take.  If a row splits across chunks then we will count the row in the first
    // chunk that contains the row.
    rows_to_take: u64,
    // A "trailer" is when a chunk ends with a partial list.  If there is no repetition index there is
    // never a trailer.
    //
    // A chunk that is all preamble may or may not have a trailer.
    //
    // If this is true then we want to include the trailer
    take_trailer: bool,
}

// First, we schedule a bunch of [`ChunkInstructions`] based on the users ranges.  Then we
// start decoding them, based on a batch size, which might not align with what we scheduled.
//
// This results in `ChunkDrainInstructions` which targets a contiguous slice of a `ChunkInstructions`
//
// So if `ChunkInstructions` is "skip preamble, skip 10, take 50, take trailer" and we are decoding in
// batches of size 10 we might have a `ChunkDrainInstructions` that targets that chunk and has its own
// skip of 17 and take of 10.  This would mean we decode the chunk, skip the preamble and 27 rows, and
// then take 10 rows.
//
// One very confusing bit is that `rows_to_take` includes the trailer.  So if we have two chunks:
//  -no preamble, skip 5, take 10, take trailer
//  -take preamble, skip 0, take 50, no trailer
//
// and we are draining 20 rows then the drain instructions for the first batch will be:
//  - no preamble, skip 0 (from chunk 0), take 11 (from chunk 0)
//  - take preamble (from chunk 1), skip 0 (from chunk 1), take 9 (from chunk 1)
#[derive(Debug, PartialEq, Eq)]
struct ChunkDrainInstructions {
    chunk_instructions: ChunkInstructions,
    rows_to_skip: u64,
    rows_to_take: u64,
    preamble_action: PreambleAction,
}

impl ChunkInstructions {
    // Given a repetition index and a set of user ranges we need to figure out how to read from the chunks
    //
    // We assume that `user_ranges` are in sorted order and non-overlapping
    //
    // The output will be a set of `ChunkInstructions` which tell us how to read from the chunks
    fn schedule_instructions(
        chunk_index: &MiniBlockChunkIndex,
        user_ranges: &[Range<u64>],
    ) -> Vec<Self> {
        // Bind the per-page chunk count once; re-deriving it each iteration
        // costs a width match plus a length read.
        let num_chunks = chunk_index.num_chunks();
        // This is an in-exact capacity guess but pretty good.  The actual capacity can be
        // smaller if instructions are merged.  It can be larger if there are multiple instructions
        // per row which can happen with lists.
        let mut chunk_instructions = Vec::with_capacity(user_ranges.len());

        for user_range in user_ranges {
            let mut rows_needed = user_range.end - user_range.start;
            let mut need_preamble = false;

            // Need to find the first chunk with a first row >= user_range.start.  If there are
            // multiple chunks with the same first row we need to take the first one.
            let mut block_index = chunk_index.find_chunk(user_range.start);

            let mut to_skip = user_range.start - chunk_index.first_row(block_index);

            while rows_needed > 0 || need_preamble {
                // Check if we've gone past the last block (should not happen)
                if block_index >= num_chunks {
                    log::warn!(
                        "schedule_instructions inconsistency: block_index >= num_chunks, exiting early"
                    );
                    break;
                }

                let starts_including_trailer = chunk_index.rows_in_chunk(block_index);
                let has_preamble = chunk_index.has_preamble(block_index);
                let has_trailer = chunk_index.has_trailer(block_index);
                let rows_avail = starts_including_trailer.saturating_sub(to_skip);

                // Handle blocks that are entirely preamble (rows_avail = 0)
                // These blocks have no rows to take but may have a preamble we need
                // We only look for preamble if to_skip == 0 (we're not skipping rows)
                if rows_avail == 0 && to_skip == 0 {
                    // Only process if this chunk has a preamble we need
                    if has_preamble && need_preamble {
                        chunk_instructions.push(Self {
                            chunk_idx: block_index,
                            preamble: PreambleAction::Take,
                            rows_to_skip: 0,
                            rows_to_take: 0,
                            // We still need to look at has_trailer to distinguish between "all preamble
                            // and row ends at end of chunk" and "all preamble and row bleeds into next
                            // chunk".  Both cases will have 0 rows available.
                            take_trailer: has_trailer,
                        });
                        // Only set need_preamble = false if the chunk has at least one row,
                        // Or we are reaching the last block,
                        // Otherwise, the chunk is entirely preamble and we need the next chunk's preamble too
                        if starts_including_trailer > 0 || block_index == num_chunks - 1 {
                            need_preamble = false;
                        }
                    }
                    // Move to next block
                    block_index += 1;
                    continue;
                }

                // Edge case: if rows_avail == 0 but to_skip > 0
                // This theoretically shouldn't happen (binary search should avoid it)
                // but handle it for safety
                if rows_avail == 0 && to_skip > 0 {
                    // This block doesn't have enough rows to skip, move to next block
                    // Adjust to_skip by the number of rows in this block
                    to_skip -= starts_including_trailer;
                    block_index += 1;
                    continue;
                }

                let rows_to_take = rows_avail.min(rows_needed);
                rows_needed -= rows_to_take;

                let mut take_trailer = false;
                let preamble = if has_preamble {
                    if need_preamble {
                        PreambleAction::Take
                    } else {
                        PreambleAction::Skip
                    }
                } else {
                    PreambleAction::Absent
                };

                // Are we taking the trailer?  If so, make sure we mark that we need the preamble
                if rows_to_take == rows_avail && has_trailer {
                    take_trailer = true;
                    need_preamble = true;
                } else {
                    need_preamble = false;
                };

                chunk_instructions.push(Self {
                    preamble,
                    chunk_idx: block_index,
                    rows_to_skip: to_skip,
                    rows_to_take,
                    take_trailer,
                });

                to_skip = 0;
                block_index += 1;
            }
        }

        // If there were multiple ranges we may have multiple instructions for a single chunk.  Merge them now if they
        // are _adjacent_ (i.e. don't merge "take first row of chunk 0" and "take third row of chunk 0" into "take 2
        // rows of chunk 0 starting at 0")
        if user_ranges.len() > 1 {
            // Merge adjacent instructions in place.  `write` indexes the last
            // retained instruction; each following instruction is either folded
            // into it (contiguous within the same chunk) or compacted forward.
            let mut write = 0;
            for read in 1..chunk_instructions.len() {
                let merges = {
                    let last = &chunk_instructions[write];
                    let candidate = &chunk_instructions[read];
                    last.chunk_idx == candidate.chunk_idx
                        && last.rows_to_take + last.rows_to_skip == candidate.rows_to_skip
                };
                if merges {
                    let rows_to_take = chunk_instructions[read].rows_to_take;
                    let take_trailer = chunk_instructions[read].take_trailer;
                    let last = &mut chunk_instructions[write];
                    last.rows_to_take += rows_to_take;
                    last.take_trailer |= take_trailer;
                } else {
                    write += 1;
                    if write != read {
                        chunk_instructions.swap(write, read);
                    }
                }
            }
            chunk_instructions.truncate(write + 1);
        }
        chunk_instructions
    }

    fn drain_from_instruction(
        &self,
        rows_desired: &mut u64,
        need_preamble: &mut bool,
        skip_in_chunk: &mut u64,
    ) -> (ChunkDrainInstructions, bool) {
        // If we need the premable then we shouldn't be skipping anything
        debug_assert!(!*need_preamble || *skip_in_chunk == 0);
        let rows_avail = self.rows_to_take - *skip_in_chunk;
        let has_preamble = self.preamble != PreambleAction::Absent;
        let preamble_action = match (*need_preamble, has_preamble) {
            (true, true) => PreambleAction::Take,
            (true, false) => panic!("Need preamble but there isn't one"),
            (false, true) => PreambleAction::Skip,
            (false, false) => PreambleAction::Absent,
        };

        // How many rows are we actually taking in this take step (including the preamble
        // and trailer both as individual rows)
        let rows_taking = if *rows_desired >= rows_avail {
            // We want all the rows.  If there is a trailer we are grabbing it and will need
            // the preamble of the next chunk
            // If there is a trailer and we are taking all the rows then we need the preamble
            // of the next chunk.
            //
            // Also, if this chunk is entirely preamble (rows_avail == 0 && !take_trailer) then we
            // need the preamble of the next chunk.
            *need_preamble = self.take_trailer;
            rows_avail
        } else {
            // We aren't taking all the rows.  Even if there is a trailer we aren't taking
            // it so we will not need the preamble
            *need_preamble = false;
            *rows_desired
        };
        let rows_skipped = *skip_in_chunk;

        // Update the state for the next iteration
        let consumed_chunk = if *rows_desired >= rows_avail {
            *rows_desired -= rows_avail;
            *skip_in_chunk = 0;
            true
        } else {
            *skip_in_chunk += *rows_desired;
            *rows_desired = 0;
            false
        };

        (
            ChunkDrainInstructions {
                chunk_instructions: self.clone(),
                rows_to_skip: rows_skipped,
                rows_to_take: rows_taking,
                preamble_action,
            },
            consumed_chunk,
        )
    }
}

enum Words {
    U16(ScalarBuffer<u16>),
    U32(ScalarBuffer<u32>),
}

struct WordsIter<'a> {
    iter: Box<dyn Iterator<Item = u32> + 'a>,
}

impl Words {
    pub fn len(&self) -> usize {
        match self {
            Self::U16(b) => b.len(),
            Self::U32(b) => b.len(),
        }
    }

    pub fn iter(&self) -> WordsIter<'_> {
        match self {
            Self::U16(buf) => WordsIter {
                iter: Box::new(buf.iter().map(|&x| x as u32)),
            },
            Self::U32(buf) => WordsIter {
                iter: Box::new(buf.iter().copied()),
            },
        }
    }

    pub fn from_bytes(bytes: Bytes, has_large_chunk: bool) -> Result<Self> {
        let bytes_per_value = if has_large_chunk { 4 } else { 2 };
        assert_eq!(bytes.len() % bytes_per_value, 0);
        let buffer = LanceBuffer::from_bytes(bytes, bytes_per_value as u64);
        if has_large_chunk {
            Ok(Self::U32(buffer.borrow_to_typed_slice::<u32>()))
        } else {
            Ok(Self::U16(buffer.borrow_to_typed_slice::<u16>()))
        }
    }
}

impl<'a> Iterator for WordsIter<'a> {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        self.iter.next()
    }
}

/// Per-chunk leaf value-count analysis derived from the metadata words.
///
/// `values_per_chunk` is the count shared by every non-last chunk (meaningful
/// when `uniform`), and `last_chunk_values` is the final chunk's count.
struct FlatValueCounts {
    logs: Vec<u8>,
    uniform: bool,
    values_per_chunk: u64,
    last_chunk_values: u64,
}

fn analyze_value_counts(words: &Words, items_in_page: u64) -> Result<FlatValueCounts> {
    let num_chunks = words.len();
    let logs = words.iter().map(|w| (w & 0x0F) as u8).collect::<Vec<_>>();
    let mut counted = 0u64;
    for (chunk_index, &log) in logs.iter().take(num_chunks.saturating_sub(1)).enumerate() {
        if log == 0 {
            return Err(Error::corrupt_file_named(
                "miniblock_metadata",
                format!(
                    "non-final chunk {chunk_index} of {num_chunks} has invalid log_num_values=0"
                ),
            ));
        }
        counted = counted.checked_add(1u64 << log).ok_or_else(|| {
            Error::corrupt_file_named(
                "miniblock_metadata",
                format!(
                    "value count overflow at chunk {chunk_index}: counted_values={counted}, \
                     log_num_values={log}, items_in_page={items_in_page}"
                ),
            )
        })?;
    }
    let last_chunk_values = items_in_page.checked_sub(counted).ok_or_else(|| {
        Error::corrupt_file_named(
            "miniblock_metadata",
            format!(
                "non-final chunks account for counted_values={counted}, exceeding \
                 items_in_page={items_in_page}"
            ),
        )
    })?;
    if let Some(&last_log) = logs.last()
        && last_log != 0
        && (1u64 << last_log) != last_chunk_values
    {
        return Err(Error::corrupt_file_named(
            "miniblock_metadata",
            format!(
                "final chunk log_num_values={last_log} does not match \
                 last_chunk_values={last_chunk_values}: counted_values={counted}, \
                 items_in_page={items_in_page}"
            ),
        ));
    }
    let uniform = num_chunks <= 1 || logs[..num_chunks - 1].iter().all(|&log| log == logs[0]);
    // A single-chunk page has no "non-last" chunk to derive a stride from; use the
    // page item count (min 1 so it stays a valid divisor in `find_chunk`).
    let values_per_chunk = if num_chunks <= 1 {
        items_in_page.max(1)
    } else {
        1u64 << logs[0]
    };
    Ok(FlatValueCounts {
        logs,
        uniform,
        values_per_chunk,
        last_chunk_values,
    })
}

/// Iterator over per-chunk value counts for a non-uniform flat page.  Non-last
/// chunks yield `1 << log`; the last yields the validated remaining item count.
fn flat_value_counts_iter(logs: &[u8], last_chunk_values: u64) -> impl Iterator<Item = u64> + '_ {
    let num_chunks = logs.len();
    (0..num_chunks).map(move |i| {
        if i + 1 < num_chunks {
            1u64 << logs[i]
        } else {
            last_chunk_values
        }
    })
}

/// Builds the compact per-chunk index from the metadata words and, for nested
/// pages, the raw repetition-index bytes.  The row axis is picked by page shape:
/// `UniformFlat` when all non-last chunks share a value count (fixed-width /
/// bitpacking), `Flat` for non-uniform flat pages (RLE / FSST), else `Nested`.
fn build_chunk_index(
    words: &Words,
    items_in_page: u64,
    base: u64,
    data_buf_size: u64,
    rep_index_bytes: Option<&[u8]>,
    repetition_index_depth: u16,
) -> Result<MiniBlockChunkIndex> {
    let num_chunks = words.len();
    // Validate item counts before byte sizes because both share a metadata word,
    // and an invalid count must not reach the final-chunk subtraction.
    let value_counts = analyze_value_counts(words, items_in_page)?;

    // Each chunk stores `(divided_bytes + 1) * MINIBLOCK_ALIGNMENT` bytes, so the
    // deltas are the chunk sizes and their grand total is the data buffer size.
    let byte_starts = PrefixSums::from_deltas(
        words
            .iter()
            .map(|word| ((word >> 4) as u64 + 1) * MINIBLOCK_ALIGNMENT as u64),
        num_chunks,
        data_buf_size,
    );

    // Nested pages track rows via the repetition index and keep leaf item counts
    // separately; flat pages have row == value index, so value counts are rows.
    let rows = if let Some(rep_index_data) = rep_index_bytes {
        assert!(rep_index_data.len() % 8 == 0);
        let stride = repetition_index_depth as usize + 1;
        let (row_starts, has_trailer) = parse_nested_rep(rep_index_data, stride);
        let item_counts = if value_counts.uniform {
            ItemCounts::Uniform {
                values_per_chunk: value_counts.values_per_chunk,
                last_chunk_values: value_counts.last_chunk_values,
            }
        } else {
            ItemCounts::PerChunkLog {
                logs: value_counts.logs,
                last_chunk_values: value_counts.last_chunk_values,
            }
        };
        RowMapping::Nested {
            row_starts,
            has_trailer,
            item_counts,
        }
    } else {
        if value_counts.uniform {
            RowMapping::UniformFlat {
                values_per_chunk: value_counts.values_per_chunk,
                last_chunk_values: value_counts.last_chunk_values,
                num_chunks,
            }
        } else {
            let value_starts = PrefixSums::from_deltas(
                flat_value_counts_iter(&value_counts.logs, value_counts.last_chunk_values),
                num_chunks,
                items_in_page,
            );
            RowMapping::Flat { value_starts }
        }
    };

    Ok(MiniBlockChunkIndex::new(base, byte_starts, rows))
}

impl StructuralPageScheduler for MiniBlockScheduler {
    fn init_layout(&self) -> Result<PageInitialization> {
        // We always need to fetch chunk metadata.  We may also need to fetch a dictionary and
        // we may also need to fetch the repetition index.  Here, we gather what buffers we
        // need.
        let (meta_buf_position, meta_buf_size) = self.buffer_offsets_and_sizes[0];
        let dictionary_range = self.dictionary.as_ref().map(|dictionary| {
            dictionary.dictionary_buf_position_and_size.0
                ..dictionary.dictionary_buf_position_and_size.0
                    + dictionary.dictionary_buf_position_and_size.1
        });
        let repetition_index_range = if self.repetition_index_depth > 0 {
            let (rep_index_pos, rep_index_size) =
                self.buffer_offsets_and_sizes.last().ok_or_else(|| {
                    Error::invalid_input_source(
                        "Mini-block page has a repetition index but no buffers".into(),
                    )
                })?;
            Some(*rep_index_pos..*rep_index_pos + *rep_index_size)
        } else {
            None
        };
        Ok(PageInitialization::new([
            Some(meta_buf_position..meta_buf_position + meta_buf_size),
            dictionary_range,
            repetition_index_range,
        ]))
    }

    fn init_from_buffers<'a>(
        &'a mut self,
        buffers: PageInitializationBuffers,
        _io: &Arc<dyn EncodingsIo>,
    ) -> BoxFuture<'a, Result<Arc<dyn CachedPageData>>> {
        let base = self.buffer_offsets_and_sizes[1].0;
        let data_buf_size = self.buffer_offsets_and_sizes[1].1;

        async move {
            let [meta_bytes, dictionary_bytes, rep_index_bytes] =
                buffers.try_into_array("mini-block")?;
            let meta_bytes = meta_bytes.ok_or_else(|| {
                Error::internal("Mini-block page initialization is missing chunk metadata")
            })?;

            let words = Words::from_bytes(meta_bytes, self.has_large_chunk)?;
            let chunk_index = build_chunk_index(
                &words,
                self.items_in_page,
                base,
                data_buf_size,
                rep_index_bytes.as_deref(),
                self.repetition_index_depth,
            )?;

            // decode dictionary
            let dictionary = match (self.dictionary.as_ref(), dictionary_bytes) {
                (Some(dictionary), Some(dictionary_data)) => {
                    Some(Arc::new(dictionary.dictionary_decompressor.decompress(
                        Some(LanceBuffer::from_bytes(
                            dictionary_data,
                            dictionary.dictionary_data_alignment,
                        )),
                        dictionary.num_dictionary_items,
                    )?))
                }
                (None, None) => None,
                _ => {
                    return Err(Error::internal(
                        "Mini-block page initialization dictionary slot does not match its layout",
                    ));
                }
            };

            let page_meta = Arc::new(MiniBlockCacheableState {
                chunk_index,
                dictionary,
            });
            Ok(page_meta as Arc<dyn CachedPageData>)
        }
        .boxed()
    }

    fn try_load(&mut self, data: &Arc<dyn CachedPageData>) -> Result<()> {
        self.page_meta = Some(data.clone().as_arc_any().downcast().map_err(|_| {
            Error::invalid_input_source("Cached mini-block page data has an unexpected type".into())
        })?);
        Ok(())
    }

    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>> {
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum();

        let page_meta = self.page_meta.as_ref().unwrap();

        let chunk_instructions =
            ChunkInstructions::schedule_instructions(&page_meta.chunk_index, ranges);

        debug_assert_eq!(
            num_rows,
            chunk_instructions
                .iter()
                .map(|ci| ci.rows_to_take)
                .sum::<u64>()
        );

        let chunks_needed = chunk_instructions
            .iter()
            .map(|ci| ci.chunk_idx)
            .unique()
            .collect::<Vec<_>>();

        let mut loaded_chunks = self.lookup_chunks(&chunks_needed);
        let chunk_ranges = loaded_chunks
            .iter()
            .map(|c| c.byte_range.clone())
            .collect::<Vec<_>>();
        let loaded_chunk_data = io.submit_request(chunk_ranges, self.priority);

        let rep_decompressor = self.rep_decompressor.clone();
        let def_decompressor = self.def_decompressor.clone();
        let value_decompressor = self.value_decompressor.clone();
        let num_buffers = self.num_buffers;
        let has_large_chunk = self.has_large_chunk;
        let dictionary = page_meta
            .dictionary
            .as_ref()
            .map(|dictionary| dictionary.clone());
        let def_meaning = self.def_meaning.clone();

        let res = async move {
            let loaded_chunk_data = loaded_chunk_data.await?;
            for (loaded_chunk, chunk_data) in loaded_chunks.iter_mut().zip(loaded_chunk_data) {
                loaded_chunk.data = LanceBuffer::from_bytes(chunk_data, 1);
            }

            Ok(Box::new(MiniBlockDecoder {
                rep_decompressor,
                def_decompressor,
                value_decompressor,
                def_meaning,
                loaded_chunks: VecDeque::from_iter(loaded_chunks),
                instructions: VecDeque::from(chunk_instructions),
                offset_in_current_chunk: 0,
                dictionary,
                num_rows,
                num_buffers,
                has_large_chunk,
            }) as Box<dyn StructuralPageDecoder>)
        }
        .boxed();
        let page_load_task = PageLoadTask {
            decoder_fut: res,
            num_rows,
        };
        Ok(vec![page_load_task])
    }
}

#[derive(Debug, Clone, Copy)]
struct FullZipRepIndexDetails {
    buf_position: u64,
    bytes_per_value: u64, // Will be 1, 2, 4, or 8
}

#[derive(Debug)]
enum PerValueDecompressor {
    Fixed(Arc<dyn FixedPerValueDecompressor>),
    Variable(Arc<dyn VariablePerValueDecompressor>),
}

#[derive(Debug)]
struct FullZipDecodeDetails {
    value_decompressor: PerValueDecompressor,
    def_meaning: Arc<[DefinitionInterpretation]>,
    ctrl_word_parser: ControlWordParser,
    max_rep: u16,
    max_visible_def: u16,
}

/// Describes where FullZip byte ranges should be read from.
///
/// FullZip decoding always needs a list of byte ranges, but those bytes can come
/// from two different places:
/// - Remote I/O (normal path): ranges are fetched from the underlying `EncodingsIo`.
/// - A prefetched full page (full scan fast path): the entire page has already been
///   loaded once and ranges should be sliced from memory.
///
/// This abstraction keeps scheduling code focused on "which ranges are needed"
/// instead of "how bytes are fetched", and it lets full-page scans avoid the
/// two-stage rep-index -> data I/O pipeline.
#[derive(Debug, Clone)]
enum FullZipReadSource {
    /// Fetch ranges from the storage backend through the encoding I/O interface.
    Remote(Arc<dyn EncodingsIo>),
    /// Slice ranges from an already-loaded FullZip page buffer.
    PrefetchedPage { base_offset: u64, data: LanceBuffer },
}

impl FullZipReadSource {
    /// Materialize the requested ranges as decode-ready `LanceBuffer`s.
    ///
    /// The returned buffers preserve the input range order.
    fn fetch(
        &self,
        ranges: &[Range<u64>],
        priority: u64,
    ) -> BoxFuture<'static, Result<VecDeque<LanceBuffer>>> {
        match self {
            Self::Remote(io) => {
                let io = io.clone();
                let ranges = ranges.to_vec();
                async move {
                    let data = io.submit_request(ranges, priority).await?;
                    Ok(data
                        .into_iter()
                        .map(|bytes| LanceBuffer::from_bytes(bytes, 1))
                        .collect::<VecDeque<_>>())
                }
                .boxed()
            }
            Self::PrefetchedPage { base_offset, data } => {
                let base_offset = *base_offset;
                let data = data.clone();
                let page_end = base_offset + data.len() as u64;
                std::future::ready(
                    ranges
                        .iter()
                        .map(|range| {
                            if range.start > range.end
                                || range.start < base_offset
                                || range.end > page_end
                            {
                                return Err(Error::internal(format!(
                                    "Requested range {:?} is outside page range {}..{}",
                                    range, base_offset, page_end
                                )));
                            }
                            let start = (range.start - base_offset) as usize;
                            let len = (range.end - range.start) as usize;
                            Ok(data.slice_with_length(start, len))
                        })
                        .collect::<Result<VecDeque<_>>>(),
                )
                .boxed()
            }
        }
    }
}

/// A scheduler for full-zip encoded data
///
/// When the data type has a fixed-width then we simply need to map from
/// row ranges to byte ranges using the fixed-width of the data type.
///
/// When the data type is variable-width or has any repetition then a
/// repetition index is required.
#[derive(Debug)]
pub struct FullZipScheduler {
    data_buf_position: u64,
    data_buf_size: u64,
    rep_index: Option<FullZipRepIndexDetails>,
    priority: u64,
    rows_in_page: u64,
    bits_per_offset: u8,
    details: Arc<FullZipDecodeDetails>,
    /// Cached state containing the decoded repetition index
    cached_state: Option<Arc<FullZipCacheableState>>,
    /// Whether repetition index metadata should be cached during initialize.
    enable_cache: bool,
}

impl FullZipScheduler {
    fn try_new(
        buffer_offsets_and_sizes: &[(u64, u64)],
        priority: u64,
        rows_in_page: u64,
        layout: &pb21::FullZipLayout,
        decompressors: &dyn DecompressionStrategy,
    ) -> Result<Self> {
        let (data_buf_position, data_buf_size) = buffer_offsets_and_sizes[0];
        let rep_index = buffer_offsets_and_sizes.get(1).map(|(pos, len)| {
            let num_reps = rows_in_page + 1;
            let bytes_per_rep = len / num_reps;
            debug_assert_eq!(len % num_reps, 0);
            debug_assert!(
                bytes_per_rep == 1
                    || bytes_per_rep == 2
                    || bytes_per_rep == 4
                    || bytes_per_rep == 8
            );
            FullZipRepIndexDetails {
                buf_position: *pos,
                bytes_per_value: bytes_per_rep,
            }
        });

        let value_decompressor = match layout.details {
            Some(pb21::full_zip_layout::Details::BitsPerValue(_)) => {
                let decompressor = decompressors.create_fixed_per_value_decompressor(
                    layout.value_compression.as_ref().unwrap(),
                )?;
                PerValueDecompressor::Fixed(decompressor.into())
            }
            Some(pb21::full_zip_layout::Details::BitsPerOffset(_)) => {
                let decompressor = decompressors.create_variable_per_value_decompressor(
                    layout.value_compression.as_ref().unwrap(),
                )?;
                PerValueDecompressor::Variable(decompressor.into())
            }
            None => {
                panic!("Full-zip layout must have a `details` field");
            }
        };
        let ctrl_word_parser = ControlWordParser::new(
            layout.bits_rep.try_into().unwrap(),
            layout.bits_def.try_into().unwrap(),
        );
        let def_meaning = layout
            .layers
            .iter()
            .map(|l| ProtobufUtils21::repdef_layer_to_def_interp(*l))
            .collect::<Vec<_>>();

        let max_rep = def_meaning.iter().filter(|d| d.is_list()).count() as u16;
        let max_visible_def = max_visible_level(&def_meaning);

        let bits_per_offset = match layout.details {
            Some(pb21::full_zip_layout::Details::BitsPerValue(_)) => 32,
            Some(pb21::full_zip_layout::Details::BitsPerOffset(bits_per_offset)) => {
                bits_per_offset as u8
            }
            None => panic!("Full-zip layout must have a `details` field"),
        };

        let details = Arc::new(FullZipDecodeDetails {
            value_decompressor,
            def_meaning: def_meaning.into(),
            ctrl_word_parser,
            max_rep,
            max_visible_def,
        });
        Ok(Self {
            data_buf_position,
            data_buf_size,
            rep_index,
            details,
            priority,
            rows_in_page,
            bits_per_offset,
            cached_state: None,
            enable_cache: false,
        })
    }

    fn covers_entire_page(ranges: &[Range<u64>], rows_in_page: u64) -> bool {
        if ranges.is_empty() {
            return false;
        }
        let mut expected_start = 0;
        for range in ranges {
            if range.start != expected_start || range.end > rows_in_page || range.end < range.start
            {
                return false;
            }
            expected_start = range.end;
        }
        expected_start == rows_in_page
    }

    fn create_page_load_task(
        io_future: BoxFuture<'static, Result<Vec<Bytes>>>,
        num_rows: u64,
        details: Arc<FullZipDecodeDetails>,
        bits_per_offset: u8,
    ) -> PageLoadTask {
        let load_task = async move {
            let buffers = io_future.await?;
            let data = buffers
                .into_iter()
                .map(|bytes| LanceBuffer::from_bytes(bytes, 1))
                .collect::<VecDeque<_>>();
            Self::create_decoder(details, data, num_rows, bits_per_offset)
        }
        .boxed();
        PageLoadTask {
            decoder_fut: load_task,
            num_rows,
        }
    }

    /// Creates a decoder from the loaded data
    fn create_decoder(
        details: Arc<FullZipDecodeDetails>,
        data: VecDeque<LanceBuffer>,
        num_rows: u64,
        bits_per_offset: u8,
    ) -> Result<Box<dyn StructuralPageDecoder>> {
        match &details.value_decompressor {
            PerValueDecompressor::Fixed(decompressor) => {
                let bits_per_value = decompressor.bits_per_value();
                if bits_per_value % 8 != 0 {
                    return Err(lance_core::Error::not_supported_source("Bit-packed full-zip encoding (non-byte-aligned values) is not yet implemented".into()));
                }
                let bytes_per_value = bits_per_value / 8;
                let total_bytes_per_value =
                    bytes_per_value as usize + details.ctrl_word_parser.bytes_per_word();
                // total_bytes_per_value == 0 is valid for constant-null FSL pages written by
                // earlier encoders that produced bits_per_value=0 with no ctrl-word bytes.
                // FixedFullZipDecoder::drain handles this case by producing AllNull output
                // without touching the (empty) data buffer.
                Ok(Box::new(FixedFullZipDecoder {
                    details,
                    data,
                    num_rows,
                    offset_in_current: 0,
                    bytes_per_value: bytes_per_value as usize,
                    total_bytes_per_value,
                }) as Box<dyn StructuralPageDecoder>)
            }
            PerValueDecompressor::Variable(_decompressor) => {
                Ok(Box::new(VariableFullZipDecoder::new(
                    details,
                    data,
                    num_rows,
                    bits_per_offset,
                    bits_per_offset,
                )?))
            }
        }
    }

    /// Extracts byte ranges from a repetition index buffer
    /// The buffer contains pairs of (start, end) values for each range
    fn extract_byte_ranges_from_pairs(
        buffer: LanceBuffer,
        bytes_per_value: u64,
        data_buf_position: u64,
    ) -> Vec<Range<u64>> {
        ByteUnpacker::new(buffer, bytes_per_value as usize)
            .chunks(2)
            .into_iter()
            .map(|mut c| {
                let start = c.next().unwrap() + data_buf_position;
                let end = c.next().unwrap() + data_buf_position;
                start..end
            })
            .collect::<Vec<_>>()
    }

    /// Extracts byte ranges from a cached repetition index buffer
    /// The buffer contains all values and we need to extract specific ranges
    fn extract_byte_ranges_from_cached(
        buffer: &LanceBuffer,
        ranges: &[Range<u64>],
        bytes_per_value: u64,
        data_buf_position: u64,
    ) -> Vec<Range<u64>> {
        ranges
            .iter()
            .map(|r| {
                let start_offset = (r.start * bytes_per_value) as usize;
                let end_offset = (r.end * bytes_per_value) as usize;

                let start_slice = &buffer[start_offset..start_offset + bytes_per_value as usize];
                let start_val =
                    ByteUnpacker::new(start_slice.iter().copied(), bytes_per_value as usize)
                        .next()
                        .unwrap();

                let end_slice = &buffer[end_offset..end_offset + bytes_per_value as usize];
                let end_val =
                    ByteUnpacker::new(end_slice.iter().copied(), bytes_per_value as usize)
                        .next()
                        .unwrap();

                (data_buf_position + start_val)..(data_buf_position + end_val)
            })
            .collect()
    }

    /// Computes the ranges in the repetition index that need to be loaded
    fn compute_rep_index_ranges(
        ranges: &[Range<u64>],
        rep_index: &FullZipRepIndexDetails,
    ) -> Vec<Range<u64>> {
        ranges
            .iter()
            .flat_map(|r| {
                let first_val_start =
                    rep_index.buf_position + (r.start * rep_index.bytes_per_value);
                let first_val_end = first_val_start + rep_index.bytes_per_value;
                let last_val_start = rep_index.buf_position + (r.end * rep_index.bytes_per_value);
                let last_val_end = last_val_start + rep_index.bytes_per_value;
                [first_val_start..first_val_end, last_val_start..last_val_end]
            })
            .collect()
    }

    /// Schedules ranges in the presence of a repetition index
    fn schedule_ranges_rep(
        &self,
        ranges: &[Range<u64>],
        io: &Arc<dyn EncodingsIo>,
        rep_index: FullZipRepIndexDetails,
    ) -> Result<Vec<PageLoadTask>> {
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum();
        let data_buf_position = self.data_buf_position;
        let priority = self.priority;
        let details = self.details.clone();
        let bits_per_offset = self.bits_per_offset;

        if Self::covers_entire_page(ranges, self.rows_in_page) {
            let full_range = self.data_buf_position..(self.data_buf_position + self.data_buf_size);
            let page_data = io.submit_single(full_range.clone(), priority);
            let load_task = async move {
                let page_data = page_data.await?;
                let source = FullZipReadSource::PrefetchedPage {
                    base_offset: full_range.start,
                    data: LanceBuffer::from_bytes(page_data, 1),
                };
                let read_ranges = vec![full_range];
                let data = source.fetch(&read_ranges, priority).await?;
                Self::create_decoder(details, data, num_rows, bits_per_offset)
            }
            .boxed();
            let page_load_task = PageLoadTask {
                decoder_fut: load_task,
                num_rows,
            };
            return Ok(vec![page_load_task]);
        }

        if let Some(cached_state) = &self.cached_state {
            let byte_ranges = Self::extract_byte_ranges_from_cached(
                &cached_state.rep_index_buffer,
                ranges,
                rep_index.bytes_per_value,
                data_buf_position,
            );
            let io_future = io.submit_request(byte_ranges, priority);
            let page_load_task =
                Self::create_page_load_task(io_future, num_rows, details, bits_per_offset);
            return Ok(vec![page_load_task]);
        }

        let rep_ranges = Self::compute_rep_index_ranges(ranges, &rep_index);
        let rep_data = io.submit_request(rep_ranges, priority);
        let io_clone = io.clone();
        let load_task = async move {
            let rep_data = rep_data.await?;
            let rep_buffer = LanceBuffer::concat(
                &rep_data
                    .into_iter()
                    .map(|d| LanceBuffer::from_bytes(d, 1))
                    .collect::<Vec<_>>(),
            );
            let byte_ranges = Self::extract_byte_ranges_from_pairs(
                rep_buffer,
                rep_index.bytes_per_value,
                data_buf_position,
            );
            let source = FullZipReadSource::Remote(io_clone);
            let data = source.fetch(&byte_ranges, priority).await?;
            Self::create_decoder(details, data, num_rows, bits_per_offset)
        }
        .boxed();
        let page_load_task = PageLoadTask {
            decoder_fut: load_task,
            num_rows,
        };
        Ok(vec![page_load_task])
    }

    // In the simple case there is no repetition and we just have large fixed-width
    // rows of data.  We can just map row ranges to byte ranges directly using the
    // fixed-width of the data type.
    fn schedule_ranges_simple(
        &self,
        ranges: &[Range<u64>],
        io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>> {
        // Convert row ranges to item ranges (i.e. multiply by items per row)
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum();

        let PerValueDecompressor::Fixed(decompressor) = &self.details.value_decompressor else {
            unreachable!()
        };

        // Convert item ranges to byte ranges (i.e. multiply by bytes per item)
        let bits_per_value = decompressor.bits_per_value();
        if !bits_per_value.is_multiple_of(8) {
            return Err(Error::invalid_input_source(
                format!(
                    "Full-zip fixed-width values must be byte aligned, got {} bits per value",
                    bits_per_value
                )
                .into(),
            ));
        }
        let bytes_per_value = bits_per_value / 8;
        let bytes_per_cw = self.details.ctrl_word_parser.bytes_per_word();
        let total_bytes_per_value = bytes_per_value + bytes_per_cw as u64;
        let byte_ranges = ranges
            .iter()
            .map(|r| {
                debug_assert!(r.end <= self.rows_in_page);
                let start = self.data_buf_position + r.start * total_bytes_per_value;
                let end = self.data_buf_position + r.end * total_bytes_per_value;
                start..end
            })
            .collect::<Vec<_>>();

        let io_future = io.submit_request(byte_ranges, self.priority);
        let page_load_task = Self::create_page_load_task(
            io_future,
            num_rows,
            self.details.clone(),
            self.bits_per_offset,
        );
        Ok(vec![page_load_task])
    }
}

/// Cacheable state for FullZip encoding, storing the decoded repetition index
#[derive(Debug)]
struct FullZipCacheableState {
    /// The raw repetition index buffer for future decoding
    rep_index_buffer: LanceBuffer,
}

impl DeepSizeOf for FullZipCacheableState {
    fn deep_size_of_children(&self, _context: &mut Context) -> usize {
        self.rep_index_buffer.len()
    }
}

impl CachedPageData for FullZipCacheableState {
    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self
    }
}

impl StructuralPageScheduler for FullZipScheduler {
    fn needs_initialization(&self) -> bool {
        self.enable_cache && self.rep_index.is_some()
    }

    fn init_layout(&self) -> Result<PageInitialization> {
        let repetition_index_range = if self.enable_cache
            && let Some(rep_index) = self.rep_index
        {
            let total_size = (self.rows_in_page + 1) * rep_index.bytes_per_value;
            Some(rep_index.buf_position..(rep_index.buf_position + total_size))
        } else {
            None
        };
        Ok(PageInitialization::new([repetition_index_range]))
    }

    fn init_from_buffers<'a>(
        &'a mut self,
        buffers: PageInitializationBuffers,
        _io: &Arc<dyn EncodingsIo>,
    ) -> BoxFuture<'a, Result<Arc<dyn CachedPageData>>> {
        let [rep_index_buffer] = match buffers.try_into_array("full-zip") {
            Ok(buffers) => buffers,
            Err(error) => return std::future::ready(Err(error)).boxed(),
        };
        if let Some(rep_index_buffer) = rep_index_buffer {
            let state = Arc::new(FullZipCacheableState {
                rep_index_buffer: LanceBuffer::from_bytes(rep_index_buffer, 1),
            });
            std::future::ready(Ok(state as Arc<dyn CachedPageData>)).boxed()
        } else {
            std::future::ready(Ok(Arc::new(NoCachedPageData) as Arc<dyn CachedPageData>)).boxed()
        }
    }

    /// Loads previously cached repetition index data from the cache system.
    /// This method is called when a scheduler instance needs to use cached data
    /// that was initialized by another instance or in a previous operation.
    fn try_load(&mut self, cache: &Arc<dyn CachedPageData>) -> Result<()> {
        self.cached_state = Some(cache.clone().as_arc_any().downcast().map_err(|_| {
            Error::invalid_input_source("Cached full-zip page data has an unexpected type".into())
        })?);
        Ok(())
    }

    fn schedule_ranges(
        &self,
        ranges: &[Range<u64>],
        io: &Arc<dyn EncodingsIo>,
    ) -> Result<Vec<PageLoadTask>> {
        if let Some(rep_index) = self.rep_index {
            self.schedule_ranges_rep(ranges, io, rep_index)
        } else {
            self.schedule_ranges_simple(ranges, io)
        }
    }
}

/// A decoder for full-zip encoded data when the data has a fixed-width
///
/// Here we need to unzip the control words from the values themselves and
/// then decompress the requested values.
///
/// We use a PerValueDecompressor because we will only be decompressing the
/// requested data.  This decoder / scheduler does not do any read amplification.
#[derive(Debug)]
struct FixedFullZipDecoder {
    details: Arc<FullZipDecodeDetails>,
    data: VecDeque<LanceBuffer>,
    offset_in_current: usize,
    bytes_per_value: usize,
    total_bytes_per_value: usize,
    num_rows: u64,
}

impl FixedFullZipDecoder {
    fn slice_next_task(&mut self, num_rows: u64) -> Result<FullZipDecodeTaskItem> {
        debug_assert!(num_rows > 0);
        let cur_buf = self.data.front_mut().ok_or_else(|| {
            Error::corrupt_file_named(
                "fixed_full_zip",
                format!("page data ran out with {num_rows} row(s) left to decode"),
            )
        })?;
        let start = self.offset_in_current;
        if self.details.ctrl_word_parser.has_rep() {
            // This is a slightly slower path.  In order to figure out where to split we need to
            // examine the rep index so we can convert num_lists to num_rows
            let mut rows_started = 0;
            // We always need at least one value.  Now loop through until we have passed num_rows
            // values
            let mut num_items = 0;
            let bytes_per_word = self.details.ctrl_word_parser.bytes_per_word();
            while self.offset_in_current < cur_buf.len() {
                // The item walk is driven by the control words themselves, so a page that
                // disagrees with its own layout can step past the end of the buffer.  Check
                // before every read and every advance so that a malformed page is reported
                // instead of panicking in `slice_with_length`.
                let bytes_avail = cur_buf.len() - self.offset_in_current;
                if bytes_avail < bytes_per_word {
                    return Err(Error::corrupt_file_named(
                        "fixed_full_zip",
                        format!(
                            "truncated control word: {bytes_avail} byte(s) remain in the page \
                             buffer but a control word requires {bytes_per_word}"
                        ),
                    ));
                }
                let control = self.details.ctrl_word_parser.parse_desc(
                    &cur_buf[self.offset_in_current..],
                    self.details.max_rep,
                    self.details.max_visible_def,
                );
                if control.is_new_row {
                    if rows_started == num_rows {
                        break;
                    }
                    rows_started += 1;
                }
                num_items += 1;
                let bytes_in_item = if control.is_visible {
                    self.total_bytes_per_value
                } else {
                    bytes_per_word
                };
                if bytes_in_item > bytes_avail {
                    return Err(Error::corrupt_file_named(
                        "fixed_full_zip",
                        format!(
                            "truncated item: {bytes_avail} byte(s) remain in the page buffer at \
                             offset {} but the item requires {bytes_in_item}",
                            self.offset_in_current
                        ),
                    ));
                }
                self.offset_in_current += bytes_in_item;
            }

            let task_slice = cur_buf.slice_with_length(start, self.offset_in_current - start);
            if self.offset_in_current == cur_buf.len() {
                self.data.pop_front();
                self.offset_in_current = 0;
            }

            Ok(FullZipDecodeTaskItem {
                data: PerValueDataBlock::Fixed(FixedWidthDataBlock {
                    data: task_slice,
                    bits_per_value: self.bytes_per_value as u64 * 8,
                    num_values: num_items,
                    block_info: BlockInfo::new(),
                }),
                rows_in_buf: rows_started,
            })
        } else {
            // If there's no repetition we can calculate the slicing point by just multiplying
            // the number of rows by the total bytes per value
            let cur_buf = self.data.front_mut().unwrap();
            let bytes_avail = cur_buf.len() - self.offset_in_current;
            let offset_in_cur = self.offset_in_current;

            let bytes_needed = num_rows as usize * self.total_bytes_per_value;
            let mut rows_taken = num_rows;
            let task_slice = if bytes_needed >= bytes_avail {
                self.offset_in_current = 0;
                rows_taken = bytes_avail as u64 / self.total_bytes_per_value as u64;
                self.data
                    .pop_front()
                    .unwrap()
                    .slice_with_length(offset_in_cur, bytes_avail)
            } else {
                self.offset_in_current += bytes_needed;
                cur_buf.slice_with_length(offset_in_cur, bytes_needed)
            };
            Ok(FullZipDecodeTaskItem {
                data: PerValueDataBlock::Fixed(FixedWidthDataBlock {
                    data: task_slice,
                    bits_per_value: self.bytes_per_value as u64 * 8,
                    num_values: rows_taken,
                    block_info: BlockInfo::new(),
                }),
                rows_in_buf: rows_taken,
            })
        }
    }
}

impl StructuralPageDecoder for FixedFullZipDecoder {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn DecodePageTask>> {
        if self.total_bytes_per_value == 0 {
            // No bytes per row: constant-null page with no ctrl-word bytes.
            // The decompressor (ConstantDecompressor) ignores its input and returns AllNull.
            return Ok(Box::new(FixedFullZipDecodeTask {
                details: self.details.clone(),
                data: vec![FullZipDecodeTaskItem {
                    data: PerValueDataBlock::Fixed(FixedWidthDataBlock {
                        data: LanceBuffer::empty(),
                        bits_per_value: 0,
                        num_values: num_rows,
                        block_info: BlockInfo::new(),
                    }),
                    rows_in_buf: num_rows,
                }],
                bytes_per_value: 0,
                num_rows: num_rows as usize,
            }));
        }
        let mut task_data = Vec::with_capacity(self.data.len());
        let mut remaining = num_rows;
        while remaining > 0 {
            let task_item = self.slice_next_task(remaining)?;
            remaining -= task_item.rows_in_buf;
            task_data.push(task_item);
        }
        Ok(Box::new(FixedFullZipDecodeTask {
            details: self.details.clone(),
            data: task_data,
            bytes_per_value: self.bytes_per_value,
            num_rows: num_rows as usize,
        }))
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

/// A decoder for full-zip encoded data when the data has a variable-width
///
/// Here we need to unzip the control words AND lengths from the values and
/// then decompress the requested values.
#[derive(Debug)]
struct VariableFullZipDecoder {
    details: Arc<FullZipDecodeDetails>,
    decompressor: Arc<dyn VariablePerValueDecompressor>,
    data: LanceBuffer,
    offsets: LanceBuffer,
    rep: ScalarBuffer<u16>,
    def: ScalarBuffer<u16>,
    repdef_starts: Vec<usize>,
    data_starts: Vec<usize>,
    offset_starts: Vec<usize>,
    visible_item_counts: Vec<u64>,
    bits_per_offset: u8,
    current_idx: usize,
    num_rows: u64,
}

impl VariableFullZipDecoder {
    fn new(
        details: Arc<FullZipDecodeDetails>,
        data: VecDeque<LanceBuffer>,
        num_rows: u64,
        in_bits_per_length: u8,
        out_bits_per_offset: u8,
    ) -> Result<Self> {
        let decompressor = match details.value_decompressor {
            PerValueDecompressor::Variable(ref d) => d.clone(),
            _ => unreachable!(),
        };

        assert_eq!(in_bits_per_length % 8, 0);
        assert!(out_bits_per_offset == 32 || out_bits_per_offset == 64);

        let mut decoder = Self {
            details,
            decompressor,
            data: LanceBuffer::empty(),
            offsets: LanceBuffer::empty(),
            rep: LanceBuffer::empty().borrow_to_typed_slice(),
            def: LanceBuffer::empty().borrow_to_typed_slice(),
            bits_per_offset: out_bits_per_offset,
            repdef_starts: Vec::with_capacity(num_rows as usize + 1),
            data_starts: Vec::with_capacity(num_rows as usize + 1),
            offset_starts: Vec::with_capacity(num_rows as usize + 1),
            visible_item_counts: Vec::with_capacity(num_rows as usize + 1),
            current_idx: 0,
            num_rows,
        };

        // There's no great time to do this and this is the least worst time.  If we don't unzip then
        // we can't slice the data during the decode phase.  This is because we need the offsets to be
        // unpacked to know where the values start and end.
        //
        // We don't want to unzip on the decode thread because that is a single-threaded path
        // We don't want to unzip on the scheduling thread because that is a single-threaded path
        //
        // Fortunately, we know variable length data will always be read indirectly and so we can do it
        // here, which should be on the indirect thread.  The primary disadvantage to doing it here is that
        // we load all the data into memory and then throw it away only to load it all into memory again during
        // the decode.
        //
        // There are some alternatives to investigate:
        //   - Instead of just reading the beginning and end of the rep index we could read the entire
        //     range in between.  This will give us the break points that we need for slicing and won't increase
        //     the number of IOPs but it will mean we are doing more total I/O and we need to load the rep index
        //     even when doing a full scan.
        //   - We could force each decode task to do a full unzip of all the data.  Each decode task now
        //     has to do more work but the work is all fused.
        //   - We could just try doing this work on the decode thread and see if it is a problem.
        decoder.unzip(data, in_bits_per_length, out_bits_per_offset, num_rows)?;

        Ok(decoder)
    }

    fn slice_batch_data_and_rebase_offsets_typed<T>(
        data: &LanceBuffer,
        offsets: &LanceBuffer,
    ) -> Result<(LanceBuffer, LanceBuffer)>
    where
        T: arrow_buffer::ArrowNativeType
            + Copy
            + PartialOrd
            + std::ops::Sub<Output = T>
            + std::fmt::Display
            + TryInto<usize>,
    {
        let offsets_slice = offsets.borrow_to_typed_slice::<T>();
        let offsets_slice = offsets_slice.as_ref();
        if offsets_slice.is_empty() {
            return Err(Error::internal(
                "Variable offsets cannot be empty".to_string(),
            ));
        }

        let base = offsets_slice[0];
        let end = *offsets_slice.last().unwrap();
        if end < base {
            return Err(Error::internal(format!(
                "Invalid variable offsets: end ({end}) is less than base ({base})"
            )));
        }

        let data_start = base.try_into().map_err(|_| {
            Error::internal(format!("Variable offset ({base}) does not fit into usize"))
        })?;
        let data_end = end.try_into().map_err(|_| {
            Error::internal(format!("Variable offset ({end}) does not fit into usize"))
        })?;
        if data_end > data.len() {
            return Err(Error::internal(format!(
                "Invalid variable offsets: end ({data_end}) exceeds data len ({})",
                data.len()
            )));
        }

        let mut rebased_offsets = Vec::with_capacity(offsets_slice.len());
        for &offset in offsets_slice {
            if offset < base {
                return Err(Error::internal(format!(
                    "Invalid variable offsets: offset ({offset}) is less than base ({base})"
                )));
            }
            rebased_offsets.push(offset - base);
        }

        let sliced_data = data.slice_with_length(data_start, data_end - data_start);
        // Copy into a compact buffer so each output batch owns only what it references.
        let sliced_data = LanceBuffer::copy_slice(&sliced_data);
        let rebased_offsets = LanceBuffer::reinterpret_vec(rebased_offsets);
        Ok((sliced_data, rebased_offsets))
    }

    fn slice_batch_data_and_rebase_offsets(
        data: &LanceBuffer,
        offsets: &LanceBuffer,
        bits_per_offset: u8,
    ) -> Result<(LanceBuffer, LanceBuffer)> {
        match bits_per_offset {
            32 => Self::slice_batch_data_and_rebase_offsets_typed::<u32>(data, offsets),
            64 => Self::slice_batch_data_and_rebase_offsets_typed::<u64>(data, offsets),
            _ => Err(Error::internal(format!(
                "Unsupported bits_per_offset={bits_per_offset}"
            ))),
        }
    }

    /// Reads a single length prefix from the front of `data`.
    ///
    /// The bytes come from the file. A page whose item walk ends with a partial
    /// trailing item leaves fewer than `bits_per_offset / 8` bytes here, so this
    /// is bounds checked and reports a corrupt file rather than reading past the
    /// end of the buffer.
    fn parse_length(data: &[u8], bits_per_offset: u8) -> Result<u64> {
        let width = bits_per_offset as usize / 8;
        if data.len() < width {
            return Err(Error::corrupt_file_named(
                "variable_full_zip",
                format!(
                    "truncated length prefix: {} byte(s) remain in the page buffer but a \
                     {}-bit length prefix requires {}",
                    data.len(),
                    bits_per_offset,
                    width
                ),
            ));
        }
        Ok(match bits_per_offset {
            8 => data[0] as u64,
            16 => u16::from_le_bytes(data[..2].try_into().unwrap()) as u64,
            32 => u32::from_le_bytes(data[..4].try_into().unwrap()) as u64,
            64 => u64::from_le_bytes(data[..8].try_into().unwrap()),
            _ => unreachable!(),
        })
    }

    fn unzip(
        &mut self,
        data: VecDeque<LanceBuffer>,
        in_bits_per_length: u8,
        out_bits_per_offset: u8,
        num_rows: u64,
    ) -> Result<()> {
        // This undercounts if there are lists but, at this point, we don't really know how many items we have
        let mut rep = Vec::with_capacity(num_rows as usize);
        let mut def = Vec::with_capacity(num_rows as usize);
        let bytes_cw = self.details.ctrl_word_parser.bytes_per_word() * num_rows as usize;

        // This undercounts if there are lists
        // It can also overcount if there are invisible items
        let bytes_per_offset = out_bits_per_offset as usize / 8;
        let bytes_offsets = bytes_per_offset * (num_rows as usize + 1);
        let mut offsets_data = Vec::with_capacity(bytes_offsets);

        let bytes_per_length = in_bits_per_length as usize / 8;
        let bytes_lengths = bytes_per_length * num_rows as usize;

        let bytes_data = data.iter().map(|d| d.len()).sum::<usize>();
        // This overcounts since bytes_lengths and bytes_cw are undercounts
        // It can also undercount if there are invisible items (hence the saturating_sub)
        let mut unzipped_data =
            Vec::with_capacity((bytes_data - bytes_cw).saturating_sub(bytes_lengths));

        let mut current_offset = 0_u64;
        let mut visible_item_count = 0_u64;
        for databuf in data.into_iter() {
            let mut databuf = databuf.as_ref();
            while !databuf.is_empty() {
                let data_start = unzipped_data.len();
                let offset_start = offsets_data.len();
                // We might have only-rep or only-def, neither, or both.  They move at the same
                // speed though so we only need one index into it
                let repdef_start = rep.len().max(def.len());
                // TODO: Kind of inefficient we parse the control word twice here
                let ctrl_desc = self.details.ctrl_word_parser.parse_desc(
                    databuf,
                    self.details.max_rep,
                    self.details.max_visible_def,
                );
                self.details
                    .ctrl_word_parser
                    .parse(databuf, &mut rep, &mut def);
                databuf = &databuf[self.details.ctrl_word_parser.bytes_per_word()..];

                if ctrl_desc.is_new_row {
                    self.repdef_starts.push(repdef_start);
                    self.data_starts.push(data_start);
                    self.offset_starts.push(offset_start);
                    self.visible_item_counts.push(visible_item_count);
                }
                if ctrl_desc.is_visible {
                    visible_item_count += 1;
                    if ctrl_desc.is_valid_item {
                        let length = Self::parse_length(databuf, in_bits_per_length)?;
                        match out_bits_per_offset {
                            32 => offsets_data
                                .extend_from_slice(&(current_offset as u32).to_le_bytes()),
                            64 => offsets_data.extend_from_slice(&current_offset.to_le_bytes()),
                            _ => unreachable!(),
                        };
                        databuf = &databuf[bytes_per_offset..];
                        unzipped_data.extend_from_slice(&databuf[..length as usize]);
                        databuf = &databuf[length as usize..];
                        current_offset += length;
                    } else {
                        // Null items still get an offset
                        match out_bits_per_offset {
                            32 => offsets_data
                                .extend_from_slice(&(current_offset as u32).to_le_bytes()),
                            64 => offsets_data.extend_from_slice(&current_offset.to_le_bytes()),
                            _ => unreachable!(),
                        }
                    }
                }
            }
        }
        self.repdef_starts.push(rep.len().max(def.len()));
        self.data_starts.push(unzipped_data.len());
        self.offset_starts.push(offsets_data.len());
        self.visible_item_counts.push(visible_item_count);
        match out_bits_per_offset {
            32 => offsets_data.extend_from_slice(&(current_offset as u32).to_le_bytes()),
            64 => offsets_data.extend_from_slice(&current_offset.to_le_bytes()),
            _ => unreachable!(),
        };
        self.rep = ScalarBuffer::from(rep);
        self.def = ScalarBuffer::from(def);
        self.data = LanceBuffer::from(unzipped_data);
        self.offsets = LanceBuffer::from(offsets_data);
        Ok(())
    }
}

impl StructuralPageDecoder for VariableFullZipDecoder {
    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn DecodePageTask>> {
        let start = self.current_idx;
        let end = start + num_rows as usize;

        let offset_start = self.offset_starts[start];
        let offset_end = self.offset_starts[end] + (self.bits_per_offset as usize / 8);
        let offsets = self
            .offsets
            .slice_with_length(offset_start, offset_end - offset_start);
        // Keep each batch's variable data buffer bounded to the selected rows.
        let (data, offsets) =
            Self::slice_batch_data_and_rebase_offsets(&self.data, &offsets, self.bits_per_offset)?;

        let repdef_start = self.repdef_starts[start];
        let repdef_end = self.repdef_starts[end];
        let rep = if self.rep.is_empty() {
            self.rep.clone()
        } else {
            self.rep.slice(repdef_start, repdef_end - repdef_start)
        };
        let def = if self.def.is_empty() {
            self.def.clone()
        } else {
            self.def.slice(repdef_start, repdef_end - repdef_start)
        };

        let visible_item_counts_start = self.visible_item_counts[start];
        let visible_item_counts_end = self.visible_item_counts[end];
        let num_visible_items = visible_item_counts_end - visible_item_counts_start;

        self.current_idx += num_rows as usize;

        Ok(Box::new(VariableFullZipDecodeTask {
            details: self.details.clone(),
            decompressor: self.decompressor.clone(),
            data,
            offsets,
            bits_per_offset: self.bits_per_offset,
            num_visible_items,
            rep,
            def,
        }))
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

#[derive(Debug)]
struct VariableFullZipDecodeTask {
    details: Arc<FullZipDecodeDetails>,
    decompressor: Arc<dyn VariablePerValueDecompressor>,
    data: LanceBuffer,
    offsets: LanceBuffer,
    bits_per_offset: u8,
    num_visible_items: u64,
    rep: ScalarBuffer<u16>,
    def: ScalarBuffer<u16>,
}

impl DecodePageTask for VariableFullZipDecodeTask {
    fn decode(self: Box<Self>) -> Result<DecodedPage> {
        let block = VariableWidthBlock {
            data: self.data,
            offsets: self.offsets,
            bits_per_offset: self.bits_per_offset,
            num_values: self.num_visible_items,
            block_info: BlockInfo::new(),
        };
        let decomopressed = self.decompressor.decompress(block)?;
        let rep = if self.rep.is_empty() {
            None
        } else {
            Some(self.rep.to_vec())
        };
        let def = if self.def.is_empty() {
            None
        } else {
            Some(self.def.to_vec())
        };
        let unraveler = RepDefUnraveler::new(
            rep,
            def,
            self.details.def_meaning.clone(),
            self.num_visible_items,
        );
        Ok(DecodedPage {
            data: decomopressed,
            repdef: unraveler,
        })
    }
}

#[derive(Debug)]
struct FullZipDecodeTaskItem {
    data: PerValueDataBlock,
    rows_in_buf: u64,
}

/// A task to unzip and decompress full-zip encoded data when that data
/// has a fixed-width.
#[derive(Debug)]
struct FixedFullZipDecodeTask {
    details: Arc<FullZipDecodeDetails>,
    data: Vec<FullZipDecodeTaskItem>,
    num_rows: usize,
    bytes_per_value: usize,
}

impl DecodePageTask for FixedFullZipDecodeTask {
    fn decode(self: Box<Self>) -> Result<DecodedPage> {
        let estimated_size_bytes = if self.details.ctrl_word_parser.bytes_per_word() == 0 {
            let PerValueDecompressor::Fixed(decompressor) = &self.details.value_decompressor else {
                return Err(Error::internal(
                    "FixedFullZipDecodeTask requires a fixed-width decompressor",
                ));
            };
            decompressor
                .decoded_size_bytes(self.num_rows as u64)
                .unwrap_or_else(|| {
                    self.data
                        .iter()
                        .map(|task_item| task_item.data.data_size())
                        .sum::<u64>()
                        * 2
                })
        } else {
            // Rep/def levels can suppress values, so the exact output size is not known
            // until they are decoded. Keep the existing conservative estimate.
            self.data
                .iter()
                .map(|task_item| task_item.data.data_size())
                .sum::<u64>()
                * 2
        };
        let mut data_builder = DataBlockBuilder::with_capacity_estimate(estimated_size_bytes);

        if self.details.ctrl_word_parser.bytes_per_word() == 0 {
            // Fast path, no need to unzip because there is no rep/def
            //
            // We decompress each buffer and add it to our output buffer
            for task_item in self.data.into_iter() {
                let PerValueDataBlock::Fixed(fixed_data) = task_item.data else {
                    unreachable!()
                };
                let PerValueDecompressor::Fixed(decompressor) = &self.details.value_decompressor
                else {
                    unreachable!()
                };
                debug_assert_eq!(fixed_data.num_values, task_item.rows_in_buf);
                let decompressed = decompressor.decompress(fixed_data, task_item.rows_in_buf)?;
                data_builder.append(&decompressed, 0..task_item.rows_in_buf)?;
            }

            let unraveler = RepDefUnraveler::new(
                None,
                None,
                self.details.def_meaning.clone(),
                self.num_rows as u64,
            );

            Ok(DecodedPage {
                data: data_builder.finish(),
                repdef: unraveler,
            })
        } else {
            // Slow path, unzipping needed
            let mut rep = Vec::with_capacity(self.num_rows);
            let mut def = Vec::with_capacity(self.num_rows);

            for task_item in self.data.into_iter() {
                let PerValueDataBlock::Fixed(fixed_data) = task_item.data else {
                    unreachable!()
                };
                let mut buf_slice = fixed_data.data.as_ref();
                let num_values = fixed_data.num_values as usize;
                // We will be unzipping repdef in to `rep` and `def` and the
                // values into `values` (which contains the compressed values)
                let mut values = Vec::with_capacity(
                    fixed_data.data.len()
                        - (self.details.ctrl_word_parser.bytes_per_word() * num_values),
                );
                let mut visible_items = 0;
                for _ in 0..num_values {
                    // Extract rep/def
                    self.details
                        .ctrl_word_parser
                        .parse(buf_slice, &mut rep, &mut def);
                    buf_slice = &buf_slice[self.details.ctrl_word_parser.bytes_per_word()..];

                    let is_visible = def
                        .last()
                        .map(|d| *d <= self.details.max_visible_def)
                        .unwrap_or(true);
                    if is_visible {
                        // Extract value
                        values.extend_from_slice(buf_slice[..self.bytes_per_value].as_ref());
                        buf_slice = &buf_slice[self.bytes_per_value..];
                        visible_items += 1;
                    }
                }

                // Finally, we decompress the values and add them to our output buffer
                let values_buf = LanceBuffer::from(values);
                let fixed_data = FixedWidthDataBlock {
                    bits_per_value: self.bytes_per_value as u64 * 8,
                    block_info: BlockInfo::new(),
                    data: values_buf,
                    num_values: visible_items,
                };
                let PerValueDecompressor::Fixed(decompressor) = &self.details.value_decompressor
                else {
                    unreachable!()
                };
                let decompressed = decompressor.decompress(fixed_data, visible_items)?;
                data_builder.append(&decompressed, 0..visible_items)?;
            }

            let repetition = if rep.is_empty() { None } else { Some(rep) };
            let definition = if def.is_empty() { None } else { Some(def) };

            let unraveler = RepDefUnraveler::new(
                repetition,
                definition,
                self.details.def_meaning.clone(),
                self.num_rows as u64,
            );
            let data = data_builder.finish();

            Ok(DecodedPage {
                data,
                repdef: unraveler,
            })
        }
    }
}

#[derive(Debug)]
struct StructuralPrimitiveFieldSchedulingJob<'a> {
    scheduler: &'a StructuralPrimitiveFieldScheduler,
    mappings: std::vec::IntoIter<PageRangeMapping>,
}

impl<'a> StructuralPrimitiveFieldSchedulingJob<'a> {
    pub fn try_new(
        scheduler: &'a StructuralPrimitiveFieldScheduler,
        ranges: Vec<Range<u64>>,
    ) -> Result<Self> {
        let mappings = map_ranges_to_pages(&scheduler.page_row_ends, &ranges)?.into_iter();
        Ok(Self {
            scheduler,
            mappings,
        })
    }
}

impl StructuralSchedulingJob for StructuralPrimitiveFieldSchedulingJob<'_> {
    fn schedule_next(&mut self, context: &mut SchedulerContext) -> Result<Vec<ScheduledScanLine>> {
        let Some(first_mapping) = self.mappings.next() else {
            return Ok(Vec::new());
        };
        // Pages are scheduled in row order until their reads add up to a
        // batch, then submitted together so the I/O scheduler can coalesce
        // neighbouring pages.
        let batch = Arc::new(RequestBatch::new(context.io().clone()));
        let batch_io: Arc<dyn EncodingsIo> = batch.clone();
        let cur_path = context.current_path();
        let mut scan_lines = Vec::new();
        let mut mapping = Some(first_mapping);
        while let Some(cur_mapping) = mapping.take() {
            let cur_page = self.scheduler.built_page(cur_mapping.page_idx)?;

            trace!(
                "Scheduling {} rows across {} ranges from page with {} rows (column_index={}, page_index={})",
                cur_mapping
                    .ranges_in_page
                    .iter()
                    .map(|r| r.end - r.start)
                    .sum::<u64>(),
                cur_mapping.ranges_in_page.len(),
                cur_page.row_range.end - cur_page.row_range.start,
                self.scheduler.column_index,
                cur_page.page_index,
            );

            let page_decoders = cur_page
                .scheduler
                .schedule_ranges(&cur_mapping.ranges_in_page, &batch_io)?;

            scan_lines.extend(page_decoders.into_iter().map(|page_load_task| {
                let cur_path = cur_path.clone();
                let page_decoder = page_load_task.decoder_fut;
                let unloaded_page = async move {
                    let page_decoder = page_decoder.await?;
                    Ok(LoadedPageShard {
                        decoder: page_decoder,
                        path: cur_path,
                    })
                }
                .boxed();
                ScheduledScanLine {
                    decoders: vec![MessageType::UnloadedPage(UnloadedPageShard(unloaded_page))],
                    rows_scheduled: page_load_task.num_rows,
                }
            }));

            if batch.pending_bytes() < IO_REQUEST_BATCH_BYTES {
                mapping = self.mappings.next();
            }
        }
        batch.flush();
        Ok(scan_lines)
    }
}

#[derive(Debug)]
struct PageInfoAndScheduler {
    page_index: usize,
    row_range: Range<u64>,
    scheduler: Box<dyn StructuralPageScheduler>,
}

/// Everything needed to build a page scheduler from its `PageInfo` on demand.
///
/// Building a page scheduler clones buffer tables and instantiates
/// decompressors, so a column with hundreds of thousands of pages must not pay
/// for every page on every read. The field scheduler keeps this source and
/// builds only the pages `initialize` is asked to cover.
#[derive(Debug)]
struct PageSchedulerSource {
    column_info: Arc<ColumnInfo>,
    decompressors: Arc<dyn DecompressionStrategy>,
    cache_repetition_index: bool,
    target_field: Field,
}

/// A scheduler for a leaf node
///
/// Here we look at the layout of the various pages and delegate scheduling to a scheduler
/// appropriate for the layout of the page.
#[derive(Debug)]
pub struct StructuralPrimitiveFieldScheduler {
    /// Exclusive end row of every page in the column, in page order, so page
    /// `i` covers `page_row_ends[i - 1]..page_row_ends[i]` (page 0 starts at 0).
    page_row_ends: Vec<u64>,
    /// Page schedulers built so far, ordered by `page_index`. After
    /// `initialize` this holds exactly the pages the requested ranges touch,
    /// or every page when no ranges were given.
    page_schedulers: Vec<PageInfoAndScheduler>,
    /// Metadata to build pages that are not in `page_schedulers` yet. `None`
    /// once every page has been built.
    page_source: Option<PageSchedulerSource>,
    column_index: u32,
    target_data_type_tag: Arc<str>,
}

impl StructuralPrimitiveFieldScheduler {
    /// Creates the scheduler and builds every page scheduler up front.
    ///
    /// This pays for all pages of the column on every read. Prefer
    /// [`Self::try_new_lazy`], which builds only the pages a read touches.
    #[deprecated(
        since = "13.0.0",
        note = "builds every page scheduler eagerly; use try_new_lazy"
    )]
    pub fn try_new(
        column_info: &ColumnInfo,
        decompressors: &dyn DecompressionStrategy,
        cache_repetition_index: bool,
        target_field: &Field,
    ) -> Result<Self> {
        let page_row_ends = Self::page_row_ends(column_info)?;
        let mut page_schedulers = Vec::with_capacity(page_row_ends.len());
        let mut row_start = 0_u64;
        for (page_index, (page_info, &row_end)) in column_info
            .page_infos
            .iter()
            .zip(&page_row_ends)
            .enumerate()
        {
            page_schedulers.push(Self::page_info_to_scheduler(
                page_info,
                page_index,
                row_start..row_end,
                decompressors,
                cache_repetition_index,
                target_field,
            )?);
            row_start = row_end;
        }
        Ok(Self {
            page_row_ends,
            page_schedulers,
            page_source: None,
            column_index: column_info.index,
            target_data_type_tag: format!("{:?}", target_field.data_type()).into(),
        })
    }

    /// Creates the scheduler without building any page schedulers.
    ///
    /// Only the per-page row boundaries are computed here. Page schedulers
    /// are built in [`StructuralFieldScheduler::initialize`] for the pages the
    /// requested ranges touch (or for every page when no ranges are given), so
    /// a small read from a column with many pages stays cheap.
    pub fn try_new_lazy(
        column_info: Arc<ColumnInfo>,
        decompressors: Arc<dyn DecompressionStrategy>,
        cache_repetition_index: bool,
        target_field: &Field,
    ) -> Result<Self> {
        Ok(Self {
            page_row_ends: Self::page_row_ends(&column_info)?,
            page_schedulers: Vec::new(),
            column_index: column_info.index,
            target_data_type_tag: format!("{:?}", target_field.data_type()).into(),
            page_source: Some(PageSchedulerSource {
                column_info,
                decompressors,
                cache_repetition_index,
                target_field: target_field.clone(),
            }),
        })
    }

    /// Exclusive end row of every page of `column_info`, in page order.
    fn page_row_ends(column_info: &ColumnInfo) -> Result<Vec<u64>> {
        let mut page_row_ends = Vec::with_capacity(column_info.page_infos.len());
        let mut row_start = 0_u64;
        for (page_index, page_info) in column_info.page_infos.iter().enumerate() {
            let row_end = row_start.checked_add(page_info.num_rows).ok_or_else(|| {
                Error::invalid_input_source(
                    format!(
                        "Page row range overflowed for column {} at page {page_index}: start={row_start}, rows={}",
                        column_info.index, page_info.num_rows
                    )
                    .into(),
                )
            })?;
            page_row_ends.push(row_end);
            row_start = row_end;
        }
        Ok(page_row_ends)
    }

    /// Wraps already-built page schedulers; nothing is built lazily.
    #[cfg(test)]
    fn from_page_schedulers(
        page_schedulers: Vec<PageInfoAndScheduler>,
        column_index: u32,
        target_data_type_tag: Arc<str>,
    ) -> Self {
        Self {
            page_row_ends: page_schedulers
                .iter()
                .map(|page| page.row_range.end)
                .collect(),
            page_schedulers,
            page_source: None,
            column_index,
            target_data_type_tag,
        }
    }

    fn page_row_range(&self, page_index: usize) -> Range<u64> {
        let start = match page_index.checked_sub(1) {
            Some(previous) => self.page_row_ends[previous],
            None => 0,
        };
        start..self.page_row_ends[page_index]
    }

    /// Position of `page_index` in `page_schedulers` if it has been built.
    fn built_page_position(&self, page_index: usize) -> Option<usize> {
        self.page_schedulers
            .binary_search_by_key(&page_index, |page| page.page_index)
            .ok()
    }

    fn built_page(&self, page_index: usize) -> Result<&PageInfoAndScheduler> {
        let position = self.built_page_position(page_index).ok_or_else(|| {
            Error::internal(format!(
                "Page {page_index} of column {} was scheduled but not initialized; the scheduled ranges must be covered by the ranges passed to initialize",
                self.column_index
            ))
        })?;
        Ok(&self.page_schedulers[position])
    }

    /// Builds the schedulers for `page_indices` (ascending) that do not exist
    /// yet and returns the positions of all of them in `page_schedulers`.
    fn build_pages(&mut self, page_indices: &[usize]) -> Result<Vec<usize>> {
        if let Some(source) = &self.page_source {
            let mut missing = Vec::new();
            for &page_index in page_indices {
                if self.built_page_position(page_index).is_some() {
                    continue;
                }
                let page_info = source
                    .column_info
                    .page_infos
                    .get(page_index)
                    .ok_or_else(|| {
                        Error::internal(format!(
                            "Page {page_index} is out of bounds for column {} with {} pages",
                            self.column_index,
                            source.column_info.page_infos.len()
                        ))
                    })?;
                missing.push(Self::page_info_to_scheduler(
                    page_info,
                    page_index,
                    self.page_row_range(page_index),
                    source.decompressors.as_ref(),
                    source.cache_repetition_index,
                    &source.target_field,
                )?);
            }
            if !missing.is_empty() {
                self.page_schedulers = std::mem::take(&mut self.page_schedulers)
                    .into_iter()
                    .merge_by(missing, |built, new| built.page_index < new.page_index)
                    .collect();
            }
            if self.page_schedulers.len() == self.page_row_ends.len() {
                self.page_source = None;
            }
        }
        page_indices
            .iter()
            .map(|&page_index| {
                self.built_page_position(page_index).ok_or_else(|| {
                    Error::internal(format!(
                        "Page {page_index} of column {} is missing after construction",
                        self.column_index
                    ))
                })
            })
            .collect()
    }

    fn page_layout_to_scheduler(
        page_info: &PageInfo,
        page_layout: &PageLayout,
        decompressors: &dyn DecompressionStrategy,
        cache_repetition_index: bool,
        target_field: &Field,
    ) -> Result<Box<dyn StructuralPageScheduler>> {
        use pb21::page_layout::Layout;
        Ok(match page_layout.layout.as_ref().expect_ok()? {
            Layout::MiniBlockLayout(mini_block) => Box::new(MiniBlockScheduler::try_new(
                &page_info.buffer_offsets_and_sizes,
                page_info.priority,
                mini_block.num_items,
                mini_block,
                decompressors,
            )?),
            Layout::SparseLayout(sparse_layout) => {
                Box::new(sparse::SparseStructuralScheduler::try_new(
                    &page_info.buffer_offsets_and_sizes,
                    page_info.priority,
                    page_info.num_rows,
                    target_field.data_type(),
                    sparse_layout,
                    decompressors,
                )?)
            }
            Layout::FullZipLayout(full_zip) => {
                let mut scheduler = FullZipScheduler::try_new(
                    &page_info.buffer_offsets_and_sizes,
                    page_info.priority,
                    page_info.num_rows,
                    full_zip,
                    decompressors,
                )?;
                scheduler.enable_cache = cache_repetition_index;
                Box::new(scheduler)
            }
            Layout::ConstantLayout(constant_layout) => {
                let def_meaning = constant_layout
                    .layers
                    .iter()
                    .map(|l| ProtobufUtils21::repdef_layer_to_def_interp(*l))
                    .collect::<Vec<_>>();
                let has_scalar_value = constant_layout.inline_value.is_some()
                    || page_info.buffer_offsets_and_sizes.len() == 1
                    || page_info.buffer_offsets_and_sizes.len() == 3;
                if has_scalar_value {
                    Box::new(constant::ConstantPageScheduler::try_new(
                        page_info.buffer_offsets_and_sizes.clone(),
                        constant_layout.inline_value.clone(),
                        target_field.data_type(),
                        def_meaning.into(),
                    )?) as Box<dyn StructuralPageScheduler>
                } else if def_meaning.len() == 1
                    && def_meaning[0] == DefinitionInterpretation::NullableItem
                {
                    Box::new(SimpleAllNullScheduler::default()) as Box<dyn StructuralPageScheduler>
                } else {
                    // RLE levels select a validated cache representation; other
                    // block compressions keep flowing through the eager decompressor.
                    let rep_codec = LevelCodec::try_new(
                        constant_layout.rep_compression.as_ref(),
                        decompressors,
                    )?;
                    let def_codec = LevelCodec::try_new(
                        constant_layout.def_compression.as_ref(),
                        decompressors,
                    )?;

                    Box::new(ComplexAllNullScheduler::new(
                        page_info.buffer_offsets_and_sizes.clone(),
                        def_meaning.into(),
                        rep_codec,
                        def_codec,
                        constant_layout.num_rep_values,
                        constant_layout.num_def_values,
                    )) as Box<dyn StructuralPageScheduler>
                }
            }
            Layout::BlobLayout(blob) => {
                let inner_scheduler = Self::page_layout_to_scheduler(
                    page_info,
                    blob.inner_layout.as_ref().expect_ok()?.as_ref(),
                    decompressors,
                    cache_repetition_index,
                    target_field,
                )?;
                let def_meaning = blob
                    .layers
                    .iter()
                    .map(|l| ProtobufUtils21::repdef_layer_to_def_interp(*l))
                    .collect::<Vec<_>>();
                if matches!(target_field.data_type(), DataType::Struct(_)) {
                    // User wants to decode blob into struct
                    Box::new(BlobDescriptionPageScheduler::new(
                        inner_scheduler,
                        def_meaning.into(),
                    ))
                } else {
                    // User wants to decode blob into binary data
                    Box::new(BlobPageScheduler::new(
                        inner_scheduler,
                        page_info.priority,
                        page_info.num_rows,
                        def_meaning.into(),
                    ))
                }
            }
        })
    }

    fn page_info_to_scheduler(
        page_info: &PageInfo,
        page_index: usize,
        row_range: Range<u64>,
        decompressors: &dyn DecompressionStrategy,
        cache_repetition_index: bool,
        target_field: &Field,
    ) -> Result<PageInfoAndScheduler> {
        let page_layout = page_info.encoding.as_structural();
        let scheduler = Self::page_layout_to_scheduler(
            page_info,
            page_layout,
            decompressors,
            cache_repetition_index,
            target_field,
        )?;
        Ok(PageInfoAndScheduler {
            page_index,
            row_range,
            scheduler,
        })
    }
}

pub trait CachedPageData: Any + Send + Sync + DeepSizeOf + 'static {
    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static>;
}

pub struct NoCachedPageData;

impl DeepSizeOf for NoCachedPageData {
    fn deep_size_of_children(&self, _ctx: &mut Context) -> usize {
        0
    }
}
impl CachedPageData for NoCachedPageData {
    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self
    }
}

#[doc = "Deprecated compatibility wrapper; new code uses per-page PageDataCacheKey entries."]
pub struct CachedFieldData {
    pages: Vec<Arc<dyn CachedPageData>>,
}

impl DeepSizeOf for CachedFieldData {
    fn deep_size_of_children(&self, ctx: &mut Context) -> usize {
        self.pages.deep_size_of_children(ctx)
    }
}

/// Compatibility key for the former whole-field page metadata cache.
#[derive(Debug, Clone)]
pub struct FieldDataCacheKey {
    pub column_index: u32,
    pub view_tag: String,
}

impl CacheKey for FieldDataCacheKey {
    type ValueType = CachedFieldData;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("{}:{}", self.column_index, self.view_tag).into()
    }

    fn type_name() -> &'static str {
        "FieldData"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.encoding.logical.primitive.field-data-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.column_index);
        builder.write_str(&self.view_tag);
    }
}

/// Sized wrapper around a page's cached metadata so [`PageDataCacheKey`] can use
/// the sized [`CacheKey`] path; the concrete per-layout state still lives behind
/// `dyn CachedPageData`.
pub struct CachedPage {
    data: Arc<dyn CachedPageData>,
}

impl DeepSizeOf for CachedPage {
    fn deep_size_of_children(&self, ctx: &mut Context) -> usize {
        self.data.deep_size_of_children(ctx)
    }
}

// Cache key for a single page's initialized metadata, keyed per page (not per
// column) so a read populates and reuses only the pages it touches.
//
// `view` distinguishes decode shapes of one physical column: a blob column
// materializes as a `Struct<position, size>` descriptor in one variant and raw
// `LargeBinary` in another, each caching a different `CachedPageData` type.
// Without it, two readers at the same column/page but different shapes collide
// and crash on a downcast.
/// Decode shape used to isolate cached page metadata for one physical column.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PageCacheView {
    /// The column's native non-blob representation.
    Native,
    /// Blob position and size descriptors materialized as a struct.
    BlobDescriptor,
    /// Blob contents materialized as binary values.
    BlobPayload,
}

impl PageCacheView {
    fn cache_tag(self) -> &'static str {
        match self {
            Self::Native => "n",
            Self::BlobDescriptor => "bd",
            Self::BlobPayload => "bp",
        }
    }
}

#[derive(Debug, Clone)]
pub struct PageDataCacheKey {
    pub column_index: u32,
    pub page_index: u32,
    pub view: PageCacheView,
    pub target_data_type_tag: Arc<str>,
}

impl CacheKey for PageDataCacheKey {
    type ValueType = CachedPage;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!(
            "v2/{}/{}/{}/{}",
            self.column_index,
            self.page_index,
            self.view.cache_tag(),
            self.target_data_type_tag
        )
        .into()
    }

    fn type_name() -> &'static str {
        "PageData"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.encoding.logical.primitive.page-data-key", 2)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.column_index);
        builder.write_u32(self.page_index);
        builder.write_variant(match self.view {
            PageCacheView::Native => 0,
            PageCacheView::BlobDescriptor => 1,
            PageCacheView::BlobPayload => 2,
        });
        builder.write_str(&self.target_data_type_tag);
    }
}

/// One page's slice of a scheduling request: the page's index and the
/// page-local row sub-ranges (relative to the page's first row) of the request
/// that fall in it.
#[derive(Debug)]
struct PageRangeMapping {
    page_idx: usize,
    ranges_in_page: Vec<Range<u64>>,
}

/// Visits every intersection between ordered row ranges and ordered pages.
///
/// `page_row_ends` holds the exclusive end row of each page (see
/// [`StructuralPrimitiveFieldScheduler::page_row_ends`]). Each range
/// binary-searches its first overlapping page and then visits only the pages
/// it touches. For `R` ranges, `P` pages, and `T` intersections this takes
/// `O(R log P + T)` time without scanning sparse gaps.
fn for_each_page_range(
    page_row_ends: &[u64],
    ranges: &[Range<u64>],
    mut visit: impl FnMut(usize, Range<u64>),
) -> Result<()> {
    for (range_idx, range) in ranges.iter().enumerate() {
        if range.start > range.end {
            return Err(Error::invalid_input_source(
                format!(
                    "Requested row range {range_idx} is reversed: {}..{}",
                    range.start, range.end
                )
                .into(),
            ));
        }
        if range_idx > 0 {
            let previous = &ranges[range_idx - 1];
            if previous.start >= range.start || previous.end > range.start {
                return Err(Error::invalid_input_source(
                    format!(
                        "Requested row ranges overlap or are not strictly ordered at index {range_idx}: {previous:?}, then {range:?}"
                    )
                    .into(),
                ));
            }
        }
    }

    for range in ranges {
        let mut page_idx = page_row_ends.partition_point(|page_end| *page_end <= range.start);
        let mut page_start = match page_idx.checked_sub(1) {
            Some(previous) => page_row_ends[previous],
            None => 0,
        };
        while let Some(&page_end) = page_row_ends.get(page_idx) {
            if page_start >= range.end {
                break;
            }
            let start_in_page = range.start.max(page_start) - page_start;
            let end_in_page = range.end.min(page_end) - page_start;
            if start_in_page < end_in_page {
                visit(page_idx, start_in_page..end_in_page);
            }
            page_idx += 1;
            page_start = page_end;
        }
    }
    Ok(())
}

/// Maps global row `ranges` onto a column's pages, yielding the page-local
/// sub-ranges for every page any range touches.
///
/// `ranges` must be ascending and non-overlapping (the precondition
/// `schedule_ranges` relies on).
fn map_ranges_to_pages(
    page_row_ends: &[u64],
    ranges: &[Range<u64>],
) -> Result<Vec<PageRangeMapping>> {
    let mut result: Vec<PageRangeMapping> =
        Vec::with_capacity(ranges.len().min(page_row_ends.len()));
    for_each_page_range(page_row_ends, ranges, |page_idx, range_in_page| {
        if let Some(mapping) = result.last_mut()
            && mapping.page_idx == page_idx
        {
            mapping.ranges_in_page.push(range_in_page);
        } else {
            result.push(PageRangeMapping {
                page_idx,
                ranges_in_page: vec![range_in_page],
            });
        }
    })?;
    Ok(result)
}

fn pages_overlapping_ranges(page_row_ends: &[u64], ranges: &[Range<u64>]) -> Result<Vec<usize>> {
    let mut result = Vec::with_capacity(ranges.len().min(page_row_ends.len()));
    for_each_page_range(page_row_ends, ranges, |page_idx, _| {
        if result.last().copied() != Some(page_idx) {
            result.push(page_idx);
        }
    })?;
    Ok(result)
}

#[derive(Debug)]
struct PageCacheMiss {
    /// Position of the page in `StructuralPrimitiveFieldScheduler::page_schedulers`,
    /// which holds only the built pages and so may differ from `page_index`.
    page_idx: usize,
    cache_key: PageDataCacheKey,
    initialization: PageInitialization,
}

async fn read_page_initialization_buffers(
    io: &Arc<dyn EncodingsIo>,
    all_ranges: Vec<Range<u64>>,
) -> Result<Vec<Bytes>> {
    // `submit_request` coalesces (and later un-coalesces) in one forward
    // pass, so submit ranges in file order. Page layouts are not required
    // to declare their initialization ranges in that order, so retain each
    // range's original position and restore it before initializing pages.
    let mut sorted_ranges = all_ranges.into_iter().enumerate().collect::<Vec<_>>();
    sorted_ranges.sort_unstable_by_key(|(_, range)| (range.start, range.end));
    let sorted_requests = sorted_ranges
        .iter()
        .map(|(_, range)| range.clone())
        .collect::<Vec<_>>();
    let expected_buffer_count = sorted_requests.len();
    let sorted_buffers = if sorted_requests.is_empty() {
        Vec::new()
    } else {
        io.submit_request(sorted_requests, 0).await?
    };
    if sorted_buffers.len() != expected_buffer_count {
        return Err(Error::invalid_input_source(
            format!(
                "Page initialization I/O returned {} buffers for {} ranges",
                sorted_buffers.len(),
                expected_buffer_count
            )
            .into(),
        ));
    }
    let mut buffers = vec![None; expected_buffer_count];
    for ((original_idx, _), buffer) in sorted_ranges.into_iter().zip(sorted_buffers) {
        buffers[original_idx] = Some(buffer);
    }
    buffers
        .into_iter()
        .map(|buffer| {
            buffer.ok_or_else(|| Error::internal("Page initialization buffer was not assigned"))
        })
        .collect()
}

async fn cache_initialized_page(
    cache: Arc<LanceCache>,
    cache_key: PageDataCacheKey,
    data: Arc<dyn CachedPageData>,
) {
    if data
        .clone()
        .as_arc_any()
        .downcast::<NoCachedPageData>()
        .is_err()
    {
        cache
            .insert_with_key(&cache_key, Arc::new(CachedPage { data }))
            .await;
    }
}

impl StructuralPrimitiveFieldScheduler {
    /// Checks the built pages at `positions` (ascending indices into
    /// `page_schedulers`) against the cache and returns those still needing
    /// initialization I/O.
    async fn find_cache_misses(
        &mut self,
        positions: impl ExactSizeIterator<Item = usize>,
        cache: &Arc<LanceCache>,
    ) -> Result<Vec<PageCacheMiss>> {
        // Serve cache hits in place; the rest are misses we must read.
        let mut misses = Vec::with_capacity(positions.len());
        for page_idx in positions {
            let page = &self.page_schedulers[page_idx];
            if !page.scheduler.needs_initialization() {
                continue;
            }
            let page_index = u32::try_from(page.page_index).map_err(|_| {
                Error::invalid_input_source(
                    format!("Page index does not fit u32: {}", page.page_index).into(),
                )
            })?;
            let cache_key = PageDataCacheKey {
                column_index: self.column_index,
                page_index,
                view: page.scheduler.cache_view(),
                target_data_type_tag: self.target_data_type_tag.clone(),
            };
            if let Some(cached_page) = cache.get_with_key(&cache_key).await {
                match self.page_schedulers[page_idx]
                    .scheduler
                    .try_load(&cached_page.data)
                {
                    Ok(()) => continue,
                    Err(error) => {
                        debug!("Replacing invalid cached page data for {cache_key:?}: {error}");
                    }
                }
            }
            let initialization = self.page_schedulers[page_idx].scheduler.init_layout()?;
            misses.push(PageCacheMiss {
                page_idx,
                cache_key,
                initialization,
            });
        }
        Ok(misses)
    }

    fn initialization_ranges(misses: &[PageCacheMiss]) -> Vec<Range<u64>> {
        let num_ranges = misses
            .iter()
            .map(|miss| miss.initialization.num_required_ranges())
            .sum();
        let mut all_ranges = Vec::with_capacity(num_ranges);
        for miss in misses {
            all_ranges.extend(miss.initialization.required_ranges().cloned());
        }
        all_ranges
    }

    async fn initialize_cache_misses(
        &mut self,
        misses: Vec<PageCacheMiss>,
        buffers: Vec<Bytes>,
        cache: Arc<LanceCache>,
        io: &Arc<dyn EncodingsIo>,
    ) -> Result<()> {
        let mut buffers = buffers.into_iter();

        // Initialize each missing page from its slice of the buffers, in file
        // order. `init_from_buffers` can perform further I/O (e.g. loading blob
        // descriptions), so run pages concurrently; peel disjoint `&mut` page
        // borrows off the slice to satisfy the borrow checker without a lookup
        // table.
        let mut tail = self.page_schedulers.as_mut_slice();
        let mut next_page = 0;
        let mut page_inits = FuturesOrdered::new();
        for miss in misses {
            let PageCacheMiss {
                page_idx,
                cache_key,
                initialization,
            } = miss;
            let relative_idx = page_idx
                .checked_sub(next_page)
                .ok_or_else(|| Error::internal("Page initialization indices are not ordered"))?;
            if relative_idx >= tail.len() {
                return Err(Error::internal(format!(
                    "Page initialization index {page_idx} is out of bounds"
                )));
            }
            let (_, rest) = tail.split_at_mut(relative_idx);
            let (page, rest) = rest.split_first_mut().ok_or_else(|| {
                Error::internal(format!(
                    "Page initialization index {page_idx} is out of bounds"
                ))
            })?;
            tail = rest;
            next_page = page_idx + 1;

            let count = initialization.num_required_ranges();
            let page_buffers = buffers.by_ref().take(count).collect::<Vec<_>>();
            if page_buffers.len() != count {
                return Err(Error::invalid_input_source(
                    format!(
                        "Page {page_idx} received {} initialization buffers, expected {count}",
                        page_buffers.len()
                    )
                    .into(),
                ));
            }
            let page_buffers = initialization.with_buffers(page_buffers)?;
            let cache = cache.clone();
            let scheduler = &mut page.scheduler;
            page_inits.push_back(async move {
                let cached = scheduler.init_from_buffers(page_buffers, io).await?;
                scheduler.try_load(&cached)?;
                cache_initialized_page(cache, cache_key, cached).await;
                Ok::<(), Error>(())
            });
        }
        if buffers.next().is_some() {
            return Err(Error::invalid_input_source(
                "Page initialization returned unassigned buffers".into(),
            ));
        }

        page_inits.try_collect::<Vec<()>>().await?;
        Ok(())
    }

    async fn initialize_pages(
        &mut self,
        requested_ranges: Option<&[Range<u64>]>,
        context: &SchedulerContext,
    ) -> Result<()> {
        let cache = context.cache().clone();
        let page_indices = match requested_ranges {
            None => (0..self.page_row_ends.len()).collect::<Vec<_>>(),
            Some(ranges) => pages_overlapping_ranges(&self.page_row_ends, ranges)?,
        };
        let positions = self.build_pages(&page_indices)?;
        let misses = self
            .find_cache_misses(positions.into_iter(), &cache)
            .await?;

        if misses.is_empty() {
            return Ok(());
        }

        // Concatenate the misses' metadata ranges into one request so adjacent
        // ranges coalesce into shared GETs instead of one request per page.
        let all_ranges = Self::initialization_ranges(&misses);
        let buffers = read_page_initialization_buffers(context.io(), all_ranges).await?;
        self.initialize_cache_misses(misses, buffers, cache, context.io())
            .await
    }
}

impl StructuralFieldScheduler for StructuralPrimitiveFieldScheduler {
    fn initialize<'a>(
        &'a mut self,
        requested_ranges: Option<&'a [Range<u64>]>,
        _filter: &'a FilterExpression,
        context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>> {
        self.initialize_pages(requested_ranges, context).boxed()
    }

    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[Range<u64>],
        _filter: &FilterExpression,
    ) -> Result<Box<dyn StructuralSchedulingJob + 'a>> {
        let ranges = ranges.to_vec();
        Ok(Box::new(StructuralPrimitiveFieldSchedulingJob::try_new(
            self, ranges,
        )?))
    }
}

/// Takes the output from several pages decoders and
/// concatenates them.
#[derive(Debug)]
pub struct StructuralCompositeDecodeArrayTask {
    tasks: Vec<Box<dyn DecodePageTask>>,
    should_validate: bool,
    data_type: DataType,
}

impl StructuralCompositeDecodeArrayTask {
    fn restore_validity(
        array: Arc<dyn Array>,
        unraveler: &mut CompositeRepDefUnraveler,
    ) -> Result<Arc<dyn Array>> {
        let validity = unraveler.unravel_validity(array.len())?;
        let Some(validity) = validity else {
            return Ok(array);
        };
        if array.data_type() == &DataType::Null {
            // We unravel from a null array but we don't add the null buffer because arrow-rs doesn't like it
            return Ok(array);
        }
        if validity.len() != array.len() {
            return Err(Error::invalid_input_source(
                format!(
                    "Structural validity has {} entries for an array with {} values",
                    validity.len(),
                    array.len()
                )
                .into(),
            ));
        }
        // SAFETY: The array buffers have already been validated and the null buffer length
        // matches the array. We are only attaching the null buffer here.
        Ok(make_array(unsafe {
            array
                .to_data()
                .into_builder()
                .nulls(Some(validity))
                .build_unchecked()
        }))
    }
}

impl StructuralDecodeArrayTask for StructuralCompositeDecodeArrayTask {
    fn decode(self: Box<Self>) -> Result<DecodedArray> {
        let mut arrays = Vec::with_capacity(self.tasks.len());
        let mut unravelers = Vec::with_capacity(self.tasks.len());
        let mut data_size = 0u64;
        for task in self.tasks {
            let decoded = task.decode()?;
            data_size += decoded.data.data_size();
            unravelers.push(decoded.repdef);

            let array = make_array(
                decoded
                    .data
                    .into_arrow(self.data_type.clone(), self.should_validate)?,
            );

            arrays.push(array);
        }
        let array_refs = arrays.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();
        let array = arrow_select::concat::concat(&array_refs)?;
        let mut repdef = CompositeRepDefUnraveler::new(unravelers);

        let array = Self::restore_validity(array, &mut repdef)?;

        Ok(DecodedArray {
            array,
            repdef,
            data_size,
        })
    }
}

/// Returns the total bytes consumed by validity bitmaps of nested child fields
/// for `rows` logical rows of `data_type`.
///
/// Only `FixedSizeList` types have nested children that contribute additional
/// bitmaps; all other fixed-width leaf types return 0.
fn fsl_child_bitmap_bytes(data_type: &arrow_schema::DataType, rows: u64) -> u64 {
    match data_type {
        arrow_schema::DataType::FixedSizeList(child_field, dimension) => {
            let child_rows = rows * *dimension as u64;
            let own = if child_field.is_nullable() {
                child_rows.div_ceil(8)
            } else {
                0
            };
            own + fsl_child_bitmap_bytes(child_field.data_type(), child_rows)
        }
        _ => 0,
    }
}

#[derive(Debug)]
pub struct StructuralPrimitiveFieldDecoder {
    field: Arc<ArrowField>,
    page_decoders: VecDeque<Box<dyn StructuralPageDecoder>>,
    should_validate: bool,
    rows_drained_in_current: u64,
}

impl StructuralPrimitiveFieldDecoder {
    pub fn new(field: &Arc<ArrowField>, should_validate: bool) -> Self {
        Self {
            field: field.clone(),
            page_decoders: VecDeque::new(),
            should_validate,
            rows_drained_in_current: 0,
        }
    }

    fn decoded_bytes_for_rows(&self, rows: u64) -> lance_core::Result<u64> {
        let mut remaining = rows;
        let mut total = 0u64;
        for (page_num, page) in self.page_decoders.iter().enumerate() {
            if remaining == 0 {
                break;
            }
            let available = if page_num == 0 {
                page.num_rows().saturating_sub(self.rows_drained_in_current)
            } else {
                page.num_rows()
            };
            let take = available.min(remaining);
            total += page.decoded_bytes(take)?;
            remaining -= take;
        }
        if remaining > 0 {
            return Err(lance_core::Error::not_supported(format!(
                "plan_decoded_bytes: queued pages cover only {} of {} requested rows",
                rows - remaining,
                rows,
            )));
        }
        Ok(total)
    }
}

impl StructuralFieldDecoder for StructuralPrimitiveFieldDecoder {
    fn accept_page(&mut self, child: LoadedPageShard) -> Result<()> {
        assert!(child.path.is_empty());
        self.page_decoders.push_back(child.decoder);
        Ok(())
    }

    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn StructuralDecodeArrayTask>> {
        let mut remaining = num_rows;
        let mut tasks = Vec::new();
        while remaining > 0 {
            let queued_pages = self.page_decoders.len();
            let Some(cur_page) = self.page_decoders.front_mut() else {
                return Err(Error::internal(format!(
                    "Primitive decoder missing page decoder while draining field '{}' (data_type={:?}, requested_rows={}, remaining_rows={}, rows_drained_in_current={}, queued_pages={})",
                    self.field.name(),
                    self.field.data_type(),
                    num_rows,
                    remaining,
                    self.rows_drained_in_current,
                    queued_pages
                )));
            };
            let num_in_page = cur_page.num_rows() - self.rows_drained_in_current;
            let to_take = num_in_page.min(remaining);

            let task = cur_page.drain(to_take)?;
            tasks.push(task);

            if to_take == num_in_page {
                self.page_decoders.pop_front();
                self.rows_drained_in_current = 0;
            } else {
                self.rows_drained_in_current += to_take;
            }

            remaining -= to_take;
        }
        Ok(Box::new(StructuralCompositeDecodeArrayTask {
            tasks,
            should_validate: self.should_validate,
            data_type: self.field.data_type().clone(),
        }))
    }

    fn data_type(&self) -> &DataType {
        self.field.data_type()
    }

    fn plan_decoded_bytes(&self, rows_remaining: u64) -> lance_core::Result<[u64; 8]> {
        use crate::decoder::CANDIDATE_BATCH_SIZES;
        let data_type = self.field.data_type();
        let is_nullable = self.field.is_nullable();

        // Fixed-width: exact from type metadata, no page inspection needed.
        if let Some(byte_width) = data_type.byte_width_opt() {
            let mut out = [0u64; 8];
            for (i, &c) in CANDIDATE_BATCH_SIZES.iter().enumerate() {
                let rows = (c as u64).min(rows_remaining);
                out[i] = rows * byte_width as u64;
                if is_nullable {
                    out[i] += rows.div_ceil(8);
                }
                out[i] += fsl_child_bitmap_bytes(data_type, rows);
            }
            return Ok(out);
        }

        // Boolean: bit-packed, 1 bit per value.
        if matches!(data_type, DataType::Boolean) {
            let mut out = [0u64; 8];
            for (i, &c) in CANDIDATE_BATCH_SIZES.iter().enumerate() {
                let rows = (c as u64).min(rows_remaining);
                out[i] = rows.div_ceil(8);
                if is_nullable {
                    out[i] += rows.div_ceil(8);
                }
            }
            return Ok(out);
        }

        // Null type: no data bytes.
        if matches!(data_type, DataType::Null) {
            return Ok([0u64; 8]);
        }

        // Variable-width: walk page decoders for exact byte counts.
        let mut out = [0u64; 8];
        for (i, &c) in CANDIDATE_BATCH_SIZES.iter().enumerate() {
            let rows = (c as u64).min(rows_remaining);
            out[i] = self.decoded_bytes_for_rows(rows)?;
            if is_nullable {
                out[i] += rows.div_ceil(8);
            }
        }
        Ok(out)
    }
}

/// The serialized representation of full-zip data
struct SerializedFullZip {
    /// The zipped values buffer
    values: LanceBuffer,
    /// The repetition index (only present if there is repetition)
    repetition_index: Option<LanceBuffer>,
}

// We align and pad mini-blocks to 8 byte boundaries for two reasons.  First,
// to allow us to store a chunk size in 12 bits.
//
// If we directly record the size in bytes with 12 bits we would be limited to
// 4KiB which is too small.  Since we know each mini-block consists of 8 byte
// words we can store the # of words instead which gives us 32KiB.
//
// Second, each chunk in a mini-block is aligned to 8 bytes.  This allows multi-byte
// values like offsets to be stored in a mini-block and safely read back out.  It also
// helps ensure zero-copy reads in cases where zero-copy is possible (e.g. no decoding
// needed).
//
// Note: by "aligned to 8 bytes" we mean BOTH "aligned to 8 bytes from the start of
// the page" and "aligned to 8 bytes from the start of the file."
const MINIBLOCK_ALIGNMENT: usize = 8;

/// An encoder for primitive (leaf) arrays
///
/// This encoder is fairly complicated and follows a number of paths depending
/// on the data.
///
/// First, we convert the validity & offsets information into repetition and
/// definition levels.  Then we compress the data itself into a single buffer.
///
/// If the data is narrow then we encode the data in small chunks (each chunk
/// should be a few disk sectors and contains a buffer of repetition, a buffer
/// of definition, and a buffer of value data).  This approach is called
/// "mini-block".  These mini-blocks are stored into a single data buffer.
///
/// If the data is wide then we zip together the repetition and definition value
/// with the value data into a single buffer.  This approach is called "zipped".
///
/// If there is any repetition information then we create a repetition index
///
/// In addition, the compression process may create zero or more metadata buffers.
/// For example, a dictionary compression will create dictionary metadata.  Any
/// mini-block approach has a metadata buffer of block sizes.  This metadata is
/// stored in a separate buffer on disk and read at initialization time.
///
/// TODO: We should concatenate metadata buffers from all pages into a single buffer
/// at (roughly) the end of the file so there is, at most, one read per column of
/// metadata per file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MiniblockChunkSize {
    U16,
    U32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ComplexNullEncoding {
    RawLevels,
    CompressedLevels,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FixedWidthDictionaryEncoding {
    Exclude64Bit,
    Include64Bit,
}

trait PrimitivePageEncodingBehavior: Send + Sync + Debug {
    fn validate_field(&self, _field: &Field, _metadata: &HashMap<String, String>) -> Result<()> {
        Ok(())
    }

    fn try_plan_pages(
        &self,
        _ctx: &PrimitivePlanContext<'_>,
        _arrays: &[ArrayRef],
        _normalized: &NormalizedStructuralPlan,
        _row_number: u64,
        _num_rows: u64,
        _num_values: u64,
    ) -> Result<Option<Vec<PrimitivePageData>>> {
        Ok(None)
    }

    fn try_encode_page(
        &self,
        _ctx: &PrimitiveEncodeContext,
        page: PrimitivePageData,
    ) -> Result<PrimitiveEncodeAttempt> {
        Ok(PrimitiveEncodeAttempt::Unhandled(page))
    }
}

/// One executable primitive-page behavior selected by an exact file
/// composition.
#[derive(Debug, Clone)]
pub struct PrimitivePageEncoding {
    behavior: Arc<dyn PrimitivePageEncodingBehavior>,
}

impl PrimitivePageEncoding {
    /// Reject an explicit request for sparse structural encoding.
    pub fn reject_sparse() -> Self {
        Self {
            behavior: Arc::new(RejectSparsePrimitiveEncoding),
        }
    }

    /// Encode constant non-null values as a constant page when applicable.
    pub fn constant() -> Self {
        Self {
            behavior: Arc::new(ConstantPrimitiveEncoding),
        }
    }

    /// Plan and encode sparse structural pages when applicable.
    pub fn sparse(compression: Arc<dyn CompressionStrategy>) -> Self {
        Self {
            behavior: Arc::new(SparsePrimitiveEncoding { compression }),
        }
    }

    /// Encode dense pages with the original u16 miniblock grammar.
    pub fn dense_u16(compression: Arc<dyn CompressionStrategy>) -> Self {
        Self {
            behavior: Arc::new(DenseU16PrimitiveEncoding { compression }),
        }
    }

    /// Encode dense pages with the u32 miniblock grammar.
    pub fn dense_u32(compression: Arc<dyn CompressionStrategy>) -> Self {
        Self {
            behavior: Arc::new(DenseU32PrimitiveEncoding { compression }),
        }
    }
}

#[derive(Debug)]
struct RejectSparsePrimitiveEncoding;

#[derive(Debug)]
struct ConstantPrimitiveEncoding;

#[derive(Debug)]
struct SparsePrimitiveEncoding {
    compression: Arc<dyn CompressionStrategy>,
}

#[derive(Debug)]
struct DenseU16PrimitiveEncoding {
    compression: Arc<dyn CompressionStrategy>,
}

#[derive(Debug)]
struct DenseU32PrimitiveEncoding {
    compression: Arc<dyn CompressionStrategy>,
}

pub struct PrimitiveStructuralEncoder {
    // Accumulates arrays until we have enough data to justify a disk page
    accumulation_queue: AccumulationQueue,

    keep_original_array: bool,
    accumulated_repdefs: Vec<RepDefBuilder>,
    page_encodings: Arc<[PrimitivePageEncoding]>,
    column_index: u32,
    field: Field,
    encoding_metadata: Arc<HashMap<String, String>>,
}

struct CompressedLevelsChunk {
    data: LanceBuffer,
    num_levels: u16,
}

struct CompressedLevels {
    data: Vec<CompressedLevelsChunk>,
    compression: CompressiveEncoding,
    rep_index: Option<LanceBuffer>,
}

struct SerializedMiniBlockPage {
    num_buffers: u64,
    data: LanceBuffer,
    metadata: LanceBuffer,
}

#[derive(Debug, Clone, Copy)]
struct DictEncodingBudget {
    max_dict_entries: u32,
    max_encoded_size: usize,
}

enum PrimitivePageStructure {
    Dense {
        repdef: SerializedRepDefs,
        single_row_miniblock_repdef_levels: Option<u64>,
    },
    Sparse {
        plan: sparse::SparseStructuralPlan,
        prepared_values: Option<sparse::writer::PreparedSparseValues>,
    },
}

// A primitive page after structural encoding selection and optional dense splitting.
struct PrimitivePageData {
    // Arrow leaf arrays that contain this page's visible values.
    arrays: Vec<ArrayRef>,
    // Structural representation aligned to this page.
    structure: PrimitivePageStructure,
    // Top-level row number of the first row in this page.
    row_number: u64,
    // Number of top-level rows in this page.
    num_rows: u64,
}

struct PrimitivePlanContext<'a> {
    column_idx: u32,
    field: &'a Field,
    encoding_metadata: &'a HashMap<String, String>,
}

enum PrimitiveEncodeAttempt {
    Encoded(EncodedPage),
    Unhandled(PrimitivePageData),
}

// Immutable encoder state shared by per-page encode tasks.
//
// Cloning this only clones Arc-backed configuration and field metadata.  Page data
// stays in PrimitivePageData and is moved into exactly one task.
#[derive(Clone)]
struct PrimitiveEncodeContext {
    // Column being encoded.
    column_idx: u32,
    field: Field,
    encoding_metadata: Arc<HashMap<String, String>>,
    is_simple_validity: bool,
    has_repdef_info: bool,
}

impl PrimitiveStructuralEncoder {
    pub fn try_new(
        options: &EncodingOptions,
        page_encodings: Arc<[PrimitivePageEncoding]>,
        column_index: u32,
        field: Field,
        encoding_metadata: Arc<HashMap<String, String>>,
    ) -> Result<Self> {
        for page_encoding in page_encodings.iter() {
            page_encoding
                .behavior
                .validate_field(&field, &encoding_metadata)?;
        }
        Ok(Self {
            accumulation_queue: AccumulationQueue::new(
                options.cache_bytes_per_column,
                column_index,
                options.keep_original_array,
            ),
            keep_original_array: options.keep_original_array,
            accumulated_repdefs: Vec::new(),
            column_index,
            page_encodings,
            field,
            encoding_metadata,
        })
    }

    fn encode_page(
        page_encodings: &[PrimitivePageEncoding],
        ctx: &PrimitiveEncodeContext,
        mut page: PrimitivePageData,
    ) -> Result<EncodedPage> {
        for page_encoding in page_encodings {
            match page_encoding.behavior.try_encode_page(ctx, page)? {
                PrimitiveEncodeAttempt::Encoded(page) => return Ok(page),
                PrimitiveEncodeAttempt::Unhandled(unhandled) => page = unhandled,
            }
        }
        Err(Error::invalid_input_source(
            format!(
                "No primitive page encoding atom supports field '{}'",
                ctx.field.name
            )
            .into(),
        ))
    }

    // TODO: This is a heuristic we may need to tune at some point
    //
    // As data gets narrow then the "zipping" process gets too expensive
    //   and we prefer mini-block
    // As data gets wide then the # of values per block shrinks (very wide)
    //   data doesn't even fit in a mini-block and the block overhead gets
    //   too large and we prefer zipped.
    fn is_narrow(data_block: &DataBlock) -> bool {
        const MINIBLOCK_MAX_BYTE_LENGTH_PER_VALUE: u64 = 256;

        if let Some(max_len_array) = data_block.get_stat(Stat::MaxLength) {
            let max_len_array = max_len_array
                .as_any()
                .downcast_ref::<PrimitiveArray<UInt64Type>>()
                .unwrap();
            if max_len_array.value(0) < MINIBLOCK_MAX_BYTE_LENGTH_PER_VALUE {
                return true;
            }
        }
        false
    }

    fn prefers_miniblock(
        data_block: &DataBlock,
        encoding_metadata: &HashMap<String, String>,
    ) -> bool {
        // If the user specifically requested miniblock then use it
        if let Some(user_requested) = encoding_metadata.get(STRUCTURAL_ENCODING_META_KEY) {
            return user_requested.to_lowercase() == STRUCTURAL_ENCODING_MINIBLOCK;
        }
        // Otherwise only use miniblock if it is narrow
        Self::is_narrow(data_block)
    }

    fn prefers_fullzip(encoding_metadata: &HashMap<String, String>) -> bool {
        // Fullzip is the backup option so the only reason we wouldn't use it is if the
        // user specifically requested not to use it (in which case we're probably going
        // to emit an error)
        if let Some(user_requested) = encoding_metadata.get(STRUCTURAL_ENCODING_META_KEY) {
            return user_requested.to_lowercase() == STRUCTURAL_ENCODING_FULLZIP;
        }
        true
    }

    // Converts value data, repetition levels, and definition levels into a single
    // buffer of mini-blocks.  In addition, creates a buffer of mini-block metadata
    // which tells us the size of each block.  Finally, if repetition is present then
    // we also create a buffer for the repetition index.
    //
    // Each chunk is serialized as:
    // | num_bufs (1 byte) | buf_lens (2 bytes per buffer) | P | buf0 | P | buf1 | ... | bufN | P |
    //
    // P - Padding inserted to ensure each buffer is 8-byte aligned and the buffer size is a multiple
    //     of 8 bytes (so that the next chunk is 8-byte aligned).
    //
    // Each block has a u16 word of metadata.  The upper 12 bits contain the
    // # of 8-byte words in the block (if the block does not fill the final word
    // then up to 7 bytes of padding are added).  The lower 4 bits describe the log_2
    // number of values (e.g. if there are 1024 then the lower 4 bits will be
    // 0xA)  All blocks except the last must have power-of-two number of values.
    // This not only makes metadata smaller but it makes decoding easier since
    // batch sizes are typically a power of 2.  4 bits would allow us to express
    // up to 32Ki values.
    //
    // This means blocks can have 1 to 32Ki values and 8 - 32Ki bytes.
    //
    // All metadata words are serialized (as little endian) into a single buffer
    // of metadata values.
    //
    // If there is repetition then we also create a repetition index.  This is a
    // single buffer of integer vectors (stored in row major order).  There is one
    // entry for each chunk.  The size of the vector is based on the depth of random
    // access we want to support.
    //
    // A vector of size 2 is the minimum and will support row-based random access (e.g.
    // "take the 57th row").  A vector of size 3 will support 1 level of nested access
    // (e.g. "take the 3rd item in the 57th row").  A vector of size 4 will support 2
    // levels of nested access and so on.
    //
    // The first number in the vector is the number of top-level rows that complete in
    // the chunk.  The second number is the number of second-level rows that complete
    // after the final top-level row completed (or beginning of the chunk if no top-level
    // row completes in the chunk).  And so on.  The final number in the vector is always
    // the number of leftover items not covered by earlier entries in the vector.
    //
    // Currently we are limited to 0 levels of nested access but that will change in the
    // future.
    //
    // The repetition index and the chunk metadata are read at initialization time and
    // cached in memory.
    fn serialize_miniblocks(
        miniblocks: MiniBlockCompressed,
        rep: Option<Vec<CompressedLevelsChunk>>,
        def: Option<Vec<CompressedLevelsChunk>>,
        miniblock_chunk_size: MiniblockChunkSize,
    ) -> Result<SerializedMiniBlockPage> {
        let bytes_rep = rep
            .as_ref()
            .map(|rep| rep.iter().map(|r| r.data.len()).sum::<usize>())
            .unwrap_or(0);
        let bytes_def = def
            .as_ref()
            .map(|def| def.iter().map(|d| d.data.len()).sum::<usize>())
            .unwrap_or(0);
        let bytes_data = miniblocks.data.iter().map(|d| d.len()).sum::<usize>();
        let mut num_buffers = miniblocks.data.len();
        if rep.is_some() {
            num_buffers += 1;
        }
        if def.is_some() {
            num_buffers += 1;
        }
        // 2 bytes for the length of each buffer and up to 7 bytes of padding per buffer
        let max_extra = 9 * num_buffers;
        let mut data_buffer = Vec::with_capacity(bytes_rep + bytes_def + bytes_data + max_extra);
        let chunk_size_bytes = match miniblock_chunk_size {
            MiniblockChunkSize::U16 => 2,
            MiniblockChunkSize::U32 => 4,
        };
        let mut meta_buffer = Vec::with_capacity(miniblocks.chunks.len() * chunk_size_bytes);

        let mut rep_iter = rep.map(|r| r.into_iter());
        let mut def_iter = def.map(|d| d.into_iter());

        let mut buffer_offsets = vec![0; miniblocks.data.len()];
        for chunk in miniblocks.chunks {
            let start_pos = data_buffer.len();
            // Start of chunk should be aligned
            debug_assert_eq!(start_pos % MINIBLOCK_ALIGNMENT, 0);

            let rep = rep_iter.as_mut().map(|r| r.next().unwrap());
            let def = def_iter.as_mut().map(|d| d.next().unwrap());

            // Write the number of levels, or 0 if there is no rep/def
            let num_levels = rep
                .as_ref()
                .map(|r| r.num_levels)
                .unwrap_or(def.as_ref().map(|d| d.num_levels).unwrap_or(0));
            data_buffer.extend_from_slice(&num_levels.to_le_bytes());

            // Write the buffer lengths
            if let Some(rep) = rep.as_ref() {
                let bytes_rep = u16::try_from(rep.data.len()).map_err(|_| {
                    Error::internal(format!(
                        "Repetition buffer size ({} bytes) too large",
                        rep.data.len()
                    ))
                })?;
                data_buffer.extend_from_slice(&bytes_rep.to_le_bytes());
            }
            if let Some(def) = def.as_ref() {
                let bytes_def = u16::try_from(def.data.len()).map_err(|_| {
                    Error::internal(format!(
                        "Definition buffer size ({} bytes) too large",
                        def.data.len()
                    ))
                })?;
                data_buffer.extend_from_slice(&bytes_def.to_le_bytes());
            }

            if miniblock_chunk_size == MiniblockChunkSize::U32 {
                for &buffer_size in &chunk.buffer_sizes {
                    data_buffer.extend_from_slice(&buffer_size.to_le_bytes());
                }
            } else {
                for &buffer_size in &chunk.buffer_sizes {
                    let buffer_size = u16::try_from(buffer_size).map_err(|_| {
                        Error::internal(format!(
                            "Mini-block buffer size ({} bytes) too large for 16-bit metadata",
                            buffer_size
                        ))
                    })?;
                    data_buffer.extend_from_slice(&buffer_size.to_le_bytes());
                }
            }

            // Pad
            let add_padding = |data_buffer: &mut Vec<u8>| {
                let pad = pad_bytes::<MINIBLOCK_ALIGNMENT>(data_buffer.len());
                data_buffer.extend(iter::repeat_n(FILL_BYTE, pad));
            };
            add_padding(&mut data_buffer);

            // Write the buffers themselves
            if let Some(rep) = rep.as_ref() {
                data_buffer.extend_from_slice(&rep.data);
                add_padding(&mut data_buffer);
            }
            if let Some(def) = def.as_ref() {
                data_buffer.extend_from_slice(&def.data);
                add_padding(&mut data_buffer);
            }
            for (buffer_size, (buffer, buffer_offset)) in chunk
                .buffer_sizes
                .iter()
                .zip(miniblocks.data.iter().zip(buffer_offsets.iter_mut()))
            {
                let start = *buffer_offset;
                let end = start + *buffer_size as usize;
                *buffer_offset += *buffer_size as usize;
                data_buffer.extend_from_slice(&buffer[start..end]);
                add_padding(&mut data_buffer);
            }

            let chunk_bytes = data_buffer.len() - start_pos;
            let max_chunk_size = match miniblock_chunk_size {
                MiniblockChunkSize::U16 => 32 * 1024,
                MiniblockChunkSize::U32 => 1_u64 << 31,
            };
            if chunk_bytes == 0 || chunk_bytes as u64 > max_chunk_size {
                return Err(Error::internal(format!(
                    "Mini-block chunk size {} bytes exceeds the {} byte metadata limit",
                    chunk_bytes, max_chunk_size
                )));
            }
            if chunk_bytes % MINIBLOCK_ALIGNMENT != 0 {
                return Err(Error::internal(format!(
                    "Mini-block chunk size {} bytes is not aligned to {} bytes",
                    chunk_bytes, MINIBLOCK_ALIGNMENT
                )));
            }
            if chunk.log_num_values > 15 {
                return Err(Error::internal(format!(
                    "Mini-block log_num_values {} exceeds the 4-bit metadata limit",
                    chunk.log_num_values
                )));
            }
            // We subtract 1 here from chunk_bytes because we want to be able to express
            // a size of 32KiB and not (32Ki - 8)B which is what we'd get otherwise with
            // 0xFFF
            let divided_bytes = chunk_bytes / MINIBLOCK_ALIGNMENT;
            let divided_bytes_minus_one = (divided_bytes - 1) as u64;

            let metadata = (divided_bytes_minus_one << 4) | chunk.log_num_values as u64;
            if miniblock_chunk_size == MiniblockChunkSize::U32 {
                meta_buffer.extend_from_slice(&(metadata as u32).to_le_bytes());
            } else {
                meta_buffer.extend_from_slice(&(metadata as u16).to_le_bytes());
            }
        }

        let data_buffer = LanceBuffer::from(data_buffer);
        let metadata_buffer = LanceBuffer::from(meta_buffer);

        Ok(SerializedMiniBlockPage {
            num_buffers: miniblocks.data.len() as u64,
            data: data_buffer,
            metadata: metadata_buffer,
        })
    }

    /// Compresses a buffer of levels into chunks
    ///
    /// If these are repetition levels then we also calculate the repetition index here (that
    /// is the third return value)
    fn compress_levels(
        mut levels: RepDefSlicer<'_>,
        num_elements: u64,
        compression_strategy: &dyn CompressionStrategy,
        chunks: &[MiniBlockChunk],
        // This will be 0 if we are compressing def levels
        max_rep: u16,
    ) -> Result<CompressedLevels> {
        let mut rep_index = if max_rep > 0 {
            Vec::with_capacity(chunks.len())
        } else {
            vec![]
        };
        // Make the levels into a FixedWidth data block
        let num_levels = levels.num_levels() as u64;
        let levels_buf = levels.all_levels().clone();

        let mut fixed_width_block = FixedWidthDataBlock {
            data: levels_buf,
            bits_per_value: 16,
            num_values: num_levels,
            block_info: BlockInfo::new(),
        };
        // Compute statistics to enable optimal compression for rep/def levels
        fixed_width_block.compute_stat();

        let levels_block = DataBlock::FixedWidth(fixed_width_block);
        let levels_field = Field::new_arrow("", DataType::UInt16, false)?;
        // Pick a block compressor
        let compressor =
            compression_strategy.create_block_compressor(&levels_field, &levels_block)?;
        let mut compressor_desc = None;
        // Compress blocks of levels (sized according to the chunks)
        let mut level_chunks = Vec::with_capacity(chunks.len());
        let mut values_counter = 0;
        for (chunk_idx, chunk) in chunks.iter().enumerate() {
            let chunk_num_values = chunk.num_values(values_counter, num_elements);
            debug_assert!(chunk_num_values > 0);
            values_counter += chunk_num_values;
            let chunk_levels = if chunk_idx < chunks.len() - 1 {
                levels.slice_next(chunk_num_values as usize)
            } else {
                levels.slice_rest()
            };
            let num_chunk_levels = (chunk_levels.len() / 2) as u64;
            if max_rep > 0 {
                // If max_rep > 0 then we are working with rep levels and we need
                // to calculate the repetition index.  The repetition index for a
                // chunk is currently 2 values (in the future it may be more).
                //
                // The first value is the number of rows that _finish_ in the
                // chunk.
                //
                // The second value is the number of "leftovers" after the last
                // finished row in the chunk.
                let rep_values = chunk_levels.borrow_to_typed_slice::<u16>();
                let rep_values = rep_values.as_ref();

                // We skip 1 here because a max_rep at spot 0 doesn't count as a finished list (we
                // will count it in the previous chunk)
                let mut num_rows = rep_values.iter().skip(1).filter(|v| **v == max_rep).count();
                let num_leftovers = if chunk_idx < chunks.len() - 1 {
                    rep_values
                        .iter()
                        .rev()
                        .position(|v| *v == max_rep)
                        // # of leftovers includes the max_rep spot
                        .map(|pos| pos + 1)
                        .unwrap_or(rep_values.len())
                } else {
                    // Last chunk can't have leftovers
                    0
                };

                if chunk_idx != 0 && rep_values.first() == Some(&max_rep) {
                    // This chunk starts with a new row and so, if we thought we had leftovers
                    // in the previous chunk, we were mistaken
                    // TODO: Can use unchecked here
                    let rep_len = rep_index.len();
                    if rep_index[rep_len - 1] != 0 {
                        // We thought we had leftovers but that was actually a full row
                        rep_index[rep_len - 2] += 1;
                        rep_index[rep_len - 1] = 0;
                    }
                }

                if chunk_idx == chunks.len() - 1 {
                    // The final list
                    num_rows += 1;
                }
                rep_index.push(num_rows as u64);
                rep_index.push(num_leftovers as u64);
            }
            let mut chunk_fixed_width = FixedWidthDataBlock {
                data: chunk_levels,
                bits_per_value: 16,
                num_values: num_chunk_levels,
                block_info: BlockInfo::new(),
            };
            chunk_fixed_width.compute_stat();
            let chunk_levels_block = DataBlock::FixedWidth(chunk_fixed_width);
            let (compressed_levels, chunk_compressor_desc) =
                compressor.compress(chunk_levels_block)?;
            if let Some(compressor_desc) = compressor_desc.as_ref() {
                if compressor_desc != &chunk_compressor_desc {
                    return Err(Error::internal(
                        "Rep/def block compressor changed encoding between chunks".to_string(),
                    ));
                }
            } else {
                compressor_desc = Some(chunk_compressor_desc);
            }
            let compressed_levels = compressed_levels.ok_or_else(|| {
                Error::internal(
                    "Rep/def block compressor selected a metadata-only codec".to_string(),
                )
            })?;
            let num_levels = u16::try_from(num_chunk_levels).map_err(|_| {
                Error::invalid_input_source(
                    format!(
                        "Mini-block cannot encode {} rep/def levels in one chunk. \
                         This usually means a top-level row contains too much nested structure \
                         for the current layout.",
                        num_chunk_levels
                    )
                    .into(),
                )
            })?;
            level_chunks.push(CompressedLevelsChunk {
                data: compressed_levels,
                num_levels,
            });
        }
        debug_assert_eq!(levels.num_levels_remaining(), 0);
        let rep_index = if rep_index.is_empty() {
            None
        } else {
            Some(LanceBuffer::reinterpret_vec(rep_index))
        };
        Ok(CompressedLevels {
            data: level_chunks,
            compression: compressor_desc.ok_or_else(|| {
                Error::internal("Rep/def compression produced no chunks".to_string())
            })?,
            rep_index,
        })
    }

    fn encode_simple_all_null(
        column_idx: u32,
        num_rows: u64,
        row_number: u64,
    ) -> Result<EncodedPage> {
        let description =
            ProtobufUtils21::constant_layout(&[DefinitionInterpretation::NullableItem], None);
        Ok(EncodedPage {
            column_idx,
            data: vec![],
            description: PageEncoding::Structural(description),
            num_rows,
            row_number,
        })
    }

    fn encode_complex_all_null_vals(
        data: &Arc<[u16]>,
        compression_strategy: &dyn CompressionStrategy,
    ) -> Result<(LanceBuffer, pb21::CompressiveEncoding)> {
        let buffer = LanceBuffer::reinterpret_slice(data.clone());
        let mut fixed_width_block = FixedWidthDataBlock {
            data: buffer,
            bits_per_value: 16,
            num_values: data.len() as u64,
            block_info: BlockInfo::new(),
        };
        fixed_width_block.compute_stat();

        let levels_block = DataBlock::FixedWidth(fixed_width_block);
        let levels_field = Field::new_arrow("", DataType::UInt16, false)?;
        let (compressed_buffer, encoding) =
            compress_required_block(compression_strategy, &levels_field, levels_block)?;
        Ok((compressed_buffer, encoding))
    }

    // Encodes a page where all values are null but we have rep/def
    // information that we need to store (e.g. to distinguish between
    // different kinds of null)
    fn encode_complex_all_null(
        column_idx: u32,
        repdef: crate::repdef::SerializedRepDefs,
        row_number: u64,
        num_rows: u64,
        complex_null_encoding: ComplexNullEncoding,
        compression_strategy: &dyn CompressionStrategy,
    ) -> Result<EncodedPage> {
        if complex_null_encoding == ComplexNullEncoding::RawLevels {
            let rep_bytes = if let Some(rep) = repdef.repetition_levels.as_ref() {
                LanceBuffer::reinterpret_slice(rep.clone())
            } else {
                LanceBuffer::empty()
            };

            let def_bytes = if let Some(def) = repdef.definition_levels.as_ref() {
                LanceBuffer::reinterpret_slice(def.clone())
            } else {
                LanceBuffer::empty()
            };

            let description = ProtobufUtils21::constant_layout(&repdef.def_meaning, None);
            return Ok(EncodedPage {
                column_idx,
                data: vec![rep_bytes, def_bytes],
                description: PageEncoding::Structural(description),
                num_rows,
                row_number,
            });
        }

        let (rep_bytes, rep_encoding, num_rep_values) = if let Some(rep) =
            repdef.repetition_levels.as_ref()
        {
            let num_values = rep.len() as u64;
            let (buffer, encoding) = Self::encode_complex_all_null_vals(rep, compression_strategy)?;
            (buffer, Some(encoding), num_values)
        } else {
            (LanceBuffer::empty(), None, 0)
        };

        let (def_bytes, def_encoding, num_def_values) = if let Some(def) =
            repdef.definition_levels.as_ref()
        {
            let num_values = def.len() as u64;
            let (buffer, encoding) = Self::encode_complex_all_null_vals(def, compression_strategy)?;
            (buffer, Some(encoding), num_values)
        } else {
            (LanceBuffer::empty(), None, 0)
        };

        let description = ProtobufUtils21::compressed_all_null_constant_layout(
            &repdef.def_meaning,
            rep_encoding,
            def_encoding,
            num_rep_values,
            num_def_values,
        );
        Ok(EncodedPage {
            column_idx,
            data: vec![rep_bytes, def_bytes],
            description: PageEncoding::Structural(description),
            num_rows,
            row_number,
        })
    }

    fn leaf_validity(
        repdef: &crate::repdef::SerializedRepDefs,
        num_values: usize,
    ) -> Result<Option<BooleanBuffer>> {
        let rep = repdef
            .repetition_levels
            .as_ref()
            .map(|rep| rep.as_ref().to_vec());
        let def = repdef
            .definition_levels
            .as_ref()
            .map(|def| def.as_ref().to_vec());
        let mut unraveler = RepDefUnraveler::new(
            rep,
            def,
            repdef.def_meaning.clone().into(),
            num_values as u64,
        );
        if unraveler.is_all_valid() {
            return Ok(None);
        }
        let mut validity = BooleanBufferBuilder::new(num_values);
        unraveler.unravel_validity(&mut validity)?;
        Ok(Some(validity.finish()))
    }

    fn is_constant_values(
        arrays: &[ArrayRef],
        scalar: &ArrayRef,
        validity: Option<&BooleanBuffer>,
    ) -> Result<bool> {
        debug_assert_eq!(scalar.len(), 1);
        debug_assert_eq!(scalar.null_count(), 0);

        match scalar.data_type() {
            DataType::Boolean => {
                let mut global_idx = 0usize;
                let scalar_val = scalar.as_boolean().value(0);
                for arr in arrays {
                    let bool_arr = arr.as_boolean();
                    for i in 0..arr.len() {
                        let is_valid = validity.map(|v| v.value(global_idx)).unwrap_or(true);
                        global_idx += 1;
                        if !is_valid {
                            continue;
                        }
                        if bool_arr.value(i) != scalar_val {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            }
            DataType::Utf8 => Self::is_constant_utf8::<i32>(arrays, scalar, validity),
            DataType::LargeUtf8 => Self::is_constant_utf8::<i64>(arrays, scalar, validity),
            DataType::Binary => Self::is_constant_binary::<i32>(arrays, scalar, validity),
            DataType::LargeBinary => Self::is_constant_binary::<i64>(arrays, scalar, validity),
            data_type => {
                let mut global_idx = 0usize;
                let Some(byte_width) = data_type.byte_width_opt() else {
                    return Ok(false);
                };
                let scalar_data = scalar.to_data();
                if scalar_data.buffers().len() != 1 || !scalar_data.child_data().is_empty() {
                    return Ok(false);
                }
                let scalar_bytes = scalar_data.buffers()[0].as_slice();
                if scalar_bytes.len() != byte_width {
                    return Ok(false);
                }

                for arr in arrays {
                    let data = arr.to_data();
                    if data.buffers().is_empty() {
                        return Ok(false);
                    }
                    let buf = data.buffers()[0].as_slice();
                    let base = data.offset();
                    for i in 0..arr.len() {
                        let is_valid = validity.map(|v| v.value(global_idx)).unwrap_or(true);
                        global_idx += 1;
                        if !is_valid {
                            continue;
                        }
                        let start = (base + i) * byte_width;
                        if buf[start..start + byte_width] != scalar_bytes[..] {
                            return Ok(false);
                        }
                    }
                }
                Ok(true)
            }
        }
    }

    fn is_constant_utf8<O: arrow_array::OffsetSizeTrait>(
        arrays: &[ArrayRef],
        scalar: &ArrayRef,
        validity: Option<&BooleanBuffer>,
    ) -> Result<bool> {
        debug_assert_eq!(scalar.len(), 1);
        let scalar_val = scalar.as_string::<O>().value(0).as_bytes();
        let mut global_idx = 0usize;
        for arr in arrays {
            let str_arr = arr.as_string::<O>();
            for i in 0..arr.len() {
                let is_valid = validity.map(|v| v.value(global_idx)).unwrap_or(true);
                global_idx += 1;
                if !is_valid {
                    continue;
                }
                if str_arr.value(i).as_bytes() != scalar_val {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn is_constant_binary<O: arrow_array::OffsetSizeTrait>(
        arrays: &[ArrayRef],
        scalar: &ArrayRef,
        validity: Option<&BooleanBuffer>,
    ) -> Result<bool> {
        debug_assert_eq!(scalar.len(), 1);
        let scalar_val = scalar.as_binary::<O>().value(0);
        let mut global_idx = 0usize;
        for arr in arrays {
            let bin_arr = arr.as_binary::<O>();
            for i in 0..arr.len() {
                let is_valid = validity.map(|v| v.value(global_idx)).unwrap_or(true);
                global_idx += 1;
                if !is_valid {
                    continue;
                }
                if bin_arr.value(i) != scalar_val {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    fn find_constant_scalar(
        arrays: &[ArrayRef],
        validity: Option<&BooleanBuffer>,
    ) -> Result<Option<ArrayRef>> {
        if arrays.is_empty() {
            return Ok(None);
        }

        let global_scalar_idx = if let Some(validity) = validity {
            let Some(idx) = (0..validity.len()).find(|&i| validity.value(i)) else {
                return Ok(None);
            };
            idx
        } else {
            0
        };

        let mut idx_remaining = global_scalar_idx;
        let mut scalar_arr_idx = 0usize;
        while scalar_arr_idx < arrays.len() {
            let len = arrays[scalar_arr_idx].len();
            if idx_remaining < len {
                break;
            }
            idx_remaining -= len;
            scalar_arr_idx += 1;
        }

        if scalar_arr_idx >= arrays.len() {
            return Ok(None);
        }

        let scalar =
            lance_arrow::scalar::extract_scalar_value(&arrays[scalar_arr_idx], idx_remaining)?;
        if scalar.null_count() != 0 {
            return Ok(None);
        }
        if !Self::is_constant_values(arrays, &scalar, validity)? {
            return Ok(None);
        }
        Ok(Some(scalar))
    }

    fn resolve_dict_values_compression_metadata(
        field_metadata: &HashMap<String, String>,
        env_compression: Option<String>,
        env_compression_level: Option<String>,
    ) -> HashMap<String, String> {
        let mut metadata = HashMap::new();

        let compression = field_metadata
            .get(DICT_VALUES_COMPRESSION_META_KEY)
            .cloned()
            .or(env_compression)
            .unwrap_or_else(|| DEFAULT_DICT_VALUES_COMPRESSION.to_string());
        metadata.insert(COMPRESSION_META_KEY.to_string(), compression);

        if let Some(compression_level) = field_metadata
            .get(DICT_VALUES_COMPRESSION_LEVEL_META_KEY)
            .cloned()
            .or(env_compression_level)
        {
            metadata.insert(COMPRESSION_LEVEL_META_KEY.to_string(), compression_level);
        }

        metadata
    }

    fn build_dict_values_compressor_field(field: &Field) -> Result<Field> {
        // This is an internal synthetic field used only to feed metadata into
        // `create_block_compressor` for dictionary values. The concrete type/name here
        // are not semantically meaningful; we rely on explicit metadata below to control
        // general compression selection for dictionary values.
        let mut dict_values_field = Field::new_arrow("", DataType::UInt16, false)?;
        dict_values_field.metadata = Self::resolve_dict_values_compression_metadata(
            &field.metadata,
            env::var(DICT_VALUES_COMPRESSION_ENV_VAR).ok(),
            env::var(DICT_VALUES_COMPRESSION_LEVEL_ENV_VAR).ok(),
        );
        Ok(dict_values_field)
    }

    #[allow(clippy::too_many_arguments)]
    fn encode_miniblock(
        column_idx: u32,
        field: &Field,
        compression_strategy: &dyn CompressionStrategy,
        data: DataBlock,
        repdef: crate::repdef::SerializedRepDefs,
        row_number: u64,
        dictionary_data: Option<DataBlock>,
        num_rows: u64,
        miniblock_chunk_size: MiniblockChunkSize,
    ) -> Result<EncodedPage> {
        if let DataBlock::AllNull(_null_block) = data {
            // We should not be using mini-block for all-null.  There are other structural
            // encodings for that.
            unreachable!()
        }

        let num_items = data.num_values();

        let compressor = compression_strategy.create_miniblock_compressor(field, &data)?;
        let common_chunk_buffers =
            u64::from(repdef.rep_slicer().is_some()) + u64::from(repdef.def_slicer().is_some());
        let support_large_chunk = miniblock_chunk_size == MiniblockChunkSize::U32;
        let compression_context =
            MiniBlockCompressionContext::new(common_chunk_buffers, support_large_chunk, true);
        let (compressed_data, value_encoding) = compressor.compress(compression_context, data)?;

        let max_rep = repdef.def_meaning.iter().filter(|l| l.is_list()).count() as u16;

        let mut compressed_rep = repdef
            .rep_slicer()
            .map(|rep_slicer| {
                Self::compress_levels(
                    rep_slicer,
                    num_items,
                    compression_strategy,
                    &compressed_data.chunks,
                    max_rep,
                )
            })
            .transpose()?;

        let (rep_index, rep_index_depth) =
            match compressed_rep.as_mut().and_then(|cr| cr.rep_index.as_mut()) {
                Some(rep_index) => (Some(rep_index.clone()), 1),
                None => (None, 0),
            };

        let mut compressed_def = repdef
            .def_slicer()
            .map(|def_slicer| {
                Self::compress_levels(
                    def_slicer,
                    num_items,
                    compression_strategy,
                    &compressed_data.chunks,
                    /*max_rep=*/ 0,
                )
            })
            .transpose()?;

        // TODO: Parquet sparsely encodes values here.  We could do the same but
        // then we won't have log2 values per chunk.  This means more metadata
        // and potentially more decoder asymmetry.  However, it may be worth
        // investigating at some point

        let rep_data = compressed_rep
            .as_mut()
            .map(|cr| std::mem::take(&mut cr.data));
        let def_data = compressed_def
            .as_mut()
            .map(|cd| std::mem::take(&mut cd.data));

        let serialized =
            Self::serialize_miniblocks(compressed_data, rep_data, def_data, miniblock_chunk_size)?;
        let has_large_chunk = miniblock_chunk_size == MiniblockChunkSize::U32;

        // Metadata, Data, Dictionary, (maybe) Repetition Index
        let mut data = Vec::with_capacity(4);
        data.push(serialized.metadata);
        data.push(serialized.data);

        if let Some(dictionary_data) = dictionary_data {
            let num_dictionary_items = dictionary_data.num_values();
            let dict_values_field = Self::build_dict_values_compressor_field(field)?;

            let (dictionary_buffer, dictionary_encoding) =
                compress_required_block(compression_strategy, &dict_values_field, dictionary_data)?;

            data.push(dictionary_buffer);
            if let Some(rep_index) = rep_index {
                data.push(rep_index);
            }

            let description = ProtobufUtils21::miniblock_layout(
                compressed_rep.map(|cr| cr.compression),
                compressed_def.map(|cd| cd.compression),
                value_encoding,
                rep_index_depth,
                serialized.num_buffers,
                Some((dictionary_encoding, num_dictionary_items)),
                &repdef.def_meaning,
                num_items,
                has_large_chunk,
            );
            Ok(EncodedPage {
                num_rows,
                column_idx,
                data,
                description: PageEncoding::Structural(description),
                row_number,
            })
        } else {
            let description = ProtobufUtils21::miniblock_layout(
                compressed_rep.map(|cr| cr.compression),
                compressed_def.map(|cd| cd.compression),
                value_encoding,
                rep_index_depth,
                serialized.num_buffers,
                None,
                &repdef.def_meaning,
                num_items,
                has_large_chunk,
            );

            if let Some(rep_index) = rep_index {
                let view = rep_index.borrow_to_typed_slice::<u64>();
                let total = view.chunks_exact(2).map(|c| c[0]).sum::<u64>();
                debug_assert_eq!(total, num_rows);

                data.push(rep_index);
            }

            Ok(EncodedPage {
                num_rows,
                column_idx,
                data,
                description: PageEncoding::Structural(description),
                row_number,
            })
        }
    }

    // For fixed-size data we encode < control word | data > for each value
    fn serialize_full_zip_fixed(
        fixed: FixedWidthDataBlock,
        mut repdef: ControlWordIterator,
        num_values: u64,
    ) -> Result<SerializedFullZip> {
        if !fixed.bits_per_value.is_multiple_of(8) {
            return Err(Error::invalid_input_source(
                format!(
                    "Full-zip fixed-width values must be byte aligned, got {} bits per value",
                    fixed.bits_per_value
                )
                .into(),
            ));
        }

        let len = fixed.data.len() + repdef.bytes_per_word() * num_values as usize;
        let mut zipped_data = Vec::with_capacity(len);

        let max_rep_index_val = if repdef.has_repetition() {
            len as u64
        } else {
            // Setting this to 0 means we won't write a repetition index
            0
        };
        let mut rep_index_builder =
            BytepackedIntegerEncoder::with_capacity(num_values as usize + 1, max_rep_index_val);

        let bytes_per_value = fixed.bits_per_value as usize / 8;
        let mut offset = 0;

        if bytes_per_value == 0 {
            // No data, just dump the repdef into the buffer
            while let Some(control) = repdef.append_next(&mut zipped_data) {
                if control.is_new_row {
                    // We have finished a row
                    debug_assert!(offset <= len);
                    rep_index_builder.append_trusted(offset as u64);
                }
                offset = zipped_data.len();
            }
        } else {
            // We have data, zip it with the repdef
            let mut data_iter = fixed.data.chunks_exact(bytes_per_value);
            while let Some(control) = repdef.append_next(&mut zipped_data) {
                if control.is_new_row {
                    // We have finished a row
                    debug_assert!(offset <= len);
                    rep_index_builder.append_trusted(offset as u64);
                }
                if control.is_visible {
                    let value = data_iter.next().unwrap();
                    zipped_data.extend_from_slice(value);
                }
                offset = zipped_data.len();
            }
        }

        debug_assert_eq!(zipped_data.len(), len);
        // Put the final value in the rep index
        rep_index_builder.append_trusted(zipped_data.len() as u64);

        let zipped_data = LanceBuffer::from(zipped_data);
        let rep_index = rep_index_builder.into_data();
        let rep_index = if rep_index.is_empty() {
            None
        } else {
            Some(LanceBuffer::from(rep_index))
        };
        Ok(SerializedFullZip {
            values: zipped_data,
            repetition_index: rep_index,
        })
    }

    // For variable-size data we encode < control word | length | data > for each value
    //
    // In addition, we create a second buffer, the repetition index
    fn serialize_full_zip_variable(
        variable: VariableWidthBlock,
        mut repdef: ControlWordIterator,
        num_items: u64,
    ) -> Result<SerializedFullZip> {
        let bytes_per_offset = variable.bits_per_offset as usize / 8;
        if !variable.bits_per_offset.is_multiple_of(8) {
            return Err(Error::invalid_input_source(
                format!(
                    "Full-zip variable-width offsets must be byte aligned, got {} bits per offset",
                    variable.bits_per_offset
                )
                .into(),
            ));
        }
        let len = variable.data.len()
            + repdef.bytes_per_word() * num_items as usize
            + bytes_per_offset * variable.num_values as usize;
        let mut buf = Vec::with_capacity(len);

        let max_rep_index_val = len as u64;
        let mut rep_index_builder =
            BytepackedIntegerEncoder::with_capacity(num_items as usize + 1, max_rep_index_val);

        // TODO: byte pack the item lengths with varint encoding
        match bytes_per_offset {
            4 => {
                let offs = variable.offsets.borrow_to_typed_slice::<u32>();
                let mut rep_offset = 0;
                let mut windows_iter = offs.as_ref().windows(2);
                while let Some(control) = repdef.append_next(&mut buf) {
                    if control.is_new_row {
                        // We have finished a row
                        debug_assert!(rep_offset <= len);
                        rep_index_builder.append_trusted(rep_offset as u64);
                    }
                    if control.is_visible {
                        let window = windows_iter.next().unwrap();
                        if control.is_valid_item {
                            buf.extend_from_slice(&(window[1] - window[0]).to_le_bytes());
                            buf.extend_from_slice(
                                &variable.data[window[0] as usize..window[1] as usize],
                            );
                        }
                    }
                    rep_offset = buf.len();
                }
            }
            8 => {
                let offs = variable.offsets.borrow_to_typed_slice::<u64>();
                let mut rep_offset = 0;
                let mut windows_iter = offs.as_ref().windows(2);
                while let Some(control) = repdef.append_next(&mut buf) {
                    if control.is_new_row {
                        // We have finished a row
                        debug_assert!(rep_offset <= len);
                        rep_index_builder.append_trusted(rep_offset as u64);
                    }
                    if control.is_visible {
                        let window = windows_iter.next().unwrap();
                        if control.is_valid_item {
                            buf.extend_from_slice(&(window[1] - window[0]).to_le_bytes());
                            buf.extend_from_slice(
                                &variable.data[window[0] as usize..window[1] as usize],
                            );
                        }
                    }
                    rep_offset = buf.len();
                }
            }
            _ => {
                return Err(Error::invalid_input_source(
                    format!(
                        "Full-zip variable-width offsets must be 32 or 64 bits, got {} bits",
                        variable.bits_per_offset
                    )
                    .into(),
                ));
            }
        }

        // We might have saved a few bytes by not copying lengths when the length was zero.  However,
        // if we are over `len` then we have a bug.
        debug_assert!(buf.len() <= len);
        // Put the final value in the rep index
        rep_index_builder.append_trusted(buf.len() as u64);

        let zipped_data = LanceBuffer::from(buf);
        let rep_index = rep_index_builder.into_data();
        debug_assert!(!rep_index.is_empty());
        let rep_index = Some(LanceBuffer::from(rep_index));
        Ok(SerializedFullZip {
            values: zipped_data,
            repetition_index: rep_index,
        })
    }

    /// Serializes data into a single buffer according to the full-zip format which zips
    /// together the repetition, definition, and value data into a single buffer.
    fn serialize_full_zip(
        compressed_data: PerValueDataBlock,
        repdef: ControlWordIterator,
        num_items: u64,
    ) -> Result<SerializedFullZip> {
        match compressed_data {
            PerValueDataBlock::Fixed(fixed) => {
                Self::serialize_full_zip_fixed(fixed, repdef, num_items)
            }
            PerValueDataBlock::Variable(var) => {
                Self::serialize_full_zip_variable(var, repdef, num_items)
            }
        }
    }

    fn expand_boolean_to_bytes(fixed: FixedWidthDataBlock) -> FixedWidthDataBlock {
        debug_assert_eq!(fixed.bits_per_value, 1);
        let num_values = fixed.num_values as usize;
        let bool_buf = BooleanBuffer::new(fixed.data.into_buffer(), 0, num_values);
        let expanded: Vec<u8> = (0..num_values).map(|i| bool_buf.value(i) as u8).collect();
        FixedWidthDataBlock {
            data: LanceBuffer::from(expanded),
            bits_per_value: 8,
            num_values: fixed.num_values,
            block_info: BlockInfo::new(),
        }
    }

    fn encode_full_zip(
        column_idx: u32,
        field: &Field,
        compression_strategy: &dyn CompressionStrategy,
        data: DataBlock,
        repdef: crate::repdef::SerializedRepDefs,
        row_number: u64,
        num_lists: u64,
    ) -> Result<EncodedPage> {
        let max_rep = repdef
            .repetition_levels
            .as_ref()
            .map_or(0, |r| r.iter().max().copied().unwrap_or(0));
        let max_def = repdef
            .definition_levels
            .as_ref()
            .map_or(0, |d| d.iter().max().copied().unwrap_or(0));

        // To handle FSL we just flatten
        // let data = data.flatten();

        let (num_items, num_visible_items) =
            if let Some(rep_levels) = repdef.repetition_levels.as_ref() {
                // If there are rep levels there may be "invisible" items and we need to encode
                // rep_levels.len() things which might be larger than data.num_values()
                (rep_levels.len() as u64, data.num_values())
            } else {
                // If there are no rep levels then we encode data.num_values() things
                (data.num_values(), data.num_values())
            };

        let max_visible_def = repdef.max_visible_level.unwrap_or(u16::MAX);

        let repdef_iter = build_control_word_iterator(
            repdef.repetition_levels.as_deref(),
            max_rep,
            repdef.definition_levels.as_deref(),
            max_def,
            max_visible_def,
            num_items as usize,
        );
        let bits_rep = repdef_iter.bits_rep();
        let bits_def = repdef_iter.bits_def();

        // Full-zip requires byte-aligned values; expand 1-bit booleans to 1 byte each.
        let data = match data {
            DataBlock::FixedWidth(fixed) if fixed.bits_per_value == 1 => {
                DataBlock::FixedWidth(Self::expand_boolean_to_bytes(fixed))
            }
            other => other,
        };

        let compressor = compression_strategy.create_per_value(field, &data)?;
        let (compressed_data, value_encoding) = compressor.compress(data)?;

        let description = match &compressed_data {
            PerValueDataBlock::Fixed(fixed) => ProtobufUtils21::fixed_full_zip_layout(
                bits_rep,
                bits_def,
                fixed.bits_per_value as u32,
                value_encoding,
                &repdef.def_meaning,
                num_items as u32,
                num_visible_items as u32,
            ),
            PerValueDataBlock::Variable(variable) => ProtobufUtils21::variable_full_zip_layout(
                bits_rep,
                bits_def,
                variable.bits_per_offset as u32,
                value_encoding,
                &repdef.def_meaning,
                num_items as u32,
                num_visible_items as u32,
            ),
        };

        let zipped = Self::serialize_full_zip(compressed_data, repdef_iter, num_items)?;

        let data = if let Some(repindex) = zipped.repetition_index {
            vec![zipped.values, repindex]
        } else {
            vec![zipped.values]
        };

        Ok(EncodedPage {
            num_rows: num_lists,
            column_idx,
            data,
            description: PageEncoding::Structural(description),
            row_number,
        })
    }

    fn should_dictionary_encode(
        data_block: &DataBlock,
        field: &Field,
        fixed_width_dictionary_encoding: FixedWidthDictionaryEncoding,
    ) -> Option<DictEncodingBudget> {
        const DEFAULT_SAMPLE_SIZE: usize = 4096;
        const DEFAULT_SAMPLE_UNIQUE_RATIO: f64 = 0.98;

        // Since we only dictionary encode FixedWidth and VariableWidth blocks for now, we skip
        // estimating the size for other types.
        match data_block {
            DataBlock::FixedWidth(fixed) => {
                if fixed.bits_per_value == 64
                    && fixed_width_dictionary_encoding == FixedWidthDictionaryEncoding::Exclude64Bit
                {
                    return None;
                }
                if fixed.bits_per_value != 64 && fixed.bits_per_value != 128 {
                    return None;
                }
                if fixed.bits_per_value % 8 != 0 {
                    return None;
                }
            }
            DataBlock::VariableWidth(var) => {
                if var.bits_per_offset != 32 && var.bits_per_offset != 64 {
                    return None;
                }
            }
            _ => return None,
        }

        // Don't dictionary encode tiny arrays.
        let too_small = env::var("LANCE_ENCODING_DICT_TOO_SMALL")
            .ok()
            .and_then(|val| val.parse().ok())
            .unwrap_or(100);
        if data_block.num_values() < too_small {
            return None;
        }

        let num_values = data_block.num_values();

        // Apply divisor threshold and cap. This is intentionally conservative: the goal is to
        // avoid spending too much CPU trying to estimate very high cardinalities.
        let divisor: u64 = field
            .metadata
            .get(DICT_DIVISOR_META_KEY)
            .and_then(|val| val.parse().ok())
            .or_else(|| {
                env::var("LANCE_ENCODING_DICT_DIVISOR")
                    .ok()
                    .and_then(|val| val.parse().ok())
            })
            .unwrap_or(DEFAULT_DICT_DIVISOR);

        let max_cardinality: u64 = env::var("LANCE_ENCODING_DICT_MAX_CARDINALITY")
            .ok()
            .and_then(|val| val.parse().ok())
            .unwrap_or(DEFAULT_DICT_MAX_CARDINALITY);

        let threshold_cardinality = num_values
            .checked_div(divisor.max(1))
            .unwrap_or(0)
            .min(max_cardinality);
        if threshold_cardinality == 0 {
            return None;
        }

        // Get size ratio from metadata or env var.
        let threshold_ratio = field
            .metadata
            .get(DICT_SIZE_RATIO_META_KEY)
            .and_then(|val| val.parse::<f64>().ok())
            .or_else(|| {
                env::var("LANCE_ENCODING_DICT_SIZE_RATIO")
                    .ok()
                    .and_then(|val| val.parse().ok())
            })
            .unwrap_or(DEFAULT_DICT_SIZE_RATIO);

        if threshold_ratio <= 0.0 || threshold_ratio > 1.0 {
            panic!(
                "Invalid parameter: dict-size-ratio is {} which is not in the range (0, 1].",
                threshold_ratio
            );
        }

        let data_size = data_block.data_size();
        if data_size == 0 {
            return None;
        }

        let max_encoded_size = (data_size as f64 * threshold_ratio) as u64;
        let max_encoded_size = usize::try_from(max_encoded_size).ok()?;

        // Avoid probing dictionary encoding on data that appears to be near-unique
        // or likely to exceed the dictionary budget.
        if let Some(sample_unique_ratio) =
            Self::sample_unique_ratio(data_block, DEFAULT_SAMPLE_SIZE)?
        {
            if sample_unique_ratio >= DEFAULT_SAMPLE_UNIQUE_RATIO {
                return None;
            }

            let projected_cardinality = (sample_unique_ratio * num_values as f64).ceil() as u64;
            if projected_cardinality > threshold_cardinality {
                return None;
            }
        }

        let max_dict_entries = u32::try_from(threshold_cardinality.min(i32::MAX as u64)).ok()?;
        Some(DictEncodingBudget {
            max_dict_entries,
            max_encoded_size,
        })
    }

    /// Samples whether a page looks near-unique before attempting dictionary encoding.
    ///
    /// The probe uses deterministic block sampling (not RNG sampling), which keeps
    /// the check cheap and reproducible across runs. The result is only a gate for
    /// whether we try dictionary encoding, not a cardinality statistic.
    /// Returns `Some(None)` when there are too few reliable samples or the block type does not
    /// support dictionary encoding. Returns `None` for malformed data.
    fn sample_unique_ratio(data_block: &DataBlock, max_samples: usize) -> Option<Option<f64>> {
        use std::collections::HashSet;

        const NUM_SAMPLE_BLOCKS: usize = 32;
        const MIN_RELIABLE_SAMPLES: usize = 1024;

        let num_values = usize::try_from(data_block.num_values()).ok()?;
        if num_values == 0 {
            return Some(None);
        }

        let sample_count = num_values.min(max_samples).max(1);
        if sample_count < MIN_RELIABLE_SAMPLES {
            return Some(None);
        }

        let block_count = NUM_SAMPLE_BLOCKS.min(sample_count).min(num_values).max(1);
        let samples_per_block = (sample_count / block_count).max(1);
        let mut indices = Vec::with_capacity(sample_count);
        for block_idx in 0..block_count {
            let block_start = block_idx * num_values / block_count;
            let next_block_start = ((block_idx + 1) * num_values / block_count).min(num_values);
            let block_len = next_block_start.saturating_sub(block_start);
            let samples_in_block = samples_per_block.min(block_len);
            indices.extend((0..samples_in_block).map(|offset| block_start + offset));
        }

        if indices.len() < MIN_RELIABLE_SAMPLES {
            return Some(None);
        }

        let ratio = match data_block {
            DataBlock::FixedWidth(fixed) => match fixed.bits_per_value {
                64 => {
                    let values = fixed.data.borrow_to_typed_slice::<u64>();
                    let values = values.as_ref();
                    let mut unique: HashSet<u64> =
                        HashSet::with_capacity(indices.len().min(MIN_RELIABLE_SAMPLES));
                    for idx in indices.iter().copied() {
                        unique.insert(values.get(idx).copied()?);
                    }
                    unique.len() as f64 / indices.len() as f64
                }
                128 => {
                    let values = fixed.data.borrow_to_typed_slice::<u128>();
                    let values = values.as_ref();
                    let mut unique: HashSet<u128> =
                        HashSet::with_capacity(indices.len().min(MIN_RELIABLE_SAMPLES));
                    for idx in indices.iter().copied() {
                        unique.insert(values.get(idx).copied()?);
                    }
                    unique.len() as f64 / indices.len() as f64
                }
                _ => return Some(None),
            },
            DataBlock::VariableWidth(var) => {
                use xxhash_rust::xxh3::xxh3_64;

                // Hash variable-width slices instead of storing borrowed slice keys.
                let mut unique: HashSet<u64> =
                    HashSet::with_capacity(indices.len().min(MIN_RELIABLE_SAMPLES));
                match var.bits_per_offset {
                    32 => {
                        let offsets_ref = var.offsets.borrow_to_typed_slice::<u32>();
                        let offsets: &[u32] = offsets_ref.as_ref();
                        for i in indices.iter().copied() {
                            let start = usize::try_from(*offsets.get(i)?).ok()?;
                            let end = usize::try_from(*offsets.get(i + 1)?).ok()?;
                            if start > end || end > var.data.len() {
                                return None;
                            }
                            unique.insert(xxh3_64(&var.data[start..end]));
                        }
                    }
                    64 => {
                        let offsets_ref = var.offsets.borrow_to_typed_slice::<u64>();
                        let offsets: &[u64] = offsets_ref.as_ref();
                        for i in indices.iter().copied() {
                            let start = usize::try_from(*offsets.get(i)?).ok()?;
                            let end = usize::try_from(*offsets.get(i + 1)?).ok()?;
                            if start > end || end > var.data.len() {
                                return None;
                            }
                            unique.insert(xxh3_64(&var.data[start..end]));
                        }
                    }
                    _ => return Some(None),
                }
                unique.len() as f64 / indices.len() as f64
            }
            _ => return Some(None),
        };

        Some(Some(ratio))
    }

    fn slice_repdef(repdef: &SerializedRepDefs, range: Range<usize>) -> SerializedRepDefs {
        let repetition_levels = repdef
            .repetition_levels
            .as_ref()
            .map(|levels| levels[range.clone()].to_vec());
        let definition_levels = repdef
            .definition_levels
            .as_ref()
            .map(|levels| levels[range].to_vec());
        SerializedRepDefs::new_with_fixed_size_list_levels(
            repetition_levels,
            definition_levels,
            repdef.def_meaning.clone(),
            repdef.has_fixed_size_list_levels(),
        )
    }

    fn slice_arrays(
        arrays: &[ArrayRef],
        value_start: u64,
        num_values: u64,
    ) -> Result<Vec<ArrayRef>> {
        if num_values == 0 {
            return Ok(Vec::new());
        }

        let mut values_to_skip = usize::try_from(value_start).map_err(|_| {
            Error::invalid_input(format!("Value start {} is too large", value_start))
        })?;
        let mut values_remaining = usize::try_from(num_values).map_err(|_| {
            Error::invalid_input(format!("Value count {} is too large", num_values))
        })?;
        let mut sliced = Vec::new();

        for array in arrays {
            if values_to_skip >= array.len() {
                values_to_skip -= array.len();
                continue;
            }

            let offset = values_to_skip;
            let len = (array.len() - offset).min(values_remaining);
            sliced.push(array.slice(offset, len));
            values_remaining -= len;
            values_to_skip = 0;

            if values_remaining == 0 {
                break;
            }
        }

        if values_remaining != 0 {
            return Err(Error::internal(format!(
                "Page split requested {} values starting at {}, but the page did not contain enough values",
                num_values, value_start
            )));
        }

        Ok(sliced)
    }

    fn split_pages_for_miniblock_repdef_budget(
        arrays: Vec<ArrayRef>,
        repdef: SerializedRepDefs,
        budget: MiniBlockRepDefBudget,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<PrimitivePageData>> {
        if budget == MiniBlockRepDefBudget::WithinBudget {
            return Ok(vec![PrimitivePageData {
                arrays,
                structure: PrimitivePageStructure::Dense {
                    repdef,
                    single_row_miniblock_repdef_levels: None,
                },
                row_number,
                num_rows,
            }]);
        }
        if let MiniBlockRepDefBudget::SingleRowOverBudget(num_levels) = budget {
            return Ok(vec![PrimitivePageData {
                arrays,
                structure: PrimitivePageStructure::Dense {
                    repdef,
                    single_row_miniblock_repdef_levels: Some(num_levels),
                },
                row_number,
                num_rows,
            }]);
        }

        let MiniBlockRepDefBudget::RequiresPageSplit(splits) = budget else {
            unreachable!();
        };

        let mut pages = Vec::with_capacity(splits.len());
        for split in splits {
            let arrays = Self::slice_arrays(&arrays, split.value_start, split.num_values)?;
            let repdef = Self::slice_repdef(&repdef, split.level_range);
            pages.push(PrimitivePageData {
                arrays,
                structure: PrimitivePageStructure::Dense {
                    repdef,
                    single_row_miniblock_repdef_levels: None,
                },
                row_number: row_number + split.row_start,
                num_rows: split.num_rows,
            });
        }
        Ok(pages)
    }

    fn encode_dense_page(
        ctx: PrimitiveEncodeContext,
        page: PrimitivePageData,
        compression_strategy: Arc<dyn CompressionStrategy>,
        miniblock_chunk_size: MiniblockChunkSize,
        complex_null_encoding: ComplexNullEncoding,
        fixed_width_dictionary_encoding: FixedWidthDictionaryEncoding,
    ) -> Result<EncodedPage> {
        let PrimitiveEncodeContext {
            column_idx,
            field,
            encoding_metadata,
            is_simple_validity,
            has_repdef_info,
        } = ctx;
        let PrimitivePageData {
            arrays,
            structure,
            row_number,
            num_rows,
        } = page;
        let num_values = arrays.iter().map(|arr| arr.len() as u64).sum();

        let (repdef, single_row_miniblock_repdef_levels) = match structure {
            PrimitivePageStructure::Dense {
                repdef,
                single_row_miniblock_repdef_levels,
            } => (repdef, single_row_miniblock_repdef_levels),
            PrimitivePageStructure::Sparse { .. } => {
                unreachable!("dense atom received sparse page")
            }
        };

        if num_values == 0 {
            // This page contains only structural events, such as empty/null list rows.
            // The existing complex-null layout stores the rep/def stream without value buffers.
            log::debug!(
                "Encoding column {} with {} items ({} rows) using complex-null layout",
                column_idx,
                num_values,
                num_rows
            );
            return Self::encode_complex_all_null(
                column_idx,
                repdef,
                row_number,
                num_rows,
                complex_null_encoding,
                compression_strategy.as_ref(),
            );
        }

        let leaf_validity = Self::leaf_validity(&repdef, num_values as usize)?;
        let all_null = leaf_validity
            .as_ref()
            .map(|validity| validity.count_set_bits() == 0)
            .unwrap_or(false);

        if all_null {
            return if is_simple_validity {
                log::debug!(
                    "Encoding column {} with {} items ({} rows) using simple-null layout",
                    column_idx,
                    num_values,
                    num_rows
                );
                Self::encode_simple_all_null(column_idx, num_values, row_number)
            } else {
                log::debug!(
                    "Encoding column {} with {} items ({} rows) using complex-null layout",
                    column_idx,
                    num_values,
                    num_rows
                );
                Self::encode_complex_all_null(
                    column_idx,
                    repdef,
                    row_number,
                    num_rows,
                    complex_null_encoding,
                    compression_strategy.as_ref(),
                )
            };
        }

        if let DataType::Struct(fields) = &field.data_type()
            && fields.is_empty()
        {
            if has_repdef_info {
                return Err(Error::invalid_input_source(format!("Empty structs with rep/def information are not yet supported.  The field {} is an empty struct that either has nulls or is in a list.", field.name).into()));
            }
            // This is maybe a little confusing but the reader should never look at this anyways and it
            // seems like overkill to invent a new layout just for "empty structs".
            return Self::encode_simple_all_null(column_idx, num_values, row_number);
        }

        let data_block = DataBlock::from_arrays(&arrays, num_values);

        if let Some(num_levels) = single_row_miniblock_repdef_levels {
            let requested_encoding = encoding_metadata
                .get(STRUCTURAL_ENCODING_META_KEY)
                .map(|requested| requested.to_lowercase());
            let fullzip_error = match &data_block {
                // 1-bit booleans are widened to bytes inside `encode_full_zip`.
                DataBlock::FixedWidth(fixed)
                    if fixed.bits_per_value != 1 && !fixed.bits_per_value.is_multiple_of(8) =>
                {
                    Some(format!(
                        "Full-zip fixed-width values must be byte aligned, got {} bits per value",
                        fixed.bits_per_value
                    ))
                }
                DataBlock::VariableWidth(variable)
                    if !variable.bits_per_offset.is_multiple_of(8) =>
                {
                    Some(format!(
                        "Full-zip variable-width offsets must be byte aligned, got {} bits per offset",
                        variable.bits_per_offset
                    ))
                }
                DataBlock::VariableWidth(variable)
                    if variable.bits_per_offset != 32 && variable.bits_per_offset != 64 =>
                {
                    Some(format!(
                        "Full-zip variable-width offsets must be 32 or 64 bits, got {} bits",
                        variable.bits_per_offset
                    ))
                }
                DataBlock::Dictionary(_) => {
                    Some("Full-zip does not encode dictionary data blocks directly".to_string())
                }
                DataBlock::FixedSizeList(fsl) => match fsl.clone().try_into_flat() {
                    Some(flat) if flat.bits_per_value.is_multiple_of(8) => None,
                    Some(flat) => Some(format!(
                        "Full-zip fixed-size-list values must be byte aligned after flattening, got {} bits per value",
                        flat.bits_per_value
                    )),
                    None => Some(
                        "Full-zip fixed-size-list capability requires a flat fixed-width child"
                            .to_string(),
                    ),
                },
                DataBlock::FixedWidth(_) | DataBlock::VariableWidth(_) | DataBlock::Struct(_) => {
                    None
                }
                other => Some(format!(
                    "Full-zip does not support value block type {}",
                    other.name()
                )),
            };
            match requested_encoding.as_deref() {
                Some(STRUCTURAL_ENCODING_FULLZIP) => {
                    if let Some(reason) = fullzip_error {
                        return Err(Error::invalid_input_source(reason.into()));
                    }
                    return Self::encode_full_zip(
                        column_idx,
                        &field,
                        compression_strategy.as_ref(),
                        data_block,
                        repdef,
                        row_number,
                        num_rows,
                    );
                }
                Some(STRUCTURAL_ENCODING_MINIBLOCK) | None => {
                    if requested_encoding.is_none() && fullzip_error.is_none() {
                        log::debug!(
                            "Encoding column {} with {} items using full-zip layout because mini-block cannot split the structural page",
                            column_idx,
                            num_values
                        );
                        return Self::encode_full_zip(
                            column_idx,
                            &field,
                            compression_strategy.as_ref(),
                            data_block,
                            repdef,
                            row_number,
                            num_rows,
                        );
                    }
                    return Err(Error::invalid_input_source(
                        format!(
                            "Mini-block cannot encode {} rep/def levels in one top-level row. \
                             This usually means the row contains too much nested structure \
                             for the current layout.",
                            num_levels
                        )
                        .into(),
                    ));
                }
                _ => {}
            }
        }

        let requires_full_zip_packed_struct =
            if let DataBlock::Struct(ref struct_data_block) = data_block {
                struct_data_block.has_variable_width_child()
            } else {
                false
            };

        if requires_full_zip_packed_struct {
            log::debug!(
                "Encoding column {} with {} items using full-zip packed struct layout",
                column_idx,
                num_values
            );
            return Self::encode_full_zip(
                column_idx,
                &field,
                compression_strategy.as_ref(),
                data_block,
                repdef,
                row_number,
                num_rows,
            );
        }

        if let DataBlock::Dictionary(dict) = data_block {
            log::debug!(
                "Encoding column {} with {} items using dictionary encoding (already dictionary encoded)",
                column_idx,
                num_values
            );
            let (mut indices_data_block, dictionary_data_block) = dict.into_parts();
            // TODO: https://github.com/lancedb/lance/issues/4809
            // If we compute stats on dictionary_data_block => panic.
            // If we don't compute stats on indices_data_block => panic.
            // This is messy.  Don't make me call compute_stat ever.
            indices_data_block.compute_stat();
            return Self::encode_miniblock(
                column_idx,
                &field,
                compression_strategy.as_ref(),
                indices_data_block,
                repdef,
                row_number,
                Some(dictionary_data_block),
                num_rows,
                miniblock_chunk_size,
            );
        }

        // Try dictionary encoding first if applicable. If encoding aborts, fall back to the
        // preferred structural encoding.
        let dict_result = Self::should_dictionary_encode(
            &data_block,
            &field,
            fixed_width_dictionary_encoding,
        )
        .and_then(|budget| {
            log::debug!(
                "Encoding column {} with {} items using dictionary encoding (mini-block layout)",
                column_idx,
                num_values
            );
            dict::dictionary_encode(
                &data_block,
                budget.max_dict_entries,
                budget.max_encoded_size,
            )
        });

        if let Some((indices_data_block, dictionary_data_block)) = dict_result {
            Self::encode_miniblock(
                column_idx,
                &field,
                compression_strategy.as_ref(),
                indices_data_block,
                repdef,
                row_number,
                Some(dictionary_data_block),
                num_rows,
                miniblock_chunk_size,
            )
        } else if Self::prefers_miniblock(&data_block, encoding_metadata.as_ref()) {
            log::debug!(
                "Encoding column {} with {} items using mini-block layout",
                column_idx,
                num_values
            );
            Self::encode_miniblock(
                column_idx,
                &field,
                compression_strategy.as_ref(),
                data_block,
                repdef,
                row_number,
                None,
                num_rows,
                miniblock_chunk_size,
            )
        } else if Self::prefers_fullzip(encoding_metadata.as_ref()) {
            log::debug!(
                "Encoding column {} with {} items using full-zip layout",
                column_idx,
                num_values
            );
            Self::encode_full_zip(
                column_idx,
                &field,
                compression_strategy.as_ref(),
                data_block,
                repdef,
                row_number,
                num_rows,
            )
        } else {
            Err(Error::invalid_input_source(format!("Cannot determine structural encoding for field {}.  This typically indicates an invalid value of the field metadata key {}", field.name, STRUCTURAL_ENCODING_META_KEY).into()))
        }
    }

    /// Rebuilds dictionary chunks whose values array is empty.
    ///
    /// Such chunks are entirely null and retain their key validity until rep/def has been
    /// recorded.  At flush time their keys are zeroed and attached to a neighboring non-empty
    /// dictionary.  If every chunk is empty, one non-null placeholder value makes key zero valid.
    /// Rep/def retains the logical nullness, so these replacement keys are never exposed.
    fn rebuild_empty_dictionary_chunks(arrays: Vec<ArrayRef>) -> Result<Vec<ArrayRef>> {
        if !arrays.iter().any(|array| {
            array
                .as_any_dictionary_opt()
                .is_some_and(|dictionary| dictionary.values().is_empty())
        }) {
            return Ok(arrays);
        }

        let zeroed = |data_type: &DataType, len: usize| {
            new_null_array(data_type, len)
                .to_data()
                .into_builder()
                .nulls(None)
                .build()
                .map(make_array)
        };
        let rebuild =
            |keys: Vec<ArrayRef>, data_type: &DataType, values: ArrayRef| -> Result<ArrayRef> {
                let keys = keys.iter().map(|keys| keys.as_ref()).collect::<Vec<_>>();
                let keys = arrow_select::concat::concat(&keys)?;
                let data = keys
                    .to_data()
                    .into_builder()
                    .data_type(data_type.clone())
                    .child_data(vec![values.to_data()])
                    .build()?;
                Ok(make_array(data))
            };

        let mut rebuilt = Vec::with_capacity(arrays.len());
        let mut pending_keys = Vec::with_capacity(arrays.len());
        let mut empty_data_type = None;
        for array in arrays {
            let Some(dictionary) = array.as_any_dictionary_opt() else {
                return Err(Error::invalid_input_source(
                    "Cannot mix dictionary and non-dictionary chunks".into(),
                ));
            };
            if dictionary.values().is_empty() {
                pending_keys.push(zeroed(dictionary.keys().data_type(), array.len())?);
                empty_data_type.get_or_insert_with(|| array.data_type().clone());
            } else if pending_keys.is_empty() {
                rebuilt.push(array);
            } else {
                pending_keys.push(make_array(dictionary.keys().to_data()));
                rebuilt.push(rebuild(
                    std::mem::take(&mut pending_keys),
                    array.data_type(),
                    dictionary.values().clone(),
                )?);
            }
        }

        if pending_keys.is_empty() {
            return Ok(rebuilt);
        }
        if let Some(array) = rebuilt.pop() {
            let Some(dictionary) = array.as_any_dictionary_opt() else {
                return Err(Error::invalid_input_source(
                    "Cannot mix dictionary and non-dictionary chunks".into(),
                ));
            };
            let mut keys = Vec::with_capacity(pending_keys.len() + 1);
            keys.push(make_array(dictionary.keys().to_data()));
            keys.append(&mut pending_keys);
            rebuilt.push(rebuild(
                keys,
                array.data_type(),
                dictionary.values().clone(),
            )?);
        } else {
            let data_type = empty_data_type.ok_or_else(|| {
                Error::internal("Missing data type for an empty dictionary chunk")
            })?;
            let DataType::Dictionary(_, value_type) = &data_type else {
                return Err(Error::internal(format!(
                    "Expected dictionary data type, got {data_type}"
                )));
            };
            rebuilt.push(rebuild(pending_keys, &data_type, zeroed(value_type, 1)?)?);
        }
        Ok(rebuilt)
    }

    // Creates encode tasks, consuming all buffered data
    fn do_flush(
        &mut self,
        arrays: Vec<ArrayRef>,
        repdefs: Vec<RepDefBuilder>,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        let arrays = Self::rebuild_empty_dictionary_chunks(arrays)?;
        DataBlock::validate_arrays(&arrays, &self.field.name)?;
        let num_values = arrays.iter().map(|arr| arr.len() as u64).sum();
        let is_simple_validity = repdefs.iter().all(|rd| rd.is_simple_validity());
        let has_repdef_info = repdefs.iter().any(|rd| !rd.is_empty());
        let normalized = RepDefBuilder::normalize(repdefs);
        let plan_ctx = PrimitivePlanContext {
            column_idx: self.column_index,
            field: &self.field,
            encoding_metadata: &self.encoding_metadata,
        };
        let mut pages = None;
        for page_encoding in self.page_encodings.iter() {
            if let Some(planned) = page_encoding.behavior.try_plan_pages(
                &plan_ctx,
                &arrays,
                &normalized,
                row_number,
                num_rows,
                num_values,
            )? {
                pages = Some(planned);
                break;
            }
        }
        let pages = pages.ok_or_else(|| {
            Error::invalid_input_source(
                format!(
                    "No primitive page planner supports field '{}'",
                    self.field.name
                )
                .into(),
            )
        })?;

        let mut tasks = Vec::with_capacity(pages.len());
        let ctx = PrimitiveEncodeContext {
            column_idx: self.column_index,
            field: self.field.clone(),
            encoding_metadata: self.encoding_metadata.clone(),
            is_simple_validity,
            has_repdef_info,
        };
        for page in pages {
            let ctx = ctx.clone();
            let page_encodings = self.page_encodings.clone();
            let task =
                spawn_cpu(move || Self::encode_page(page_encodings.as_ref(), &ctx, page)).boxed();
            tasks.push(task);
        }
        Ok(tasks)
    }

    fn extract_validity_buf(
        array: Arc<dyn Array>,
        repdef: &mut RepDefBuilder,
        keep_original_array: bool,
    ) -> Result<Arc<dyn Array>> {
        if let Some(validity) = array.nulls() {
            if keep_original_array {
                repdef.add_validity_bitmap(validity.clone());
            } else {
                repdef.add_validity_bitmap(deep_copy_nulls(Some(validity)).unwrap());
            }
            // Empty dictionaries have no valid key payload.  Keep the validity until rep/def is
            // grouped with the buffered page; `do_flush` then rebuilds the keys against either a
            // neighboring values array or an all-empty-page placeholder.
            if let Some(dictionary) = array.as_any_dictionary_opt()
                && dictionary.values().is_empty()
            {
                return Ok(array);
            }
            let data_no_nulls = array.to_data().into_builder().nulls(None).build()?;
            Ok(make_array(data_no_nulls))
        } else {
            repdef.add_no_null(array.len());
            Ok(array)
        }
    }

    fn extract_validity(
        mut array: Arc<dyn Array>,
        repdef: &mut RepDefBuilder,
        keep_original_array: bool,
    ) -> Result<Arc<dyn Array>> {
        match array.data_type() {
            DataType::Null => {
                repdef.add_validity_bitmap(NullBuffer::new(BooleanBuffer::new_unset(array.len())));
                Ok(array)
            }
            DataType::Dictionary(_, _) => {
                array = dict::normalize_dict_nulls(array)?;
                array = dict::clear_out_of_range_null_keys(array)?;
                Self::extract_validity_buf(array, repdef, keep_original_array)
            }
            // Extract our validity buf but NOT any child validity bufs. (they will be encoded in
            // as part of the values).  Note: for FSL we do not use repdef.add_fsl because we do
            // NOT want to increase the repdef depth.
            //
            // This would be quite catasrophic for something like vector embeddings.  Imagine we
            // had thousands of vectors and some were null but no vector contained null items.  If
            // we treated the vectors (primitive FSL) like we treat structural FSL we would end up
            // with a rep/def value for every single item in the vector.
            _ => Self::extract_validity_buf(array, repdef, keep_original_array),
        }
    }
}

impl PrimitivePageEncodingBehavior for RejectSparsePrimitiveEncoding {
    fn validate_field(&self, field: &Field, metadata: &HashMap<String, String>) -> Result<()> {
        if metadata
            .get(STRUCTURAL_ENCODING_META_KEY)
            .is_some_and(|requested| requested.eq_ignore_ascii_case(STRUCTURAL_ENCODING_SPARSE))
        {
            return Err(Error::invalid_input_source(
                format!(
                    "Field '{}' requests sparse structural encoding, which is not enabled by the selected file format",
                    field.name
                )
                .into(),
            ));
        }
        Ok(())
    }
}

fn plan_dense_primitive_pages(
    arrays: &[ArrayRef],
    normalized: &NormalizedStructuralPlan,
    row_number: u64,
    num_rows: u64,
    num_values: u64,
) -> Result<Vec<PrimitivePageData>> {
    let (repdef, miniblock_repdef_budget) = normalized.serialize_with_miniblock_repdef_budget(
        miniblock::max_repdef_levels_per_chunk,
        num_rows,
        num_values,
    )?;
    PrimitiveStructuralEncoder::split_pages_for_miniblock_repdef_budget(
        arrays.to_vec(),
        repdef,
        miniblock_repdef_budget,
        row_number,
        num_rows,
    )
}

impl PrimitivePageEncodingBehavior for DenseU16PrimitiveEncoding {
    fn try_plan_pages(
        &self,
        _ctx: &PrimitivePlanContext<'_>,
        arrays: &[ArrayRef],
        normalized: &NormalizedStructuralPlan,
        row_number: u64,
        num_rows: u64,
        num_values: u64,
    ) -> Result<Option<Vec<PrimitivePageData>>> {
        Ok(Some(plan_dense_primitive_pages(
            arrays, normalized, row_number, num_rows, num_values,
        )?))
    }

    fn try_encode_page(
        &self,
        ctx: &PrimitiveEncodeContext,
        page: PrimitivePageData,
    ) -> Result<PrimitiveEncodeAttempt> {
        if !matches!(&page.structure, PrimitivePageStructure::Dense { .. }) {
            return Ok(PrimitiveEncodeAttempt::Unhandled(page));
        }
        Ok(PrimitiveEncodeAttempt::Encoded(
            PrimitiveStructuralEncoder::encode_dense_page(
                ctx.clone(),
                page,
                self.compression.clone(),
                MiniblockChunkSize::U16,
                ComplexNullEncoding::RawLevels,
                FixedWidthDictionaryEncoding::Exclude64Bit,
            )?,
        ))
    }
}

impl PrimitivePageEncodingBehavior for DenseU32PrimitiveEncoding {
    fn try_plan_pages(
        &self,
        _ctx: &PrimitivePlanContext<'_>,
        arrays: &[ArrayRef],
        normalized: &NormalizedStructuralPlan,
        row_number: u64,
        num_rows: u64,
        num_values: u64,
    ) -> Result<Option<Vec<PrimitivePageData>>> {
        Ok(Some(plan_dense_primitive_pages(
            arrays, normalized, row_number, num_rows, num_values,
        )?))
    }

    fn try_encode_page(
        &self,
        ctx: &PrimitiveEncodeContext,
        page: PrimitivePageData,
    ) -> Result<PrimitiveEncodeAttempt> {
        if !matches!(&page.structure, PrimitivePageStructure::Dense { .. }) {
            return Ok(PrimitiveEncodeAttempt::Unhandled(page));
        }
        Ok(PrimitiveEncodeAttempt::Encoded(
            PrimitiveStructuralEncoder::encode_dense_page(
                ctx.clone(),
                page,
                self.compression.clone(),
                MiniblockChunkSize::U32,
                ComplexNullEncoding::CompressedLevels,
                FixedWidthDictionaryEncoding::Include64Bit,
            )?,
        ))
    }
}

impl PrimitivePageEncodingBehavior for SparsePrimitiveEncoding {
    fn try_plan_pages(
        &self,
        ctx: &PrimitivePlanContext<'_>,
        arrays: &[ArrayRef],
        normalized: &NormalizedStructuralPlan,
        row_number: u64,
        num_rows: u64,
        num_values: u64,
    ) -> Result<Option<Vec<PrimitivePageData>>> {
        let requested_encoding = ctx.encoding_metadata.get(STRUCTURAL_ENCODING_META_KEY);
        let requests_sparse = requested_encoding
            .is_some_and(|requested| requested.eq_ignore_ascii_case(STRUCTURAL_ENCODING_SPARSE));
        if requests_sparse {
            let plan = sparse::writer::plan(normalized, num_values)?;
            if sparse::writer::uses_constant_layout(&plan, ctx.field) {
                return Ok(None);
            }
            return Ok(Some(vec![PrimitivePageData {
                arrays: arrays.to_vec(),
                structure: PrimitivePageStructure::Sparse {
                    plan,
                    prepared_values: None,
                },
                row_number,
                num_rows,
            }]));
        }

        let (_, miniblock_repdef_budget) = normalized.serialize_with_miniblock_repdef_budget(
            miniblock::max_repdef_levels_per_chunk,
            num_rows,
            num_values,
        )?;
        let automatic_sparse = layout::select_automatic_sparse(
            requested_encoding.map(String::as_str),
            &miniblock_repdef_budget,
            || {
                let data = DataBlock::from_arrays(arrays, num_values);
                if !sparse::writer::supports_value_block(&data) {
                    return Ok(None);
                }
                let prepared_values = match sparse::writer::prepare_values(
                    ctx.field,
                    self.compression.as_ref(),
                    data,
                    MiniblockChunkSize::U32,
                ) {
                    Ok(prepared_values) => prepared_values,
                    Err(error) => {
                        debug!(
                            "Keeping column {} on its dense structural path because sparse value preparation is unavailable: {}",
                            ctx.column_idx, error
                        );
                        return Ok(None);
                    }
                };
                let plan = sparse::writer::plan(normalized, num_values)?;
                if sparse::writer::uses_constant_layout(&plan, ctx.field) {
                    return Ok(None);
                }
                Ok(Some((plan, prepared_values)))
            },
        )?;
        Ok(automatic_sparse.map(|(plan, prepared_values)| {
            vec![PrimitivePageData {
                arrays: arrays.to_vec(),
                structure: PrimitivePageStructure::Sparse {
                    plan,
                    prepared_values: Some(prepared_values),
                },
                row_number,
                num_rows,
            }]
        }))
    }

    fn try_encode_page(
        &self,
        ctx: &PrimitiveEncodeContext,
        page: PrimitivePageData,
    ) -> Result<PrimitiveEncodeAttempt> {
        if !matches!(&page.structure, PrimitivePageStructure::Sparse { .. }) {
            return Ok(PrimitiveEncodeAttempt::Unhandled(page));
        }
        let PrimitivePageData {
            arrays,
            structure:
                PrimitivePageStructure::Sparse {
                    plan,
                    prepared_values,
                },
            row_number,
            num_rows,
        } = page
        else {
            unreachable!()
        };
        let num_values = arrays.iter().map(|array| array.len() as u64).sum();
        log::debug!(
            "Encoding column {} with {} visible items ({} rows) using sparse layout",
            ctx.column_idx,
            num_values,
            num_rows
        );
        Ok(PrimitiveEncodeAttempt::Encoded(
            sparse::writer::encode_page(
                ctx.column_idx,
                &ctx.field,
                self.compression.as_ref(),
                prepared_values.map_or_else(
                    || {
                        sparse::writer::SparseValueInput::Unprepared(DataBlock::from_arrays(
                            &arrays, num_values,
                        ))
                    },
                    sparse::writer::SparseValueInput::Prepared,
                ),
                plan,
                row_number,
                num_rows,
                MiniblockChunkSize::U32,
            )?,
        ))
    }
}

impl PrimitivePageEncodingBehavior for ConstantPrimitiveEncoding {
    fn try_encode_page(
        &self,
        ctx: &PrimitiveEncodeContext,
        page: PrimitivePageData,
    ) -> Result<PrimitiveEncodeAttempt> {
        let PrimitivePageStructure::Dense { repdef, .. } = &page.structure else {
            return Ok(PrimitiveEncodeAttempt::Unhandled(page));
        };
        let num_values: u64 = page.arrays.iter().map(|array| array.len() as u64).sum();
        if num_values == 0 {
            return Ok(PrimitiveEncodeAttempt::Unhandled(page));
        }
        let leaf_validity = PrimitiveStructuralEncoder::leaf_validity(repdef, num_values as usize)?;
        if leaf_validity
            .as_ref()
            .is_some_and(|validity| validity.count_set_bits() == 0)
            || matches!(ctx.field.data_type(), DataType::Struct(fields) if fields.is_empty())
        {
            return Ok(PrimitiveEncodeAttempt::Unhandled(page));
        }
        let Some(scalar) =
            PrimitiveStructuralEncoder::find_constant_scalar(&page.arrays, leaf_validity.as_ref())?
        else {
            return Ok(PrimitiveEncodeAttempt::Unhandled(page));
        };
        let PrimitivePageData {
            structure: PrimitivePageStructure::Dense { repdef, .. },
            row_number,
            num_rows,
            ..
        } = page
        else {
            unreachable!()
        };
        log::debug!(
            "Encoding column {} with {} items ({} rows) using constant layout",
            ctx.column_idx,
            num_values,
            num_rows
        );
        Ok(PrimitiveEncodeAttempt::Encoded(
            constant::encode_constant_page(ctx.column_idx, scalar, repdef, row_number, num_rows)?,
        ))
    }
}

impl FieldEncoder for PrimitiveStructuralEncoder {
    // Buffers data, if there is enough to write a page then we create an encode task
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        _external_buffers: &mut OutOfLineBuffers,
        mut repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        let array = Self::extract_validity(array, &mut repdef, self.keep_original_array)?;
        self.accumulated_repdefs.push(repdef);

        if let Some((arrays, row_number, num_rows)) =
            self.accumulation_queue.insert(array, row_number, num_rows)
        {
            let accumulated_repdefs = std::mem::take(&mut self.accumulated_repdefs);
            Ok(self.do_flush(arrays, accumulated_repdefs, row_number, num_rows)?)
        } else {
            Ok(vec![])
        }
    }

    // If there is any data left in the buffer then create an encode task from it
    fn flush(&mut self, _external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>> {
        if let Some((arrays, row_number, num_rows)) = self.accumulation_queue.flush() {
            let accumulated_repdefs = std::mem::take(&mut self.accumulated_repdefs);
            Ok(self.do_flush(arrays, accumulated_repdefs, row_number, num_rows)?)
        } else {
            Ok(vec![])
        }
    }

    fn num_columns(&self) -> u32 {
        1
    }

    fn finish(
        &mut self,
        _external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<crate::encoder::EncodedColumn>>> {
        std::future::ready(Ok(vec![EncodedColumn::default()])).boxed()
    }
}

#[cfg(test)]
#[allow(clippy::single_range_in_vec_init)]
mod tests {
    use super::{
        CachedPage, CachedPageData, ChunkInstructions, DataBlock, DecodeMiniBlockTask,
        DecodePageTask, FixedFullZipDecodeTask, FixedPerValueDecompressor, FixedWidthDataBlock,
        FixedWidthDictionaryEncoding, FullZipCacheableState, FullZipDecodeDetails,
        FullZipDecodeTaskItem, FullZipReadSource, FullZipRepIndexDetails, FullZipScheduler,
        LazyLevels, LevelCodec, LevelCursor, LevelPlan, MiniBlockChunk, MiniBlockChunkIndex,
        MiniBlockCompressed, MiniblockChunkSize, PageCacheView, PageDataCacheKey,
        PageInfoAndScheduler, PageInitialization, PerValueDataBlock, PerValueDecompressor,
        PreambleAction, RunEndsBuilder, RunPosition, RunStorage, SimpleAllNullScheduler,
        StructuralPageScheduler, StructuralPrimitiveFieldScheduler, VariableFullZipDecoder,
        dense_levels_from_block, map_ranges_to_pages, pages_overlapping_ranges,
        validate_complex_all_null_levels,
    };
    use crate::EncodingsIo;
    use crate::buffer::LanceBuffer;
    use crate::compression::{
        BlockCompressor, DefaultDecompressionStrategy, MiniBlockDecompressor,
    };
    use crate::constants::{
        COMPRESSION_LEVEL_META_KEY, COMPRESSION_META_KEY, DICT_VALUES_COMPRESSION_LEVEL_META_KEY,
        DICT_VALUES_COMPRESSION_META_KEY, STRUCTURAL_ENCODING_META_KEY,
        STRUCTURAL_ENCODING_MINIBLOCK,
    };
    use crate::data::BlockInfo;
    use crate::decoder::{
        FilterExpression, PageEncoding, RequestBatch, SchedulerContext, StructuralFieldDecoder,
        StructuralFieldScheduler,
    };
    use crate::encodings::logical::primitive::fullzip::PerValueCompressor;
    use crate::encodings::logical::primitive::{
        ChunkDrainInstructions, LoadedChunk, PrimitiveStructuralEncoder,
        StructuralPrimitiveFieldDecoder,
    };
    use crate::encodings::physical::rle::{RleDecompressor, RleEncoder, RleRuns, RunLengthWidth};
    use crate::encodings::physical::value::{ValueDecompressor, ValueEncoder};
    use crate::format::ProtobufUtils21;
    use crate::format::pb21;
    use crate::format::pb21::compressive_encoding::Compression;
    use crate::repdef::build_control_word_iterator;
    use crate::testing::TestEncoding;
    use crate::testing::{SimulatedScheduler, TestCases, check_round_trip_encoding_of_data};
    use arrow_array::{
        Array, ArrayRef, DictionaryArray, FixedSizeListArray, Float32Array, Int8Array,
        PrimitiveArray, StringArray, UInt8Array, make_array, new_null_array, types::Int32Type,
    };
    use arrow_buffer::{BooleanBuffer, NullBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field as ArrowField};
    use std::collections::HashMap;
    use std::{collections::VecDeque, ops::Range, sync::Arc};

    #[derive(Debug)]
    struct CacheMarker(u8);

    impl lance_core::cache::DeepSizeOf for CacheMarker {
        fn deep_size_of_children(&self, _context: &mut lance_core::cache::Context) -> usize {
            0
        }
    }

    impl CachedPageData for CacheMarker {
        fn as_arc_any(self: Arc<Self>) -> Arc<dyn std::any::Any + Send + Sync + 'static> {
            self
        }
    }

    #[test]
    fn test_is_narrow() {
        let int8_array = Int8Array::from(vec![1, 2, 3]);
        let array_ref: ArrayRef = Arc::new(int8_array);
        let block = DataBlock::from_array(array_ref);

        assert!(PrimitiveStructuralEncoder::is_narrow(&block));

        let string_array = StringArray::from(vec![Some("hello"), Some("world")]);
        let block = DataBlock::from_array(string_array);
        assert!(PrimitiveStructuralEncoder::is_narrow(&block));

        let string_array = StringArray::from(vec![
            Some("hello world".repeat(100)),
            Some("world".to_string()),
        ]);
        let block = DataBlock::from_array(string_array);
        assert!((!PrimitiveStructuralEncoder::is_narrow(&block)));
    }

    fn valued_dictionary() -> ArrayRef {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                PrimitiveArray::<Int32Type>::from(vec![0]),
                Arc::new(StringArray::from(vec!["a"])),
            )
            .unwrap(),
        )
    }

    fn empty_dictionary() -> ArrayRef {
        new_null_array(
            &DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            1,
        )
    }

    fn null_valued_dictionary() -> ArrayRef {
        Arc::new(
            DictionaryArray::<Int32Type>::try_new(
                PrimitiveArray::<Int32Type>::from(vec![0]),
                Arc::new(StringArray::from(vec![None::<&str>])),
            )
            .unwrap(),
        )
    }

    fn sliced_empty_dictionary_with_hidden_key_payload() -> ArrayRef {
        let keys = PrimitiveArray::<Int32Type>::new(
            ScalarBuffer::from(vec![5, 7, 9]),
            Some(NullBuffer::new(BooleanBuffer::new_unset(3))),
        );
        let values = Arc::new(StringArray::from(Vec::<&str>::new())) as ArrayRef;
        let dictionary = DictionaryArray::<Int32Type>::try_new(keys, values).unwrap();
        Arc::new(dictionary.slice(1, 1))
    }

    #[rstest::rstest]
    #[case::empty_after_value(vec![valued_dictionary(), empty_dictionary()])]
    #[case::empty_before_value(vec![empty_dictionary(), valued_dictionary()])]
    #[case::null_value_after_value(vec![valued_dictionary(), null_valued_dictionary()])]
    #[case::hidden_sliced_after_value(vec![
        valued_dictionary(),
        sliced_empty_dictionary_with_hidden_key_payload(),
    ])]
    #[tokio::test]
    async fn test_mixed_valued_and_all_null_dictionary_chunks(#[case] dictionaries: Vec<ArrayRef>) {
        check_round_trip_encoding_of_data(
            dictionaries,
            &TestCases::default()
                .with_structural_encodings()
                .with_page_sizes(vec![1])
                .with_range(0..1)
                .with_range(1..2)
                .with_indices(vec![0])
                .with_indices(vec![1]),
            HashMap::new(),
        )
        .await;
    }

    #[tokio::test]
    async fn test_sliced_empty_dictionary_with_hidden_key_payload() {
        check_round_trip_encoding_of_data(
            vec![sliced_empty_dictionary_with_hidden_key_payload()],
            &TestCases::default()
                .with_structural_encodings()
                .with_page_sizes(vec![4096]),
            HashMap::new(),
        )
        .await;
    }

    #[test]
    fn test_primitive_decoder_empty_page_queue_returns_error() {
        let field = Arc::new(ArrowField::new("vector", DataType::Float32, true));
        let mut decoder = StructuralPrimitiveFieldDecoder::new(&field, false);

        let err = decoder.drain(1).unwrap_err();
        assert!(
            matches!(&err, lance_core::Error::Internal { .. }),
            "expected internal error, got: {err:?}"
        );
        let message = err.to_string();
        for expected in [
            "Primitive decoder missing page decoder",
            "field 'vector'",
            "data_type=Float32",
            "requested_rows=1",
            "remaining_rows=1",
            "rows_drained_in_current=0",
            "queued_pages=0",
        ] {
            assert!(
                message.contains(expected),
                "expected error to contain {expected:?}, got: {message}"
            );
        }
    }

    #[test]
    fn test_plan_decoded_bytes_i32() {
        use crate::decoder::CANDIDATE_BATCH_SIZES;

        let field = Arc::new(ArrowField::new("x", DataType::Int32, false));
        let decoder = StructuralPrimitiveFieldDecoder::new(&field, false);

        let rows_remaining = 100u64;
        let bytes = decoder.plan_decoded_bytes(rows_remaining).unwrap();

        for (i, &candidate) in CANDIDATE_BATCH_SIZES.iter().enumerate() {
            let rows = (candidate as u64).min(rows_remaining);
            assert_eq!(bytes[i], rows * 4, "candidate batch size {candidate}");
        }
    }

    #[test]
    fn test_plan_decoded_bytes_i32_with_nulls() {
        use arrow_array::Int32Array;

        // Use N == CANDIDATE_BATCH_SIZES[5] == 1024.  At this size both the
        // values buffer (1024*4 = 4096 bytes) and the validity bitmap
        // (ceil(1024/8) = 128 bytes) are naturally aligned to Arrow's 64-byte
        // allocation boundary, so plan_decoded_bytes matches
        // get_buffer_memory_size exactly without needing alignment arithmetic.
        const N: i32 = 1024;
        let array = Int32Array::from_iter((0..N).map(|i| if i % 2 == 0 { Some(i) } else { None }));
        let expected = array.get_buffer_memory_size() as u64;

        let field = Arc::new(ArrowField::new("x", DataType::Int32, true));
        let decoder = StructuralPrimitiveFieldDecoder::new(&field, false);

        let bytes = decoder.plan_decoded_bytes(N as u64).unwrap();
        // bytes[5] corresponds to CANDIDATE_BATCH_SIZES[5] == 1024 == N
        assert_eq!(
            bytes[5], expected,
            "plan_decoded_bytes({N}) = {} but get_buffer_memory_size = {expected}",
            bytes[5],
        );
    }

    #[test]
    fn test_plan_decoded_bytes_nullable_fsl_with_nullable_items() {
        // FixedSizeList<nullable i32, DIM> where the FSL itself is also nullable.
        // Arrow allocates three buffers:
        //   1. FSL null bitmap:       ceil(rows / 8)
        //   2. Child i32 null bitmap: ceil(rows * DIM / 8)
        //   3. Child i32 values:      rows * DIM * 4
        //
        // N=1024, DIM=4 keeps all three naturally 64-byte aligned so the
        // comparison against get_buffer_memory_size() is exact.
        use arrow_array::{FixedSizeListArray, Int32Array};
        use arrow_buffer::NullBuffer;

        const N: usize = 1024;
        const DIM: i32 = 4;

        let child_values = Int32Array::from_iter(
            (0..(N as i32 * DIM)).map(|i| if i % 2 == 0 { Some(i) } else { None }),
        );
        let item_field = Arc::new(ArrowField::new("item", DataType::Int32, true));
        let fsl_nulls = NullBuffer::from((0..N).map(|i| i % 3 != 0).collect::<Vec<_>>());
        let fsl_array = FixedSizeListArray::new(
            item_field.clone(),
            DIM,
            Arc::new(child_values),
            Some(fsl_nulls),
        );
        let expected = fsl_array.get_buffer_memory_size() as u64;

        let fsl_type = DataType::FixedSizeList(item_field, DIM);
        let field = Arc::new(ArrowField::new("v", fsl_type, true));
        let decoder = StructuralPrimitiveFieldDecoder::new(&field, false);

        let bytes = decoder.plan_decoded_bytes(N as u64).unwrap();
        // bytes[5] corresponds to CANDIDATE_BATCH_SIZES[5] == 1024 == N
        assert_eq!(
            bytes[5], expected,
            "plan_decoded_bytes({N}) = {} but get_buffer_memory_size = {expected}",
            bytes[5],
        );
    }

    #[test]
    fn test_plan_decoded_bytes_fixed_size_list_of_f32() {
        use crate::decoder::CANDIDATE_BATCH_SIZES;

        const DIMENSION: i32 = 8;
        // Items are non-nullable, which is the typical case for vector embeddings.
        let item_field = Arc::new(ArrowField::new("item", DataType::Float32, false));
        let fsl_type = DataType::FixedSizeList(item_field, DIMENSION);
        let field = Arc::new(ArrowField::new("vector", fsl_type, true));
        let decoder = StructuralPrimitiveFieldDecoder::new(&field, false);

        let rows_remaining = 100u64;
        let bytes = decoder.plan_decoded_bytes(rows_remaining).unwrap();

        // Each FSL row is DIMENSION f32 values = DIMENSION * 4 bytes, plus
        // 1 bit of validity bitmap (the field is nullable).
        let bytes_per_row = DIMENSION as u64 * 4;
        for (i, &candidate) in CANDIDATE_BATCH_SIZES.iter().enumerate() {
            let rows = (candidate as u64).min(rows_remaining);
            assert_eq!(
                bytes[i],
                rows * bytes_per_row + rows.div_ceil(8),
                "candidate batch size {candidate}"
            );
        }
    }

    #[test]
    fn test_fullzip_fixed_rejects_non_byte_aligned_values() {
        let fixed = FixedWidthDataBlock {
            data: LanceBuffer::from(vec![0_u8]),
            bits_per_value: 1,
            num_values: 8,
            block_info: BlockInfo::new(),
        };
        let repdef = build_control_word_iterator(None, 0, None, 0, u16::MAX, 8);

        let Err(err) = PrimitiveStructuralEncoder::serialize_full_zip_fixed(fixed, repdef, 8)
        else {
            panic!("expected full-zip to reject 1-bit fixed-width values");
        };
        assert!(
            err.to_string().contains("byte aligned"),
            "unexpected error: {err}"
        );
    }

    fn decode_fixed_fullzip_no_levels(
        decompressor: Arc<dyn FixedPerValueDecompressor>,
        data: Vec<FullZipDecodeTaskItem>,
        num_rows: usize,
        bytes_per_value: usize,
    ) -> DataBlock {
        Box::new(FixedFullZipDecodeTask {
            details: Arc::new(FullZipDecodeDetails {
                value_decompressor: PerValueDecompressor::Fixed(decompressor),
                def_meaning: Arc::from([]),
                ctrl_word_parser: crate::repdef::ControlWordParser::new(0, 0),
                max_rep: 0,
                max_visible_def: u16::MAX,
            }),
            data,
            num_rows,
            bytes_per_value,
        })
        .decode()
        .unwrap()
        .data
    }

    #[test]
    fn test_fixed_fullzip_decode_preallocates_exact_output_size() {
        #[derive(Debug)]
        struct IdentityFixedDecompressor;

        impl FixedPerValueDecompressor for IdentityFixedDecompressor {
            fn decompress(
                &self,
                data: FixedWidthDataBlock,
                num_rows: u64,
            ) -> crate::Result<DataBlock> {
                assert_eq!(data.num_values, num_rows);
                Ok(DataBlock::FixedWidth(data))
            }

            fn bits_per_value(&self) -> u64 {
                32
            }

            fn decoded_size_bytes(&self, num_values: u64) -> Option<u64> {
                num_values.checked_mul(4)
            }
        }

        let make_item = |num_rows: u64| FullZipDecodeTaskItem {
            data: PerValueDataBlock::Fixed(FixedWidthDataBlock {
                data: LanceBuffer::from(vec![7_u8; num_rows as usize * 4]),
                bits_per_value: 32,
                num_values: num_rows,
                block_info: BlockInfo::new(),
            }),
            rows_in_buf: num_rows,
        };

        let num_rows = 512;
        let decoded = decode_fixed_fullzip_no_levels(
            Arc::new(IdentityFixedDecompressor),
            vec![make_item(128), make_item(384)],
            num_rows,
            4,
        );
        let values = decoded.as_fixed_width_ref().unwrap();
        let expected_size = num_rows * 4;
        assert_eq!(values.data.len(), expected_size);
        assert_eq!(values.data.clone().into_buffer().capacity(), expected_size);
    }

    #[test]
    fn test_fixed_fullzip_decode_falls_back_when_output_size_is_not_exact() {
        #[derive(Debug)]
        struct FallbackFixedDecompressor;

        impl FixedPerValueDecompressor for FallbackFixedDecompressor {
            fn decompress(
                &self,
                data: FixedWidthDataBlock,
                num_rows: u64,
            ) -> crate::Result<DataBlock> {
                assert_eq!(data.num_values, num_rows);
                Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
                    data: LanceBuffer::from(vec![7_u8; num_rows as usize * 4]),
                    bits_per_value: 32,
                    num_values: num_rows,
                    block_info: BlockInfo::new(),
                }))
            }

            fn bits_per_value(&self) -> u64 {
                // This deliberately cannot be multiplied by num_rows. If FullZip treats
                // bits_per_value as an exact decoded-size estimate, decoding will fail.
                u64::MAX - 7
            }
        }

        let num_rows = 2;
        let decoded = decode_fixed_fullzip_no_levels(
            Arc::new(FallbackFixedDecompressor),
            vec![FullZipDecodeTaskItem {
                data: PerValueDataBlock::Fixed(FixedWidthDataBlock {
                    data: LanceBuffer::from(vec![0_u8; num_rows * 4]),
                    bits_per_value: 32,
                    num_values: num_rows as u64,
                    block_info: BlockInfo::new(),
                }),
                rows_in_buf: num_rows as u64,
            }],
            num_rows,
            4,
        );
        let values = decoded.as_fixed_width_ref().unwrap();
        assert_eq!(values.num_values, num_rows as u64);
        assert_eq!(values.data.len(), num_rows * 4);
    }

    #[test]
    fn test_fixed_fullzip_real_fsl_preallocates_exact_output_size() {
        let num_rows = 64;
        let dimension = 32;
        let items = Arc::new(Float32Array::from_iter_values(
            (0..num_rows * dimension).map(|value| value as f32),
        ));
        let item_field = Arc::new(ArrowField::new("item", DataType::Float32, false));
        let sample = FixedSizeListArray::new(item_field, dimension as i32, items, None);

        let (data, compression) = PerValueCompressor::compress(
            &ValueEncoder::default(),
            DataBlock::from_array(sample.clone()),
        )
        .unwrap();
        let Compression::FixedSizeList(fsl) = compression.compression.unwrap() else {
            panic!("expected fixed-size-list compression");
        };
        let decompressor = ValueDecompressor::from_fsl(fsl.as_ref()).unwrap();
        let expected_size = num_rows * dimension * size_of::<f32>();
        assert_eq!(
            FixedPerValueDecompressor::decoded_size_bytes(&decompressor, num_rows as u64),
            Some(expected_size as u64)
        );

        let decoded = decode_fixed_fullzip_no_levels(
            Arc::new(decompressor),
            vec![FullZipDecodeTaskItem {
                data,
                rows_in_buf: num_rows as u64,
            }],
            num_rows,
            dimension * size_of::<f32>(),
        );
        let fsl = decoded.as_fixed_size_list_ref().unwrap();
        let values = fsl.child.as_fixed_width_ref().unwrap();
        assert_eq!(values.data.len(), expected_size);
        assert_eq!(values.data.clone().into_buffer().capacity(), expected_size);

        let decoded_array = make_array(
            decoded
                .into_arrow(sample.data_type().clone(), true)
                .unwrap(),
        );
        assert_eq!(decoded_array.as_ref(), &sample);
    }

    #[test]
    fn test_fixed_fullzip_nullable_fsl_uses_fallback_end_to_end() {
        #[derive(Debug)]
        struct NullableFslDecompressor {
            inner: ValueDecompressor,
        }

        impl FixedPerValueDecompressor for NullableFslDecompressor {
            fn decompress(
                &self,
                data: FixedWidthDataBlock,
                num_rows: u64,
            ) -> crate::Result<DataBlock> {
                FixedPerValueDecompressor::decompress(&self.inner, data, num_rows)
            }

            fn bits_per_value(&self) -> u64 {
                // FullZip must not use this physical row width as an exact decoded-size
                // estimate for the nullable, multi-buffer Arrow output.
                u64::MAX - 7
            }

            fn decoded_size_bytes(&self, num_values: u64) -> Option<u64> {
                FixedPerValueDecompressor::decoded_size_bytes(&self.inner, num_values)
            }
        }

        let num_rows = 64;
        let items = Arc::new(UInt8Array::from_iter(
            (0..num_rows).map(|value| (value % 3 != 0).then_some(value as u8)),
        ));
        let item_field = Arc::new(ArrowField::new("item", DataType::UInt8, true));
        let sample = FixedSizeListArray::new(item_field, 1, items, None);

        let (data, compression) = PerValueCompressor::compress(
            &ValueEncoder::default(),
            DataBlock::from_array(sample.clone()),
        )
        .unwrap();
        let Compression::FixedSizeList(fsl) = compression.compression.unwrap() else {
            panic!("expected fixed-size-list compression");
        };
        let decompressor = NullableFslDecompressor {
            inner: ValueDecompressor::from_fsl(fsl.as_ref()).unwrap(),
        };
        assert_eq!(
            FixedPerValueDecompressor::decoded_size_bytes(&decompressor, num_rows as u64),
            None
        );

        let decoded = decode_fixed_fullzip_no_levels(
            Arc::new(decompressor),
            vec![FullZipDecodeTaskItem {
                data,
                rows_in_buf: num_rows as u64,
            }],
            num_rows,
            2,
        );
        let decoded_array = make_array(
            decoded
                .into_arrow(sample.data_type().clone(), true)
                .unwrap(),
        );
        assert_eq!(decoded_array.as_ref(), &sample);
    }

    #[test]
    fn test_miniblock_decode_uses_exact_fixed_width_output_size() {
        #[derive(Debug)]
        struct FixedWidthMiniBlockDecompressor;

        impl MiniBlockDecompressor for FixedWidthMiniBlockDecompressor {
            fn decompress(
                &self,
                data: Vec<LanceBuffer>,
                num_values: u64,
            ) -> crate::Result<DataBlock> {
                assert_eq!(data.len(), 1);
                Ok(DataBlock::FixedWidth(FixedWidthDataBlock {
                    data: data.into_iter().next().unwrap(),
                    bits_per_value: 32,
                    num_values,
                    block_info: BlockInfo::new(),
                }))
            }

            fn decoded_size_bytes(&self, num_values: u64) -> Option<u64> {
                num_values.checked_mul(4)
            }
        }

        let num_rows = 512;
        let expected_size = num_rows * 4;
        let mut chunk_data = Vec::new();
        chunk_data.extend_from_slice(&0_u16.to_le_bytes());
        chunk_data.extend_from_slice(&(expected_size as u16).to_le_bytes());
        let header_padding =
            lance_core::utils::bit::pad_bytes::<{ super::MINIBLOCK_ALIGNMENT }>(chunk_data.len());
        chunk_data.resize(chunk_data.len() + header_padding, 0);
        chunk_data.resize(chunk_data.len() + expected_size as usize, 7);

        let task = DecodeMiniBlockTask {
            rep_decompressor: None,
            def_decompressor: None,
            value_decompressor: Arc::new(FixedWidthMiniBlockDecompressor),
            dictionary_data: None,
            def_meaning: Arc::from([]),
            num_buffers: 1,
            max_visible_level: 0,
            instructions: vec![(
                ChunkDrainInstructions {
                    chunk_instructions: ChunkInstructions {
                        chunk_idx: 0,
                        preamble: PreambleAction::Absent,
                        rows_to_skip: 0,
                        rows_to_take: num_rows,
                        take_trailer: false,
                    },
                    rows_to_skip: 0,
                    rows_to_take: num_rows,
                    preamble_action: PreambleAction::Absent,
                },
                LoadedChunk {
                    byte_range: 0..chunk_data.len() as u64,
                    data: LanceBuffer::from(chunk_data),
                    items_in_chunk: num_rows,
                    chunk_idx: 0,
                },
            )],
            has_large_chunk: false,
        };

        let decoded = Box::new(task).decode().unwrap();
        let values = decoded.data.as_fixed_width_ref().unwrap();
        assert_eq!(values.data.len(), expected_size as usize);
        assert_eq!(
            values.data.clone().into_buffer().capacity(),
            expected_size as usize
        );
    }

    #[test]
    fn test_map_range() {
        // Null in the middle
        // [[A, B, C], [D, E], NULL, [F, G, H]]
        let rep = Some(vec![1, 0, 0, 1, 0, 1, 1, 0, 0]);
        let def = Some(vec![0, 0, 0, 0, 0, 1, 0, 0, 0]);
        let max_visible_def = 0;
        let total_items = 8;
        let max_rep = 1;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Absent,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(0..1, 0..3, 0..3);
        check(1..2, 3..5, 3..5);
        check(2..3, 5..5, 5..6);
        check(3..4, 5..8, 6..9);
        check(0..2, 0..5, 0..5);
        check(1..3, 3..5, 3..6);
        check(2..4, 5..8, 5..9);
        check(0..3, 0..5, 0..6);
        check(1..4, 3..8, 3..9);
        check(0..4, 0..8, 0..9);

        // Null at start
        // [NULL, [A, B], [C]]
        let rep = Some(vec![1, 1, 0, 1]);
        let def = Some(vec![1, 0, 0, 0]);
        let max_visible_def = 0;
        let total_items = 3;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Absent,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(0..1, 0..0, 0..1);
        check(1..2, 0..2, 1..3);
        check(2..3, 2..3, 3..4);
        check(0..2, 0..2, 0..3);
        check(1..3, 0..3, 1..4);
        check(0..3, 0..3, 0..4);

        // Null at end
        // [[A], [B, C], NULL]
        let rep = Some(vec![1, 1, 0, 1]);
        let def = Some(vec![0, 0, 0, 1]);
        let max_visible_def = 0;
        let total_items = 3;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Absent,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(0..1, 0..1, 0..1);
        check(1..2, 1..3, 1..3);
        check(2..3, 3..3, 3..4);
        check(0..2, 0..3, 0..3);
        check(1..3, 1..3, 1..4);
        check(0..3, 0..3, 0..4);

        // No nulls, with repetition
        // [[A, B], [C, D], [E, F]]
        let rep = Some(vec![1, 0, 1, 0, 1, 0]);
        let def: Option<&[u16]> = None;
        let max_visible_def = 0;
        let total_items = 6;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Absent,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(0..1, 0..2, 0..2);
        check(1..2, 2..4, 2..4);
        check(2..3, 4..6, 4..6);
        check(0..2, 0..4, 0..4);
        check(1..3, 2..6, 2..6);
        check(0..3, 0..6, 0..6);

        // No repetition, with nulls (this case is trivial)
        // [A, B, NULL, C]
        let rep: Option<&[u16]> = None;
        let def = Some(vec![0, 0, 1, 0]);
        let max_visible_def = 1;
        let total_items = 4;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Absent,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(0..1, 0..1, 0..1);
        check(1..2, 1..2, 1..2);
        check(2..3, 2..3, 2..3);
        check(0..2, 0..2, 0..2);
        check(1..3, 1..3, 1..3);
        check(0..3, 0..3, 0..3);

        // Tricky case, this chunk is a continuation and starts with a rep-index = 0
        // [[..., A] [B, C], NULL]
        //
        // What we do will depend on the preamble action
        let rep = Some(vec![0, 1, 0, 1]);
        let def = Some(vec![0, 0, 0, 1]);
        let max_visible_def = 0;
        let total_items = 3;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Take,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        // If we are taking the preamble then the range must start at 0
        check(0..1, 0..3, 0..3);
        check(0..2, 0..3, 0..4);

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Skip,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(0..1, 1..3, 1..3);
        check(1..2, 3..3, 3..4);
        check(0..2, 1..3, 1..4);

        // Another preamble case but now it doesn't end with a new list
        // [[..., A], NULL, [D, E]]
        //
        // What we do will depend on the preamble action
        let rep = Some(vec![0, 1, 1, 0]);
        let def = Some(vec![0, 1, 0, 0]);
        let max_visible_def = 0;
        let total_items = 4;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Take,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        // If we are taking the preamble then the range must start at 0
        check(0..1, 0..1, 0..2);
        check(0..2, 0..3, 0..4);

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Skip,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        // If we are taking the preamble then the range must start at 0
        check(0..1, 1..1, 1..2);
        check(1..2, 1..3, 2..4);
        check(0..2, 1..3, 1..4);

        // Now a preamble case without any definition levels
        // [[..., A] [B, C], [D]]
        let rep = Some(vec![0, 1, 0, 1]);
        let def: Option<Vec<u16>> = None;
        let max_visible_def = 0;
        let total_items = 4;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Take,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        // If we are taking the preamble then the range must start at 0
        check(0..1, 0..3, 0..3);
        check(0..2, 0..4, 0..4);

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Skip,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(0..1, 1..3, 1..3);
        check(1..2, 3..4, 3..4);
        check(0..2, 1..4, 1..4);

        // If we have nested lists then non-top level lists may be empty/null
        // and we need to make sure we still handle them as invisible items (we
        // failed to do this previously)
        let rep = Some(vec![2, 1, 2, 0, 1, 2]);
        let def = Some(vec![0, 1, 2, 0, 0, 0]);
        let max_rep = 2;
        let max_visible_def = 0;
        let total_items = 4;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Absent,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(0..3, 0..4, 0..6);
        check(0..1, 0..1, 0..2);
        check(1..2, 1..3, 2..5);
        check(2..3, 3..4, 5..6);

        // Invisible items in a preamble that we are taking (regressing a previous failure)
        let rep = Some(vec![0, 0, 1, 0, 1, 1]);
        let def = Some(vec![0, 1, 0, 0, 0, 0]);
        let max_rep = 1;
        let max_visible_def = 0;
        let total_items = 5;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Take,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(0..0, 0..1, 0..2);
        check(0..1, 0..3, 0..4);
        check(0..2, 0..4, 0..5);

        // Skip preamble (with invis items) and skip a few rows (with invis items)
        // and then take a few rows but not all the rows
        let rep = Some(vec![0, 1, 0, 1, 0, 1, 0, 1]);
        let def = Some(vec![1, 0, 1, 1, 0, 0, 0, 0]);
        let max_rep = 1;
        let max_visible_def = 0;
        let total_items = 5;

        let check = |range, expected_item_range, expected_level_range| {
            let (item_range, level_range) = DecodeMiniBlockTask::map_range(
                range,
                rep.as_ref(),
                def.as_ref(),
                max_rep,
                max_visible_def,
                total_items,
                PreambleAction::Skip,
            );
            assert_eq!(item_range, expected_item_range);
            assert_eq!(level_range, expected_level_range);
        };

        check(2..3, 2..4, 5..7);
    }

    #[test]
    fn test_slice_batch_data_and_rebase_offsets_u32() {
        let data = LanceBuffer::copy_slice(b"0123456789abcdefghij");
        let offsets = LanceBuffer::reinterpret_vec(vec![6_u32, 8_u32, 8_u32, 12_u32]);

        let (sliced_data, normalized_offsets) =
            VariableFullZipDecoder::slice_batch_data_and_rebase_offsets(&data, &offsets, 32)
                .unwrap();

        assert_eq!(sliced_data.as_ref(), b"6789ab");
        let normalized = normalized_offsets.borrow_to_typed_slice::<u32>();
        assert_eq!(normalized.as_ref(), &[0, 2, 2, 6]);
    }

    #[test]
    fn test_slice_batch_data_and_rebase_offsets_u64() {
        let data = LanceBuffer::copy_slice(b"abcdefghijklmnopqrstuvwxyz");
        let offsets = LanceBuffer::reinterpret_vec(vec![10_u64, 12_u64, 16_u64, 20_u64]);

        let (sliced_data, normalized_offsets) =
            VariableFullZipDecoder::slice_batch_data_and_rebase_offsets(&data, &offsets, 64)
                .unwrap();

        assert_eq!(sliced_data.as_ref(), b"klmnopqrst");
        let normalized = normalized_offsets.borrow_to_typed_slice::<u64>();
        assert_eq!(normalized.as_ref(), &[0, 2, 6, 10]);
    }

    #[test]
    fn test_slice_batch_data_and_rebase_offsets_rejects_invalid_offsets() {
        let data = LanceBuffer::copy_slice(b"abcd");
        let offsets = LanceBuffer::reinterpret_vec(vec![3_u32, 2_u32]);

        let err = VariableFullZipDecoder::slice_batch_data_and_rebase_offsets(&data, &offsets, 32)
            .expect_err("offset end before start should error");
        assert!(err.to_string().contains("less than base"));
    }

    #[test]
    fn test_schedule_instructions() {
        // Convert repetition index to bytes for testing
        let rep_data: Vec<u64> = vec![5, 2, 3, 0, 4, 7, 2, 0];
        let rep_bytes: Vec<u8> = rep_data.iter().flat_map(|v| v.to_le_bytes()).collect();
        let chunk_index = MiniBlockChunkIndex::new_nested_for_test(&rep_bytes, 2);

        let check = |user_ranges, expected_instructions| {
            let instructions = ChunkInstructions::schedule_instructions(&chunk_index, user_ranges);
            assert_eq!(instructions, expected_instructions);
        };

        // The instructions we expect if we're grabbing the whole range
        let expected_take_all = vec![
            ChunkInstructions {
                chunk_idx: 0,
                preamble: PreambleAction::Absent,
                rows_to_skip: 0,
                rows_to_take: 6,
                take_trailer: true,
            },
            ChunkInstructions {
                chunk_idx: 1,
                preamble: PreambleAction::Take,
                rows_to_skip: 0,
                rows_to_take: 2,
                take_trailer: false,
            },
            ChunkInstructions {
                chunk_idx: 2,
                preamble: PreambleAction::Absent,
                rows_to_skip: 0,
                rows_to_take: 5,
                take_trailer: true,
            },
            ChunkInstructions {
                chunk_idx: 3,
                preamble: PreambleAction::Take,
                rows_to_skip: 0,
                rows_to_take: 1,
                take_trailer: false,
            },
        ];

        // Take all as 1 range
        check(&[0..14], expected_take_all.clone());

        // Take all a individual rows
        check(
            &[
                0..1,
                1..2,
                2..3,
                3..4,
                4..5,
                5..6,
                6..7,
                7..8,
                8..9,
                9..10,
                10..11,
                11..12,
                12..13,
                13..14,
            ],
            expected_take_all,
        );

        // Test some partial takes

        // 2 rows in the same chunk but not contiguous
        check(
            &[0..1, 3..4],
            vec![
                ChunkInstructions {
                    chunk_idx: 0,
                    preamble: PreambleAction::Absent,
                    rows_to_skip: 0,
                    rows_to_take: 1,
                    take_trailer: false,
                },
                ChunkInstructions {
                    chunk_idx: 0,
                    preamble: PreambleAction::Absent,
                    rows_to_skip: 3,
                    rows_to_take: 1,
                    take_trailer: false,
                },
            ],
        );

        // Taking just a trailer/preamble
        check(
            &[5..6],
            vec![
                ChunkInstructions {
                    chunk_idx: 0,
                    preamble: PreambleAction::Absent,
                    rows_to_skip: 5,
                    rows_to_take: 1,
                    take_trailer: true,
                },
                ChunkInstructions {
                    chunk_idx: 1,
                    preamble: PreambleAction::Take,
                    rows_to_skip: 0,
                    rows_to_take: 0,
                    take_trailer: false,
                },
            ],
        );

        // Skipping an entire chunk
        check(
            &[7..10],
            vec![
                ChunkInstructions {
                    chunk_idx: 1,
                    preamble: PreambleAction::Skip,
                    rows_to_skip: 1,
                    rows_to_take: 1,
                    take_trailer: false,
                },
                ChunkInstructions {
                    chunk_idx: 2,
                    preamble: PreambleAction::Absent,
                    rows_to_skip: 0,
                    rows_to_take: 2,
                    take_trailer: false,
                },
            ],
        );
    }

    #[test]
    fn test_drain_instructions() {
        fn drain_from_instructions(
            instructions: &mut VecDeque<ChunkInstructions>,
            mut rows_desired: u64,
            need_preamble: &mut bool,
            skip_in_chunk: &mut u64,
        ) -> Vec<ChunkDrainInstructions> {
            // Note: instructions.len() is an upper bound, we typically take much fewer
            let mut drain_instructions = Vec::with_capacity(instructions.len());
            while rows_desired > 0 || *need_preamble {
                let (next_instructions, consumed_chunk) = instructions
                    .front()
                    .unwrap()
                    .drain_from_instruction(&mut rows_desired, need_preamble, skip_in_chunk);
                if consumed_chunk {
                    instructions.pop_front();
                }
                drain_instructions.push(next_instructions);
            }
            drain_instructions
        }

        // Convert repetition index to bytes for testing
        let rep_data: Vec<u64> = vec![5, 2, 3, 0, 4, 7, 2, 0];
        let rep_bytes: Vec<u8> = rep_data.iter().flat_map(|v| v.to_le_bytes()).collect();
        let chunk_index = MiniBlockChunkIndex::new_nested_for_test(&rep_bytes, 2);
        let user_ranges = vec![1..7, 10..14];

        // First, schedule the ranges
        let scheduled = ChunkInstructions::schedule_instructions(&chunk_index, &user_ranges);

        let mut to_drain = VecDeque::from(scheduled.clone());

        // Now we drain in batches of 4

        let mut need_preamble = false;
        let mut skip_in_chunk = 0;

        let next_batch =
            drain_from_instructions(&mut to_drain, 4, &mut need_preamble, &mut skip_in_chunk);

        assert!(!need_preamble);
        assert_eq!(skip_in_chunk, 4);
        assert_eq!(
            next_batch,
            vec![ChunkDrainInstructions {
                chunk_instructions: scheduled[0].clone(),
                rows_to_take: 4,
                rows_to_skip: 0,
                preamble_action: PreambleAction::Absent,
            }]
        );

        let next_batch =
            drain_from_instructions(&mut to_drain, 4, &mut need_preamble, &mut skip_in_chunk);

        assert!(!need_preamble);
        assert_eq!(skip_in_chunk, 2);

        assert_eq!(
            next_batch,
            vec![
                ChunkDrainInstructions {
                    chunk_instructions: scheduled[0].clone(),
                    rows_to_take: 1,
                    rows_to_skip: 4,
                    preamble_action: PreambleAction::Absent,
                },
                ChunkDrainInstructions {
                    chunk_instructions: scheduled[1].clone(),
                    rows_to_take: 1,
                    rows_to_skip: 0,
                    preamble_action: PreambleAction::Take,
                },
                ChunkDrainInstructions {
                    chunk_instructions: scheduled[2].clone(),
                    rows_to_take: 2,
                    rows_to_skip: 0,
                    preamble_action: PreambleAction::Absent,
                }
            ]
        );

        let next_batch =
            drain_from_instructions(&mut to_drain, 2, &mut need_preamble, &mut skip_in_chunk);

        assert!(!need_preamble);
        assert_eq!(skip_in_chunk, 0);

        assert_eq!(
            next_batch,
            vec![
                ChunkDrainInstructions {
                    chunk_instructions: scheduled[2].clone(),
                    rows_to_take: 1,
                    rows_to_skip: 2,
                    preamble_action: PreambleAction::Absent,
                },
                ChunkDrainInstructions {
                    chunk_instructions: scheduled[3].clone(),
                    rows_to_take: 1,
                    rows_to_skip: 0,
                    preamble_action: PreambleAction::Take,
                },
            ]
        );

        // Regression case.  Need a chunk with preamble, rows, and trailer (the middle chunk here)
        let rep_data: Vec<u64> = vec![5, 2, 3, 3, 20, 0];
        let rep_bytes: Vec<u8> = rep_data.iter().flat_map(|v| v.to_le_bytes()).collect();
        let chunk_index = MiniBlockChunkIndex::new_nested_for_test(&rep_bytes, 2);
        let user_ranges = vec![0..28];

        // First, schedule the ranges
        let scheduled = ChunkInstructions::schedule_instructions(&chunk_index, &user_ranges);

        let mut to_drain = VecDeque::from(scheduled.clone());

        // Drain first chunk and some of second chunk

        let mut need_preamble = false;
        let mut skip_in_chunk = 0;

        let next_batch =
            drain_from_instructions(&mut to_drain, 7, &mut need_preamble, &mut skip_in_chunk);

        assert_eq!(
            next_batch,
            vec![
                ChunkDrainInstructions {
                    chunk_instructions: scheduled[0].clone(),
                    rows_to_take: 6,
                    rows_to_skip: 0,
                    preamble_action: PreambleAction::Absent,
                },
                ChunkDrainInstructions {
                    chunk_instructions: scheduled[1].clone(),
                    rows_to_take: 1,
                    rows_to_skip: 0,
                    preamble_action: PreambleAction::Take,
                },
            ]
        );

        assert!(!need_preamble);
        assert_eq!(skip_in_chunk, 1);

        // Now, the tricky part.  We drain the second chunk, including the trailer, and need to make sure
        // we get a drain task to take the preamble of the third chunk (and nothing else)
        let next_batch =
            drain_from_instructions(&mut to_drain, 2, &mut need_preamble, &mut skip_in_chunk);

        assert_eq!(
            next_batch,
            vec![
                ChunkDrainInstructions {
                    chunk_instructions: scheduled[1].clone(),
                    rows_to_take: 2,
                    rows_to_skip: 1,
                    preamble_action: PreambleAction::Skip,
                },
                ChunkDrainInstructions {
                    chunk_instructions: scheduled[2].clone(),
                    rows_to_take: 0,
                    rows_to_skip: 0,
                    preamble_action: PreambleAction::Take,
                },
            ]
        );

        assert!(!need_preamble);
        assert_eq!(skip_in_chunk, 0);
    }

    use super::chunk_index::{PrefixSums, RowMapping};
    use super::{MINIBLOCK_ALIGNMENT, Words, build_chunk_index};
    use bytes::Bytes;
    use lance_core::cache::{Context, DeepSizeOf};
    use rstest::rstest;

    /// Builds a `Words` metadata buffer (u16 words) from `(log_num_values, num_bytes)`
    /// pairs, returning the words and the total data-buffer size.
    fn words_from(entries: &[(u32, u32)]) -> (Words, u64) {
        let mut raw = Vec::with_capacity(entries.len() * 2);
        let mut total = 0u64;
        for &(log, num_bytes) in entries {
            assert!(num_bytes > 0 && num_bytes % MINIBLOCK_ALIGNMENT as u32 == 0);
            let divided = num_bytes / MINIBLOCK_ALIGNMENT as u32 - 1;
            let word = (divided << 4) | log;
            assert!(word <= u16::MAX as u32, "test word {word} exceeds u16");
            raw.extend_from_slice(&(word as u16).to_le_bytes());
            total += num_bytes as u64;
        }
        (Words::from_bytes(Bytes::from(raw), false).unwrap(), total)
    }

    fn rep_bytes_from(values: &[u64]) -> Vec<u8> {
        values.iter().flat_map(|v| v.to_le_bytes()).collect()
    }

    #[rstest]
    // Two full chunks of 8 values (log 3) plus a partial last chunk; byte sizes vary
    // independently of value counts.
    #[case::uniform_partial_last(&[(3, 16), (3, 24), (0, 8)], 19, "uniform_flat", 8, 3)]
    // Single chunk covers the whole page.
    #[case::single_chunk(&[(0, 24)], 5, "uniform_flat", 5, 5)]
    // Last chunk is also full (exact multiple).
    #[case::exact_multiple(&[(3, 16), (3, 16)], 16, "uniform_flat", 8, 8)]
    // Non-last chunks differ in size, so this is a non-uniform flat page.
    #[case::non_uniform(&[(4, 16), (2, 16), (0, 8)], 21, "flat", 16, 1)]
    fn test_flat_detection(
        #[case] entries: &[(u32, u32)],
        #[case] items_in_page: u64,
        #[case] expected_kind: &str,
        #[case] expected_first_items: u64,
        #[case] expected_last_items: u64,
    ) {
        let base = 100u64;
        let (words, data_buf_size) = words_from(entries);
        let index = build_chunk_index(&words, items_in_page, base, data_buf_size, None, 0).unwrap();

        assert_eq!(index.row_mapping_debug(), expected_kind);
        assert_eq!(index.num_chunks(), entries.len());
        assert_eq!(index.items_in_chunk(0), expected_first_items);
        assert_eq!(index.items_in_chunk(entries.len() - 1), expected_last_items);

        // Byte ranges are absolute, contiguous, and exactly cover the data buffer.
        let mut expected_start = base;
        for (i, &(_, num_bytes)) in entries.iter().enumerate() {
            let range = index.byte_range(i);
            assert_eq!(range.start, expected_start);
            assert_eq!(range.end - range.start, num_bytes as u64);
            expected_start = range.end;
        }
        assert_eq!(expected_start, base + data_buf_size);

        // For flat pages rows == items, so the per-chunk items sum to the page total.
        let total_items: u64 = (0..index.num_chunks())
            .map(|i| index.items_in_chunk(i))
            .sum();
        assert_eq!(total_items, items_in_page);
    }

    #[test]
    fn test_nested_detection_and_axes() {
        // Repetition index (stride 2): three chunks holding 5, 4, 3 rows, no trailers.
        let rep = rep_bytes_from(&[5, 0, 4, 0, 3, 0]);

        // Uniform leaf chunking: value counts 4, 4, 2.
        let (words, data_buf_size) = words_from(&[(2, 8), (2, 8), (0, 8)]);
        let index = build_chunk_index(&words, 10, 0, data_buf_size, Some(&rep), 1).unwrap();
        assert_eq!(index.row_mapping_debug(), "nested");
        assert_eq!(index.num_chunks(), 3);
        // Rows come from the repetition index, not the value counts.
        assert_eq!(index.first_row(0), 0);
        assert_eq!(index.rows_in_chunk(0), 5);
        assert_eq!(index.first_row(1), 5);
        assert_eq!(index.rows_in_chunk(1), 4);
        assert_eq!(index.first_row(2), 9);
        assert_eq!(index.rows_in_chunk(2), 3);
        // Items come from the value words.
        assert_eq!(index.items_in_chunk(0), 4);
        assert_eq!(index.items_in_chunk(1), 4);
        assert_eq!(index.items_in_chunk(2), 2);

        // Non-uniform leaf chunking: value counts 8, 2, 5.
        let (words_nu, dbs_nu) = words_from(&[(3, 8), (1, 8), (0, 8)]);
        let index_nu = build_chunk_index(&words_nu, 15, 0, dbs_nu, Some(&rep), 1).unwrap();
        assert_eq!(index_nu.row_mapping_debug(), "nested");
        assert_eq!(index_nu.items_in_chunk(0), 8);
        assert_eq!(index_nu.items_in_chunk(1), 2);
        assert_eq!(index_nu.items_in_chunk(2), 5);
        // The row axis is unchanged by the leaf chunking.
        assert_eq!(index_nu.rows_in_chunk(0), 5);
    }

    #[test]
    fn test_uniform_flat_matches_prefix_sum_flat() {
        // Distribution: 4 chunks of 4 values, last chunk 3 (15 items total).
        let (words, data_buf_size) = words_from(&[(2, 8), (2, 8), (2, 8), (0, 8)]);
        let uniform = build_chunk_index(&words, 15, 0, data_buf_size, None, 0).unwrap();
        assert_eq!(uniform.row_mapping_debug(), "uniform_flat");

        // The same distribution expressed as a non-uniform Flat prefix-sum index.
        let byte_starts = PrefixSums::from_deltas([8u64, 8, 8, 8].into_iter(), 4, 32);
        let value_starts = PrefixSums::from_deltas([4u64, 4, 4, 3].into_iter(), 4, 15);
        let flat = MiniBlockChunkIndex::new(0, byte_starts, RowMapping::Flat { value_starts });
        assert_eq!(flat.row_mapping_debug(), "flat");

        // Lookup parity: identical byte ranges and item counts.
        for i in 0..4 {
            assert_eq!(uniform.byte_range(i), flat.byte_range(i));
            assert_eq!(uniform.items_in_chunk(i), flat.items_in_chunk(i));
        }

        // Scheduler parity across scan / single-row / partial / scattered multi-range.
        let range_sets: Vec<Vec<std::ops::Range<u64>>> = vec![
            vec![0..15],
            vec![0..1],
            vec![7..8],
            vec![14..15],
            vec![3..10],
            vec![0..2, 5..6, 12..15],
        ];
        for ranges in &range_sets {
            let from_uniform = ChunkInstructions::schedule_instructions(&uniform, ranges);
            let from_flat = ChunkInstructions::schedule_instructions(&flat, ranges);
            assert_eq!(from_uniform, from_flat, "mismatch for ranges {ranges:?}");
        }

        // A full scan yields one Absent, no-trailer instruction per chunk.
        let full = ChunkInstructions::schedule_instructions(&uniform, &[0..15]);
        assert_eq!(full.len(), 4);
        for (i, inst) in full.iter().enumerate() {
            assert_eq!(inst.chunk_idx, i);
            assert_eq!(inst.preamble, PreambleAction::Absent);
            assert_eq!(inst.rows_to_skip, 0);
            assert!(!inst.take_trailer);
        }
        assert_eq!(full.iter().map(|i| i.rows_to_take).sum::<u64>(), 15);
    }

    #[test]
    fn test_deep_size_per_variant_below_legacy() {
        // The previous representation cached 48 bytes per chunk (24 for ChunkMeta plus
        // 24 for a rep-index block); every variant's heap must be well below that.
        const LEGACY_PER_CHUNK: usize = 48;
        let num_chunks = 3;
        let heap = |index: &MiniBlockChunkIndex| index.deep_size_of_children(&mut Context::new());

        let (uniform_words, uniform_dbs) = words_from(&[(2, 8), (2, 8), (0, 8)]);
        let uniform = build_chunk_index(&uniform_words, 10, 0, uniform_dbs, None, 0).unwrap();
        assert_eq!(uniform.row_mapping_debug(), "uniform_flat");
        assert!(heap(&uniform) < LEGACY_PER_CHUNK * num_chunks);

        let (flat_words, flat_dbs) = words_from(&[(3, 8), (1, 8), (0, 8)]);
        let flat = build_chunk_index(&flat_words, 11, 0, flat_dbs, None, 0).unwrap();
        assert_eq!(flat.row_mapping_debug(), "flat");
        assert!(heap(&flat) < LEGACY_PER_CHUNK * num_chunks);
        // Flat carries a value-starts array that UniformFlat derives arithmetically.
        assert!(heap(&flat) > heap(&uniform));

        let rep = rep_bytes_from(&[4, 0, 3, 0, 3, 0]);
        let (nested_words, nested_dbs) = words_from(&[(2, 8), (2, 8), (0, 8)]);
        let nested = build_chunk_index(&nested_words, 10, 0, nested_dbs, Some(&rep), 1).unwrap();
        assert_eq!(nested.row_mapping_debug(), "nested");
        assert!(heap(&nested) < LEGACY_PER_CHUNK * num_chunks);
    }

    #[tokio::test]
    async fn test_fullzip_initialize_is_lazy() {
        use futures::{FutureExt, future::BoxFuture};
        use std::ops::Range;
        use std::sync::Mutex;

        #[derive(Debug, Clone)]
        struct RecordingScheduler {
            data: bytes::Bytes,
            requests: Arc<Mutex<Vec<Vec<Range<u64>>>>>,
        }

        impl RecordingScheduler {
            fn new(data: bytes::Bytes) -> Self {
                Self {
                    data,
                    requests: Arc::new(Mutex::new(Vec::new())),
                }
            }

            fn requests(&self) -> Vec<Vec<Range<u64>>> {
                self.requests.lock().unwrap().clone()
            }
        }

        impl crate::EncodingsIo for RecordingScheduler {
            fn submit_request(
                &self,
                ranges: Vec<Range<u64>>,
                _priority: u64,
            ) -> BoxFuture<'static, crate::Result<Vec<bytes::Bytes>>> {
                self.requests.lock().unwrap().push(ranges.clone());
                let data = ranges
                    .into_iter()
                    .map(|range| self.data.slice(range.start as usize..range.end as usize))
                    .collect::<Vec<_>>();
                std::future::ready(Ok(data)).boxed()
            }
        }

        #[derive(Debug)]
        struct TestFixedDecompressor;

        impl FixedPerValueDecompressor for TestFixedDecompressor {
            fn decompress(
                &self,
                _data: FixedWidthDataBlock,
                _num_rows: u64,
            ) -> crate::Result<DataBlock> {
                unimplemented!("Test decompressor")
            }

            fn bits_per_value(&self) -> u64 {
                32
            }
        }

        let io = Arc::new(RecordingScheduler::new(bytes::Bytes::from(vec![
            0;
            16 * 1024
        ])));
        let mut scheduler = FullZipScheduler {
            data_buf_position: 0,
            data_buf_size: 4096,
            rep_index: Some(FullZipRepIndexDetails {
                buf_position: 1000,
                bytes_per_value: 4,
            }),
            priority: 0,
            rows_in_page: 100,
            bits_per_offset: 32,
            details: Arc::new(FullZipDecodeDetails {
                value_decompressor: PerValueDecompressor::Fixed(Arc::new(TestFixedDecompressor)),
                def_meaning: Arc::new([crate::repdef::DefinitionInterpretation::NullableItem]),
                ctrl_word_parser: crate::repdef::ControlWordParser::new(0, 1),
                max_rep: 0,
                max_visible_def: 0,
            }),
            cached_state: None,
            enable_cache: false,
        };

        let io_dyn: Arc<dyn crate::EncodingsIo> = io.clone();
        let initialization = scheduler.init_layout().unwrap();
        assert!(
            initialization.required_ranges().next().is_none(),
            "FullZip should not request any ranges when caching is disabled"
        );
        let buffers = initialization.with_buffers(vec![]).unwrap();
        let cached_data = scheduler.init_from_buffers(buffers, &io_dyn).await.unwrap();

        assert!(
            cached_data
                .as_arc_any()
                .downcast_ref::<super::NoCachedPageData>()
                .is_some(),
            "FullZip initialize should not eagerly load repetition index data"
        );
        assert!(scheduler.cached_state.is_none());
        assert!(
            io.requests().is_empty(),
            "FullZip initialize should not issue any I/O"
        );
    }

    /// Prove that an N-page cache miss issues one `submit_request` carrying every
    /// page's metadata range, rather than one submission per page. The production
    /// I/O scheduler can then coalesce adjacent ranges within that batch.
    #[tokio::test]
    async fn test_initialize_coalesces_missed_page_metadata() {
        use std::ops::Range;

        use futures::FutureExt;
        use futures::future::BoxFuture;

        use super::{
            CachedPage, CachedPageData, Error, PageCacheView, PageDataCacheKey,
            PageInfoAndScheduler, PageInitialization, PageInitializationBuffers, PageLoadTask,
            StructuralPrimitiveFieldScheduler,
        };
        use crate::decoder::{FilterExpression, SchedulerContext, StructuralFieldScheduler};

        // Give each fake page one fixed-size metadata buffer. The ranges are far
        // apart because this test observes batching, not the I/O layer's distance-
        // based range coalescing.
        #[derive(Debug)]
        struct FakePageScheduler {
            meta_range: Range<u64>,
        }
        impl StructuralPageScheduler for FakePageScheduler {
            fn init_layout(&self) -> crate::Result<PageInitialization> {
                Ok(PageInitialization::new([Some(self.meta_range.clone())]))
            }
            fn init_from_buffers<'a>(
                &'a mut self,
                buffers: PageInitializationBuffers,
                _io: &Arc<dyn EncodingsIo>,
            ) -> BoxFuture<'a, crate::Result<Arc<dyn CachedPageData>>> {
                let [buffer] = buffers.try_into_array("fake").unwrap();
                let buffer = buffer.unwrap();
                assert_eq!(
                    &buffer[..8],
                    &self.meta_range.start.to_le_bytes(),
                    "each page gets the buffer for its own range"
                );
                std::future::ready(Ok(Arc::new(CacheMarker(42)) as Arc<dyn CachedPageData>)).boxed()
            }
            fn try_load(&mut self, data: &Arc<dyn CachedPageData>) -> crate::Result<()> {
                data.clone()
                    .as_arc_any()
                    .downcast::<CacheMarker>()
                    .map(|_| ())
                    .map_err(|_| {
                        Error::invalid_input_source(
                            "Fake page cache data has an unexpected type".into(),
                        )
                    })
            }
            fn schedule_ranges(
                &self,
                _ranges: &[Range<u64>],
                _io: &Arc<dyn EncodingsIo>,
            ) -> crate::Result<Vec<PageLoadTask>> {
                unreachable!("initialize never schedules data reads");
            }
        }

        const NUM_PAGES: u64 = 64;
        const META_LEN: u64 = 64;
        const STRIDE: u64 = 1024 * 1024;
        let page_schedulers = (0..NUM_PAGES)
            .map(|i| PageInfoAndScheduler {
                page_index: i as usize,
                row_range: (i * 100)..((i + 1) * 100),
                scheduler: Box::new(FakePageScheduler {
                    meta_range: (i * STRIDE)..(i * STRIDE + META_LEN),
                }),
            })
            .collect();
        let mut scheduler = StructuralPrimitiveFieldScheduler::from_page_schedulers(
            page_schedulers,
            0,
            Arc::from("test"),
        );

        let io = Arc::new(RecordingScheduler::default());
        // A no-op cache always misses, so every page is a miss.
        let cache = Arc::new(lance_core::cache::LanceCache::no_cache());
        let context = SchedulerContext::new(io.clone(), cache.clone());

        // `None` requests every page; against an always-miss cache that's an
        // N-page miss.
        let filter = FilterExpression::no_filter();
        scheduler.initialize(None, &filter, &context).await.unwrap();

        {
            let requests = io.requests.lock().unwrap();
            assert_eq!(
                requests.len(),
                1,
                "an N-page miss must issue exactly one request, not one per page"
            );
            assert_eq!(
                requests[0].len() as u64,
                NUM_PAGES,
                "the single request must carry every missed page's metadata range"
            );
        }

        io.requests.lock().unwrap().clear();
        let cache_misses_before = cache.stats().await.misses;
        let requested_ranges = Arc::<[Range<u64>]>::from([150..151, 6_350..6_351]);
        scheduler
            .initialize(Some(&requested_ranges), &filter, &context)
            .await
            .unwrap();
        assert_eq!(
            cache.stats().await.misses - cache_misses_before,
            2,
            "range-scoped initialization must only look up touched pages"
        );

        {
            let requests = io.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0],
                vec![
                    STRIDE..(STRIDE + META_LEN),
                    63 * STRIDE..(63 * STRIDE + META_LEN),
                ],
                "range-scoped initialization must omit untouched pages"
            );
        }

        io.requests.lock().unwrap().clear();
        let cache = Arc::new(lance_core::cache::LanceCache::with_capacity(4 * 1024));
        let cache_key = PageDataCacheKey {
            column_index: 9,
            page_index: 0,
            view: PageCacheView::Native,
            target_data_type_tag: Arc::from("test"),
        };
        cache
            .insert_with_key(
                &cache_key,
                Arc::new(CachedPage {
                    data: Arc::new(super::NoCachedPageData),
                }),
            )
            .await;
        let mut recovering_scheduler = StructuralPrimitiveFieldScheduler::from_page_schedulers(
            vec![PageInfoAndScheduler {
                page_index: 0,
                row_range: 0..100,
                scheduler: Box::new(FakePageScheduler {
                    meta_range: 0..META_LEN,
                }),
            }],
            9,
            Arc::from("test"),
        );
        let recovering_context = SchedulerContext::new(io.clone(), cache.clone());
        recovering_scheduler
            .initialize(None, &filter, &recovering_context)
            .await
            .unwrap();
        assert_eq!(io.requests.lock().unwrap().as_slice(), &[vec![0..META_LEN]]);

        io.requests.lock().unwrap().clear();
        let mut warm_scheduler = StructuralPrimitiveFieldScheduler::from_page_schedulers(
            vec![PageInfoAndScheduler {
                page_index: 0,
                row_range: 0..100,
                scheduler: Box::new(FakePageScheduler {
                    meta_range: 0..META_LEN,
                }),
            }],
            9,
            Arc::from("test"),
        );
        warm_scheduler
            .initialize(None, &filter, &recovering_context)
            .await
            .unwrap();
        assert!(
            io.requests.lock().unwrap().is_empty(),
            "the rebuilt cache entry must be reusable"
        );

        let mut out_of_order_scheduler = StructuralPrimitiveFieldScheduler::from_page_schedulers(
            vec![
                PageInfoAndScheduler {
                    page_index: 0,
                    row_range: 0..100,
                    scheduler: Box::new(FakePageScheduler {
                        meta_range: STRIDE..(STRIDE + META_LEN),
                    }),
                },
                PageInfoAndScheduler {
                    page_index: 1,
                    row_range: 100..200,
                    scheduler: Box::new(FakePageScheduler {
                        meta_range: 0..META_LEN,
                    }),
                },
            ],
            0,
            Arc::from("test"),
        );
        out_of_order_scheduler
            .initialize(None, &filter, &context)
            .await
            .unwrap();
        assert_eq!(
            io.requests.lock().unwrap().as_slice(),
            &[vec![0..META_LEN, STRIDE..(STRIDE + META_LEN)]],
            "metadata reads must be sorted without changing page buffer assignment"
        );
    }

    #[tokio::test]
    async fn page_cache_key_distinguishes_page_view_and_target_type() {
        let cache = lance_core::cache::LanceCache::with_capacity(4 * 1024);
        let cases = [
            (
                PageDataCacheKey {
                    column_index: 7,
                    page_index: 0,
                    view: PageCacheView::Native,
                    target_data_type_tag: Arc::from("Boolean"),
                },
                10,
            ),
            (
                PageDataCacheKey {
                    column_index: 7,
                    page_index: 1,
                    view: PageCacheView::Native,
                    target_data_type_tag: Arc::from("Boolean"),
                },
                11,
            ),
            (
                PageDataCacheKey {
                    column_index: 7,
                    page_index: 0,
                    view: PageCacheView::Native,
                    target_data_type_tag: Arc::from("UInt8"),
                },
                12,
            ),
            (
                PageDataCacheKey {
                    column_index: 7,
                    page_index: 0,
                    view: PageCacheView::BlobDescriptor,
                    target_data_type_tag: Arc::from("Struct"),
                },
                20,
            ),
            (
                PageDataCacheKey {
                    column_index: 7,
                    page_index: 0,
                    view: PageCacheView::BlobPayload,
                    target_data_type_tag: Arc::from("LargeBinary"),
                },
                21,
            ),
        ];

        for (key, marker) in &cases {
            cache
                .insert_with_key(
                    key,
                    Arc::new(CachedPage {
                        data: Arc::new(CacheMarker(*marker)),
                    }),
                )
                .await;
        }

        assert_eq!(cache.size().await, cases.len());
        for (key, expected) in &cases {
            let cached = cache.get_with_key(key).await.unwrap();
            let marker = cached
                .data
                .clone()
                .as_arc_any()
                .downcast::<CacheMarker>()
                .unwrap();
            assert_eq!(marker.0, *expected);
        }
    }

    #[tokio::test]
    async fn no_op_pages_do_not_create_cache_entries() {
        let mut scheduler = StructuralPrimitiveFieldScheduler::from_page_schedulers(
            vec![PageInfoAndScheduler {
                page_index: 0,
                row_range: 0..10,
                scheduler: Box::new(SimpleAllNullScheduler::default()),
            }],
            3,
            Arc::from("test"),
        );
        let io: Arc<dyn crate::EncodingsIo> =
            Arc::new(SimulatedScheduler::new(bytes::Bytes::new()));
        let cache = Arc::new(lance_core::cache::LanceCache::with_capacity(4 * 1024));
        let context = SchedulerContext::new(io, cache.clone());

        scheduler
            .initialize(None, &FilterExpression::no_filter(), &context)
            .await
            .unwrap();

        assert_eq!(cache.size().await, 0);
    }

    #[test]
    fn page_initialization_preserves_optional_buffer_slots() {
        let initialization = PageInitialization::new([None, Some(20..22), None, Some(10..11)]);
        assert_eq!(
            initialization
                .required_ranges()
                .cloned()
                .collect::<Vec<_>>(),
            [20..22, 10..11]
        );

        let buffers = initialization
            .with_buffers(vec![
                bytes::Bytes::from_static(b"ab"),
                bytes::Bytes::from_static(b"c"),
            ])
            .unwrap();
        let [first, second, third, fourth] = buffers.try_into_array("test").unwrap();
        assert!(first.is_none());
        assert_eq!(second.unwrap(), bytes::Bytes::from_static(b"ab"));
        assert!(third.is_none());
        assert_eq!(fourth.unwrap(), bytes::Bytes::from_static(b"c"));

        let error = PageInitialization::new([Some(0..1)])
            .with_buffers(Vec::new())
            .unwrap_err();
        assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
        assert!(error.to_string().contains("1 declared ranges"));
    }

    #[tokio::test]
    async fn initialize_builds_only_requested_page_schedulers() {
        use crate::compression::DefaultDecompressionStrategy;
        use crate::decoder::{
            ColumnInfo, FilterExpression, PageEncoding, PageInfo, SchedulerContext,
            StructuralFieldScheduler,
        };
        use crate::format::pb;
        use lance_core::datatypes::Field;

        const NUM_PAGES: u64 = 64;
        const ROWS_PER_PAGE: u64 = 100;
        // All-null constant pages need no initialization I/O, so this test
        // observes construction alone.
        let page_infos = (0..NUM_PAGES)
            .map(|page_index| PageInfo {
                num_rows: ROWS_PER_PAGE,
                priority: page_index * ROWS_PER_PAGE,
                encoding: PageEncoding::Structural(pb21::PageLayout {
                    layout: Some(pb21::page_layout::Layout::ConstantLayout(
                        pb21::ConstantLayout {
                            layers: vec![pb21::RepDefLayer::RepdefNullableItem as i32],
                            ..Default::default()
                        },
                    )),
                }),
                buffer_offsets_and_sizes: Arc::new([]),
            })
            .collect::<Vec<_>>();
        let column_info = Arc::new(ColumnInfo::new(
            3,
            Arc::from(page_infos),
            Vec::new(),
            pb::ColumnEncoding::default(),
        ));
        let field = Field::try_from(&ArrowField::new("x", DataType::Int32, true)).unwrap();
        #[allow(deprecated)]
        let eager = StructuralPrimitiveFieldScheduler::try_new(
            &column_info,
            &DefaultDecompressionStrategy {},
            false,
            &field,
        )
        .unwrap();
        assert_eq!(eager.page_schedulers.len(), NUM_PAGES as usize);
        assert!(eager.page_source.is_none());

        let mut scheduler = StructuralPrimitiveFieldScheduler::try_new_lazy(
            column_info,
            Arc::new(DefaultDecompressionStrategy {}),
            false,
            &field,
        )
        .unwrap();
        assert!(
            scheduler.page_schedulers.is_empty(),
            "construction must not build page schedulers"
        );
        assert_eq!(scheduler.page_row_ends.len(), NUM_PAGES as usize);

        let io: Arc<dyn EncodingsIo> = Arc::new(SimulatedScheduler::new(bytes::Bytes::new()));
        let cache = Arc::new(lance_core::cache::LanceCache::no_cache());
        let mut context = SchedulerContext::new(io, cache);
        let filter = FilterExpression::no_filter();

        let requested_ranges = [150..151, 6_350..6_351];
        scheduler
            .initialize(Some(&requested_ranges), &filter, &context)
            .await
            .unwrap();
        let built_pages = |scheduler: &StructuralPrimitiveFieldScheduler| {
            scheduler
                .page_schedulers
                .iter()
                .map(|page| page.page_index)
                .collect::<Vec<_>>()
        };
        assert_eq!(built_pages(&scheduler), [1, 63]);
        assert_eq!(scheduler.page_schedulers[1].row_range, 6_300..6_400);
        assert!(scheduler.page_source.is_some());

        {
            let mut job = scheduler
                .schedule_ranges(&requested_ranges, &filter)
                .unwrap();
            // Both pages fit one read batch, so one call schedules them together.
            assert_eq!(job.schedule_next(&mut context).unwrap().len(), 2);
            assert!(job.schedule_next(&mut context).unwrap().is_empty());

            // Rows outside the initialized ranges must fail cleanly, not panic
            // on an uninitialized page.
            let mut job = scheduler.schedule_ranges(&[0..1], &filter).unwrap();
            let error = job.schedule_next(&mut context).unwrap_err();
            assert!(matches!(error, lance_core::Error::Internal { .. }));
            assert!(error.to_string().contains("not initialized"));
        }

        // Widening the request builds only the new pages and keeps page order.
        scheduler
            .initialize(Some(&[0..250]), &filter, &context)
            .await
            .unwrap();
        assert_eq!(built_pages(&scheduler), [0, 1, 2, 63]);

        // The eager path builds every page and releases the page source.
        scheduler.initialize(None, &filter, &context).await.unwrap();
        assert_eq!(scheduler.page_schedulers.len(), NUM_PAGES as usize);
        assert!(scheduler.page_source.is_none());
        let mut job = scheduler.schedule_ranges(&[0..1], &filter).unwrap();
        assert_eq!(job.schedule_next(&mut context).unwrap().len(), 1);
    }

    /// Records every `submit_request` (ranges and priority) and answers each
    /// range with a zero buffer that starts with the range's offset, so a test
    /// can count requests and check that every reader got its own ranges back.
    #[derive(Debug, Default)]
    struct RecordingScheduler {
        requests: std::sync::Mutex<Vec<Vec<Range<u64>>>>,
        priorities: std::sync::Mutex<Vec<u64>>,
    }

    impl crate::EncodingsIo for RecordingScheduler {
        fn submit_request(
            &self,
            ranges: Vec<Range<u64>>,
            priority: u64,
        ) -> futures::future::BoxFuture<'static, crate::Result<Vec<Bytes>>> {
            use futures::FutureExt;

            let buffers = ranges
                .iter()
                .map(|range| {
                    let mut buffer = vec![0u8; (range.end - range.start) as usize];
                    let marker = range.start.to_le_bytes();
                    let marker_len = marker.len().min(buffer.len());
                    buffer[..marker_len].copy_from_slice(&marker[..marker_len]);
                    Bytes::from(buffer)
                })
                .collect();
            self.requests.lock().unwrap().push(ranges);
            self.priorities.lock().unwrap().push(priority);
            std::future::ready(Ok(buffers)).boxed()
        }
    }

    #[tokio::test]
    async fn page_read_batch_submits_pending_reads_as_one_request() {
        let io = Arc::new(RecordingScheduler::default());
        let batch = RequestBatch::new(io.clone());
        let first = batch.submit_request(vec![100..108, 300..308], 7);
        let second = batch.submit_request(vec![200..208], 3);
        assert_eq!(batch.pending_bytes(), 24);
        assert!(
            io.requests.lock().unwrap().is_empty(),
            "nothing is read before the flush"
        );

        batch.flush();
        {
            let requests = io.requests.lock().unwrap();
            assert_eq!(requests.len(), 1, "the batch is one request");
            assert_eq!(
                requests[0],
                vec![100..108, 200..208, 300..308],
                "the batch is sorted by file offset"
            );
        }
        assert_eq!(
            io.priorities.lock().unwrap()[0],
            3,
            "the batch takes the lowest priority"
        );
        let first = first.await.unwrap();
        assert_eq!(first.len(), 2);
        assert_eq!(first[0].as_ref(), &100_u64.to_le_bytes());
        assert_eq!(first[1].as_ref(), &300_u64.to_le_bytes());
        let second = second.await.unwrap();
        assert_eq!(second.len(), 1);
        assert_eq!(second[0].as_ref(), &200_u64.to_le_bytes());

        // A read submitted after the flush is batched until it is awaited.
        let late = batch.submit_request(vec![400..408], 9).await.unwrap();
        assert_eq!(late[0].as_ref(), &400_u64.to_le_bytes());
        assert_eq!(io.requests.lock().unwrap().len(), 2);

        // Queued reads that would exceed the budget are submitted first, so
        // no batch grows past it.
        let io = Arc::new(RecordingScheduler::default());
        let batch = RequestBatch::new(io.clone());
        let budget = crate::decoder::IO_REQUEST_BATCH_BYTES;
        let first = batch.submit_request(vec![0..budget], 1);
        assert!(io.requests.lock().unwrap().is_empty());
        let second = batch.submit_request(vec![budget..(budget + 8)], 2);
        assert_eq!(
            io.requests.lock().unwrap().as_slice(),
            &[vec![0..budget]],
            "the full batch is submitted before the read that would overflow it"
        );
        batch.flush();
        assert_eq!(io.requests.lock().unwrap().len(), 2);
        assert_eq!(first.await.unwrap()[0].len(), budget as usize);
        assert_eq!(second.await.unwrap()[0].as_ref(), &budget.to_le_bytes());
    }

    #[tokio::test]
    async fn request_batch_flush_keeps_collecting_and_awaiting_flushes() {
        let io = Arc::new(RecordingScheduler::default());
        let batch = RequestBatch::new(io.clone());
        let first = batch.submit_request(vec![100..108], 1);
        let second = batch.submit_request(vec![300..308], 2);
        batch.flush();
        assert_eq!(
            io.requests.lock().unwrap().as_slice(),
            &[vec![100..108, 300..308]],
            "a flush submits the collected reads as one request"
        );
        // The batch keeps collecting after a flush.
        let third = batch.submit_request(vec![500..508], 3);
        let fourth = batch.submit_request(vec![700..708], 4);
        assert_eq!(batch.pending_bytes(), 16);
        assert_eq!(io.requests.lock().unwrap().len(), 1);
        // Awaiting a read submits whatever is queued, so a read queued outside
        // a scheduling step (indirect I/O) is never stuck behind a missing flush.
        assert_eq!(third.await.unwrap()[0].as_ref(), &500_u64.to_le_bytes());
        assert_eq!(
            io.requests.lock().unwrap().as_slice(),
            &[vec![100..108, 300..308], vec![500..508, 700..708]]
        );
        assert_eq!(fourth.await.unwrap()[0].as_ref(), &700_u64.to_le_bytes());
        assert_eq!(first.await.unwrap()[0].as_ref(), &100_u64.to_le_bytes());
        assert_eq!(second.await.unwrap()[0].as_ref(), &300_u64.to_le_bytes());
        assert_eq!(io.requests.lock().unwrap().len(), 2);

        // Futures that submit when first polled and are polled in the same
        // pass (struct children initializing, list columns fetching items)
        // end up in one request: the flush waits for one yield.
        let (fifth, sixth) = futures::join!(
            async { batch.submit_request(vec![900..908], 5).await.unwrap() },
            async { batch.submit_request(vec![1100..1108], 6).await.unwrap() },
        );
        assert_eq!(fifth[0].as_ref(), &900_u64.to_le_bytes());
        assert_eq!(sixth[0].as_ref(), &1100_u64.to_le_bytes());
        assert_eq!(
            io.requests.lock().unwrap().last().unwrap(),
            &vec![900..908, 1100..1108],
            "reads submitted by sibling futures share one request"
        );
        assert_eq!(io.requests.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn schedule_next_batches_page_reads_up_to_the_budget() {
        use super::{
            CachedPageData, IO_REQUEST_BATCH_BYTES, PageInfoAndScheduler, PageInitialization,
            PageInitializationBuffers, PageLoadTask, StructuralPageScheduler,
            StructuralPrimitiveFieldScheduler,
        };
        use crate::decoder::{FilterExpression, SchedulerContext, StructuralFieldScheduler};

        /// A page that reads `data_ranges`, one shard (and one load task) per
        /// range. The bytes it gets back are covered by the batch test, so its
        /// load futures just fail and the test never has to build a decoder.
        #[derive(Debug)]
        struct FakeDataPage {
            data_ranges: Vec<Range<u64>>,
        }

        impl StructuralPageScheduler for FakeDataPage {
            fn needs_initialization(&self) -> bool {
                false
            }

            fn init_layout(&self) -> crate::Result<PageInitialization> {
                unreachable!("fake data pages need no initialization")
            }

            fn init_from_buffers<'a>(
                &'a mut self,
                _buffers: PageInitializationBuffers,
                _io: &Arc<dyn EncodingsIo>,
            ) -> futures::future::BoxFuture<'a, crate::Result<Arc<dyn CachedPageData>>>
            {
                unreachable!("fake data pages need no initialization")
            }

            fn try_load(&mut self, _data: &Arc<dyn CachedPageData>) -> crate::Result<()> {
                Ok(())
            }

            fn schedule_ranges(
                &self,
                ranges: &[Range<u64>],
                io: &Arc<dyn EncodingsIo>,
            ) -> crate::Result<Vec<PageLoadTask>> {
                use futures::FutureExt;

                let num_rows = ranges.iter().map(|r| r.end - r.start).sum();
                Ok(self
                    .data_ranges
                    .iter()
                    .enumerate()
                    .map(|(shard, range)| {
                        drop(io.submit_request(vec![range.clone()], range.start));
                        PageLoadTask {
                            decoder_fut: std::future::ready(Err(lance_core::Error::internal(
                                "fake page",
                            )))
                            .boxed(),
                            // The page's rows are reported once, on the first shard.
                            num_rows: if shard == 0 { num_rows } else { 0 },
                        }
                    })
                    .collect())
            }
        }

        fn scheduler_with_pages(
            page_bytes: u64,
            num_pages: u64,
        ) -> StructuralPrimitiveFieldScheduler {
            // Leave a gap between pages so the test can tell batching from
            // the I/O layer's coalescing of adjacent ranges.
            let stride = page_bytes * 2;
            let pages = (0..num_pages)
                .map(|page| PageInfoAndScheduler {
                    page_index: page as usize,
                    row_range: (page * 10)..((page + 1) * 10),
                    scheduler: Box::new(FakeDataPage {
                        data_ranges: vec![(page * stride)..(page * stride + page_bytes)],
                    }),
                })
                .collect();
            StructuralPrimitiveFieldScheduler::from_page_schedulers(pages, 0, Arc::from("test"))
        }

        let filter = FilterExpression::no_filter();
        let cache = Arc::new(lance_core::cache::LanceCache::no_cache());

        // Small pages: every page of the request is read in one batch.
        let io = Arc::new(RecordingScheduler::default());
        let scheduler = scheduler_with_pages(64, 6);
        let mut context = SchedulerContext::new(io.clone(), cache.clone());
        let mut job = scheduler.schedule_ranges(&[5..45], &filter).unwrap();
        let scan_lines = job.schedule_next(&mut context).unwrap();
        context.flush_io();
        assert_eq!(scan_lines.len(), 5, "one scan line per touched page");
        assert_eq!(
            scan_lines
                .iter()
                .map(|line| line.rows_scheduled)
                .sum::<u64>(),
            40
        );
        assert!(job.schedule_next(&mut context).unwrap().is_empty());
        {
            let requests = io.requests.lock().unwrap();
            assert_eq!(requests.len(), 1, "five pages cost one request");
            assert_eq!(
                requests[0],
                vec![0..64, 128..192, 256..320, 384..448, 512..576],
                "the untouched last page is not read"
            );
        }
        assert_eq!(
            io.priorities.lock().unwrap()[0],
            0,
            "the batch carries the first page's priority"
        );

        // Pages as large as the batch budget are read one request at a time.
        let io = Arc::new(RecordingScheduler::default());
        let scheduler = scheduler_with_pages(IO_REQUEST_BATCH_BYTES, 2);
        let mut context = SchedulerContext::new(io.clone(), cache);
        let mut job = scheduler.schedule_ranges(&[0..20], &filter).unwrap();
        let first = job.schedule_next(&mut context).unwrap();
        context.flush_io();
        assert_eq!(first.len(), 1);
        assert_eq!(io.requests.lock().unwrap().len(), 1);
        let second = job.schedule_next(&mut context).unwrap();
        context.flush_io();
        assert_eq!(second.len(), 1);
        assert_eq!(io.requests.lock().unwrap().len(), 2);
        assert!(job.schedule_next(&mut context).unwrap().is_empty());

        // A page that shards its own reads (like a blob page) keeps every
        // shard in a bounded read instead of one request for the whole page.
        let io = Arc::new(RecordingScheduler::default());
        let shard = IO_REQUEST_BATCH_BYTES;
        let scheduler = StructuralPrimitiveFieldScheduler::from_page_schedulers(
            vec![PageInfoAndScheduler {
                page_index: 0,
                row_range: 0..10,
                scheduler: Box::new(FakeDataPage {
                    data_ranges: vec![0..shard, (2 * shard)..(3 * shard)],
                }),
            }],
            0,
            Arc::from("test"),
        );
        let mut context = SchedulerContext::new(
            io.clone(),
            Arc::new(lance_core::cache::LanceCache::no_cache()),
        );
        let mut job = scheduler.schedule_ranges(&[0..10], &filter).unwrap();
        let scan_lines = job.schedule_next(&mut context).unwrap();
        context.flush_io();
        assert_eq!(scan_lines.len(), 2, "one scan line per shard");
        assert_eq!(
            io.requests.lock().unwrap().as_slice(),
            &[vec![0..shard], vec![(2 * shard)..(3 * shard)]],
            "each shard stays its own bounded request"
        );
        assert!(job.schedule_next(&mut context).unwrap().is_empty());
    }

    #[test]
    fn page_range_mapping_validates_and_splits_ranges() {
        // Exclusive end rows of three ten-row pages.
        let pages = [10_u64, 20, 30];

        let mappings = map_ranges_to_pages(&pages, &[8..22]).unwrap();
        assert_eq!(mappings.len(), 3);
        assert_eq!(mappings[0].page_idx, 0);
        assert_eq!(mappings[0].ranges_in_page, [8..10]);
        assert_eq!(mappings[1].page_idx, 1);
        assert_eq!(mappings[1].ranges_in_page, [0..10]);
        assert_eq!(mappings[2].page_idx, 2);
        assert_eq!(mappings[2].ranges_in_page, [0..2]);

        let sparse_mappings = map_ranges_to_pages(&pages, &[1..2, 3..4, 28..29]).unwrap();
        assert_eq!(sparse_mappings.len(), 2);
        assert_eq!(sparse_mappings[0].page_idx, 0);
        assert_eq!(sparse_mappings[0].ranges_in_page, [1..2, 3..4]);
        assert_eq!(sparse_mappings[1].page_idx, 2);
        assert_eq!(sparse_mappings[1].ranges_in_page, [8..9]);
        assert_eq!(
            pages_overlapping_ranges(&pages, &[1..2, 3..4, 28..29]).unwrap(),
            [0, 2]
        );

        for (ranges, expected_message) in [
            (vec![Range { start: 2, end: 1 }], "is reversed"),
            (vec![15..16, 5..6], "not strictly ordered"),
            (vec![5..15, 10..20], "overlap"),
            (vec![5..5, 5..6], "not strictly ordered"),
        ] {
            let error = map_ranges_to_pages(&pages, &ranges).unwrap_err();
            assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
            assert!(error.to_string().contains(expected_message));
        }
    }

    #[tokio::test]
    async fn test_fullzip_read_source_slices_prefetched_page() {
        let page_start = 200_u64;
        let page_data = LanceBuffer::copy_slice(&[0, 1, 2, 3, 4, 5, 6, 7]);
        let source = FullZipReadSource::PrefetchedPage {
            base_offset: page_start,
            data: page_data,
        };
        let ranges = vec![
            page_start..(page_start + 3),
            (page_start + 4)..(page_start + 8),
        ];
        let mut data = source.fetch(&ranges, 0).await.unwrap();
        assert_eq!(data.pop_front().unwrap().as_ref(), &[0, 1, 2]);
        assert_eq!(data.pop_front().unwrap().as_ref(), &[4, 5, 6, 7]);
    }

    #[tokio::test]
    async fn test_fullzip_initialize_caches_rep_index_when_enabled() {
        use futures::{FutureExt, future::BoxFuture};
        use std::ops::Range;
        use std::sync::Mutex;

        #[derive(Debug, Clone)]
        struct RecordingScheduler {
            data: bytes::Bytes,
            requests: Arc<Mutex<Vec<Vec<Range<u64>>>>>,
        }

        impl RecordingScheduler {
            fn new(data: bytes::Bytes) -> Self {
                Self {
                    data,
                    requests: Arc::new(Mutex::new(Vec::new())),
                }
            }

            fn requests(&self) -> Vec<Vec<Range<u64>>> {
                self.requests.lock().unwrap().clone()
            }
        }

        impl crate::EncodingsIo for RecordingScheduler {
            fn submit_request(
                &self,
                ranges: Vec<Range<u64>>,
                _priority: u64,
            ) -> BoxFuture<'static, crate::Result<Vec<bytes::Bytes>>> {
                self.requests.lock().unwrap().push(ranges.clone());
                let data = ranges
                    .into_iter()
                    .map(|range| self.data.slice(range.start as usize..range.end as usize))
                    .collect::<Vec<_>>();
                std::future::ready(Ok(data)).boxed()
            }
        }

        #[derive(Debug)]
        struct TestFixedDecompressor;

        impl FixedPerValueDecompressor for TestFixedDecompressor {
            fn decompress(
                &self,
                _data: FixedWidthDataBlock,
                _num_rows: u64,
            ) -> crate::Result<DataBlock> {
                unimplemented!("Test decompressor")
            }

            fn bits_per_value(&self) -> u64 {
                32
            }
        }

        let rows_in_page = 100_u64;
        let bytes_per_value = 4_u64;
        let rep_start = 1000_u64;
        let rep_size = ((rows_in_page + 1) * bytes_per_value) as usize;
        let mut data = vec![0_u8; 16 * 1024];
        data[rep_start as usize..rep_start as usize + rep_size].fill(7);
        let io = Arc::new(RecordingScheduler::new(bytes::Bytes::from(data)));

        let mut scheduler = FullZipScheduler {
            data_buf_position: 0,
            data_buf_size: 4096,
            rep_index: Some(FullZipRepIndexDetails {
                buf_position: rep_start,
                bytes_per_value,
            }),
            priority: 0,
            rows_in_page,
            bits_per_offset: 32,
            details: Arc::new(FullZipDecodeDetails {
                value_decompressor: PerValueDecompressor::Fixed(Arc::new(TestFixedDecompressor)),
                def_meaning: Arc::new([crate::repdef::DefinitionInterpretation::NullableItem]),
                ctrl_word_parser: crate::repdef::ControlWordParser::new(0, 1),
                max_rep: 0,
                max_visible_def: 0,
            }),
            cached_state: None,
            enable_cache: true,
        };

        let io_dyn: Arc<dyn crate::EncodingsIo> = io.clone();
        let initialization = scheduler.init_layout().unwrap();
        let ranges = initialization
            .required_ranges()
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(
            ranges,
            vec![rep_start..(rep_start + (rows_in_page + 1) * bytes_per_value)]
        );
        // The field scheduler fetches the declared ranges (coalesced across all
        // pages) and hands the bytes back to the page to finish initialization.
        let buffers = io_dyn.submit_request(ranges, 0).await.unwrap();
        let buffers = initialization.with_buffers(buffers).unwrap();
        let cached_data = scheduler.init_from_buffers(buffers, &io_dyn).await.unwrap();
        assert!(
            cached_data
                .clone()
                .as_arc_any()
                .downcast_ref::<FullZipCacheableState>()
                .is_some()
        );
        assert!(scheduler.cached_state.is_none());
        scheduler.try_load(&cached_data).unwrap();
        assert!(scheduler.cached_state.is_some());
        assert_eq!(
            io.requests(),
            vec![vec![
                rep_start..(rep_start + (rows_in_page + 1) * bytes_per_value)
            ]]
        );
    }

    #[tokio::test]
    async fn test_fullzip_full_page_bypasses_rep_index_io() {
        use futures::{FutureExt, future::BoxFuture};
        use std::ops::Range;
        use std::sync::Mutex;

        #[derive(Debug, Clone)]
        struct RecordingScheduler {
            data: bytes::Bytes,
            requests: Arc<Mutex<Vec<Vec<Range<u64>>>>>,
        }

        impl RecordingScheduler {
            fn new(data: bytes::Bytes) -> Self {
                Self {
                    data,
                    requests: Arc::new(Mutex::new(Vec::new())),
                }
            }

            fn requests(&self) -> Vec<Vec<Range<u64>>> {
                self.requests.lock().unwrap().clone()
            }
        }

        impl crate::EncodingsIo for RecordingScheduler {
            fn submit_request(
                &self,
                ranges: Vec<Range<u64>>,
                _priority: u64,
            ) -> BoxFuture<'static, crate::Result<Vec<bytes::Bytes>>> {
                self.requests.lock().unwrap().push(ranges.clone());
                let data = ranges
                    .into_iter()
                    .map(|range| self.data.slice(range.start as usize..range.end as usize))
                    .collect::<Vec<_>>();
                std::future::ready(Ok(data)).boxed()
            }
        }

        #[derive(Debug)]
        struct TestFixedDecompressor;

        impl FixedPerValueDecompressor for TestFixedDecompressor {
            fn decompress(
                &self,
                _data: FixedWidthDataBlock,
                _num_rows: u64,
            ) -> crate::Result<DataBlock> {
                unimplemented!("Test decompressor")
            }

            fn bits_per_value(&self) -> u64 {
                32
            }
        }

        let rows_in_page = 100_u64;
        let data_start = 256_u64;
        let data_size = 500_u64;
        let rep_start = 4096_u64;
        let bytes_per_value = 4_u64;

        let mut bytes = vec![0_u8; 16 * 1024];
        for i in 0..=rows_in_page {
            let offset = (i * 5) as u32;
            let pos = rep_start as usize + (i * bytes_per_value) as usize;
            bytes[pos..pos + 4].copy_from_slice(&offset.to_le_bytes());
        }
        let io = Arc::new(RecordingScheduler::new(bytes::Bytes::from(bytes)));

        let scheduler = FullZipScheduler {
            data_buf_position: data_start,
            data_buf_size: data_size,
            rep_index: Some(FullZipRepIndexDetails {
                buf_position: rep_start,
                bytes_per_value,
            }),
            priority: 0,
            rows_in_page,
            bits_per_offset: 32,
            details: Arc::new(FullZipDecodeDetails {
                value_decompressor: PerValueDecompressor::Fixed(Arc::new(TestFixedDecompressor)),
                def_meaning: Arc::new([crate::repdef::DefinitionInterpretation::NullableItem]),
                ctrl_word_parser: crate::repdef::ControlWordParser::new(0, 1),
                max_rep: 0,
                max_visible_def: 0,
            }),
            cached_state: None,
            enable_cache: false,
        };

        let io_dyn: Arc<dyn crate::EncodingsIo> = io.clone();
        let tasks = scheduler
            .schedule_ranges_rep(
                &[0..rows_in_page],
                &io_dyn,
                FullZipRepIndexDetails {
                    buf_position: rep_start,
                    bytes_per_value,
                },
            )
            .unwrap();

        let requests = io.requests();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0], vec![data_start..(data_start + data_size)]);

        let _ = tasks.into_iter().next().unwrap().decoder_fut.await.unwrap();
        let requests_after_await = io.requests();
        assert_eq!(
            requests_after_await.len(),
            1,
            "full page path should not issue rep-index I/O"
        );
    }

    /// This test is used to reproduce fuzz test https://github.com/lancedb/lance/issues/4492
    #[tokio::test]
    async fn test_fuzz_issue_4492_empty_rep_values() {
        use lance_datagen::{RowCount, Seed, array, gen_batch};

        let seed = 1823859942947654717u64;
        let num_rows = 2741usize;

        // Generate the exact same data that caused the failure
        let batch_gen = gen_batch().with_seed(Seed::from(seed));
        let base_generator = array::rand_type(&DataType::FixedSizeBinary(32));
        let list_generator = array::rand_list_any(base_generator, false);

        let batch = batch_gen
            .anon_col(list_generator)
            .into_batch_rows(RowCount::from(num_rows as u64))
            .unwrap();

        let list_array = batch.column(0).clone();

        // Force miniblock encoding
        let mut metadata = HashMap::new();
        metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            STRUCTURAL_ENCODING_MINIBLOCK.to_string(),
        );

        let test_cases = TestCases::default()
            .with_structural_encodings()
            .with_batch_size(100)
            .with_range(0..num_rows.min(500) as u64)
            .with_indices(vec![0, num_rows as u64 / 2, (num_rows - 1) as u64]);

        check_round_trip_encoding_of_data(vec![list_array], &test_cases, metadata).await
    }

    async fn test_minichunk_size_helper(
        string_data: Vec<Option<String>>,
        minichunk_size: u64,
        encodings: &[TestEncoding],
    ) {
        use crate::constants::MINICHUNK_SIZE_META_KEY;
        use crate::testing::{TestCases, check_round_trip_encoding_of_data};
        use arrow_array::{ArrayRef, StringArray};
        use std::sync::Arc;

        let string_array: ArrayRef = Arc::new(StringArray::from(string_data));

        let mut metadata = HashMap::new();
        metadata.insert(
            MINICHUNK_SIZE_META_KEY.to_string(),
            minichunk_size.to_string(),
        );
        metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            STRUCTURAL_ENCODING_MINIBLOCK.to_string(),
        );

        let test_cases = TestCases::default()
            .with_encodings(encodings.iter().copied())
            .with_batch_size(1000);

        check_round_trip_encoding_of_data(vec![string_array], &test_cases, metadata).await;
    }

    #[tokio::test]
    async fn test_minichunk_size_roundtrip() {
        // Test that minichunk size can be configured and works correctly in round-trip encoding
        let mut string_data = Vec::new();
        for i in 0..100 {
            string_data.push(Some(format!("test_string_{}", i).repeat(50)));
        }
        // configure minichunk size to 64 bytes (smaller than the default 4kb) for Lance 2.1
        test_minichunk_size_helper(
            string_data,
            64,
            &[
                TestEncoding::StructuralU16,
                TestEncoding::StructuralU32,
                TestEncoding::StructuralSparse,
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_minichunk_size_128kb_v2_2() {
        // Test that minichunk size can be configured to 128KB and works correctly with Lance 2.2
        let mut string_data = Vec::new();
        // create a 500kb string array
        for i in 0..10000 {
            string_data.push(Some(format!("test_string_{}", i).repeat(50)));
        }
        test_minichunk_size_helper(
            string_data,
            128 * 1024,
            &[TestEncoding::StructuralU32, TestEncoding::StructuralSparse],
        )
        .await;
    }

    #[tokio::test]
    async fn test_binary_large_minichunk_size_over_max_miniblock_values() {
        let mut string_data = Vec::new();
        // 128kb/chunk / 6 bytes (t_9999) = 21845 items per chunk
        for i in 0..10000 {
            string_data.push(Some(format!("t_{}", i)));
        }
        test_minichunk_size_helper(
            string_data,
            128 * 1024,
            &[TestEncoding::StructuralU32, TestEncoding::StructuralSparse],
        )
        .await;
    }

    #[tokio::test]
    async fn test_large_dictionary_general_compression() {
        use arrow_array::{ArrayRef, StringArray};
        use std::collections::HashMap;
        use std::sync::Arc;

        // Create large string dictionary data (>32KiB) with low cardinality
        // Use 100 unique strings, each 500 bytes long = 50KB dictionary
        let unique_values: Vec<String> = (0..100)
            .map(|i| format!("value_{:04}_{}", i, "x".repeat(500)))
            .collect();

        // Repeat these strings many times to create a large array
        let repeated_strings: Vec<_> = unique_values
            .iter()
            .cycle()
            .take(10_000)
            .map(|s| Some(s.as_str()))
            .collect();

        let string_array = Arc::new(StringArray::from(repeated_strings)) as ArrayRef;

        // Configure test to use V2_2 and verify encoding
        let test_cases = TestCases::default()
            .with_u32_structural_encodings()
            .with_verify_encoding(Arc::new(|cols: &[crate::encoder::EncodedColumn], _| {
                assert_eq!(cols.len(), 1);
                let col = &cols[0];

                // Navigate to the dictionary encoding in the page layout
                if let Some(PageEncoding::Structural(page_layout)) =
                    &col.final_pages.first().map(|p| &p.description)
                    && let Some(pb21::page_layout::Layout::MiniBlockLayout(mini_block)) =
                        &page_layout.layout
                    && let Some(dictionary_encoding) = &mini_block.dictionary
                {
                    match dictionary_encoding.compression.as_ref() {
                        Some(Compression::General(general)) => {
                            // Verify it's using LZ4 or Zstd
                            let compression = general.compression.as_ref().unwrap();
                            assert!(
                                compression.scheme()
                                    == pb21::CompressionScheme::CompressionAlgorithmLz4
                                    || compression.scheme()
                                        == pb21::CompressionScheme::CompressionAlgorithmZstd,
                                "Expected LZ4 or Zstd compression for large dictionary"
                            );
                        }
                        _ => panic!("Expected General compression for large dictionary"),
                    }
                }
            }));

        check_round_trip_encoding_of_data(vec![string_array], &test_cases, HashMap::new()).await;
    }

    fn dictionary_encoding_from_page(
        page: &crate::encoder::EncodedPage,
    ) -> &crate::format::pb21::CompressiveEncoding {
        let PageEncoding::Structural(layout) = &page.description else {
            panic!("Expected structural page encoding");
        };
        let pb21::page_layout::Layout::MiniBlockLayout(layout) = layout.layout.as_ref().unwrap()
        else {
            panic!("Expected mini-block layout");
        };
        layout
            .dictionary
            .as_ref()
            .unwrap_or_else(|| panic!("Expected dictionary encoding"))
    }

    async fn encode_variable_dict_page(
        metadata: HashMap<String, String>,
    ) -> crate::encoder::EncodedPage {
        use arrow_array::types::Int32Type;
        use arrow_array::{ArrayRef, DictionaryArray, Int32Array, StringArray};

        let values = Arc::new(StringArray::from(
            (0..128)
                .map(|i| format!("value_{i:04}_{}", "x".repeat(256)))
                .collect::<Vec<_>>(),
        )) as ArrayRef;
        let keys = Int32Array::from_iter_values((0..20_000).map(|i| i % 128));
        let dict_array =
            Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap()) as ArrayRef;

        let field = arrow_schema::Field::new(
            "dict_col",
            DataType::Dictionary(Box::new(DataType::Int32), Box::new(DataType::Utf8)),
            false,
        )
        .with_metadata(metadata);

        encode_first_page(field, dict_array, TestEncoding::StructuralU32).await
    }

    async fn encode_auto_fixed_dict_page(
        metadata: HashMap<String, String>,
    ) -> crate::encoder::EncodedPage {
        use arrow_array::{ArrayRef, Decimal128Array};

        // 128-bit fixed-width values with low cardinality to trigger dictionary encoding.
        let values = (0..20_000)
            .map(|i| match i % 3 {
                0 => 10_i128,
                1 => 20_i128,
                _ => 30_i128,
            })
            .collect::<Vec<_>>();
        let decimal = Decimal128Array::from_iter_values(values)
            .with_precision_and_scale(38, 0)
            .unwrap();
        let decimal = Arc::new(decimal) as ArrayRef;

        let mut field_metadata = metadata;
        // Strongly encourage dictionary encoding for this synthetic test data.
        field_metadata.insert(
            "lance-encoding:dict-size-ratio".to_string(),
            "0.99".to_string(),
        );
        let field = arrow_schema::Field::new("fixed_col", DataType::Decimal128(38, 0), false)
            .with_metadata(field_metadata);

        encode_first_page(field, decimal, TestEncoding::StructuralU32).await
    }

    #[tokio::test]
    async fn test_dict_values_general_compression_default_lz4_for_variable_dict_values() {
        let page = encode_variable_dict_page(HashMap::new()).await;
        let dictionary_encoding = dictionary_encoding_from_page(&page);
        let Some(Compression::General(general)) = dictionary_encoding.compression.as_ref() else {
            panic!("Expected General compression for dictionary values");
        };
        let compression = general.compression.as_ref().unwrap();
        assert_eq!(
            compression.scheme(),
            pb21::CompressionScheme::CompressionAlgorithmLz4
        );
    }

    #[tokio::test]
    async fn test_dict_values_general_compression_default_lz4_for_fixed_dict_values() {
        let page = encode_auto_fixed_dict_page(HashMap::new()).await;
        let dictionary_encoding = dictionary_encoding_from_page(&page);
        let Some(Compression::General(general)) = dictionary_encoding.compression.as_ref() else {
            panic!("Expected General compression for dictionary values");
        };
        let compression = general.compression.as_ref().unwrap();
        assert_eq!(
            compression.scheme(),
            pb21::CompressionScheme::CompressionAlgorithmLz4
        );
    }

    #[tokio::test]
    async fn test_dict_values_general_compression_zstd() {
        let mut metadata = HashMap::new();
        metadata.insert(
            DICT_VALUES_COMPRESSION_META_KEY.to_string(),
            "zstd".to_string(),
        );
        let page = encode_variable_dict_page(metadata).await;
        let dictionary_encoding = dictionary_encoding_from_page(&page);
        let Some(Compression::General(general)) = dictionary_encoding.compression.as_ref() else {
            panic!("Expected General compression for dictionary values");
        };
        let compression = general.compression.as_ref().unwrap();
        assert_eq!(
            compression.scheme(),
            pb21::CompressionScheme::CompressionAlgorithmZstd
        );
    }

    #[tokio::test]
    async fn test_dict_values_general_compression_none() {
        let mut metadata = HashMap::new();
        metadata.insert(
            DICT_VALUES_COMPRESSION_META_KEY.to_string(),
            "none".to_string(),
        );
        let page = encode_variable_dict_page(metadata).await;
        let dictionary_encoding = dictionary_encoding_from_page(&page);
        assert!(
            !matches!(
                dictionary_encoding.compression.as_ref(),
                Some(Compression::General(_))
            ),
            "Expected dictionary values to avoid General compression"
        );
    }

    #[test]
    fn test_resolve_dict_values_compression_metadata_defaults_to_lz4() {
        let metadata = PrimitiveStructuralEncoder::resolve_dict_values_compression_metadata(
            &HashMap::new(),
            None,
            None,
        );
        assert_eq!(metadata.get(COMPRESSION_META_KEY), Some(&"lz4".to_string()),);
        assert!(!metadata.contains_key(COMPRESSION_LEVEL_META_KEY));
    }

    #[test]
    fn test_resolve_dict_values_compression_metadata_metadata_overrides_env() {
        let field_metadata = HashMap::from([
            (
                DICT_VALUES_COMPRESSION_META_KEY.to_string(),
                "none".to_string(),
            ),
            (
                DICT_VALUES_COMPRESSION_LEVEL_META_KEY.to_string(),
                "7".to_string(),
            ),
        ]);
        let metadata = PrimitiveStructuralEncoder::resolve_dict_values_compression_metadata(
            &field_metadata,
            Some("zstd".to_string()),
            Some("3".to_string()),
        );
        assert_eq!(
            metadata.get(COMPRESSION_META_KEY),
            Some(&"none".to_string()),
        );
        assert_eq!(
            metadata.get(COMPRESSION_LEVEL_META_KEY),
            Some(&"7".to_string()),
        );
    }

    #[test]
    fn test_resolve_dict_values_compression_metadata_env_fallback() {
        let metadata = PrimitiveStructuralEncoder::resolve_dict_values_compression_metadata(
            &HashMap::new(),
            Some("zstd".to_string()),
            Some("9".to_string()),
        );
        assert_eq!(
            metadata.get(COMPRESSION_META_KEY),
            Some(&"zstd".to_string()),
        );
        assert_eq!(
            metadata.get(COMPRESSION_LEVEL_META_KEY),
            Some(&"9".to_string()),
        );
    }

    #[tokio::test]
    async fn test_dictionary_encode_int64() {
        use crate::constants::{DICT_SIZE_RATIO_META_KEY, STRUCTURAL_ENCODING_META_KEY};
        use crate::testing::{TestCases, check_round_trip_encoding_of_data};
        use arrow_array::{ArrayRef, Int64Array};
        use std::collections::HashMap;
        use std::sync::Arc;

        // Low cardinality with poor RLE opportunity.
        let values = (0..1000)
            .map(|i| match i % 3 {
                0 => 10i64,
                1 => 20i64,
                _ => 30i64,
            })
            .collect::<Vec<_>>();
        let array = Arc::new(Int64Array::from(values)) as ArrayRef;

        let mut metadata = HashMap::new();
        metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            STRUCTURAL_ENCODING_MINIBLOCK.to_string(),
        );
        metadata.insert(DICT_SIZE_RATIO_META_KEY.to_string(), "0.99".to_string());

        let test_cases = TestCases::default()
            .with_u32_structural_encodings()
            .with_batch_size(1000)
            .with_range(0..1000)
            .with_indices(vec![0, 1, 10, 999])
            .with_expected_encoding("dictionary");

        check_round_trip_encoding_of_data(vec![array], &test_cases, metadata).await;
    }

    #[tokio::test]
    async fn test_dictionary_encode_float64() {
        use crate::constants::{DICT_SIZE_RATIO_META_KEY, STRUCTURAL_ENCODING_META_KEY};
        use crate::testing::{TestCases, check_round_trip_encoding_of_data};
        use arrow_array::{ArrayRef, Float64Array};
        use std::collections::HashMap;
        use std::sync::Arc;

        // Low cardinality with poor RLE opportunity.
        let values = (0..1000)
            .map(|i| match i % 3 {
                0 => 0.1f64,
                1 => 0.2f64,
                _ => 0.3f64,
            })
            .collect::<Vec<_>>();
        let array = Arc::new(Float64Array::from(values)) as ArrayRef;

        let mut metadata = HashMap::new();
        metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            STRUCTURAL_ENCODING_MINIBLOCK.to_string(),
        );
        metadata.insert(DICT_SIZE_RATIO_META_KEY.to_string(), "0.99".to_string());

        let test_cases = TestCases::default()
            .with_u32_structural_encodings()
            .with_batch_size(1000)
            .with_range(0..1000)
            .with_indices(vec![0, 1, 10, 999])
            .with_expected_encoding("dictionary");

        check_round_trip_encoding_of_data(vec![array], &test_cases, metadata).await;
    }

    #[test]
    fn test_miniblock_dictionary_out_of_line_bitpacking_decode() {
        let rows = 10_000;
        let unique_values = 2_000;

        let dictionary_encoding =
            ProtobufUtils21::out_of_line_bitpacking(64, ProtobufUtils21::flat(11, None));
        let layout = pb21::MiniBlockLayout {
            rep_compression: None,
            def_compression: None,
            value_compression: Some(ProtobufUtils21::flat(64, None)),
            dictionary: Some(dictionary_encoding),
            num_dictionary_items: unique_values,
            layers: vec![pb21::RepDefLayer::RepdefAllValidItem as i32],
            num_buffers: 1,
            repetition_index_depth: 0,
            num_items: rows,
            has_large_chunk: false,
        };

        let buffer_offsets_and_sizes = vec![(0, 0), (0, 0), (0, 0)];
        let scheduler = super::MiniBlockScheduler::try_new(
            &buffer_offsets_and_sizes,
            /*priority=*/ 0,
            /*items_in_page=*/ rows,
            &layout,
            &DefaultDecompressionStrategy::default(),
        )
        .unwrap();

        let dictionary = scheduler.dictionary.unwrap();
        assert_eq!(dictionary.num_dictionary_items, unique_values);
        assert_eq!(
            dictionary.dictionary_data_alignment,
            crate::encoder::MIN_PAGE_BUFFER_ALIGNMENT
        );
    }

    // Dictionary encoding decision tests
    fn create_test_fixed_data_block(
        num_values: u64,
        cardinality: u64,
        bits_per_value: u64,
    ) -> DataBlock {
        assert!(cardinality > 0);
        assert!(cardinality <= num_values);
        let block_info = BlockInfo::default();

        assert_eq!(bits_per_value % 8, 0);
        let data = match bits_per_value {
            32 => {
                let values = (0..num_values)
                    .map(|i| (i % cardinality) as u32)
                    .collect::<Vec<_>>();
                crate::buffer::LanceBuffer::reinterpret_vec(values)
            }
            64 => {
                let values = (0..num_values).map(|i| i % cardinality).collect::<Vec<_>>();
                crate::buffer::LanceBuffer::reinterpret_vec(values)
            }
            128 => {
                let values = (0..num_values)
                    .map(|i| (i % cardinality) as u128)
                    .collect::<Vec<_>>();
                crate::buffer::LanceBuffer::reinterpret_vec(values)
            }
            _ => unreachable!(),
        };
        DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value,
            data,
            num_values,
            block_info,
        })
    }

    /// Helper to create VariableWidth (string) test data block with exact cardinality
    fn create_test_variable_width_block(num_values: u64, cardinality: u64) -> DataBlock {
        use arrow_array::StringArray;

        assert!(cardinality <= num_values && cardinality > 0);

        let mut values = Vec::with_capacity(num_values as usize);
        for i in 0..num_values {
            values.push(format!("value_{:016}", i % cardinality));
        }

        let array = StringArray::from(values);
        DataBlock::from_array(Arc::new(array) as ArrayRef)
    }

    fn create_sorted_string_array(num_values: u64, cardinality: u64) -> ArrayRef {
        use arrow_array::StringArray;

        assert!(cardinality <= num_values && cardinality > 0);

        let mut values = Vec::with_capacity(num_values as usize);
        for i in 0..num_values {
            let value_idx = i * cardinality / num_values;
            values.push(format!("value_{:016}", value_idx));
        }

        Arc::new(StringArray::from(values)) as ArrayRef
    }

    fn create_sorted_variable_width_block(num_values: u64, cardinality: u64) -> DataBlock {
        DataBlock::from_array(create_sorted_string_array(num_values, cardinality))
    }

    #[test]
    fn test_should_dictionary_encode() {
        use crate::constants::DICT_SIZE_RATIO_META_KEY;
        use lance_core::datatypes::Field as LanceField;

        // Create data where dict encoding saves space
        let block = create_test_variable_width_block(1000, 10);

        let mut metadata = HashMap::new();
        metadata.insert(DICT_SIZE_RATIO_META_KEY.to_string(), "0.8".to_string());
        let arrow_field =
            arrow_schema::Field::new("test", DataType::Utf8, false).with_metadata(metadata);
        let field = LanceField::try_from(&arrow_field).unwrap();

        let result = PrimitiveStructuralEncoder::should_dictionary_encode(
            &block,
            &field,
            FixedWidthDictionaryEncoding::Exclude64Bit,
        );

        assert!(
            result.is_some(),
            "Should use dictionary encode based on size"
        );
    }

    #[test]
    fn test_block_sampling_detects_low_cardinality_in_short_sorted_runs() {
        let sample_count: usize = 4096;
        let num_values: u64 = 200_000;
        let cardinality: u64 = 8_000;
        let run_length = num_values / cardinality;
        let stride = num_values as usize / sample_count;
        assert!(
            stride > run_length as usize,
            "test must construct the stride > run_length case"
        );

        let block = create_sorted_variable_width_block(num_values, cardinality);
        let sample_unique_ratio =
            PrimitiveStructuralEncoder::sample_unique_ratio(&block, sample_count).unwrap();

        assert!(
            sample_unique_ratio.is_some_and(|ratio| ratio < 0.98),
            "sorted low-cardinality data must not be classified as near-unique"
        );
    }

    #[test]
    fn test_should_dictionary_encode_sorted_low_cardinality() {
        use crate::constants::DICT_SIZE_RATIO_META_KEY;
        use lance_core::datatypes::Field as LanceField;

        let block = create_sorted_variable_width_block(200_000, 8_000);

        let mut metadata = HashMap::new();
        metadata.insert(DICT_SIZE_RATIO_META_KEY.to_string(), "0.8".to_string());
        let arrow_field =
            arrow_schema::Field::new("test", DataType::Utf8, false).with_metadata(metadata);
        let field = LanceField::try_from(&arrow_field).unwrap();

        let result = PrimitiveStructuralEncoder::should_dictionary_encode(
            &block,
            &field,
            FixedWidthDictionaryEncoding::Include64Bit,
        );

        assert!(
            result.is_some(),
            "sorted low-cardinality data should reach dictionary encoding"
        );
    }

    #[test]
    fn test_should_not_dictionary_encode_sorted_high_cardinality_short_runs() {
        use crate::constants::DICT_SIZE_RATIO_META_KEY;
        use lance_core::datatypes::Field as LanceField;

        let num_values = 200_002;
        let cardinality = 100_001;
        let block = create_sorted_variable_width_block(num_values, cardinality);

        let mut metadata = HashMap::new();
        metadata.insert(DICT_SIZE_RATIO_META_KEY.to_string(), "0.8".to_string());
        let arrow_field =
            arrow_schema::Field::new("test", DataType::Utf8, false).with_metadata(metadata);
        let field = LanceField::try_from(&arrow_field).unwrap();

        let result = PrimitiveStructuralEncoder::should_dictionary_encode(
            &block,
            &field,
            FixedWidthDictionaryEncoding::Include64Bit,
        );

        assert!(
            result.is_none(),
            "sorted high-cardinality short runs should not trigger a full dictionary probe"
        );
    }

    #[tokio::test]
    async fn test_encode_sorted_low_cardinality_uses_dictionary_layout() {
        use crate::constants::DICT_SIZE_RATIO_META_KEY;

        let mut metadata = HashMap::new();
        metadata.insert(DICT_SIZE_RATIO_META_KEY.to_string(), "0.8".to_string());
        let field = arrow_schema::Field::new("test", DataType::Utf8, false).with_metadata(metadata);
        let array = create_sorted_string_array(200_000, 8_000);

        let page = encode_first_page(field, array, TestEncoding::StructuralU32).await;
        let _ = dictionary_encoding_from_page(&page);
    }

    #[test]
    fn test_should_not_dictionary_encode_unsupported_bits() {
        use crate::constants::DICT_SIZE_RATIO_META_KEY;
        use lance_core::datatypes::Field as LanceField;

        let block = create_test_fixed_data_block(1000, 1000, 32);

        let mut metadata = HashMap::new();
        metadata.insert(DICT_SIZE_RATIO_META_KEY.to_string(), "0.8".to_string());
        let arrow_field =
            arrow_schema::Field::new("test", DataType::Int32, false).with_metadata(metadata);
        let field = LanceField::try_from(&arrow_field).unwrap();

        let result = PrimitiveStructuralEncoder::should_dictionary_encode(
            &block,
            &field,
            FixedWidthDictionaryEncoding::Exclude64Bit,
        );

        assert!(
            result.is_none(),
            "Should not use dictionary encode for unsupported bit width"
        );
    }

    #[test]
    fn test_should_not_dictionary_encode_near_unique_sample() {
        use crate::constants::DICT_SIZE_RATIO_META_KEY;
        use lance_core::datatypes::Field as LanceField;

        let num_values = 5000;
        let block = create_test_variable_width_block(num_values, num_values);

        let mut metadata = HashMap::new();
        metadata.insert(DICT_SIZE_RATIO_META_KEY.to_string(), "1.0".to_string());
        let arrow_field =
            arrow_schema::Field::new("test", DataType::Utf8, false).with_metadata(metadata);
        let field = LanceField::try_from(&arrow_field).unwrap();

        let result = PrimitiveStructuralEncoder::should_dictionary_encode(
            &block,
            &field,
            FixedWidthDictionaryEncoding::Exclude64Bit,
        );

        assert!(
            result.is_none(),
            "Should not probe dictionary encoding for near-unique data"
        );
    }

    #[test]
    fn test_v2_1_miniblock_serializes_log_num_values_15() {
        let miniblocks = MiniBlockCompressed {
            data: vec![LanceBuffer::from(vec![1_u8; 16])],
            chunks: vec![
                MiniBlockChunk {
                    buffer_sizes: vec![8],
                    log_num_values: 15,
                },
                MiniBlockChunk {
                    buffer_sizes: vec![8],
                    log_num_values: 0,
                },
            ],
            num_values: 32_769,
        };

        let serialized = PrimitiveStructuralEncoder::serialize_miniblocks(
            miniblocks,
            None,
            None,
            MiniblockChunkSize::U16,
        )
        .unwrap();

        let chunk_metadata = serialized.metadata.borrow_to_typed_slice::<u16>();
        assert_eq!(chunk_metadata.len(), 2);
        assert_eq!(
            chunk_metadata[0] & 0x0F,
            15,
            "V2.1 metadata should use all 4 bits for log_num_values"
        );
    }

    async fn encode_first_page(
        field: arrow_schema::Field,
        array: ArrayRef,
        version: TestEncoding,
    ) -> crate::encoder::EncodedPage {
        use crate::repdef::RepDefBuilder;
        use crate::{
            encoder::{
                ColumnIndexSequence, EncodingOptions, MIN_PAGE_BUFFER_ALIGNMENT, OutOfLineBuffers,
            },
            testing::{create_test_field_encoder, test_encoding_strategy},
        };

        let lance_field = lance_core::datatypes::Field::try_from(&field).unwrap();
        let encoding_strategy = test_encoding_strategy(version);
        let mut column_index_seq = ColumnIndexSequence::default();
        let encoding_options = EncodingOptions {
            cache_bytes_per_column: 1,
            max_page_bytes: 32 * 1024 * 1024,
            keep_original_array: true,
            buffer_alignment: MIN_PAGE_BUFFER_ALIGNMENT,
        };

        let mut encoder = create_test_field_encoder(
            encoding_strategy.as_ref(),
            &lance_field,
            &mut column_index_seq,
            &encoding_options,
        )
        .unwrap();

        let mut external_buffers = OutOfLineBuffers::new(0, MIN_PAGE_BUFFER_ALIGNMENT);
        let repdef = RepDefBuilder::default();
        let num_rows = array.len() as u64;
        let mut pages = Vec::new();
        for task in encoder
            .maybe_encode(array, &mut external_buffers, repdef, 0, num_rows)
            .unwrap()
        {
            pages.push(task.await.unwrap());
        }
        for task in encoder.flush(&mut external_buffers).unwrap() {
            pages.push(task.await.unwrap());
        }
        pages.into_iter().next().unwrap()
    }

    #[tokio::test]
    async fn test_constant_layout_out_of_line_fixed_size_binary_v2_2() {
        use crate::format::pb21::page_layout::Layout;

        let val = vec![0xABu8; 33];
        let arr: ArrayRef = Arc::new(
            arrow_array::FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                std::iter::repeat_n(Some(val.as_slice()), 256),
                33,
            )
            .unwrap(),
        );
        let field = arrow_schema::Field::new("c", DataType::FixedSizeBinary(33), true);
        let page = encode_first_page(field, arr.clone(), TestEncoding::StructuralU32).await;

        let PageEncoding::Structural(layout) = &page.description else {
            panic!("Expected structural encoding");
        };
        let Layout::ConstantLayout(layout) = layout.layout.as_ref().unwrap() else {
            panic!("Expected constant layout in slot 2");
        };
        assert!(layout.inline_value.is_none());
        assert_eq!(page.data.len(), 1);

        let test_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralU32)
            .with_page_sizes(vec![4096]);
        check_round_trip_encoding_of_data(vec![arr], &test_cases, HashMap::new()).await;
    }

    #[tokio::test]
    async fn test_constant_layout_out_of_line_utf8_v2_2() {
        use crate::format::pb21::page_layout::Layout;

        let arr: ArrayRef = Arc::new(arrow_array::StringArray::from_iter_values(
            std::iter::repeat_n("hello", 512),
        ));
        let field = arrow_schema::Field::new("c", DataType::Utf8, true);
        let page = encode_first_page(field, arr.clone(), TestEncoding::StructuralU32).await;

        let PageEncoding::Structural(layout) = &page.description else {
            panic!("Expected structural encoding");
        };
        let Layout::ConstantLayout(layout) = layout.layout.as_ref().unwrap() else {
            panic!("Expected constant layout in slot 2");
        };
        assert!(layout.inline_value.is_none());
        assert_eq!(page.data.len(), 1);

        let test_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralU32)
            .with_page_sizes(vec![4096]);
        check_round_trip_encoding_of_data(vec![arr], &test_cases, HashMap::new()).await;
    }

    #[tokio::test]
    async fn test_constant_layout_nullable_item_v2_2() {
        use crate::format::pb21::page_layout::Layout;

        let arr: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![
            Some(7),
            None,
            Some(7),
            None,
            Some(7),
        ]));
        let field = arrow_schema::Field::new("c", DataType::Int32, true);
        let page = encode_first_page(field, arr.clone(), TestEncoding::StructuralU32).await;

        let PageEncoding::Structural(layout) = &page.description else {
            panic!("Expected structural encoding");
        };
        let Layout::ConstantLayout(layout) = layout.layout.as_ref().unwrap() else {
            panic!("Expected constant layout in slot 2");
        };
        assert!(layout.inline_value.is_some());
        assert_eq!(page.data.len(), 2);

        let test_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralU32)
            .with_page_sizes(vec![4096]);
        check_round_trip_encoding_of_data(vec![arr], &test_cases, HashMap::new()).await;
    }

    #[tokio::test]
    async fn test_constant_layout_list_repdef_v2_2() {
        use crate::format::pb21::page_layout::Layout;
        use arrow_array::builder::{Int32Builder, ListBuilder};

        let mut builder = ListBuilder::new(Int32Builder::new());
        builder.values().append_value(7);
        builder.values().append_null();
        builder.values().append_value(7);
        builder.append(true);

        builder.append(true);

        builder.values().append_value(7);
        builder.append(true);

        builder.append_null();

        let arr: ArrayRef = Arc::new(builder.finish());
        let field = arrow_schema::Field::new(
            "c",
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            true,
        );
        let page = encode_first_page(field, arr.clone(), TestEncoding::StructuralU32).await;

        let PageEncoding::Structural(layout) = &page.description else {
            panic!("Expected structural encoding");
        };
        let Layout::ConstantLayout(layout) = layout.layout.as_ref().unwrap() else {
            panic!("Expected constant layout in slot 2");
        };
        assert!(layout.inline_value.is_some());
        assert_eq!(page.data.len(), 2);

        let test_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralU32)
            .with_page_sizes(vec![4096]);
        check_round_trip_encoding_of_data(vec![arr], &test_cases, HashMap::new()).await;
    }

    #[tokio::test]
    async fn test_constant_layout_fixed_size_list_not_used_v2_2() {
        use crate::format::pb21::page_layout::Layout;
        use arrow_array::builder::{FixedSizeListBuilder, Int32Builder};

        let mut builder = FixedSizeListBuilder::new(Int32Builder::new(), 3);
        for _ in 0..64 {
            builder.values().append_value(1);
            builder.values().append_null();
            builder.values().append_value(3);
            builder.append(true);
        }
        let arr: ArrayRef = Arc::new(builder.finish());
        let field = arrow_schema::Field::new(
            "c",
            DataType::FixedSizeList(
                Arc::new(arrow_schema::Field::new("item", DataType::Int32, true)),
                3,
            ),
            true,
        );
        let page = encode_first_page(field, arr.clone(), TestEncoding::StructuralU32).await;

        if let PageEncoding::Structural(layout) = &page.description {
            assert!(
                !matches!(layout.layout.as_ref().unwrap(), Layout::ConstantLayout(_)),
                "FixedSizeList should not use constant layout yet"
            );
        }

        let test_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralU32)
            .with_page_sizes(vec![4096]);
        check_round_trip_encoding_of_data(vec![arr], &test_cases, HashMap::new()).await;
    }

    #[tokio::test]
    async fn test_constant_layout_not_written_before_v2_2() {
        use crate::format::pb21::page_layout::Layout;

        let arr: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![7; 1024]));
        let field = arrow_schema::Field::new("c", DataType::Int32, true);
        let page = encode_first_page(field, arr.clone(), TestEncoding::StructuralU16).await;

        let PageEncoding::Structural(layout) = &page.description else {
            return;
        };
        assert!(
            !matches!(layout.layout.as_ref().unwrap(), Layout::ConstantLayout(_)),
            "Should not emit constant layout before v2.2"
        );

        let test_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralU16)
            .with_page_sizes(vec![4096]);
        check_round_trip_encoding_of_data(vec![arr], &test_cases, HashMap::new()).await;
    }

    #[tokio::test]
    async fn test_all_null_constant_layout_still_works_v2_2() {
        use crate::format::pb21::page_layout::Layout;

        let arr: ArrayRef = Arc::new(arrow_array::Int32Array::from(vec![None, None, None]));
        let field = arrow_schema::Field::new("c", DataType::Int32, true);
        let page = encode_first_page(field, arr.clone(), TestEncoding::StructuralU32).await;

        let PageEncoding::Structural(layout) = &page.description else {
            panic!("Expected structural encoding");
        };
        let Layout::ConstantLayout(layout) = layout.layout.as_ref().unwrap() else {
            panic!("Expected layout in slot 2");
        };
        assert!(layout.inline_value.is_none());
        assert_eq!(page.data.len(), 0);

        let test_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralU32)
            .with_page_sizes(vec![4096]);
        check_round_trip_encoding_of_data(vec![arr], &test_cases, HashMap::new()).await;
    }

    fn hand_built_dictionary_with_out_of_range_null_keys() -> ArrayRef {
        use arrow_array::{DictionaryArray, Int32Array, types::Int32Type};
        use arrow_buffer::NullBuffer;

        let keys = Int32Array::new(
            vec![0, 7, 7].into(),
            Some(NullBuffer::from(vec![true, false, false])),
        );
        let values = Arc::new(StringArray::from(vec!["a"]));
        Arc::new(DictionaryArray::<Int32Type>::try_new(keys, values).unwrap()) as ArrayRef
    }

    fn concatenated_dictionary_with_out_of_range_null_keys() -> ArrayRef {
        use arrow_array::{builder::StringDictionaryBuilder, new_null_array, types::Int32Type};

        let mut builder = StringDictionaryBuilder::<Int32Type>::new();
        builder.append_value("a");
        for _ in 0..7 {
            builder.append_null();
        }
        let valued = Arc::new(builder.finish()) as ArrayRef;
        let all_null = new_null_array(valued.data_type(), 8);
        arrow_select::concat::concat(&[valued.as_ref(), all_null.as_ref()]).unwrap()
    }

    #[rstest::rstest]
    #[case::hand_built(hand_built_dictionary_with_out_of_range_null_keys(), 1)]
    #[case::concatenated(concatenated_dictionary_with_out_of_range_null_keys(), 8)]
    #[tokio::test]
    async fn test_dictionary_out_of_range_null_keys_round_trip(
        #[case] dictionary: ArrayRef,
        #[case] split_at: usize,
    ) {
        let num_rows = dictionary.len();
        let dictionaries = vec![
            dictionary.slice(0, split_at),
            dictionary.slice(split_at, num_rows - split_at),
        ];
        let test_cases = TestCases::default()
            .with_encoding(TestEncoding::StructuralU32)
            .with_page_sizes(vec![1])
            .with_range(split_at as u64..num_rows as u64)
            .with_indices(vec![split_at as u64]);

        check_round_trip_encoding_of_data(dictionaries, &test_cases, HashMap::new()).await;
    }

    #[test]
    fn test_encode_decode_complex_all_null_vals_roundtrip() {
        use crate::compression::{DecompressionStrategy, DefaultDecompressionStrategy};

        let values: Arc<[u16]> = Arc::from((0..2048).map(|i| (i % 5) as u16).collect::<Vec<u16>>());

        let compression_strategy = crate::testing::test_compression_strategy(
            TestEncoding::StructuralU16,
            crate::compression_config::CompressionParams::default(),
        );
        let decompression_strategy = DefaultDecompressionStrategy::default();

        let (compressed_buf, encoding) = PrimitiveStructuralEncoder::encode_complex_all_null_vals(
            &values,
            compression_strategy.as_ref(),
        )
        .unwrap();

        let decompressor = decompression_strategy
            .create_block_decompressor(&encoding)
            .unwrap();
        let decompressed = decompressor
            .decompress(Some(compressed_buf), values.len() as u64)
            .unwrap();
        let decompressed_fixed_width = decompressed.as_fixed_width().unwrap();
        assert_eq!(decompressed_fixed_width.num_values, values.len() as u64);
        assert_eq!(decompressed_fixed_width.bits_per_value, 16);
        let rep_result = decompressed_fixed_width.data.borrow_to_typed_slice::<u16>();
        assert_eq!(rep_result.as_ref(), values.as_ref());
    }

    #[tokio::test]
    async fn test_complex_all_null_compression_gated_by_version() {
        use crate::format::pb21::page_layout::Layout;
        use arrow_array::ListArray;

        let list_array = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(
            (0..1000).map(|i| if i % 2 == 0 { None } else { Some(vec![]) }),
        );
        let arr: ArrayRef = Arc::new(list_array);
        let field = arrow_schema::Field::new(
            "c",
            DataType::List(Arc::new(arrow_schema::Field::new(
                "item",
                DataType::Int32,
                true,
            ))),
            true,
        );

        let page_v21 =
            encode_first_page(field.clone(), arr.clone(), TestEncoding::StructuralU16).await;
        let PageEncoding::Structural(layout_v21) = &page_v21.description else {
            panic!("Expected structural encoding");
        };
        let Layout::ConstantLayout(layout_v21) = layout_v21.layout.as_ref().unwrap() else {
            panic!("Expected constant layout");
        };
        assert!(layout_v21.rep_compression.is_none());
        assert!(layout_v21.def_compression.is_none());
        assert_eq!(layout_v21.num_rep_values, 0);
        assert_eq!(layout_v21.num_def_values, 0);

        let page_v22 = encode_first_page(field, arr, TestEncoding::StructuralU32).await;
        let PageEncoding::Structural(layout_v22) = &page_v22.description else {
            panic!("Expected structural encoding");
        };
        let Layout::ConstantLayout(layout_v22) = layout_v22.layout.as_ref().unwrap() else {
            panic!("Expected constant layout");
        };
        assert!(layout_v22.def_compression.is_some());
        assert!(layout_v22.num_def_values > 0);
    }

    #[tokio::test]
    async fn test_complex_all_null_round_trip() {
        use arrow_array::ListArray;

        let list_array = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(
            (0..1000).map(|i| if i % 2 == 0 { None } else { Some(vec![]) }),
        );

        let test_cases = TestCases::default().with_u32_structural_encodings();
        check_round_trip_encoding_of_data(vec![Arc::new(list_array)], &test_cases, HashMap::new())
            .await;
    }

    #[tokio::test]
    async fn test_complex_all_null_constant_def_round_trip() {
        use arrow_array::ListArray;

        // Every row is a null list => constant def levels => a single RLE run,
        // exercising the lazy run-form decode end to end.
        let list_array = ListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(
            (0..5000).map(|_| None::<Vec<Option<i32>>>),
        );

        let test_cases = TestCases::default().with_u32_structural_encodings();
        check_round_trip_encoding_of_data(vec![Arc::new(list_array)], &test_cases, HashMap::new())
            .await;
    }

    fn encoded_u16_frame(levels: &[u16], run_length_width: RunLengthWidth) -> LanceBuffer {
        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::reinterpret_slice(Arc::from(levels)),
            bits_per_value: 16,
            num_values: levels.len() as u64,
            block_info: BlockInfo::new(),
        });
        BlockCompressor::compress(&RleEncoder::with_run_length_width(run_length_width), block)
            .unwrap()
            .0
            .unwrap()
    }

    fn encoded_u16_runs(levels: &[u16], run_length_width: RunLengthWidth) -> RleRuns {
        let frame = encoded_u16_frame(levels, run_length_width);
        RleDecompressor::with_run_length_width(16, run_length_width)
            .decode_u16_runs(frame, levels.len() as u64)
            .unwrap()
    }

    #[test]
    fn miniblock_levels_use_one_count_for_mixed_codecs() {
        let actual_num_levels = usize::from(u16::MAX) + 8;
        let levels = vec![1_u16; actual_num_levels];
        let rep_frame = encoded_u16_frame(&levels, RunLengthWidth::U8);
        let rep_decompressor = RleDecompressor::new(16);
        let def_frame = LanceBuffer::reinterpret_slice(Arc::from(levels.clone()));
        let def_decompressor = ValueDecompressor::from_flat(&pb21::Flat {
            bits_per_value: 16,
            data: None,
        });

        let num_levels = DecodeMiniBlockTask::resolve_num_levels(
            Some(&rep_decompressor),
            Some(&rep_frame),
            Some(&def_decompressor),
            Some(&def_frame),
            7,
        )
        .unwrap();
        assert_eq!(num_levels, actual_num_levels as u64);

        let rep =
            DecodeMiniBlockTask::decode_levels(&rep_decompressor, rep_frame, num_levels).unwrap();
        let def =
            DecodeMiniBlockTask::decode_levels(&def_decompressor, def_frame, num_levels).unwrap();
        assert_eq!(rep.as_ref(), levels);
        assert_eq!(def, rep);
    }

    #[test]
    fn miniblock_levels_reject_non_wrapped_count_mismatch() {
        let frame = encoded_u16_frame(&[1_u16; 8], RunLengthWidth::U8);
        let decompressor = RleDecompressor::new(16);

        let error = DecodeMiniBlockTask::resolve_num_levels(
            Some(&decompressor),
            Some(&frame),
            None,
            None,
            7,
        )
        .unwrap_err();
        assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
        assert!(error.to_string().contains("not congruent modulo 65536"));
    }

    #[test]
    fn miniblock_levels_reject_cross_stream_count_disagreement() {
        let rep_levels = vec![1_u16; usize::from(u16::MAX) + 8];
        let def_levels = vec![1_u16; rep_levels.len() + usize::from(u16::MAX) + 1];
        let rep_frame = encoded_u16_frame(&rep_levels, RunLengthWidth::U8);
        let def_frame = encoded_u16_frame(&def_levels, RunLengthWidth::U8);
        let decompressor = RleDecompressor::new(16);

        let error = DecodeMiniBlockTask::resolve_num_levels(
            Some(&decompressor),
            Some(&rep_frame),
            Some(&decompressor),
            Some(&def_frame),
            7,
        )
        .unwrap_err();
        assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
        assert!(
            error
                .to_string()
                .contains("structural streams disagree on the level count")
        );
    }

    fn physical_levels(levels: &[u16]) -> LazyLevels {
        LazyLevels::Runs(Arc::new(RunStorage::Physical(
            encoded_u16_runs(levels, RunLengthWidth::U8).into_owned(),
        )))
    }

    fn coalesced_levels(levels: &[u16]) -> LazyLevels {
        let mut values = Vec::new();
        let mut ends = RunEndsBuilder::with_capacity(levels.len(), levels.len());
        for (index, &value) in levels.iter().enumerate() {
            if values.last() == Some(&value) {
                ends.set_last(index + 1).unwrap();
            } else {
                values.push(value);
                ends.push(index + 1).unwrap();
            }
        }
        LazyLevels::Runs(Arc::new(RunStorage::Coalesced {
            values: values.into_boxed_slice(),
            ends: ends.finish(),
        }))
    }

    #[test]
    fn lazy_levels_runs_match_dense() {
        // Runs: 3x2, 1x1, 3x3, 0x2  =>  [3,3,1,3,3,3,0,0]
        let expanded: Vec<u16> = vec![3, 3, 1, 3, 3, 3, 0, 0];
        let coalesced = coalesced_levels(&expanded);
        let physical = physical_levels(&expanded);
        let dense = LazyLevels::Dense(ScalarBuffer::<u16>::from(expanded.clone()));
        let n = expanded.len();

        assert_eq!(coalesced.len(), n);
        assert_eq!(physical.len(), n);
        assert_eq!(dense.len(), n);

        // Rows begin at each `max_rep` (3) position; row `num_rows` maps to `len`.
        let max_rep = 3u16;
        let row_starts: Vec<usize> = (0..n).filter(|&i| expanded[i] == max_rep).collect();
        for target in 0..=row_starts.len() as u64 {
            let want = row_starts.get(target as usize).copied().unwrap_or(n);
            for runs in [&coalesced, &physical] {
                let mut cursor = LevelCursor::default();
                assert_eq!(
                    runs.seek_row_start(&mut cursor, target, max_rep).unwrap(),
                    want,
                    "seek_row_start({target})"
                );
            }
            let mut c_dense = LevelCursor::default();
            assert_eq!(
                dense.seek_row_start(&mut c_dense, target, max_rep).unwrap(),
                want
            );
        }

        // `count_le_cursor` (fresh cursor per range) and `extend_into` agree with
        // the dense reference on every sub-range.
        for start in 0..=n {
            for end in start..=n {
                for max in [0u16, 1, 2, 3] {
                    let want = expanded[start..end].iter().filter(|&&d| d <= max).count() as u64;
                    for runs in [&coalesced, &physical] {
                        let mut cursor = RunPosition::default();
                        assert_eq!(
                            runs.count_le_cursor(&mut cursor, start..end, max).0,
                            want,
                            "count_le_cursor({start}..{end}, {max})"
                        );
                    }
                    let mut d_cur = RunPosition::default();
                    assert_eq!(dense.count_le_cursor(&mut d_cur, start..end, max).0, want);
                }
                for runs in [&coalesced, &physical] {
                    let mut got = Vec::new();
                    runs.extend_into(start..end, RunPosition::default(), &mut got);
                    assert_eq!(
                        got,
                        expanded[start..end].to_vec(),
                        "extend_into({start}..{end})"
                    );
                }
                let mut got_dense = Vec::new();
                dense.extend_into(start..end, RunPosition::default(), &mut got_dense);
                assert_eq!(got_dense, expanded[start..end].to_vec());
            }
        }
    }

    #[test]
    fn physical_run_hints_support_deferred_materialization() {
        let expanded: Vec<u16> = vec![3, 3, 1, 1, 2, 2, 0, 0];
        let physical = physical_levels(&expanded);
        let LazyLevels::Runs(runs) = &physical else {
            panic!("expected physical runs");
        };
        let mut first_hint = RunPosition::default();
        runs.seek(&mut first_hint, 2);
        let mut second_hint = RunPosition::default();
        runs.seek(&mut second_hint, 6);

        let mut second = Vec::new();
        physical.extend_into(6..8, second_hint, &mut second);
        let mut first = Vec::new();
        physical.extend_into(2..4, first_hint, &mut first);
        assert_eq!(second, expanded[6..8]);
        assert_eq!(first, expanded[2..4]);
    }

    /// Fuzz parity for the run-oriented complex-all-null drain: the cursor walk
    /// over `LazyLevels` must yield the exact level slices and visible
    /// count that a brute-force reference over the fully expanded levels does, for
    /// dense, physical-run, and coalesced-run forms and arbitrarily shaped range requests.
    mod complex_all_null_drain_parity {
        use std::ops::Range;

        use arrow_buffer::ScalarBuffer;
        use proptest::prelude::*;

        use super::super::{LazyLevels, LevelCursor, RunPosition};
        use super::{coalesced_levels, physical_levels};
        use crate::Result;

        #[derive(Debug, Clone)]
        struct DrainInput {
            max_rep: u16,
            max_visible: u16,
            rep: Option<Vec<u16>>,
            def: Option<Vec<u16>>,
            ranges: Vec<Range<u64>>,
        }

        fn dense_levels(levels: &[u16]) -> LazyLevels {
            LazyLevels::Dense(ScalarBuffer::from(levels.to_vec()))
        }

        fn rle_levels(levels: &[u16]) -> LazyLevels {
            coalesced_levels(levels)
        }

        fn seek(
            rep: Option<&LazyLevels>,
            cursor: &mut LevelCursor,
            row: u64,
            max_rep: u16,
        ) -> Result<usize> {
            match rep {
                Some(rep) => rep.seek_row_start(cursor, row, max_rep),
                None => {
                    cursor.row = row;
                    cursor.level = row as usize;
                    Ok(row as usize)
                }
            }
        }

        /// Mirror of `ComplexAllNullPageDecoder::drain`, driving the real
        /// `seek_row_start` / `count_le_cursor` with monotonic cursors.
        fn simulate_drain(
            rep: Option<&LazyLevels>,
            def: Option<&LazyLevels>,
            max_rep: u16,
            max_visible: u16,
            ranges: &[Range<u64>],
        ) -> Result<(Vec<Range<usize>>, u64)> {
            let mut rep_cursor = LevelCursor::default();
            let mut def_run_cursor = RunPosition::default();
            let mut slices: Vec<Range<usize>> = Vec::new();
            let mut visible = 0u64;
            for range in ranges {
                let level_start = seek(rep, &mut rep_cursor, range.start, max_rep)?;
                let level_end = seek(rep, &mut rep_cursor, range.end, max_rep)?;
                visible += match def {
                    Some(def) => {
                        def.count_le_cursor(
                            &mut def_run_cursor,
                            level_start..level_end,
                            max_visible,
                        )
                        .0
                    }
                    None => (level_end - level_start) as u64,
                };
                match slices.last_mut() {
                    Some(last) if last.end == level_start => last.end = level_end,
                    _ => slices.push(level_start..level_end),
                }
            }
            Ok((slices, visible))
        }

        /// Independent brute-force reference over fully expanded levels.
        fn reference_drain(
            rep: Option<&[u16]>,
            def: Option<&[u16]>,
            max_rep: u16,
            max_visible: u16,
            ranges: &[Range<u64>],
        ) -> (Vec<Range<usize>>, u64) {
            let total_levels = rep
                .map(|r| r.len())
                .or_else(|| def.map(|d| d.len()))
                .unwrap_or(0);
            // Level index where each row starts (or `total_levels` for the end row).
            let row_starts: Vec<usize> = match rep {
                Some(rep) => (0..rep.len()).filter(|&i| rep[i] == max_rep).collect(),
                None => (0..total_levels).collect(),
            };
            let level_of_row = |row: u64| {
                row_starts
                    .get(row as usize)
                    .copied()
                    .unwrap_or(total_levels)
            };

            let mut slices: Vec<Range<usize>> = Vec::new();
            let mut visible = 0u64;
            for range in ranges {
                let ls = level_of_row(range.start);
                let le = level_of_row(range.end);
                visible += match def {
                    Some(def) => def[ls..le].iter().filter(|&&d| d <= max_visible).count() as u64,
                    None => (le - ls) as u64,
                };
                match slices.last_mut() {
                    Some(last) if last.end == ls => last.end = le,
                    _ => slices.push(ls..le),
                }
            }
            (slices, visible)
        }

        fn ranges_strategy(num_rows: u64) -> BoxedStrategy<Vec<Range<u64>>> {
            if num_rows == 0 {
                return Just(Vec::new()).boxed();
            }
            // (gap, len) pairs; a zero gap yields ranges adjacent in row space,
            // which exercises the level-slice coalescing path.
            proptest::collection::vec((0u64..=3, 1u64..=4), 0..=8)
                .prop_map(move |pairs| {
                    let mut ranges = Vec::new();
                    let mut pos = 0u64;
                    for (gap, len) in pairs {
                        pos = pos.saturating_add(gap);
                        if pos >= num_rows {
                            break;
                        }
                        let end = (pos + len).min(num_rows);
                        ranges.push(pos..end);
                        pos = end;
                    }
                    ranges
                })
                .boxed()
        }

        fn drain_input() -> impl Strategy<Value = DrainInput> {
            (
                1u16..=3,
                0u16..=3,
                any::<bool>(),
                any::<bool>(),
                1usize..=48,
            )
                .prop_flat_map(|(max_rep, max_visible, has_rep, has_def, len)| {
                    // Complex-all-null always has definition levels when there is
                    // no repetition, so force `def` present in that case.
                    let has_def = has_def || !has_rep;
                    let rep = if has_rep {
                        proptest::collection::vec(0u16..=max_rep, len)
                            .prop_map(move |mut v| {
                                // Row 0 must start at a max-rep boundary.
                                v[0] = max_rep;
                                Some(v)
                            })
                            .boxed()
                    } else {
                        Just(None).boxed()
                    };
                    let def = if has_def {
                        proptest::collection::vec(0u16..=(max_visible + 2), len)
                            .prop_map(Some)
                            .boxed()
                    } else {
                        Just(None).boxed()
                    };
                    (Just(max_rep), Just(max_visible), rep, def)
                })
                .prop_flat_map(|(max_rep, max_visible, rep, def)| {
                    let num_rows = match &rep {
                        Some(rep) => rep.iter().filter(|&&v| v == max_rep).count() as u64,
                        None => def.as_ref().map(|d| d.len() as u64).unwrap_or(0),
                    };
                    ranges_strategy(num_rows).prop_map(move |ranges| DrainInput {
                        max_rep,
                        max_visible,
                        rep: rep.clone(),
                        def: def.clone(),
                        ranges,
                    })
                })
        }

        proptest! {
            #![proptest_config(ProptestConfig::with_cases(512))]

            #[test]
            fn drain_matches_reference(input in drain_input()) {
                let DrainInput { max_rep, max_visible, rep, def, ranges } = input;

                let reference =
                    reference_drain(rep.as_deref(), def.as_deref(), max_rep, max_visible, &ranges);

                let rep_dense = rep.as_deref().map(dense_levels);
                let def_dense = def.as_deref().map(dense_levels);
                let got_dense =
                    simulate_drain(rep_dense.as_ref(), def_dense.as_ref(), max_rep, max_visible, &ranges)
                        .unwrap();
                prop_assert_eq!(&got_dense, &reference, "dense form diverged from reference");

                let rep_rle = rep.as_deref().map(rle_levels);
                let def_rle = def.as_deref().map(rle_levels);
                let got_rle =
                    simulate_drain(rep_rle.as_ref(), def_rle.as_ref(), max_rep, max_visible, &ranges)
                        .unwrap();
                prop_assert_eq!(&got_rle, &reference, "rle form diverged from reference");

                let rep_physical = rep.as_deref().map(physical_levels);
                let def_physical = def.as_deref().map(physical_levels);
                let got_physical =
                    simulate_drain(rep_physical.as_ref(), def_physical.as_ref(), max_rep, max_visible, &ranges)
                        .unwrap();
                prop_assert_eq!(&got_physical, &reference, "physical form diverged from reference");
            }
        }
    }

    #[test]
    fn lazy_levels_runs_are_compact() {
        let single_run = |n: usize| {
            let mut ends = RunEndsBuilder::with_capacity(n, 1);
            ends.push(n).unwrap();
            LazyLevels::Runs(Arc::new(RunStorage::Coalesced {
                values: vec![1u16].into_boxed_slice(),
                ends: ends.finish(),
            }))
        };
        // Run-form footprint is independent of the logical length within an end width...
        assert_eq!(single_run(100).deep_size(), single_run(10_000).deep_size());
        assert!(single_run(10_000_000).deep_size() < 100);
        assert_eq!(single_run(10_000_000).len(), 10_000_000);
        // ...while Dense pays 2 bytes per value.
        assert_eq!(
            LazyLevels::Dense(ScalarBuffer::<u16>::from(vec![1u16; 1000])).deep_size(),
            2000
        );
    }

    #[test]
    fn lazy_levels_selects_smallest_representation() {
        let runs = encoded_u16_runs(&[7u16; 10], RunLengthWidth::U8);
        assert_eq!(LazyLevels::select_plan(&runs), LevelPlan::Dense);

        let equal_size: Vec<u16> = std::iter::repeat_n(0, 256)
            .chain(std::iter::repeat_n(1, 100))
            .chain(std::iter::repeat_n(2, 100))
            .collect();
        let runs = encoded_u16_runs(&equal_size, RunLengthWidth::U8);
        assert_eq!(LazyLevels::select_plan(&runs), LevelPlan::Coalesced);

        let moderate_runs: Vec<u16> = (0..250)
            .flat_map(|run| std::iter::repeat_n((run % 2) as u16, 4))
            .collect();
        let runs = encoded_u16_runs(&moderate_runs, RunLengthWidth::U8);
        assert_eq!(LazyLevels::select_plan(&runs), LevelPlan::Physical);

        let split_constant = vec![7u16; 5000];
        let runs = encoded_u16_runs(&split_constant, RunLengthWidth::U8);
        assert_eq!(LazyLevels::select_plan(&runs), LevelPlan::Coalesced);

        let high_density: Vec<u16> = (0..70_000).map(|index| (index % 2) as u16).collect();
        let runs = encoded_u16_runs(&high_density, RunLengthWidth::U32);
        assert_eq!(LazyLevels::select_plan(&runs), LevelPlan::Dense);
    }

    #[test]
    fn physical_runs_detach_from_large_encoded_frame() {
        let levels: Vec<u16> = (0..250)
            .flat_map(|run| std::iter::repeat_n((run % 2) as u16, 4))
            .collect();
        let frame = encoded_u16_frame(&levels, RunLengthWidth::U8);
        let frame_offset = 4096;
        let mut allocation = vec![0; frame_offset + frame.len() + 1_000_000];
        allocation[frame_offset..frame_offset + frame.len()].copy_from_slice(frame.as_ref());
        let frame = LanceBuffer::from(allocation).slice_with_length(frame_offset, frame.len());
        let runs = RleDecompressor::with_run_length_width(16, RunLengthWidth::U8)
            .decode_u16_runs(frame, levels.len() as u64)
            .unwrap();

        assert_eq!(LazyLevels::select_plan(&runs), LevelPlan::Physical);
        let cached = LazyLevels::from_rle_runs(runs).unwrap();
        assert!(
            matches!(cached, LazyLevels::Runs(ref runs) if matches!(runs.as_ref(), RunStorage::Physical(_)))
        );
        assert_eq!(cached.len(), levels.len());
        assert!(cached.deep_size() < 4096);
    }

    #[test]
    fn complex_all_null_levels_reject_invalid_values_and_lengths() {
        let invalid_levels = vec![0u16, 3];
        for levels in [
            LazyLevels::Dense(ScalarBuffer::from(invalid_levels.clone())),
            physical_levels(&invalid_levels),
            coalesced_levels(&invalid_levels),
        ] {
            let error = validate_complex_all_null_levels(&None, &Some(levels), 0, 2).unwrap_err();
            assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
            assert!(error.to_string().contains("Invalid definition level 3"));
        }

        let rep = Some(LazyLevels::Dense(ScalarBuffer::from(vec![0u16; 2])));
        let def = Some(LazyLevels::Dense(ScalarBuffer::from(vec![0u16])));
        let error = validate_complex_all_null_levels(&rep, &def, 0, 0).unwrap_err();
        assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
        assert!(
            error
                .to_string()
                .contains("repetition has 2, definition has 1")
        );
    }

    #[test]
    fn block_levels_reject_malformed_payload_size() {
        let block = DataBlock::FixedWidth(FixedWidthDataBlock {
            data: LanceBuffer::from(vec![0]),
            bits_per_value: 16,
            num_values: 1,
            block_info: BlockInfo::new(),
        });
        let error = dense_levels_from_block(block, 1, "definition").unwrap_err();
        assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
        assert!(
            error
                .to_string()
                .contains("expected 2 bytes for 1 values, got 1")
        );
    }

    #[test]
    fn complex_all_null_level_codec_validates_rle_metadata() {
        let encoding = pb21::CompressiveEncoding {
            compression: Some(Compression::Rle(Box::new(pb21::Rle {
                values: None,
                run_lengths: Some(Box::new(ProtobufUtils21::flat(8, None))),
            }))),
        };

        let error = LevelCodec::try_new(Some(&encoding), &DefaultDecompressionStrategy::default())
            .unwrap_err();
        assert!(matches!(error, lance_core::Error::InvalidInput { .. }));
        assert!(
            error
                .to_string()
                .contains("RLE compression missing values encoding")
        );
    }

    // https://github.com/lance-format/lance/issues/6681
    #[tokio::test]
    async fn test_sparse_boolean_list_roundtrip() {
        use arrow_array::builder::{BooleanBuilder, ListBuilder};

        let mut list_builder = ListBuilder::new(BooleanBuilder::new());
        for i in 0..1000i32 {
            if i % 64 == 0 {
                // Alternate true/false so the array is not constant (constant path avoids the bug).
                list_builder.values().append_value(i % 128 == 0);
                list_builder.append(true);
            } else {
                list_builder.append(false);
            }
        }
        let list_array = Arc::new(list_builder.finish());

        let test_cases = TestCases::default().with_structural_encodings();
        check_round_trip_encoding_of_data(vec![list_array], &test_cases, HashMap::new()).await;
    }

    fn truncated_tail_details() -> std::sync::Arc<super::FullZipDecodeDetails> {
        use crate::compression::VariablePerValueDecompressor;
        use crate::encodings::physical::binary::VariableDecoder;
        use crate::repdef::{ControlWordParser, DefinitionInterpretation};
        use std::sync::Arc;
        Arc::new(super::FullZipDecodeDetails {
            value_decompressor: super::PerValueDecompressor::Variable(Arc::new(
                VariableDecoder::default(),
            )
                as Arc<dyn VariablePerValueDecompressor>),
            def_meaning: vec![DefinitionInterpretation::NullableItem].into(),
            ctrl_word_parser: ControlWordParser::new(0, 0),
            max_rep: 0,
            max_visible_def: 0,
        })
    }

    fn decode_variable_full_zip(
        buf: Vec<u8>,
        bits_per_offset: u8,
    ) -> lance_core::Result<super::VariableFullZipDecoder> {
        use std::collections::VecDeque;
        let mut data = VecDeque::new();
        data.push_back(crate::buffer::LanceBuffer::from(buf));
        super::VariableFullZipDecoder::new(
            truncated_tail_details(),
            data,
            1,
            bits_per_offset,
            bits_per_offset,
        )
    }

    /// A well-formed length prefix decodes without incident, for both widths.
    #[test]
    fn variable_full_zip_wellformed_length_prefix() {
        assert!(decode_variable_full_zip(0u32.to_le_bytes().to_vec(), 32).is_ok());
        assert!(decode_variable_full_zip(0u64.to_le_bytes().to_vec(), 64).is_ok());
    }

    /// A page whose item walk ends with a partial length prefix must surface a
    /// corrupt-file error rather than read past the end of the buffer.
    ///
    /// This asserts the error variant and message rather than merely expecting a
    /// panic: before the length prefix was bounds checked, the read was
    /// `get_unchecked` behind a `debug_assert!`, so a debug build panicked here
    /// (which a `#[should_panic]` test would have accepted as a pass) while a
    /// release build read up to 8 bytes out of a 4 byte allocation.
    #[test]
    fn variable_full_zip_truncated_length_prefix_is_corrupt_file() {
        use lance_core::Error;

        for (bits, buf_len) in [(32u8, 3usize), (64u8, 4usize)] {
            let err = decode_variable_full_zip(vec![0xAA; buf_len], bits)
                .expect_err("a truncated length prefix must not decode");
            assert!(
                matches!(err, Error::CorruptFile { .. }),
                "expected CorruptFile for a {}-bit prefix with {} byte(s), got: {:?}",
                bits,
                buf_len,
                err
            );
            let msg = err.to_string();
            assert!(
                msg.contains("truncated length prefix"),
                "error should say what is wrong, got: {msg}"
            );
        }
    }

    /// Drains `num_rows` from a single-buffer fixed full-zip page built from `buf`.
    ///
    /// `bits_rep` picks the control word width (1 -> one byte, 9 -> two bytes) and each
    /// visible item is a one-word control plus a 4 byte value.
    fn drain_fixed_full_zip(buf: Vec<u8>, bits_rep: u8, num_rows: u64) -> lance_core::Result<()> {
        use crate::compression::FixedPerValueDecompressor;
        use crate::decoder::StructuralPageDecoder;
        use crate::repdef::{ControlWordParser, DefinitionInterpretation};
        use std::collections::VecDeque;
        use std::sync::Arc;

        #[derive(Debug)]
        struct UnusedFixedDecompressor;

        impl FixedPerValueDecompressor for UnusedFixedDecompressor {
            fn decompress(&self, _: FixedWidthDataBlock, _: u64) -> crate::Result<DataBlock> {
                unreachable!("these cases fail while slicing, before any value is decoded")
            }

            fn bits_per_value(&self) -> u64 {
                32
            }
        }

        let details = Arc::new(super::FullZipDecodeDetails {
            value_decompressor: super::PerValueDecompressor::Fixed(Arc::new(
                UnusedFixedDecompressor,
            )),
            def_meaning: vec![DefinitionInterpretation::AllValidList].into(),
            ctrl_word_parser: ControlWordParser::new(bits_rep, 0),
            max_rep: 1,
            max_visible_def: 0,
        });
        let bytes_per_word = bits_rep.div_ceil(8) as usize;

        let mut data = VecDeque::new();
        data.push_back(crate::buffer::LanceBuffer::from(buf));
        let mut decoder = super::FixedFullZipDecoder {
            details,
            data,
            offset_in_current: 0,
            bytes_per_value: 4,
            total_bytes_per_value: 4 + bytes_per_word,
            num_rows,
        };
        decoder.drain(num_rows).map(|_| ())
    }

    /// Two one-item rows, each a control word plus a 4 byte value, slice cleanly.
    #[test]
    fn fixed_full_zip_wellformed_page_slices() {
        assert!(drain_fixed_full_zip(vec![1, 0, 0, 0, 0, 1, 0, 0, 0, 0], 1, 2).is_ok());
    }

    /// The item walk is driven by the page's own control words, so a page that disagrees
    /// with its layout can run off the end of the buffer.  Each way of doing so must
    /// surface a corrupt-file error.
    ///
    /// These assert the error variant and message rather than merely expecting a panic:
    /// before the walk was bounds checked these aborted the process inside
    /// `LanceBuffer::slice_with_length`, which a `#[should_panic]` test would have
    /// accepted as a pass.
    #[rstest::rstest]
    // Second value is one byte short of a full 4 byte value
    #[case::truncated_item(vec![1, 0, 0, 0, 0, 1, 0, 0, 0], 1, 2, "truncated item")]
    // Two byte control words, with a lone trailing byte after the first item
    #[case::truncated_control_word(
        vec![1, 0, 0, 0, 0, 0, 1],
        9,
        2,
        "truncated control word"
    )]
    // A well-formed two row page cannot satisfy a request for three rows
    #[case::page_exhausted(vec![1, 0, 0, 0, 0, 1, 0, 0, 0, 0], 1, 3, "page data ran out")]
    fn fixed_full_zip_malformed_page_is_corrupt_file(
        #[case] buf: Vec<u8>,
        #[case] bits_rep: u8,
        #[case] num_rows: u64,
        #[case] expected_msg: &str,
    ) {
        use lance_core::Error;

        let err = drain_fixed_full_zip(buf, bits_rep, num_rows)
            .expect_err("a malformed page must not slice");
        assert!(
            matches!(err, Error::CorruptFile { .. }),
            "expected CorruptFile, got: {err:?}"
        );
        let msg = err.to_string();
        assert!(
            msg.contains(expected_msg),
            "error should say what is wrong, got: {msg}"
        );
    }
}
