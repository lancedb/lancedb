// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::fmt::{Debug, Display};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::{
    cmp::{Reverse, min},
    collections::BinaryHeap,
};
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::Range,
    time::Instant,
};

use crate::metrics::NoOpMetricsCollector;
use crate::prefilter::NoFilter;
use crate::scalar::registry::{TrainingCriteria, TrainingOrdering};
use crate::vector::graph::OrderedFloat;
use arrow::array::{BooleanBuilder, FixedSizeListBuilder, Float32Builder, Int32Builder};
use arrow::datatypes::{self, Float32Type, Int32Type, UInt64Type};
use arrow::{
    array::{
        AsArray, LargeBinaryBuilder, ListBuilder, StringBuilder, UInt32Builder, UInt64Builder,
    },
    buffer::{Buffer, OffsetBuffer},
};
use arrow::{buffer::ScalarBuffer, datatypes::UInt32Type};
use arrow_array::{
    Array, ArrayRef, BooleanArray, Float32Array, LargeBinaryArray, ListArray, OffsetSizeTrait,
    RecordBatch, UInt32Array, UInt64Array,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::Time;
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use fst::{Automaton, IntoStreamer, Streamer};
use futures::{FutureExt, Stream, StreamExt, TryStreamExt, stream};
use itertools::{Either, Itertools};
use lance_arrow::{RecordBatchExt, iter_str_array};
use lance_core::cache::{
    CacheCodec, CacheKey, CacheKeySchema, KeyBuilder, LanceCache, WeakLanceCache,
};
use lance_core::deepsize::DeepSizeOf;
use lance_core::error::{DataFusionResult, LanceOptionExt};
use lance_core::utils::address::RowAddress;
use lance_core::utils::tokio::{get_num_compute_intensive_cpus, spawn_cpu};
use lance_core::utils::tracing::{IO_TYPE_LOAD_SCALAR_PART, TRACE_IO_EVENTS};
use lance_core::{Error, ROW_ID, ROW_ID_FIELD, Result};
use lance_select::{RowAddrMask, RowAddrTreeMap};
use roaring::RoaringBitmap;
use std::sync::LazyLock;
use tokio::{
    sync::{Mutex, OnceCell},
    task::spawn_blocking,
};
use tracing::{info, instrument, warn};

use super::documents::{
    DocId, DocLengths, DocVisibility, PartitionDocumentStore, PartitionDocuments,
};
use super::encoding::{MAX_POSTING_BLOCK_SIZE, PositionBlockBuilder};
use super::impact::{IMPACT_LEVEL1_BLOCKS, ImpactSkipData, ImpactSkipDataBuilder};
use super::iter::PostingListIterator;
use super::tokenizer::{LEGACY_BLOCK_SIZE, validate_block_size};
use super::{DocumentGranularity, InvertedIndexBuilder, InvertedIndexParams, wand::*};
use super::{
    builder::{
        BLOCK_SIZE, ScoredDoc, doc_file_path,
        inverted_list_schema_for_version_with_block_size_and_impacts, posting_file_path,
        token_file_path,
    },
    iter::PlainPostingListIterator,
    query::*,
    scorer::{B, IndexBM25Scorer, K1, Scorer, idf},
};
use super::{
    builder::{InnerBuilder, PositionRecorder},
    iter::CompressedPostingListIterator,
};
use crate::pbold;
use crate::progress::IndexBuildProgress;
use crate::scalar::inverted::scorer::MemBM25Scorer;
use crate::scalar::inverted::tokenizer::document_tokenizer::LanceTokenizer;
use crate::scalar::{
    AnyQuery, BuiltinIndexType, CreatedIndex, IndexReader, IndexStore, MetricsCollector,
    OldIndexDataFilter, RowIdRemapper, ScalarIndex, ScalarIndexParams, SearchResult, TokenQuery,
    UpdateCriteria,
};
use crate::{
    FtsPrewarmDiagnostics, FtsPrewarmOptions, FtsPrewarmPartitionStatus, FtsPrewarmResult, Index,
};
use crate::{prefilter::PreFilter, scalar::inverted::iter::take_fst_keys};
use std::str::FromStr;

mod cache;
mod doc_set;
mod flat_search;
mod format;
mod inverted_index;
mod partition;
mod posting_batch_builder;
mod posting_builder;
mod posting_list;
mod posting_prewarm;
mod posting_reader;
mod prewarm;
mod search;
mod search_candidates;
mod token_set;

pub use cache::*;
pub use doc_set::*;
pub use flat_search::*;
pub use format::*;
pub use inverted_index::*;
pub use partition::*;
pub(super) use posting_batch_builder::*;
pub use posting_builder::*;
pub use posting_list::*;
pub use posting_reader::*;
use prewarm::*;
pub(super) use search_candidates::*;
pub use token_set::*;

#[cfg(test)]
mod tests;
