// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::utils::row_addr_remap::RowAddrRemap;
use std::{
    any::Any,
    cmp::Ordering,
    collections::{HashMap, HashSet},
    fmt::{Debug, Display},
    ops::Bound,
    sync::Arc,
};

use super::{
    AnyQuery, BuiltinIndexType, IndexFile, IndexReader, IndexStore, IndexWriter, MetricsCollector,
    OldIndexDataFilter, SargableQuery, ScalarIndex, ScalarIndexParams, SearchResult,
    compute_next_prefix,
};
use crate::cache_pb::{BTreeIndexHeader, RangeToFile};
use crate::{Index, IndexType};
use crate::{metrics::NoOpMetricsCollector, scalar::registry::TrainingCriteria};
use crate::{pbold, scalar::btree::flat::FlatIndex};
use crate::{
    progress::{IndexBuildProgress, noop_progress},
    scalar::{
        CreatedIndex, RowIdRemapper, UpdateCriteria,
        expression::{SargableQueryParser, ScalarQueryParser},
        registry::{
            BasicTrainer, ScalarIndexLoad, ScalarIndexPlugin, TrainingOrdering, TrainingRequest,
            VALUE_COLUMN_NAME, single_flight_open,
        },
    },
};
use arrow_arith::numeric::add;
use arrow_array::{
    Array, ArrayAccessor, ArrowNativeTypeOp, PrimitiveArray, RecordBatch, UInt32Array,
    cast::AsArray,
    new_empty_array,
    types::{
        ArrowPrimitiveType, Decimal128Type, Decimal256Type, Float16Type, Float32Type, Float64Type,
        Int8Type, Int16Type, Int32Type, Int64Type, UInt8Type, UInt16Type, UInt32Type, UInt64Type,
    },
};
use arrow_ord::ord::make_comparator;
use arrow_schema::{DataType, Field, IntervalUnit, Schema, SortOptions};
use async_trait::async_trait;
use datafusion::physical_plan::{
    ExecutionPlan, SendableRecordBatchStream,
    sorts::sort_preserving_merge::SortPreservingMergeExec, stream::RecordBatchStreamAdapter,
    union::UnionExec,
};
use datafusion_common::{DFSchema, DataFusionError, ScalarValue};
use datafusion_expr::execution_props::ExecutionProps;
use datafusion_physical_expr::{
    PhysicalExpr, PhysicalSortExpr, create_physical_expr, expressions::Column,
};
use futures::{
    FutureExt, Stream, StreamExt, TryFutureExt, TryStreamExt,
    future::BoxFuture,
    stream::{self},
};
use lance_core::deepsize::DeepSizeOf;
use lance_core::{
    Error, ROW_ID, Result,
    cache::{
        CacheCodec, CacheCodecImpl, CacheEntryReader, CacheEntryWriter, CacheKey, CacheKeySchema,
        KeyBuilder, LanceCache, WeakLanceCache,
    },
    error::LanceOptionExt,
    utils::{
        tokio::get_num_compute_intensive_cpus,
        tracing::{IO_TYPE_LOAD_SCALAR_PART, TRACE_IO_EVENTS},
    },
};
use lance_datafusion::{
    chunker::chunk_concat_stream,
    exec::{LanceExecutionOptions, OneShotExec, execute_plan},
};
use lance_select::{NullableRowAddrSet, RowSetOps};
use log::{debug, warn};
use object_store::Error as ObjectStoreError;
use rangemap::RangeInclusiveMap;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize, Serializer};
use tracing::{info, instrument};

mod flat;

pub const BTREE_LOOKUP_NAME: &str = "page_lookup.lance";
const BTREE_PAGES_NAME: &str = "page_data.lance";
pub const DEFAULT_BTREE_BATCH_SIZE: u64 = 4096;
const BATCH_SIZE_META_KEY: &str = "batch_size";
const DEFAULT_RANGE_PARTITIONED: bool = false;
const RANGE_PARTITIONED_META_KEY: &str = "range_partitioned";
const PAGE_NUM_PER_RANGE_PARTITION_META_KEY: &str = "page_num_per_range_partition";
const BTREE_INDEX_VERSION: u32 = 0;
pub(crate) const BTREE_VALUES_COLUMN: &str = "values";
pub(crate) const BTREE_IDS_COLUMN: &str = "ids";

/// Wraps a ScalarValue and implements Ord (ScalarValue only implements PartialOrd)
#[derive(Clone, Debug)]
pub struct OrderableScalarValue(pub ScalarValue);

impl DeepSizeOf for OrderableScalarValue {
    fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
        // deepsize and size both factor in the size of the ScalarValue
        self.0.size() - std::mem::size_of::<ScalarValue>()
    }
}

impl Display for OrderableScalarValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(&self.0, f)
    }
}

impl PartialEq for OrderableScalarValue {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl Eq for OrderableScalarValue {}

impl PartialOrd for OrderableScalarValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

// manual implementation of `Ord` that panics when asked to compare scalars of different type
// and always puts nulls before non-nulls (this is consistent with Option<T>'s implementation
// of Ord)
//
// TODO: Consider upstreaming this
impl Ord for OrderableScalarValue {
    fn cmp(&self, other: &Self) -> Ordering {
        use ScalarValue::*;
        // This purposely doesn't have a catch-all "(_, _)" so that
        // any newly added enum variant will require editing this list
        // or else face a compile error
        match (&self.0, &other.0) {
            (Decimal32(v1, p1, s1), Decimal32(v2, p2, s2)) => {
                if p1.eq(p2) && s1.eq(s2) {
                    v1.cmp(v2)
                } else {
                    // Two decimal values can only be compared if they have the same precision and scale.
                    panic!("Attempt to compare decimals with unequal precision / scale")
                }
            }
            (Decimal32(v1, _, _), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Decimal32(_, _, _), _) => panic!("Attempt to compare decimal with non-decimal"),
            (Decimal64(v1, p1, s1), Decimal64(v2, p2, s2)) => {
                if p1.eq(p2) && s1.eq(s2) {
                    v1.cmp(v2)
                } else {
                    // Two decimal values can only be compared if they have the same precision and scale.
                    panic!("Attempt to compare decimals with unequal precision / scale")
                }
            }
            (Decimal64(v1, _, _), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Decimal64(_, _, _), _) => panic!("Attempt to compare decimal with non-decimal"),
            (Decimal128(v1, p1, s1), Decimal128(v2, p2, s2)) => {
                if p1.eq(p2) && s1.eq(s2) {
                    v1.cmp(v2)
                } else {
                    // Two decimal values can only be compared if they have the same precision and scale.
                    panic!("Attempt to compare decimals with unequal precision / scale")
                }
            }
            (Decimal128(v1, _, _), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Decimal128(_, _, _), _) => panic!("Attempt to compare decimal with non-decimal"),
            (Decimal256(v1, p1, s1), Decimal256(v2, p2, s2)) => {
                if p1.eq(p2) && s1.eq(s2) {
                    v1.cmp(v2)
                } else {
                    // Two decimal values can only be compared if they have the same precision and scale.
                    panic!("Attempt to compare decimals with unequal precision / scale")
                }
            }
            (Decimal256(v1, _, _), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Decimal256(_, _, _), _) => panic!("Attempt to compare decimal with non-decimal"),

            (Boolean(v1), Boolean(v2)) => v1.cmp(v2),
            (Boolean(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Boolean(_), _) => panic!("Attempt to compare boolean with non-boolean"),
            (Float32(v1), Float32(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.total_cmp(f2),
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
            (Float32(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Float32(_), _) => panic!("Attempt to compare f32 with non-f32"),
            (Float64(v1), Float64(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.total_cmp(f2),
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
            (Float64(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Float64(_), _) => panic!("Attempt to compare f64 with non-f64"),
            (Float16(v1), Float16(v2)) => match (v1, v2) {
                (Some(f1), Some(f2)) => f1.total_cmp(f2),
                (None, Some(_)) => Ordering::Less,
                (Some(_), None) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            },
            (Float16(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Float16(_), _) => panic!("Attempt to compare f16 with non-f16"),
            (Int8(v1), Int8(v2)) => v1.cmp(v2),
            (Int8(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Int8(_), _) => panic!("Attempt to compare Int8 with non-Int8"),
            (Int16(v1), Int16(v2)) => v1.cmp(v2),
            (Int16(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Int16(_), _) => panic!("Attempt to compare Int16 with non-Int16"),
            (Int32(v1), Int32(v2)) => v1.cmp(v2),
            (Int32(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Int32(_), _) => panic!("Attempt to compare Int32 with non-Int32"),
            (Int64(v1), Int64(v2)) => v1.cmp(v2),
            (Int64(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Int64(_), _) => panic!("Attempt to compare Int64 with non-Int64"),
            (UInt8(v1), UInt8(v2)) => v1.cmp(v2),
            (UInt8(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (UInt8(_), _) => panic!("Attempt to compare UInt8 with non-UInt8"),
            (UInt16(v1), UInt16(v2)) => v1.cmp(v2),
            (UInt16(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (UInt16(_), _) => panic!("Attempt to compare UInt16 with non-UInt16"),
            (UInt32(v1), UInt32(v2)) => v1.cmp(v2),
            (UInt32(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (UInt32(_), _) => panic!("Attempt to compare UInt32 with non-UInt32"),
            (UInt64(v1), UInt64(v2)) => v1.cmp(v2),
            (UInt64(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (UInt64(_), _) => panic!("Attempt to compare UInt64 with non-UInt64"),
            (Utf8(v1) | Utf8View(v1) | LargeUtf8(v1), Utf8(v2) | Utf8View(v2) | LargeUtf8(v2)) => {
                v1.cmp(v2)
            }
            (Utf8(v1) | Utf8View(v1) | LargeUtf8(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Utf8(_) | Utf8View(_) | LargeUtf8(_), _) => {
                panic!("Attempt to compare Utf8 with non-Utf8")
            }
            (
                Binary(v1) | LargeBinary(v1) | BinaryView(v1),
                Binary(v2) | LargeBinary(v2) | BinaryView(v2),
            ) => v1.cmp(v2),
            (Binary(v1) | LargeBinary(v1) | BinaryView(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Binary(_) | LargeBinary(_) | BinaryView(_), _) => {
                panic!("Attempt to compare Binary with non-Binary")
            }
            (FixedSizeBinary(_, v1), FixedSizeBinary(_, v2)) => v1.cmp(v2),
            (FixedSizeBinary(_, v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (FixedSizeBinary(_, _), _) => {
                panic!("Attempt to compare FixedSizeBinary with non-FixedSizeBinary")
            }
            (FixedSizeList(left), FixedSizeList(right)) => {
                if left.eq(right) {
                    todo!()
                } else {
                    panic!(
                        "Attempt to compare fixed size list elements with different widths/fields"
                    )
                }
            }
            (FixedSizeList(left), Null) => {
                if left.is_null(0) {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (FixedSizeList(_), _) => {
                panic!("Attempt to compare FixedSizeList with non-FixedSizeList")
            }
            (List(_), List(_)) => todo!(),
            (List(left), Null) => {
                if left.is_null(0) {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (List(_), _) => {
                panic!("Attempt to compare List with non-List")
            }
            (LargeList(_), _) => todo!(),
            (ListView(_), _) => todo!(),
            (LargeListView(_), _) => todo!(),
            (Map(_), Map(_)) => todo!(),
            (Map(left), Null) => {
                if left.is_null(0) {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Map(_), _) => {
                panic!("Attempt to compare Map with non-Map")
            }
            (Date32(v1), Date32(v2)) => v1.cmp(v2),
            (Date32(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Date32(_), _) => panic!("Attempt to compare Date32 with non-Date32"),
            (Date64(v1), Date64(v2)) => v1.cmp(v2),
            (Date64(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Date64(_), _) => panic!("Attempt to compare Date64 with non-Date64"),
            (Time32Second(v1), Time32Second(v2)) => v1.cmp(v2),
            (Time32Second(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Time32Second(_), _) => panic!("Attempt to compare Time32Second with non-Time32Second"),
            (Time32Millisecond(v1), Time32Millisecond(v2)) => v1.cmp(v2),
            (Time32Millisecond(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Time32Millisecond(_), _) => {
                panic!("Attempt to compare Time32Millisecond with non-Time32Millisecond")
            }
            (Time64Microsecond(v1), Time64Microsecond(v2)) => v1.cmp(v2),
            (Time64Microsecond(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Time64Microsecond(_), _) => {
                panic!("Attempt to compare Time64Microsecond with non-Time64Microsecond")
            }
            (Time64Nanosecond(v1), Time64Nanosecond(v2)) => v1.cmp(v2),
            (Time64Nanosecond(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Time64Nanosecond(_), _) => {
                panic!("Attempt to compare Time64Nanosecond with non-Time64Nanosecond")
            }
            (TimestampSecond(v1, _), TimestampSecond(v2, _)) => v1.cmp(v2),
            (TimestampSecond(v1, _), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (TimestampSecond(_, _), _) => {
                panic!("Attempt to compare TimestampSecond with non-TimestampSecond")
            }
            (TimestampMillisecond(v1, _), TimestampMillisecond(v2, _)) => v1.cmp(v2),
            (TimestampMillisecond(v1, _), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (TimestampMillisecond(_, _), _) => {
                panic!("Attempt to compare TimestampMillisecond with non-TimestampMillisecond")
            }
            (TimestampMicrosecond(v1, _), TimestampMicrosecond(v2, _)) => v1.cmp(v2),
            (TimestampMicrosecond(v1, _), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (TimestampMicrosecond(_, _), _) => {
                panic!("Attempt to compare TimestampMicrosecond with non-TimestampMicrosecond")
            }
            (TimestampNanosecond(v1, _), TimestampNanosecond(v2, _)) => v1.cmp(v2),
            (TimestampNanosecond(v1, _), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (TimestampNanosecond(_, _), _) => {
                panic!("Attempt to compare TimestampNanosecond with non-TimestampNanosecond")
            }
            (IntervalYearMonth(v1), IntervalYearMonth(v2)) => v1.cmp(v2),
            (IntervalYearMonth(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (IntervalYearMonth(_), _) => {
                panic!("Attempt to compare IntervalYearMonth with non-IntervalYearMonth")
            }
            (IntervalDayTime(v1), IntervalDayTime(v2)) => v1.cmp(v2),
            (IntervalDayTime(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (IntervalDayTime(_), _) => {
                panic!("Attempt to compare IntervalDayTime with non-IntervalDayTime")
            }
            (IntervalMonthDayNano(v1), IntervalMonthDayNano(v2)) => v1.cmp(v2),
            (IntervalMonthDayNano(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (IntervalMonthDayNano(_), _) => {
                panic!("Attempt to compare IntervalMonthDayNano with non-IntervalMonthDayNano")
            }
            (DurationSecond(v1), DurationSecond(v2)) => v1.cmp(v2),
            (DurationSecond(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (DurationSecond(_), _) => {
                panic!("Attempt to compare DurationSecond with non-DurationSecond")
            }
            (DurationMillisecond(v1), DurationMillisecond(v2)) => v1.cmp(v2),
            (DurationMillisecond(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (DurationMillisecond(_), _) => {
                panic!("Attempt to compare DurationMillisecond with non-DurationMillisecond")
            }
            (DurationMicrosecond(v1), DurationMicrosecond(v2)) => v1.cmp(v2),
            (DurationMicrosecond(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (DurationMicrosecond(_), _) => {
                panic!("Attempt to compare DurationMicrosecond with non-DurationMicrosecond")
            }
            (DurationNanosecond(v1), DurationNanosecond(v2)) => v1.cmp(v2),
            (DurationNanosecond(v1), Null) => {
                if v1.is_none() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (DurationNanosecond(_), _) => {
                panic!("Attempt to compare DurationNanosecond with non-DurationNanosecond")
            }
            (Struct(_arr), Struct(_arr2)) => todo!(),
            (Struct(arr), Null) => {
                if arr.is_empty() {
                    Ordering::Equal
                } else {
                    Ordering::Greater
                }
            }
            (Struct(_arr), _) => panic!("Attempt to compare Struct with non-Struct"),
            (Dictionary(_k1, v1), Dictionary(_k2, v2)) => Self(*v1.clone()).cmp(&Self(*v2.clone())),
            (Dictionary(_, v1), Null) => Self(*v1.clone()).cmp(&Self(ScalarValue::Null)),
            (Dictionary(_, _), _) => panic!("Attempt to compare Dictionary with non-Dictionary"),
            // What would a btree of unions even look like?  May not be possible.
            (Union(_, _, _), _) => todo!("Support for union scalars"),
            (RunEndEncoded(_, _, _), _) => {
                todo!("Support for run-end encoded scalars")
            }
            (Null, Null) => Ordering::Equal,
            (Null, _) => todo!(),
        }
    }
}

/// Returns the first index `i` in `[lo, hi)` for which `pred(i)` is `false`.
///
/// `pred` must be `true` for a (possibly empty) prefix of the range and `false`
/// for the rest, i.e. the range is partitioned by `pred`.
fn partition_point(lo: usize, hi: usize, mut pred: impl FnMut(usize) -> bool) -> usize {
    let mut lo = lo;
    let mut hi = hi;
    while lo < hi {
        let mid = lo + (hi - lo) / 2;
        if pred(mid) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    lo
}

/// Builds a comparator over two array accessors of the same `Ord` item type,
/// matching arrow's NULLs-first ascending order (`null < non-null`, `null == null`).
///
/// Unlike [`make_comparator`], the returned closure is generic (not boxed), so the
/// element comparison inlines into the scan instead of dispatching through a vtable
/// on every call.
fn accessor_cmp<'a, T, L, R>(left: L, right: R) -> impl Fn(usize, usize) -> Ordering + 'a
where
    T: Ord,
    L: ArrayAccessor<Item = T> + 'a,
    R: ArrayAccessor<Item = T> + 'a,
{
    move |i, j| match (left.is_null(i), right.is_null(j)) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => left.value(i).cmp(&right.value(j)),
    }
}

/// Views `arr` as `PrimitiveArray<K>` for comparison. Zero-copy (shared buffers)
/// when `arr` already has type `K`; otherwise — a logical type whose physical
/// storage is `K::Native`, e.g. `Date32`/`Time32` over `i32` or `Timestamp`/
/// `Duration` over `i64` — the array data is relabeled to `K` without copying the
/// values, so all such logical types share one comparison path.
fn reinterpret_primitive<K: ArrowPrimitiveType>(arr: &dyn Array) -> Result<PrimitiveArray<K>> {
    if let Some(arr) = arr.as_primitive_opt::<K>() {
        return Ok(arr.clone());
    }
    let data = arr
        .to_data()
        .into_builder()
        .data_type(K::DATA_TYPE)
        .build()
        .map_err(|e| {
            Error::internal(format!(
                "failed to reinterpret {} as {}: {e}",
                arr.data_type(),
                K::DATA_TYPE
            ))
        })?;
    Ok(PrimitiveArray::<K>::from(data))
}

/// Like [`accessor_cmp`] but for primitive columns, comparing native values with
/// [`ArrowNativeTypeOp::compare`] (total order, so floats match arrow's NaN-last
/// `make_comparator` ordering).
fn primitive_cmp<'a, T>(
    left: &'a PrimitiveArray<T>,
    right: &'a PrimitiveArray<T>,
) -> impl Fn(usize, usize) -> Ordering + 'a
where
    T: ArrowPrimitiveType,
{
    move |i, j| match (left.is_null(i), right.is_null(j)) {
        (true, true) => Ordering::Equal,
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        (false, false) => left.value(i).compare(right.value(j)),
    }
}

/// Satisfies scalar queries by searching the `page_lookup.lance` batch directly.
///
/// The batch holds one row per page with columns `min | max | null_count | page_idx`,
/// sorted ascending by `min` with NULLs first (the order the index is trained in).
/// Both query paths binary-search the sorted `min` column for a starting row and
/// scan forward filtering by `max`:
///
/// - Equality / `IN` (`candidate_pages_for_values`) dispatch on the query's
///   *physical storage type* to a monomorphized, inlined comparator: numerics go
///   through `scan_native` (logical types sharing a native — e.g. `Date32` and
///   `Int32` — fold to one path), byte-likes through `scan_accessor`. Only types
///   without a native fast path (struct-backed intervals, booleans) fall back to the
///   boxed [`make_comparator`] via `scan_fallback`.
/// - Range searches (`pages_between`) currently use [`make_comparator`] directly.
#[derive(Debug, PartialEq, DeepSizeOf)]
pub struct BTreeLookup {
    /// One row per page (`min | max | null_count | page_idx`), sorted by `min`.
    batch: RecordBatch,
    /// Pages with at least one null value (does not include `all_null_pages`).
    null_pages: Vec<u32>,
    /// Pages that are entirely null.
    all_null_pages: Vec<u32>,
    /// Index of the first row whose `max` is non-null. Entirely-null pages sort to
    /// the front (NULLs first) and are skipped when searching value ranges.
    search_start: usize,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum Matches {
    Some(u32),
    All(u32),
}

impl Matches {
    fn page_id(&self) -> u32 {
        match self {
            Self::Some(page_id) => *page_id,
            Self::All(page_id) => *page_id,
        }
    }
}

impl BTreeLookup {
    /// Build a lookup over the `page_lookup.lance` batch. The batch is retained as
    /// the source of truth; only the small null-page index lists are precomputed.
    fn try_new(batch: RecordBatch) -> Result<Self> {
        let mut null_pages = Vec::new();
        let mut all_null_pages = Vec::new();
        let mut search_start = batch.num_rows();

        if batch.num_rows() > 0 {
            let maxs = batch.column(1);
            let null_counts = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| Error::internal("BTree lookup null_count column must be UInt32"))?;
            let page_numbers = batch
                .column(3)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .ok_or_else(|| Error::internal("BTree lookup page_idx column must be UInt32"))?;

            for idx in 0..batch.num_rows() {
                let page_number = page_numbers.values()[idx];
                // An entirely-null page has a null `max`; it is never searched by value.
                if maxs.is_null(idx) {
                    all_null_pages.push(page_number);
                    continue;
                }
                if search_start == batch.num_rows() {
                    search_start = idx;
                }
                if null_counts.values()[idx] > 0 {
                    null_pages.push(page_number);
                }
            }
        } else {
            search_start = 0;
        }

        Ok(Self {
            batch,
            null_pages,
            all_null_pages,
            search_start,
        })
    }

    fn page_numbers(&self) -> Result<&UInt32Array> {
        self.batch
            .column(3)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .ok_or_else(|| Error::internal("BTree lookup page_idx column must be UInt32"))
    }

    // All pages that could have a value equal to val
    fn pages_eq(&self, query: &OrderableScalarValue) -> Result<Vec<Matches>> {
        if query.0.is_null() {
            Ok(self.pages_null())
        } else {
            let query_arr = query.0.to_array_of_size(1)?;
            let pages = self.candidate_pages_for_values(query_arr.as_ref())?;
            Ok(pages.into_iter().map(Matches::Some).collect())
        }
    }

    // All pages that could have a value equal to one of the values
    fn pages_in(
        &self,
        values: impl IntoIterator<Item = OrderableScalarValue>,
    ) -> Result<Vec<Matches>> {
        // Equality lookups never produce a full-page (`Matches::All`) match because a
        // single value cannot cover an entire page's range, so every candidate is
        // `Matches::Some`. Refining this for low-cardinality data is the TODO in
        // `pages_between`.
        let values = values.into_iter();
        let mut has_null = false;
        let mut non_null = Vec::with_capacity(values.size_hint().0);
        for val in values {
            if val.0.is_null() {
                has_null = true;
            } else {
                non_null.push(val.0);
            }
        }

        // Build a single array holding every queried value so the comparators are
        // constructed once and reused across all of them, rather than per value.
        let mut all_pages = if non_null.is_empty() {
            Vec::new()
        } else {
            let query_arr = ScalarValue::iter_to_array(non_null)?;
            self.candidate_pages_for_values(query_arr.as_ref())?
        };
        if has_null {
            all_pages.extend(self.pages_null().into_iter().map(|m| m.page_id()));
        }
        all_pages.sort_unstable();
        all_pages.dedup();
        Ok(all_pages.into_iter().map(Matches::Some).collect())
    }

    /// Candidate page numbers (deduped, ascending) for an equality search against
    /// every value in `query`. A page is a candidate when its `[min, max]` range
    /// could contain the value, i.e. `min <= value <= max`.
    ///
    /// The comparators are built once over the whole `query` array and reused for
    /// each value, so an N-value `IN` costs three comparator constructions instead
    /// of three per value.
    fn candidate_pages_for_values(&self, query: &dyn Array) -> Result<Vec<u32>> {
        let num_rows = self.batch.num_rows();
        if self.search_start >= num_rows || query.is_empty() {
            return Ok(vec![]);
        }

        let mins = self.batch.column(0).as_ref();
        let maxs = self.batch.column(1).as_ref();
        let page_ids = self.page_numbers()?.values();

        // Compare against the page columns with a native, monomorphized comparator
        // that inlines, rather than the boxed `DynComparator` from `make_comparator`
        // (one vtable call per comparison). Logical types that share a physical
        // storage type route to one path via a zero-copy reinterpret, so e.g. every
        // date/time/timestamp/duration type reuses the `i32`/`i64` path instead of
        // generating its own. Types with no native path (intervals with struct
        // natives, booleans, ...) take the `make_comparator` fallback. The query
        // array always matches the column type, so its type selects the branch.
        use DataType::*;
        match query.data_type() {
            Int8 => self.scan_native::<Int8Type>(mins, maxs, query, page_ids),
            Int16 => self.scan_native::<Int16Type>(mins, maxs, query, page_ids),
            // i32-backed: Int32, Date32, Time32, Decimal32, year-month intervals.
            Int32 | Date32 | Time32(_) | Decimal32(_, _) | Interval(IntervalUnit::YearMonth) => {
                self.scan_native::<Int32Type>(mins, maxs, query, page_ids)
            }
            // i64-backed: Int64, Date64, Time64, Timestamp, Duration, Decimal64.
            Int64 | Date64 | Time64(_) | Timestamp(_, _) | Duration(_) | Decimal64(_, _) => {
                self.scan_native::<Int64Type>(mins, maxs, query, page_ids)
            }
            UInt8 => self.scan_native::<UInt8Type>(mins, maxs, query, page_ids),
            UInt16 => self.scan_native::<UInt16Type>(mins, maxs, query, page_ids),
            UInt32 => self.scan_native::<UInt32Type>(mins, maxs, query, page_ids),
            UInt64 => self.scan_native::<UInt64Type>(mins, maxs, query, page_ids),
            Float16 => self.scan_native::<Float16Type>(mins, maxs, query, page_ids),
            Float32 => self.scan_native::<Float32Type>(mins, maxs, query, page_ids),
            Float64 => self.scan_native::<Float64Type>(mins, maxs, query, page_ids),
            Decimal128(_, _) => self.scan_native::<Decimal128Type>(mins, maxs, query, page_ids),
            Decimal256(_, _) => self.scan_native::<Decimal256Type>(mins, maxs, query, page_ids),
            Utf8 => Ok(self.scan_accessor(
                mins.as_string::<i32>(),
                maxs.as_string::<i32>(),
                query.as_string::<i32>(),
                page_ids,
            )),
            LargeUtf8 => Ok(self.scan_accessor(
                mins.as_string::<i64>(),
                maxs.as_string::<i64>(),
                query.as_string::<i64>(),
                page_ids,
            )),
            Binary => Ok(self.scan_accessor(
                mins.as_binary::<i32>(),
                maxs.as_binary::<i32>(),
                query.as_binary::<i32>(),
                page_ids,
            )),
            LargeBinary => Ok(self.scan_accessor(
                mins.as_binary::<i64>(),
                maxs.as_binary::<i64>(),
                query.as_binary::<i64>(),
                page_ids,
            )),
            FixedSizeBinary(_) => Ok(self.scan_accessor(
                mins.as_fixed_size_binary(),
                maxs.as_fixed_size_binary(),
                query.as_fixed_size_binary(),
                page_ids,
            )),
            _ => self.scan_fallback(mins, maxs, query, page_ids),
        }
    }

    /// Native-comparator equality scan for a primitive physical type `K`. The page
    /// columns and `query` are reinterpreted to `PrimitiveArray<K>` (zero-copy when
    /// already that type) and compared with [`primitive_cmp`].
    fn scan_native<K: ArrowPrimitiveType>(
        &self,
        mins: &dyn Array,
        maxs: &dyn Array,
        query: &dyn Array,
        page_ids: &[u32],
    ) -> Result<Vec<u32>> {
        let mins = reinterpret_primitive::<K>(mins)?;
        let maxs = reinterpret_primitive::<K>(maxs)?;
        let query = reinterpret_primitive::<K>(query)?;
        Ok(self.scan_equality_pages(
            query.len(),
            page_ids,
            |idx| maxs.is_null(idx),
            primitive_cmp(&mins, &query),
            primitive_cmp(&maxs, &query),
            primitive_cmp(&mins, &mins),
        ))
    }

    /// Native-comparator equality scan for byte-like columns (`Utf8`/`Binary`/
    /// `FixedSizeBinary` and their large variants), compared lexicographically via
    /// [`accessor_cmp`].
    fn scan_accessor<T, A>(&self, mins: A, maxs: A, query: A, page_ids: &[u32]) -> Vec<u32>
    where
        T: Ord,
        A: ArrayAccessor<Item = T> + Copy,
    {
        self.scan_equality_pages(
            query.len(),
            page_ids,
            |idx| maxs.is_null(idx),
            accessor_cmp(mins, query),
            accessor_cmp(maxs, query),
            accessor_cmp(mins, mins),
        )
    }

    /// Fallback equality scan for types without a native path (intervals with struct
    /// natives, booleans, ...), using arrow's boxed `make_comparator`.
    fn scan_fallback(
        &self,
        mins: &dyn Array,
        maxs: &dyn Array,
        query: &dyn Array,
        page_ids: &[u32],
    ) -> Result<Vec<u32>> {
        // The batch is sorted ascending by `min` with NULLs first; compare the query
        // values the same way so the binary searches stay consistent.
        let opts = SortOptions {
            descending: false,
            nulls_first: true,
        };
        let cmp_min = make_comparator(mins, query, opts)?;
        let cmp_max = make_comparator(maxs, query, opts)?;
        let cmp_min_min = make_comparator(mins, mins, opts)?;
        Ok(self.scan_equality_pages(
            query.len(),
            page_ids,
            |idx| maxs.is_null(idx),
            cmp_min,
            cmp_max,
            cmp_min_min,
        ))
    }

    /// Binary-search + forward-scan the page batch for equality candidates.
    ///
    /// Monomorphized over the comparator closures so a typed-native comparator
    /// inlines (no per-call vtable dispatch). The closures encode NULLs-first,
    /// ascending order:
    ///   * `max_is_null(i)` — whether page `i`'s `max` is null (an all-null page)
    ///   * `cmp_min(i, j)` — page `i`'s `min` vs query value `j`
    ///   * `cmp_max(i, j)` — page `i`'s `max` vs query value `j`
    ///   * `cmp_min_min(i, anchor)` — two page `min`s, to expand left onto a straddle
    fn scan_equality_pages(
        &self,
        num_query: usize,
        page_ids: &[u32],
        max_is_null: impl Fn(usize) -> bool,
        cmp_min: impl Fn(usize, usize) -> Ordering,
        cmp_max: impl Fn(usize, usize) -> Ordering,
        cmp_min_min: impl Fn(usize, usize) -> Ordering,
    ) -> Vec<u32> {
        let num_rows = self.batch.num_rows();
        // High-cardinality lookups hit ~one page per value; presize to avoid the
        // element-by-element `RawVec` growth that profiling flagged.
        let mut pages = Vec::with_capacity(num_query);
        for j in 0..num_query {
            // Start row: peek a little to the left of the value. A query for 7 must
            // still reach a page like [5, 10], so we include every page whose `min`
            // equals the largest `min` strictly less than the value.
            let p = partition_point(0, num_rows, |i| cmp_min(i, j) == Ordering::Less);
            let start = if p == 0 {
                self.search_start
            } else {
                let anchor = p - 1;
                partition_point(0, p, |i| cmp_min_min(i, anchor) == Ordering::Less)
            }
            .max(self.search_start);

            // End row: pages whose `min` exceeds the value cannot match.
            let end = partition_point(start, num_rows, |i| cmp_min(i, j) != Ordering::Greater);

            // The window splits at `p` (first row with `min >= value`):
            //   * `[start, p)` — the peek-left/straddle region (`min < value`). A page
            //     here matches only if its `max` reaches the value, so it needs the
            //     filter, and it may include a null-`min`/null-`max` straddle page.
            //   * `[p, end)` — rows with `min == value`. These always match (`max >=
            //     min == value`) and can't have a null `max` (all-null pages sort to
            //     the front, before `search_start <= start`), so we copy them in one
            //     slice instead of pushing per row.
            let bulk_start = p.max(start);
            for (offset, &page_id) in page_ids[start..bulk_start].iter().enumerate() {
                let idx = start + offset;
                // All-null pages are only matched by IS NULL queries.
                if max_is_null(idx) {
                    continue;
                }
                // Candidate when the page's `max` reaches the value (`max >= value`).
                if cmp_max(idx, j) != Ordering::Less {
                    pages.push(page_id);
                }
            }
            pages.extend_from_slice(&page_ids[bulk_start..end]);
        }

        pages.sort_unstable();
        pages.dedup();
        pages
    }

    // All pages that could have a value in the range
    fn pages_between(
        &self,
        range: (Bound<&OrderableScalarValue>, Bound<&OrderableScalarValue>),
    ) -> Result<Vec<Matches>> {
        let num_rows = self.batch.num_rows();
        // No searchable (non-all-null) pages.
        if self.search_start >= num_rows {
            return Ok(vec![]);
        }

        let mins = self.batch.column(0).as_ref();
        let maxs = self.batch.column(1).as_ref();
        let page_numbers = self.page_numbers()?;

        // The batch is sorted ascending by `min` with NULLs first; compare bounds
        // the same way so the binary searches and the null `min` of a straddling
        // page are handled consistently.
        let opts = SortOptions {
            descending: false,
            nulls_first: true,
        };
        // Bounds become 1-row arrays of the column type so arrow's type-dispatched
        // comparator can compare them against the `min`/`max` columns.
        let lower_arr = match range.0 {
            Bound::Unbounded => None,
            Bound::Included(v) | Bound::Excluded(v) => Some(v.0.to_array_of_size(1)?),
        };
        let upper_arr = match range.1 {
            Bound::Unbounded => None,
            Bound::Included(v) | Bound::Excluded(v) => Some(v.0.to_array_of_size(1)?),
        };

        // Start row: peek a little to the left of the lower bound. A query for 7
        // must still reach a page like [5, 10], so we include every page whose
        // `min` equals the largest `min` strictly less than the lower bound.
        let start = match &lower_arr {
            None => self.search_start,
            Some(lower) => {
                let cmp = make_comparator(mins, lower.as_ref(), opts)?;
                // first row with min >= lower
                let p = partition_point(0, num_rows, |i| cmp(i, 0) == Ordering::Less);
                if p == 0 {
                    self.search_start
                } else {
                    // first row sharing the straddling page's `min`
                    let straddle = mins.slice(p - 1, 1);
                    let cmp = make_comparator(mins, straddle.as_ref(), opts)?;
                    partition_point(0, p, |i| cmp(i, 0) == Ordering::Less)
                }
            }
        }
        .max(self.search_start);

        // End row: pages whose `min` exceeds the upper bound cannot match. The
        // upper bound is treated as inclusive even when the query bound is
        // exclusive, so an [x, x) query still reaches a page whose `min` == x.
        let end = match &upper_arr {
            None => num_rows,
            Some(upper) => {
                let cmp = make_comparator(mins, upper.as_ref(), opts)?;
                partition_point(start, num_rows, |i| cmp(i, 0) != Ordering::Greater)
            }
        };

        if start >= end {
            return Ok(vec![]);
        }

        // Comparators reused across the candidate rows.
        let cmp_max_lower = lower_arr
            .as_ref()
            .map(|l| make_comparator(maxs, l.as_ref(), opts))
            .transpose()?;
        let cmp_min_lower = lower_arr
            .as_ref()
            .map(|l| make_comparator(mins, l.as_ref(), opts))
            .transpose()?;
        let cmp_max_upper = upper_arr
            .as_ref()
            .map(|u| make_comparator(maxs, u.as_ref(), opts))
            .transpose()?;

        let mut matches = Vec::new();
        for idx in start..end {
            // All-null pages are only matched by IS NULL queries.
            if maxs.is_null(idx) {
                continue;
            }

            // Candidate filter: the page's `max` reaches the lower bound.
            let lower_ok = match (range.0, &cmp_max_lower) {
                (Bound::Unbounded, _) => true,
                (Bound::Included(_), Some(cmp)) => cmp(idx, 0) != Ordering::Less, // max >= lower
                (Bound::Excluded(_), Some(cmp)) => cmp(idx, 0) == Ordering::Greater, // max > lower
                _ => unreachable!("lower bound and its comparator are constructed together"),
            };
            if !lower_ok {
                continue;
            }

            let page_number = page_numbers.values()[idx];

            // A page with a null `min` straddles the NULL/non-NULL boundary, so it
            // is only ever a partial match.
            if mins.is_null(idx) {
                matches.push(Matches::Some(page_number));
                continue;
            }

            // Full match requires the page to sit entirely within the query range.
            let lower_full = match (range.0, &cmp_min_lower) {
                (Bound::Unbounded, _) => true,
                (Bound::Included(_), Some(cmp)) => cmp(idx, 0) != Ordering::Less, // min >= lower
                (Bound::Excluded(_), Some(cmp)) => cmp(idx, 0) == Ordering::Greater, // min > lower
                _ => unreachable!("lower bound and its comparator are constructed together"),
            };
            let upper_full = match (range.1, &cmp_max_upper) {
                (Bound::Unbounded, _) => true,
                (Bound::Included(_), Some(cmp)) => cmp(idx, 0) != Ordering::Greater, // max <= upper
                (Bound::Excluded(_), Some(cmp)) => cmp(idx, 0) == Ordering::Less,    // max < upper
                _ => unreachable!("upper bound and its comparator are constructed together"),
            };
            if lower_full && upper_full {
                matches.push(Matches::All(page_number));
            } else {
                matches.push(Matches::Some(page_number));
            }
        }

        Ok(matches)
    }

    fn pages_null(&self) -> Vec<Matches> {
        self.null_pages
            .iter()
            .copied()
            .map(Matches::Some)
            .chain(self.all_null_pages.iter().copied().map(Matches::All))
            .collect()
    }
}

// We only need to open a file reader for pages if we need to load a page.  If all
// pages are cached we don't open it.  If we do open it we should only open it once.
#[derive(Clone)]
struct LazyIndexReader {
    index_reader: Arc<tokio::sync::Mutex<Option<Arc<dyn IndexReader>>>>,
    store: Arc<dyn IndexStore>,
    ranges_to_files: Option<Arc<RangeInclusiveMap<u32, (String, u32)>>>,
}

impl LazyIndexReader {
    fn new(
        store: Arc<dyn IndexStore>,
        ranges_to_files: Option<Arc<RangeInclusiveMap<u32, (String, u32)>>>,
    ) -> Self {
        Self {
            index_reader: Arc::new(tokio::sync::Mutex::new(None)),
            store,
            ranges_to_files,
        }
    }

    async fn get(&self) -> Result<Arc<dyn IndexReader>> {
        let mut reader = self.index_reader.lock().await;
        if reader.is_none() {
            let index_reader = if let Some(ranges_to_files) = &self.ranges_to_files {
                Arc::new(LazyRangedIndexReader::new(
                    self.store.clone(),
                    ranges_to_files.clone(),
                ))
            } else {
                self.store.open_index_file(BTREE_PAGES_NAME).await?
            };
            *reader = Some(index_reader);
        }
        Ok(reader.as_ref().unwrap().clone())
    }
}

/// Index reader to dispatch page query to corresponding ranged page-files.
struct LazyRangedIndexReader {
    #[allow(clippy::type_complexity)]
    readers:
        Arc<tokio::sync::Mutex<HashMap<String, Arc<tokio::sync::OnceCell<Arc<dyn IndexReader>>>>>>,
    store: Arc<dyn IndexStore>,
    ranges_to_files: Arc<RangeInclusiveMap<u32, (String, u32)>>,
}

impl LazyRangedIndexReader {
    fn new(
        store: Arc<dyn IndexStore>,
        ranges_to_files: Arc<RangeInclusiveMap<u32, (String, u32)>>,
    ) -> Self {
        Self {
            readers: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            store,
            ranges_to_files,
        }
    }

    async fn get_reader(&self, file_name: &str) -> Result<Arc<dyn IndexReader>> {
        let reader_cell = {
            let mut guard = self.readers.lock().await;
            guard
                .entry(file_name.to_string())
                .or_insert_with(|| Arc::new(tokio::sync::OnceCell::new()))
                .clone()
        };
        let reader = reader_cell
            .get_or_try_init(|| async { self.store.open_index_file(file_name).await })
            .await?;
        Ok(reader.clone())
    }

    async fn get_reader_and_local_page_idx(
        &self,
        page_idx: u32,
    ) -> Result<(Arc<dyn IndexReader>, u32)> {
        let (page_file_name, offset) = self.ranges_to_files.get(&page_idx).ok_or_else(|| {
            Error::internal(format!(
                "Unexpected page index, index {} is out of range.",
                page_idx
            ))
        })?;
        let reader = self.get_reader(page_file_name).await?;
        Ok((reader.clone(), page_idx - *offset))
    }
}

#[async_trait]
impl IndexReader for LazyRangedIndexReader {
    async fn read_record_batch(&self, n: u64, batch_size: u64) -> Result<RecordBatch> {
        let (reader, local_page_idx) = self.get_reader_and_local_page_idx(n as u32).await?;
        reader
            .read_record_batch(local_page_idx as u64, batch_size)
            .await
    }

    async fn read_range(
        &self,
        _range: std::ops::Range<usize>,
        _projection: Option<&[&str]>,
    ) -> Result<RecordBatch> {
        unimplemented!("Read range is not implemented for lazy page file reader.");
    }

    async fn num_batches(&self, batch_size: u64) -> u32 {
        let mut total_batches = 0;
        for (_, (file_name, _)) in self.ranges_to_files.iter() {
            let reader = self
                .get_reader(file_name)
                .await
                .unwrap_or_else(|_| panic!("Cannot open page file {}.", file_name));
            total_batches += reader.as_ref().num_batches(batch_size).await;
        }
        total_batches
    }

    fn num_rows(&self) -> usize {
        unimplemented!("only async functions are available for lazy page index reader.");
    }

    fn schema(&self) -> &lance_core::datatypes::Schema {
        unimplemented!("only async functions are available for lazy page index reader.");
    }
}

/// A btree index satisfies scalar queries using a b tree
///
/// The upper layers of the btree are expected to be cached and, when unloaded,
/// are stored in a btree structure in memory.  The leaves of the btree are left
/// to be searched by some other kind of index (currently a flat search).
///
/// This strikes a balance between an expensive memory structure containing all
/// of the values and an expensive disk structure that can't be efficiently searched.
///
/// For example, given 1Bi values we can store 256Ki leaves of size 4Ki.  We only
/// need memory space for 256Ki leaves (depends on the data type but usually a few MiB
/// at most) and can narrow our search to 4Ki values.
///
// Cache key implementation for type-safe cache access
#[derive(Debug, Clone, DeepSizeOf)]
pub struct CachedScalarIndex(Arc<dyn ScalarIndex>);

impl CachedScalarIndex {
    pub fn new(index: Arc<dyn ScalarIndex>) -> Self {
        Self(index)
    }

    pub fn into_inner(self) -> Arc<dyn ScalarIndex> {
        self.0
    }
}

#[derive(Debug, Clone)]
pub struct BTreePageKey {
    pub page_number: u32,
}

impl CacheKey for BTreePageKey {
    type ValueType = FlatIndex;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        format!("page-{}", self.page_number).into()
    }

    fn type_name() -> &'static str {
        "BTreePage"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.btree-page-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_u32(self.page_number);
    }

    fn codec() -> Option<CacheCodec> {
        // Pages are cached as `FlatIndex` values (see `ValueType` above).
        Some(CacheCodec::from_impl::<FlatIndex>())
    }
}

/// The serializable state of a [`BTreeIndex`].
///
/// A `BTreeIndex` holds non-serializable infrastructure (an `IndexStore`, a
/// cache handle, a fragment-reuse index). `BTreeIndexState` captures just the
/// data needed to rebuild it: the `page_lookup.lance` batch (from which
/// `BTreeIndex::try_from_serialized` reconstructs the in-memory lookup with
/// no IO) plus the page batch size and range-partition map.
#[derive(Debug, Clone)]
struct BTreeIndexState {
    lookup_batch: RecordBatch,
    batch_size: u64,
    ranges_to_files: Option<Arc<RangeInclusiveMap<u32, (String, u32)>>>,
}

impl DeepSizeOf for BTreeIndexState {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        // `ranges_to_files` is tiny and `RangeInclusiveMap` is not `DeepSizeOf`;
        // the lookup batch dominates, matching how `BTreeIndex` accounts for itself.
        self.lookup_batch.deep_size_of_children(context)
    }
}

impl BTreeIndexState {
    fn from_index(index: &dyn ScalarIndex) -> Result<Self> {
        let btree = index.as_any().downcast_ref::<BTreeIndex>().ok_or_else(|| {
            Error::internal("BTreeIndexState::from_index called with a non-BTree index")
        })?;
        Ok(Self {
            lookup_batch: btree.page_lookup.batch.clone(),
            batch_size: btree.batch_size,
            ranges_to_files: btree.ranges_to_files.clone(),
        })
    }

    fn reconstruct(
        &self,
        store: Arc<dyn IndexStore>,
        index_cache: &LanceCache,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Arc<dyn ScalarIndex>> {
        let index = BTreeIndex::try_from_serialized(
            self.lookup_batch.clone(),
            store,
            index_cache,
            self.batch_size,
            self.ranges_to_files.clone(),
            frag_reuse_index,
        )?;
        Ok(Arc::new(index) as Arc<dyn ScalarIndex>)
    }
}

impl CacheCodecImpl for BTreeIndexState {
    const TYPE_ID: &'static str = "lance.scalar.BTreeIndexState";
    const CURRENT_VERSION: u32 = 1;

    /// Wire format:
    /// ```text
    /// HEADER    : BTreeIndexHeader proto (batch_size + page-range mapping)
    /// ARROW_IPC : page-lookup batch
    /// ```
    fn serialize(&self, w: &mut CacheEntryWriter<'_>) -> Result<()> {
        let ranges_to_files = match &self.ranges_to_files {
            None => Vec::new(),
            Some(ranges) => ranges
                .iter()
                .map(|(range, (path, page_offset))| RangeToFile {
                    start: *range.start(),
                    end: *range.end(),
                    page_offset: *page_offset,
                    path: path.clone(),
                })
                .collect(),
        };
        let header = BTreeIndexHeader {
            batch_size: self.batch_size,
            has_ranges_to_files: self.ranges_to_files.is_some(),
            ranges_to_files,
        };
        w.write_header(&header)?;
        w.write_ipc(&self.lookup_batch)?;
        Ok(())
    }

    fn deserialize(r: &mut CacheEntryReader<'_>) -> Result<Self> {
        let header: BTreeIndexHeader = r.read_header()?;
        let ranges_to_files = if header.has_ranges_to_files {
            let map: RangeInclusiveMap<u32, (String, u32)> = header
                .ranges_to_files
                .into_iter()
                .map(|entry| (entry.start..=entry.end, (entry.path, entry.page_offset)))
                .collect();
            Some(Arc::new(map))
        } else {
            None
        };
        let lookup_batch = r.read_ipc()?;
        Ok(Self {
            lookup_batch,
            batch_size: header.batch_size,
            ranges_to_files,
        })
    }
}

/// Cache key for a [`BTreeIndexState`]. The cache it is used with is already
/// namespaced per-index, so the key string is a constant.
struct BTreeIndexStateKey;

impl CacheKey for BTreeIndexStateKey {
    type ValueType = BTreeIndexState;

    fn key(&self) -> std::borrow::Cow<'_, str> {
        "state".into()
    }

    fn type_name() -> &'static str {
        "BTreeIndexState"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.scalar.btree-index-state-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_variant(0);
    }

    fn codec() -> Option<CacheCodec> {
        Some(CacheCodec::from_impl::<BTreeIndexState>())
    }
}

/// Note: this is very similar to the IVF index except we store the IVF part in a btree
/// for faster lookup
#[derive(Clone, Debug)]
pub struct BTreeIndex {
    page_lookup: Arc<BTreeLookup>,
    index_cache: WeakLanceCache,
    store: Arc<dyn IndexStore>,
    data_type: DataType,
    batch_size: u64,

    /// A map that translates a global_page_idx stored in the single lookup file into the
    /// specific page file and local_page_idx.
    ///
    /// This is the key data structure used for efficiently reading data from a merged,
    /// range-partitioned index. It stores mappings from a contiguous range of global page
    /// indices to a tuple containing:
    ///
    /// 1. The path to the corresponding page file (e.g., `part_i_page_file.lance`).
    /// 2. The start offset that was used to calculate the local_page_idx for that partition.
    ///
    /// When a query needs to access a specific page using its `global_page_idx`:
    ///
    /// 1. The `global_page_idx` is used to look up its range in this `RangeInclusiveMap`,
    ///    and the map returns the `(file_path, start_offset)` tuple for that range.
    /// 3. The `local_page_idx` is calculated using the formula:
    ///    `local_page_idx = global_page_idx - start_offset`.
    /// 4. With the `file_path` and `local_page_idx`, the system can directly open the
    ///    correct partition file and read the specific page.
    ///
    /// # Example
    ///
    /// If the map contains an entry `(100..=199) => ("part_2_page_file.lance", 100)`, and we
    /// need to find `global_page_idx = 142`:
    ///
    /// - The map finds that 142 falls within the range `100..=199`, and it returns
    ///   `("part_2_page_file.lance", 100)`.
    /// - The local page_idx is calculated: `142 - 100 = 42`.
    /// - The system now knows to read page `42` from the file `part_2_page_file.lance`.
    ranges_to_files: Option<Arc<RangeInclusiveMap<u32, (String, u32)>>>,
    frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
}

impl DeepSizeOf for BTreeIndex {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        // We don't include the index cache, or anything stored in it. For example:
        // sub_index and fri. `page_lookup` owns the lookup batch (the single source
        // of truth), so accounting for it covers the lookup data.
        self.page_lookup.deep_size_of_children(context) + self.store.deep_size_of_children(context)
    }
}

impl BTreeIndex {
    #[allow(clippy::too_many_arguments)]
    fn new(
        page_lookup: Arc<BTreeLookup>,
        store: Arc<dyn IndexStore>,
        data_type: DataType,
        index_cache: WeakLanceCache,
        batch_size: u64,
        ranges_to_files: Option<Arc<RangeInclusiveMap<u32, (String, u32)>>>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Self {
        Self {
            page_lookup,
            store,
            data_type,
            index_cache,
            batch_size,
            ranges_to_files,
            frag_reuse_index,
        }
    }

    /// For each key in `keys`, whether this index contains it — a batched
    /// existence check returning a mask aligned to `keys`.
    ///
    /// The per-key sibling of `search(Equals(..))`, but one call replaces N
    /// probes: keys are grouped by page using the same page resolution as
    /// [`ScalarIndex::search`] (`pages_eq`), each touched page is loaded once
    /// (session-cached), and membership is tested against the page's values via
    /// `FlatIndex::contains_values`. Avoids the per-key `SearchResult` /
    /// `RowAddrTreeMap` allocation when the caller only wants a yes/no.
    ///
    /// Intended for primary-key dedup, where keys are non-null; a null key maps
    /// to `false`.
    pub async fn contains_keys(
        &self,
        keys: &[ScalarValue],
        metrics: &dyn MetricsCollector,
    ) -> Result<Vec<bool>> {
        // Group each key (by input position) under every page whose value range
        // could hold it. Mirrors `search`'s page selection so the two agree.
        let mut by_page: HashMap<u32, Vec<(usize, OrderableScalarValue)>> = HashMap::new();
        for (idx, key) in keys.iter().enumerate() {
            if key.is_null() {
                continue;
            }
            let ov = OrderableScalarValue(key.clone());
            for matches in self.page_lookup.pages_eq(&ov)? {
                by_page
                    .entry(matches.page_id())
                    .or_default()
                    .push((idx, ov.clone()));
            }
        }

        let index_reader = LazyIndexReader::new(self.store.clone(), self.ranges_to_files.clone());
        let page_tasks = by_page.into_iter().map(|(page_number, entries)| {
            let index_reader = index_reader.clone();
            async move {
                let page = self.lookup_page(page_number, index_reader, metrics).await?;
                let needles: Vec<OrderableScalarValue> =
                    entries.iter().map(|(_, ov)| ov.clone()).collect();
                let present = page.contains_values(&needles)?;
                Result::Ok((entries, present))
            }
        });

        let mut result = vec![false; keys.len()];
        let page_results: Vec<_> = stream::iter(page_tasks)
            .buffer_unordered(get_num_compute_intensive_cpus())
            .try_collect()
            .await?;
        for (entries, present) in page_results {
            for (idx, ov) in entries {
                if present.contains(&ov) {
                    result[idx] = true;
                }
            }
        }
        Ok(result)
    }

    async fn lookup_page(
        &self,
        page_number: u32,
        index_reader: LazyIndexReader,
        metrics: &dyn MetricsCollector,
    ) -> Result<Arc<FlatIndex>> {
        let result = self
            .index_cache
            .get_or_insert_with_key_hit(BTreePageKey { page_number }, move || async move {
                self.read_page(page_number, index_reader, metrics).await
            })
            .await;
        match &result {
            Ok((_, true)) => metrics.record_index_cache_hit(),
            _ => metrics.record_index_cache_miss(),
        }
        result.map(|(page, _)| page)
    }

    #[instrument(level = "debug", skip_all)]
    async fn read_page(
        &self,
        page_number: u32,
        index_reader: LazyIndexReader,
        metrics: &dyn MetricsCollector,
    ) -> Result<FlatIndex> {
        metrics.record_part_load();
        info!(target: TRACE_IO_EVENTS, r#type=IO_TYPE_LOAD_SCALAR_PART, index_type="btree", part_id=page_number);
        let index_reader = index_reader.get().await?;
        let mut serialized_page = index_reader
            .read_record_batch(page_number as u64, self.batch_size)
            .await?;
        if let Some(frag_reuse_index_ref) = self.frag_reuse_index.as_ref() {
            serialized_page =
                frag_reuse_index_ref.remap_row_ids_record_batch(serialized_page, 1)?;
        }
        FlatIndex::try_new(serialized_page)
    }

    /// Compile a sargable predicate into a physical expr against the per-page
    /// schema ([values, ids]). Built once in `search` and shared across pages so
    /// a large IN-list is not re-materialized for every page.
    fn compile_predicate(&self, query: &SargableQuery) -> Result<Arc<dyn PhysicalExpr>> {
        let schema = Arc::new(Schema::new(vec![
            Field::new(BTREE_VALUES_COLUMN, self.data_type.clone(), true),
            Field::new(BTREE_IDS_COLUMN, DataType::UInt64, false),
        ]));
        let df_schema = DFSchema::try_from(schema)?;
        Ok(create_physical_expr(
            &query.to_expr(BTREE_VALUES_COLUMN.to_string()),
            &df_schema,
            &ExecutionProps::default(),
        )?)
    }

    async fn search_page(
        &self,
        query: &SargableQuery,
        matches: Matches,
        index_reader: LazyIndexReader,
        prebuilt: Option<&Arc<dyn PhysicalExpr>>,
        metrics: &dyn MetricsCollector,
    ) -> Result<NullableRowAddrSet> {
        let subindex = self
            .lookup_page(matches.page_id(), index_reader, metrics)
            .await?;

        match matches {
            // For a large IsIn the predicate is compiled once (see `search`) and
            // reused here, instead of rebuilding the whole IN-list per page.
            Matches::Some(_) => match prebuilt {
                Some(expr) => subindex.search_prebuilt(expr, metrics),
                None => subindex.search(query, metrics),
            },
            Matches::All(_) => Ok(match query {
                // This means we hit an all-null page so just grab all row ids as true
                SargableQuery::IsNull() => subindex.all_ignore_nulls(),
                _ => subindex.all(),
            }),
        }
    }

    #[instrument(level = "debug", skip_all)]
    fn try_from_serialized(
        data: RecordBatch,
        store: Arc<dyn IndexStore>,
        index_cache: &LanceCache,
        batch_size: u64,
        ranges_to_files: Option<Arc<RangeInclusiveMap<u32, (String, u32)>>>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
    ) -> Result<Self> {
        let data_type = data.column(0).data_type().clone();
        let page_lookup = Arc::new(BTreeLookup::try_new(data)?);

        Ok(Self::new(
            page_lookup,
            store,
            data_type,
            WeakLanceCache::from(index_cache),
            batch_size,
            ranges_to_files,
            frag_reuse_index,
        ))
    }

    async fn load(
        store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        index_cache: &LanceCache,
    ) -> Result<Arc<Self>> {
        let (page_lookup_file, standalone_partition_page_file) =
            match store.open_index_file(BTREE_LOOKUP_NAME).await {
                Ok(page_lookup_file) => (page_lookup_file, None),
                Err(original_err) if is_missing_lookup_error(&original_err) => {
                    let files = store.list_files_with_sizes().await?;
                    let Some((lookup_file, page_file)) = find_single_partition_files(&files)?
                    else {
                        return Err(original_err);
                    };
                    (
                        store.open_index_file(lookup_file).await?,
                        Some(page_file.to_string()),
                    )
                }
                Err(other_err) => return Err(other_err),
            };
        let num_rows_in_lookup = page_lookup_file.num_rows();
        let serialized_lookup = page_lookup_file
            .read_range(0..num_rows_in_lookup, None)
            .await?;
        let file_schema = page_lookup_file.schema();
        let batch_size = file_schema
            .metadata
            .get(BATCH_SIZE_META_KEY)
            .map(|bs| bs.parse().unwrap_or(DEFAULT_BTREE_BATCH_SIZE))
            .unwrap_or(DEFAULT_BTREE_BATCH_SIZE);

        let range_partitioned = file_schema
            .metadata
            .get(RANGE_PARTITIONED_META_KEY)
            .map(|bs| bs.parse().unwrap_or(DEFAULT_RANGE_PARTITIONED))
            .unwrap_or(DEFAULT_RANGE_PARTITIONED);
        // For range-partitioned indices, construct the `ranges_to_files` map.
        // This converts the list of (partition ID, page count) from metadata into a map
        // from a global page range to its corresponding file and starting offset.
        let ranges_to_files = if let Some(page_file_name) = standalone_partition_page_file {
            let page_numbers = serialized_lookup
                .column(3)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let max_page_number = page_numbers.values().iter().copied().max().unwrap_or(0);
            let mut range_map = RangeInclusiveMap::new();
            range_map.insert(0..=max_page_number, (page_file_name, 0));
            Some(Arc::new(range_map))
        } else if range_partitioned {
            let part_sizes_str = file_schema
            .metadata
            .get(PAGE_NUM_PER_RANGE_PARTITION_META_KEY)
            .expect("Range-partitioned Btree lookup file must have page-number-per-range-file metadata!");
            let part_sizes_vec: Vec<(u64, u32)> = serde_json::from_str(part_sizes_str)?;
            let mut offset: u32 = 0;

            let range_map = part_sizes_vec
                .into_iter()
                .map(|(id, size)| {
                    let range = offset..=(offset + size - 1);
                    let file_with_size = (part_page_data_file_path(id), offset);
                    offset += size;
                    (range, file_with_size)
                })
                .collect();

            Some(Arc::new(range_map))
        } else {
            None
        };

        Ok(Arc::new(Self::try_from_serialized(
            serialized_lookup,
            store,
            index_cache,
            batch_size,
            ranges_to_files,
            frag_reuse_index,
        )?))
    }

    // For legacy reasons a btree index expects the training input to use value/_rowid
    fn train_schema(&self) -> Schema {
        let value_field = Field::new(VALUE_COLUMN_NAME, self.data_type.clone(), true);
        let row_id_field = Field::new(ROW_ID, DataType::UInt64, false);
        Schema::new(vec![value_field, row_id_field])
    }

    /// Create a stream of all the data in the index, in the same format used to train the index
    async fn data_stream(&self) -> Result<SendableRecordBatchStream> {
        let lazy_reader = LazyIndexReader::new(self.store.clone(), self.ranges_to_files.clone());
        let reader = lazy_reader.get().await?;
        let new_schema = Arc::new(self.train_schema());
        let new_schema_clone = new_schema.clone();
        let reader_stream = IndexReaderStream::new(reader, self.batch_size).await;
        let batches = reader_stream
            .map(|fut| fut.map_err(DataFusionError::from))
            .buffered(self.store.io_parallelism())
            .map_ok(move |batch| {
                RecordBatch::try_new(
                    new_schema.clone(),
                    vec![batch.column(0).clone(), batch.column(1).clone()],
                )
                .unwrap()
            })
            .boxed();
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            new_schema_clone,
            batches,
        )))
    }

    /// Merge N source BTree segments plus an additional `new_data` stream into
    /// a single BTree under `dest_store`, without re-reading the dataset.
    pub async fn merge_segments(
        segments: &[Arc<Self>],
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_data_filters: &[Option<OldIndexDataFilter>],
    ) -> Result<CreatedIndex> {
        let Some(first) = segments.first() else {
            return Err(Error::invalid_input(
                "cannot merge BTree index without at least one source segment".to_string(),
            ));
        };

        if old_data_filters.len() != segments.len() {
            return Err(Error::invalid_input(format!(
                "BTree merge: expected one old-data filter per source segment \
                 (segments={}, filters={})",
                segments.len(),
                old_data_filters.len()
            )));
        }

        for segment in segments.iter().skip(1) {
            if segment.data_type != first.data_type {
                return Err(Error::index(format!(
                    "cannot merge BTree segments with different value types ({:?} vs {:?})",
                    first.data_type, segment.data_type
                )));
            }
        }

        let new_schema = new_data.schema();
        let value_column_index = new_schema.index_of(VALUE_COLUMN_NAME)?;
        let new_value_type = new_schema.field(value_column_index).data_type();
        if new_value_type != &first.data_type {
            return Err(Error::invalid_input(format!(
                "BTree merge: new_data value column type {:?} does not match \
                 segment value type {:?}",
                new_value_type, first.data_type
            )));
        }

        let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(segments.len() + 1);
        for (segment, old_data_filter) in segments.iter().zip(old_data_filters) {
            if filter_keeps_nothing(old_data_filter) {
                continue;
            }
            let stream = segment.data_stream().await?;
            let stream = match segment.frag_reuse_index.clone() {
                Some(frag_reuse_index) => remap_row_ids(stream, frag_reuse_index),
                None => stream,
            };
            let stream = match old_data_filter.clone() {
                Some(filter) => filter_row_ids(stream, filter),
                None => stream,
            };
            inputs.push(Arc::new(OneShotExec::new(stream)));
        }
        inputs.push(Arc::new(OneShotExec::new(new_data)));

        let sort_expr = PhysicalSortExpr {
            expr: Arc::new(Column::new(VALUE_COLUMN_NAME, value_column_index)),
            options: SortOptions {
                descending: false,
                nulls_first: true,
            },
        };
        // UnionExec yields multiple partitions; SortPreservingMergeExec merges
        // them back into a single partition while preserving value-ordering.
        let unioned = UnionExec::try_new(inputs)?;
        let ordered = Arc::new(SortPreservingMergeExec::new([sort_expr].into(), unioned));
        let unchunked = execute_plan(
            ordered,
            LanceExecutionOptions {
                use_spilling: true,
                ..Default::default()
            },
        )?;
        let merged_stream = chunk_concat_stream(unchunked, first.batch_size as usize);

        let files =
            train_btree_index(merged_stream, dest_store, first.batch_size, None, None).await?;

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::BTreeIndexDetails::default())
                .unwrap(),
            index_version: BTREE_INDEX_VERSION,
            files,
        })
    }
}

/// Filter a stream of record batches using the selection semantics encapsulated
/// by `old_data_filter`.
fn filter_row_ids(
    stream: SendableRecordBatchStream,
    old_data_filter: OldIndexDataFilter,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let filtered = stream.map(move |batch_result| {
        let batch = batch_result?;
        let row_ids = batch[ROW_ID]
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .ok_or_else(|| Error::internal("expected UInt64Array for row_id column"))?;
        let mask = old_data_filter.filter_row_ids(row_ids);
        Ok(arrow_select::filter::filter_record_batch(&batch, &mask)?)
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, filtered))
}

/// True if `filter` would keep no rows at all (its keep-set is empty), letting
/// the merge skip reading the segment entirely.
fn filter_keeps_nothing(filter: &Option<OldIndexDataFilter>) -> bool {
    match filter {
        Some(OldIndexDataFilter::Fragments { to_keep, .. }) => to_keep.is_empty(),
        Some(OldIndexDataFilter::RowIds(valid)) => valid.is_empty(),
        None => false,
    }
}

fn remap_row_ids(
    stream: SendableRecordBatchStream,
    frag_reuse_index: Arc<dyn RowIdRemapper>,
) -> SendableRecordBatchStream {
    let schema = stream.schema();
    let remapped = stream.map(move |batch_result| {
        let batch = batch_result?;
        Ok(frag_reuse_index.remap_row_ids_record_batch(batch, 1)?)
    });
    Box::pin(RecordBatchStreamAdapter::new(schema, remapped))
}

fn wrap_bound(bound: &Bound<ScalarValue>) -> Bound<OrderableScalarValue> {
    match bound {
        Bound::Unbounded => Bound::Unbounded,
        Bound::Included(val) => Bound::Included(OrderableScalarValue(val.clone())),
        Bound::Excluded(val) => Bound::Excluded(OrderableScalarValue(val.clone())),
    }
}

fn serialize_with_display<T: Display, S: Serializer>(
    value: &Option<T>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    if let Some(value) = value {
        serializer.collect_str(value)
    } else {
        serializer.collect_str("N/A")
    }
}

#[derive(Serialize)]
struct BTreeStatistics {
    #[serde(serialize_with = "serialize_with_display")]
    min: Option<OrderableScalarValue>,
    #[serde(serialize_with = "serialize_with_display")]
    max: Option<OrderableScalarValue>,
    num_pages: u32,
}

#[async_trait]
impl Index for BTreeIndex {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_index(self: Arc<Self>) -> Arc<dyn Index> {
        self
    }

    async fn prewarm(&self) -> Result<()> {
        let index_reader = LazyIndexReader::new(self.store.clone(), self.ranges_to_files.clone());
        let reader = index_reader.get().await?;
        let num_pages = reader.num_batches(self.batch_size).await;
        let mut pages = stream::iter(0..num_pages)
            .map(|page_idx| {
                let index_reader = index_reader.clone();
                async move {
                    let page = self
                        .read_page(page_idx, index_reader, &NoOpMetricsCollector)
                        .await?;
                    Result::Ok((page_idx, page))
                }
            })
            .buffer_unordered(get_num_compute_intensive_cpus());

        while let Some((page_idx, page)) = pages.try_next().await? {
            let inserted = self
                .index_cache
                .insert_with_key(
                    &BTreePageKey {
                        page_number: page_idx,
                    },
                    Arc::new(page),
                )
                .await;

            if !inserted {
                return Err(Error::internal(
                    "Failed to prewarm index: cache is no longer available".to_string(),
                ));
            }
        }

        Ok(())
    }

    fn index_type(&self) -> IndexType {
        IndexType::BTree
    }

    fn statistics(&self) -> Result<serde_json::Value> {
        let lookup = &self.page_lookup;
        let batch = &lookup.batch;
        let num_rows = batch.num_rows();
        // The batch is sorted by `min`, so the smallest searchable value is the
        // `min` of the first non-all-null page and the largest is the `max` of the
        // last page.
        let (min, max) = if lookup.search_start >= num_rows {
            (None, None)
        } else {
            let min = OrderableScalarValue(ScalarValue::try_from_array(
                batch.column(0),
                lookup.search_start,
            )?);
            let max =
                OrderableScalarValue(ScalarValue::try_from_array(batch.column(1), num_rows - 1)?);
            (Some(min), Some(max))
        };
        serde_json::to_value(&BTreeStatistics {
            num_pages: num_rows as u32,
            min,
            max,
        })
        .map_err(|err| err.into())
    }

    async fn calculate_included_frags(&self) -> Result<RoaringBitmap> {
        let mut frag_ids = RoaringBitmap::default();

        let lazy_reader = LazyIndexReader::new(self.store.clone(), self.ranges_to_files.clone());
        let sub_index_reader = lazy_reader.get().await?;
        let mut reader_stream = IndexReaderStream::new(sub_index_reader, self.batch_size)
            .await
            .buffered(self.store.io_parallelism());
        while let Some(serialized) = reader_stream.try_next().await? {
            let page = FlatIndex::try_new(serialized)?;
            frag_ids |= page.calculate_included_frags()?;
        }

        Ok(frag_ids)
    }
}

#[async_trait]
impl ScalarIndex for BTreeIndex {
    async fn search(
        &self,
        query: &dyn AnyQuery,
        metrics: &dyn MetricsCollector,
    ) -> Result<SearchResult> {
        let query = query.as_any().downcast_ref::<SargableQuery>().unwrap();
        let mut pages = match query {
            SargableQuery::Equals(val) => self
                .page_lookup
                .pages_eq(&OrderableScalarValue(val.clone())),
            SargableQuery::Range(start, end) => self
                .page_lookup
                .pages_between((wrap_bound(start).as_ref(), wrap_bound(end).as_ref())),
            SargableQuery::IsIn(values) => self
                .page_lookup
                .pages_in(values.iter().map(|val| OrderableScalarValue(val.clone()))),
            SargableQuery::FullTextSearch(_) => {
                return Err(Error::invalid_input(
                    "full text search is not supported for BTree index, build a inverted index for it",
                ));
            }
            SargableQuery::IsNull() => Ok(self.page_lookup.pages_null()),
            SargableQuery::LikePrefix(prefix) => {
                // Convert LikePrefix to a range query: [prefix, next_prefix)
                match prefix {
                    ScalarValue::Utf8(Some(s)) => {
                        let start = Bound::Included(OrderableScalarValue(prefix.clone()));
                        let end = match compute_next_prefix(s) {
                            Some(next) => {
                                Bound::Excluded(OrderableScalarValue(ScalarValue::Utf8(Some(next))))
                            }
                            None => Bound::Unbounded,
                        };
                        self.page_lookup
                            .pages_between((start.as_ref(), end.as_ref()))
                    }
                    ScalarValue::LargeUtf8(Some(s)) => {
                        let start = Bound::Included(OrderableScalarValue(prefix.clone()));
                        let end = match compute_next_prefix(s) {
                            Some(next) => Bound::Excluded(OrderableScalarValue(
                                ScalarValue::LargeUtf8(Some(next)),
                            )),
                            None => Bound::Unbounded,
                        };
                        self.page_lookup
                            .pages_between((start.as_ref(), end.as_ref()))
                    }
                    _ => {
                        // Conservative: return all pages for non-string types
                        // This is consistent with ZoneMap behavior
                        self.page_lookup
                            .pages_between((Bound::Unbounded, Bound::Unbounded))
                    }
                }
            }
        }?;

        // For non-IsNull queries, also include null pages so that null row IDs
        // are tracked in the result. Any comparison with NULL yields NULL, and
        // we need this information for correct three-valued logic (e.g. NOT,
        // OR). Without this, a query like `NOT(x = 0)` on data where 0 doesn't
        // exist would incorrectly include NULL rows.
        //
        // We add them as Matches::Some (not Matches::All) so that
        // FlatIndex::search() evaluates the predicate and correctly marks
        // the rows as NULL rather than TRUE.
        //
        // TODO: the lookup batch retains a per-page `null_count`. A fully-covered
        // page with zero nulls is a true Matches::All, while one with nulls needs
        // Matches::Some only to track the null rows; surfacing `null_count` here
        // could refine that classification (see #6802).
        if !matches!(query, SargableQuery::IsNull()) {
            let existing: HashSet<u32> = pages.iter().map(|m| m.page_id()).collect();
            for &page_id in self
                .page_lookup
                .null_pages
                .iter()
                .chain(self.page_lookup.all_null_pages.iter())
            {
                if !existing.contains(&page_id) {
                    pages.push(Matches::Some(page_id));
                }
            }
        }

        // Compile a large IsIn predicate once and reuse it across every page;
        // rebuilding the full IN-list per page is O(pages * values) and dominates
        // the lookup for sets with many values.
        let prebuilt = match query {
            SargableQuery::IsIn(_) => Some(self.compile_predicate(query)?),
            _ => None,
        };

        let lazy_index_reader =
            LazyIndexReader::new(self.store.clone(), self.ranges_to_files.clone());
        let page_tasks = pages
            .into_iter()
            .map(|page_index| {
                self.search_page(
                    query,
                    page_index,
                    lazy_index_reader.clone(),
                    prebuilt.as_ref(),
                    metrics,
                )
                .boxed()
            })
            .collect::<Vec<_>>();
        debug!("Searching {} btree pages", page_tasks.len());

        // Collect both matching row IDs and null row IDs from all pages
        let results: Vec<NullableRowAddrSet> = stream::iter(page_tasks)
            // I/O and compute mixed here but important case is index in cache so
            // use compute intensive thread count
            .buffered(get_num_compute_intensive_cpus())
            .try_collect()
            .await?;

        // Merge matching row IDs
        let selection = NullableRowAddrSet::union_all(&results);

        Ok(SearchResult::Exact(selection))
    }

    fn can_remap(&self) -> bool {
        true
    }

    async fn remap(
        &self,
        mapping: &RowAddrRemap,
        dest_store: &dyn IndexStore,
    ) -> Result<CreatedIndex> {
        // (part_id, path)
        // The part_id is None for a basic index
        // For a range-based index we use Some(0), Some(1), ...
        //   even if those weren't the original part ids
        let part_page_files: Vec<(Option<u32>, &str)> =
            if let Some(ranges_to_files) = &self.ranges_to_files {
                // Range-based Index: Directly collect references to the file paths.
                ranges_to_files
                    .iter()
                    .enumerate()
                    .map(|(part_id, (_, (path, _)))| (Some(part_id as u32), path.as_str()))
                    .collect()
            } else {
                // Basic Index: There is only one source page file.
                vec![(None, BTREE_PAGES_NAME)]
            };

        let mapping = Arc::new(mapping.clone());
        let train_schema = Arc::new(self.train_schema());
        let mut remapped_files = Vec::new();

        // TODO: Could potentially parallelize this across parts, unclear it would be worth it
        for (part_id, page_file) in part_page_files {
            // Retrain on the remapped pages
            let sub_index_reader = self.store.open_index_file(page_file).await?;
            let mapping = mapping.clone();

            let train_schema_clone = train_schema.clone();
            let train_schema = train_schema.clone();

            let remapped_stream = IndexReaderStream::new(sub_index_reader, self.batch_size)
                .await
                .buffered(self.store.io_parallelism())
                .map_err(DataFusionError::from)
                .and_then(move |batch| {
                    // Remap the batch and then convert from the serialized schema to the training input schema
                    let remapped =
                        FlatIndex::remap_batch(batch, &mapping).map_err(DataFusionError::from);
                    let with_train_schema = remapped.and_then(|batch| {
                        RecordBatch::try_new(train_schema.clone(), batch.columns().to_vec())
                            .map_err(DataFusionError::from)
                    });
                    std::future::ready(with_train_schema)
                });

            let remapped_stream = Box::pin(RecordBatchStreamAdapter::new(
                train_schema_clone,
                remapped_stream,
            ));

            let mut files =
                train_btree_index(remapped_stream, dest_store, self.batch_size, None, part_id)
                    .await?;
            remapped_files.append(&mut files);
        }

        if let Some(ranges_to_files) = &self.ranges_to_files {
            let num_parts = ranges_to_files.len();
            // Merge the lookups if we are a range-based index
            let page_files = (0..num_parts)
                .map(|part_id| part_page_data_file_path((part_id as u64) << 32))
                .collect::<Vec<_>>();
            let lookup_files = (0..num_parts)
                .map(|part_id| part_lookup_file_path((part_id as u64) << 32))
                .collect::<Vec<_>>();
            let merged_files = merge_metadata_files(
                dest_store,
                &page_files,
                &lookup_files,
                None,
                noop_progress(),
            )
            .await?;
            remapped_files.retain(|file| file.path.ends_with("_page_data.lance"));
            remapped_files.extend(merged_files);
        }

        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::BTreeIndexDetails::default())
                .unwrap(),
            index_version: BTREE_INDEX_VERSION,
            files: remapped_files,
        })
    }

    async fn update(
        &self,
        new_data: SendableRecordBatchStream,
        dest_store: &dyn IndexStore,
        old_data_filter: Option<OldIndexDataFilter>,
    ) -> Result<CreatedIndex> {
        // Updating is the single-segment case of a segment merge: union this
        // index's data with `new_data`, re-sort on value, and retrain.
        Self::merge_segments(
            &[Arc::new(self.clone())],
            new_data,
            dest_store,
            &[old_data_filter],
        )
        .await
    }

    fn update_criteria(&self) -> UpdateCriteria {
        UpdateCriteria::only_new_data(TrainingCriteria::new(TrainingOrdering::Values).with_row_id())
    }

    fn derive_index_params(&self) -> Result<ScalarIndexParams> {
        let params = serde_json::to_value(BTreeParameters {
            zone_size: Some(self.batch_size),
            range_id: None,
        })?;
        Ok(ScalarIndexParams::for_builtin(BuiltinIndexType::BTree).with_params(&params))
    }
}

struct BatchStats {
    min: ScalarValue,
    max: ScalarValue,
    null_count: u32,
}

fn analyze_batch(batch: &RecordBatch) -> Result<BatchStats> {
    let values = batch.column_by_name(VALUE_COLUMN_NAME).expect_ok()?;
    if values.is_empty() {
        return Err(Error::internal(
            "received an empty batch in btree training".to_string(),
        ));
    }
    let min = ScalarValue::try_from_array(&values, 0)
        .map_err(|e| Error::internal(format!("failed to get min value from batch: {}", e)))?;
    let max = ScalarValue::try_from_array(&values, values.len() - 1)
        .map_err(|e| Error::internal(format!("failed to get max value from batch: {}", e)))?;

    Ok(BatchStats {
        min,
        max,
        null_count: values.null_count() as u32,
    })
}

/// A trait that must be implemented by anything that wishes to act as a btree subindex
#[async_trait]
pub trait BTreeSubIndex: Debug + Send + Sync + DeepSizeOf {
    /// Trains the subindex on a single batch of data and serializes it to Arrow
    async fn train(&self, batch: RecordBatch) -> Result<RecordBatch>;

    /// Deserialize a subindex from Arrow
    async fn load_subindex(&self, serialized: RecordBatch) -> Result<Arc<dyn ScalarIndex>>;

    /// Retrieve the data used to originally train this page
    ///
    /// In order to perform an update we need to merge the old data in with the new data which
    /// means we need to access the new data.  Right now this is convenient for flat indices but
    /// we may need to take a different approach if we ever decide to use a sub-index other than
    /// flat
    async fn retrieve_data(&self, serialized: RecordBatch) -> Result<RecordBatch>;

    /// The schema of the subindex when serialized to Arrow
    fn schema(&self) -> &Arc<Schema>;

    /// Given a serialized page, deserialize it, remap the row ids, and re-serialize it
    async fn remap_subindex(
        &self,
        serialized: RecordBatch,
        mapping: &RowAddrRemap,
    ) -> Result<RecordBatch>;
}

struct EncodedBatch {
    stats: BatchStats,
    page_number: u32,
}

async fn train_btree_page(
    batch: RecordBatch,
    batch_idx: u32,
    writer: &mut dyn IndexWriter,
    schema: Arc<Schema>,
) -> Result<EncodedBatch> {
    let stats = analyze_batch(&batch)?;

    // Renames from value/_rowid to values/ids
    let trained = RecordBatch::try_new(
        schema.clone(),
        vec![
            batch.column_by_name(VALUE_COLUMN_NAME).expect_ok()?.clone(),
            batch.column_by_name(ROW_ID).expect_ok()?.clone(),
        ],
    )?;

    writer.write_record_batch(trained).await?;
    Ok(EncodedBatch {
        stats,
        page_number: batch_idx,
    })
}

fn btree_stats_as_batch(stats: Vec<EncodedBatch>, value_type: &DataType) -> Result<RecordBatch> {
    let mins = if stats.is_empty() {
        new_empty_array(value_type)
    } else {
        ScalarValue::iter_to_array(stats.iter().map(|stat| stat.stats.min.clone()))?
    };
    let maxs = if stats.is_empty() {
        new_empty_array(value_type)
    } else {
        ScalarValue::iter_to_array(stats.iter().map(|stat| stat.stats.max.clone()))?
    };
    let null_counts = UInt32Array::from_iter_values(stats.iter().map(|stat| stat.stats.null_count));
    let page_numbers = UInt32Array::from_iter_values(stats.iter().map(|stat| stat.page_number));

    let schema = Arc::new(Schema::new(vec![
        // min and max can be null if the entire batch is null values
        Field::new("min", mins.data_type().clone(), true),
        Field::new("max", maxs.data_type().clone(), true),
        Field::new("null_count", null_counts.data_type().clone(), false),
        Field::new("page_idx", page_numbers.data_type().clone(), false),
    ]));

    let columns = vec![
        mins,
        maxs,
        Arc::new(null_counts) as Arc<dyn Array>,
        Arc::new(page_numbers) as Arc<dyn Array>,
    ];

    Ok(RecordBatch::try_new(schema, columns)?)
}

/// Train a btree index from a stream of sorted page-size batches of values and row ids
pub async fn train_btree_index(
    batches_source: SendableRecordBatchStream,
    index_store: &dyn IndexStore,
    batch_size: u64,
    fragment_ids: Option<Vec<u32>>,
    range_id: Option<u32>,
) -> Result<Vec<IndexFile>> {
    // Create `partition_id` for distributed index building.
    // This ID serves as a high-level mask (first 32 bits of a u64) to ensure
    // that index partitions generated by different workers do not conflict.
    // Lance supports two strategies for distributed training: fragment-based and range-based.
    let partition_id = fragment_ids
        .as_ref()
        // --- Fragment-based Partitioning ---
        // Used when training sub-indexes on a fragment-level-split basis. The `partition_id` is
        // derived from `fragment_ids` to associate the index pages with their source fragment.
        .and_then(|frag_ids| frag_ids.first())
        .map(|&first_frag_id| (first_frag_id as u64) << 32)
        // --- Range-based Partitioning ---
        // Built upon data globally sorted by an external compute engine. The `range_id` creates
        // a unique name for the index pages generated by each worker.
        .or_else(|| range_id.map(|id| (id as u64) << 32));

    let flat_schema = Arc::new(Schema::new(vec![
        Field::new(
            BTREE_VALUES_COLUMN,
            batches_source.schema().field(0).data_type().clone(),
            true,
        ),
        Field::new(BTREE_IDS_COLUMN, DataType::UInt64, false),
    ]));

    let mut sub_index_file = match partition_id {
        None => {
            index_store
                .new_index_file(BTREE_PAGES_NAME, flat_schema.clone())
                .await?
        }
        Some(partition_id) => {
            index_store
                .new_index_file(
                    part_page_data_file_path(partition_id).as_str(),
                    flat_schema.clone(),
                )
                .await?
        }
    };

    let mut encoded_batches = Vec::new();
    let mut batch_idx = 0;

    let value_type = batches_source
        .schema()
        .field_with_name(VALUE_COLUMN_NAME)?
        .data_type()
        .clone();

    let mut batches_source = chunk_concat_stream(batches_source, batch_size as usize);

    while let Some(batch) = batches_source.try_next().await? {
        encoded_batches.push(
            train_btree_page(
                batch,
                batch_idx,
                sub_index_file.as_mut(),
                flat_schema.clone(),
            )
            .await?,
        );
        batch_idx += 1;
    }
    let pages_file = sub_index_file.finish().await?;
    let record_batch = btree_stats_as_batch(encoded_batches, &value_type)?;
    let mut file_schema = record_batch.schema().as_ref().clone();
    file_schema
        .metadata
        .insert(BATCH_SIZE_META_KEY.to_string(), batch_size.to_string());
    file_schema.metadata.insert(
        RANGE_PARTITIONED_META_KEY.to_string(),
        range_id.is_some().to_string(),
    );
    let mut btree_index_file = match partition_id {
        None => {
            index_store
                .new_index_file(BTREE_LOOKUP_NAME, Arc::new(file_schema))
                .await?
        }
        Some(partition_id) => {
            index_store
                .new_index_file(
                    part_lookup_file_path(partition_id).as_str(),
                    Arc::new(file_schema),
                )
                .await?
        }
    };
    btree_index_file.write_record_batch(record_batch).await?;
    let lookup_file = btree_index_file.finish().await?;
    Ok(vec![pages_file, lookup_file])
}

fn find_single_partition_files(files: &[super::IndexFile]) -> Result<Option<(&str, &str)>> {
    let lookup_files = files
        .iter()
        .filter_map(|file| {
            (file.path.starts_with("part_") && file.path.ends_with("_page_lookup.lance"))
                .then_some(file.path.as_str())
        })
        .collect::<Vec<_>>();
    let page_files = files
        .iter()
        .filter_map(|file| {
            (file.path.starts_with("part_") && file.path.ends_with("_page_data.lance"))
                .then_some(file.path.as_str())
        })
        .collect::<Vec<_>>();

    if lookup_files.len() != 1 || page_files.len() != 1 {
        return Ok(None);
    }

    let lookup_partition_id = extract_partition_id(lookup_files[0])?;
    let page_partition_id = extract_partition_id(page_files[0])?;
    if lookup_partition_id != page_partition_id {
        return Ok(None);
    }

    Ok(Some((lookup_files[0], page_files[0])))
}

fn is_missing_lookup_error(err: &Error) -> bool {
    matches!(err, Error::NotFound { .. })
        || matches!(
            err,
            Error::IO { source, .. }
                if source
                    .downcast_ref::<ObjectStoreError>()
                    .map(|os_err| matches!(os_err, ObjectStoreError::NotFound { .. }))
                    .unwrap_or(false)
        )
}

/// Merge multiple partition page / lookup files into a complete metadata file
///
/// In a distributed environment, each worker node writes partition page / lookup file for the partitions it processes,
/// and this function merges these files into a final metadata file.
/// - For fragment-based indices, it performs a full K-way sort-merge of page files to create new global page and lookup files.
/// - For range-based indices, it concatenates lookup files, as data is already globally sorted.
async fn merge_metadata_files(
    store: &dyn IndexStore,
    part_page_files: &[String],
    part_lookup_files: &[String],
    batch_readhead: Option<usize>,
    progress: Arc<dyn IndexBuildProgress>,
) -> Result<Vec<IndexFile>> {
    if part_lookup_files.is_empty() || part_page_files.is_empty() {
        return Err(Error::internal(
            "No partition files provided for merging".to_string(),
        ));
    }

    // Step 1: Create lookup map for page files by partition ID
    if part_lookup_files.len() != part_page_files.len() {
        return Err(Error::internal(format!(
            "Number of partition lookup files ({}) does not match number of partition page files ({})",
            part_lookup_files.len(),
            part_page_files.len()
        )));
    }
    let mut page_files_map = HashMap::new();
    for page_file in part_page_files {
        let partition_id = extract_partition_id(page_file)?;
        page_files_map.insert(partition_id, page_file);
    }

    // Step 2: Validate that all lookup files have corresponding page files
    for lookup_file in part_lookup_files {
        let partition_id = extract_partition_id(lookup_file)?;
        if !page_files_map.contains_key(&partition_id) {
            return Err(Error::internal(format!(
                "No corresponding page file found for lookup file: {} (partition_id: {})",
                lookup_file, partition_id
            )));
        }
    }

    // Step 3: Extract shared metadata and generate lookup_schema
    let first_lookup_reader = store.open_index_file(&part_lookup_files[0]).await?;
    let batch_size = first_lookup_reader
        .schema()
        .metadata
        .get(BATCH_SIZE_META_KEY)
        .map(|bs| bs.parse().unwrap_or(DEFAULT_BTREE_BATCH_SIZE))
        .unwrap_or(DEFAULT_BTREE_BATCH_SIZE);
    let range_partitioned = first_lookup_reader
        .schema()
        .metadata
        .get(RANGE_PARTITIONED_META_KEY)
        .map(|bs| bs.parse().unwrap_or(DEFAULT_RANGE_PARTITIONED))
        .unwrap_or(DEFAULT_RANGE_PARTITIONED);

    // Get the value type from lookup schema (min column)
    let value_type = first_lookup_reader
        .schema()
        .fields
        .first()
        .unwrap()
        .data_type();

    let mut metadata = HashMap::new();
    metadata.insert(BATCH_SIZE_META_KEY.to_string(), batch_size.to_string());
    let lookup_schema = Arc::new(Schema::new(vec![
        Field::new("min", value_type.clone(), true),
        Field::new("max", value_type.clone(), true),
        Field::new("null_count", DataType::UInt32, false),
        Field::new("page_idx", DataType::UInt32, false),
    ]));

    // Step 4: Merge pages and lookups and generate new index files
    if range_partitioned {
        merge_range_partitioned_lookups(
            store,
            part_lookup_files,
            lookup_schema,
            metadata,
            batch_size,
            batch_readhead,
            progress,
        )
        .await
        .map(|file| vec![file])
    } else {
        merge_pages_and_lookups(
            store,
            part_page_files,
            part_lookup_files,
            &page_files_map,
            lookup_schema,
            metadata,
            batch_size,
            batch_readhead,
            progress,
        )
        .await
    }
}

/// Merges multiple lookup files from a range-partitioned index into a single, unified lookup file.
///
/// A range-partitioned B-Tree index creates a separate `page_lookup.lance` file for
/// each partition. Each of these files has its own local `page_idx` column, where the indices
/// start from 0.
///
/// This function's primary goal is to combine these separate files into one large
/// `page_lookup.lance` file. To do this, it remaps the local `page_idx` from each partition
/// file into a contiguous, global `page_idx` space. It processes partition files sequentially,
/// calculating an offset based on the number of pages in all previously processed partitions.
///
/// **The reverse operation occurs when the B-Tree index is loaded**: a global `page_idx` is translated
/// back into a `(partition_id, local_page_idx)` tuple. This translation is made possible by the
/// metadata stored under the `PAGE_NUM_PER_RANGE_PARTITION_META_KEY`, which this function
/// is responsible for writing.
///
/// # Examples
///
/// If we have two partition lookup files:
/// - `part_0_page_lookup.lance`: Contains 3 pages. Its `page_idx` column is `[0, 1, 2]`.
/// - `part_1_page_lookup.lance`: Contains 4 pages. Its `page_idx` column is `[0, 1, 2, 3]`.
///
/// The merge process works as follows:
/// 1. Process `part_0`: The offset is 0. The indices `[0, 1, 2]` are written as is.
/// 2. Process `part_1`: The offset is 3 and the local indices `[0, 1, 2, 3]` are remapped
///    by adding the offset, resulting in `[3, 4, 5, 6]`.
///
/// The final, merged `_page_lookup.lance` will have a single `page_idx` column containing
/// `[0, 1, 2, 3, 4, 5, 6]`.
async fn merge_range_partitioned_lookups(
    store: &dyn IndexStore,
    part_lookup_files: &[String],
    lookup_schema: Arc<Schema>,
    mut metadata: HashMap<String, String>,
    batch_size: u64,
    batch_readhead: Option<usize>,
    progress: Arc<dyn IndexBuildProgress>,
) -> Result<IndexFile> {
    let sorted_part_lookup_files = sort_files_by_partition_id(part_lookup_files)?;
    let mut lookup_file = store
        .new_index_file(BTREE_LOOKUP_NAME, lookup_schema)
        .await?;

    // stores partition id and the number of pages in that partition
    let mut pages_per_file: Vec<(u64, u32)> = Vec::with_capacity(sorted_part_lookup_files.len());
    let mut num_pages_written = 0u32;

    progress
        .stage_start(
            "merge_lookups",
            Some(sorted_part_lookup_files.len() as u64),
            "files",
        )
        .await?;

    for (idx, (part_id, part_lookup_file)) in sorted_part_lookup_files.into_iter().enumerate() {
        let lookup_reader = store.open_index_file(&part_lookup_file).await?;
        let reader_stream = IndexReaderStream::new(lookup_reader.clone(), batch_size).await;
        let mut stream = reader_stream.buffered(batch_readhead.unwrap_or(1)).boxed();
        while let Some(batch) = stream.next().await {
            let original_batch = batch?;
            let modified_batch = add_offset_to_page_idx(&original_batch, num_pages_written)?;
            lookup_file.write_record_batch(modified_batch).await?;
        }
        pages_per_file.push((part_id, lookup_reader.num_rows() as u32));
        num_pages_written += lookup_reader.num_rows() as u32;
        progress
            .stage_progress("merge_lookups", idx as u64 + 1)
            .await?;
    }

    metadata.insert(RANGE_PARTITIONED_META_KEY.to_string(), "true".to_string());
    metadata.insert(
        PAGE_NUM_PER_RANGE_PARTITION_META_KEY.to_string(),
        serde_json::to_string(&pages_per_file)?,
    );

    let lookup_file = lookup_file.finish_with_metadata(metadata).await?;
    progress.stage_complete("merge_lookups").await?;

    // In this mode, we only clean up lookup files, and page files are untouched.
    cleanup_partition_files(store, part_lookup_files, &[]).await;
    Ok(lookup_file)
}

/// Merges partition files using a K-way sort-merge algorithm.
///
/// This function assumes its inputs have been pre-validated. It reads from all
/// partitioned page files simultaneously, merges them into a single sorted stream,
/// writes a new global page file, and generates a corresponding global lookup file.
#[allow(clippy::too_many_arguments)]
async fn merge_pages_and_lookups(
    store: &dyn IndexStore,
    part_page_files: &[String],
    part_lookup_files: &[String],
    page_files_map: &HashMap<u64, &String>,
    lookup_schema: Arc<Schema>,
    metadata: HashMap<String, String>,
    batch_size: u64,
    batch_readhead: Option<usize>,
    progress: Arc<dyn IndexBuildProgress>,
) -> Result<Vec<IndexFile>> {
    // Create a new global page file
    let partition_id = extract_partition_id(part_lookup_files[0].as_str())?;
    let page_file = page_files_map.get(&partition_id).unwrap();
    let page_reader = store.open_index_file(page_file).await?;
    let page_schema = page_reader.schema().clone();

    let arrow_schema = Arc::new(Schema::from(&page_schema));
    let mut page_file = store
        .new_index_file(BTREE_PAGES_NAME, arrow_schema.clone())
        .await?;
    progress.stage_start("merge_pages", None, "pages").await?;
    let lookup_entries = merge_pages(
        part_lookup_files,
        page_files_map,
        store,
        batch_size,
        &mut page_file,
        arrow_schema.clone(),
        batch_readhead,
        progress.clone(),
    )
    .await?;
    let page_file = page_file.finish().await?;
    progress.stage_complete("merge_pages").await?;

    let lookup_batch = RecordBatch::try_new(
        lookup_schema.clone(),
        vec![
            ScalarValue::iter_to_array(lookup_entries.iter().map(|(min, _, _, _)| min.clone()))?,
            ScalarValue::iter_to_array(lookup_entries.iter().map(|(_, max, _, _)| max.clone()))?,
            Arc::new(UInt32Array::from_iter_values(
                lookup_entries
                    .iter()
                    .map(|(_, _, null_count, _)| *null_count),
            )),
            Arc::new(UInt32Array::from_iter_values(
                lookup_entries.iter().map(|(_, _, _, page_idx)| *page_idx),
            )),
        ],
    )?;
    let mut lookup_file = store
        .new_index_file(BTREE_LOOKUP_NAME, lookup_schema)
        .await?;
    progress
        .stage_start("write_lookup_file", Some(1), "files")
        .await?;
    lookup_file.write_record_batch(lookup_batch).await?;
    let lookup_file = lookup_file.finish_with_metadata(metadata).await?;
    progress.stage_progress("write_lookup_file", 1).await?;
    progress.stage_complete("write_lookup_file").await?;

    // After successfully writing the merged files, delete all partition files
    // Only perform deletion after files are successfully written, ensuring debug information is not lost in case of failure
    cleanup_partition_files(store, part_lookup_files, part_page_files).await;

    Ok(vec![page_file, lookup_file])
}

// Adjust local_page_idx_ in each look-up file to create a contiguous global_page_idx
fn add_offset_to_page_idx(batch: &RecordBatch, offset: u32) -> Result<RecordBatch> {
    let (page_idx_pos, _) = batch.schema().column_with_name("page_idx").ok_or_else(|| {
        Error::internal("Column 'page_idx' not found in RecordBatch schema".to_string())
    })?;
    let page_idx_array = batch
        .column(page_idx_pos)
        .as_any()
        .downcast_ref::<UInt32Array>()
        .ok_or_else(|| {
            Error::internal("Failed to downcast 'page_idx' column to UInt32Array".to_string())
        })?;
    let offset_array = UInt32Array::from(vec![offset; page_idx_array.len()]);
    let new_page_idx_array_ref = add(page_idx_array, &offset_array)?;
    let mut new_columns = batch.columns().to_vec();
    new_columns[page_idx_pos] = new_page_idx_array_ref;
    let new_batch = RecordBatch::try_new(batch.schema(), new_columns)?;
    Ok(new_batch)
}

/// Merge pages using Datafusion's SortPreservingMergeExec
/// which implements a K-way merge algorithm with fixed-size output batches
#[allow(clippy::too_many_arguments)]
async fn merge_pages(
    part_lookup_files: &[String],
    page_files_map: &HashMap<u64, &String>,
    store: &dyn IndexStore,
    batch_size: u64,
    page_file: &mut Box<dyn IndexWriter>,
    arrow_schema: Arc<Schema>,
    batch_readhead: Option<usize>,
    progress: Arc<dyn IndexBuildProgress>,
) -> Result<Vec<(ScalarValue, ScalarValue, u32, u32)>> {
    let mut lookup_entries = Vec::new();
    let mut page_idx = 0u32;

    debug!(
        "Starting SortPreservingMerge with {} partitions",
        part_lookup_files.len()
    );

    let value_field = arrow_schema.field(0).clone().with_name(VALUE_COLUMN_NAME);
    let row_id_field = arrow_schema.field(1).clone().with_name(ROW_ID);
    let stream_schema = Arc::new(Schema::new(vec![value_field, row_id_field]));

    // Create execution plans for each stream
    let mut inputs: Vec<Arc<dyn ExecutionPlan>> = Vec::new();
    for lookup_file in part_lookup_files {
        let partition_id = extract_partition_id(lookup_file)?;
        let page_file_name = (*page_files_map.get(&partition_id).ok_or_else(|| {
            Error::internal(format!(
                "Page file not found for partition ID: {}",
                partition_id
            ))
        })?)
        .clone();

        let reader = store.open_index_file(&page_file_name).await?;

        let reader_stream = IndexReaderStream::new(reader, batch_size).await;

        let stream = reader_stream
            .map(|fut| fut.map_err(DataFusionError::from))
            .buffered(batch_readhead.unwrap_or(1))
            .boxed();

        let sendable_stream =
            Box::pin(RecordBatchStreamAdapter::new(stream_schema.clone(), stream));
        inputs.push(Arc::new(OneShotExec::new(sendable_stream)));
    }

    // Create Union execution plan to combine all partitions
    let union_inputs = UnionExec::try_new(inputs)?;

    // Create SortPreservingMerge execution plan
    let value_column_index = stream_schema.index_of(VALUE_COLUMN_NAME)?;
    let sort_expr = PhysicalSortExpr {
        expr: Arc::new(Column::new(VALUE_COLUMN_NAME, value_column_index)),
        options: SortOptions {
            descending: false,
            nulls_first: true,
        },
    };

    let merge_exec = Arc::new(SortPreservingMergeExec::new(
        [sort_expr].into(),
        union_inputs,
    ));

    let unchunked = execute_plan(
        merge_exec,
        LanceExecutionOptions {
            use_spilling: false,
            ..Default::default()
        },
    )?;

    // Use chunk_concat_stream to ensure fixed batch sizes
    let mut chunked_stream = chunk_concat_stream(unchunked, batch_size as usize);

    // Process chunked stream
    while let Some(batch) = chunked_stream.try_next().await? {
        let writer_batch = RecordBatch::try_new(
            arrow_schema.clone(),
            vec![batch.column(0).clone(), batch.column(1).clone()],
        )?;

        page_file.write_record_batch(writer_batch).await?;

        let min_val = ScalarValue::try_from_array(batch.column(0), 0)?;
        let max_val = ScalarValue::try_from_array(batch.column(0), batch.num_rows() - 1)?;
        let null_count = batch.column(0).null_count() as u32;

        lookup_entries.push((min_val, max_val, null_count, page_idx));
        page_idx += 1;
        progress
            .stage_progress("merge_pages", page_idx as u64)
            .await?;
    }

    Ok(lookup_entries)
}

// Sorts file paths by the partition ID extracted from file name.
fn sort_files_by_partition_id(part_files: &[String]) -> Result<Vec<(u64, String)>> {
    let mut files_with_ids: Vec<(u64, &String)> = part_files
        .iter()
        .map(|file| extract_partition_id(file).map(|id| (id, file)))
        .collect::<Result<Vec<_>>>()?;

    files_with_ids.sort_unstable_by_key(|k| k.0);

    let sorted_files = files_with_ids
        .into_iter()
        .map(|(id, file)| (id, file.clone()))
        .collect();

    Ok(sorted_files)
}

/// Extract partition ID from partition file name
/// Expected format: "part_{partition_id}_{suffix}.lance"
fn extract_partition_id(filename: &str) -> Result<u64> {
    if !filename.starts_with("part_") {
        return Err(Error::internal(format!(
            "Invalid partition file name format: {}",
            filename
        )));
    }

    let parts: Vec<&str> = filename.split('_').collect();
    if parts.len() < 3 {
        return Err(Error::internal(format!(
            "Invalid partition file name format: {}",
            filename
        )));
    }

    parts[1].parse::<u64>().map_err(|_| {
        Error::internal(format!(
            "Failed to parse partition ID from filename: {}",
            filename
        ))
    })
}

/// Clean up partition files after successful merge
///
/// This function safely deletes partition lookup and page files after a successful merge operation.
/// File deletion failures are logged but do not affect the overall success of the merge operation.
async fn cleanup_partition_files(
    store: &dyn IndexStore,
    part_lookup_files: &[String],
    part_page_files: &[String],
) {
    // Clean up partition lookup files
    for file_name in part_lookup_files {
        cleanup_single_file(
            store,
            file_name,
            "part_",
            "_page_lookup.lance",
            "partition lookup",
        )
        .await;
    }

    // Clean up partition page files
    for file_name in part_page_files {
        cleanup_single_file(
            store,
            file_name,
            "part_",
            "_page_data.lance",
            "partition page",
        )
        .await;
    }
}

/// Helper function to clean up a single partition file
///
/// Performs safety checks on the filename pattern before attempting deletion.
async fn cleanup_single_file(
    store: &dyn IndexStore,
    file_name: &str,
    expected_prefix: &str,
    expected_suffix: &str,
    file_type: &str,
) {
    if file_name.starts_with(expected_prefix) && file_name.ends_with(expected_suffix) {
        match store.delete_index_file(file_name).await {
            Ok(()) => {
                debug!("Successfully deleted {} file: {}", file_type, file_name);
            }
            Err(e) => {
                warn!(
                    "Failed to delete {} file '{}': {}. \
                    This does not affect the merge operation, but may leave \
                    partition files that should be cleaned up manually.",
                    file_type, file_name, e
                );
            }
        }
    } else {
        // If the filename doesn't match the expected format, log a warning but don't attempt deletion
        warn!(
            "Skipping deletion of file '{}' as it does not match the expected \
            {} file pattern ({}*{})",
            file_name, file_type, expected_prefix, expected_suffix
        );
    }
}

pub(crate) fn part_page_data_file_path(partition_id: u64) -> String {
    format!("part_{}_{}", partition_id, BTREE_PAGES_NAME)
}

pub(crate) fn part_lookup_file_path(partition_id: u64) -> String {
    format!("part_{}_{}", partition_id, BTREE_LOOKUP_NAME)
}

/// A stream that reads the original training data back out of the index
///
/// This is used for updating the index
struct IndexReaderStream {
    reader: Arc<dyn IndexReader>,
    batch_size: u64,
    num_batches: u32,
    batch_idx: u32,
}

impl IndexReaderStream {
    async fn new(reader: Arc<dyn IndexReader>, batch_size: u64) -> Self {
        let num_batches = reader.num_batches(batch_size).await;
        Self {
            reader,
            batch_size,
            num_batches,
            batch_idx: 0,
        }
    }
}

impl Stream for IndexReaderStream {
    type Item = BoxFuture<'static, Result<RecordBatch>>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        _cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.batch_idx >= this.num_batches {
            return std::task::Poll::Ready(None);
        }
        let batch_num = this.batch_idx;
        this.batch_idx += 1;
        let reader_copy = this.reader.clone();
        let batch_size = this.batch_size;
        let read_task = async move {
            reader_copy
                .read_record_batch(batch_num as u64, batch_size)
                .await
        }
        .boxed();
        std::task::Poll::Ready(Some(read_task))
    }
}

/// Parameters for a btree index
#[derive(Debug, Serialize, Deserialize)]
pub struct BTreeParameters {
    /// The number of rows to include in each zone
    pub zone_size: Option<u64>,

    /// DEPRECATED: range-based distributed BTree building has been retired.
    /// Setting this to `Some(..)` now emits a warning and is ignored at build time
    /// (see `BTreeIndexPlugin::train_index`). Build one segment per worker and
    /// commit them with `commit_existing_index_segments(...)`, optionally
    /// consolidating with `merge_existing_index_segments(...)`. The field is
    /// retained (rather than removed) so the plugin can detect stale `range_id`
    /// inputs and warn loudly instead of serde silently dropping an unknown field.
    ///
    /// Historically, this was the ordinal ID of a globally sorted range
    /// partition. Lance used it to write `part_*` BTree files that were later
    /// merged by `merge_index_metadata`. That flow has been retired. A
    /// pre-sorted training stream is still accepted, but this field no longer
    /// affects file names, commit behavior, or query semantics.
    pub range_id: Option<u32>,
}

struct BTreeTrainingRequest {
    parameters: BTreeParameters,
    criteria: TrainingCriteria,
}

impl BTreeTrainingRequest {
    pub fn new(parameters: BTreeParameters) -> Self {
        Self {
            parameters,
            // BTree indexes need data sorted by the value column
            criteria: TrainingCriteria::new(TrainingOrdering::Values).with_row_id(),
        }
    }
}

impl TrainingRequest for BTreeTrainingRequest {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn criteria(&self) -> &TrainingCriteria {
        &self.criteria
    }
}

#[derive(Debug, Default)]
pub struct BTreeIndexPlugin;

#[async_trait]
impl BasicTrainer for BTreeIndexPlugin {
    fn new_training_request(
        &self,
        params: &str,
        field: &Field,
    ) -> Result<Box<dyn TrainingRequest>> {
        if field.data_type().is_nested() {
            return Err(Error::invalid_input_source(
                "A btree index can only be created on a non-nested field.".into(),
            ));
        }

        let params = serde_json::from_str::<BTreeParameters>(params)?;
        Ok(Box::new(BTreeTrainingRequest::new(params)))
    }

    async fn train_index(
        &self,
        data: SendableRecordBatchStream,
        index_store: &dyn IndexStore,
        request: Box<dyn TrainingRequest>,
        _fragment_ids: Option<Vec<u32>>,
        _progress: Arc<dyn crate::progress::IndexBuildProgress>,
    ) -> Result<CreatedIndex> {
        let request = request
            .as_any()
            .downcast_ref::<BTreeTrainingRequest>()
            .unwrap();
        if request.parameters.range_id.is_some() {
            // `range_id` is deprecated and now ignored. A pre-sorted data stream is
            // still supported (pass it as the training data), but `range_id` no longer
            // needs to be set: each build now produces one canonical segment, and
            // distribution is handled by the segmented-index APIs. The field will be
            // removed in a future release.
            warn!(
                "BTree `range_id` is deprecated and now ignored; a pre-sorted data \
                 stream is still supported, but `range_id` no longer needs to be passed. \
                 Use the segmented-index APIs instead (build per-fragment segments, then \
                 commit_existing_index_segments(...) / merge_existing_index_segments(...)). \
                 The `range_id` field will be removed in a future release."
            );
        }
        let files = train_btree_index(
            data,
            index_store,
            request
                .parameters
                .zone_size
                .unwrap_or(DEFAULT_BTREE_BATCH_SIZE),
            None,
            None,
        )
        .await?;
        Ok(CreatedIndex {
            index_details: prost_types::Any::from_msg(&pbold::BTreeIndexDetails::default())
                .unwrap(),
            index_version: BTREE_INDEX_VERSION,
            files,
        })
    }
}

#[async_trait]
impl ScalarIndexPlugin for BTreeIndexPlugin {
    fn basic_trainer(&self) -> Option<&dyn BasicTrainer> {
        Some(self)
    }

    fn name(&self) -> &str {
        "BTree"
    }

    fn provides_exact_answer(&self) -> bool {
        true
    }

    fn version(&self) -> u32 {
        BTREE_INDEX_VERSION
    }

    fn new_query_parser(
        &self,
        index_name: String,
        _index_details: &prost_types::Any,
    ) -> Option<Box<dyn ScalarQueryParser>> {
        Some(Box::new(SargableQueryParser::new(
            index_name,
            self.name().to_string(),
            false,
        )))
    }

    async fn load_index(
        &self,
        index_store: Arc<dyn IndexStore>,
        _index_details: &prost_types::Any,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Arc<dyn ScalarIndex>> {
        Ok(BTreeIndex::load(index_store, frag_reuse_index, cache).await? as Arc<dyn ScalarIndex>)
    }

    async fn get_from_cache(
        &self,
        index_store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
    ) -> Result<Option<Arc<dyn ScalarIndex>>> {
        let Some(state) = cache.get_with_key(&BTreeIndexStateKey).await else {
            return Ok(None);
        };
        Ok(Some(state.reconstruct(
            index_store,
            cache,
            frag_reuse_index,
        )?))
    }

    async fn put_in_cache(&self, cache: &LanceCache, index: Arc<dyn ScalarIndex>) -> Result<()> {
        let state = BTreeIndexState::from_index(index.as_ref())?;
        cache
            .insert_with_key(&BTreeIndexStateKey, Arc::new(state))
            .await;
        Ok(())
    }

    async fn get_or_insert_in_cache(
        &self,
        index_store: Arc<dyn IndexStore>,
        frag_reuse_index: Option<Arc<dyn RowIdRemapper>>,
        cache: &LanceCache,
        load: ScalarIndexLoad<'_>,
    ) -> Result<Arc<dyn ScalarIndex>> {
        single_flight_open(
            cache,
            BTreeIndexStateKey,
            load,
            BTreeIndexState::from_index,
            move |state| state.reconstruct(index_store, cache, frag_reuse_index),
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use lance_core::utils::row_addr_remap::RowAddrRemap;
    use std::sync::atomic::Ordering;
    use std::{collections::HashMap, sync::Arc};

    use arrow::datatypes::{Float32Type, Float64Type, Int32Type, UInt64Type};
    use arrow_array::{FixedSizeListArray, record_batch};
    use datafusion::{
        execution::{SendableRecordBatchStream, TaskContext},
        physical_plan::{ExecutionPlan, sorts::sort::SortExec, stream::RecordBatchStreamAdapter},
    };
    use datafusion_common::{DataFusionError, ScalarValue};
    use datafusion_physical_expr::{PhysicalSortExpr, expressions::col};
    use futures::TryStreamExt;
    use futures::stream;
    use lance_core::cache::LanceCache;
    use lance_core::deepsize::DeepSizeOf;
    use lance_core::utils::tempfile::TempObjDir;
    use lance_datafusion::{chunker::break_stream, datagen::DatafusionDatagenExt};
    use lance_datagen::{ArrayGeneratorExt, BatchCount, RowCount, array, gen_batch};
    use lance_io::object_store::ObjectStore;
    use lance_select::{RowAddrTreeMap, RowSetOps};
    use object_store::path::Path;

    use crate::metrics::LocalMetricsCollector;
    use crate::progress::{IndexBuildProgress, noop_progress};
    use crate::{
        metrics::NoOpMetricsCollector,
        scalar::{
            IndexStore, OldIndexDataFilter, SargableQuery, ScalarIndex, SearchResult,
            btree::{BTREE_PAGES_NAME, BTreeIndex},
            lance_format::LanceIndexStore,
        },
    };

    use super::{
        BTreeIndexPlugin, BTreeIndexState, BTreeLookup, BTreePageKey, DEFAULT_BTREE_BATCH_SIZE,
        Matches, OrderableScalarValue, part_lookup_file_path, part_page_data_file_path,
        train_btree_index,
    };
    use crate::scalar::registry::ScalarIndexPlugin;
    use arrow_array::RecordBatch;
    use lance_core::cache::{CacheCodecImpl, CacheEntryReader, CacheEntryWriter, CacheKey};

    /// Serialize a `BTreeIndexState` body (no envelope) for tests.
    fn serialize_state(state: &BTreeIndexState) -> Vec<u8> {
        let mut buf = Vec::new();
        state
            .serialize(&mut CacheEntryWriter::new(&mut buf))
            .unwrap();
        buf
    }

    /// Deserialize a `BTreeIndexState` body (no envelope) for tests.
    fn deserialize_state(buf: Vec<u8>) -> lance_core::Result<BTreeIndexState> {
        let data = bytes::Bytes::from(buf);
        let mut reader = CacheEntryReader::new(&data, 0, BTreeIndexState::CURRENT_VERSION);
        BTreeIndexState::deserialize(&mut reader)
    }
    use rangemap::RangeInclusiveMap;

    lance_testing::define_stage_event_progress!(
        RecordingProgress,
        IndexBuildProgress,
        lance_core::Result<()>
    );
    #[test]
    fn test_scalar_value_size() {
        let size_of_i32 = OrderableScalarValue(ScalarValue::Int32(Some(0))).deep_size_of();
        let size_of_many_i32 = OrderableScalarValue(ScalarValue::FixedSizeList(Arc::new(
            FixedSizeListArray::from_iter_primitive::<Int32Type, _, _>(
                vec![Some(vec![Some(0); 128])],
                128,
            ),
        )))
        .deep_size_of();

        // deep_size_of should account for the rust type overhead
        assert!(size_of_i32 > 4);
        assert!(size_of_many_i32 > 128 * 4);
    }

    #[test]
    fn test_orderable_dictionary_cmp() {
        use arrow_schema::DataType;
        use std::cmp::Ordering;

        let dict = |s: &str, key: DataType| {
            OrderableScalarValue(ScalarValue::Dictionary(
                Box::new(key),
                Box::new(ScalarValue::Utf8(Some(s.to_string()))),
            ))
        };

        // Dictionary scalars are ordered by their underlying value, regardless
        // of the key type. This is exercised when loading a scalar index built
        // on a dictionary-encoded column into a BTreeMap.
        assert_eq!(
            dict("a", DataType::Int16).cmp(&dict("b", DataType::Int16)),
            Ordering::Less
        );
        assert_eq!(
            dict("b", DataType::Int32).cmp(&dict("b", DataType::Int16)),
            Ordering::Equal
        );

        // A non-null dictionary value sorts after null.
        assert_eq!(
            dict("a", DataType::Int16).cmp(&OrderableScalarValue(ScalarValue::Null)),
            Ordering::Greater
        );
    }

    #[tokio::test]
    async fn test_null_ids() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Generate 50,000 rows of random data with 80% nulls
        let stream = gen_batch()
            .col(
                "value",
                array::rand::<Float32Type>().with_nulls(&[true, false, false, false, false]),
            )
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(5000), BatchCount::from(10));

        train_btree_index(stream, test_store.as_ref(), 5000, None, None)
            .await
            .unwrap();

        let index = BTreeIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        assert_eq!(index.page_lookup.null_pages.len(), 10);

        let remap_dir = TempObjDir::default();
        let remap_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            remap_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Remap with a no-op mapping.  The remapped index should be identical to the original
        index
            .remap(&RowAddrRemap::empty(), remap_store.as_ref())
            .await
            .unwrap();

        let remap_index = BTreeIndex::load(remap_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        assert_eq!(remap_index.page_lookup, index.page_lookup);

        let original_pages = test_store.open_index_file(BTREE_PAGES_NAME).await.unwrap();
        let remapped_pages = remap_store.open_index_file(BTREE_PAGES_NAME).await.unwrap();

        assert_eq!(original_pages.num_rows(), remapped_pages.num_rows());

        let original_data = original_pages
            .read_record_batch(0, original_pages.num_rows() as u64)
            .await
            .unwrap();
        let remapped_data = remapped_pages
            .read_record_batch(0, remapped_pages.num_rows() as u64)
            .await
            .unwrap();

        assert_eq!(original_data, remapped_data);
    }

    #[tokio::test]
    async fn test_nan_ordering() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let values = vec![
            0.0,
            1.0,
            2.0,
            3.0,
            f64::NAN,
            f64::NEG_INFINITY,
            f64::INFINITY,
        ];

        // This is a bit overkill but we've had bugs in the past where DF's sort
        // didn't agree with Arrow's sort so we do an end-to-end test here
        // and use DF to sort the data like we would in a real dataset.
        let data = gen_batch()
            .col("value", array::cycle::<Float64Type>(values.clone()))
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_exec(RowCount::from(10), BatchCount::from(100));
        let schema = data.schema();
        let sort_expr = PhysicalSortExpr::new_default(col("value", schema.as_ref()).unwrap());
        let plan = Arc::new(SortExec::new([sort_expr].into(), data));
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let stream = break_stream(stream, 64);
        let stream = stream.map_err(DataFusionError::from);
        let stream =
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream;

        train_btree_index(stream, test_store.as_ref(), 64, None, None)
            .await
            .unwrap();

        let index = BTreeIndex::load(test_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        for (idx, value) in values.into_iter().enumerate() {
            let query = SargableQuery::Equals(ScalarValue::Float64(Some(value)));
            let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();
            assert_eq!(
                result,
                SearchResult::exact(RowAddrTreeMap::from_iter(((idx as u64)..1000).step_by(7)))
            );
        }
    }

    #[tokio::test]
    async fn test_contains_keys_matches_search() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // 1000 distinct Int32 values [0, 1000), spread across many small pages
        // (batch_size 64) so the keys below exercise multi-page grouping.
        let data = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_exec(RowCount::from(100), BatchCount::from(10));
        let schema = data.schema();
        let sort_expr = PhysicalSortExpr::new_default(col("value", schema.as_ref()).unwrap());
        let plan = Arc::new(SortExec::new([sort_expr].into(), data));
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let stream = break_stream(stream, 64);
        let stream = stream.map_err(DataFusionError::from);
        let stream =
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream;

        train_btree_index(stream, test_store.as_ref(), 64, None, None)
            .await
            .unwrap();
        let index = BTreeIndex::load(test_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Present (range ends, mid, and adjacent values that straddle page
        // boundaries), interleaved with absent (below/above range, and a gap).
        let keys: Vec<i32> = vec![0, 999, 500, 1, 998, -1, 1000, 1500, 250, 251, 7, 64, 63, 65];
        let scalar_keys: Vec<ScalarValue> =
            keys.iter().map(|k| ScalarValue::Int32(Some(*k))).collect();

        let batched = index
            .contains_keys(&scalar_keys, &NoOpMetricsCollector)
            .await
            .unwrap();

        // Oracle: the per-key Equals search the batched path replaces.
        let mut oracle = Vec::with_capacity(keys.len());
        for k in &scalar_keys {
            let result = index
                .search(&SargableQuery::Equals(k.clone()), &NoOpMetricsCollector)
                .await
                .unwrap();
            oracle.push(!result.row_addrs().is_empty());
        }
        assert_eq!(
            batched, oracle,
            "contains_keys must agree with per-key Equals search; keys={keys:?}"
        );

        // And both must match ground truth: [0, 1000) present, others absent.
        let expected: Vec<bool> = keys.iter().map(|k| (0..1000).contains(k)).collect();
        assert_eq!(batched, expected);

        // Empty input → empty mask.
        assert!(
            index
                .contains_keys(&[], &NoOpMetricsCollector)
                .await
                .unwrap()
                .is_empty()
        );

        // A null key maps to false (and must not panic).
        let with_null = vec![ScalarValue::Int32(Some(5)), ScalarValue::Int32(None)];
        assert_eq!(
            index
                .contains_keys(&with_null, &NoOpMetricsCollector)
                .await
                .unwrap(),
            vec![true, false]
        );
    }

    #[tokio::test]
    async fn test_page_cache() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let data = gen_batch()
            .col("value", array::step::<Float32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_exec(RowCount::from(1000), BatchCount::from(10));
        let schema = data.schema();
        let sort_expr = PhysicalSortExpr::new_default(col("value", schema.as_ref()).unwrap());
        let plan = Arc::new(SortExec::new([sort_expr].into(), data));
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let stream = break_stream(stream, 64);
        let stream = stream.map_err(DataFusionError::from);
        let stream =
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream;

        train_btree_index(stream, test_store.as_ref(), 64, None, None)
            .await
            .unwrap();

        let cache = Arc::new(LanceCache::with_capacity(100 * 1024 * 1024));
        let index = BTreeIndex::load(test_store, None, cache.as_ref())
            .await
            .unwrap();

        let query = SargableQuery::Equals(ScalarValue::Float32(Some(0.0)));
        let metrics = LocalMetricsCollector::default();
        let query1 = index.search(&query, &metrics);
        let query2 = index.search(&query, &metrics);
        tokio::join!(query1, query2).0.unwrap();
        assert_eq!(metrics.parts_loaded.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_page_cache_hit_miss_counts() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let data = gen_batch()
            .col("value", array::step::<Float32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_exec(RowCount::from(1000), BatchCount::from(10));
        let schema = data.schema();
        let sort_expr = PhysicalSortExpr::new_default(col("value", schema.as_ref()).unwrap());
        let plan = Arc::new(SortExec::new([sort_expr].into(), data));
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        let stream = break_stream(stream, 64);
        let stream = stream.map_err(DataFusionError::from);
        let stream =
            Box::pin(RecordBatchStreamAdapter::new(schema, stream)) as SendableRecordBatchStream;

        train_btree_index(stream, test_store.as_ref(), 64, None, None)
            .await
            .unwrap();

        let cache = Arc::new(LanceCache::with_capacity(100 * 1024 * 1024));
        let index = BTreeIndex::load(test_store, None, cache.as_ref())
            .await
            .unwrap();

        // First search: cold cache — the page fetch must miss.
        let query = SargableQuery::Equals(ScalarValue::Float32(Some(0.0)));
        let cold = LocalMetricsCollector::default();
        index.search(&query, &cold).await.unwrap();
        assert_eq!(cold.index_cache_hits(), 0);
        assert_eq!(cold.index_cache_misses(), 1);
        assert_eq!(cold.parts_loaded.load(Ordering::Relaxed), 1);

        // Second search: same key, page must now be served from cache.
        let warm = LocalMetricsCollector::default();
        index.search(&query, &warm).await.unwrap();
        assert_eq!(warm.index_cache_hits(), 1);
        assert_eq!(warm.index_cache_misses(), 0);
        assert_eq!(warm.parts_loaded.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_like_prefix_search() {
        use arrow::datatypes::DataType;
        use arrow_array::StringArray;

        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create string data with various prefixes
        let values = vec![
            "apple",
            "app",
            "application",
            "banana",
            "band",
            "test_ns$table1",
            "test_ns$table2",
            "test_ns2$table1",
            "test",
            "testing",
        ];
        let row_ids: Vec<u64> = (0..values.len() as u64).collect();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("value", DataType::Utf8, false),
            arrow::datatypes::Field::new("_rowid", DataType::UInt64, false),
        ]));

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(values.clone())),
                Arc::new(arrow_array::UInt64Array::from(row_ids)),
            ],
        )
        .unwrap();

        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(async { Ok(batch) }),
        ));

        train_btree_index(stream, test_store.as_ref(), 100, None, None)
            .await
            .unwrap();

        let index = BTreeIndex::load(test_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Test LikePrefix for "app" - should match "apple", "app", "application" (row ids 0, 1, 2)
        let query = SargableQuery::LikePrefix(ScalarValue::Utf8(Some("app".to_string())));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match &result {
            SearchResult::Exact(row_ids) => {
                let ids: Vec<u64> = row_ids
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert!(ids.contains(&0), "Should contain row 0 (apple)");
                assert!(ids.contains(&1), "Should contain row 1 (app)");
                assert!(ids.contains(&2), "Should contain row 2 (application)");
                assert!(!ids.contains(&3), "Should not contain row 3 (banana)");
            }
            _ => panic!("Expected Exact result"),
        }

        // Test LikePrefix for "test_ns$" - should match "test_ns$table1", "test_ns$table2" (row ids 5, 6)
        let query = SargableQuery::LikePrefix(ScalarValue::Utf8(Some("test_ns$".to_string())));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match &result {
            SearchResult::Exact(row_ids) => {
                let ids: Vec<u64> = row_ids
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert!(ids.contains(&5), "Should contain row 5 (test_ns$table1)");
                assert!(ids.contains(&6), "Should contain row 6 (test_ns$table2)");
                assert!(
                    !ids.contains(&7),
                    "Should not contain row 7 (test_ns2$table1)"
                );
            }
            _ => panic!("Expected Exact result"),
        }

        // Test LikePrefix for "test" - should match "test", "testing", "test_ns$table1", "test_ns$table2", "test_ns2$table1"
        let query = SargableQuery::LikePrefix(ScalarValue::Utf8(Some("test".to_string())));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match &result {
            SearchResult::Exact(row_ids) => {
                let ids: Vec<u64> = row_ids
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert!(
                    ids.contains(&5),
                    "Should contain row 5 (test_ns$table1): {:?}",
                    ids
                );
                assert!(
                    ids.contains(&6),
                    "Should contain row 6 (test_ns$table2): {:?}",
                    ids
                );
                assert!(
                    ids.contains(&7),
                    "Should contain row 7 (test_ns2$table1): {:?}",
                    ids
                );
                assert!(ids.contains(&8), "Should contain row 8 (test): {:?}", ids);
                assert!(
                    ids.contains(&9),
                    "Should contain row 9 (testing): {:?}",
                    ids
                );
            }
            _ => panic!("Expected Exact result"),
        }
    }

    #[tokio::test]
    async fn test_like_prefix_search_large_utf8() {
        use arrow::datatypes::DataType;
        use arrow_array::LargeStringArray;

        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let values = vec!["apple", "app", "application", "banana"];
        let row_ids: Vec<u64> = (0..values.len() as u64).collect();

        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("value", DataType::LargeUtf8, false),
            arrow::datatypes::Field::new("_rowid", DataType::UInt64, false),
        ]));

        let batch = arrow::record_batch::RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(LargeStringArray::from(values)),
                Arc::new(arrow_array::UInt64Array::from(row_ids)),
            ],
        )
        .unwrap();

        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::once(async { Ok(batch) }),
        ));

        train_btree_index(stream, test_store.as_ref(), 100, None, None)
            .await
            .unwrap();

        let index = BTreeIndex::load(test_store, None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Test LikePrefix with LargeUtf8
        let query = SargableQuery::LikePrefix(ScalarValue::LargeUtf8(Some("app".to_string())));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match &result {
            SearchResult::Exact(row_ids) => {
                let ids: Vec<u64> = row_ids
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert!(ids.contains(&0), "Should contain row 0 (apple)");
                assert!(ids.contains(&1), "Should contain row 1 (app)");
                assert!(ids.contains(&2), "Should contain row 2 (application)");
                assert!(!ids.contains(&3), "Should not contain row 3 (banana)");
            }
            _ => panic!("Expected Exact result"),
        }
    }

    #[tokio::test]
    async fn test_fragment_btree_index_consistency() {
        // Setup stores for both indexes
        let full_tmpdir = TempObjDir::default();
        let full_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            full_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let fragment_tmpdir = TempObjDir::default();
        let fragment_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            fragment_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Method 1: Build complete index directly using the same data
        // Create deterministic data for comparison - use 2 * DEFAULT_BTREE_BATCH_SIZE for testing
        let total_count = 2 * DEFAULT_BTREE_BATCH_SIZE;
        let full_data_gen = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(total_count / 2), BatchCount::from(2));
        let full_data_source = Box::pin(RecordBatchStreamAdapter::new(
            full_data_gen.schema(),
            full_data_gen,
        ));

        train_btree_index(
            full_data_source,
            full_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            None,
        )
        .await
        .unwrap();

        // Method 2: Build fragment-based index using the same data split into fragments
        // Create fragment 1 index - first half of the data (0 to DEFAULT_BTREE_BATCH_SIZE-1)
        let half_count = DEFAULT_BTREE_BATCH_SIZE;
        let fragment1_gen = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(half_count), BatchCount::from(1));
        let fragment1_data_source = Box::pin(RecordBatchStreamAdapter::new(
            fragment1_gen.schema(),
            fragment1_gen,
        ));

        train_btree_index(
            fragment1_data_source,
            fragment_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            Some(vec![1]), // fragment_id = 1
            None,
        )
        .await
        .unwrap();

        // Create fragment 2 index - second half of the data (DEFAULT_BTREE_BATCH_SIZE to 2*DEFAULT_BTREE_BATCH_SIZE-1)
        let start_val = DEFAULT_BTREE_BATCH_SIZE as i32;
        let end_val = (2 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let values_second_half: Vec<i32> = (start_val..end_val).collect();
        let row_ids_second_half: Vec<u64> = (start_val as u64..end_val as u64).collect();
        let fragment2_gen = gen_batch()
            .col("value", array::cycle::<Int32Type>(values_second_half))
            .col("_rowid", array::cycle::<UInt64Type>(row_ids_second_half))
            .into_df_stream(RowCount::from(half_count), BatchCount::from(1));
        let fragment2_data_source = Box::pin(RecordBatchStreamAdapter::new(
            fragment2_gen.schema(),
            fragment2_gen,
        ));

        train_btree_index(
            fragment2_data_source,
            fragment_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            Some(vec![2]), // fragment_id = 2
            None,
        )
        .await
        .unwrap();

        // Merge the fragment files
        let part_page_files = vec![
            part_page_data_file_path(1 << 32),
            part_page_data_file_path(2 << 32),
        ];

        let part_lookup_files = vec![
            part_lookup_file_path(1 << 32),
            part_lookup_file_path(2 << 32),
        ];

        let progress = Arc::new(RecordingProgress::default());
        super::merge_metadata_files(
            fragment_store.as_ref(),
            &part_page_files,
            &part_lookup_files,
            Option::from(1usize),
            progress.clone(),
        )
        .await
        .unwrap();

        let tags = progress
            .recorded_events()
            .iter()
            .map(|(kind, stage, _)| format!("{kind}:{stage}"))
            .collect::<Vec<_>>();
        let merge_start = tags
            .iter()
            .position(|e| e == "start:merge_pages")
            .expect("missing merge_pages start");
        let merge_complete = tags
            .iter()
            .position(|e| e == "complete:merge_pages")
            .expect("missing merge_pages complete");
        let lookup_start = tags
            .iter()
            .position(|e| e == "start:write_lookup_file")
            .expect("missing write_lookup_file start");
        let lookup_complete = tags
            .iter()
            .position(|e| e == "complete:write_lookup_file")
            .expect("missing write_lookup_file complete");
        assert!(merge_start < merge_complete);
        assert!(merge_complete < lookup_start);
        assert!(lookup_start < lookup_complete);
        assert!(
            tags.iter().any(|e| e == "progress:merge_pages"),
            "expected merge_pages progress callbacks"
        );
        assert!(
            tags.iter().any(|e| e == "progress:write_lookup_file"),
            "expected write_lookup_file progress callbacks"
        );

        // Load both indexes
        let full_index = BTreeIndex::load(full_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let merged_index = BTreeIndex::load(fragment_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Test queries one by one to identify the exact problem

        // Test 1: Query for value 0 (should be in first page)
        let query_0 = SargableQuery::Equals(ScalarValue::Int32(Some(0)));
        let full_result_0 = full_index
            .search(&query_0, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_0 = merged_index
            .search(&query_0, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(full_result_0, merged_result_0, "Query for value 0 failed");

        // Test 2: Query for value in middle of first batch (should be in first page)
        let mid_first_batch = (DEFAULT_BTREE_BATCH_SIZE / 2) as i32;
        let query_mid_first = SargableQuery::Equals(ScalarValue::Int32(Some(mid_first_batch)));
        let full_result_mid_first = full_index
            .search(&query_mid_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_mid_first = merged_index
            .search(&query_mid_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_mid_first, merged_result_mid_first,
            "Query for value {} failed",
            mid_first_batch
        );

        // Test 3: Query for first value in second batch (should be in second page)
        let first_second_batch = DEFAULT_BTREE_BATCH_SIZE as i32;
        let query_first_second =
            SargableQuery::Equals(ScalarValue::Int32(Some(first_second_batch)));
        let full_result_first_second = full_index
            .search(&query_first_second, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_first_second = merged_index
            .search(&query_first_second, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_first_second, merged_result_first_second,
            "Query for value {} failed",
            first_second_batch
        );

        // Test 4: Query for value in middle of second batch (should be in second page)
        let mid_second_batch = (DEFAULT_BTREE_BATCH_SIZE + DEFAULT_BTREE_BATCH_SIZE / 2) as i32;
        let query_mid_second = SargableQuery::Equals(ScalarValue::Int32(Some(mid_second_batch)));

        let full_result_mid_second = full_index
            .search(&query_mid_second, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_mid_second = merged_index
            .search(&query_mid_second, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_mid_second, merged_result_mid_second,
            "Query for value {} failed",
            mid_second_batch
        );
    }

    #[tokio::test]
    async fn test_fragment_btree_index_boundary_queries() {
        // Setup stores for both indexes
        let full_tmpdir = TempObjDir::default();
        let full_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            full_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let fragment_tmpdir = TempObjDir::default();
        let fragment_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            fragment_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Use 3 * DEFAULT_BTREE_BATCH_SIZE for more comprehensive boundary testing
        let total_count = 3 * DEFAULT_BTREE_BATCH_SIZE;

        // Method 1: Build complete index directly
        let full_data_gen = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(total_count / 3), BatchCount::from(3));
        let full_data_source = Box::pin(RecordBatchStreamAdapter::new(
            full_data_gen.schema(),
            full_data_gen,
        ));

        train_btree_index(
            full_data_source,
            full_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            None,
        )
        .await
        .unwrap();

        // Method 2: Build fragment-based index using 3 fragments
        // Fragment 1: 0 to DEFAULT_BTREE_BATCH_SIZE-1
        let fragment_size = DEFAULT_BTREE_BATCH_SIZE;
        let fragment1_gen = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(fragment_size), BatchCount::from(1));
        let fragment1_data_source = Box::pin(RecordBatchStreamAdapter::new(
            fragment1_gen.schema(),
            fragment1_gen,
        ));

        train_btree_index(
            fragment1_data_source,
            fragment_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            Some(vec![1]),
            None,
        )
        .await
        .unwrap();

        // Fragment 2: DEFAULT_BTREE_BATCH_SIZE to 2*DEFAULT_BTREE_BATCH_SIZE-1
        let start_val2 = DEFAULT_BTREE_BATCH_SIZE as i32;
        let end_val2 = (2 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let values_fragment2: Vec<i32> = (start_val2..end_val2).collect();
        let row_ids_fragment2: Vec<u64> = (start_val2 as u64..end_val2 as u64).collect();
        let fragment2_gen = gen_batch()
            .col("value", array::cycle::<Int32Type>(values_fragment2))
            .col("_rowid", array::cycle::<UInt64Type>(row_ids_fragment2))
            .into_df_stream(RowCount::from(fragment_size), BatchCount::from(1));
        let fragment2_data_source = Box::pin(RecordBatchStreamAdapter::new(
            fragment2_gen.schema(),
            fragment2_gen,
        ));

        train_btree_index(
            fragment2_data_source,
            fragment_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            Some(vec![2]),
            None,
        )
        .await
        .unwrap();

        // Fragment 3: 2*DEFAULT_BTREE_BATCH_SIZE to 3*DEFAULT_BTREE_BATCH_SIZE-1
        let start_val3 = (2 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let end_val3 = (3 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let values_fragment3: Vec<i32> = (start_val3..end_val3).collect();
        let row_ids_fragment3: Vec<u64> = (start_val3 as u64..end_val3 as u64).collect();
        let fragment3_gen = gen_batch()
            .col("value", array::cycle::<Int32Type>(values_fragment3))
            .col("_rowid", array::cycle::<UInt64Type>(row_ids_fragment3))
            .into_df_stream(RowCount::from(fragment_size), BatchCount::from(1));
        let fragment3_data_source = Box::pin(RecordBatchStreamAdapter::new(
            fragment3_gen.schema(),
            fragment3_gen,
        ));

        train_btree_index(
            fragment3_data_source,
            fragment_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            Some(vec![3]),
            None,
        )
        .await
        .unwrap();

        // Merge all fragment files
        let part_page_files = vec![
            part_page_data_file_path(1 << 32),
            part_page_data_file_path(2 << 32),
            part_page_data_file_path(3 << 32),
        ];

        let part_lookup_files = vec![
            part_lookup_file_path(1 << 32),
            part_lookup_file_path(2 << 32),
            part_lookup_file_path(3 << 32),
        ];

        super::merge_metadata_files(
            fragment_store.as_ref(),
            &part_page_files,
            &part_lookup_files,
            Option::from(1usize),
            noop_progress(),
        )
        .await
        .unwrap();

        // Load both indexes
        let full_index = BTreeIndex::load(full_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let merged_index = BTreeIndex::load(fragment_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        // === Boundary Value Tests ===

        // Test 1: Query minimum value (boundary: data start)
        let query_min = SargableQuery::Equals(ScalarValue::Int32(Some(0)));
        let full_result_min = full_index
            .search(&query_min, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_min = merged_index
            .search(&query_min, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_min, merged_result_min,
            "Query for minimum value 0 failed"
        );

        // Test 2: Query maximum value (boundary: data end)
        let max_val = (3 * DEFAULT_BTREE_BATCH_SIZE - 1) as i32;
        let query_max = SargableQuery::Equals(ScalarValue::Int32(Some(max_val)));
        let full_result_max = full_index
            .search(&query_max, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_max = merged_index
            .search(&query_max, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_max, merged_result_max,
            "Query for maximum value {} failed",
            max_val
        );

        // Test 3: Query fragment boundary value (last value of first fragment)
        let fragment1_last = (DEFAULT_BTREE_BATCH_SIZE - 1) as i32;
        let query_frag1_last = SargableQuery::Equals(ScalarValue::Int32(Some(fragment1_last)));
        let full_result_frag1_last = full_index
            .search(&query_frag1_last, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_frag1_last = merged_index
            .search(&query_frag1_last, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_frag1_last, merged_result_frag1_last,
            "Query for fragment 1 last value {} failed",
            fragment1_last
        );

        // Test 4: Query fragment boundary value (first value of second fragment)
        let fragment2_first = DEFAULT_BTREE_BATCH_SIZE as i32;
        let query_frag2_first = SargableQuery::Equals(ScalarValue::Int32(Some(fragment2_first)));
        let full_result_frag2_first = full_index
            .search(&query_frag2_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_frag2_first = merged_index
            .search(&query_frag2_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_frag2_first, merged_result_frag2_first,
            "Query for fragment 2 first value {} failed",
            fragment2_first
        );

        // Test 5: Query fragment boundary value (last value of second fragment)
        let fragment2_last = (2 * DEFAULT_BTREE_BATCH_SIZE - 1) as i32;
        let query_frag2_last = SargableQuery::Equals(ScalarValue::Int32(Some(fragment2_last)));
        let full_result_frag2_last = full_index
            .search(&query_frag2_last, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_frag2_last = merged_index
            .search(&query_frag2_last, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_frag2_last, merged_result_frag2_last,
            "Query for fragment 2 last value {} failed",
            fragment2_last
        );

        // Test 6: Query fragment boundary value (first value of third fragment)
        let fragment3_first = (2 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let query_frag3_first = SargableQuery::Equals(ScalarValue::Int32(Some(fragment3_first)));
        let full_result_frag3_first = full_index
            .search(&query_frag3_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_frag3_first = merged_index
            .search(&query_frag3_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_frag3_first, merged_result_frag3_first,
            "Query for fragment 3 first value {} failed",
            fragment3_first
        );

        // === Non-existent Value Tests ===

        // Test 7: Query value below minimum
        let query_below_min = SargableQuery::Equals(ScalarValue::Int32(Some(-1)));
        let full_result_below = full_index
            .search(&query_below_min, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_below = merged_index
            .search(&query_below_min, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_below, merged_result_below,
            "Query for value below minimum (-1) failed"
        );

        // Test 8: Query value above maximum
        let query_above_max = SargableQuery::Equals(ScalarValue::Int32(Some(max_val + 1)));
        let full_result_above = full_index
            .search(&query_above_max, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_above = merged_index
            .search(&query_above_max, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_above,
            merged_result_above,
            "Query for value above maximum ({}) failed",
            max_val + 1
        );

        // === Range Query Tests ===

        // Test 9: Cross-fragment range query (from first fragment to second fragment)
        let range_start = (DEFAULT_BTREE_BATCH_SIZE - 100) as i32;
        let range_end = (DEFAULT_BTREE_BATCH_SIZE + 100) as i32;
        let query_cross_frag = SargableQuery::Range(
            std::collections::Bound::Included(ScalarValue::Int32(Some(range_start))),
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(range_end))),
        );
        let full_result_cross = full_index
            .search(&query_cross_frag, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_cross = merged_index
            .search(&query_cross_frag, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_cross, merged_result_cross,
            "Cross-fragment range query [{}, {}] failed",
            range_start, range_end
        );

        // Test 10: Range query within single fragment
        let single_frag_start = 100i32;
        let single_frag_end = 200i32;
        let query_single_frag = SargableQuery::Range(
            std::collections::Bound::Included(ScalarValue::Int32(Some(single_frag_start))),
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(single_frag_end))),
        );
        let full_result_single = full_index
            .search(&query_single_frag, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_single = merged_index
            .search(&query_single_frag, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_single, merged_result_single,
            "Single fragment range query [{}, {}] failed",
            single_frag_start, single_frag_end
        );

        // Test 11: Large range query spanning all fragments
        let large_range_start = 100i32;
        let large_range_end = (3 * DEFAULT_BTREE_BATCH_SIZE - 100) as i32;
        let query_large_range = SargableQuery::Range(
            std::collections::Bound::Included(ScalarValue::Int32(Some(large_range_start))),
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(large_range_end))),
        );
        let full_result_large = full_index
            .search(&query_large_range, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_large = merged_index
            .search(&query_large_range, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_large, merged_result_large,
            "Large range query [{}, {}] failed",
            large_range_start, large_range_end
        );

        // === Range Boundary Query Tests ===

        // Test 12: Less than query (implemented using range query, from minimum to specified value)
        let lt_val = (DEFAULT_BTREE_BATCH_SIZE / 2) as i32;
        let query_lt = SargableQuery::Range(
            std::collections::Bound::Included(ScalarValue::Int32(Some(0))),
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(lt_val))),
        );
        let full_result_lt = full_index
            .search(&query_lt, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_lt = merged_index
            .search(&query_lt, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_lt, merged_result_lt,
            "Less than query (<{}) failed",
            lt_val
        );

        // Test 13: Greater than query (implemented using range query, from specified value to maximum)
        let gt_val = (2 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let max_range_val = (3 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let query_gt = SargableQuery::Range(
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(gt_val))),
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(max_range_val))),
        );
        let full_result_gt = full_index
            .search(&query_gt, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_gt = merged_index
            .search(&query_gt, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_gt, merged_result_gt,
            "Greater than query (>{}) failed",
            gt_val
        );

        // Test 14: Less than or equal query (implemented using range query, including boundary value)
        let lte_val = (DEFAULT_BTREE_BATCH_SIZE - 1) as i32;
        let query_lte = SargableQuery::Range(
            std::collections::Bound::Included(ScalarValue::Int32(Some(0))),
            std::collections::Bound::Included(ScalarValue::Int32(Some(lte_val))),
        );
        let full_result_lte = full_index
            .search(&query_lte, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_lte = merged_index
            .search(&query_lte, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_lte, merged_result_lte,
            "Less than or equal query (<={}) failed",
            lte_val
        );

        // Test 15: Greater than or equal query (implemented using range query, including boundary value)
        let gte_val = (2 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let query_gte = SargableQuery::Range(
            std::collections::Bound::Included(ScalarValue::Int32(Some(gte_val))),
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(max_range_val))),
        );
        let full_result_gte = full_index
            .search(&query_gte, &NoOpMetricsCollector)
            .await
            .unwrap();
        let merged_result_gte = merged_index
            .search(&query_gte, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_gte, merged_result_gte,
            "Greater than or equal query (>={}) failed",
            gte_val
        );
    }

    #[test]
    fn test_extract_partition_id() {
        // Test valid partition file names
        assert_eq!(
            super::extract_partition_id("part_123_page_data.lance").unwrap(),
            123
        );
        assert_eq!(
            super::extract_partition_id("part_456_page_lookup.lance").unwrap(),
            456
        );
        assert_eq!(
            super::extract_partition_id("part_4294967296_page_data.lance").unwrap(),
            4294967296
        );

        // Test invalid file names
        assert!(super::extract_partition_id("invalid_filename.lance").is_err());
        assert!(super::extract_partition_id("part_abc_page_data.lance").is_err());
        assert!(super::extract_partition_id("part_123").is_err());
        assert!(super::extract_partition_id("part_").is_err());
    }

    #[tokio::test]
    async fn test_cleanup_partition_files() {
        // Create a test store
        let tmpdir = TempObjDir::default();
        let test_store: Arc<dyn crate::scalar::IndexStore> = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Test files with different patterns
        let lookup_files = vec![
            "part_123_page_lookup.lance".to_string(),
            "invalid_lookup_file.lance".to_string(),
            "part_456_page_lookup.lance".to_string(),
        ];

        let page_files = vec![
            "part_123_page_data.lance".to_string(),
            "invalid_page_file.lance".to_string(),
            "part_456_page_data.lance".to_string(),
        ];

        // The cleanup function should handle both valid and invalid file patterns gracefully
        // This test mainly verifies that the function doesn't panic and handles edge cases
        super::cleanup_partition_files(test_store.as_ref(), &lookup_files, &page_files).await;
    }

    #[tokio::test]
    async fn test_btree_null_handling_in_queries() {
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::memory()),
            Path::default(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create test data: [null, 0, 5] at row IDs [0, 1, 2]
        // BTree expects sorted data with nulls first (or filtered out)
        let batch = record_batch!(
            ("value", Int32, [None, Some(0), Some(5)]),
            ("_rowid", UInt64, [0, 1, 2])
        )
        .unwrap();
        let stream = stream::once(futures::future::ok(batch.clone()));
        let stream = Box::pin(RecordBatchStreamAdapter::new(batch.schema(), stream));

        // Train the btree index with FlatIndexMetadata as sub-index
        super::train_btree_index(stream, store.as_ref(), 256, None, None)
            .await
            .unwrap();

        let cache = LanceCache::with_capacity(1024 * 1024);
        let index = super::BTreeIndex::load(store.clone(), None, &cache)
            .await
            .unwrap();

        // Test 1: Search for value 5 - should return allow=[2], null=[0]
        let query = SargableQuery::Equals(ScalarValue::Int32(Some(5)));
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match result {
            SearchResult::Exact(row_ids) => {
                let actual_rows: Vec<u64> = row_ids
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert_eq!(actual_rows, vec![2], "Should find row 2 where value == 5");

                // Check that null_row_ids contains row 0
                let null_row_ids = row_ids.null_rows();
                assert!(!null_row_ids.is_empty(), "null_row_ids should be non-empty");
                let null_rows: Vec<u64> =
                    null_row_ids.row_addrs().unwrap().map(u64::from).collect();
                assert_eq!(null_rows, vec![0], "Should report row 0 as null");
            }
            _ => panic!("Expected Exact search result"),
        }

        // Test 2: Range query [0, 3] - should return allow=[1], null=[0]
        let query = SargableQuery::Range(
            std::ops::Bound::Included(ScalarValue::Int32(Some(0))),
            std::ops::Bound::Included(ScalarValue::Int32(Some(3))),
        );
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match result {
            SearchResult::Exact(row_ids) => {
                let actual_rows: Vec<u64> = row_ids
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                assert_eq!(actual_rows, vec![1], "Should find row 1 where value == 0");

                // Should report row 0 as null
                let null_row_ids = row_ids.null_rows();
                assert!(!null_row_ids.is_empty(), "null_row_ids should be non-empty");
                let null_rows: Vec<u64> =
                    null_row_ids.row_addrs().unwrap().map(u64::from).collect();
                assert_eq!(null_rows, vec![0], "Should report row 0 as null");
            }
            _ => panic!("Expected Exact search result"),
        }

        // Test 3: IsIn query [0, 5] - should return allow=[1, 2], null=[0]
        let query = SargableQuery::IsIn(vec![
            ScalarValue::Int32(Some(0)),
            ScalarValue::Int32(Some(5)),
        ]);
        let result = index.search(&query, &NoOpMetricsCollector).await.unwrap();

        match result {
            SearchResult::Exact(row_ids) => {
                let mut actual_rows: Vec<u64> = row_ids
                    .true_rows()
                    .row_addrs()
                    .unwrap()
                    .map(u64::from)
                    .collect();
                actual_rows.sort();
                assert_eq!(
                    actual_rows,
                    vec![1, 2],
                    "Should find rows 1 and 2 where value in [0, 5]"
                );

                // Should report row 0 as null
                let null_row_ids = row_ids.null_rows();
                assert!(!null_row_ids.is_empty(), "null_row_ids should be non-empty");
                let null_rows: Vec<u64> =
                    null_row_ids.row_addrs().unwrap().map(u64::from).collect();
                assert_eq!(null_rows, vec![0], "Should report row 0 as null");
            }
            _ => panic!("Expected Exact search result"),
        }
    }

    #[tokio::test]
    async fn test_range_btree_index_consistency() {
        // Setup stores for both indexes
        let full_tmpdir = TempObjDir::default();
        let full_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            full_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let range_tmpdir = TempObjDir::default();
        let range_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            range_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Method 1: Build complete index directly using the same data
        // Create deterministic data for comparison - use 4 * DEFAULT_BTREE_BATCH_SIZE for testing
        let total_count = 4 * DEFAULT_BTREE_BATCH_SIZE;
        let full_data_gen = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(total_count / 4), BatchCount::from(4));
        let full_data_source = Box::pin(RecordBatchStreamAdapter::new(
            full_data_gen.schema(),
            full_data_gen,
        ));

        train_btree_index(
            full_data_source,
            full_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            None,
        )
        .await
        .unwrap();

        // Method 2: Build range-based index using the same data split into ranges
        // Create range 1 index, intentionally make it not divisible by DEFAULT_BTREE_BATCH_SIZE
        let range1_gen = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(
                RowCount::from(DEFAULT_BTREE_BATCH_SIZE / 2),
                BatchCount::from(5),
            );
        let range1_data_source = Box::pin(RecordBatchStreamAdapter::new(
            range1_gen.schema(),
            range1_gen,
        ));

        train_btree_index(
            range1_data_source,
            range_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            Option::from(0u32),
        )
        .await
        .unwrap();

        // Create range 2 index, also intentionally make it not divisible by DEFAULT_BTREE_BATCH_SIZE
        let start_val = (DEFAULT_BTREE_BATCH_SIZE * 2 + DEFAULT_BTREE_BATCH_SIZE / 2) as i32;
        let end_val = (4 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let values_second_half: Vec<i32> = (start_val..end_val).collect();
        let row_ids_second_half: Vec<u64> = (start_val as u64..end_val as u64).collect();
        let range2_gen = gen_batch()
            .col("value", array::cycle::<Int32Type>(values_second_half))
            .col("_rowid", array::cycle::<UInt64Type>(row_ids_second_half))
            .into_df_stream(
                RowCount::from(DEFAULT_BTREE_BATCH_SIZE / 2),
                BatchCount::from(3),
            );
        let range2_data_source = Box::pin(RecordBatchStreamAdapter::new(
            range2_gen.schema(),
            range2_gen,
        ));

        train_btree_index(
            range2_data_source,
            range_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            Option::from(1u32),
        )
        .await
        .unwrap();

        // Merge the fragment files
        let part_page_files = vec![
            part_page_data_file_path(0 << 32),
            part_page_data_file_path(1 << 32),
        ];

        let part_lookup_files = vec![
            part_lookup_file_path(0 << 32),
            part_lookup_file_path(1 << 32),
        ];

        super::merge_metadata_files(
            range_store.as_ref(),
            &part_page_files,
            &part_lookup_files,
            Option::from(1usize),
            noop_progress(),
        )
        .await
        .unwrap();

        let full_index = BTreeIndex::load(full_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let ranged_index = BTreeIndex::load(range_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Equality Tests

        // Test 1: Query for value 0
        let query_0 = SargableQuery::Equals(ScalarValue::Int32(Some(0)));
        let full_result_0 = full_index
            .search(&query_0, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_0 = ranged_index
            .search(&query_0, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(full_result_0, ranged_result_0, "Query for value 0 failed");

        // Test 2: Query for value in middle of first batch (should be in first page)
        let mid_first_batch = (DEFAULT_BTREE_BATCH_SIZE / 2) as i32;
        let query_mid_first = SargableQuery::Equals(ScalarValue::Int32(Some(mid_first_batch)));
        let full_result_mid_first = full_index
            .search(&query_mid_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_mid_first = ranged_index
            .search(&query_mid_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_mid_first, ranged_result_mid_first,
            "Query for value {} failed",
            mid_first_batch
        );

        // Test 3: Query for value in the last batch (should be in the second range file)
        let mid_last_batch = (DEFAULT_BTREE_BATCH_SIZE * 3 + (DEFAULT_BTREE_BATCH_SIZE / 2)) as i32;
        let query_mid_last = SargableQuery::Equals(ScalarValue::Int32(Some(mid_last_batch)));
        let full_result_mid_last = full_index
            .search(&query_mid_last, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_mid_last = ranged_index
            .search(&query_mid_last, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_mid_last, ranged_result_mid_last,
            "Query for value {} failed",
            mid_last_batch
        );

        // Test 4: Query upper bound.
        let max_val = (4 * DEFAULT_BTREE_BATCH_SIZE - 1) as i32;
        let query_max = SargableQuery::Equals(ScalarValue::Int32(Some(max_val)));
        let full_result_max = full_index
            .search(&query_max, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_max = ranged_index
            .search(&query_max, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_max, ranged_result_max,
            "Query for maximum value {} failed",
            max_val
        );

        // Test 5: Query first value of the second page file.
        let second_first_val = (DEFAULT_BTREE_BATCH_SIZE * 2 + DEFAULT_BTREE_BATCH_SIZE / 2) as i32;
        let query_second_first = SargableQuery::Equals(ScalarValue::Int32(Some(second_first_val)));
        let full_result_second_first = full_index
            .search(&query_second_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_second_first = ranged_index
            .search(&query_second_first, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_second_first, ranged_result_second_first,
            "Query for first value of the second page file {} failed",
            second_first_val
        );

        // Test 6: Query value below the minimum
        let query_below_min = SargableQuery::Equals(ScalarValue::Int32(Some(-1)));
        let full_result_below = full_index
            .search(&query_below_min, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_below = ranged_index
            .search(&query_below_min, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_below, ranged_result_below,
            "Query for value below minimum (-1) failed"
        );

        // Test 7: Query value above the maximum
        let query_above_max = SargableQuery::Equals(ScalarValue::Int32(Some(max_val + 1)));
        let full_result_above = full_index
            .search(&query_above_max, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_above = ranged_index
            .search(&query_above_max, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_above,
            ranged_result_above,
            "Query for value above maximum ({}) failed",
            max_val + 1
        );

        // Range Tests

        // Test 8: Cross-range query: One range including different values from adjacent range files.
        let range_start =
            (DEFAULT_BTREE_BATCH_SIZE * 2 + DEFAULT_BTREE_BATCH_SIZE / 2 - 100) as i32;
        let range_end = range_start + 200;
        let query_cross_range = SargableQuery::Range(
            std::collections::Bound::Included(ScalarValue::Int32(Some(range_start))),
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(range_end))),
        );
        let full_result_cross = full_index
            .search(&query_cross_range, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_cross = ranged_index
            .search(&query_cross_range, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_cross, ranged_result_cross,
            "Cross-range range query [{}, {}] failed",
            range_start, range_end
        );

        // Test 9 Test simple range within a single page file
        let single_range_start = (DEFAULT_BTREE_BATCH_SIZE * 4 - 300) as i32;
        let single_range_end = single_range_start + 200;
        let query_single_range = SargableQuery::Range(
            std::collections::Bound::Included(ScalarValue::Int32(Some(single_range_start))),
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(single_range_end))),
        );
        let full_result_single = full_index
            .search(&query_single_range, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_single = ranged_index
            .search(&query_single_range, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_single, ranged_result_single,
            "Single range query [{}, {}] failed",
            single_range_start, single_range_end
        );

        // Test 10: Large range query spanning almost all values
        let large_range_start = 100_i32;
        let large_range_end = (DEFAULT_BTREE_BATCH_SIZE * 4 - 100) as i32;
        let query_large_range = SargableQuery::Range(
            std::collections::Bound::Included(ScalarValue::Int32(Some(large_range_start))),
            std::collections::Bound::Excluded(ScalarValue::Int32(Some(large_range_end))),
        );
        let full_result_single = full_index
            .search(&query_large_range, &NoOpMetricsCollector)
            .await
            .unwrap();
        let ranged_result_single = ranged_index
            .search(&query_large_range, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(
            full_result_single, ranged_result_single,
            "Single fragment range query [{}, {}] failed",
            large_range_start, large_range_end
        );

        let remap_dir = TempObjDir::default();
        let remap_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            remap_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Remap with a no-op mapping.  The remapped index should be identical to the original
        ranged_index
            .remap(&RowAddrRemap::empty(), remap_store.as_ref())
            .await
            .unwrap();

        let remap_index = BTreeIndex::load(remap_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        assert_eq!(remap_index.page_lookup, ranged_index.page_lookup);

        let ranged_pages = range_store
            .open_index_file(part_page_data_file_path(1 << 32).as_str())
            .await
            .unwrap();
        let remapped_pages = remap_store
            .open_index_file(part_page_data_file_path(1 << 32).as_str())
            .await
            .unwrap();

        assert_eq!(ranged_pages.num_rows(), remapped_pages.num_rows());

        let original_data = ranged_pages
            .read_record_batch(0, ranged_pages.num_rows() as u64)
            .await
            .unwrap();
        let remapped_data = remapped_pages
            .read_record_batch(0, remapped_pages.num_rows() as u64)
            .await
            .unwrap();

        assert_eq!(original_data, remapped_data);
    }

    #[tokio::test]
    async fn test_update_ranged_index() {
        // Setup stores for both indexes
        let old_tmpdir = TempObjDir::default();
        let old_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            old_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let new_tmpdir = TempObjDir::default();
        let new_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            new_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create range 1 index, intentionally make it not divisible by DEFAULT_BTREE_BATCH_SIZE
        let range1_gen = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(
                RowCount::from(DEFAULT_BTREE_BATCH_SIZE / 2),
                BatchCount::from(5),
            );
        let range1_data_source = Box::pin(RecordBatchStreamAdapter::new(
            range1_gen.schema(),
            range1_gen,
        ));

        train_btree_index(
            range1_data_source,
            old_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            Option::from(1u32),
        )
        .await
        .unwrap();

        // Create range 2 index, also intentionally make it not divisible by DEFAULT_BTREE_BATCH_SIZE
        let start_val = (DEFAULT_BTREE_BATCH_SIZE * 2 + DEFAULT_BTREE_BATCH_SIZE / 2) as i32;
        let end_val = (4 * DEFAULT_BTREE_BATCH_SIZE) as i32;
        let values_second_half: Vec<i32> = (start_val..end_val).collect();
        let row_ids_second_half: Vec<u64> = (start_val as u64..end_val as u64).collect();
        let range2_gen = gen_batch()
            .col("value", array::cycle::<Int32Type>(values_second_half))
            .col("_rowid", array::cycle::<UInt64Type>(row_ids_second_half))
            .into_df_stream(
                RowCount::from(DEFAULT_BTREE_BATCH_SIZE / 2),
                BatchCount::from(3),
            );
        let range2_data_source = Box::pin(RecordBatchStreamAdapter::new(
            range2_gen.schema(),
            range2_gen,
        ));

        train_btree_index(
            range2_data_source,
            old_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            Option::from(2u32),
        )
        .await
        .unwrap();

        // Merge the fragment files
        let part_page_files = vec![
            part_page_data_file_path(1 << 32),
            part_page_data_file_path(2 << 32),
        ];

        let part_lookup_files = vec![
            part_lookup_file_path(1 << 32),
            part_lookup_file_path(2 << 32),
        ];

        super::merge_metadata_files(
            old_store.as_ref(),
            &part_page_files,
            &part_lookup_files,
            Option::from(1usize),
            noop_progress(),
        )
        .await
        .unwrap();

        // create some update data
        let start_val = (DEFAULT_BTREE_BATCH_SIZE * 2) as i32;
        let end_val = (DEFAULT_BTREE_BATCH_SIZE * 3) as i32;
        let row_id_delta = (DEFAULT_BTREE_BATCH_SIZE * 3) as i32;
        let values: Vec<i32> = (start_val..end_val).collect();
        let row_ids: Vec<u64> =
            ((start_val + row_id_delta) as u64..(end_val + row_id_delta) as u64).collect();
        let update_data = gen_batch()
            .col("value", array::cycle::<Int32Type>(values))
            .col("_rowid", array::cycle::<UInt64Type>(row_ids))
            .into_df_stream(
                RowCount::from(DEFAULT_BTREE_BATCH_SIZE / 2),
                BatchCount::from(2),
            );
        let update_data_source = Box::pin(RecordBatchStreamAdapter::new(
            update_data.schema(),
            update_data,
        ));

        let ranged_index = BTreeIndex::load(old_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        // update the ranged index
        ranged_index
            .update(update_data_source, new_store.as_ref(), None)
            .await
            .expect("Error in updating ranged index");

        let updated_index = BTreeIndex::load(new_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        assert!(
            updated_index.ranges_to_files.is_none(),
            "Updated ranged-btree-index should fall back to non-ranged"
        );

        let updated_value = (DEFAULT_BTREE_BATCH_SIZE * 2 + (DEFAULT_BTREE_BATCH_SIZE / 2)) as i32;
        let updated_query = SargableQuery::Equals(ScalarValue::Int32(Some(updated_value)));

        let query_result = updated_index
            .search(&updated_query, &NoOpMetricsCollector)
            .await
            .unwrap();
        match query_result {
            SearchResult::Exact(row_id_map) => {
                assert!(
                    row_id_map.selected(updated_value as u64),
                    "Updated index should contain original rowids."
                );
                assert!(
                    row_id_map.selected((updated_value + row_id_delta) as u64),
                    "Updated index should contain new rowids"
                );
            }
            _ => {
                panic!("Btree search result should always be Exact.");
            }
        }
    }

    #[tokio::test]
    async fn test_update_with_exact_row_id_filter() {
        let old_tmpdir = TempObjDir::default();
        let old_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            old_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let new_tmpdir = TempObjDir::default();
        let new_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            new_tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let old_data = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(512), BatchCount::from(2));
        let old_data_source = Box::pin(RecordBatchStreamAdapter::new(old_data.schema(), old_data));
        train_btree_index(
            old_data_source,
            old_store.as_ref(),
            DEFAULT_BTREE_BATCH_SIZE,
            None,
            None,
        )
        .await
        .unwrap();

        let index = BTreeIndex::load(old_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let new_data = gen_batch()
            .col("value", array::step_custom::<Int32Type>(2000, 1))
            .col("_rowid", array::step_custom::<UInt64Type>(2000, 1))
            .into_df_stream(RowCount::from(100), BatchCount::from(1));
        let new_data_source = Box::pin(RecordBatchStreamAdapter::new(new_data.schema(), new_data));

        let mut retained_old_rows = RowAddrTreeMap::new();
        retained_old_rows.insert_range(0..64);
        retained_old_rows.insert_range(300..364);

        index
            .update(
                new_data_source,
                new_store.as_ref(),
                Some(OldIndexDataFilter::RowIds(retained_old_rows)),
            )
            .await
            .unwrap();

        let updated_index = BTreeIndex::load(new_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        let present = |value: i32| {
            let updated_index = updated_index.clone();
            async move {
                let query = SargableQuery::Equals(ScalarValue::Int32(Some(value)));
                match updated_index
                    .search(&query, &NoOpMetricsCollector)
                    .await
                    .unwrap()
                {
                    SearchResult::Exact(row_id_map) => row_id_map.selected(value as u64),
                    _ => unreachable!("Btree search result should always be Exact"),
                }
            }
        };

        assert!(present(12).await);
        assert!(present(320).await);
        assert!(!present(120).await);
        assert!(!present(420).await);
        assert!(present(2005).await);
    }

    /// Rust equivalent of Python test `test_btree_remap_big_deletions`
    ///
    /// This test verifies that btree index remapping works correctly when a large
    /// portion of the data is deleted. The Python test:
    /// 1. Writes 15K rows in 3 fragments (values 0-14999)
    /// 2. Creates a btree index (will have multiple pages)
    /// 3. Deletes rows where a > 1000 AND a < 10000 (deletes values 1001-9999)
    /// 4. Runs compaction (materializes deletions via remap)
    /// 5. Verifies the index still works for remaining values
    #[tokio::test]
    async fn test_btree_remap_big_deletions() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Generate 15000 rows with values 0-14999 and row_ids 0-14999
        // Using a smaller batch size to ensure we get multiple pages
        let batch_size = 4096;
        let total_rows = 15000;

        let stream = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(total_rows), BatchCount::from(1));

        train_btree_index(stream, test_store.as_ref(), batch_size, None, None)
            .await
            .unwrap();

        let index = BTreeIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Create a mapping that simulates deleting rows where value > 1000 AND value < 10000
        // Since values match row_ids in our test data:
        // - Rows 0-1000 (values 0-1000) are kept with same row_ids
        // - Rows 1001-9999 (values 1001-9999) are deleted (mapped to None)
        // - Rows 10000-14999 (values 10000-14999) are remapped to new row_ids 1001-5999
        let mut mapping: HashMap<u64, Option<u64>> = HashMap::new();

        // Mark deleted rows (values 1001-9999)
        for old_id in 1001..10000 {
            mapping.insert(old_id, None);
        }

        // Remap all other rows
        for (new_id, old_id) in (100_000..).zip((0..1000).chain(10000..15000)) {
            mapping.insert(old_id, Some(new_id));
        }

        let remap_dir = TempObjDir::default();
        let remap_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            remap_dir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Remap the index with our deletion mapping
        index
            .remap(&RowAddrRemap::direct(mapping), remap_store.as_ref())
            .await
            .unwrap();

        let remapped_index = BTreeIndex::load(remap_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Verify values that should exist (values 0-1000 and 10000-14999)
        // These correspond to: original values 0-1000 at row_ids 0-1000
        // and original values 10000-14999 at new row_ids 1001-5999
        let should_exist = vec![0, 500, 1000, 10000, 13000, 14000, 14999];
        for value in should_exist {
            let query = SargableQuery::Equals(ScalarValue::Int32(Some(value)));
            let result = remapped_index
                .search(&query, &NoOpMetricsCollector)
                .await
                .unwrap();
            match result {
                SearchResult::Exact(row_id_map) => {
                    assert!(
                        !row_id_map.is_empty(),
                        "Value {} should exist in remapped index but was not found",
                        value
                    );
                }
                _ => {
                    panic!("Btree search result should always be Exact.");
                }
            }
        }

        // Verify values that should NOT exist (values 1001-9999 were deleted)
        let should_not_exist = vec![1001, 5000, 8000, 9999];
        for value in should_not_exist {
            let query = SargableQuery::Equals(ScalarValue::Int32(Some(value)));
            let result = remapped_index
                .search(&query, &NoOpMetricsCollector)
                .await
                .unwrap();
            match result {
                SearchResult::Exact(row_id_map) => {
                    assert!(
                        row_id_map.is_empty(),
                        "Value {} should NOT exist in remapped index but was found",
                        value
                    );
                }
                _ => {
                    panic!("Btree search result should always be Exact.");
                }
            }
        }
    }

    /// Regression test: BTree search must track null row IDs for non-IsNull
    /// queries, even when no pages match the queried value.
    ///
    /// Without this, `NOT(x = val)` when `val` is absent from the data would
    /// produce an empty null set, causing NULL rows to incorrectly pass.
    #[tokio::test]
    async fn test_search_tracks_nulls_for_absent_value() {
        use arrow_array::{Int32Array, UInt64Array};

        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // Create data with 80% nulls so that training produces separate
        // all-null pages (which are not in the BTree map). Non-null values
        // are all in [100, 5099], so value 0 never appears.
        let num_rows = 5000u64;
        let values: Int32Array = (0..num_rows)
            .map(|i| {
                if i % 5 != 0 {
                    None // 80% null
                } else {
                    Some(100 + i as i32) // non-null values in [100, 5099]
                }
            })
            .collect();
        let row_ids = UInt64Array::from_iter_values(0..num_rows);
        let data = arrow_array::RecordBatch::try_from_iter(vec![
            ("value", Arc::new(values) as arrow_array::ArrayRef),
            ("_rowid", Arc::new(row_ids) as arrow_array::ArrayRef),
        ])
        .unwrap();

        let schema = data.schema();
        let stream: SendableRecordBatchStream = Box::pin(RecordBatchStreamAdapter::new(
            schema,
            stream::iter(vec![Ok(data)]),
        ));
        train_btree_index(stream, test_store.as_ref(), num_rows, None, None)
            .await
            .unwrap();

        let index = BTreeIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Verify we have all-null pages (the bug depends on this)
        assert!(
            !index.page_lookup.all_null_pages.is_empty(),
            "Test setup requires all-null pages; got null_pages={}, all_null_pages={}",
            index.page_lookup.null_pages.len(),
            index.page_lookup.all_null_pages.len(),
        );

        let metrics = NoOpMetricsCollector;

        // Search for Equals(0) — value 0 doesn't exist in any page
        let result = index
            .search(
                &SargableQuery::Equals(ScalarValue::Int32(Some(0))),
                &metrics,
            )
            .await
            .unwrap();

        match result {
            SearchResult::Exact(set) => {
                // No rows should be TRUE (value 0 doesn't exist)
                assert!(set.true_rows().is_empty(), "No rows should match Equals(0)");
                // NULL rows MUST be tracked as null
                assert!(
                    !set.null_rows().is_empty(),
                    "Null rows must be tracked even when no pages match the value"
                );
            }
            _ => panic!("BTree search should return Exact"),
        }

        // Also verify Range query tracks nulls when no values match
        let result = index
            .search(
                &SargableQuery::Range(
                    std::ops::Bound::Unbounded,
                    std::ops::Bound::Excluded(ScalarValue::Int32(Some(50))),
                ),
                &metrics,
            )
            .await
            .unwrap();

        match result {
            SearchResult::Exact(set) => {
                assert!(set.true_rows().is_empty(), "No rows should be < 50");
                assert!(
                    !set.null_rows().is_empty(),
                    "Null rows must be tracked for range queries too"
                );
            }
            _ => panic!("BTree search should return Exact"),
        }
    }

    fn sample_lookup_batch() -> RecordBatch {
        record_batch!(
            ("min", Int32, [Some(0), Some(10), Some(20)]),
            ("max", Int32, [Some(9), Some(19), Some(29)]),
            ("null_count", UInt32, [0, 2, 0]),
            ("page_idx", UInt32, [0, 1, 2])
        )
        .unwrap()
    }

    fn osv(v: i32) -> OrderableScalarValue {
        OrderableScalarValue(ScalarValue::Int32(Some(v)))
    }

    /// The rewritten [`BTreeLookup`] searches the lookup batch directly, so this
    /// exercises the binary-search bounds, duplicate `min` values, a partial-null
    /// (null `min`) straddling page, and the `Matches::Some`/`All` classification.
    #[test]
    fn test_btree_lookup_pages_between() {
        // Pages sorted by `min`, NULLs first. Page 0 straddles the NULL/non-NULL
        // boundary; pages 2 and 3 share a `min` of 20.
        let batch = record_batch!(
            ("min", Int32, [None, Some(10), Some(20), Some(20), Some(40)]),
            (
                "max",
                Int32,
                [Some(5), Some(20), Some(20), Some(30), Some(50)]
            ),
            ("null_count", UInt32, [2, 0, 0, 0, 0]),
            ("page_idx", UInt32, [0, 1, 2, 3, 4])
        )
        .unwrap();
        let lookup = BTreeLookup::try_new(batch).unwrap();
        assert_eq!(lookup.null_pages, vec![0]);
        assert!(lookup.all_null_pages.is_empty());
        assert_eq!(lookup.search_start, 0);

        let between = |lo: i32, hi: i32| {
            let mut m = lookup
                .pages_between((
                    std::ops::Bound::Included(&osv(lo)),
                    std::ops::Bound::Included(&osv(hi)),
                ))
                .unwrap();
            m.sort_by_key(|m| m.page_id());
            m
        };

        // Equality only ever yields partial (Some) matches.
        assert_eq!(lookup.pages_eq(&osv(15)).unwrap(), vec![Matches::Some(1)]);
        assert_eq!(
            lookup.pages_eq(&osv(20)).unwrap(),
            vec![Matches::Some(1), Matches::Some(2), Matches::Some(3)]
        );
        assert!(lookup.pages_eq(&osv(35)).unwrap().is_empty());

        // [20, 25]: page 2 ([20, 20]) sits entirely inside -> All; pages 1 and 3
        // only partially overlap -> Some. The null-min page 0 (max 5) is excluded.
        assert_eq!(
            between(20, 25),
            vec![Matches::Some(1), Matches::All(2), Matches::Some(3)]
        );

        // A query below all non-null data still reaches the straddling page 0,
        // which is only ever a partial match because its `min` is NULL.
        assert_eq!(between(0, 5), vec![Matches::Some(0)]);

        // Unbounded above: page 4 ([40, 50]) is fully covered from 40 onward.
        assert_eq!(
            lookup
                .pages_between((
                    std::ops::Bound::Included(&osv(40)),
                    std::ops::Bound::Unbounded
                ))
                .unwrap(),
            vec![Matches::All(4)]
        );

        // Empty / inverted ranges select nothing.
        assert!(between(31, 39).is_empty());
        assert!(
            lookup
                .pages_between((
                    std::ops::Bound::Included(&osv(25)),
                    std::ops::Bound::Included(&osv(15))
                ))
                .unwrap()
                .is_empty()
        );
    }

    /// Exercises the native byte comparator path (`accessor_cmp`) for
    /// variable-length `Binary` and fixed-width `FixedSizeBinary` (e.g. UUID)
    /// columns, including the null-min straddle page and duplicate `min`s.
    #[test]
    fn test_btree_lookup_pages_eq_bytes() {
        use arrow_array::{
            ArrayRef, BinaryArray, FixedSizeBinaryArray, LargeBinaryArray, LargeStringArray,
            UInt32Array,
        };
        use arrow_schema::{DataType, Field, Schema};

        // 2-byte big-endian keys, so lexicographic byte order matches numeric
        // order. Same layout as the int test: page 0 is a null-min straddle,
        // pages 2 and 3 share `min` 20, and 35 falls in a gap.
        fn be(v: u16) -> [u8; 2] {
            v.to_be_bytes()
        }
        let mins = [None, Some(10u16), Some(20), Some(20), Some(40)];
        let maxs = [Some(5u16), Some(20), Some(20), Some(30), Some(50)];
        let null_count = UInt32Array::from(vec![2u32, 0, 0, 0, 0]);
        let page_idx = UInt32Array::from(vec![0u32, 1, 2, 3, 4]);

        let assert_byte_lookup =
            |min_arr: ArrayRef, max_arr: ArrayRef, sv: &dyn Fn(u16) -> ScalarValue| {
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("min", min_arr.data_type().clone(), true),
                        Field::new("max", max_arr.data_type().clone(), true),
                        Field::new("null_count", DataType::UInt32, false),
                        Field::new("page_idx", DataType::UInt32, false),
                    ])),
                    vec![
                        min_arr,
                        max_arr,
                        Arc::new(null_count.clone()),
                        Arc::new(page_idx.clone()),
                    ],
                )
                .unwrap();
                let lookup = BTreeLookup::try_new(batch).unwrap();

                let eq = |v: u16| {
                    let mut p: Vec<u32> = lookup
                        .pages_eq(&OrderableScalarValue(sv(v)))
                        .unwrap()
                        .into_iter()
                        .map(|m| m.page_id())
                        .collect();
                    p.sort_unstable();
                    p
                };
                assert_eq!(eq(15), vec![1]); // only page 1 ([10, 20])
                assert_eq!(eq(20), vec![1, 2, 3]); // shared min of 2 & 3, max of 1
                assert!(eq(35).is_empty()); // gap between pages 3 and 4
                assert_eq!(eq(5), vec![0]); // reaches the null-min straddle via its max

                // IN merges and dedups across values.
                let mut in_pages: Vec<u32> = lookup
                    .pages_in([5u16, 15].into_iter().map(|v| OrderableScalarValue(sv(v))))
                    .unwrap()
                    .into_iter()
                    .map(|m| m.page_id())
                    .collect();
                in_pages.sort_unstable();
                assert_eq!(in_pages, vec![0, 1]);
            };

        let fsb = |arr: &[Option<u16>]| -> ArrayRef {
            Arc::new(
                FixedSizeBinaryArray::try_from_sparse_iter_with_size(
                    arr.iter().copied().map(|o| o.map(be)),
                    2,
                )
                .unwrap(),
            )
        };
        assert_byte_lookup(fsb(&mins), fsb(&maxs), &|v| {
            ScalarValue::FixedSizeBinary(2, Some(be(v).to_vec()))
        });

        let bin = |arr: &[Option<u16>]| -> ArrayRef {
            Arc::new(BinaryArray::from_iter(
                arr.iter().copied().map(|o| o.map(|v| be(v).to_vec())),
            ))
        };
        assert_byte_lookup(bin(&mins), bin(&maxs), &|v| {
            ScalarValue::Binary(Some(be(v).to_vec()))
        });

        let lbin = |arr: &[Option<u16>]| -> ArrayRef {
            Arc::new(LargeBinaryArray::from_iter(
                arr.iter().copied().map(|o| o.map(|v| be(v).to_vec())),
            ))
        };
        assert_byte_lookup(lbin(&mins), lbin(&maxs), &|v| {
            ScalarValue::LargeBinary(Some(be(v).to_vec()))
        });

        // `LargeUtf8` over zero-padded decimal strings, whose lexicographic order
        // matches the numeric order of the keys.
        let lstr = |arr: &[Option<u16>]| -> ArrayRef {
            Arc::new(LargeStringArray::from_iter(
                arr.iter().copied().map(|o| o.map(|v| format!("{v:02}"))),
            ))
        };
        assert_byte_lookup(lstr(&mins), lstr(&maxs), &|v| {
            ScalarValue::LargeUtf8(Some(format!("{v:02}")))
        });
    }

    /// Exercises the physical-type reinterpret path: temporal columns (`Date32`
    /// over `i32`, `Timestamp` over `i64`) are compared through the integer native
    /// path without a dedicated per-type branch.
    #[test]
    fn test_btree_lookup_pages_eq_temporal() {
        use arrow_array::{ArrayRef, Date32Array, TimestampMicrosecondArray, UInt32Array};
        use arrow_schema::{DataType, Field, Schema};

        let null_count = UInt32Array::from(vec![2u32, 0, 0, 0, 0]);
        let page_idx = UInt32Array::from(vec![0u32, 1, 2, 3, 4]);

        let assert_lookup =
            |min_arr: ArrayRef, max_arr: ArrayRef, sv: &dyn Fn(i64) -> ScalarValue| {
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("min", min_arr.data_type().clone(), true),
                        Field::new("max", max_arr.data_type().clone(), true),
                        Field::new("null_count", DataType::UInt32, false),
                        Field::new("page_idx", DataType::UInt32, false),
                    ])),
                    vec![
                        min_arr,
                        max_arr,
                        Arc::new(null_count.clone()),
                        Arc::new(page_idx.clone()),
                    ],
                )
                .unwrap();
                let lookup = BTreeLookup::try_new(batch).unwrap();
                let eq = |v: i64| {
                    let mut p: Vec<u32> = lookup
                        .pages_eq(&OrderableScalarValue(sv(v)))
                        .unwrap()
                        .into_iter()
                        .map(|m| m.page_id())
                        .collect();
                    p.sort_unstable();
                    p
                };
                assert_eq!(eq(15), vec![1]); // only page 1 ([10, 20])
                assert_eq!(eq(20), vec![1, 2, 3]); // shared min of 2 & 3, max of 1
                assert!(eq(35).is_empty()); // gap between pages 3 and 4
                assert_eq!(eq(5), vec![0]); // reaches the null-min straddle via its max
            };

        // Timestamp (i64-backed) → Int64 native path.
        assert_lookup(
            Arc::new(TimestampMicrosecondArray::from(vec![
                None,
                Some(10),
                Some(20),
                Some(20),
                Some(40),
            ])),
            Arc::new(TimestampMicrosecondArray::from(vec![
                Some(5),
                Some(20),
                Some(20),
                Some(30),
                Some(50),
            ])),
            &|v| ScalarValue::TimestampMicrosecond(Some(v), None),
        );

        // Date32 (i32-backed) → Int32 native path.
        assert_lookup(
            Arc::new(Date32Array::from(vec![
                None,
                Some(10),
                Some(20),
                Some(20),
                Some(40),
            ])),
            Arc::new(Date32Array::from(vec![
                Some(5),
                Some(20),
                Some(20),
                Some(30),
                Some(50),
            ])),
            &|v| ScalarValue::Date32(Some(v as i32)),
        );
    }

    /// Exercises the remaining physical-type dispatch arms that the temporal and
    /// byte tests don't reach: every integer width and signedness, `Float16`, and
    /// the 128-/256-bit decimal paths. All share the temporal test's numeric layout
    /// (mins `[_, 10, 20, 20, 40]`, maxs `[5, 20, 20, 30, 50]`) so the assertions are
    /// identical; only the array/scalar type varies.
    #[test]
    fn test_btree_lookup_pages_eq_numeric_widths() {
        use arrow::datatypes::i256;
        use arrow_array::{
            ArrayRef, Decimal128Array, Decimal256Array, Float16Array, Int8Array, Int16Array,
            UInt8Array, UInt16Array, UInt32Array, UInt64Array,
        };
        use arrow_schema::{DataType, Field, Schema};
        use half::f16;

        let null_count = UInt32Array::from(vec![2u32, 0, 0, 0, 0]);
        let page_idx = UInt32Array::from(vec![0u32, 1, 2, 3, 4]);
        let assert_lookup =
            |min_arr: ArrayRef, max_arr: ArrayRef, sv: &dyn Fn(i64) -> ScalarValue| {
                let batch = RecordBatch::try_new(
                    Arc::new(Schema::new(vec![
                        Field::new("min", min_arr.data_type().clone(), true),
                        Field::new("max", max_arr.data_type().clone(), true),
                        Field::new("null_count", DataType::UInt32, false),
                        Field::new("page_idx", DataType::UInt32, false),
                    ])),
                    vec![
                        min_arr,
                        max_arr,
                        Arc::new(null_count.clone()),
                        Arc::new(page_idx.clone()),
                    ],
                )
                .unwrap();
                let lookup = BTreeLookup::try_new(batch).unwrap();
                let eq = |v: i64| {
                    let mut p: Vec<u32> = lookup
                        .pages_eq(&OrderableScalarValue(sv(v)))
                        .unwrap()
                        .into_iter()
                        .map(|m| m.page_id())
                        .collect();
                    p.sort_unstable();
                    p
                };
                assert_eq!(eq(15), vec![1]); // only page 1 ([10, 20])
                assert_eq!(eq(20), vec![1, 2, 3]); // shared min of 2 & 3, max of 1
                assert!(eq(35).is_empty()); // gap between pages 3 and 4
                assert_eq!(eq(5), vec![0]); // reaches the null-min straddle via its max
            };

        assert_lookup(
            Arc::new(Int8Array::from(vec![
                None,
                Some(10),
                Some(20),
                Some(20),
                Some(40),
            ])),
            Arc::new(Int8Array::from(vec![
                Some(5),
                Some(20),
                Some(20),
                Some(30),
                Some(50),
            ])),
            &|v| ScalarValue::Int8(Some(v as i8)),
        );
        assert_lookup(
            Arc::new(Int16Array::from(vec![
                None,
                Some(10),
                Some(20),
                Some(20),
                Some(40),
            ])),
            Arc::new(Int16Array::from(vec![
                Some(5),
                Some(20),
                Some(20),
                Some(30),
                Some(50),
            ])),
            &|v| ScalarValue::Int16(Some(v as i16)),
        );
        assert_lookup(
            Arc::new(UInt8Array::from(vec![
                None,
                Some(10),
                Some(20),
                Some(20),
                Some(40),
            ])),
            Arc::new(UInt8Array::from(vec![
                Some(5),
                Some(20),
                Some(20),
                Some(30),
                Some(50),
            ])),
            &|v| ScalarValue::UInt8(Some(v as u8)),
        );
        assert_lookup(
            Arc::new(UInt16Array::from(vec![
                None,
                Some(10),
                Some(20),
                Some(20),
                Some(40),
            ])),
            Arc::new(UInt16Array::from(vec![
                Some(5),
                Some(20),
                Some(20),
                Some(30),
                Some(50),
            ])),
            &|v| ScalarValue::UInt16(Some(v as u16)),
        );
        assert_lookup(
            Arc::new(UInt32Array::from(vec![
                None,
                Some(10),
                Some(20),
                Some(20),
                Some(40),
            ])),
            Arc::new(UInt32Array::from(vec![
                Some(5),
                Some(20),
                Some(20),
                Some(30),
                Some(50),
            ])),
            &|v| ScalarValue::UInt32(Some(v as u32)),
        );
        assert_lookup(
            Arc::new(UInt64Array::from(vec![
                None,
                Some(10),
                Some(20),
                Some(20),
                Some(40),
            ])),
            Arc::new(UInt64Array::from(vec![
                Some(5),
                Some(20),
                Some(20),
                Some(30),
                Some(50),
            ])),
            &|v| ScalarValue::UInt64(Some(v as u64)),
        );

        let f = |v: f64| f16::from_f64(v);
        assert_lookup(
            Arc::new(Float16Array::from(vec![
                None,
                Some(f(10.0)),
                Some(f(20.0)),
                Some(f(20.0)),
                Some(f(40.0)),
            ])),
            Arc::new(Float16Array::from(vec![
                Some(f(5.0)),
                Some(f(20.0)),
                Some(f(20.0)),
                Some(f(30.0)),
                Some(f(50.0)),
            ])),
            &|v| ScalarValue::Float16(Some(f(v as f64))),
        );

        // Decimal128 (i128 native path). Comparison is on the raw integer, so a
        // scale of 0 lets the values double as plain integers.
        let dec128 = |vals: Vec<Option<i128>>| -> ArrayRef {
            Arc::new(
                Decimal128Array::from(vals)
                    .with_precision_and_scale(18, 0)
                    .unwrap(),
            )
        };
        assert_lookup(
            dec128(vec![None, Some(10), Some(20), Some(20), Some(40)]),
            dec128(vec![Some(5), Some(20), Some(20), Some(30), Some(50)]),
            &|v| ScalarValue::Decimal128(Some(v as i128), 18, 0),
        );

        // Decimal256 (i256 native path).
        let dec256 = |vals: Vec<Option<i128>>| -> ArrayRef {
            Arc::new(
                Decimal256Array::from(
                    vals.into_iter()
                        .map(|o| o.map(i256::from_i128))
                        .collect::<Vec<_>>(),
                )
                .with_precision_and_scale(40, 0)
                .unwrap(),
            )
        };
        assert_lookup(
            dec256(vec![None, Some(10), Some(20), Some(20), Some(40)]),
            dec256(vec![Some(5), Some(20), Some(20), Some(30), Some(50)]),
            &|v| ScalarValue::Decimal256(Some(i256::from_i128(v as i128)), 40, 0),
        );
    }

    /// Exercises the NULL paths of the lookup directly: `pages_eq(NULL)` and
    /// `pages_in` with a NULL in the value list (and a NULL-only list), including
    /// the partial-null (`Some`) vs entirely-null (`All`) page classification.
    #[test]
    fn test_btree_lookup_pages_null() {
        // Page 0 is entirely null (null max -> All); page 1 is a partial-null
        // straddle (max 5, null_count > 0 -> Some); page 2 also carries a null.
        let batch = record_batch!(
            ("min", Int32, [None, None, Some(10), Some(20), Some(40)]),
            ("max", Int32, [None, Some(5), Some(20), Some(30), Some(50)]),
            ("null_count", UInt32, [3, 2, 1, 0, 0]),
            ("page_idx", UInt32, [0, 1, 2, 3, 4])
        )
        .unwrap();
        let lookup = BTreeLookup::try_new(batch).unwrap();
        assert_eq!(lookup.all_null_pages, vec![0]);
        assert_eq!(lookup.null_pages, vec![1, 2]);

        // pages_eq(NULL) short-circuits to the null pages: partial-null pages are
        // `Some`, the entirely-null page is `All`.
        assert_eq!(
            lookup
                .pages_eq(&OrderableScalarValue(ScalarValue::Int32(None)))
                .unwrap(),
            vec![Matches::Some(1), Matches::Some(2), Matches::All(0)]
        );

        let in_ids = |vals: Vec<Option<i32>>| {
            let mut p: Vec<u32> = lookup
                .pages_in(
                    vals.into_iter()
                        .map(|v| OrderableScalarValue(ScalarValue::Int32(v))),
                )
                .unwrap()
                .into_iter()
                .map(|m| m.page_id())
                .collect();
            p.sort_unstable();
            p
        };
        // Baseline: a non-null value only -> just its value page.
        assert_eq!(in_ids(vec![Some(45)]), vec![4]);
        // A NULL in the list unions in every null page (0, 1, 2).
        assert_eq!(in_ids(vec![Some(45), None]), vec![0, 1, 2, 4]);
        // A NULL-only list (empty non-null set) returns exactly the null pages.
        assert_eq!(in_ids(vec![None]), vec![0, 1, 2]);
    }

    /// A 0-row page_lookup batch (an index over an empty dataset) must yield no
    /// candidates for any query rather than panicking on the binary-search bounds.
    #[test]
    fn test_btree_lookup_empty_batch() {
        use arrow_schema::{DataType, Field, Schema};

        let schema = Arc::new(Schema::new(vec![
            Field::new("min", DataType::Int32, true),
            Field::new("max", DataType::Int32, true),
            Field::new("null_count", DataType::UInt32, false),
            Field::new("page_idx", DataType::UInt32, false),
        ]));
        let lookup = BTreeLookup::try_new(RecordBatch::new_empty(schema)).unwrap();
        assert_eq!(lookup.search_start, 0);
        assert!(lookup.null_pages.is_empty());
        assert!(lookup.all_null_pages.is_empty());

        assert!(lookup.pages_eq(&osv(5)).unwrap().is_empty());
        assert!(lookup.pages_in([osv(5)]).unwrap().is_empty());
        assert!(
            lookup
                .pages_between((
                    std::ops::Bound::Included(&osv(0)),
                    std::ops::Bound::Included(&osv(100)),
                ))
                .unwrap()
                .is_empty()
        );
        assert!(lookup.pages_null().is_empty());
    }

    /// A straddle page (null `min`, non-null `max`) can sort ahead of an entirely-
    /// null page within the leading NULL-`min` group. When it does, `search_start`
    /// points at the straddle and the all-null page falls inside the forward-scan
    /// window, so both the equality and range scans must skip it (it matches only
    /// IS NULL).
    #[test]
    fn test_btree_lookup_skips_all_null_page_in_scan_window() {
        // Page 0: straddle (null min, max 5). Page 1: entirely null (null min/max).
        let batch = record_batch!(
            ("min", Int32, [None, None, Some(10), Some(20), Some(40)]),
            ("max", Int32, [Some(5), None, Some(20), Some(30), Some(50)]),
            ("null_count", UInt32, [2, 3, 0, 0, 0]),
            ("page_idx", UInt32, [0, 1, 2, 3, 4])
        )
        .unwrap();
        let lookup = BTreeLookup::try_new(batch).unwrap();
        assert_eq!(lookup.search_start, 0); // straddle page 0 has a non-null max
        assert_eq!(lookup.all_null_pages, vec![1]);
        assert_eq!(lookup.null_pages, vec![0]);

        // Equality for 5 peeks left across the all-null page 1 (index 1, inside the
        // scan window) and must skip it, reaching only the straddle page 0.
        assert_eq!(
            lookup
                .pages_eq(&osv(5))
                .unwrap()
                .into_iter()
                .map(|m| m.page_id())
                .collect::<Vec<_>>(),
            vec![0]
        );

        // The same all-null page sits inside the range scan window and is skipped:
        // page 0 (straddle) is a partial match, pages 2-4 are fully covered.
        let mut between = lookup
            .pages_between((
                std::ops::Bound::Included(&osv(0)),
                std::ops::Bound::Included(&osv(100)),
            ))
            .unwrap();
        between.sort_by_key(|m| m.page_id());
        assert_eq!(
            between,
            vec![
                Matches::Some(0),
                Matches::All(2),
                Matches::All(3),
                Matches::All(4),
            ]
        );
    }

    fn assert_state_roundtrips(state: &BTreeIndexState) {
        let restored = deserialize_state(serialize_state(state)).unwrap();
        assert_eq!(restored.lookup_batch, state.lookup_batch);
        assert_eq!(restored.batch_size, state.batch_size);
        assert_eq!(restored.ranges_to_files, state.ranges_to_files);
    }

    #[test]
    fn test_btree_page_key_codec() {
        // FlatIndex pages can be serialized by a persistent cache backend.
        assert!(BTreePageKey::codec().is_some());
    }

    #[test]
    fn test_btree_index_state_roundtrip() {
        // Not range-partitioned.
        assert_state_roundtrips(&BTreeIndexState {
            lookup_batch: sample_lookup_batch(),
            batch_size: DEFAULT_BTREE_BATCH_SIZE,
            ranges_to_files: None,
        });

        // Range-partitioned across multiple files.
        let ranges: RangeInclusiveMap<u32, (String, u32)> = [
            (0..=99, ("part_0_page_file.lance".to_string(), 0)),
            (100..=199, ("part_1_page_file.lance".to_string(), 100)),
        ]
        .into_iter()
        .collect();
        assert_state_roundtrips(&BTreeIndexState {
            lookup_batch: sample_lookup_batch(),
            batch_size: 8192,
            ranges_to_files: Some(Arc::new(ranges)),
        });

        // Empty index.
        assert_state_roundtrips(&BTreeIndexState {
            lookup_batch: RecordBatch::new_empty(sample_lookup_batch().schema()),
            batch_size: DEFAULT_BTREE_BATCH_SIZE,
            ranges_to_files: None,
        });
    }

    #[tokio::test]
    async fn test_btree_index_state_reconstruct_and_plugin_cache() {
        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let stream = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(1000), BatchCount::from(5));
        train_btree_index(stream, test_store.as_ref(), 1000, None, None)
            .await
            .unwrap();

        let index = BTreeIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();

        // Round-trip the state through the codec and reconstruct an index from it.
        let state = BTreeIndexState {
            lookup_batch: index.page_lookup.batch.clone(),
            batch_size: index.batch_size,
            ranges_to_files: index.ranges_to_files.clone(),
        };
        let restored = deserialize_state(serialize_state(&state)).unwrap();
        let reconstructed = restored
            .reconstruct(test_store.clone(), &LanceCache::no_cache(), None)
            .unwrap();
        assert_eq!(
            reconstructed
                .as_any()
                .downcast_ref::<BTreeIndex>()
                .unwrap()
                .page_lookup,
            index.page_lookup
        );

        // The plugin's put/get hooks round-trip through a real cache + the codec.
        let cache = LanceCache::with_capacity(64 * 1024 * 1024);
        let plugin = BTreeIndexPlugin;
        plugin.put_in_cache(&cache, index.clone()).await.unwrap();
        let from_cache = plugin
            .get_from_cache(test_store.clone(), None, &cache)
            .await
            .unwrap()
            .expect("index should be served from the cache");

        // Searches against the cached index match the original.
        let query = SargableQuery::Range(
            std::ops::Bound::Included(ScalarValue::Int32(Some(100))),
            std::ops::Bound::Excluded(ScalarValue::Int32(Some(200))),
        );
        let expected = index.search(&query, &NoOpMetricsCollector).await.unwrap();
        let actual = from_cache
            .search(&query, &NoOpMetricsCollector)
            .await
            .unwrap();
        assert_eq!(expected, actual);
    }

    /// The lookup batch must decode zero-copy through the full envelope even
    /// though the proto header pushes the IPC section to a non-aligned offset.
    #[test]
    fn test_btree_index_state_lookup_is_zero_copy() {
        use lance_core::cache::CacheCodec;
        const ALIGN: usize = 64;

        let ranges: RangeInclusiveMap<u32, (String, u32)> =
            [(0..=99, ("part_0_page_file.lance".to_string(), 0))]
                .into_iter()
                .collect();
        let state = BTreeIndexState {
            lookup_batch: sample_lookup_batch(),
            batch_size: 8192,
            ranges_to_files: Some(Arc::new(ranges)),
        };

        let codec = CacheCodec::from_impl::<BTreeIndexState>();
        let any: Arc<dyn std::any::Any + Send + Sync> = Arc::new(state);
        let mut buf = Vec::new();
        codec.serialize(&any, &mut buf).unwrap();

        let mut v = vec![0u8; buf.len() + ALIGN];
        let pad = (ALIGN - (v.as_ptr() as usize % ALIGN)) % ALIGN;
        v[pad..pad + buf.len()].copy_from_slice(&buf);
        let data = bytes::Bytes::from(v).slice(pad..pad + buf.len());

        let restored = codec.deserialize(&data).hit().unwrap();
        let restored = restored.downcast::<BTreeIndexState>().unwrap();

        let base = data.as_ptr() as usize;
        let end = base + data.len();
        for col in restored.lookup_batch.columns() {
            for buffer in col.to_data().buffers() {
                let ptr = buffer.as_ptr() as usize;
                assert!(
                    ptr >= base && ptr < end,
                    "lookup batch buffer was realigned out of the input — misaligned IPC section",
                );
            }
        }
    }

    #[test]
    fn test_btree_index_state_rejects_truncated_header() {
        // A header length prefix that overruns the buffer must error rather
        // than panic or silently misread it.
        let mut buf = Vec::new();
        buf.extend_from_slice(&100u32.to_le_bytes()); // claims a 100-byte header
        buf.extend_from_slice(&[0u8; 4]); // but only 4 bytes follow
        assert!(deserialize_state(buf).is_err());
    }

    #[tokio::test]
    async fn test_btree_index_state_reconstruct_applies_frag_reuse_index() {
        use crate::frag_reuse::{FragReuseIndex, FragReuseIndexDetails, FragReuseIndexHandle};
        use std::collections::HashMap;
        use uuid::Uuid;

        let tmpdir = TempObjDir::default();
        let test_store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        // value == _rowid for all rows in [0, 1000).
        let stream = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(1000), BatchCount::from(1));
        train_btree_index(stream, test_store.as_ref(), 1000, None, None)
            .await
            .unwrap();

        let index = BTreeIndex::load(test_store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        let state = BTreeIndexState {
            lookup_batch: index.page_lookup.batch.clone(),
            batch_size: index.batch_size,
            ranges_to_files: index.ranges_to_files.clone(),
        };

        // Remap row 0 -> row 5000 (outside the original [0, 1000) range so no collision).
        // Querying for value == 0 should now return row 5000, confirming reconstruct threaded
        // the FragReuseIndex through to the rebuilt BTreeIndex.
        let frag_reuse_index: Arc<dyn crate::scalar::RowIdRemapper> =
            Arc::new(FragReuseIndexHandle(Arc::new(FragReuseIndex::new(
                Uuid::new_v4(),
                vec![HashMap::from([(0u64, Some(5000u64))])],
                FragReuseIndexDetails { versions: vec![] },
            ))));
        let reconstructed = state
            .reconstruct(
                test_store.clone(),
                &LanceCache::no_cache(),
                Some(frag_reuse_index),
            )
            .unwrap();

        let result = reconstructed
            .search(
                &SargableQuery::Equals(ScalarValue::Int32(Some(0))),
                &NoOpMetricsCollector,
            )
            .await
            .unwrap();
        let row_ids: Vec<u64> = match &result {
            SearchResult::Exact(set) => set
                .true_rows()
                .row_addrs()
                .unwrap()
                .map(u64::from)
                .collect(),
            other => panic!("expected Exact, got {other:?}"),
        };
        assert_eq!(
            row_ids,
            vec![5000],
            "frag_reuse_index remap was not applied"
        );
    }

    #[tokio::test]
    async fn test_btree_index_state_range_partitioned_plugin_cache_roundtrip() {
        // Build a range-partitioned BTree (two range partitions merged into one index) and
        // round-trip it through the plugin's cache hooks. This exercises the
        // `ranges_to_files = Some` path end-to-end through serialize/deserialize/reconstruct.
        let tmpdir = TempObjDir::default();
        let store = Arc::new(LanceIndexStore::new(
            Arc::new(ObjectStore::local()),
            tmpdir.clone(),
            Arc::new(LanceCache::no_cache()),
        ));

        let half = DEFAULT_BTREE_BATCH_SIZE;
        let total = (2 * half) as i32;

        // Partition 0: values/rowids [0, half).
        let part0 = gen_batch()
            .col("value", array::step::<Int32Type>())
            .col("_rowid", array::step::<UInt64Type>())
            .into_df_stream(RowCount::from(half), BatchCount::from(1));
        train_btree_index(part0, store.as_ref(), half, None, Some(0u32))
            .await
            .unwrap();

        // Partition 1: values/rowids [half, 2*half).
        let values: Vec<i32> = (half as i32..total).collect();
        let row_ids: Vec<u64> = (half..total as u64).collect();
        let part1 = gen_batch()
            .col("value", array::cycle::<Int32Type>(values))
            .col("_rowid", array::cycle::<UInt64Type>(row_ids))
            .into_df_stream(RowCount::from(half), BatchCount::from(1));
        train_btree_index(part1, store.as_ref(), half, None, Some(1u32))
            .await
            .unwrap();

        super::merge_metadata_files(
            store.as_ref(),
            &[
                part_page_data_file_path(0 << 32),
                part_page_data_file_path(1 << 32),
            ],
            &[
                part_lookup_file_path(0 << 32),
                part_lookup_file_path(1 << 32),
            ],
            Some(1usize),
            noop_progress(),
        )
        .await
        .unwrap();

        let index = BTreeIndex::load(store.clone(), None, &LanceCache::no_cache())
            .await
            .unwrap();
        assert!(
            index.ranges_to_files.is_some(),
            "test setup should produce a range-partitioned index",
        );

        let cache = LanceCache::with_capacity(64 * 1024 * 1024);
        let plugin = BTreeIndexPlugin;
        plugin.put_in_cache(&cache, index.clone()).await.unwrap();
        let from_cache = plugin
            .get_from_cache(store.clone(), None, &cache)
            .await
            .unwrap()
            .expect("index should be served from the cache");

        // Search a value from each range partition and confirm both paths agree.
        for value in [0i32, total - 1] {
            let query = SargableQuery::Equals(ScalarValue::Int32(Some(value)));
            let expected = index.search(&query, &NoOpMetricsCollector).await.unwrap();
            let actual = from_cache
                .search(&query, &NoOpMetricsCollector)
                .await
                .unwrap();
            assert_eq!(expected, actual, "value {value}");
        }
    }
}
