// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! In-memory BTree index for scalar fields.
//!
//! Provides O(log n) lookups and range queries. Used for primary key lookups
//! and scalar column filtering.
//!
//! Backed by [`super::arena_skiplist`] — a single-writer, lock-free-read
//! skiplist with no epoch reclamation. Reads (the point-lookup hot path and
//! scans) take no lock and no epoch pin; writes go through an (uncontended,
//! since the MemTable serializes them) `Mutex`.
//!
//! Three backends, chosen lazily by column type on first insert. The compact
//! backends store only a small comparable key + the row position (not the fat
//! value) so nodes are RocksDB-sized; the value is decoded from the key at
//! flush. Small nodes match RocksDB's cache behavior on the bottom-level walk.
//! - [`FixedIntBackend`] for fixed-width integers/dates: key is a compact
//!   [`FixedKey`] `{ order-preserving u64, position }` (~24B node).
//! - [`BytesBackend`] for strings / binary / `FixedSizeBinary` (UUID): key is
//!   [`BytesKey`] with the bytes stored inline for small values (UUID, short
//!   keys) and boxed only for long ones.
//! - [`ScalarBackend`] for everything else: the original `OrderableScalarValue`
//!   key (fat node, but handles arbitrary scalar types).

use std::sync::{Mutex, OnceLock};

use arrow_array::types::*;
use arrow_array::{Array, RecordBatch};
use arrow_schema::DataType;
use datafusion::common::ScalarValue;
use lance_core::{Error, Result};
use lance_index::scalar::btree::OrderableScalarValue;

use super::RowPosition;
use super::arena_skiplist::{SkipListReader, SkipListWriter, new_skiplist};

/// Composite key for the scalar (fallback) backend.
///
/// By combining (scalar_value, row_position), each entry is unique.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct IndexKey {
    /// The indexed scalar value.
    pub value: OrderableScalarValue,
    /// Row position (makes the key unique for non-unique indexes).
    pub row_position: RowPosition,
}

impl PartialOrd for IndexKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IndexKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // First compare by value, then by row_position
        match self.value.cmp(&other.value) {
            std::cmp::Ordering::Equal => self.row_position.cmp(&other.row_position),
            ord => ord,
        }
    }
}

/// Compact key for the fixed-width-integer backend: an order-preserving `u64`
/// encoding of the value plus the row position. Sorts by `(enc, position)` —
/// identical ordering to `(value, position)` because `enc` is order-preserving.
/// 16 bytes, so a node is ~24B (vs ~72B for the `OrderableScalarValue` node).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct FixedKey {
    enc: u64,
    position: RowPosition,
}

/// Sign-flip a signed integer to an order-preserving unsigned key.
#[inline]
fn encode_signed(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

#[inline]
fn decode_signed(enc: u64) -> i64 {
    (enc ^ (1u64 << 63)) as i64
}

/// Whether `dt` is handled by the compact fixed-width-integer backend.
fn is_fixed_int(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Date32
            | DataType::Date64
    )
}

/// Order-preserving `u64` encoding of a fixed-int `ScalarValue`, or `None` if
/// the value is null or not a fixed-int type.
fn encode_scalar(value: &ScalarValue) -> Option<u64> {
    Some(match value {
        ScalarValue::Int8(Some(v)) => encode_signed(*v as i64),
        ScalarValue::Int16(Some(v)) => encode_signed(*v as i64),
        ScalarValue::Int32(Some(v)) => encode_signed(*v as i64),
        ScalarValue::Int64(Some(v)) => encode_signed(*v),
        ScalarValue::UInt8(Some(v)) => *v as u64,
        ScalarValue::UInt16(Some(v)) => *v as u64,
        ScalarValue::UInt32(Some(v)) => *v as u64,
        ScalarValue::UInt64(Some(v)) => *v,
        ScalarValue::Date32(Some(v)) => encode_signed(*v as i64),
        ScalarValue::Date64(Some(v)) => encode_signed(*v),
        _ => return None,
    })
}

/// Decode a fixed-int `enc` back to its typed `ScalarValue` for `data_type`.
fn decode_enc(enc: u64, data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Int8 => ScalarValue::Int8(Some(decode_signed(enc) as i8)),
        DataType::Int16 => ScalarValue::Int16(Some(decode_signed(enc) as i16)),
        DataType::Int32 => ScalarValue::Int32(Some(decode_signed(enc) as i32)),
        DataType::Int64 => ScalarValue::Int64(Some(decode_signed(enc))),
        DataType::UInt8 => ScalarValue::UInt8(Some(enc as u8)),
        DataType::UInt16 => ScalarValue::UInt16(Some(enc as u16)),
        DataType::UInt32 => ScalarValue::UInt32(Some(enc as u32)),
        DataType::UInt64 => ScalarValue::UInt64(Some(enc)),
        DataType::Date32 => ScalarValue::Date32(Some(decode_signed(enc) as i32)),
        DataType::Date64 => ScalarValue::Date64(Some(decode_signed(enc))),
        other => unreachable!("decode_enc on non-fixed-int type {other:?}"),
    }
}

/// The typed null `ScalarValue` for a fixed-int `data_type`.
fn null_scalar(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Int8 => ScalarValue::Int8(None),
        DataType::Int16 => ScalarValue::Int16(None),
        DataType::Int32 => ScalarValue::Int32(None),
        DataType::Int64 => ScalarValue::Int64(None),
        DataType::UInt8 => ScalarValue::UInt8(None),
        DataType::UInt16 => ScalarValue::UInt16(None),
        DataType::UInt32 => ScalarValue::UInt32(None),
        DataType::UInt64 => ScalarValue::UInt64(None),
        DataType::Date32 => ScalarValue::Date32(None),
        DataType::Date64 => ScalarValue::Date64(None),
        other => unreachable!("null_scalar on non-fixed-int type {other:?}"),
    }
}

/// Max key length stored inline in [`InlineBytes`]. 23 covers a 16-byte UUID
/// (`FixedSizeBinary(16)`) and short string/binary primary keys, keeping the
/// node a single allocation (no second cache miss on the seek).
const INLINE_CAP: usize = 23;

/// A byte key that lives inline in the node for small values and spills to the
/// heap only for long ones — so the common cases (UUID, short string PKs) get
/// the small-node win, and long keys still work (with the usual boxed penalty).
enum InlineBytes {
    Inline { len: u8, buf: [u8; INLINE_CAP] },
    Heap(Box<[u8]>),
}

impl InlineBytes {
    fn new(bytes: &[u8]) -> Self {
        if bytes.len() <= INLINE_CAP {
            let mut buf = [0u8; INLINE_CAP];
            buf[..bytes.len()].copy_from_slice(bytes);
            Self::Inline {
                len: bytes.len() as u8,
                buf,
            }
        } else {
            Self::Heap(bytes.into())
        }
    }

    #[inline]
    fn as_slice(&self) -> &[u8] {
        match self {
            Self::Inline { len, buf } => &buf[..*len as usize],
            Self::Heap(b) => b,
        }
    }
}

impl PartialEq for InlineBytes {
    fn eq(&self, other: &Self) -> bool {
        self.as_slice() == other.as_slice()
    }
}
impl Eq for InlineBytes {}
impl PartialOrd for InlineBytes {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for InlineBytes {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_slice().cmp(other.as_slice())
    }
}

/// Compact key for the byte backend: an order-preserving inline byte key plus
/// the row position. Sorts by `(bytes, position)` — lexicographic byte order is
/// the natural order for strings, binary, and `FixedSizeBinary`/UUID.
struct BytesKey {
    bytes: InlineBytes,
    position: RowPosition,
}

impl PartialEq for BytesKey {
    fn eq(&self, other: &Self) -> bool {
        self.position == other.position && self.bytes == other.bytes
    }
}
impl Eq for BytesKey {}
impl PartialOrd for BytesKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for BytesKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bytes
            .cmp(&other.bytes)
            .then(self.position.cmp(&other.position))
    }
}

/// Whether `dt` is handled by the compact byte backend (strings / binary /
/// fixed-size binary, including UUID).
fn is_bytes_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::FixedSizeBinary(_)
    )
}

/// The order-preserving key bytes for a byte-typed `ScalarValue`, or `None` if
/// null or not a byte type. Strings encode as their UTF-8 bytes.
fn value_bytes(value: &ScalarValue) -> Option<&[u8]> {
    match value {
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => Some(s.as_bytes()),
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => Some(b.as_slice()),
        _ => None,
    }
}

/// Decode key bytes back to a typed `ScalarValue` for `data_type`.
fn decode_bytes(bytes: &[u8], data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Utf8 => ScalarValue::Utf8(Some(String::from_utf8_lossy(bytes).into_owned())),
        DataType::LargeUtf8 => {
            ScalarValue::LargeUtf8(Some(String::from_utf8_lossy(bytes).into_owned()))
        }
        DataType::Binary => ScalarValue::Binary(Some(bytes.to_vec())),
        DataType::LargeBinary => ScalarValue::LargeBinary(Some(bytes.to_vec())),
        DataType::FixedSizeBinary(n) => ScalarValue::FixedSizeBinary(*n, Some(bytes.to_vec())),
        other => unreachable!("decode_bytes on non-byte type {other:?}"),
    }
}

/// The typed null `ScalarValue` for a byte `data_type`.
fn null_bytes_scalar(data_type: &DataType) -> ScalarValue {
    match data_type {
        DataType::Utf8 => ScalarValue::Utf8(None),
        DataType::LargeUtf8 => ScalarValue::LargeUtf8(None),
        DataType::Binary => ScalarValue::Binary(None),
        DataType::LargeBinary => ScalarValue::LargeBinary(None),
        DataType::FixedSizeBinary(n) => ScalarValue::FixedSizeBinary(*n, None),
        other => unreachable!("null_bytes_scalar on non-byte type {other:?}"),
    }
}

/// Compact backend for fixed-width integers / dates. The skiplist holds only
/// non-null entries as [`FixedKey`]; nulls are tracked separately (they never
/// appear in concrete point lookups and sort first at flush).
struct FixedIntBackend {
    reader: SkipListReader<FixedKey>,
    writer: Mutex<SkipListWriter<FixedKey>>,
    /// Row positions whose value is null (rare; not on the hot path).
    null_positions: Mutex<Vec<RowPosition>>,
    data_type: DataType,
}

impl FixedIntBackend {
    fn new(data_type: DataType) -> Self {
        let (writer, reader) = new_skiplist::<FixedKey>();
        Self {
            reader,
            writer: Mutex::new(writer),
            null_positions: Mutex::new(Vec::new()),
            data_type,
        }
    }

    fn insert_array_and_report_existing(&self, array: &dyn Array, row_offset: u64) -> Result<bool> {
        let mut had_existing = false;
        macro_rules! insert_int {
            ($array_type:ty, $to_i64:expr) => {{
                let typed = array
                    .as_any()
                    .downcast_ref::<arrow_array::PrimitiveArray<$array_type>>()
                    .unwrap();
                let mut writer = self.writer.lock().unwrap();
                let mut nulls: Vec<RowPosition> = Vec::new();
                let had_existing_nulls = !self.null_positions.lock().unwrap().is_empty();
                let mut saw_null = false;
                for (row_idx, value) in typed.iter().enumerate() {
                    let position = row_offset + row_idx as u64;
                    match value {
                        Some(v) => {
                            let enc = $to_i64(v);
                            let key = FixedKey { enc, position };
                            had_existing |= writer.insert_and_check_neighbors(key, |prev, next| {
                                prev.is_some_and(|key| key.enc == enc)
                                    || next.is_some_and(|key| key.enc == enc)
                            });
                        }
                        None => {
                            if had_existing_nulls || saw_null {
                                had_existing = true;
                            }
                            saw_null = true;
                            nulls.push(position);
                        }
                    }
                }
                drop(writer);
                if !nulls.is_empty() {
                    self.null_positions.lock().unwrap().extend(nulls);
                }
            }};
        }

        match array.data_type() {
            DataType::Int8 => insert_int!(Int8Type, |v: i8| encode_signed(v as i64)),
            DataType::Int16 => insert_int!(Int16Type, |v: i16| encode_signed(v as i64)),
            DataType::Int32 => insert_int!(Int32Type, |v: i32| encode_signed(v as i64)),
            DataType::Int64 => insert_int!(Int64Type, encode_signed),
            DataType::UInt8 => insert_int!(UInt8Type, |v: u8| v as u64),
            DataType::UInt16 => insert_int!(UInt16Type, |v: u16| v as u64),
            DataType::UInt32 => insert_int!(UInt32Type, |v: u32| v as u64),
            DataType::UInt64 => insert_int!(UInt64Type, |v: u64| v),
            DataType::Date32 => insert_int!(Date32Type, |v: i32| encode_signed(v as i64)),
            DataType::Date64 => insert_int!(Date64Type, encode_signed),
            other => {
                return Err(Error::invalid_input(format!(
                    "FixedIntBackend received non-fixed-int array {other:?}"
                )));
            }
        }
        Ok(had_existing)
    }

    fn get_newest_visible(
        &self,
        value: &ScalarValue,
        max_visible_row: RowPosition,
    ) -> Option<RowPosition> {
        // Concrete value lookups never hit nulls. A null query falls back to
        // the newest visible null position.
        let Some(enc) = encode_scalar(value) else {
            if value.is_null() {
                return self
                    .null_positions
                    .lock()
                    .unwrap()
                    .iter()
                    .copied()
                    .filter(|p| *p <= max_visible_row)
                    .max();
            }
            return None;
        };
        let target = FixedKey {
            enc,
            position: max_visible_row,
        };
        self.reader
            .upper_bound_with(&target, |key| (key.enc == enc).then_some(key.position))
            .flatten()
    }

    fn get(&self, value: &ScalarValue) -> Vec<RowPosition> {
        let Some(enc) = encode_scalar(value) else {
            if value.is_null() {
                return self.null_positions.lock().unwrap().clone();
            }
            return Vec::new();
        };
        let start = FixedKey { enc, position: 0 };
        let mut positions = Vec::new();
        for key in self.reader.range_from(&start) {
            if key.enc != enc {
                break;
            }
            positions.push(key.position);
        }
        positions
    }

    fn len(&self) -> usize {
        self.reader.len() + self.null_positions.lock().unwrap().len()
    }

    fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn snapshot(&self) -> Vec<(OrderableScalarValue, Vec<RowPosition>)> {
        let mut result: Vec<(OrderableScalarValue, Vec<RowPosition>)> = Vec::new();

        // Nulls sort first (None < Some), matching OrderableScalarValue order.
        let nulls = self.null_positions.lock().unwrap();
        if !nulls.is_empty() {
            let mut positions = nulls.clone();
            positions.sort_unstable();
            result.push((
                OrderableScalarValue(null_scalar(&self.data_type)),
                positions,
            ));
        }
        drop(nulls);

        let mut cur_enc: Option<u64> = None;
        for key in self.reader.iter() {
            if cur_enc == Some(key.enc) {
                result.last_mut().unwrap().1.push(key.position);
            } else {
                cur_enc = Some(key.enc);
                result.push((
                    OrderableScalarValue(decode_enc(key.enc, &self.data_type)),
                    vec![key.position],
                ));
            }
        }
        result
    }
}

/// Compact backend for byte-typed columns (strings / binary / `FixedSizeBinary`
/// / UUID). The skiplist holds non-null entries as [`BytesKey`] (key bytes
/// inline for small values); nulls are tracked separately and sort first.
struct BytesBackend {
    reader: SkipListReader<BytesKey>,
    writer: Mutex<SkipListWriter<BytesKey>>,
    null_positions: Mutex<Vec<RowPosition>>,
    data_type: DataType,
}

impl BytesBackend {
    fn new(data_type: DataType) -> Self {
        let (writer, reader) = new_skiplist::<BytesKey>();
        Self {
            reader,
            writer: Mutex::new(writer),
            null_positions: Mutex::new(Vec::new()),
            data_type,
        }
    }

    fn insert_array_and_report_existing(&self, array: &dyn Array, row_offset: u64) -> Result<bool> {
        let mut had_existing = false;
        // Append (position, key bytes) for each row; nulls go to the side list.
        // `$v => $to_bytes` extracts the key bytes from each non-null value
        // inline (no closure, so the borrow ties directly to the row value).
        macro_rules! insert_bytes {
            ($array_type:ty, $v:ident => $to_bytes:expr) => {{
                let typed = array.as_any().downcast_ref::<$array_type>().unwrap();
                let mut writer = self.writer.lock().unwrap();
                let mut nulls: Vec<RowPosition> = Vec::new();
                let had_existing_nulls = !self.null_positions.lock().unwrap().is_empty();
                let mut saw_null = false;
                for row_idx in 0..typed.len() {
                    let position = row_offset + row_idx as u64;
                    if typed.is_null(row_idx) {
                        if had_existing_nulls || saw_null {
                            had_existing = true;
                        }
                        saw_null = true;
                        nulls.push(position);
                    } else {
                        let $v = typed.value(row_idx);
                        let bytes: &[u8] = $to_bytes;
                        let key = BytesKey {
                            bytes: InlineBytes::new(bytes),
                            position,
                        };
                        had_existing |= writer.insert_and_check_neighbors(key, |prev, next| {
                            prev.is_some_and(|key| key.bytes.as_slice() == bytes)
                                || next.is_some_and(|key| key.bytes.as_slice() == bytes)
                        });
                    }
                }
                drop(writer);
                if !nulls.is_empty() {
                    self.null_positions.lock().unwrap().extend(nulls);
                }
            }};
        }

        match array.data_type() {
            DataType::Utf8 => insert_bytes!(arrow_array::StringArray, v => v.as_bytes()),
            DataType::LargeUtf8 => insert_bytes!(arrow_array::LargeStringArray, v => v.as_bytes()),
            DataType::Binary => insert_bytes!(arrow_array::BinaryArray, v => v),
            DataType::LargeBinary => insert_bytes!(arrow_array::LargeBinaryArray, v => v),
            DataType::FixedSizeBinary(_) => {
                insert_bytes!(arrow_array::FixedSizeBinaryArray, v => v)
            }
            other => {
                return Err(Error::invalid_input(format!(
                    "BytesBackend received non-byte array {other:?}"
                )));
            }
        }
        Ok(had_existing)
    }

    fn get_newest_visible(
        &self,
        value: &ScalarValue,
        max_visible_row: RowPosition,
    ) -> Option<RowPosition> {
        let Some(bytes) = value_bytes(value) else {
            if value.is_null() {
                return self
                    .null_positions
                    .lock()
                    .unwrap()
                    .iter()
                    .copied()
                    .filter(|p| *p <= max_visible_row)
                    .max();
            }
            return None;
        };
        let target = BytesKey {
            bytes: InlineBytes::new(bytes),
            position: max_visible_row,
        };
        self.reader
            .upper_bound_with(&target, |key| {
                (key.bytes.as_slice() == bytes).then_some(key.position)
            })
            .flatten()
    }

    fn get(&self, value: &ScalarValue) -> Vec<RowPosition> {
        let Some(bytes) = value_bytes(value) else {
            if value.is_null() {
                return self.null_positions.lock().unwrap().clone();
            }
            return Vec::new();
        };
        let start = BytesKey {
            bytes: InlineBytes::new(bytes),
            position: 0,
        };
        let mut positions = Vec::new();
        for key in self.reader.range_from(&start) {
            if key.bytes.as_slice() != bytes {
                break;
            }
            positions.push(key.position);
        }
        positions
    }

    fn len(&self) -> usize {
        self.reader.len() + self.null_positions.lock().unwrap().len()
    }

    fn data_type(&self) -> DataType {
        self.data_type.clone()
    }

    fn snapshot(&self) -> Vec<(OrderableScalarValue, Vec<RowPosition>)> {
        let mut result: Vec<(OrderableScalarValue, Vec<RowPosition>)> = Vec::new();

        // Nulls sort first (None < Some).
        let nulls = self.null_positions.lock().unwrap();
        if !nulls.is_empty() {
            let mut positions = nulls.clone();
            positions.sort_unstable();
            result.push((
                OrderableScalarValue(null_bytes_scalar(&self.data_type)),
                positions,
            ));
        }
        drop(nulls);

        let mut cur: Option<Vec<u8>> = None;
        for key in self.reader.iter() {
            let bytes = key.bytes.as_slice();
            if cur.as_deref() == Some(bytes) {
                result.last_mut().unwrap().1.push(key.position);
            } else {
                cur = Some(bytes.to_vec());
                result.push((
                    OrderableScalarValue(decode_bytes(bytes, &self.data_type)),
                    vec![key.position],
                ));
            }
        }
        result
    }
}

/// Fallback backend for arbitrary scalar types, keyed by `OrderableScalarValue`.
struct ScalarBackend {
    reader: SkipListReader<IndexKey>,
    writer: Mutex<SkipListWriter<IndexKey>>,
}

impl ScalarBackend {
    fn new() -> Self {
        let (writer, reader) = new_skiplist::<IndexKey>();
        Self {
            reader,
            writer: Mutex::new(writer),
        }
    }

    fn add(&self, value: OrderableScalarValue, row_position: RowPosition) -> bool {
        let probe = value.clone();
        self.writer.lock().unwrap().insert_and_check_neighbors(
            IndexKey {
                value,
                row_position,
            },
            |prev, next| {
                prev.is_some_and(|key| key.value == probe)
                    || next.is_some_and(|key| key.value == probe)
            },
        )
    }

    fn insert_array_and_report_existing(&self, array: &dyn Array, row_offset: u64) -> Result<bool> {
        let mut had_existing = false;
        macro_rules! insert_primitive {
            ($array_type:ty, $scalar_variant:ident) => {{
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::PrimitiveArray<$array_type>>()
                    .unwrap();
                for (row_idx, value) in typed_array.iter().enumerate() {
                    let row_position = row_offset + row_idx as u64;
                    had_existing |= self.add(
                        OrderableScalarValue(ScalarValue::$scalar_variant(value)),
                        row_position,
                    );
                }
            }};
        }

        match array.data_type() {
            DataType::Int8 => insert_primitive!(Int8Type, Int8),
            DataType::Int16 => insert_primitive!(Int16Type, Int16),
            DataType::Int32 => insert_primitive!(Int32Type, Int32),
            DataType::Int64 => insert_primitive!(Int64Type, Int64),
            DataType::UInt8 => insert_primitive!(UInt8Type, UInt8),
            DataType::UInt16 => insert_primitive!(UInt16Type, UInt16),
            DataType::UInt32 => insert_primitive!(UInt32Type, UInt32),
            DataType::UInt64 => insert_primitive!(UInt64Type, UInt64),
            DataType::Float32 => insert_primitive!(Float32Type, Float32),
            DataType::Float64 => insert_primitive!(Float64Type, Float64),
            DataType::Date32 => insert_primitive!(Date32Type, Date32),
            DataType::Date64 => insert_primitive!(Date64Type, Date64),
            DataType::Utf8 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::StringArray>()
                    .unwrap();
                for (row_idx, value) in typed_array.iter().enumerate() {
                    let row_position = row_offset + row_idx as u64;
                    had_existing |= self.add(
                        OrderableScalarValue(ScalarValue::Utf8(value.map(|s| s.to_string()))),
                        row_position,
                    );
                }
            }
            DataType::LargeUtf8 => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::LargeStringArray>()
                    .unwrap();
                for (row_idx, value) in typed_array.iter().enumerate() {
                    let row_position = row_offset + row_idx as u64;
                    had_existing |= self.add(
                        OrderableScalarValue(ScalarValue::LargeUtf8(value.map(|s| s.to_string()))),
                        row_position,
                    );
                }
            }
            DataType::Boolean => {
                let typed_array = array
                    .as_any()
                    .downcast_ref::<arrow_array::BooleanArray>()
                    .unwrap();
                for (row_idx, value) in typed_array.iter().enumerate() {
                    let row_position = row_offset + row_idx as u64;
                    had_existing |= self.add(
                        OrderableScalarValue(ScalarValue::Boolean(value)),
                        row_position,
                    );
                }
            }
            // Fallback for other types - use per-row extraction
            _ => {
                for row_idx in 0..array.len() {
                    let value = ScalarValue::try_from_array(array, row_idx)?;
                    let row_position = row_offset + row_idx as u64;
                    had_existing |= self.add(OrderableScalarValue(value), row_position);
                }
            }
        }
        Ok(had_existing)
    }

    fn get_newest_visible(
        &self,
        value: &ScalarValue,
        max_visible_row: RowPosition,
    ) -> Option<RowPosition> {
        let target = IndexKey {
            value: OrderableScalarValue(value.clone()),
            row_position: max_visible_row,
        };
        self.reader
            .upper_bound_with(&target, |key| {
                (key.value.0 == *value).then_some(key.row_position)
            })
            .flatten()
    }

    fn get(&self, value: &ScalarValue) -> Vec<RowPosition> {
        let start = IndexKey {
            value: OrderableScalarValue(value.clone()),
            row_position: 0,
        };
        let mut positions = Vec::new();
        for key in self.reader.range_from(&start) {
            if key.value.0 != *value {
                break;
            }
            positions.push(key.row_position);
        }
        positions
    }

    fn len(&self) -> usize {
        self.reader.len()
    }

    fn data_type(&self) -> Option<DataType> {
        self.reader.front_with(|key| key.value.0.data_type())
    }

    fn snapshot(&self) -> Vec<(OrderableScalarValue, Vec<RowPosition>)> {
        let mut result: Vec<(OrderableScalarValue, Vec<RowPosition>)> = Vec::new();
        for key in self.reader.iter() {
            if let Some(last) = result.last_mut()
                && last.0 == key.value
            {
                last.1.push(key.row_position);
                continue;
            }
            result.push((key.value.clone(), vec![key.row_position]));
        }
        result
    }
}

/// The chosen backend for a `BTreeMemIndex`, selected by column type.
enum Backend {
    FixedInt(FixedIntBackend),
    Bytes(BytesBackend),
    Scalar(ScalarBackend),
}

impl Backend {
    fn for_type(data_type: &DataType) -> Self {
        if is_fixed_int(data_type) {
            Self::FixedInt(FixedIntBackend::new(data_type.clone()))
        } else if is_bytes_type(data_type) {
            Self::Bytes(BytesBackend::new(data_type.clone()))
        } else {
            Self::Scalar(ScalarBackend::new())
        }
    }

    fn insert_array_and_report_existing(&self, array: &dyn Array, row_offset: u64) -> Result<bool> {
        match self {
            Self::FixedInt(b) => b.insert_array_and_report_existing(array, row_offset),
            Self::Bytes(b) => b.insert_array_and_report_existing(array, row_offset),
            Self::Scalar(b) => b.insert_array_and_report_existing(array, row_offset),
        }
    }

    fn get_newest_visible(&self, value: &ScalarValue, max: RowPosition) -> Option<RowPosition> {
        match self {
            Self::FixedInt(b) => b.get_newest_visible(value, max),
            Self::Bytes(b) => b.get_newest_visible(value, max),
            Self::Scalar(b) => b.get_newest_visible(value, max),
        }
    }

    fn get(&self, value: &ScalarValue) -> Vec<RowPosition> {
        match self {
            Self::FixedInt(b) => b.get(value),
            Self::Bytes(b) => b.get(value),
            Self::Scalar(b) => b.get(value),
        }
    }

    fn len(&self) -> usize {
        match self {
            Self::FixedInt(b) => b.len(),
            Self::Bytes(b) => b.len(),
            Self::Scalar(b) => b.len(),
        }
    }

    fn data_type(&self) -> Option<DataType> {
        match self {
            Self::FixedInt(b) => Some(b.data_type()),
            Self::Bytes(b) => Some(b.data_type()),
            Self::Scalar(b) => b.data_type(),
        }
    }

    fn snapshot(&self) -> Vec<(OrderableScalarValue, Vec<RowPosition>)> {
        match self {
            Self::FixedInt(b) => b.snapshot(),
            Self::Bytes(b) => b.snapshot(),
            Self::Scalar(b) => b.snapshot(),
        }
    }
}

/// In-memory BTree index for scalar fields.
///
/// The backing `Backend` is selected lazily on first insert from the column's
/// Arrow type: a compact `FixedKey` for fixed-width integers, fat
/// `OrderableScalarValue` for everything else. Before the first insert the index
/// is empty (all reads return empty / `None`).
pub struct BTreeMemIndex {
    backend: OnceLock<Backend>,
    /// Field ID this index is built on.
    field_id: i32,
    /// Column name (for Arrow batch lookups).
    column_name: String,
}

impl std::fmt::Debug for BTreeMemIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BTreeMemIndex")
            .field("field_id", &self.field_id)
            .field("column_name", &self.column_name)
            .field("len", &self.len())
            .finish()
    }
}

impl BTreeMemIndex {
    /// Create a new BTree index for the given field.
    pub fn new(field_id: i32, column_name: String) -> Self {
        Self {
            backend: OnceLock::new(),
            field_id,
            column_name,
        }
    }

    /// The newest row position for `value` that is visible at `max_visible_row`
    /// (inclusive), or `None` if the value has no visible row. A single
    /// **seek-and-stop** on the backing skiplist — no range collect, no
    /// allocation. This is the point-lookup hot path.
    pub fn get_newest_visible(
        &self,
        value: &ScalarValue,
        max_visible_row: RowPosition,
    ) -> Option<RowPosition> {
        self.backend
            .get()?
            .get_newest_visible(value, max_visible_row)
    }

    /// Get the field ID this index is built on.
    pub fn field_id(&self) -> i32 {
        self.field_id
    }

    /// Insert rows from a batch into the index.
    pub fn insert(&self, batch: &RecordBatch, row_offset: u64) -> Result<()> {
        self.insert_and_report_existing(batch, row_offset)
            .map(|_| ())
    }

    /// Insert rows and report whether any inserted key already existed in the
    /// index or repeated earlier in this batch.
    pub fn insert_and_report_existing(&self, batch: &RecordBatch, row_offset: u64) -> Result<bool> {
        let col_idx = batch
            .schema()
            .column_with_name(&self.column_name)
            .map(|(idx, _)| idx)
            .ok_or_else(|| {
                Error::invalid_input(format!("Column '{}' not found in batch", self.column_name))
            })?;

        let column = batch.column(col_idx);
        let backend = self
            .backend
            .get_or_init(|| Backend::for_type(column.data_type()));
        backend.insert_array_and_report_existing(column.as_ref(), row_offset)
    }

    /// Look up row positions for an exact value.
    pub fn get(&self, value: &ScalarValue) -> Vec<RowPosition> {
        self.backend.get().map(|b| b.get(value)).unwrap_or_default()
    }

    /// Get the number of entries (not unique values).
    pub fn len(&self) -> usize {
        self.backend.get().map(|b| b.len()).unwrap_or(0)
    }

    /// Check if the index is empty.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get the column name.
    pub fn column_name(&self) -> &str {
        &self.column_name
    }

    /// Get a snapshot of all entries grouped by value in sorted order.
    pub fn snapshot(&self) -> Vec<(OrderableScalarValue, Vec<RowPosition>)> {
        self.backend.get().map(|b| b.snapshot()).unwrap_or_default()
    }

    /// Get the data type of the indexed column.
    ///
    /// Returns None if the index is empty.
    pub fn data_type(&self) -> Option<arrow_schema::DataType> {
        self.backend.get().and_then(|b| b.data_type())
    }

    /// Export the index data as sorted RecordBatches for BTree index training.
    pub fn to_training_batches(&self, batch_size: usize) -> Result<Vec<RecordBatch>> {
        use arrow_schema::{DataType, Field, Schema};
        use lance_core::ROW_ID;
        use lance_index::scalar::registry::VALUE_COLUMN_NAME;
        use std::sync::Arc;

        let snapshot = self.snapshot();
        if snapshot.is_empty() {
            return Ok(vec![]);
        }

        let data_type = snapshot[0].0.0.data_type();
        let schema = Arc::new(Schema::new(vec![
            Field::new(VALUE_COLUMN_NAME, data_type, true),
            Field::new(ROW_ID, DataType::UInt64, false),
        ]));

        let mut batches = Vec::new();
        let mut values: Vec<ScalarValue> = Vec::with_capacity(batch_size);
        let mut row_ids: Vec<u64> = Vec::with_capacity(batch_size);

        // Expand each (value, [positions]) group into one row per position, in
        // sorted (value, position) order.
        for (value, positions) in &snapshot {
            for position in positions {
                values.push(value.0.clone());
                row_ids.push(*position);
                if values.len() >= batch_size {
                    batches.push(build_training_batch(&schema, &values, &row_ids)?);
                    values.clear();
                    row_ids.clear();
                }
            }
        }
        if !values.is_empty() {
            batches.push(build_training_batch(&schema, &values, &row_ids)?);
        }

        Ok(batches)
    }
}

/// Build a single training batch from values and row IDs.
fn build_training_batch(
    schema: &std::sync::Arc<arrow_schema::Schema>,
    values: &[ScalarValue],
    row_ids: &[u64],
) -> Result<RecordBatch> {
    use arrow_array::UInt64Array;
    use std::sync::Arc;

    let value_array = ScalarValue::iter_to_array(values.iter().cloned())?;
    let row_id_array = Arc::new(UInt64Array::from(row_ids.to_vec()));

    RecordBatch::try_new(schema.clone(), vec![value_array, row_id_array])
        .map_err(|e| Error::io(format!("Failed to create training batch: {}", e)))
}

/// Configuration for a BTree scalar index.
#[derive(Debug, Clone)]
pub struct BTreeIndexConfig {
    /// Index name.
    pub name: String,
    /// Field ID the index is built on.
    pub field_id: i32,
    /// Column name (for Arrow batch lookups).
    pub column: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, Int64Array, StringArray, UInt32Array};
    use arrow_schema::{DataType, Field, Schema as ArrowSchema};
    use std::sync::Arc;

    fn create_test_schema() -> Arc<ArrowSchema> {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, true),
        ]))
    }

    fn create_test_batch(schema: &ArrowSchema, start_id: i32) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(schema.clone()),
            vec![
                Arc::new(Int32Array::from(vec![start_id, start_id + 1, start_id + 2])),
                Arc::new(StringArray::from(vec!["alice", "bob", "charlie"])),
            ],
        )
        .unwrap()
    }

    #[test]
    fn test_btree_index_insert_and_lookup() {
        let schema = create_test_schema();
        let index = BTreeMemIndex::new(0, "id".to_string());

        let batch = create_test_batch(&schema, 0);
        index.insert(&batch, 0).unwrap();

        assert_eq!(index.len(), 3);
        assert_eq!(index.get(&ScalarValue::Int32(Some(0))), vec![0]);
        assert_eq!(index.get(&ScalarValue::Int32(Some(1))), vec![1]);
    }

    #[test]
    fn test_btree_get_newest_visible_seek_and_stop() {
        let schema = create_test_schema();
        let index = BTreeMemIndex::new(0, "id".to_string());
        index.insert(&create_test_batch(&schema, 0), 0).unwrap();
        index.insert(&create_test_batch(&schema, 0), 3).unwrap();

        assert_eq!(
            index.get_newest_visible(&ScalarValue::Int32(Some(0)), 5),
            Some(3)
        );
        assert_eq!(
            index.get_newest_visible(&ScalarValue::Int32(Some(1)), 5),
            Some(4)
        );
        // Visibility watermark below the newest update.
        assert_eq!(
            index.get_newest_visible(&ScalarValue::Int32(Some(0)), 2),
            Some(0)
        );
        // Watermark below every version.
        assert_eq!(
            index.get_newest_visible(&ScalarValue::Int32(Some(1)), 0),
            None
        );
        // Absent key.
        assert_eq!(
            index.get_newest_visible(&ScalarValue::Int32(Some(999)), 5),
            None
        );
        let mut all = index.get(&ScalarValue::Int32(Some(0)));
        all.sort_unstable();
        assert_eq!(all, vec![0, 3]);
    }

    #[test]
    fn test_fixed_int_signed_ordering_negatives() {
        // Negative + positive i64 keys must sort correctly via the encoding.
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "k",
            DataType::Int64,
            true,
        )]));
        let index = BTreeMemIndex::new(0, "k".to_string());
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                Some(5),
                Some(-3),
                Some(i64::MIN),
                Some(i64::MAX),
                Some(0),
            ]))],
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();

        // snapshot is value-sorted; decode round-trips.
        let snap = index.snapshot();
        let values: Vec<i64> = snap
            .iter()
            .map(|(v, _)| match v.0 {
                ScalarValue::Int64(Some(x)) => x,
                _ => panic!("unexpected"),
            })
            .collect();
        assert_eq!(values, vec![i64::MIN, -3, 0, 5, i64::MAX]);
        assert_eq!(index.get(&ScalarValue::Int64(Some(-3))), vec![1]);
        assert_eq!(
            index.get_newest_visible(&ScalarValue::Int64(Some(i64::MIN)), 10),
            Some(2)
        );
    }

    #[test]
    fn test_fixed_int_unsigned() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "k",
            DataType::UInt32,
            false,
        )]));
        let index = BTreeMemIndex::new(0, "k".to_string());
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(UInt32Array::from(vec![10u32, 4_000_000_000, 1]))],
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();
        let snap = index.snapshot();
        let values: Vec<u32> = snap
            .iter()
            .map(|(v, _)| match v.0 {
                ScalarValue::UInt32(Some(x)) => x,
                _ => panic!(),
            })
            .collect();
        assert_eq!(values, vec![1, 10, 4_000_000_000]);
        assert_eq!(
            index.get(&ScalarValue::UInt32(Some(4_000_000_000))),
            vec![1]
        );
    }

    #[test]
    fn test_fixed_int_nulls() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "k",
            DataType::Int32,
            true,
        )]));
        let index = BTreeMemIndex::new(0, "k".to_string());
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int32Array::from(vec![
                Some(7),
                None,
                Some(3),
                None,
            ]))],
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();

        assert_eq!(index.len(), 4);
        // Nulls sort first.
        let snap = index.snapshot();
        assert_eq!(snap[0].0.0, ScalarValue::Int32(None));
        assert_eq!(snap[0].1, vec![1, 3]);
        assert_eq!(snap[1].0.0, ScalarValue::Int32(Some(3)));
        assert_eq!(snap[2].0.0, ScalarValue::Int32(Some(7)));
        // Null lookup returns null positions.
        let mut nulls = index.get(&ScalarValue::Int32(None));
        nulls.sort_unstable();
        assert_eq!(nulls, vec![1, 3]);
        assert_eq!(
            index.get_newest_visible(&ScalarValue::Int32(None), 10),
            Some(3)
        );
    }

    #[test]
    fn test_btree_index_multiple_batches() {
        let schema = create_test_schema();
        let index = BTreeMemIndex::new(0, "id".to_string());
        index.insert(&create_test_batch(&schema, 0), 0).unwrap();
        index.insert(&create_test_batch(&schema, 10), 3).unwrap();

        assert_eq!(index.len(), 6);
        assert_eq!(index.get(&ScalarValue::Int32(Some(10))), vec![3]);
    }

    #[test]
    fn test_btree_index_to_training_batches() {
        use lance_core::ROW_ID;
        use lance_index::scalar::registry::VALUE_COLUMN_NAME;

        let schema = create_test_schema();
        let index = BTreeMemIndex::new(0, "id".to_string());
        index.insert(&create_test_batch(&schema, 0), 0).unwrap();
        index.insert(&create_test_batch(&schema, 10), 3).unwrap();

        let batches = index.to_training_batches(100).unwrap();
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 6);
        assert_eq!(batch.schema().field(0).name(), VALUE_COLUMN_NAME);
        assert_eq!(batch.schema().field(1).name(), ROW_ID);

        let values = batch
            .column_by_name(VALUE_COLUMN_NAME)
            .unwrap()
            .as_any()
            .downcast_ref::<Int32Array>()
            .unwrap();
        assert_eq!(
            (0..6).map(|i| values.value(i)).collect::<Vec<_>>(),
            vec![0, 1, 2, 10, 11, 12]
        );
        let row_ids = batch
            .column_by_name(ROW_ID)
            .unwrap()
            .as_any()
            .downcast_ref::<arrow_array::UInt64Array>()
            .unwrap();
        assert_eq!(
            (0..6).map(|i| row_ids.value(i)).collect::<Vec<_>>(),
            vec![0, 1, 2, 3, 4, 5]
        );
    }

    #[test]
    fn test_btree_index_snapshot() {
        let schema = create_test_schema();
        let index = BTreeMemIndex::new(0, "id".to_string());
        index.insert(&create_test_batch(&schema, 0), 0).unwrap();

        let snapshot = index.snapshot();
        assert_eq!(snapshot.len(), 3);
        assert_eq!(snapshot[0].0.0, ScalarValue::Int32(Some(0)));
        assert_eq!(snapshot[1].0.0, ScalarValue::Int32(Some(1)));
        assert_eq!(snapshot[2].0.0, ScalarValue::Int32(Some(2)));
    }

    #[test]
    fn test_bytes_backend_strings() {
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "s",
            DataType::Utf8,
            true,
        )]));
        let index = BTreeMemIndex::new(0, "s".to_string());
        // Mix short (inline) and a long (> INLINE_CAP, heap) key; include a null.
        let long = "z".repeat(64);
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![
                Some("delta"),
                Some("alpha"),
                None,
                Some(long.as_str()),
                Some("alpha"), // duplicate value, newer position
            ]))],
        )
        .unwrap();
        index.insert(&batch, 0).unwrap();

        // Newest visible position for a duplicated value.
        assert_eq!(
            index.get_newest_visible(&ScalarValue::Utf8(Some("alpha".to_string())), 10),
            Some(4)
        );
        // Visibility watermark hides the newer duplicate.
        assert_eq!(
            index.get_newest_visible(&ScalarValue::Utf8(Some("alpha".to_string())), 3),
            Some(1)
        );
        // Long (heap) key round-trips.
        assert_eq!(
            index.get_newest_visible(&ScalarValue::Utf8(Some(long.clone())), 10),
            Some(3)
        );
        // snapshot: null first, then lexicographic order; decode round-trips.
        let snap = index.snapshot();
        assert_eq!(snap[0].0.0, ScalarValue::Utf8(None));
        assert_eq!(snap[0].1, vec![2]);
        assert_eq!(snap[1].0.0, ScalarValue::Utf8(Some("alpha".to_string())));
        assert_eq!(snap[1].1, vec![1, 4]);
        assert_eq!(snap[2].0.0, ScalarValue::Utf8(Some("delta".to_string())));
        assert_eq!(snap[3].0.0, ScalarValue::Utf8(Some(long)));
    }

    #[test]
    fn test_bytes_backend_fixed_size_binary_uuid() {
        // UUIDs are FixedSizeBinary(16); verify the compact byte backend.
        let schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "id",
            DataType::FixedSizeBinary(16),
            false,
        )]));
        let index = BTreeMemIndex::new(0, "id".to_string());
        let a = [0x11u8; 16];
        let b = [0x22u8; 16];
        let c = [0xAAu8; 16];
        let values = vec![c.to_vec(), a.to_vec(), b.to_vec()];
        let arr = arrow_array::FixedSizeBinaryArray::try_from_iter(values.into_iter()).unwrap();
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).unwrap();
        index.insert(&batch, 0).unwrap();

        // Point lookup by UUID bytes.
        assert_eq!(
            index.get_newest_visible(&ScalarValue::FixedSizeBinary(16, Some(a.to_vec())), 10),
            Some(1)
        );
        // snapshot is byte-sorted: a (0x11) < b (0x22) < c (0xAA).
        let snap = index.snapshot();
        let order: Vec<Vec<u8>> = snap
            .iter()
            .map(|(v, _)| match &v.0 {
                ScalarValue::FixedSizeBinary(16, Some(bytes)) => bytes.clone(),
                _ => panic!("unexpected"),
            })
            .collect();
        assert_eq!(order, vec![a.to_vec(), b.to_vec(), c.to_vec()]);
    }
}
