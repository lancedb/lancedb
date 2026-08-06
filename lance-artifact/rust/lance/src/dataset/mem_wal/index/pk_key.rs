// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Composite primary-key encoding for MemWAL dedup.
//!
//! A multi-column primary key is reduced to a single order-preserving byte
//! string ([`encode_pk_tuple`]) so the whole tuple is one comparable key:
//! lexicographic byte order equals tuple order, and distinct tuples never
//! collide. Encoded as a `Binary` value, the tuple is indexed directly by a
//! [`super::BTreeMemIndex`] (its byte backend) — both in memory and, after
//! flush, as the on-disk BTree's `Binary` value column — so a probe builds
//! `ScalarValue::Binary(key)` and every layer agrees.
//!
//! Single-column primary keys do **not** use this — they key the typed
//! `BTreeMemIndex` on the column value directly.

use arrow_array::{BinaryArray, RecordBatch};
use datafusion::common::ScalarValue;
use lance_core::{Error, Result};

/// Sign-flip a signed integer to an order-preserving unsigned key (matches the
/// fixed-int BTree backend). Big-endian bytes of the result sort like the value.
#[inline]
fn encode_signed(v: i64) -> u64 {
    (v as u64) ^ (1u64 << 63)
}

/// Append an order-preserving encoding of one non-null byte string: each `0x00`
/// is escaped to `0x00 0xFF`, then a `0x00 0x00` terminator is appended. The
/// terminator sorts before any escaped content, so a prefix orders before its
/// extensions and no value can forge a column boundary.
fn encode_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    for &b in bytes {
        out.push(b);
        if b == 0x00 {
            out.push(0xFF);
        }
    }
    out.extend_from_slice(&[0x00, 0x00]);
}

/// Append the order-preserving encoding of a single PK column value. A leading
/// tag (`0x00` null / `0x01` non-null) makes nulls sort first and keeps the
/// per-column encoding self-delimiting (fixed-width for ints, terminated for
/// bytes), so concatenating columns stays injective and order-preserving.
fn encode_value(out: &mut Vec<u8>, value: &ScalarValue) -> Result<()> {
    if value.is_null() {
        out.push(0x00);
        return Ok(());
    }
    out.push(0x01);
    macro_rules! be_signed {
        ($v:expr) => {
            out.extend_from_slice(&encode_signed($v as i64).to_be_bytes())
        };
    }
    match value {
        ScalarValue::Int8(Some(v)) => be_signed!(*v),
        ScalarValue::Int16(Some(v)) => be_signed!(*v),
        ScalarValue::Int32(Some(v)) => be_signed!(*v),
        ScalarValue::Int64(Some(v)) => be_signed!(*v),
        ScalarValue::Date32(Some(v)) => be_signed!(*v),
        ScalarValue::Date64(Some(v)) => be_signed!(*v),
        ScalarValue::UInt8(Some(v)) => out.extend_from_slice(&(*v as u64).to_be_bytes()),
        ScalarValue::UInt16(Some(v)) => out.extend_from_slice(&(*v as u64).to_be_bytes()),
        ScalarValue::UInt32(Some(v)) => out.extend_from_slice(&(*v as u64).to_be_bytes()),
        ScalarValue::UInt64(Some(v)) => out.extend_from_slice(&v.to_be_bytes()),
        ScalarValue::Boolean(Some(b)) => out.push(*b as u8),
        ScalarValue::Utf8(Some(s)) | ScalarValue::LargeUtf8(Some(s)) => {
            encode_bytes(out, s.as_bytes())
        }
        ScalarValue::Binary(Some(b))
        | ScalarValue::LargeBinary(Some(b))
        | ScalarValue::FixedSizeBinary(_, Some(b)) => encode_bytes(out, b),
        other => {
            return Err(Error::invalid_input(format!(
                "Unsupported primary-key column type for composite key: {other:?}"
            )));
        }
    }
    Ok(())
}

/// Encode a PK tuple (values in PK column order) to one order-preserving key.
pub fn encode_pk_tuple(values: &[ScalarValue]) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(values.len() * 9);
    for value in values {
        encode_value(&mut out, value)?;
    }
    Ok(out)
}

/// Encode row `row` of `batch`'s PK columns (at `pk_indices`) to one key.
fn encode_pk_row(batch: &RecordBatch, pk_indices: &[usize], row: usize) -> Result<Vec<u8>> {
    let mut out = Vec::with_capacity(pk_indices.len() * 9);
    for &col in pk_indices {
        let value = ScalarValue::try_from_array(batch.column(col), row)?;
        encode_value(&mut out, &value)?;
    }
    Ok(out)
}

/// Encode every row of `batch`'s PK columns (at `pk_indices`) into a `Binary`
/// column of order-preserving composite keys — the form a [`super::BTreeMemIndex`]
/// indexes directly (its byte backend), so the composite PK reuses the same
/// index as a single-column one.
pub fn encode_pk_batch(batch: &RecordBatch, pk_indices: &[usize]) -> Result<BinaryArray> {
    let mut keys: Vec<Vec<u8>> = Vec::with_capacity(batch.num_rows());
    for row in 0..batch.num_rows() {
        keys.push(encode_pk_row(batch, pk_indices, row)?);
    }
    Ok(BinaryArray::from_iter_values(keys.iter()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use std::sync::Arc;

    fn tuple(a: i32, b: &str) -> Vec<ScalarValue> {
        vec![ScalarValue::Int32(Some(a)), ScalarValue::from(b)]
    }

    #[test]
    fn encoding_is_order_preserving_and_injective() {
        // Sorting tuples by their encoding must match tuple order, and distinct
        // tuples must produce distinct bytes.
        let tuples = [
            tuple(1, "a"),
            tuple(1, "ab"),
            tuple(1, "b"),
            tuple(2, "a"),
            tuple(-1, "z"),
        ];
        let mut encoded: Vec<(Vec<u8>, &Vec<ScalarValue>)> = tuples
            .iter()
            .map(|t| (encode_pk_tuple(t).unwrap(), t))
            .collect();
        encoded.sort_by(|x, y| x.0.cmp(&y.0));
        let order: Vec<_> = encoded.iter().map(|(_, t)| (*t).clone()).collect();
        // -1 < 1 < 2; within id=1, "a" < "ab" < "b".
        assert_eq!(
            order,
            vec![
                tuple(-1, "z"),
                tuple(1, "a"),
                tuple(1, "ab"),
                tuple(1, "b"),
                tuple(2, "a"),
            ]
        );
        // Injective: 5 distinct tuples → 5 distinct keys.
        let mut keys: Vec<Vec<u8>> = tuples.iter().map(|t| encode_pk_tuple(t).unwrap()).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), 5);
    }

    #[test]
    fn null_sorts_first_and_is_distinct() {
        let null_a = vec![ScalarValue::Int32(None), ScalarValue::from("a")];
        let one_a = tuple(1, "a");
        assert!(encode_pk_tuple(&null_a).unwrap() < encode_pk_tuple(&one_a).unwrap());
        assert_ne!(
            encode_pk_tuple(&null_a).unwrap(),
            encode_pk_tuple(&one_a).unwrap()
        );
    }

    #[test]
    fn prefix_safety_with_embedded_zero() {
        // A string containing 0x00 must not collide with or sort incorrectly
        // against a shorter one (escaping + terminator).
        let with_zero = vec![ScalarValue::Binary(Some(vec![0x00]))];
        let empty = vec![ScalarValue::Binary(Some(vec![]))];
        assert!(encode_pk_tuple(&empty).unwrap() < encode_pk_tuple(&with_zero).unwrap());
    }

    #[test]
    fn encode_pk_batch_matches_per_tuple_encoding() {
        // Each row of the encoded `Binary` column equals `encode_pk_tuple` of
        // that row's PK values — so the column a BTreeMemIndex indexes is exactly
        // what a probe builds.
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 1])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ],
        )
        .unwrap();
        let encoded = encode_pk_batch(&batch, &[0, 1]).unwrap();
        assert_eq!(encoded.value(0), encode_pk_tuple(&tuple(2, "a")).unwrap());
        assert_eq!(encoded.value(1), encode_pk_tuple(&tuple(1, "b")).unwrap());
        // (1,"b") encodes below (2,"a").
        assert!(encoded.value(1) < encoded.value(0));
    }
}
