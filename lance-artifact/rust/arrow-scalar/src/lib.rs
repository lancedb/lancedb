// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! A scalar type backed by a single-element Arrow array with [`Ord`], [`Hash`],
//! and [`Eq`] support.
//!
//! Comparisons and hashing are delegated to [`arrow_row::OwnedRow`], which
//! provides a correct total ordering for all Arrow types (including proper NaN
//! handling for floats and null ordering).

mod convert;
pub mod serde;

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{Float16Type, Float32Type, Float64Type};
use arrow_array::{ArrayRef, make_array, new_null_array};
use arrow_cast::display::ArrayFormatter;
use arrow_data::transform::MutableArrayData;
use arrow_row::{OwnedRow, RowConverter, SortField};
use arrow_schema::{ArrowError, DataType};

type Result<T> = std::result::Result<T, ArrowError>;

/// A scalar value backed by a length-1 Arrow array.
///
/// `ArrowScalar` provides [`Eq`], [`Ord`], and [`Hash`] by caching an
/// [`OwnedRow`] at construction time. This means comparisons and hashing are
/// O(1) row-byte operations rather than per-type dispatch.
///
/// # Cross-type comparison
///
/// Comparing scalars of different data types produces an arbitrary but
/// consistent ordering based on the underlying row bytes. This is intentional
/// — it allows scalars to be used as keys in sorted collections regardless of
/// type, but the ordering across types is not semantically meaningful.
///
/// # Examples
///
/// ```
/// use lance_arrow_scalar::ArrowScalar;
///
/// let a = ArrowScalar::from(1i32);
/// let b = ArrowScalar::from(2i32);
/// assert!(a < b);
///
/// let c = ArrowScalar::from("hello");
/// assert_eq!(c, ArrowScalar::from("hello"));
/// ```
pub struct ArrowScalar {
    array: ArrayRef,
    row: OwnedRow,
}

impl ArrowScalar {
    /// Create a scalar by extracting the element at `offset` from `array`.
    pub fn try_new(array: &ArrayRef, offset: usize) -> Result<Self> {
        if offset >= array.len() {
            return Err(ArrowError::InvalidArgumentError(
                "Scalar index out of bounds".to_string(),
            ));
        }

        let data = array.to_data();
        let mut mutable = MutableArrayData::new(vec![&data], true, 1);
        mutable.extend(0, offset, offset + 1);
        let single = make_array(mutable.freeze());
        Self::try_from_array(single)
    }

    /// Create a scalar from a length-1 array.
    pub fn try_from_array(array: ArrayRef) -> Result<Self> {
        if array.len() != 1 {
            return Err(ArrowError::InvalidArgumentError(format!(
                "ArrowScalar requires a length-1 array, got length {}",
                array.len()
            )));
        }

        let row = Self::compute_row(&array)?;
        Ok(Self { array, row })
    }

    /// Create a null scalar of the given data type.
    pub fn new_null(data_type: &DataType) -> Result<Self> {
        Self::try_from_array(new_null_array(data_type, 1))
    }

    fn compute_row(array: &ArrayRef) -> Result<OwnedRow> {
        let sort_field = SortField::new(array.data_type().clone());
        let converter = RowConverter::new(vec![sort_field])?;
        let rows = converter.convert_columns(&[Arc::clone(array)])?;
        Ok(rows.row(0).owned())
    }

    /// Returns a reference to the underlying length-1 array.
    pub fn as_array(&self) -> &ArrayRef {
        &self.array
    }

    /// Returns the data type of this scalar.
    pub fn data_type(&self) -> &DataType {
        self.array.data_type()
    }

    /// Returns `true` if this scalar is null.
    pub fn is_null(&self) -> bool {
        self.array.null_count() == 1
    }

    /// Returns `true` if this scalar is a non-null floating-point NaN.
    ///
    /// ```
    /// use lance_arrow_scalar::ArrowScalar;
    ///
    /// assert!(ArrowScalar::from(f32::NAN).is_nan());
    /// assert!(!ArrowScalar::from(1.0f32).is_nan());
    /// assert!(!ArrowScalar::from(1i32).is_nan());
    /// ```
    pub fn is_nan(&self) -> bool {
        if self.is_null() {
            return false;
        }

        match self.data_type() {
            DataType::Float16 => self.array.as_primitive::<Float16Type>().value(0).is_nan(),
            DataType::Float32 => self.array.as_primitive::<Float32Type>().value(0).is_nan(),
            DataType::Float64 => self.array.as_primitive::<Float64Type>().value(0).is_nan(),
            _ => false,
        }
    }
}

impl PartialEq for ArrowScalar {
    fn eq(&self, other: &Self) -> bool {
        self.row == other.row
    }
}

impl Eq for ArrowScalar {}

impl PartialOrd for ArrowScalar {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ArrowScalar {
    fn cmp(&self, other: &Self) -> Ordering {
        self.row.cmp(&other.row)
    }
}

impl Hash for ArrowScalar {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.row.hash(state);
    }
}

impl fmt::Display for ArrowScalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_null() {
            return write!(f, "null");
        }
        let formatter =
            ArrayFormatter::try_new(&self.array, &Default::default()).map_err(|_| fmt::Error)?;
        write!(f, "{}", formatter.value(0))
    }
}

impl fmt::Debug for ArrowScalar {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ArrowScalar({}: {})", self.data_type(), self)
    }
}

impl Clone for ArrowScalar {
    fn clone(&self) -> Self {
        Self {
            array: Arc::clone(&self.array),
            row: self.row.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeSet, HashSet};
    use std::sync::Arc;

    use arrow_array::*;
    use rstest::rstest;

    use super::*;

    #[test]
    fn test_try_new_extracts_element() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![10, 20, 30]));
        let s = ArrowScalar::try_new(&array, 1).unwrap();
        assert_eq!(format!("{s}"), "20");
    }

    #[test]
    fn test_try_new_out_of_bounds() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1]));
        assert!(ArrowScalar::try_new(&array, 5).is_err());
    }

    #[test]
    fn test_try_from_array_wrong_length() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![1, 2]));
        assert!(ArrowScalar::try_from_array(array).is_err());
    }

    #[test]
    fn test_equality() {
        let a = ArrowScalar::from(42i32);
        let b = ArrowScalar::from(42i32);
        let c = ArrowScalar::from(99i32);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }

    #[test]
    fn test_ordering() {
        let a = ArrowScalar::from(1i32);
        let b = ArrowScalar::from(2i32);
        let c = ArrowScalar::from(3i32);
        assert!(a < b);
        assert!(b < c);
        assert_eq!(a.cmp(&a), Ordering::Equal);
    }

    #[test]
    fn test_hash_consistent_with_eq() {
        use std::hash::DefaultHasher;

        let a = ArrowScalar::from(42i32);
        let b = ArrowScalar::from(42i32);
        let hash_a = {
            let mut h = DefaultHasher::new();
            a.hash(&mut h);
            h.finish()
        };
        let hash_b = {
            let mut h = DefaultHasher::new();
            b.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash_a, hash_b);
    }

    #[test]
    fn test_in_hashset() {
        let mut set = HashSet::new();
        set.insert(ArrowScalar::from(1i32));
        set.insert(ArrowScalar::from(2i32));
        set.insert(ArrowScalar::from(1i32));
        assert_eq!(set.len(), 2);
    }

    #[test]
    fn test_in_btreeset() {
        let mut set = BTreeSet::new();
        set.insert(ArrowScalar::from(3i32));
        set.insert(ArrowScalar::from(1i32));
        set.insert(ArrowScalar::from(2i32));
        let values: Vec<_> = set.iter().map(|s| format!("{s}")).collect();
        assert_eq!(values, vec!["1", "2", "3"]);
    }

    #[test]
    fn test_null_scalar() {
        let array: ArrayRef = Arc::new(Int32Array::from(vec![None]));
        let s = ArrowScalar::try_from_array(array).unwrap();
        assert!(s.is_null());
        assert_eq!(format!("{s}"), "null");
    }

    #[test]
    fn test_null_sorts_first() {
        let null_scalar = {
            let array: ArrayRef = Arc::new(Int32Array::from(vec![None]));
            ArrowScalar::try_from_array(array).unwrap()
        };
        let value_scalar = ArrowScalar::from(0i32);
        assert!(null_scalar < value_scalar);
    }

    #[rstest]
    #[case::float_nan(
        ArrowScalar::from(f64::NAN),
        ArrowScalar::from(f64::INFINITY),
        Ordering::Greater
    )]
    #[case::float_normal(ArrowScalar::from(1.0f64), ArrowScalar::from(2.0f64), Ordering::Less)]
    fn test_float_ordering(
        #[case] a: ArrowScalar,
        #[case] b: ArrowScalar,
        #[case] expected: Ordering,
    ) {
        assert_eq!(a.cmp(&b), expected);
    }

    #[rstest]
    #[case::float16_nan(ArrowScalar::from(half::f16::NAN), true)]
    #[case::float32_nan(ArrowScalar::from(f32::NAN), true)]
    #[case::float64_nan(ArrowScalar::from(f64::NAN), true)]
    #[case::float64_finite(ArrowScalar::from(1.0f64), false)]
    #[case::int32(ArrowScalar::from(1i32), false)]
    fn test_is_nan(#[case] scalar: ArrowScalar, #[case] expected: bool) {
        assert_eq!(scalar.is_nan(), expected);
    }

    #[test]
    fn test_null_is_not_nan() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![None]));
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        assert!(!scalar.is_nan());
    }

    #[test]
    fn test_display_string() {
        let s = ArrowScalar::from("hello world");
        assert_eq!(format!("{s}"), "hello world");
    }

    #[test]
    fn test_debug() {
        let s = ArrowScalar::from(42i32);
        let debug = format!("{s:?}");
        assert!(debug.contains("ArrowScalar"));
        assert!(debug.contains("42"));
    }

    #[test]
    fn test_clone() {
        let a = ArrowScalar::from(42i32);
        let b = a.clone();
        assert_eq!(a, b);
    }

    #[test]
    fn test_data_type() {
        let s = ArrowScalar::from(42i32);
        assert_eq!(s.data_type(), &DataType::Int32);
    }

    #[test]
    fn test_boolean_roundtrip() {
        let t = ArrowScalar::from(true);
        let f = ArrowScalar::from(false);
        assert_eq!(t.data_type(), &DataType::Boolean);
        assert!(!t.is_null());
        assert_eq!(format!("{t}"), "true");
        assert_eq!(format!("{f}"), "false");

        // Extract from multi-element array
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
        let s = ArrowScalar::try_new(&array, 1).unwrap();
        assert_eq!(format!("{s}"), "false");
        assert_eq!(s.data_type(), &DataType::Boolean);
    }

    #[test]
    fn test_boolean_equality_and_ordering() {
        let t1 = ArrowScalar::from(true);
        let t2 = ArrowScalar::from(true);
        let f1 = ArrowScalar::from(false);
        assert_eq!(t1, t2);
        assert_ne!(t1, f1);
        // false < true in arrow row encoding
        assert!(f1 < t1);
    }

    #[test]
    fn test_boolean_null() {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![None]));
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        assert!(scalar.is_null());
        assert_eq!(scalar.data_type(), &DataType::Boolean);
        assert_eq!(format!("{scalar}"), "null");

        // null sorts before false
        let f = ArrowScalar::from(false);
        assert!(scalar < f);
    }

    #[test]
    fn test_string_view_roundtrip() {
        let array: ArrayRef = Arc::new(StringViewArray::from(vec![
            "hello world, this is a long string view",
        ]));
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        assert_eq!(scalar.data_type(), &DataType::Utf8View);
        assert!(!scalar.is_null());
        assert_eq!(
            format!("{scalar}"),
            "hello world, this is a long string view"
        );

        // Extract from multi-element array
        let array: ArrayRef = Arc::new(StringViewArray::from(vec!["alpha", "beta", "gamma"]));
        let s = ArrowScalar::try_new(&array, 1).unwrap();
        assert_eq!(format!("{s}"), "beta");
        assert_eq!(s.data_type(), &DataType::Utf8View);
    }

    #[test]
    fn test_binary_view_roundtrip() {
        let values: Vec<&[u8]> = vec![b"\xDE\xAD\xBE\xEF"];
        let array: ArrayRef = Arc::new(BinaryViewArray::from(values));
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        assert_eq!(scalar.data_type(), &DataType::BinaryView);
        assert!(!scalar.is_null());

        // Extract from multi-element array
        let values: Vec<&[u8]> = vec![b"aaa", b"bbb", b"ccc"];
        let array: ArrayRef = Arc::new(BinaryViewArray::from(values));
        let s = ArrowScalar::try_new(&array, 2).unwrap();
        assert_eq!(s.data_type(), &DataType::BinaryView);
    }

    #[test]
    fn test_string_view_equality_and_ordering() {
        let mk = |s: &str| {
            let array: ArrayRef = Arc::new(StringViewArray::from(vec![s]));
            ArrowScalar::try_from_array(array).unwrap()
        };
        let a = mk("apple");
        let b = mk("apple");
        let c = mk("banana");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a < c);
    }

    #[test]
    fn test_binary_view_equality_and_ordering() {
        let mk = |b: &[u8]| {
            let values: Vec<&[u8]> = vec![b];
            let array: ArrayRef = Arc::new(BinaryViewArray::from(values));
            ArrowScalar::try_from_array(array).unwrap()
        };
        let a = mk(b"\x01\x02");
        let b = mk(b"\x01\x02");
        let c = mk(b"\x01\x03");
        assert_eq!(a, b);
        assert_ne!(a, c);
        assert!(a < c);
    }

    #[test]
    fn test_string_view_in_collections() {
        let mk = |s: &str| {
            let array: ArrayRef = Arc::new(StringViewArray::from(vec![s]));
            ArrowScalar::try_from_array(array).unwrap()
        };

        let mut hset = HashSet::new();
        hset.insert(mk("foo"));
        hset.insert(mk("bar"));
        hset.insert(mk("foo"));
        assert_eq!(hset.len(), 2);

        let mut bset = BTreeSet::new();
        bset.insert(mk("cherry"));
        bset.insert(mk("apple"));
        bset.insert(mk("banana"));
        let sorted: Vec<_> = bset.iter().map(|s| format!("{s}")).collect();
        assert_eq!(sorted, vec!["apple", "banana", "cherry"]);
    }

    #[test]
    fn test_string_view_null() {
        let array: ArrayRef = Arc::new(StringViewArray::from(vec![Option::<&str>::None]));
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        assert!(scalar.is_null());
        assert_eq!(scalar.data_type(), &DataType::Utf8View);
        assert_eq!(format!("{scalar}"), "null");
    }

    #[test]
    fn test_binary_view_null() {
        let array: ArrayRef = Arc::new(BinaryViewArray::from(vec![Option::<&[u8]>::None]));
        let scalar = ArrowScalar::try_from_array(array).unwrap();
        assert!(scalar.is_null());
        assert_eq!(scalar.data_type(), &DataType::BinaryView);
    }

    #[test]
    fn test_cross_type_comparison_is_consistent() {
        let int_scalar = ArrowScalar::from(42i32);
        let str_scalar = ArrowScalar::from("hello");
        // The ordering is arbitrary but must be consistent
        let ord1 = int_scalar.cmp(&str_scalar);
        let ord2 = int_scalar.cmp(&str_scalar);
        assert_eq!(ord1, ord2);
        // And the reverse should be opposite
        assert_eq!(str_scalar.cmp(&int_scalar), ord1.reverse());
    }
}

#[cfg(test)]
mod prop_tests {
    use std::sync::Arc;

    use arrow_array::*;
    use arrow_ord::sort::sort;
    use arrow_schema::SortOptions;
    use proptest::prelude::*;

    use super::ArrowScalar;

    /// Generate an arbitrary Arrow array of a randomly chosen type, including
    /// nulls. Covers primitives, booleans, string/binary types and their view
    /// variants.
    fn arbitrary_array() -> BoxedStrategy<ArrayRef> {
        let len = 0..=100usize;

        prop_oneof![
            // --- integer types ---
            proptest::collection::vec(proptest::option::of(any::<i8>()), len.clone())
                .prop_map(|v| Arc::new(Int8Array::from(v)) as ArrayRef),
            proptest::collection::vec(proptest::option::of(any::<i16>()), len.clone())
                .prop_map(|v| Arc::new(Int16Array::from(v)) as ArrayRef),
            proptest::collection::vec(proptest::option::of(any::<i32>()), len.clone())
                .prop_map(|v| Arc::new(Int32Array::from(v)) as ArrayRef),
            proptest::collection::vec(proptest::option::of(any::<i64>()), len.clone())
                .prop_map(|v| Arc::new(Int64Array::from(v)) as ArrayRef),
            proptest::collection::vec(proptest::option::of(any::<u8>()), len.clone())
                .prop_map(|v| Arc::new(UInt8Array::from(v)) as ArrayRef),
            proptest::collection::vec(proptest::option::of(any::<u16>()), len.clone())
                .prop_map(|v| Arc::new(UInt16Array::from(v)) as ArrayRef),
            proptest::collection::vec(proptest::option::of(any::<u32>()), len.clone())
                .prop_map(|v| Arc::new(UInt32Array::from(v)) as ArrayRef),
            proptest::collection::vec(proptest::option::of(any::<u64>()), len.clone())
                .prop_map(|v| Arc::new(UInt64Array::from(v)) as ArrayRef),
            // --- float types ---
            proptest::collection::vec(proptest::option::of(any::<f32>()), len.clone())
                .prop_map(|v| Arc::new(Float32Array::from(v)) as ArrayRef),
            proptest::collection::vec(proptest::option::of(any::<f64>()), len.clone())
                .prop_map(|v| Arc::new(Float64Array::from(v)) as ArrayRef),
            // --- boolean ---
            proptest::collection::vec(proptest::option::of(any::<bool>()), len.clone())
                .prop_map(|v| Arc::new(BooleanArray::from(v)) as ArrayRef),
            // --- string types ---
            proptest::collection::vec(proptest::option::of(any::<String>()), len.clone()).prop_map(
                |v| {
                    let refs: Vec<Option<&str>> = v.iter().map(|o| o.as_deref()).collect();
                    Arc::new(StringArray::from(refs)) as ArrayRef
                }
            ),
            proptest::collection::vec(proptest::option::of(any::<String>()), len.clone()).prop_map(
                |v| {
                    let refs: Vec<Option<&str>> = v.iter().map(|o| o.as_deref()).collect();
                    Arc::new(LargeStringArray::from(refs)) as ArrayRef
                }
            ),
            proptest::collection::vec(proptest::option::of(any::<String>()), len.clone()).prop_map(
                |v| {
                    let refs: Vec<Option<&str>> = v.iter().map(|o| o.as_deref()).collect();
                    Arc::new(StringViewArray::from(refs)) as ArrayRef
                }
            ),
            // --- binary types ---
            proptest::collection::vec(
                proptest::option::of(proptest::collection::vec(any::<u8>(), 0..50)),
                len.clone(),
            )
            .prop_map(|v| {
                let refs: Vec<Option<&[u8]>> = v.iter().map(|o| o.as_deref()).collect();
                Arc::new(BinaryArray::from(refs)) as ArrayRef
            }),
            proptest::collection::vec(
                proptest::option::of(proptest::collection::vec(any::<u8>(), 0..50)),
                len.clone(),
            )
            .prop_map(|v| {
                let refs: Vec<Option<&[u8]>> = v.iter().map(|o| o.as_deref()).collect();
                Arc::new(LargeBinaryArray::from(refs)) as ArrayRef
            }),
            proptest::collection::vec(
                proptest::option::of(proptest::collection::vec(any::<u8>(), 0..50)),
                len,
            )
            .prop_map(|v| {
                let refs: Vec<Option<&[u8]>> = v.iter().map(|o| o.as_deref()).collect();
                Arc::new(BinaryViewArray::from(refs)) as ArrayRef
            }),
        ]
        .boxed()
    }

    proptest::proptest! {
        #[test]
        fn sorted_array_produces_sorted_scalars(array in arbitrary_array()) {
            let sorted = sort(
                &array,
                Some(SortOptions { descending: false, nulls_first: true }),
            )
            .unwrap();

            let scalars: Vec<ArrowScalar> = (0..sorted.len())
                .map(|i| ArrowScalar::try_new(&sorted, i).unwrap())
                .collect();

            for i in 1..scalars.len() {
                prop_assert!(
                    scalars[i - 1] <= scalars[i],
                    "scalar[{}] ({:?}) should be <= scalar[{}] ({:?})",
                    i - 1, scalars[i - 1], i, scalars[i],
                );
            }
        }
    }
}
