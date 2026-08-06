// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use arrow_array::*;
use half::f16;

use crate::ArrowScalar;

macro_rules! impl_from_primitive {
    ($native_ty:ty, $array_ty:ty) => {
        impl From<$native_ty> for ArrowScalar {
            fn from(value: $native_ty) -> Self {
                let array: ArrayRef = Arc::new(<$array_ty>::from(vec![value]));
                Self::try_from_array(array).expect("single-element primitive array is always valid")
            }
        }
    };
}

impl_from_primitive!(i8, Int8Array);
impl_from_primitive!(i16, Int16Array);
impl_from_primitive!(i32, Int32Array);
impl_from_primitive!(i64, Int64Array);
impl_from_primitive!(u8, UInt8Array);
impl_from_primitive!(u16, UInt16Array);
impl_from_primitive!(u32, UInt32Array);
impl_from_primitive!(u64, UInt64Array);
impl_from_primitive!(f32, Float32Array);
impl_from_primitive!(f64, Float64Array);

impl From<bool> for ArrowScalar {
    fn from(value: bool) -> Self {
        let array: ArrayRef = Arc::new(BooleanArray::from(vec![value]));
        Self::try_from_array(array).expect("single-element boolean array is always valid")
    }
}

impl From<f16> for ArrowScalar {
    fn from(value: f16) -> Self {
        let array: ArrayRef = Arc::new(Float16Array::from(vec![value]));
        Self::try_from_array(array).expect("single-element f16 array is always valid")
    }
}

impl From<&str> for ArrowScalar {
    fn from(value: &str) -> Self {
        let array: ArrayRef = Arc::new(StringArray::from(vec![value]));
        Self::try_from_array(array).expect("single-element string array is always valid")
    }
}

impl From<String> for ArrowScalar {
    fn from(value: String) -> Self {
        Self::from(value.as_str())
    }
}

impl From<&[u8]> for ArrowScalar {
    fn from(value: &[u8]) -> Self {
        let array: ArrayRef = Arc::new(BinaryArray::from_vec(vec![value]));
        Self::try_from_array(array).expect("single-element binary array is always valid")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_from_primitives() {
        let s = ArrowScalar::from(42i32);
        assert!(!s.is_null());
        assert_eq!(format!("{s}"), "42");

        let s = ArrowScalar::from(1.5f64);
        assert!(!s.is_null());

        let s = ArrowScalar::from(true);
        assert_eq!(format!("{s}"), "true");
    }

    #[test]
    fn test_from_string_types() {
        let s = ArrowScalar::from("hello");
        assert_eq!(format!("{s}"), "hello");

        let s = ArrowScalar::from(String::from("world"));
        assert_eq!(format!("{s}"), "world");
    }

    #[test]
    fn test_from_binary() {
        let bytes: &[u8] = &[0xDE, 0xAD];
        let s = ArrowScalar::from(bytes);
        assert!(!s.is_null());
    }

    #[test]
    fn test_from_f16() {
        let s = ArrowScalar::from(f16::from_f32(1.5));
        assert!(!s.is_null());
    }
}
