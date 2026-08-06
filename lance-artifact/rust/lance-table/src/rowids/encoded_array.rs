// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::ops::Range;

use lance_core::deepsize::DeepSizeOf;

/// Encoded array of u64 values.
///
/// This is a internal data type used as part of row id indices.
#[derive(Debug, Clone, PartialEq, Eq, DeepSizeOf)]
pub enum EncodedU64Array {
    /// u64 values represented as u16 offset from a base value.
    ///
    /// Useful when the min and max value are within u16 range (0..65535).
    /// Only space saving when there are more than 2 values.
    U16 { base: u64, offsets: Vec<u16> },
    /// u64 values represented as u32 offset from a base value.
    ///
    /// Useful when the min and max value are within u32 range (0..~4 billion).
    U32 { base: u64, offsets: Vec<u32> },
    /// Just a plain vector of u64 values.
    ///
    /// For when the values cover a wide range.
    U64(Vec<u64>),
}

impl EncodedU64Array {
    pub fn len(&self) -> usize {
        match self {
            Self::U16 { offsets, .. } => offsets.len(),
            Self::U32 { offsets, .. } => offsets.len(),
            Self::U64(values) => values.len(),
        }
    }

    pub fn iter(&self) -> Box<dyn DoubleEndedIterator<Item = u64> + '_> {
        match self {
            Self::U16 { base, offsets } => {
                Box::new(offsets.iter().cloned().map(move |o| base + o as u64))
            }
            Self::U32 { base, offsets } => {
                Box::new(offsets.iter().cloned().map(move |o| base + o as u64))
            }
            Self::U64(values) => Box::new(values.iter().cloned()),
        }
    }

    pub fn get(&self, i: usize) -> Option<u64> {
        match self {
            Self::U16 { base, offsets } => {
                if i < offsets.len() {
                    Some(*base + offsets[i] as u64)
                } else {
                    None
                }
            }
            Self::U32 { base, offsets } => {
                if i < offsets.len() {
                    Some(*base + offsets[i] as u64)
                } else {
                    None
                }
            }
            Self::U64(values) => values.get(i).copied(),
        }
    }

    pub fn min(&self) -> Option<u64> {
        match self {
            Self::U16 { base, offsets } => {
                if offsets.is_empty() {
                    None
                } else {
                    Some(*base)
                }
            }
            Self::U32 { base, offsets } => {
                if offsets.is_empty() {
                    None
                } else {
                    Some(*base)
                }
            }
            Self::U64(values) => values.iter().copied().min(),
        }
    }

    pub fn max(&self) -> Option<u64> {
        match self {
            Self::U16 { base, offsets } => {
                if offsets.is_empty() {
                    None
                } else {
                    Some(*base + offsets.iter().copied().max().unwrap() as u64)
                }
            }
            Self::U32 { base, offsets } => {
                if offsets.is_empty() {
                    None
                } else {
                    Some(*base + offsets.iter().copied().max().unwrap() as u64)
                }
            }
            Self::U64(values) => values.iter().copied().max(),
        }
    }

    pub fn first(&self) -> Option<u64> {
        match self {
            Self::U16 { base, offsets } => {
                if offsets.is_empty() {
                    None
                } else {
                    Some(*base + *offsets.first().unwrap() as u64)
                }
            }
            Self::U32 { base, offsets } => {
                if offsets.is_empty() {
                    None
                } else {
                    Some(*base + *offsets.first().unwrap() as u64)
                }
            }
            Self::U64(values) => values.first().copied(),
        }
    }

    pub fn last(&self) -> Option<u64> {
        match self {
            Self::U16 { base, offsets } => {
                if offsets.is_empty() {
                    None
                } else {
                    Some(*base + *offsets.last().unwrap() as u64)
                }
            }
            Self::U32 { base, offsets } => {
                if offsets.is_empty() {
                    None
                } else {
                    Some(*base + *offsets.last().unwrap() as u64)
                }
            }
            Self::U64(values) => values.last().copied(),
        }
    }

    pub fn binary_search(&self, val: u64) -> std::result::Result<usize, usize> {
        match self {
            Self::U16 { base, offsets } => match val.checked_sub(*base) {
                None => Err(0),
                Some(val) => {
                    if val > u16::MAX as u64 {
                        return Err(offsets.len());
                    }
                    let u16 = val as u16;
                    offsets.binary_search(&u16)
                }
            },
            Self::U32 { base, offsets } => match val.checked_sub(*base) {
                None => Err(0),
                Some(val) => {
                    if val > u32::MAX as u64 {
                        return Err(offsets.len());
                    }
                    let u32 = val as u32;
                    offsets.binary_search(&u32)
                }
            },
            Self::U64(values) => values.binary_search(&val),
        }
    }

    pub fn slice(&self, offset: usize, len: usize) -> Self {
        match self {
            Self::U16 { base, offsets } => offsets[offset..(offset + len)]
                .iter()
                .map(|o| *base + *o as u64)
                .collect(),
            Self::U32 { base, offsets } => offsets[offset..(offset + len)]
                .iter()
                .map(|o| *base + *o as u64)
                .collect(),
            Self::U64(values) => {
                let values = values[offset..(offset + len)].to_vec();
                Self::U64(values)
            }
        }
    }
}

impl From<Vec<u64>> for EncodedU64Array {
    fn from(values: Vec<u64>) -> Self {
        let min = values.iter().copied().min().unwrap_or(0);
        let max = values.iter().copied().max().unwrap_or(0);
        let range = max - min;
        if values.is_empty() {
            Self::U64(Vec::new())
        } else if range <= u16::MAX as u64 {
            let base = min;
            let offsets = values.iter().map(|v| (*v - base) as u16).collect();
            Self::U16 { base, offsets }
        } else if range <= u32::MAX as u64 {
            let base = min;
            let offsets = values.iter().map(|v| (*v - base) as u32).collect();
            Self::U32 { base, offsets }
        } else {
            Self::U64(values)
        }
    }
}

impl From<Range<u64>> for EncodedU64Array {
    fn from(range: Range<u64>) -> Self {
        let min = range.start;
        let max = range.end;
        let range = max - min;
        if range < u16::MAX as u64 {
            let base = min;
            let offsets = (0..range as u16).collect();
            Self::U16 { base, offsets }
        } else if range < u32::MAX as u64 {
            let base = min;
            let offsets = (0..range as u32).collect();
            Self::U32 { base, offsets }
        } else {
            Self::U64((min..max).collect())
        }
    }
}

impl FromIterator<u64> for EncodedU64Array {
    fn from_iter<I: IntoIterator<Item = u64>>(iter: I) -> Self {
        let values: Vec<u64> = iter.into_iter().collect();
        Self::from(values)
    }
}

impl IntoIterator for EncodedU64Array {
    type Item = u64;
    type IntoIter = Box<dyn DoubleEndedIterator<Item = u64>>;
    fn into_iter(self) -> Self::IntoIter {
        match self {
            Self::U16 { base, offsets } => {
                Box::new(offsets.into_iter().map(move |o| base + o as u64))
            }
            Self::U32 { base, offsets } => {
                Box::new(offsets.into_iter().map(move |o| base + o as u64))
            }
            Self::U64(values) => Box::new(values.into_iter()),
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_encoded_array_from_vec() {
        fn roundtrip_array(values: Vec<u64>, expected: &EncodedU64Array) {
            let encoded = EncodedU64Array::from(values.clone());
            assert_eq!(&encoded, expected);

            assert_eq!(values.len(), encoded.len());
            assert_eq!(values.first(), encoded.first().as_ref());
            assert_eq!(values.last(), encoded.last().as_ref());
            assert_eq!(values.iter().min(), encoded.min().as_ref());
            assert_eq!(values.iter().max(), encoded.max().as_ref());

            let roundtripped = encoded.iter().collect::<Vec<_>>();
            assert_eq!(values, roundtripped);

            for (i, v) in values.iter().enumerate() {
                assert_eq!(Some(*v), encoded.get(i));
            }

            let encoded2 = values.into_iter().collect::<EncodedU64Array>();
            assert_eq!(&encoded2, expected);
        }

        // Empty
        roundtrip_array(vec![], &EncodedU64Array::U64(vec![]));

        // Single value
        roundtrip_array(
            vec![42],
            &EncodedU64Array::U16 {
                base: 42,
                offsets: vec![0],
            },
        );

        // u16 version, it can start beyond the u16 range, but the
        // relative values must be within u16 range.
        let relative_values = [42, 0, 43, u16::MAX as u64, 99];
        let values = relative_values.map(|v| v + 2 * u16::MAX as u64).to_vec();
        let expected = EncodedU64Array::U16 {
            base: 2 * u16::MAX as u64,
            offsets: relative_values.iter().map(|v| *v as u16).collect(),
        };
        roundtrip_array(values, &expected);

        // u32 version
        let relative_values = [42, 0, 43, u32::MAX as u64, 99];
        let values = relative_values.map(|v| v + 2 * u32::MAX as u64).to_vec();
        let expected = EncodedU64Array::U32 {
            base: 2 * u32::MAX as u64,
            offsets: relative_values.iter().map(|v| *v as u32).collect(),
        };
        roundtrip_array(values, &expected);

        // u64 version
        let values = [42, 0, 43, u64::MAX, 99].to_vec();
        let expected = EncodedU64Array::U64(values.clone());
        roundtrip_array(values, &expected);
    }

    #[test]
    fn test_double_ended_iter() {
        let arrays = vec![
            EncodedU64Array::U16 {
                base: 42,
                offsets: vec![0, 1, 2, 3, 4],
            },
            EncodedU64Array::U32 {
                base: 42,
                offsets: vec![0, 1, 2, 3, 4],
            },
            EncodedU64Array::U64(vec![42, 43, 44, 45, 46]),
        ];
        for array in arrays {
            // Should be able to iterate forwards and backwards, and get the same thing.
            let forwards = array.iter().collect::<Vec<_>>();
            let mut backwards = array.iter().rev().collect::<Vec<_>>();
            backwards.reverse();
            assert_eq!(forwards, backwards);

            // Should be able to pull from both sides in lockstep.
            let mut expected = Vec::with_capacity(array.len());
            let mut actual = Vec::with_capacity(array.len());
            let mut iter = array.iter();
            // Alternating forwards and backwards
            for i in 0..array.len() {
                if i % 2 == 0 {
                    actual.push(iter.next().unwrap());
                    expected.push(array.get(i / 2).unwrap());
                } else {
                    let i = array.len() - 1 - i / 2;
                    actual.push(iter.next_back().unwrap());
                    expected.push(array.get(i).unwrap());
                };
            }
            assert_eq!(expected, actual);
        }
    }

    #[test]
    fn test_encoded_array_from_range() {
        // u16 version
        let range = (2 * u16::MAX as u64)..(40 + 2 * u16::MAX as u64);
        let encoded = EncodedU64Array::from(range.clone());
        let expected_base = 2 * u16::MAX as u64;
        assert!(
            matches!(
                encoded,
                EncodedU64Array::U16 {
                    base,
                    ..
                } if base == expected_base
            ),
            "{:?}",
            encoded
        );
        let roundtripped = encoded.into_iter().collect::<Vec<_>>();
        assert_eq!(range.collect::<Vec<_>>(), roundtripped);

        // u32 version
        let range = (2 * u32::MAX as u64)..(u16::MAX as u64 + 10 + 2 * u32::MAX as u64);
        let encoded = EncodedU64Array::from(range.clone());
        let expected_base = 2 * u32::MAX as u64;
        assert!(matches!(
            encoded,
            EncodedU64Array::U32 {
                base,
                ..
            } if base == expected_base
        ));
        let roundtripped = encoded.into_iter().collect::<Vec<_>>();
        assert_eq!(range.collect::<Vec<_>>(), roundtripped);

        // We'll skip u64 since it would take a lot of memory.

        // Empty one
        let range = 42..42;
        let encoded = EncodedU64Array::from(range);
        assert_eq!(encoded.len(), 0);
    }
}
