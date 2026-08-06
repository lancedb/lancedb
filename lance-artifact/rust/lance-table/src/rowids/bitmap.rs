// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use lance_core::deepsize::DeepSizeOf;

#[derive(PartialEq, Eq, Clone, DeepSizeOf)]
pub struct Bitmap {
    pub data: Vec<u8>,
    pub len: usize,
}

impl std::fmt::Debug for Bitmap {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Bitmap {{ data: ")?;
        for i in 0..self.len {
            write!(f, "{}", if self.get(i) { "1" } else { "0" })?;
        }
        write!(f, ", len: {} }}", self.len)
    }
}

impl Bitmap {
    pub fn new_empty(len: usize) -> Self {
        let data = vec![0; len.div_ceil(8)];
        Self { data, len }
    }

    pub fn new_full(len: usize) -> Self {
        let mut data = vec![0xff; len.div_ceil(8)];
        // Zero past the end of len
        let remainder = len % 8;
        if remainder != 0 {
            let last_byte = data.last_mut().unwrap();
            let bits_to_clear = 8 - remainder;
            for offset_from_end in 0..bits_to_clear {
                let i = 7 - offset_from_end;
                *last_byte &= !(1 << i);
            }
        }
        Self { data, len }
    }

    pub fn set(&mut self, i: usize) {
        self.data[i / 8] |= 1 << (i % 8);
    }

    pub fn clear(&mut self, i: usize) {
        self.data[i / 8] &= !(1 << (i % 8));
    }

    pub fn get(&self, i: usize) -> bool {
        self.data[i / 8] & (1 << (i % 8)) != 0
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn slice(&self, start: usize, len: usize) -> BitmapSlice<'_> {
        BitmapSlice {
            bitmap: self,
            start,
            len,
        }
    }

    pub fn count_ones(&self) -> usize {
        self.data.iter().map(|&x| x.count_ones() as usize).sum()
    }

    pub fn count_zeros(&self) -> usize {
        self.len - self.count_ones()
    }

    pub fn iter(&self) -> impl Iterator<Item = bool> + '_ {
        self.data
            .iter()
            .flat_map(|&x| (0..8).map(move |i| x & (1 << i) != 0))
            .take(self.len)
    }
}

impl From<&[bool]> for Bitmap {
    fn from(slice: &[bool]) -> Self {
        let mut bitmap = Self::new_empty(slice.len());
        for (i, &b) in slice.iter().enumerate() {
            if b {
                bitmap.set(i);
            }
        }
        bitmap
    }
}

// Make a slice of bitmap
pub struct BitmapSlice<'a> {
    bitmap: &'a Bitmap,
    start: usize,
    len: usize,
}

impl BitmapSlice<'_> {
    pub fn count_ones(&self) -> usize {
        if self.len == 0 {
            return 0;
        }
        let first_byte = self.start / 8;
        let last_byte = (self.start + self.len - 1) / 8;
        if first_byte == last_byte {
            let byte = self.bitmap.data[first_byte];
            let mut count = 0;
            for i in self.start % 8..((self.start + self.len - 1) % 8 + 1) {
                if byte & (1 << i) != 0 {
                    count += 1;
                }
            }
            count
        } else {
            let mut count = 0;
            // Handle first byte
            for i in self.start % 8..8 {
                if self.bitmap.data[first_byte] & (1 << i) != 0 {
                    count += 1;
                }
            }

            // Handle last bytes
            for i in 0..((self.start + self.len - 1) % 8 + 1) {
                if self.bitmap.data[last_byte] & (1 << i) != 0 {
                    count += 1;
                }
            }

            // Middle bytes can just use count_ones
            count += self.bitmap.data[first_byte + 1..last_byte]
                .iter()
                .map(|&x| x.count_ones() as usize)
                .sum::<usize>();
            count
        }
    }

    pub fn count_zeros(&self) -> usize {
        self.len - self.count_ones()
    }
}

impl From<BitmapSlice<'_>> for Bitmap {
    fn from(slice: BitmapSlice) -> Self {
        let mut bitmap = Self::new_empty(slice.len);
        for i in 0..slice.len {
            if slice.bitmap.get(slice.start + i) {
                bitmap.set(i);
            }
        }
        bitmap
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prop_assert_eq;

    #[test]
    fn test_bitmap() {
        let mut bitmap = Bitmap::new_empty(10);
        assert_eq!(bitmap.len(), 10);
        assert_eq!(bitmap.count_ones(), 0);

        bitmap.set(0);
        bitmap.set(1);
        bitmap.set(4);
        bitmap.set(5);
        bitmap.set(9);
        assert_eq!(bitmap.count_ones(), 5);
        assert_eq!(
            format!("{:?}", bitmap),
            "Bitmap { data: 1100110001, len: 10 }"
        );

        bitmap.clear(1);
        bitmap.clear(4);
        assert_eq!(bitmap.count_ones(), 3);
        assert_eq!(
            format!("{:?}", bitmap),
            "Bitmap { data: 1000010001, len: 10 }"
        );

        let bitmap_slice = bitmap.slice(5, 5);
        assert_eq!(bitmap_slice.count_ones(), 2);
    }

    #[test]
    fn test_equality() {
        for len in 48..56 {
            let mut bitmap1 = Bitmap::new_empty(len);
            for i in 0..len {
                if i % 2 == 0 {
                    bitmap1.set(i);
                }
            }

            let mut bitmap2 = Bitmap::new_full(len);
            for i in 0..len {
                if i % 2 == 1 {
                    bitmap2.clear(i);
                }
            }

            assert_eq!(bitmap1, bitmap2);
        }
    }

    proptest::proptest! {
        #[test]
        fn test_bitmap_slice(
            values in proptest::collection::vec(proptest::bool::ANY, 0..100),
            mut start in 0..100usize,
            mut len in 0..100usize,
        ) {
            if start > values.len() {
                start = values.len();
            }
            if len > values.len() - start {
                len = values.len() - start;
            }

            let bitmap = Bitmap::from(values.as_slice());
            let slice = bitmap.slice(start, len);
            let values_slice = values[start..(start + len)].to_vec();

            prop_assert_eq!(slice.count_ones(), values_slice.iter().filter(|&&x| x).count());
        }
    }

    #[test]
    fn test_bitmap_iter_empty() {
        let bitmap = Bitmap::new_empty(10);
        let values: Vec<bool> = bitmap.iter().collect();
        assert_eq!(values, vec![false; 10]);
    }

    #[test]
    fn test_bitmap_iter_full() {
        let bitmap = Bitmap::new_full(10);
        let values: Vec<bool> = bitmap.iter().collect();
        assert_eq!(values, vec![true; 10]);
    }

    #[test]
    fn test_bitmap_iter_partial() {
        let mut bitmap = Bitmap::new_empty(10);
        bitmap.set(0);
        bitmap.set(3);
        bitmap.set(7);
        bitmap.set(9);

        let values: Vec<bool> = bitmap.iter().collect();
        let expected = vec![
            true,  // 0
            false, // 1
            false, // 2
            true,  // 3
            false, // 4
            false, // 5
            false, // 6
            true,  // 7
            false, // 8
            true,  // 9
        ];
        assert_eq!(values, expected);
    }

    #[test]
    fn test_bitmap_iter_edge_cases() {
        // Test with length that's not a multiple of 8
        let mut bitmap = Bitmap::new_empty(15);
        bitmap.set(0);
        bitmap.set(7);
        bitmap.set(14);

        let values: Vec<bool> = bitmap.iter().collect();
        let expected = vec![
            true,  // 0
            false, // 1
            false, // 2
            false, // 3
            false, // 4
            false, // 5
            false, // 6
            true,  // 7
            false, // 8
            false, // 9
            false, // 10
            false, // 11
            false, // 12
            false, // 13
            true,  // 14
        ];
        assert_eq!(values, expected);
    }

    proptest::proptest! {
        #[test]
        fn test_bitmap_iter_property(
            values in proptest::collection::vec(proptest::bool::ANY, 0..100)
        ) {
            let bitmap = Bitmap::from(values.as_slice());
            let iter_values: Vec<bool> = bitmap.iter().collect();
            assert_eq!(iter_values, values);
        }
    }
}
