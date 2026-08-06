// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::hash::Hasher;

/// A wrapper for `&[u8]` to allow byte slices as hash keys.
///
/// ```
/// use lance_core::utils::hash::U8SliceKey;
/// use std::collections::HashMap;
///
/// let mut map: HashMap<U8SliceKey, i32> = HashMap::new();
/// map.insert(U8SliceKey(&[1, 2, 3]), 42);
///
/// assert_eq!(map.get(&U8SliceKey(&[1, 2, 3])), Some(&42));
/// assert_eq!(map.get(&U8SliceKey(&[1, 2, 4])), None);
///
/// // Equality is based on slice contents
/// assert_eq!(U8SliceKey(&[1, 2, 3]), U8SliceKey(&[1, 2, 3]));
/// assert_ne!(U8SliceKey(&[1, 2, 3]), U8SliceKey(&[1, 2, 4]));
/// ```
#[derive(Debug, Eq)]
pub struct U8SliceKey<'a>(pub &'a [u8]);

impl PartialEq for U8SliceKey<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl std::hash::Hash for U8SliceKey<'_> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_u8_slice_key() {
        // Test cases not in doctest: key not found, inequality
        let mut map = HashMap::new();
        map.insert(U8SliceKey(&[1, 2, 3]), 42);
        assert_eq!(map.get(&U8SliceKey(&[4, 5, 6])), None);
        assert_ne!(U8SliceKey(&[1]), U8SliceKey(&[2]));
    }
}
