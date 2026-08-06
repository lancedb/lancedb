// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Canonical fixed-size cache key construction.
//!
//! Cache keys are BLAKE3 digests truncated to 128 bits. Logical key fields are
//! encoded with explicit type tags, fixed-width little-endian integers, and
//! length framing for variable-width values. This makes the pre-hash encoding
//! unambiguous and stable across processes, platforms, and builds.
//!
//! The digest is a cache identity, not an authentication or access-control
//! primitive: namespace derivation keys are deterministic and not secret.
//! Truncating to 128 bits gives generic birthday resistance of approximately
//! 64 bits. This protocol does not introduce a FIPS mode; BLAKE3 is the
//! repository's selected cache-key algorithm.

use std::fmt;

/// Storage namespace identifier for canonical cache keys.
///
/// Persistent backends should include this identifier in their physical
/// namespace so future algorithm or framing changes produce cold misses.
pub const CACHE_KEY_FORMAT: &str = "blake3-128-v1";

const KEY_FORMAT_VERSION: u32 = 1;
const NAMESPACE_CONTEXT: &str = "lance-format/lance 2026-07-17 cache namespace v1";
const NAMESPACE_DOMAIN: &[u8] = b"lance-cache-namespace\0";
const ENTRY_DOMAIN: &[u8] = b"lance-cache-entry\0";

/// One-byte type discriminants in the stable key encoding.
#[derive(Clone, Copy)]
#[repr(u8)]
enum FieldTag {
    U8 = 1,
    U16 = 2,
    U32 = 3,
    U64 = 4,
    I32 = 5,
    I64 = 6,
    Bool = 7,
    Str = 8,
    Bytes = 9,
    FixedBytes = 10,
    None = 11,
    Some = 12,
    Variant = 13,
    Sequence = 14,
}

impl FieldTag {
    const fn as_u8(self) -> u8 {
        self as u8
    }
}

/// Versioned schema identity for fields emitted by a cache key.
///
/// Change the version whenever the encoded fields or their meaning changes.
/// The identifier must be stable and globally unique to the logical layout.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheKeySchema {
    id: &'static str,
    version: u32,
}

impl CacheKeySchema {
    /// Compatibility schema used by the default string-key bridge.
    pub const LEGACY_TEXT: Self = Self::new("lance.cache.legacy-text", 1);

    /// Create a stable schema identifier and encoding version.
    pub const fn new(id: &'static str, version: u32) -> Self {
        Self { id, version }
    }

    /// Return the author-assigned schema identifier.
    pub const fn id(self) -> &'static str {
        self.id
    }

    /// Return the schema encoding version.
    pub const fn version(self) -> u32 {
        self.version
    }
}

/// Opaque 128-bit key passed to cache backends.
///
/// The byte representation is canonical. It can be persisted directly and is
/// independent of the host's native integer endianness.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct InternalCacheKey([u8; 16]);

impl InternalCacheKey {
    /// Reconstruct a key from its canonical bytes.
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Borrow the canonical byte representation.
    pub const fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }

    /// Consume the key and return its canonical bytes.
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }
}

impl fmt::Debug for InternalCacheKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("InternalCacheKey(")?;
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        f.write_str(")")
    }
}

/// Pre-derived namespace key shared by entries in one logical cache scope.
#[derive(Clone, Copy, Debug)]
pub struct CacheNamespace([u8; 32]);

impl CacheNamespace {
    /// Construct the stable root namespace.
    pub fn root() -> Self {
        Self(blake3::derive_key(NAMESPACE_CONTEXT, b""))
    }

    /// Derive a child namespace from one framed hierarchy segment.
    pub fn child(self, segment: &str) -> Self {
        let mut hasher = blake3::Hasher::new_keyed(&self.0);
        write_framed(&mut hasher, NAMESPACE_DOMAIN);
        hasher.update(&KEY_FORMAT_VERSION.to_le_bytes());
        write_framed(&mut hasher, segment.as_bytes());
        Self(hasher.finalize().into())
    }
}

/// Streams typed logical fields into a canonical cache key.
///
/// Integer methods use little-endian fixed-width encoding. Variable-width
/// strings and bytes are type-tagged and length-prefixed. There is deliberately
/// no `usize` method because cache identities must not depend on target width.
///
/// # Examples
///
/// ```
/// use lance_core::cache::{CacheKeySchema, CacheNamespace, KeyBuilder};
///
/// let namespace = CacheNamespace::root().child("dataset");
/// let mut builder = KeyBuilder::new(
///     namespace,
///     "example.Page",
///     CacheKeySchema::new("example.page-key", 1),
/// );
/// builder.write_u32(7);
/// builder.write_str("values");
/// let key = builder.finish();
/// assert_eq!(key.as_bytes().len(), 16);
/// ```
pub struct KeyBuilder {
    hasher: blake3::Hasher,
}

impl KeyBuilder {
    /// Start a key in a namespace with a stable value type and key schema.
    pub fn new(
        namespace: CacheNamespace,
        stable_type_id: &'static str,
        schema: CacheKeySchema,
    ) -> Self {
        let mut hasher = blake3::Hasher::new_keyed(&namespace.0);
        write_framed(&mut hasher, ENTRY_DOMAIN);
        hasher.update(&KEY_FORMAT_VERSION.to_le_bytes());
        write_framed(&mut hasher, stable_type_id.as_bytes());
        write_framed(&mut hasher, schema.id().as_bytes());
        hasher.update(&schema.version().to_le_bytes());
        Self { hasher }
    }

    /// Append a tagged, fixed-width `u8`.
    #[inline]
    pub fn write_u8(&mut self, value: u8) {
        self.hasher.update(&[FieldTag::U8.as_u8(), value]);
    }

    /// Append a tagged, little-endian `u16`.
    #[inline]
    pub fn write_u16(&mut self, value: u16) {
        let mut encoded = [0; 3];
        encoded[0] = FieldTag::U16.as_u8();
        encoded[1..].copy_from_slice(&value.to_le_bytes());
        self.hasher.update(&encoded);
    }

    /// Append a tagged, little-endian `u32`.
    #[inline]
    pub fn write_u32(&mut self, value: u32) {
        let mut encoded = [0; 5];
        encoded[0] = FieldTag::U32.as_u8();
        encoded[1..].copy_from_slice(&value.to_le_bytes());
        self.hasher.update(&encoded);
    }

    /// Append a tagged, little-endian `u64`.
    #[inline]
    pub fn write_u64(&mut self, value: u64) {
        let mut encoded = [0; 9];
        encoded[0] = FieldTag::U64.as_u8();
        encoded[1..].copy_from_slice(&value.to_le_bytes());
        self.hasher.update(&encoded);
    }

    /// Append a tagged, little-endian `i32`.
    #[inline]
    pub fn write_i32(&mut self, value: i32) {
        let mut encoded = [0; 5];
        encoded[0] = FieldTag::I32.as_u8();
        encoded[1..].copy_from_slice(&value.to_le_bytes());
        self.hasher.update(&encoded);
    }

    /// Append a tagged, little-endian `i64`.
    #[inline]
    pub fn write_i64(&mut self, value: i64) {
        let mut encoded = [0; 9];
        encoded[0] = FieldTag::I64.as_u8();
        encoded[1..].copy_from_slice(&value.to_le_bytes());
        self.hasher.update(&encoded);
    }

    /// Append a tagged boolean.
    #[inline]
    pub fn write_bool(&mut self, value: bool) {
        self.hasher
            .update(&[FieldTag::Bool.as_u8(), u8::from(value)]);
    }

    /// Append a tagged, length-prefixed UTF-8 string.
    #[inline]
    pub fn write_str(&mut self, value: &str) {
        self.write_variable(FieldTag::Str, value.as_bytes());
    }

    /// Append tagged, length-prefixed bytes.
    #[inline]
    pub fn write_bytes(&mut self, value: &[u8]) {
        self.write_variable(FieldTag::Bytes, value);
    }

    /// Append a tagged fixed-size byte array, including its length.
    #[inline]
    pub fn write_fixed_bytes<const N: usize>(&mut self, value: &[u8; N]) {
        self.write_variable(FieldTag::FixedBytes, value);
    }

    /// Append the canonical marker for an absent optional value.
    #[inline]
    pub fn write_none(&mut self) {
        self.hasher.update(&[FieldTag::None.as_u8()]);
    }

    /// Append the canonical marker for a present optional value.
    #[inline]
    pub fn write_some(&mut self) {
        self.hasher.update(&[FieldTag::Some.as_u8()]);
    }

    /// Append a tagged enum variant ordinal.
    #[inline]
    pub fn write_variant(&mut self, variant: u32) {
        let mut encoded = [0; 5];
        encoded[0] = FieldTag::Variant.as_u8();
        encoded[1..].copy_from_slice(&variant.to_le_bytes());
        self.hasher.update(&encoded);
    }

    /// Append the length of a following sequence.
    #[inline]
    pub fn write_sequence_len(&mut self, len: u64) {
        let mut encoded = [0; 9];
        encoded[0] = FieldTag::Sequence.as_u8();
        encoded[1..].copy_from_slice(&len.to_le_bytes());
        self.hasher.update(&encoded);
    }

    /// Finalize and return the canonical 128-bit key.
    #[inline]
    pub fn finish(self) -> InternalCacheKey {
        let hash = self.hasher.finalize();
        let mut bytes = [0; 16];
        bytes.copy_from_slice(&hash.as_bytes()[..16]);
        InternalCacheKey(bytes)
    }

    #[inline]
    fn write_variable(&mut self, tag: FieldTag, value: &[u8]) {
        self.hasher.update(&[tag.as_u8()]);
        self.hasher.update(&encoded_len(value));
        self.hasher.update(value);
    }
}

#[inline]
fn write_framed(hasher: &mut blake3::Hasher, value: &[u8]) {
    hasher.update(&encoded_len(value));
    hasher.update(value);
}

#[inline]
fn encoded_len(value: &[u8]) -> [u8; 8] {
    (value.len() as u64).to_le_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SCHEMA: CacheKeySchema = CacheKeySchema::new("test.key", 1);

    fn builder() -> KeyBuilder {
        KeyBuilder::new(
            CacheNamespace::root().child("s3://bucket/dataset"),
            "test.Value",
            SCHEMA,
        )
    }

    fn key_with(write: impl FnOnce(&mut KeyBuilder)) -> InternalCacheKey {
        let mut key = builder();
        write(&mut key);
        key.finish()
    }

    #[test]
    fn key_and_namespace_have_fixed_sizes() {
        assert_eq!(std::mem::size_of::<InternalCacheKey>(), 16);
        assert_eq!(std::mem::size_of::<CacheNamespace>(), 32);
        assert_eq!(std::mem::size_of::<FieldTag>(), 1);
    }

    #[test]
    fn blake3_matches_official_empty_keyed_hash_vector() {
        let key = *b"whats the Elvish word for friend";
        assert_eq!(
            blake3::keyed_hash(&key, b"").as_bytes(),
            &[
                0x92, 0xb2, 0xb7, 0x56, 0x04, 0xed, 0x3c, 0x76, 0x1f, 0x9d, 0x6f, 0x62, 0x39, 0x2c,
                0x8a, 0x92, 0x27, 0xad, 0x0e, 0xa3, 0xf0, 0x95, 0x73, 0xe7, 0x83, 0xf1, 0x49, 0x8a,
                0x4e, 0xd6, 0x0d, 0x26,
            ]
        );
    }

    #[test]
    fn typed_fields_and_boundaries_are_unambiguous() {
        let cases = [
            key_with(|key| {
                key.write_str("ab");
                key.write_str("c");
            }),
            key_with(|key| {
                key.write_str("a");
                key.write_str("bc");
            }),
            key_with(|key| key.write_str("")),
            key_with(|key| key.write_bytes(b"")),
            key_with(|key| key.write_fixed_bytes(b"")),
            key_with(|key| key.write_u8(1)),
            key_with(|key| key.write_u16(1)),
            key_with(|key| key.write_u32(1)),
            key_with(|key| key.write_u64(1)),
            key_with(|key| key.write_i32(1)),
            key_with(|key| key.write_i64(1)),
            key_with(|key| key.write_bool(false)),
            key_with(|key| key.write_bool(true)),
            key_with(KeyBuilder::write_none),
            key_with(KeyBuilder::write_some),
            key_with(|key| key.write_variant(0)),
            key_with(|key| key.write_variant(1)),
        ];
        assert_eq!(std::collections::BTreeSet::from(cases).len(), cases.len());

        assert_ne!(
            key_with(|key| {
                key.write_sequence_len(2);
                key.write_u32(1);
                key.write_u32(2);
            }),
            key_with(|key| {
                key.write_u32(1);
                key.write_u32(2);
            })
        );
    }

    #[test]
    fn namespace_type_schema_and_version_are_domain_separated() {
        let root = CacheNamespace::root();
        let namespace = root.child("dataset").child("index");
        let nested = KeyBuilder::new(namespace, "test.Value", SCHEMA).finish();
        let combined = KeyBuilder::new(root.child("dataset/index"), "test.Value", SCHEMA).finish();
        assert_ne!(nested, combined);

        assert_ne!(
            nested,
            KeyBuilder::new(namespace, "test.OtherValue", SCHEMA).finish()
        );
        assert_ne!(
            nested,
            KeyBuilder::new(
                namespace,
                "test.Value",
                CacheKeySchema::new("test.other-key", 1),
            )
            .finish()
        );
        assert_ne!(
            nested,
            KeyBuilder::new(namespace, "test.Value", CacheKeySchema::new("test.key", 2),).finish()
        );

        let tenant_a_memory =
            KeyBuilder::new(root.child("tenant-a").child("memory"), "test.Value", SCHEMA).finish();
        assert_ne!(
            tenant_a_memory,
            KeyBuilder::new(root.child("tenant-b").child("memory"), "test.Value", SCHEMA,).finish()
        );
        assert_ne!(
            tenant_a_memory,
            KeyBuilder::new(
                root.child("tenant-a").child("persistent"),
                "test.Value",
                SCHEMA,
            )
            .finish()
        );
    }

    #[test]
    fn integers_use_fixed_width_little_endian_encoding() {
        let namespace = CacheNamespace::root().child("endianness");
        let mut key = KeyBuilder::new(namespace, "test.Value", SCHEMA);
        key.write_u32(0x0102_0304);
        let actual = key.finish();

        let mut reference = blake3::Hasher::new_keyed(&namespace.0);
        write_framed(&mut reference, ENTRY_DOMAIN);
        reference.update(&KEY_FORMAT_VERSION.to_le_bytes());
        write_framed(&mut reference, b"test.Value");
        write_framed(&mut reference, SCHEMA.id().as_bytes());
        reference.update(&SCHEMA.version().to_le_bytes());
        reference.update(&[FieldTag::U32.as_u8(), 0x04, 0x03, 0x02, 0x01]);
        let mut expected = [0; 16];
        expected.copy_from_slice(&reference.finalize().as_bytes()[..16]);

        assert_eq!(actual, InternalCacheKey::from_bytes(expected));
    }

    #[test]
    fn key_has_stable_golden_vector() {
        let mut key = builder();
        key.write_u32(7);
        key.write_str("page");
        key.write_some();
        key.write_fixed_bytes(&[0xAB; 16]);
        assert_eq!(
            key.finish().into_bytes(),
            [
                0xc4, 0x38, 0xff, 0x22, 0x30, 0x55, 0x30, 0xfc, 0x74, 0x16, 0x38, 0xe9, 0x7d, 0x45,
                0xa5, 0x68,
            ]
        );
    }
}
