// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Caches for Lance indices. They are organized in a hierarchical manner to
//! avoid collisions.
//!
//!  GlobalIndexCache
//!     │
//!     ├─► DSIndexCache (prefixed by dataset URI)
//!     │    │
//!     └────┴──► Index-specific cache (prefixed by index UUID and FRI UUID)

use std::{borrow::Cow, ops::Deref, sync::Arc};

use lance_core::cache::{CacheKey, CacheKeySchema, KeyBuilder, LanceCache};
use lance_core::deepsize::{Context, DeepSizeOf};
use lance_index::frag_reuse::FragReuseIndex;
use lance_table::format::IndexMetadata;
use uuid::Uuid;

/// A type-safe wrapper around a LanceCache that enforces namespaces for index data.
pub struct GlobalIndexCache(pub(super) LanceCache);

impl GlobalIndexCache {
    pub fn for_dataset(&self, uri: &str) -> DSIndexCache {
        // Create a sub-cache for the dataset by adding the URI as a key prefix.
        // This prevents collisions between different datasets.
        DSIndexCache(self.0.with_key_prefix(uri))
    }
}

impl Clone for GlobalIndexCache {
    fn clone(&self) -> Self {
        Self(self.0.clone())
    }
}

impl Deref for GlobalIndexCache {
    type Target = LanceCache;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DeepSizeOf for GlobalIndexCache {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.0.deep_size_of_children(context)
    }
}

/// A type-safe wrapper around a LanceCache that enforces namespaces and keys
/// for dataset-specific index data.
pub struct DSIndexCache(pub(crate) LanceCache);

impl Deref for DSIndexCache {
    type Target = LanceCache;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DSIndexCache {
    /// Create an index-specific cache with the given UUID prefix.
    pub fn for_index(&self, uuid: &Uuid, fri_uuid: Option<&Uuid>) -> LanceCache {
        let mut uuid_buffer = Uuid::encode_buffer();
        let cache = self
            .0
            .with_key_prefix(uuid.as_hyphenated().encode_lower(&mut uuid_buffer));
        if let Some(fri_uuid) = fri_uuid {
            // If a FRI UUID is provided, use it to create a more specific cache key.
            let mut fri_uuid_buffer = Uuid::encode_buffer();
            cache.with_key_prefix(fri_uuid.as_hyphenated().encode_lower(&mut fri_uuid_buffer))
        } else {
            // Otherwise, just use the index UUID as the key prefix.
            cache
        }
    }
}

pub(crate) fn write_index_identity(builder: &mut KeyBuilder, uuid: &Uuid, fri_uuid: Option<&Uuid>) {
    builder.write_fixed_bytes(uuid.as_bytes());
    if let Some(fri_uuid) = fri_uuid {
        builder.write_some();
        builder.write_fixed_bytes(fri_uuid.as_bytes());
    } else {
        builder.write_none();
    }
}

// Cache key types for type-safe cache access

#[derive(Debug)]
pub struct FragReuseIndexKey<'a> {
    pub uuid: &'a Uuid,
}

impl CacheKey for FragReuseIndexKey<'_> {
    type ValueType = FragReuseIndex;

    fn key(&self) -> Cow<'_, str> {
        Cow::Owned(format!("frag_reuse/{}", self.uuid))
    }

    fn type_name() -> &'static str {
        "FragReuseIndex"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.index.fragment-reuse-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_fixed_bytes(self.uuid.as_bytes());
    }
}

#[derive(Clone, Copy, Debug)]
pub struct IndexMetadataKey<'a> {
    pub version: u64,
    pub store_identity: &'a str,
}

impl CacheKey for IndexMetadataKey<'_> {
    type ValueType = Vec<IndexMetadata>;

    fn key(&self) -> Cow<'_, str> {
        Cow::Owned(format!(
            "{}:{}/{}",
            self.store_identity.len(),
            self.store_identity,
            self.version
        ))
    }

    fn type_name() -> &'static str {
        "Vec<IndexMetadata>"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.index.metadata-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_str(self.store_identity);
        builder.write_u64(self.version);
    }

    fn codec() -> Option<lance_core::cache::CacheCodec> {
        Some(lance_table::format::index_metadata_codec())
    }
}

pub struct ProstAny(pub Arc<prost_types::Any>);

impl DeepSizeOf for ProstAny {
    fn deep_size_of_children(&self, context: &mut Context) -> usize {
        self.0.type_url.deep_size_of_children(context) + self.0.value.deep_size_of_children(context)
    }
}

/// Cache key for scalar index details
///
/// Typically we don't use the cache for scalar index details because they are stored
/// in the manifest and readily available.  However, old versions of Lance didn't store
/// details in the manifest, and we have to perform an expensive inference process to determine
/// what they are.  These we cache.
#[derive(Debug)]
pub struct ScalarIndexDetailsKey<'a> {
    pub uuid: &'a Uuid,
}

impl CacheKey for ScalarIndexDetailsKey<'_> {
    type ValueType = ProstAny;

    fn key(&self) -> Cow<'_, str> {
        Cow::Owned(format!("type/{}", self.uuid))
    }

    fn type_name() -> &'static str {
        "ScalarIndexDetails"
    }

    fn schema() -> CacheKeySchema {
        CacheKeySchema::new("lance.index.scalar-details-key", 1)
    }

    fn write_key(&self, builder: &mut KeyBuilder) {
        builder.write_fixed_bytes(self.uuid.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn index_metadata_key_isolates_object_store_identity() {
        let first = IndexMetadataKey {
            version: 7,
            store_identity: "s3$first-options",
        };
        let second = IndexMetadataKey {
            version: 7,
            store_identity: "s3$second-options",
        };

        assert_ne!(first.key(), second.key());
    }
}
