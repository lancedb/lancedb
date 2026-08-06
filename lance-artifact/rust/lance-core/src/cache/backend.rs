// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Backend interface for cache implementors.
//!
//! This module defines the trait that custom cache backends must implement,
//! along with the entry type they operate on. Most callers should
//! use [`LanceCache`](super::LanceCache) instead of interacting with
//! backends directly.
//!
//! # Migrating custom backends
//!
//! Cache keys are opaque 16-byte values. Store
//! [`InternalCacheKey::as_bytes`] directly instead of decomposing a logical
//! prefix, key string, and Rust type name. The physical namespace must also
//! include [`CACHE_KEY_FORMAT`](super::CACHE_KEY_FORMAT), so a future key
//! protocol produces cold misses instead of aliases. Persistent or tiered
//! backends can route serializable values with [`CacheCodec::type_id`].
//!
//! Prefix invalidation and key inventory are intentionally not part of this
//! interface: one-way digests cannot support either operation without
//! retaining the logical strings that fixed-size keys are designed to remove.
//! Existing callers should migrate removed symbols as follows:
//! - replace `with_backend_and_prefix(backend, prefix)` with
//!   [`LanceCache::with_backend`](super::LanceCache::with_backend) followed by
//!   [`LanceCache::with_key_prefix`](super::LanceCache::with_key_prefix);
//! - replace `invalidate_prefix` with [`LanceCache::clear`](super::LanceCache::clear)
//!   when clearing the shared backend is acceptable, or rotate a versioned
//!   namespace to leave older entries to age out;
//! - remove uses of `prefix`, `keys`, and session key-inventory methods; opaque
//!   keys have no readable or enumerable equivalent.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;

use async_trait::async_trait;
use futures::Future;

use crate::Result;
use crate::deepsize::Context;

use super::{CacheCodec, InternalCacheKey};

/// A type-erased cache entry.
pub type CacheEntry = Arc<dyn Any + Send + Sync>;

/// Low-level pluggable cache backend.
///
/// Implementations store entries keyed by [`InternalCacheKey`] and return
/// type-erased [`CacheEntry`] values.
/// [`LanceCache`](super::LanceCache) handles key construction and type safety;
/// backend authors only need to implement storage and eviction.
#[async_trait]
pub trait CacheBackend: Send + Sync + std::fmt::Debug {
    /// Look up an entry by its key.
    ///
    /// `codec` is provided so that persistent backends can deserialize the
    /// entry from storage. In-memory backends can ignore it. When `codec`
    /// is `None`, the entry type does not support serialization yet and
    /// must be stored in-memory.
    ///
    /// The goal is for all cache entry types to eventually have codecs,
    /// at which point the `Option` will be removed.
    async fn get(&self, key: &InternalCacheKey, codec: Option<CacheCodec>) -> Option<CacheEntry>;

    /// Store an entry. `size_bytes` is used for eviction accounting.
    ///
    /// See [`get`](Self::get) for codec semantics.
    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        size_bytes: usize,
        codec: Option<CacheCodec>,
    );

    /// Get an existing entry or compute it from `loader`.
    ///
    /// Implementations should deduplicate concurrent loads for the same key
    /// so the loader runs at most once.
    ///
    /// Returns `(entry, was_cached)` where `was_cached` is `true` if the entry
    /// was already present in the cache (the loader was not invoked).
    ///
    /// See [`get`](Self::get) for codec semantics.
    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
        codec: Option<CacheCodec>,
    ) -> Result<(CacheEntry, bool)>;

    /// Remove all entries.
    async fn clear(&self);

    /// Number of entries currently stored (may flush pending operations).
    async fn num_entries(&self) -> usize;

    /// Total weighted size in bytes of all stored entries (may flush pending operations).
    async fn size_bytes(&self) -> usize;

    /// Approximate number of entries, callable from synchronous contexts.
    /// Backends that cannot provide this cheaply should return 0.
    fn approx_num_entries(&self) -> usize {
        0
    }

    /// Approximate weighted size in bytes, callable from synchronous contexts.
    /// Used as a `DeepSizeOf` fallback when exact entry traversal is unavailable.
    /// Backends that cannot provide this cheaply should return 0.
    ///
    /// Assumes entries do not share underlying buffers; if they do, the
    /// returned total may overcount.
    fn approx_size_bytes(&self) -> usize {
        0
    }

    /// Computes the size of the entries currently held in memory.
    ///
    /// `size_of_entry` threads a shared [`Context`] through each value so
    /// allocations shared by multiple entries are counted once. It returns
    /// `None` when the value's concrete type was not registered by
    /// [`LanceCache`](super::LanceCache); implementations should use the
    /// entry's declared eviction size as a fallback in that case.
    ///
    /// Backends that can enumerate their in-memory entries should include the
    /// physical key footprint in the returned total. The default returns
    /// `None`, causing `LanceCache` to use [`approx_size_bytes`](Self::approx_size_bytes).
    fn deep_size_of_entries(
        &self,
        _context: &mut Context,
        _size_of_entry: &dyn Fn(&CacheEntry, &mut Context) -> Option<usize>,
    ) -> Option<usize> {
        None
    }
}
