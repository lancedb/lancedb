// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use futures::Future;

use crate::Result;
use crate::deepsize::Context;
use crate::error::CloneableError;

use super::backend::{CacheBackend, CacheEntry};
use super::{CacheCodec, InternalCacheKey};

/// Internal record stored in the moka cache.
#[derive(Clone, Debug)]
struct MokaCacheEntry {
    entry: CacheEntry,
    size_bytes: usize,
}

/// Per-entry key cost for eviction.
pub(super) fn key_footprint(_key: &InternalCacheKey) -> usize {
    std::mem::size_of::<InternalCacheKey>()
}

fn physical_size(key: &InternalCacheKey, size_bytes: usize) -> usize {
    key_footprint(key).saturating_add(size_bytes)
}

/// Number of physical bytes represented by one Moka weight unit.
///
/// Moka limits each entry's weight to `u32`, so capacities above 4 GiB need
/// coarser units to account for a single large entry without undercharging it.
fn weight_unit(capacity: usize) -> usize {
    capacity.div_ceil(u32::MAX as usize).max(1)
}

fn entry_weight(key: &InternalCacheKey, size_bytes: usize, weight_unit: usize) -> u32 {
    physical_size(key, size_bytes)
        .div_ceil(weight_unit)
        .try_into()
        .unwrap_or(u32::MAX)
}

/// Default [`CacheBackend`] backed by a [moka](https://crates.io/crates/moka) cache.
///
/// Provides weighted-capacity eviction and concurrent-load deduplication
/// via moka's built-in `optionally_get_with`.
pub struct MokaCacheBackend {
    cache: moka::future::Cache<InternalCacheKey, MokaCacheEntry>,
    capacity: usize,
    weight_unit: usize,
}

impl std::fmt::Debug for MokaCacheBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MokaCacheBackend")
            .field("entry_count", &self.cache.entry_count())
            .finish()
    }
}

impl MokaCacheBackend {
    pub fn with_capacity(capacity: usize) -> Self {
        let weight_unit = weight_unit(capacity);
        let capacity_weight = capacity.div_ceil(weight_unit) as u64;
        let cache = moka::future::Cache::builder()
            .max_capacity(capacity_weight)
            .weigher(move |key: &InternalCacheKey, entry: &MokaCacheEntry| {
                entry_weight(key, entry.size_bytes, weight_unit)
            })
            .build();
        Self {
            cache,
            capacity,
            weight_unit,
        }
    }

    pub fn no_cache() -> Self {
        Self {
            cache: moka::future::Cache::new(0),
            capacity: 0,
            weight_unit: 1,
        }
    }

    /// Configured weighted capacity in bytes.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    fn weighted_size_bytes(&self) -> usize {
        self.cache
            .weighted_size()
            .saturating_mul(self.weight_unit as u64)
            .try_into()
            .unwrap_or(usize::MAX)
    }
}

#[async_trait]
impl CacheBackend for MokaCacheBackend {
    async fn get(&self, key: &InternalCacheKey, _codec: Option<CacheCodec>) -> Option<CacheEntry> {
        self.cache.get(key).await.map(|r| r.entry)
    }

    async fn insert(
        &self,
        key: &InternalCacheKey,
        entry: CacheEntry,
        size_bytes: usize,
        _codec: Option<CacheCodec>,
    ) {
        self.cache
            .insert(*key, MokaCacheEntry { entry, size_bytes })
            .await;
    }

    async fn get_or_insert<'a>(
        &self,
        key: &InternalCacheKey,
        loader: Pin<Box<dyn Future<Output = Result<(CacheEntry, usize)>> + Send + 'a>>,
        _codec: Option<CacheCodec>,
    ) -> Result<(CacheEntry, bool)> {
        // Track whether the loader actually ran (= cache miss).
        let was_miss = Arc::new(AtomicBool::new(false));
        let was_miss_clone = was_miss.clone();

        let init = async move {
            was_miss_clone.store(true, Ordering::Relaxed);
            loader
                .await
                .map(|(entry, size_bytes)| MokaCacheEntry { entry, size_bytes })
                .map_err(CloneableError)
        };

        let owned_key = *key;
        match self.cache.try_get_with(owned_key, init).await {
            Ok(record) => {
                let was_cached = !was_miss.load(Ordering::Relaxed);
                Ok((record.entry, was_cached))
            }
            Err(error) => Err(Arc::unwrap_or_clone(error).0),
        }
    }

    async fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
    }

    async fn num_entries(&self) -> usize {
        self.cache.run_pending_tasks().await;
        self.cache.entry_count() as usize
    }

    async fn size_bytes(&self) -> usize {
        self.cache.run_pending_tasks().await;
        self.weighted_size_bytes()
    }

    fn approx_num_entries(&self) -> usize {
        self.cache.entry_count() as usize
    }

    fn approx_size_bytes(&self) -> usize {
        // `weighted_size()` can be stale without `run_pending_tasks()`, which
        // is async and can't be called from this synchronous context.
        self.weighted_size_bytes()
    }

    fn deep_size_of_entries(
        &self,
        context: &mut Context,
        size_of_entry: &dyn Fn(&CacheEntry, &mut Context) -> Option<usize>,
    ) -> Option<usize> {
        Some(
            self.cache
                .iter()
                .map(|(key, record)| {
                    key_footprint(key.as_ref())
                        + size_of_entry(&record.entry, context).unwrap_or(record.size_bytes)
                })
                .sum(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn entry_weights_are_exact_at_byte_granularity() {
        let key = InternalCacheKey::from_bytes([0; 16]);
        assert_eq!(weight_unit(4096), 1);
        assert_eq!(entry_weight(&key, 7, 1), 23);
    }

    #[tokio::test]
    async fn size_methods_use_constant_time_weighted_accounting() {
        let backend = MokaCacheBackend::with_capacity(4096);
        let key = InternalCacheKey::from_bytes([0; 16]);
        let entry: CacheEntry = Arc::new(());
        let value_size = 7;
        let expected = physical_size(&key, value_size);

        backend.insert(&key, entry, value_size, None).await;

        assert_eq!(backend.size_bytes().await, expected);
        assert_eq!(backend.approx_size_bytes(), expected);
    }

    #[cfg(target_pointer_width = "64")]
    #[test]
    fn entry_weights_scale_for_capacities_above_four_gibibytes() {
        let key = InternalCacheKey::from_bytes([0; 16]);
        let capacity = 6 * 1024 * 1024 * 1024;
        let weight_unit = weight_unit(capacity);
        assert_eq!(weight_unit, 2);

        let size_bytes = u32::MAX as usize + 1024;
        let expected = physical_size(&key, size_bytes).div_ceil(weight_unit);
        let weight = entry_weight(&key, size_bytes, weight_unit);
        assert_eq!(weight as usize, expected);
        assert_ne!(weight, u32::MAX);
    }
}

/// Registry identifier for the built-in Moka backend.
pub const MOKA_BACKEND_KIND: &str = "moka";

/// [`BackendBuildFn`](super::registry::BackendBuildFn) for [`MokaCacheBackend`].
///
/// Recognized options:
///   * `capacity` — total weighted capacity in bytes (`usize`).
///     This must be present and non-empty.
///
/// Unknown options are rejected so typos surface immediately instead of
/// silently falling through to the default capacity.
pub(super) fn build_moka_backend(
    config: &super::registry::BackendConfig,
) -> Result<MokaCacheBackend> {
    let mut capacity: Option<usize> = None;
    for (key, value) in &config.options {
        match key.as_str() {
            "capacity" => {
                if value.is_empty() {
                    return Err(crate::Error::invalid_input(
                        "moka cache backend: capacity must not be empty",
                    ));
                } else {
                    capacity = Some(value.parse::<usize>().map_err(|err| {
                        crate::Error::invalid_input(format!(
                            "moka cache backend: cannot parse capacity {:?}: {}",
                            value, err
                        ))
                    })?);
                }
            }
            other => {
                return Err(crate::Error::invalid_input(format!(
                    "moka cache backend: unknown option {:?}",
                    other
                )));
            }
        }
    }
    let capacity = capacity.ok_or_else(|| {
        crate::Error::invalid_input(
            "moka cache backend: capacity is required; use moka://?capacity=<bytes>",
        )
    })?;
    Ok(MokaCacheBackend::with_capacity(capacity))
}

pub(super) fn build_moka(config: &super::registry::BackendConfig) -> Result<Arc<dyn CacheBackend>> {
    Ok(Arc::new(build_moka_backend(config)?))
}

#[cfg(test)]
mod moka_registry_tests {
    use super::super::backend_uri::{build_from_uri, parse_backend_uri};
    use super::super::registry::{BackendConfig, build_from_config, registry_test_lock};
    use super::*;

    #[test]
    fn test_moka_builds_from_config() {
        let _lock = registry_test_lock();
        let cfg = BackendConfig::new("moka")
            .unwrap()
            .with_option("capacity", "1048576");
        let backend = build_moka_backend(&cfg).unwrap();
        assert_eq!(backend.capacity(), 1048576);
        let _backend = build_from_config(&cfg).unwrap();
    }

    #[test]
    fn test_moka_builds_from_uri() {
        let _lock = registry_test_lock();
        let cfg = parse_backend_uri("moka://?capacity=1048576").unwrap();
        let backend = build_moka_backend(&cfg).unwrap();
        assert_eq!(backend.capacity(), 1048576);
        let _backend = build_from_uri("moka://?capacity=1048576").unwrap();
    }

    #[test]
    fn test_moka_rejects_unknown_option() {
        let _lock = registry_test_lock();
        let cfg = BackendConfig::new("moka")
            .unwrap()
            .with_option("mystery", "1");
        let err = build_from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("unknown option"));
    }

    #[test]
    fn test_moka_rejects_bad_capacity() {
        let _lock = registry_test_lock();
        let cfg = BackendConfig::new("moka")
            .unwrap()
            .with_option("capacity", "not-a-number");
        let err = build_from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("cannot parse capacity"));
    }

    #[test]
    fn test_moka_rejects_missing_capacity() {
        let _lock = registry_test_lock();
        let cfg = BackendConfig::new("moka").unwrap();
        let err = build_from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("capacity is required"));
    }

    #[test]
    fn test_moka_rejects_empty_capacity() {
        let _lock = registry_test_lock();
        let cfg = BackendConfig::new("moka")
            .unwrap()
            .with_option("capacity", "");
        let err = build_from_config(&cfg).unwrap_err();
        assert!(err.to_string().contains("capacity must not be empty"));
    }
}
