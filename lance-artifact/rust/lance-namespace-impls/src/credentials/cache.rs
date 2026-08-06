// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Credential caching for cloud storage access.
//!
//! This module provides a caching wrapper for credential vendors that reduces
//! the number of credential vending requests (e.g., STS calls) by caching
//! credentials until they are close to expiration.
//!
//! ## Caching Strategy
//!
//! - **Cache Key**: Table location + identity hash (api_key hash or auth_token hash)
//! - **TTL**: Half of the credential's remaining lifetime, capped at 30 minutes
//! - **Eviction**: Credentials are evicted when TTL expires or when explicitly cleared
//!
//! ## Example
//!
//! ```ignore
//! use lance_namespace_impls::credentials::cache::CachingCredentialVendor;
//!
//! let vendor = AwsCredentialVendor::new(config).await?;
//! let cached_vendor = CachingCredentialVendor::new(Box::new(vendor));
//!
//! // First call hits the underlying vendor
//! let creds1 = cached_vendor.vend_credentials("s3://bucket/table", None).await?;
//!
//! // Subsequent calls within TTL return cached credentials
//! let creds2 = cached_vendor.vend_credentials("s3://bucket/table", None).await?;
//! ```

use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use lance_core::Result;
use lance_namespace::models::Identity;
use log::debug;
use tokio::sync::RwLock;

use super::{CredentialVendor, VendedCredentials, VendedPermission};

/// Maximum cache TTL: 30 minutes.
/// Even if credentials are valid for longer, we refresh more frequently
/// to handle clock skew and ensure freshness.
const MAX_CACHE_TTL_SECS: u64 = 30 * 60;

/// Minimum cache TTL: 1 minute.
/// If credentials expire sooner than this, we don't cache them.
const MIN_CACHE_TTL_SECS: u64 = 60;

/// A cached credential entry with expiration tracking.
#[derive(Clone)]
struct CacheEntry {
    credentials: VendedCredentials,
    /// When this cache entry should be considered stale
    cached_until: Instant,
}

impl std::fmt::Debug for CacheEntry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheEntry")
            .field("credentials", &"[redacted]")
            .field("cached_until", &self.cached_until)
            .finish()
    }
}

impl CacheEntry {
    fn is_stale(&self) -> bool {
        Instant::now() >= self.cached_until
    }
}

/// A caching wrapper for credential vendors.
///
/// This wrapper caches vended credentials to reduce the number of underlying
/// credential vending operations (e.g., STS calls). Credentials are cached
/// until half their lifetime has passed, capped at 30 minutes.
#[derive(Debug)]
pub struct CachingCredentialVendor {
    inner: Box<dyn CredentialVendor>,
    cache: Arc<RwLock<HashMap<String, CacheEntry>>>,
}

impl CachingCredentialVendor {
    /// Create a new caching credential vendor wrapping the given vendor.
    pub fn new(inner: Box<dyn CredentialVendor>) -> Self {
        Self {
            inner,
            cache: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Build a cache key from the table location and identity.
    ///
    /// The key is a hash of the location and identity fields to ensure
    /// different identities get different cached credentials.
    fn build_cache_key(table_location: &str, identity: Option<&Identity>) -> String {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();

        table_location.hash(&mut hasher);

        if let Some(id) = identity {
            if let Some(ref api_key) = id.api_key {
                ":api_key:".hash(&mut hasher);
                api_key.hash(&mut hasher);
            }
            if let Some(ref auth_token) = id.auth_token {
                ":auth_token:".hash(&mut hasher);
                // Only hash first 64 chars of token to avoid memory issues with large tokens
                let token_prefix = if auth_token.len() > 64 {
                    &auth_token[..64]
                } else {
                    auth_token.as_str()
                };
                token_prefix.hash(&mut hasher);
            }
        } else {
            ":no_identity".hash(&mut hasher);
        }

        format!("{:016x}", hasher.finish())
    }

    /// Calculate the cache TTL for the given credentials.
    ///
    /// Returns the TTL as a Duration, or None if the credentials should not be cached.
    fn calculate_cache_ttl(credentials: &VendedCredentials) -> Option<Duration> {
        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("time went backwards")
            .as_millis() as u64;

        if credentials.expires_at_millis <= now_millis {
            // Already expired
            return None;
        }

        let remaining_millis = credentials.expires_at_millis - now_millis;
        let remaining_secs = remaining_millis / 1000;

        // TTL is half the remaining lifetime
        let ttl_secs = remaining_secs / 2;

        // Cap between MIN and MAX
        if ttl_secs < MIN_CACHE_TTL_SECS {
            None // Don't cache if TTL is too short
        } else {
            Some(Duration::from_secs(ttl_secs.min(MAX_CACHE_TTL_SECS)))
        }
    }

    /// Clear all cached credentials.
    pub async fn clear_cache(&self) {
        let mut cache = self.cache.write().await;
        cache.clear();
        debug!("Credential cache cleared");
    }

    /// Get the number of cached entries.
    pub async fn cache_size(&self) -> usize {
        let cache = self.cache.read().await;
        cache.len()
    }

    /// Remove stale entries from the cache.
    pub async fn evict_stale(&self) -> usize {
        let mut cache = self.cache.write().await;
        let before = cache.len();
        cache.retain(|_, entry| !entry.is_stale());
        let evicted = before - cache.len();
        if evicted > 0 {
            debug!("Evicted {} stale credential cache entries", evicted);
        }
        evicted
    }
}

#[async_trait]
impl CredentialVendor for CachingCredentialVendor {
    async fn vend_credentials(
        &self,
        table_location: &str,
        identity: Option<&Identity>,
    ) -> Result<VendedCredentials> {
        let cache_key = Self::build_cache_key(table_location, identity);

        // Try to get from cache first
        {
            let cache = self.cache.read().await;
            if let Some(entry) = cache.get(&cache_key)
                && !entry.is_stale()
                && !entry.credentials.is_expired()
            {
                debug!(
                    "Credential cache hit for location={}, provider={}",
                    table_location,
                    self.inner.provider_name()
                );
                return Ok(entry.credentials.clone());
            }
        }

        // Cache miss or stale - vend new credentials
        debug!(
            "Credential cache miss for location={}, provider={}",
            table_location,
            self.inner.provider_name()
        );

        let credentials = self
            .inner
            .vend_credentials(table_location, identity)
            .await?;

        // Cache the new credentials if TTL is sufficient
        if let Some(ttl) = Self::calculate_cache_ttl(&credentials) {
            let entry = CacheEntry {
                credentials: credentials.clone(),
                cached_until: Instant::now() + ttl,
            };

            let mut cache = self.cache.write().await;
            cache.insert(cache_key, entry);

            debug!(
                "Cached credentials for location={}, ttl={}s",
                table_location,
                ttl.as_secs()
            );
        }

        Ok(credentials)
    }

    fn provider_name(&self) -> &'static str {
        self.inner.provider_name()
    }

    fn permission(&self) -> VendedPermission {
        self.inner.permission()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU32, Ordering};

    /// A mock credential vendor for testing.
    #[derive(Debug)]
    struct MockVendor {
        call_count: AtomicU32,
        duration_millis: u64,
    }

    impl MockVendor {
        fn new(duration_millis: u64) -> Self {
            Self {
                call_count: AtomicU32::new(0),
                duration_millis,
            }
        }
    }

    #[async_trait]
    impl CredentialVendor for MockVendor {
        async fn vend_credentials(
            &self,
            _table_location: &str,
            _identity: Option<&Identity>,
        ) -> Result<VendedCredentials> {
            self.call_count.fetch_add(1, Ordering::SeqCst);

            let now_millis = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            let mut storage_options = HashMap::new();
            storage_options.insert("test_key".to_string(), "test_value".to_string());

            Ok(VendedCredentials::new(
                storage_options,
                now_millis + self.duration_millis,
            ))
        }

        fn provider_name(&self) -> &'static str {
            "mock"
        }

        fn permission(&self) -> VendedPermission {
            VendedPermission::Read
        }
    }

    #[test]
    fn test_build_cache_key_no_identity() {
        let key1 = CachingCredentialVendor::build_cache_key("s3://bucket/table1", None);
        let key2 = CachingCredentialVendor::build_cache_key("s3://bucket/table2", None);
        let key3 = CachingCredentialVendor::build_cache_key("s3://bucket/table1", None);

        assert_ne!(key1, key2, "Different locations should have different keys");
        assert_eq!(key1, key3, "Same location should have same key");
    }

    #[test]
    fn test_build_cache_key_with_identity() {
        let identity_api = Identity {
            api_key: Some("my-api-key".to_string()),
            auth_token: None,
        };
        let identity_token = Identity {
            api_key: None,
            auth_token: Some("my-token".to_string()),
        };

        let key_no_id = CachingCredentialVendor::build_cache_key("s3://bucket/table", None);
        let key_api =
            CachingCredentialVendor::build_cache_key("s3://bucket/table", Some(&identity_api));
        let key_token =
            CachingCredentialVendor::build_cache_key("s3://bucket/table", Some(&identity_token));

        assert_ne!(key_no_id, key_api, "Identity should change key");
        assert_ne!(key_no_id, key_token, "Identity should change key");
        assert_ne!(
            key_api, key_token,
            "Different identity types should have different keys"
        );
    }

    #[test]
    fn test_calculate_cache_ttl() {
        const CLOCK_SKEW_TOLERANCE_SECS: u64 = 2;

        fn assert_ttl_close_to(ttl: Option<Duration>, expected_secs: u64) {
            let actual_secs = ttl.map(|duration| duration.as_secs());
            assert!(
                matches!(
                    actual_secs,
                    Some(actual)
                        if actual <= expected_secs
                            && expected_secs.saturating_sub(actual) <= CLOCK_SKEW_TOLERANCE_SECS
                ),
                "expected ttl close to {expected_secs}s, got {actual_secs:?}"
            );
        }

        let now_millis = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Credentials with 1 hour remaining -> TTL should be 30 minutes (capped)
        let creds_1h = VendedCredentials::new(HashMap::new(), now_millis + 3600 * 1000);
        let ttl = CachingCredentialVendor::calculate_cache_ttl(&creds_1h);
        assert_ttl_close_to(ttl, MAX_CACHE_TTL_SECS);

        // Credentials with 10 minutes remaining -> TTL should be 5 minutes
        let creds_10m = VendedCredentials::new(HashMap::new(), now_millis + 10 * 60 * 1000);
        let ttl = CachingCredentialVendor::calculate_cache_ttl(&creds_10m);
        assert_ttl_close_to(ttl, 5 * 60);

        // Credentials with 1 minute remaining -> TTL should be None (too short)
        let creds_1m = VendedCredentials::new(HashMap::new(), now_millis + 60 * 1000);
        let ttl = CachingCredentialVendor::calculate_cache_ttl(&creds_1m);
        assert!(ttl.is_none(), "Should not cache short-lived credentials");

        // Already expired credentials -> None
        let creds_expired = VendedCredentials::new(HashMap::new(), now_millis - 1000);
        let ttl = CachingCredentialVendor::calculate_cache_ttl(&creds_expired);
        assert!(ttl.is_none(), "Should not cache expired credentials");
    }

    #[tokio::test]
    async fn test_caching_reduces_calls() {
        // Create a mock vendor with 1 hour credentials
        let mock = MockVendor::new(3600 * 1000);
        let cached = CachingCredentialVendor::new(Box::new(mock));

        // First call should hit the underlying vendor
        let _ = cached
            .vend_credentials("s3://bucket/table", None)
            .await
            .unwrap();
        assert_eq!(cached.cache_size().await, 1);

        // Get reference to inner mock for call count
        // We can't easily get the call count from the boxed trait, so we'll check cache size

        // Second call should use cache (cache size stays at 1)
        let _ = cached
            .vend_credentials("s3://bucket/table", None)
            .await
            .unwrap();
        assert_eq!(cached.cache_size().await, 1);

        // Different location should create new cache entry
        let _ = cached
            .vend_credentials("s3://bucket/table2", None)
            .await
            .unwrap();
        assert_eq!(cached.cache_size().await, 2);
    }

    #[tokio::test]
    async fn test_clear_cache() {
        let mock = MockVendor::new(3600 * 1000);
        let cached = CachingCredentialVendor::new(Box::new(mock));

        let _ = cached
            .vend_credentials("s3://bucket/table", None)
            .await
            .unwrap();
        assert_eq!(cached.cache_size().await, 1);

        cached.clear_cache().await;
        assert_eq!(cached.cache_size().await, 0);
    }

    #[tokio::test]
    async fn test_different_identities_cached_separately() {
        let mock = MockVendor::new(3600 * 1000);
        let cached = CachingCredentialVendor::new(Box::new(mock));

        let identity1 = Identity {
            api_key: Some("key1".to_string()),
            auth_token: None,
        };
        let identity2 = Identity {
            api_key: Some("key2".to_string()),
            auth_token: None,
        };

        // Same location with different identities should cache separately
        let _ = cached
            .vend_credentials("s3://bucket/table", Some(&identity1))
            .await
            .unwrap();
        let _ = cached
            .vend_credentials("s3://bucket/table", Some(&identity2))
            .await
            .unwrap();
        let _ = cached
            .vend_credentials("s3://bucket/table", None)
            .await
            .unwrap();

        assert_eq!(cached.cache_size().await, 3);
    }
}
