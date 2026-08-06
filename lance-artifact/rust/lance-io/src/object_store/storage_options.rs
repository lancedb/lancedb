// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Storage options provider and accessor for dynamic credential fetching
//!
//! This module provides:
//! - [`StorageOptionsProvider`] trait for fetching storage options from various sources
//!   (namespace servers, secret managers, etc.) with support for expiration tracking
//! - [`StorageOptionsAccessor`] for unified access to storage options with automatic
//!   caching and refresh

use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use mock_instant::thread_local::{SystemTime, UNIX_EPOCH};

#[cfg(not(test))]
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use lance_namespace::LanceNamespace;
use lance_namespace::models::DescribeTableRequest;
use tokio::sync::RwLock;

use crate::{Error, Result};

/// Key for the expiration timestamp in storage options HashMap
pub const EXPIRES_AT_MILLIS_KEY: &str = "expires_at_millis";

/// Key for the refresh offset in storage options HashMap (milliseconds before expiry to refresh)
pub const REFRESH_OFFSET_MILLIS_KEY: &str = "refresh_offset_millis";

/// Default refresh offset: 60 seconds before expiration
const DEFAULT_REFRESH_OFFSET_MILLIS: u64 = 60_000;

/// Trait for providing storage options with expiration tracking
///
/// Implementations can fetch storage options from various sources (namespace servers,
/// secret managers, etc.) and are usable from Python/Java.
///
/// # Current Use Cases
///
/// - **Temporary Credentials**: Fetch short-lived AWS temporary credentials that expire
///   after a set time period, with automatic refresh before expiration
///
/// # Future Possible Use Cases
///
/// - **Dynamic Storage Location Resolution**: Resolve logical names to actual storage
///   locations (bucket aliases, S3 Access Points, region-specific endpoints) that may
///   change based on region, tier, data migration, or failover scenarios
/// - **Runtime S3 Tags Assignment**: Inject cost allocation tags, security labels, or
///   compliance metadata into S3 requests based on the current execution context (user,
///   application, workspace, etc.)
/// - **Dynamic Endpoint Configuration**: Update storage endpoints for disaster recovery,
///   A/B testing, or gradual migration scenarios
/// - **Just-in-time Permission Elevation**: Request elevated permissions only when needed
///   for sensitive operations, then immediately revoke them
/// - **Secret Manager Integration**: Fetch encryption keys from AWS Secrets Manager,
///   Azure Key Vault, or Google Secret Manager with automatic rotation
/// - **OIDC/SAML Federation**: Integrate with identity providers to obtain storage
///   credentials based on user identity and group membership
///
/// # Equality and Hashing
///
/// Implementations must provide `provider_id()` which returns a unique identifier for
/// equality and hashing purposes. Two providers with the same ID are considered equal
/// and will share the same cached ObjectStore in the registry.
#[async_trait]
pub trait StorageOptionsProvider: Send + Sync + fmt::Debug {
    /// Fetch fresh storage options
    ///
    /// Returns None if no storage options are available, or Some(HashMap) with the options.
    /// If the [`EXPIRES_AT_MILLIS_KEY`] key is present in the HashMap, it should contain the
    /// epoch time in milliseconds when the options expire, and credentials will automatically
    /// refresh before expiration.
    /// If [`EXPIRES_AT_MILLIS_KEY`] is not provided, the options are considered to never expire.
    async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>>;

    /// Fetch fresh storage options, bypassing caches along the chain.
    ///
    /// Providers that serve from an upstream cache (e.g. base-scoped wrappers
    /// reading through a parent accessor) override this to force the upstream
    /// to re-fetch. Defaults to [`Self::fetch_storage_options`].
    async fn force_fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        self.fetch_storage_options().await
    }

    /// Return a human-readable unique identifier for this provider instance
    ///
    /// This is used for equality comparison and hashing in the object store registry.
    /// Two providers with the same ID will be treated as equal and share the same cached
    /// ObjectStore.
    ///
    /// The ID should be human-readable for debugging and logging purposes.
    /// For example: `"namespace[dir(root=/data)],table[db$schema$table1]"`
    ///
    /// The ID should uniquely identify the provider's configuration.
    fn provider_id(&self) -> String;
}

/// StorageOptionsProvider implementation that fetches options from a LanceNamespace
pub struct LanceNamespaceStorageOptionsProvider {
    namespace_client: Arc<dyn LanceNamespace>,
    table_id: Vec<String>,
}

impl fmt::Debug for LanceNamespaceStorageOptionsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.provider_id())
    }
}

impl fmt::Display for LanceNamespaceStorageOptionsProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.provider_id())
    }
}

impl LanceNamespaceStorageOptionsProvider {
    /// Create a new LanceNamespaceStorageOptionsProvider
    ///
    /// # Arguments
    /// * `namespace_client` - The namespace implementation to fetch storage options from
    /// * `table_id` - The table identifier
    pub fn new(namespace_client: Arc<dyn LanceNamespace>, table_id: Vec<String>) -> Self {
        Self {
            namespace_client,
            table_id,
        }
    }
}

#[async_trait]
impl StorageOptionsProvider for LanceNamespaceStorageOptionsProvider {
    async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        let request = DescribeTableRequest {
            id: Some(self.table_id.clone()),
            // Some server implementations may not return credentials unless explicitly requested
            vend_credentials: Some(true),
            ..Default::default()
        };

        let response = self
            .namespace_client
            .describe_table(request)
            .await
            .map_err(|e| {
                Error::io_source(Box::new(std::io::Error::other(format!(
                    "Failed to fetch storage options: {}",
                    e
                ))))
            })?;

        Ok(response.storage_options)
    }

    fn provider_id(&self) -> String {
        format!(
            "LanceNamespaceStorageOptionsProvider {{ namespace_client: {}, table_id: {:?} }}",
            self.namespace_client.namespace_id(),
            self.table_id
        )
    }
}

/// Prefix marking a storage option as scoped to a single registered base path.
///
/// A key of the form `base_<id>.<key>` applies `<key>` only to the base path
/// with manifest id `<id>`, overriding the shared (unscoped) options for that
/// base. For example `base_1.account_key = abc` gives the base with id 1 the
/// option `account_key = abc` while it inherits every unscoped option.
pub const BASE_SCOPED_OPTION_PREFIX: &str = "base_";

/// Parse a base-scoped storage option key of the form `base_<id>.<key>`.
///
/// Returns `Some((base_id, key))` only for keys that match the convention
/// exactly: the `base_` prefix, an all-digit u32 base id, a `.` separator, and
/// a non-empty remainder. Any other key (e.g. `base_url`, `base_x.key`,
/// `base_1.`) is not base-scoped.
pub fn parse_base_scoped_key(key: &str) -> Option<(u32, &str)> {
    let rest = key.strip_prefix(BASE_SCOPED_OPTION_PREFIX)?;
    let (id_str, scoped_key) = rest.split_once('.')?;
    if scoped_key.is_empty() || id_str.is_empty() || !id_str.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    let id = id_str.parse::<u32>().ok()?;
    Some((id, scoped_key))
}

/// Returns true if any key in `options` is base-scoped (`base_<id>.<key>`).
pub fn has_base_scoped_options(options: &HashMap<String, String>) -> bool {
    options
        .keys()
        .any(|key| parse_base_scoped_key(key).is_some())
}

/// Resolve the effective storage options for one base path scope.
///
/// All base-scoped keys are removed from the result. When `base_id` is
/// `Some(id)`, entries scoped to that id are overlaid on the unscoped options,
/// adding or overriding keys. `None` resolves the default scope (the primary
/// dataset base), which simply drops every base-scoped entry.
pub fn resolve_base_scoped_options(
    options: &HashMap<String, String>,
    base_id: Option<u32>,
) -> HashMap<String, String> {
    let mut resolved = HashMap::with_capacity(options.len());
    let mut overrides = Vec::new();
    for (key, value) in options {
        match parse_base_scoped_key(key) {
            Some((id, scoped_key)) => {
                if Some(id) == base_id {
                    overrides.push((scoped_key.to_string(), value.clone()));
                }
            }
            None => {
                resolved.insert(key.clone(), value.clone());
            }
        }
    }
    resolved.extend(overrides);
    resolved
}

/// A [`StorageOptionsProvider`] that resolves another accessor's options for a
/// single base path scope.
///
/// Fetching through this provider first refreshes the parent accessor when its
/// options have expired, then resolves the refreshed options for the scope. As
/// a result, dynamically vended per-base credentials (e.g. a namespace server
/// returning `base_<id>.<key>` entries in one flat map) stay fresh per base.
#[derive(Debug)]
pub struct BaseScopedStorageOptionsProvider {
    inner: Arc<StorageOptionsAccessor>,
    base_id: Option<u32>,
}

impl BaseScopedStorageOptionsProvider {
    pub fn new(inner: Arc<StorageOptionsAccessor>, base_id: Option<u32>) -> Self {
        Self { inner, base_id }
    }
}

#[async_trait]
impl StorageOptionsProvider for BaseScopedStorageOptionsProvider {
    async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        let options = self.inner.get_storage_options().await?;
        Ok(Some(resolve_base_scoped_options(&options.0, self.base_id)))
    }

    async fn force_fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
        let options = self.inner.refresh_storage_options().await?;
        Ok(Some(resolve_base_scoped_options(&options.0, self.base_id)))
    }

    fn provider_id(&self) -> String {
        match self.base_id {
            Some(id) => format!("base-scoped[base_id={}]({})", id, self.inner.accessor_id()),
            None => format!("base-scoped[default]({})", self.inner.accessor_id()),
        }
    }
}

/// Unified access to storage options with automatic caching and refresh
///
/// This struct bundles static storage options with an optional dynamic provider,
/// handling all caching and refresh logic internally. It provides a single entry point
/// for accessing storage options regardless of whether they're static or dynamic.
///
/// # Behavior
///
/// - If only static options are provided, returns those options
/// - If a provider is configured, fetches from provider and caches results
/// - Automatically refreshes cached options before expiration (based on refresh_offset)
/// - Uses `expires_at_millis` key to track expiration
///
/// # Thread Safety
///
/// The accessor is thread-safe and can be shared across multiple tasks.
/// Concurrent refresh attempts are deduplicated using a try-lock mechanism.
pub struct StorageOptionsAccessor {
    /// Initial/fallback static storage options
    initial_options: Option<HashMap<String, String>>,

    /// Optional dynamic provider for refreshing options
    provider: Option<Arc<dyn StorageOptionsProvider>>,

    /// Cached storage options with expiration tracking
    cache: Arc<RwLock<Option<CachedStorageOptions>>>,

    /// Duration before expiry to trigger refresh
    refresh_offset: Duration,

    /// True when this accessor was produced by [`Self::scoped_to_base`]; its
    /// options are already resolved for one base path scope, so scoping again
    /// is a no-op.
    scope_resolved: bool,
}

impl fmt::Debug for StorageOptionsAccessor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StorageOptionsAccessor")
            .field("has_initial_options", &self.initial_options.is_some())
            .field("has_provider", &self.provider.is_some())
            .field("refresh_offset", &self.refresh_offset)
            .finish()
    }
}

#[derive(Debug, Clone)]
struct CachedStorageOptions {
    options: HashMap<String, String>,
    expires_at_millis: Option<u64>,
}

impl StorageOptionsAccessor {
    /// Extract refresh offset from storage options, or use default
    fn extract_refresh_offset(options: &HashMap<String, String>) -> Duration {
        options
            .get(REFRESH_OFFSET_MILLIS_KEY)
            .and_then(|s| s.parse::<u64>().ok())
            .map(Duration::from_millis)
            .unwrap_or(Duration::from_millis(DEFAULT_REFRESH_OFFSET_MILLIS))
    }

    /// Effective expiration of a raw options map: the minimum of the unscoped
    /// `expires_at_millis` and every `base_<id>.expires_at_millis` entry.
    ///
    /// A flat map may vend per-base credentials that expire before the shared
    /// ones. Refreshing when the earliest credential is due keeps base-scoped
    /// accessors (which refresh through this accessor) from re-resolving stale
    /// per-base credentials out of a still-"valid" cache.
    fn effective_expires_at_millis(options: &HashMap<String, String>) -> Option<u64> {
        options
            .iter()
            .filter(|(key, _)| {
                key.as_str() == EXPIRES_AT_MILLIS_KEY
                    || matches!(
                        parse_base_scoped_key(key),
                        Some((_, scoped_key)) if scoped_key == EXPIRES_AT_MILLIS_KEY
                    )
            })
            .filter_map(|(_, value)| value.parse::<u64>().ok())
            .min()
    }

    /// Create an accessor with only static options (no refresh capability)
    ///
    /// The returned accessor will always return the provided options.
    /// This is useful when credentials don't expire or are managed externally.
    pub fn with_static_options(options: HashMap<String, String>) -> Self {
        let expires_at_millis = Self::effective_expires_at_millis(&options);
        let refresh_offset = Self::extract_refresh_offset(&options);

        Self {
            initial_options: Some(options.clone()),
            provider: None,
            cache: Arc::new(RwLock::new(Some(CachedStorageOptions {
                options,
                expires_at_millis,
            }))),
            refresh_offset,
            scope_resolved: false,
        }
    }

    /// Create an accessor with a dynamic provider (no initial options)
    ///
    /// The accessor will fetch from the provider on first access and cache
    /// the results. Refresh happens automatically before expiration.
    /// Uses the default refresh offset (60 seconds) until options are fetched.
    ///
    /// # Arguments
    /// * `provider` - The storage options provider for fetching fresh options
    pub fn with_provider(provider: Arc<dyn StorageOptionsProvider>) -> Self {
        Self {
            initial_options: None,
            provider: Some(provider),
            cache: Arc::new(RwLock::new(None)),
            refresh_offset: Duration::from_millis(DEFAULT_REFRESH_OFFSET_MILLIS),
            scope_resolved: false,
        }
    }

    /// Create an accessor with initial options and a dynamic provider
    ///
    /// Initial options are used until they expire, then the provider is called.
    /// This avoids an immediate fetch when initial credentials are still valid.
    /// The `refresh_offset_millis` key in initial_options controls refresh timing.
    ///
    /// # Arguments
    /// * `initial_options` - Initial storage options to cache
    /// * `provider` - The storage options provider for refreshing
    pub fn with_initial_and_provider(
        initial_options: HashMap<String, String>,
        provider: Arc<dyn StorageOptionsProvider>,
    ) -> Self {
        let expires_at_millis = Self::effective_expires_at_millis(&initial_options);
        let refresh_offset = Self::extract_refresh_offset(&initial_options);

        Self {
            initial_options: Some(initial_options.clone()),
            provider: Some(provider),
            cache: Arc::new(RwLock::new(Some(CachedStorageOptions {
                options: initial_options,
                expires_at_millis,
            }))),
            refresh_offset,
            scope_resolved: false,
        }
    }

    /// Get current valid storage options
    ///
    /// - Returns cached options if not expired
    /// - Fetches from provider if expired or not cached
    /// - Falls back to initial_options if provider returns None
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The provider fails to fetch options
    /// - No options are available (no cache, no provider, no initial options)
    pub async fn get_storage_options(&self) -> Result<super::StorageOptions> {
        loop {
            match self.do_get_storage_options().await? {
                Some(options) => return Ok(options),
                None => {
                    // Lock was busy, wait 10ms before retrying
                    tokio::time::sleep(Duration::from_millis(10)).await;
                    continue;
                }
            }
        }
    }

    /// Fetch fresh options from the provider and update the cache.
    ///
    /// This bypasses the cache for callers that need to validate provider-vended
    /// credentials even when initial metadata has no expiration. The force
    /// propagates through provider chains (e.g. base-scoped wrappers), so the
    /// origin provider is re-queried even when intermediate caches are valid.
    pub(crate) async fn refresh_storage_options(&self) -> Result<super::StorageOptions> {
        let Some(provider) = &self.provider else {
            return self.get_storage_options().await;
        };

        log::debug!(
            "Refreshing storage options from provider: {}",
            provider.provider_id()
        );

        let storage_options_map = provider.force_fetch_storage_options().await.map_err(|e| {
            Error::io_source(Box::new(std::io::Error::other(format!(
                "Failed to fetch storage options: {}",
                e
            ))))
        })?;

        let Some(options) = storage_options_map else {
            if let Some(initial) = &self.initial_options {
                return Ok(super::StorageOptions(initial.clone()));
            }
            log::debug!(
                "Provider {} returned no storage options, using default credentials",
                provider.provider_id()
            );
            return Ok(super::StorageOptions(HashMap::new()));
        };

        let expires_at_millis = Self::effective_expires_at_millis(&options);

        let mut cache = self.cache.write().await;
        *cache = Some(CachedStorageOptions {
            options: options.clone(),
            expires_at_millis,
        });

        Ok(super::StorageOptions(options))
    }

    async fn do_get_storage_options(&self) -> Result<Option<super::StorageOptions>> {
        // Check if we have valid cached options with read lock
        {
            let cached = self.cache.read().await;
            if !self.needs_refresh(&cached)
                && let Some(cached_opts) = &*cached
            {
                return Ok(Some(super::StorageOptions(cached_opts.options.clone())));
            }
        }

        // If no provider, return initial options or use defaults
        let Some(provider) = &self.provider else {
            return if let Some(initial) = &self.initial_options {
                Ok(Some(super::StorageOptions(initial.clone())))
            } else {
                // No provider and no initial options - use default credentials
                Ok(Some(super::StorageOptions(HashMap::new())))
            };
        };

        // Try to acquire write lock - if it fails, return None and let caller retry
        let Ok(mut cache) = self.cache.try_write() else {
            return Ok(None);
        };

        // Double-check if options are still stale after acquiring write lock
        // (another thread might have refreshed them)
        if !self.needs_refresh(&cache)
            && let Some(cached_opts) = &*cache
        {
            return Ok(Some(super::StorageOptions(cached_opts.options.clone())));
        }
        log::debug!(
            "Refreshing storage options from provider: {}",
            provider.provider_id()
        );

        let storage_options_map = provider.fetch_storage_options().await.map_err(|e| {
            Error::io_source(Box::new(std::io::Error::other(format!(
                "Failed to fetch storage options: {}",
                e
            ))))
        })?;

        let Some(options) = storage_options_map else {
            // Provider returned None, fall back to initial options or use defaults
            if let Some(initial) = &self.initial_options {
                return Ok(Some(super::StorageOptions(initial.clone())));
            }
            // Provider returned None and no initial options - use default credentials
            // This is valid when namespace doesn't vend credentials (e.g., directory namespace
            // where environment credentials are used)
            log::debug!(
                "Provider {} returned no storage options, using default credentials",
                provider.provider_id()
            );
            return Ok(Some(super::StorageOptions(HashMap::new())));
        };

        let expires_at_millis = Self::effective_expires_at_millis(&options);

        if let Some(expires_at) = expires_at_millis {
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or(Duration::from_secs(0))
                .as_millis() as u64;
            let expires_in_secs = (expires_at.saturating_sub(now_ms)) / 1000;
            log::debug!(
                "Successfully refreshed storage options from provider: {}, options expire in {} seconds",
                provider.provider_id(),
                expires_in_secs
            );
        } else {
            log::debug!(
                "Successfully refreshed storage options from provider: {} (no expiration)",
                provider.provider_id()
            );
        }

        *cache = Some(CachedStorageOptions {
            options: options.clone(),
            expires_at_millis,
        });

        Ok(Some(super::StorageOptions(options)))
    }

    fn needs_refresh(&self, cached: &Option<CachedStorageOptions>) -> bool {
        match cached {
            None => true,
            Some(cached_opts) => {
                if let Some(expires_at_millis) = cached_opts.expires_at_millis {
                    let now_ms = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap_or(Duration::from_secs(0))
                        .as_millis() as u64;

                    // Refresh if we're within the refresh offset of expiration
                    let refresh_offset_millis = self.refresh_offset.as_millis() as u64;
                    now_ms + refresh_offset_millis >= expires_at_millis
                } else {
                    // No expiration means options never expire
                    false
                }
            }
        }
    }

    /// Get the initial storage options without refresh
    ///
    /// Returns the initial options that were provided when creating the accessor.
    /// This does not trigger any refresh, even if the options have expired.
    pub fn initial_storage_options(&self) -> Option<&HashMap<String, String>> {
        self.initial_options.as_ref()
    }

    /// Get the accessor ID for equality/hashing
    ///
    /// Returns the provider_id if a provider exists, otherwise generates
    /// a stable ID from the initial options hash.
    pub fn accessor_id(&self) -> String {
        if let Some(provider) = &self.provider {
            provider.provider_id()
        } else if let Some(initial) = &self.initial_options {
            // Generate a stable ID from initial options
            use std::collections::hash_map::DefaultHasher;
            use std::hash::{Hash, Hasher};

            let mut hasher = DefaultHasher::new();
            let mut keys: Vec<_> = initial.keys().collect();
            keys.sort();
            for key in keys {
                key.hash(&mut hasher);
                initial.get(key).hash(&mut hasher);
            }
            format!("static_options_{:x}", hasher.finish())
        } else {
            "empty_accessor".to_string()
        }
    }

    /// Resolve this accessor for a single base path scope.
    ///
    /// Storage options may carry base-scoped entries (`base_<id>.<key>`) that
    /// apply only to one registered base path. The returned accessor resolves
    /// options for `base_id`: entries scoped to that base overlay the unscoped
    /// defaults, and all other scoped entries are dropped. `None` resolves the
    /// default scope used for the primary dataset base.
    ///
    /// A static accessor whose options contain no base-scoped entries is
    /// returned unchanged, preserving accessor identity (and thus object store
    /// registry cache keys). A provider-backed accessor is always wrapped
    /// through [`BaseScopedStorageOptionsProvider`] — fetched options may vend
    /// base-scoped entries at any refresh, even when the initial options carry
    /// none — so refreshed options are re-resolved for the scope on every
    /// fetch. Accessors already produced by this method are returned unchanged.
    pub fn scoped_to_base(self: &Arc<Self>, base_id: Option<u32>) -> Arc<Self> {
        if self.scope_resolved {
            return self.clone();
        }
        if self.has_provider() {
            let provider = Arc::new(BaseScopedStorageOptionsProvider::new(self.clone(), base_id));
            let mut scoped = match self.initial_storage_options() {
                Some(initial) => Self::with_initial_and_provider(
                    resolve_base_scoped_options(initial, base_id),
                    provider,
                ),
                None => Self::with_provider(provider),
            };
            scoped.scope_resolved = true;
            Arc::new(scoped)
        } else {
            match self.initial_storage_options() {
                Some(initial) if has_base_scoped_options(initial) => {
                    let mut scoped =
                        Self::with_static_options(resolve_base_scoped_options(initial, base_id));
                    scoped.scope_resolved = true;
                    Arc::new(scoped)
                }
                // Static options never change, so there is nothing to scope.
                _ => self.clone(),
            }
        }
    }

    /// Check if this accessor has a dynamic provider
    pub fn has_provider(&self) -> bool {
        self.provider.is_some()
    }

    /// Get the refresh offset duration
    pub fn refresh_offset(&self) -> Duration {
        self.refresh_offset
    }

    /// Get the storage options provider, if any
    pub fn provider(&self) -> Option<&Arc<dyn StorageOptionsProvider>> {
        self.provider.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mock_instant::thread_local::MockClock;

    #[derive(Debug)]
    struct MockStorageOptionsProvider {
        call_count: Arc<RwLock<usize>>,
        expires_in_millis: Option<u64>,
    }

    impl MockStorageOptionsProvider {
        fn new(expires_in_millis: Option<u64>) -> Self {
            Self {
                call_count: Arc::new(RwLock::new(0)),
                expires_in_millis,
            }
        }

        async fn get_call_count(&self) -> usize {
            *self.call_count.read().await
        }
    }

    #[async_trait]
    impl StorageOptionsProvider for MockStorageOptionsProvider {
        async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
            let count = {
                let mut c = self.call_count.write().await;
                *c += 1;
                *c
            };

            let mut options = HashMap::from([
                ("aws_access_key_id".to_string(), format!("AKID_{}", count)),
                (
                    "aws_secret_access_key".to_string(),
                    format!("SECRET_{}", count),
                ),
                ("aws_session_token".to_string(), format!("TOKEN_{}", count)),
            ]);

            if let Some(expires_in) = self.expires_in_millis {
                let now_ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_millis() as u64;
                let expires_at = now_ms + expires_in;
                options.insert(EXPIRES_AT_MILLIS_KEY.to_string(), expires_at.to_string());
            }

            Ok(Some(options))
        }

        fn provider_id(&self) -> String {
            let ptr = Arc::as_ptr(&self.call_count) as usize;
            format!("MockStorageOptionsProvider {{ id: {} }}", ptr)
        }
    }

    #[tokio::test]
    async fn test_static_options_only() {
        let options = HashMap::from([
            ("key1".to_string(), "value1".to_string()),
            ("key2".to_string(), "value2".to_string()),
        ]);
        let accessor = StorageOptionsAccessor::with_static_options(options.clone());

        let result = accessor.get_storage_options().await.unwrap();
        assert_eq!(result.0, options);
        assert!(!accessor.has_provider());
        assert_eq!(accessor.initial_storage_options(), Some(&options));
    }

    #[tokio::test]
    async fn test_provider_only() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));
        let accessor = StorageOptionsAccessor::with_provider(mock_provider.clone());

        let result = accessor.get_storage_options().await.unwrap();
        assert!(result.0.contains_key("aws_access_key_id"));
        assert_eq!(result.0.get("aws_access_key_id").unwrap(), "AKID_1");
        assert!(accessor.has_provider());
        assert_eq!(accessor.initial_storage_options(), None);
        assert_eq!(mock_provider.get_call_count().await, 1);
    }

    #[tokio::test]
    async fn test_initial_and_provider_uses_initial_first() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;
        let expires_at = now_ms + 600_000; // 10 minutes from now

        let initial = HashMap::from([
            ("aws_access_key_id".to_string(), "INITIAL_KEY".to_string()),
            (
                "aws_secret_access_key".to_string(),
                "INITIAL_SECRET".to_string(),
            ),
            (EXPIRES_AT_MILLIS_KEY.to_string(), expires_at.to_string()),
        ]);
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        let accessor = StorageOptionsAccessor::with_initial_and_provider(
            initial.clone(),
            mock_provider.clone(),
        );

        // First call uses initial
        let result = accessor.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("aws_access_key_id").unwrap(), "INITIAL_KEY");
        assert_eq!(mock_provider.get_call_count().await, 0); // Provider not called yet
    }

    #[tokio::test]
    async fn test_caching_and_refresh() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000))); // 10 min expiry
        // Use with_initial_and_provider to set custom refresh_offset_millis (5 min = 300000ms)
        let now_ms = MockClock::system_time().as_millis() as u64;
        let expires_at = now_ms + 600_000; // 10 minutes from now
        let initial = HashMap::from([
            (EXPIRES_AT_MILLIS_KEY.to_string(), expires_at.to_string()),
            (REFRESH_OFFSET_MILLIS_KEY.to_string(), "300000".to_string()), // 5 min refresh offset
        ]);
        let accessor =
            StorageOptionsAccessor::with_initial_and_provider(initial, mock_provider.clone());

        // First call uses initial cached options
        let result = accessor.get_storage_options().await.unwrap();
        assert!(result.0.contains_key(EXPIRES_AT_MILLIS_KEY));
        assert_eq!(mock_provider.get_call_count().await, 0);

        // Advance time to 6 minutes - should trigger refresh (within 5 min refresh offset)
        MockClock::set_system_time(Duration::from_secs(100_000 + 360));
        let result = accessor.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("aws_access_key_id").unwrap(), "AKID_1");
        assert_eq!(mock_provider.get_call_count().await, 1);
    }

    #[tokio::test]
    async fn test_expired_initial_triggers_refresh() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let now_ms = MockClock::system_time().as_millis() as u64;
        let expired_time = now_ms - 1_000; // Expired 1 second ago

        let initial = HashMap::from([
            ("aws_access_key_id".to_string(), "EXPIRED_KEY".to_string()),
            (EXPIRES_AT_MILLIS_KEY.to_string(), expired_time.to_string()),
        ]);
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(600_000)));

        let accessor =
            StorageOptionsAccessor::with_initial_and_provider(initial, mock_provider.clone());

        // Should fetch from provider since initial is expired
        let result = accessor.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("aws_access_key_id").unwrap(), "AKID_1");
        assert_eq!(mock_provider.get_call_count().await, 1);
    }

    #[tokio::test]
    async fn test_accessor_id_with_provider() {
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(None));
        let accessor = StorageOptionsAccessor::with_provider(mock_provider);

        let id = accessor.accessor_id();
        assert!(id.starts_with("MockStorageOptionsProvider"));
    }

    #[tokio::test]
    async fn test_accessor_id_static() {
        let options = HashMap::from([("key".to_string(), "value".to_string())]);
        let accessor = StorageOptionsAccessor::with_static_options(options);

        let id = accessor.accessor_id();
        assert!(id.starts_with("static_options_"));
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        // Create a mock provider with far future expiration
        let mock_provider = Arc::new(MockStorageOptionsProvider::new(Some(9999999999999)));

        let accessor = Arc::new(StorageOptionsAccessor::with_provider(mock_provider.clone()));

        // Spawn 10 concurrent tasks that all try to get options at the same time
        let mut handles = vec![];
        for i in 0..10 {
            let acc = accessor.clone();
            let handle = tokio::spawn(async move {
                let result = acc.get_storage_options().await.unwrap();
                assert_eq!(result.0.get("aws_access_key_id").unwrap(), "AKID_1");
                i
            });
            handles.push(handle);
        }

        // Wait for all tasks to complete
        let results: Vec<_> = futures::future::join_all(handles)
            .await
            .into_iter()
            .map(|r| r.unwrap())
            .collect();

        // Verify all 10 tasks completed successfully
        assert_eq!(results.len(), 10);

        // The provider should have been called exactly once
        let call_count = mock_provider.get_call_count().await;
        assert_eq!(
            call_count, 1,
            "Provider should be called exactly once despite concurrent access"
        );
    }

    #[tokio::test]
    async fn test_no_expiration_never_refreshes() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        let mock_provider = Arc::new(MockStorageOptionsProvider::new(None)); // No expiration
        let accessor = StorageOptionsAccessor::with_provider(mock_provider.clone());

        // First call fetches
        accessor.get_storage_options().await.unwrap();
        assert_eq!(mock_provider.get_call_count().await, 1);

        // Advance time significantly
        MockClock::set_system_time(Duration::from_secs(200_000));

        // Should still use cached options
        accessor.get_storage_options().await.unwrap();
        assert_eq!(mock_provider.get_call_count().await, 1);
    }

    #[test]
    fn test_parse_base_scoped_key() {
        assert_eq!(
            parse_base_scoped_key("base_1.account_key"),
            Some((1, "account_key"))
        );
        assert_eq!(
            parse_base_scoped_key("base_12.headers.x-ms-version"),
            Some((12, "headers.x-ms-version"))
        );
        assert_eq!(parse_base_scoped_key("base_0.region"), Some((0, "region")));

        // Not base-scoped keys
        assert_eq!(parse_base_scoped_key("account_key"), None);
        assert_eq!(parse_base_scoped_key("base_url"), None);
        assert_eq!(parse_base_scoped_key("base_hot.account_key"), None);
        assert_eq!(parse_base_scoped_key("base_1x.account_key"), None);
        assert_eq!(parse_base_scoped_key("base_+1.account_key"), None);
        assert_eq!(parse_base_scoped_key("base_.account_key"), None);
        assert_eq!(parse_base_scoped_key("base_1."), None);
        assert_eq!(parse_base_scoped_key("base_1"), None);
        // Overflows u32
        assert_eq!(parse_base_scoped_key("base_4294967296.key"), None);
    }

    #[test]
    fn test_resolve_base_scoped_options() {
        let options = HashMap::from([
            ("region".to_string(), "us-east-1".to_string()),
            ("account_key".to_string(), "shared-key".to_string()),
            ("base_1.account_key".to_string(), "base1-key".to_string()),
            ("base_2.account_key".to_string(), "base2-key".to_string()),
            ("base_2.endpoint".to_string(), "http://b2".to_string()),
        ]);
        assert!(has_base_scoped_options(&options));

        let base1 = resolve_base_scoped_options(&options, Some(1));
        assert_eq!(
            base1,
            HashMap::from([
                ("region".to_string(), "us-east-1".to_string()),
                ("account_key".to_string(), "base1-key".to_string()),
            ])
        );

        let base2 = resolve_base_scoped_options(&options, Some(2));
        assert_eq!(
            base2,
            HashMap::from([
                ("region".to_string(), "us-east-1".to_string()),
                ("account_key".to_string(), "base2-key".to_string()),
                ("endpoint".to_string(), "http://b2".to_string()),
            ])
        );

        // A base without scoped entries inherits only the unscoped options
        let base3 = resolve_base_scoped_options(&options, Some(3));
        assert_eq!(
            base3,
            HashMap::from([
                ("region".to_string(), "us-east-1".to_string()),
                ("account_key".to_string(), "shared-key".to_string()),
            ])
        );

        // The default scope drops every scoped entry
        let default = resolve_base_scoped_options(&options, None);
        assert_eq!(default, base3);

        assert!(!has_base_scoped_options(&HashMap::from([(
            "account_key".to_string(),
            "shared-key".to_string()
        )])));
    }

    #[tokio::test]
    async fn test_scoped_to_base_identity_and_idempotency() {
        // Static accessors without scoped keys are returned unchanged.
        let accessor = Arc::new(StorageOptionsAccessor::with_static_options(HashMap::from(
            [("account_key".to_string(), "shared-key".to_string())],
        )));
        assert!(Arc::ptr_eq(&accessor.scoped_to_base(Some(1)), &accessor));
        assert!(Arc::ptr_eq(&accessor.scoped_to_base(None), &accessor));

        // Scoping an already-scoped accessor is a no-op (the registry choke
        // point re-applies the default scope to every params it sees).
        let scoped = Arc::new(StorageOptionsAccessor::with_static_options(HashMap::from(
            [
                ("account_key".to_string(), "shared-key".to_string()),
                ("base_1.account_key".to_string(), "base1-key".to_string()),
            ],
        )))
        .scoped_to_base(Some(1));
        assert!(Arc::ptr_eq(&scoped.scoped_to_base(None), &scoped));

        let provider_scoped = Arc::new(StorageOptionsAccessor::with_provider(Arc::new(
            MockStorageOptionsProvider::new(None),
        )))
        .scoped_to_base(Some(1));
        assert!(Arc::ptr_eq(
            &provider_scoped.scoped_to_base(None),
            &provider_scoped
        ));
    }

    #[tokio::test]
    async fn test_scoped_to_base_provider_only_resolves_vended_options() {
        MockClock::set_system_time(Duration::from_secs(100_000));

        // No initial options: scoped entries arrive only through the provider.
        let provider = Arc::new(MockBaseScopedVendingProvider {
            call_count: Arc::new(RwLock::new(0)),
            expires_in_millis: 600_000,
        });
        let parent = Arc::new(StorageOptionsAccessor::with_provider(provider.clone()));

        let base1 = parent.scoped_to_base(Some(1));
        assert!(!Arc::ptr_eq(&base1, &parent));
        let result = base1.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("account_key").unwrap(), "BASE1_1");
        assert!(!result.0.contains_key("base_1.account_key"));

        let default = parent.scoped_to_base(None);
        let result = default.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("account_key").unwrap(), "SHARED_1");
        assert!(!result.0.contains_key("base_1.account_key"));

        // Both scopes were served from one parent fetch.
        assert_eq!(*provider.call_count.read().await, 1);
    }

    #[tokio::test]
    async fn test_scoped_earlier_base_expiry_refreshes_parent() {
        MockClock::set_system_time(Duration::from_secs(100_000));
        let now_ms = MockClock::system_time().as_millis() as u64;

        // Base 1 credentials expire before the shared ones; the parent must
        // refresh when the earliest credential is due, or the scoped accessor
        // would keep re-resolving stale base-1 credentials from a still-
        // "valid" parent cache.
        let provider = Arc::new(MockBaseScopedVendingProvider {
            call_count: Arc::new(RwLock::new(0)),
            expires_in_millis: 600_000,
        });
        let initial = HashMap::from([
            ("account_key".to_string(), "SHARED_0".to_string()),
            ("base_1.account_key".to_string(), "BASE1_0".to_string()),
            (
                EXPIRES_AT_MILLIS_KEY.to_string(),
                (now_ms + 600_000).to_string(),
            ),
            (
                format!("base_1.{}", EXPIRES_AT_MILLIS_KEY),
                (now_ms + 120_000).to_string(),
            ),
        ]);
        let parent = Arc::new(StorageOptionsAccessor::with_initial_and_provider(
            initial,
            provider.clone(),
        ));

        let base1 = parent.scoped_to_base(Some(1));
        let result = base1.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("account_key").unwrap(), "BASE1_0");
        assert_eq!(*provider.call_count.read().await, 0);

        // Past the base-1 expiry but before the shared expiry: the parent's
        // effective expiry is the earlier one, so the refresh chain fetches
        // fresh credentials instead of re-serving BASE1_0.
        MockClock::set_system_time(Duration::from_secs(100_000 + 121));
        let result = base1.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("account_key").unwrap(), "BASE1_1");
        assert_eq!(*provider.call_count.read().await, 1);
    }

    #[tokio::test]
    async fn test_scoped_to_base_static() {
        let accessor = Arc::new(StorageOptionsAccessor::with_static_options(HashMap::from(
            [
                ("account_key".to_string(), "shared-key".to_string()),
                ("base_1.account_key".to_string(), "base1-key".to_string()),
            ],
        )));

        let base1 = accessor.scoped_to_base(Some(1));
        let result = base1.get_storage_options().await.unwrap();
        assert_eq!(
            result.0,
            HashMap::from([("account_key".to_string(), "base1-key".to_string())])
        );
        assert!(!base1.has_provider());

        let default = accessor.scoped_to_base(None);
        let result = default.get_storage_options().await.unwrap();
        assert_eq!(
            result.0,
            HashMap::from([("account_key".to_string(), "shared-key".to_string())])
        );

        // Scoped accessor ids are stable across derivations and distinct per scope
        assert_eq!(
            accessor.scoped_to_base(Some(1)).accessor_id(),
            base1.accessor_id()
        );
        assert_ne!(base1.accessor_id(), default.accessor_id());
        assert_ne!(base1.accessor_id(), accessor.accessor_id());
    }

    #[derive(Debug)]
    struct MockBaseScopedVendingProvider {
        call_count: Arc<RwLock<usize>>,
        expires_in_millis: u64,
    }

    #[async_trait]
    impl StorageOptionsProvider for MockBaseScopedVendingProvider {
        async fn fetch_storage_options(&self) -> Result<Option<HashMap<String, String>>> {
            let count = {
                let mut c = self.call_count.write().await;
                *c += 1;
                *c
            };
            let now_ms = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            Ok(Some(HashMap::from([
                ("account_key".to_string(), format!("SHARED_{}", count)),
                ("base_1.account_key".to_string(), format!("BASE1_{}", count)),
                (
                    EXPIRES_AT_MILLIS_KEY.to_string(),
                    (now_ms + self.expires_in_millis).to_string(),
                ),
            ])))
        }

        fn provider_id(&self) -> String {
            "MockBaseScopedVendingProvider".to_string()
        }
    }

    #[tokio::test]
    async fn test_scoped_to_base_refreshes_through_parent() {
        MockClock::set_system_time(Duration::from_secs(100_000));
        let now_ms = MockClock::system_time().as_millis() as u64;

        let provider = Arc::new(MockBaseScopedVendingProvider {
            call_count: Arc::new(RwLock::new(0)),
            expires_in_millis: 600_000,
        });
        let initial = HashMap::from([
            ("account_key".to_string(), "SHARED_0".to_string()),
            ("base_1.account_key".to_string(), "BASE1_0".to_string()),
            (
                EXPIRES_AT_MILLIS_KEY.to_string(),
                (now_ms + 600_000).to_string(),
            ),
        ]);
        let parent = Arc::new(StorageOptionsAccessor::with_initial_and_provider(
            initial,
            provider.clone(),
        ));

        let base1 = parent.scoped_to_base(Some(1));
        let default = parent.scoped_to_base(None);
        assert!(base1.has_provider());

        // Initial options are resolved per scope without fetching
        let result = base1.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("account_key").unwrap(), "BASE1_0");
        assert!(!result.0.contains_key("base_1.account_key"));
        let result = default.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("account_key").unwrap(), "SHARED_0");
        assert_eq!(*provider.call_count.read().await, 0);

        // Expire the vended options; the scoped accessor refreshes through the
        // parent and re-resolves the refreshed options for its scope.
        MockClock::set_system_time(Duration::from_secs(100_000 + 601));
        let result = base1.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("account_key").unwrap(), "BASE1_1");
        assert_eq!(*provider.call_count.read().await, 1);

        // The parent refresh is shared: other scopes see it without refetching
        let result = default.get_storage_options().await.unwrap();
        assert_eq!(result.0.get("account_key").unwrap(), "SHARED_1");
        assert_eq!(*provider.call_count.read().await, 1);
    }

    #[tokio::test]
    async fn test_scoped_forced_refresh_reaches_origin_provider() {
        MockClock::set_system_time(Duration::from_secs(100_000));
        let now_ms = MockClock::system_time().as_millis() as u64;

        let provider = Arc::new(MockBaseScopedVendingProvider {
            call_count: Arc::new(RwLock::new(0)),
            expires_in_millis: 600_000,
        });
        let initial = HashMap::from([
            ("account_key".to_string(), "SHARED_0".to_string()),
            ("base_1.account_key".to_string(), "BASE1_0".to_string()),
            (
                EXPIRES_AT_MILLIS_KEY.to_string(),
                (now_ms + 600_000).to_string(),
            ),
        ]);
        let parent = Arc::new(StorageOptionsAccessor::with_initial_and_provider(
            initial,
            provider.clone(),
        ));
        let base1 = parent.scoped_to_base(Some(1));

        // A forced refresh must reach the origin provider even though both the
        // scoped and the parent caches are still valid.
        let result = base1.refresh_storage_options().await.unwrap();
        assert_eq!(result.0.get("account_key").unwrap(), "BASE1_1");
        assert_eq!(*provider.call_count.read().await, 1);
    }
}
