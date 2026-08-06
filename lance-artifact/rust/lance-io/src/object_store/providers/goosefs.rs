// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::sync::Arc;

use object_store_opendal::OpendalStore;
use opendal::{Operator, services::GooseFs};
use url::Url;

use crate::object_store::{
    DEFAULT_CLOUD_BLOCK_SIZE, DEFAULT_CLOUD_IO_PARALLELISM, DEFAULT_MAX_IOP_SIZE, ObjectStore,
    ObjectStoreParams, ObjectStoreProvider, StorageOptions,
};
use lance_core::error::{Error, Result};

/// Default GooseFS Master gRPC port.
const DEFAULT_GOOSEFS_PORT: u16 = 9200;

/// GooseFS object store provider.
///
/// Uses OpenDAL's GooseFs service to access GooseFS via gRPC.
/// URL format: `goosefs://host:port/path`
///
/// Where:
/// - `host:port` is the GooseFS Master address (default port: 9200)
/// - `/path` is the filesystem path within GooseFS
///
/// Path handling model (S3-style):
/// - The OpenDAL `root` is fixed to `/` (or a user-supplied cluster-wide base)
///   so that a single `Operator` can serve every dataset under the same
///   master. This keeps the `ObjectStoreRegistry` cache correct: two URLs
///   like `goosefs://host:9200/a.lance` and `goosefs://host:9200/b.lance`
///   share one store and each request carries its own object key.
/// - Path extraction relies on the default [`ObjectStoreProvider::extract_path`]
///   implementation, which returns the URL path (percent-decoded) as the key
///   passed to `ObjectStore::get`, `put`, etc. — mirroring how `s3://bucket/k`
///   yields key `k`.
///
/// Supported configuration keys (via `storage_options` or environment variables,
/// resolved with priority: `storage_options` > env var > URL authority > default):
///
/// | storage_options key       | env var                 | purpose                                                                                       |
/// |---------------------------|-------------------------|-----------------------------------------------------------------------------------------------|
/// | `goosefs_master_addr`     | `GOOSEFS_MASTER_ADDR`   | Master gRPC address, e.g. `host:9200`. Supports HA: `addr1:port,addr2:port`.                  |
/// | `goosefs_root`            | `GOOSEFS_ROOT`          | Cluster-wide OpenDAL root shared by all datasets under the same master. Defaults to `/`.      |
/// | `goosefs_write_type`      | `GOOSEFS_WRITE_TYPE`    | GooseFS write type (e.g. `MUST_CACHE`, `CACHE_THROUGH`, `THROUGH`, `ASYNC_THROUGH`).          |
/// | `goosefs_block_size`      | `GOOSEFS_BLOCK_SIZE`    | GooseFS block size (bytes). Distinct from Lance's own `block_size`.                           |
/// | `goosefs_chunk_size`      | `GOOSEFS_CHUNK_SIZE`    | GooseFS chunk size (bytes) used by the client.                                                |
/// | `goosefs_auth_type`       | `GOOSEFS_AUTH_TYPE`     | Authentication mode: `nosasl` or `simple`.                                                    |
/// | `goosefs_auth_username`   | `GOOSEFS_AUTH_USERNAME` | Username for `simple` auth mode.                                                              |
///
/// Note on `goosefs_root`: it is deliberately cluster-wide (not per-URL) so
/// that many datasets under the same master share a single cached `Operator`.
/// A custom root also participates in the `ObjectStoreRegistry` cache prefix,
/// so stores rooted at different subtrees do not collide.
#[derive(Default, Debug)]
pub struct GooseFsStoreProvider;

impl GooseFsStoreProvider {
    /// Resolve the GooseFS Master address from storage_options, environment, or URL.
    ///
    /// Priority:
    /// 1. `storage_options["goosefs_master_addr"]` (supports HA: "addr1:port,addr2:port")
    /// 2. `GOOSEFS_MASTER_ADDR` environment variable
    /// 3. URL authority (host:port from the URL)
    fn resolve_master_addr(url: &Url, storage_options: &StorageOptions) -> Result<String> {
        // 1. storage_options
        if let Some(addr) = storage_options
            .0
            .get("goosefs_master_addr")
            .filter(|v| !v.is_empty())
        {
            return Ok(addr.clone());
        }

        // 2. Environment variable
        if let Ok(addr) = std::env::var("GOOSEFS_MASTER_ADDR")
            && !addr.is_empty()
        {
            return Ok(addr);
        }

        // 3. URL authority
        let host = url.host_str().ok_or_else(|| {
            Error::invalid_input(
                "GooseFS URL must contain a master address (host), e.g. goosefs://host:port/path",
            )
        })?;

        let port = url.port().unwrap_or(DEFAULT_GOOSEFS_PORT);
        Ok(format!("{}:{}", host, port))
    }

    /// Resolve a storage option from storage_options or environment variable.
    fn resolve_option(
        storage_options: &StorageOptions,
        option_key: &str,
        env_key: &str,
    ) -> Option<String> {
        storage_options
            .0
            .get(option_key)
            .cloned()
            .or_else(|| std::env::var(env_key).ok())
            .filter(|v| !v.is_empty())
    }

    /// Resolve the OpenDAL `root` for this Operator. See the file-level docs on
    /// [`GooseFsStoreProvider`] for the semantics of `goosefs_root`.
    fn resolve_root(storage_options: &StorageOptions) -> String {
        Self::resolve_option(storage_options, "goosefs_root", "GOOSEFS_ROOT")
            .unwrap_or_else(|| "/".to_string())
    }
}

#[async_trait::async_trait]
impl ObjectStoreProvider for GooseFsStoreProvider {
    async fn new_store(&self, base_path: Url, params: &ObjectStoreParams) -> Result<ObjectStore> {
        let block_size = params.block_size.unwrap_or(DEFAULT_CLOUD_BLOCK_SIZE);
        let storage_options = StorageOptions(params.storage_options().cloned().unwrap_or_default());

        // Resolve master address
        let master_addr = Self::resolve_master_addr(&base_path, &storage_options)?;

        // Resolve a stable cluster-wide root. The URL path is *not* used here
        // because it varies per dataset; per-request keys are supplied by
        // `extract_path` instead.
        let root = Self::resolve_root(&storage_options);

        // Build OpenDAL config map
        let mut config_map: HashMap<String, String> = HashMap::new();
        config_map.insert("master_addr".to_string(), master_addr);
        config_map.insert("root".to_string(), root);

        // Optional: write_type
        if let Some(wt) =
            Self::resolve_option(&storage_options, "goosefs_write_type", "GOOSEFS_WRITE_TYPE")
        {
            config_map.insert("write_type".to_string(), wt);
        }

        // Optional: block_size (for GooseFS, not Lance block_size)
        if let Some(bs) =
            Self::resolve_option(&storage_options, "goosefs_block_size", "GOOSEFS_BLOCK_SIZE")
        {
            config_map.insert("block_size".to_string(), bs);
        }

        // Optional: chunk_size
        if let Some(cs) =
            Self::resolve_option(&storage_options, "goosefs_chunk_size", "GOOSEFS_CHUNK_SIZE")
        {
            config_map.insert("chunk_size".to_string(), cs);
        }

        // Optional: auth_type (nosasl / simple)
        if let Some(at) =
            Self::resolve_option(&storage_options, "goosefs_auth_type", "GOOSEFS_AUTH_TYPE")
        {
            config_map.insert("auth_type".to_string(), at);
        }

        // Optional: auth_username (used in SIMPLE auth mode)
        if let Some(au) = Self::resolve_option(
            &storage_options,
            "goosefs_auth_username",
            "GOOSEFS_AUTH_USERNAME",
        ) {
            config_map.insert("auth_username".to_string(), au);
        }

        // Create OpenDAL Operator with GooseFS service
        let operator = Operator::from_iter::<GooseFs>(config_map).map_err(|e| {
            Error::invalid_input(format!("Failed to create GooseFS operator: {:?}", e))
        })?;

        // Wrap as object_store::ObjectStore via OpendalStore bridge
        let opendal_store = Arc::new(OpendalStore::new(operator));

        Ok(ObjectStore {
            scheme: "goosefs".to_string(),
            inner: opendal_store,
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts: params.use_constant_size_upload_parts,
            list_is_lexically_ordered: params.list_is_lexically_ordered.unwrap_or(false),
            io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
            download_retry_count: storage_options.download_retry_count(),
            io_tracker: Default::default(),
            store_prefix: self
                .calculate_object_store_prefix(&base_path, params.storage_options())?,
        })
    }

    // `extract_path` uses the default `ObjectStoreProvider` trait implementation:
    // it percent-decodes the URL path and returns it as the object key, exactly
    // like S3 does for `s3://bucket/key`. Overriding it here would only
    // duplicate that behavior. See the file-level doc comment above for the
    // full path-handling model.

    /// Calculate the object store prefix used as the registry cache key.
    ///
    /// Format: `goosefs$host:port`. Because the OpenDAL root is now cluster-
    /// wide (not per-URL), all datasets under the same master intentionally
    /// share the same cached [`ObjectStore`]; the URL path is disambiguated
    /// by [`Self::extract_path`] on each request. This is analogous to how
    /// two `s3://bucket/a` and `s3://bucket/b` URLs share one store.
    fn calculate_object_store_prefix(
        &self,
        url: &Url,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Result<String> {
        // If a custom `goosefs_root` is provided, include it in the prefix so
        // that stores built with different roots don't accidentally collide.
        let opts = StorageOptions(storage_options.cloned().unwrap_or_default());
        let root = Self::resolve_root(&opts);
        if root == "/" {
            Ok(format!("{}${}", url.scheme(), url.authority()))
        } else {
            Ok(format!("{}${}#{}", url.scheme(), url.authority(), root))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_goosefs_extract_path_basic() {
        let provider = GooseFsStoreProvider;
        let url = Url::parse("goosefs://10.0.0.1:9200/data/embeddings.lance").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.to_string(), "data/embeddings.lance");
    }

    #[test]
    fn test_goosefs_extract_path_root() {
        let provider = GooseFsStoreProvider;
        let url = Url::parse("goosefs://10.0.0.1:9200/").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.to_string(), "");
    }

    #[test]
    fn test_goosefs_extract_path_deep() {
        let provider = GooseFsStoreProvider;
        let url = Url::parse("goosefs://master:9200/a/b/c/d.lance").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.to_string(), "a/b/c/d.lance");
    }

    #[test]
    fn test_goosefs_extract_path_percent_decoded() {
        // The URL contains a percent-encoded space; extract_path must decode
        // it once so the ObjectStore layer does not double-encode later.
        let provider = GooseFsStoreProvider;
        let url = Url::parse("goosefs://master:9200/dir/with%20space/f.lance").unwrap();
        let path = provider.extract_path(&url).unwrap();
        assert_eq!(path.to_string(), "dir/with space/f.lance");
    }

    #[test]
    fn test_calculate_object_store_prefix_default_root() {
        let provider = GooseFsStoreProvider;
        let url = Url::parse("goosefs://10.0.0.1:9200/data").unwrap();
        let prefix = provider.calculate_object_store_prefix(&url, None).unwrap();
        assert_eq!(prefix, "goosefs$10.0.0.1:9200");
    }

    #[test]
    fn test_calculate_object_store_prefix_with_hostname() {
        let provider = GooseFsStoreProvider;
        let url = Url::parse("goosefs://myhost:9200/data").unwrap();
        let prefix = provider.calculate_object_store_prefix(&url, None).unwrap();
        assert_eq!(prefix, "goosefs$myhost:9200");
    }

    /// Regression test: two URLs pointing at different datasets under the
    /// same master must produce the *same* cache prefix so they share one
    /// Operator, and correctness must come from `extract_path` returning
    /// distinct keys — never from a per-URL root baked into the prefix.
    #[test]
    fn test_prefix_shared_across_datasets_same_master() {
        let provider = GooseFsStoreProvider;
        let url_a = Url::parse("goosefs://10.0.0.1:9200/repro/a.lance").unwrap();
        let url_b = Url::parse("goosefs://10.0.0.1:9200/repro/b.lance").unwrap();

        let pa = provider
            .calculate_object_store_prefix(&url_a, None)
            .unwrap();
        let pb = provider
            .calculate_object_store_prefix(&url_b, None)
            .unwrap();
        assert_eq!(pa, pb, "same master must share one cache prefix");

        // Extracted keys must differ so the shared Operator can route
        // requests to the correct dataset.
        assert_ne!(
            provider.extract_path(&url_a).unwrap(),
            provider.extract_path(&url_b).unwrap(),
            "distinct URLs must yield distinct object keys",
        );
    }

    /// Different masters must never share a cache entry.
    #[test]
    fn test_prefix_isolated_across_masters() {
        let provider = GooseFsStoreProvider;
        let u1 = Url::parse("goosefs://host-a:9200/x.lance").unwrap();
        let u2 = Url::parse("goosefs://host-b:9200/x.lance").unwrap();
        assert_ne!(
            provider.calculate_object_store_prefix(&u1, None).unwrap(),
            provider.calculate_object_store_prefix(&u2, None).unwrap(),
        );
    }

    /// A user-supplied `goosefs_root` participates in the cache prefix so
    /// stores rooted at different subtrees don't collide.
    #[test]
    fn test_prefix_includes_custom_root() {
        let provider = GooseFsStoreProvider;
        let url = Url::parse("goosefs://host:9200/x.lance").unwrap();

        let default_prefix = provider.calculate_object_store_prefix(&url, None).unwrap();
        let custom_opts: HashMap<String, String> =
            HashMap::from([("goosefs_root".to_string(), "/tenant-a".to_string())]);
        let custom_prefix = provider
            .calculate_object_store_prefix(&url, Some(&custom_opts))
            .unwrap();

        assert_eq!(default_prefix, "goosefs$host:9200");
        assert_eq!(custom_prefix, "goosefs$host:9200#/tenant-a");
        assert_ne!(default_prefix, custom_prefix);
    }

    #[test]
    fn test_resolve_master_addr_from_url() {
        let url = Url::parse("goosefs://10.0.0.1:9200/data").unwrap();
        let storage_options = StorageOptions(HashMap::new());
        let addr = GooseFsStoreProvider::resolve_master_addr(&url, &storage_options).unwrap();
        assert_eq!(addr, "10.0.0.1:9200");
    }

    #[test]
    fn test_resolve_master_addr_default_port() {
        let url = Url::parse("goosefs://10.0.0.1/data").unwrap();
        let storage_options = StorageOptions(HashMap::new());
        let addr = GooseFsStoreProvider::resolve_master_addr(&url, &storage_options).unwrap();
        assert_eq!(addr, "10.0.0.1:9200");
    }

    #[test]
    fn test_resolve_master_addr_from_storage_options() {
        let url = Url::parse("goosefs://10.0.0.1:9200/data").unwrap();
        let storage_options = StorageOptions(HashMap::from([(
            "goosefs_master_addr".to_string(),
            "10.0.0.2:9200,10.0.0.3:9200".to_string(),
        )]));
        let addr = GooseFsStoreProvider::resolve_master_addr(&url, &storage_options).unwrap();
        assert_eq!(addr, "10.0.0.2:9200,10.0.0.3:9200");
    }

    #[test]
    fn test_resolve_root_defaults_to_slash() {
        let opts = StorageOptions(HashMap::new());
        assert_eq!(GooseFsStoreProvider::resolve_root(&opts), "/");
    }

    #[test]
    fn test_resolve_root_from_storage_options() {
        let opts = StorageOptions(HashMap::from([(
            "goosefs_root".to_string(),
            "/tenant-a".to_string(),
        )]));
        assert_eq!(GooseFsStoreProvider::resolve_root(&opts), "/tenant-a");
    }
}
