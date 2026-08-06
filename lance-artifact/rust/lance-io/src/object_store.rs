// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors
// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Extend [object_store::ObjectStore] functionalities

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Range;
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use futures::{FutureExt, Stream};
use futures::{StreamExt, TryStreamExt, future, stream::BoxStream};
use lance_core::deepsize::DeepSizeOf;
use lance_core::error::LanceOptionExt;
use lance_core::utils::parse::str_is_truthy;
use list_retry::ListRetryStream;
use object_store::DynObjectStore;
use object_store::ObjectStoreExt as OSObjectStoreExt;
#[cfg(feature = "aws")]
use object_store::aws::AwsCredentialProvider;
#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
use object_store::{ClientOptions, HeaderMap, HeaderValue};
use object_store::{ListResult, ObjectMeta, ObjectStore as OSObjectStore, path::Path};
use providers::local::FileStoreProvider;
use providers::memory::MemoryStoreProvider;
use tokio::io::AsyncWriteExt;
use url::Url;

use super::local::LocalObjectReader;
#[cfg(target_os = "linux")]
use crate::uring::{UringCurrentThreadReader, UringReader};
#[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
pub(crate) mod dynamic_credentials;
#[cfg(any(
    feature = "aws",
    feature = "oss",
    feature = "huggingface",
    feature = "tos"
))]
pub(crate) mod dynamic_opendal;
mod list_retry;
#[cfg(feature = "metrics")]
pub mod metrics;
pub mod providers;
pub mod storage_options;
#[cfg(test)]
pub(crate) mod test_utils;
pub mod throttle;
mod tracing;
use crate::object_reader::SmallReader;
use crate::object_writer::{LocalWriter, WriteResult};
use crate::traits::{WriteExt, Writer};
use crate::utils::tracking_store::{IOTracker, IoStats};
use crate::{object_reader::CloudObjectReader, object_writer::ObjectWriter, traits::Reader};
use lance_core::{Error, Result};

// Local disks tend to do fine with a few threads
// Note: the number of threads here also impacts the number of files
// we need to read in some situations.  So keeping this at 8 keeps the
// RAM on our scanner down.
pub const DEFAULT_LOCAL_IO_PARALLELISM: usize = 8;
// Cloud disks often need many many threads to saturate the network
pub const DEFAULT_CLOUD_IO_PARALLELISM: usize = 64;

const DEFAULT_LOCAL_BLOCK_SIZE: usize = 4 * 1024; // 4KB block size
#[cfg(any(
    feature = "aws",
    feature = "gcp",
    feature = "azure",
    feature = "oss",
    feature = "tencent",
    feature = "huggingface",
    feature = "tos",
    feature = "goosefs",
))]
const DEFAULT_CLOUD_BLOCK_SIZE: usize = 64 * 1024; // 64KB block size

pub static DEFAULT_MAX_IOP_SIZE: std::sync::LazyLock<u64> = std::sync::LazyLock::new(|| {
    std::env::var("LANCE_MAX_IOP_SIZE")
        .map(|val| val.parse().unwrap())
        .unwrap_or(16 * 1024 * 1024)
});

pub const DEFAULT_DOWNLOAD_RETRY_COUNT: usize = 3;

pub use providers::{ObjectStoreProvider, ObjectStoreRegistry};
pub use storage_options::{
    BASE_SCOPED_OPTION_PREFIX, BaseScopedStorageOptionsProvider, EXPIRES_AT_MILLIS_KEY,
    LanceNamespaceStorageOptionsProvider, REFRESH_OFFSET_MILLIS_KEY, StorageOptionsAccessor,
    StorageOptionsProvider, has_base_scoped_options, parse_base_scoped_key,
    resolve_base_scoped_options,
};

#[async_trait]
pub trait ObjectStoreExt {
    /// Returns true if the file exists.
    async fn exists(&self, path: &Path) -> Result<bool>;

    /// Read all files (start from base directory) recursively
    ///
    /// unmodified_since can be specified to only return files that have not been modified since the given time.
    fn read_dir_all<'a, 'b>(
        &'a self,
        dir_path: impl Into<&'b Path> + Send,
        unmodified_since: Option<DateTime<Utc>>,
    ) -> BoxStream<'a, Result<ObjectMeta>>;
}

#[async_trait]
impl<O: OSObjectStore + ?Sized> ObjectStoreExt for O {
    fn read_dir_all<'a, 'b>(
        &'a self,
        dir_path: impl Into<&'b Path> + Send,
        unmodified_since: Option<DateTime<Utc>>,
    ) -> BoxStream<'a, Result<ObjectMeta>> {
        let output = self.list(Some(dir_path.into())).map_err(|e| e.into());
        if let Some(unmodified_since_val) = unmodified_since {
            output
                .try_filter(move |file| future::ready(file.last_modified <= unmodified_since_val))
                .boxed()
        } else {
            output.boxed()
        }
    }

    async fn exists(&self, path: &Path) -> Result<bool> {
        match self.head(path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { path: _, source: _ }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }
}

/// Wraps [ObjectStore](object_store::ObjectStore)
#[derive(Debug, Clone)]
pub struct ObjectStore {
    // Inner object store
    pub inner: Arc<dyn OSObjectStore>,
    scheme: String,
    block_size: usize,
    max_iop_size: u64,
    /// Whether to use constant size upload parts for multipart uploads. This
    /// is only necessary for Cloudflare R2.
    pub use_constant_size_upload_parts: bool,
    /// Whether we can assume that the list of files is lexically ordered. This
    /// is true for object stores, but not for local filesystems.
    pub list_is_lexically_ordered: bool,
    io_parallelism: usize,
    /// Number of times to retry a failed download
    download_retry_count: usize,
    /// IO tracker for monitoring read/write operations
    io_tracker: IOTracker,
    /// The datastore prefix that uniquely identifies this object store. It encodes information
    /// which usually cannot be found in the URL such as Azure account name. The prefix plus the
    /// path uniquely identifies any object inside the store.
    pub store_prefix: String,
}

impl DeepSizeOf for ObjectStore {
    fn deep_size_of_children(&self, context: &mut lance_core::deepsize::Context) -> usize {
        // We aren't counting `inner` here which is problematic but an ObjectStore
        // shouldn't be too big.  The only exception might be the write cache but, if
        // the writer cache has data, it means we're using it somewhere else that isn't
        // a cache and so that doesn't really count.
        self.scheme.deep_size_of_children(context) + self.block_size.deep_size_of_children(context)
    }
}

impl std::fmt::Display for ObjectStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ObjectStore({})", self.scheme)
    }
}

pub trait WrappingObjectStore: std::fmt::Debug + Send + Sync {
    /// Wrap an object store with additional functionality
    ///
    /// The store_prefix is a string which uniquely identifies the object
    /// store being wrapped.
    fn wrap(&self, store_prefix: &str, original: Arc<dyn OSObjectStore>) -> Arc<dyn OSObjectStore>;
}

#[derive(Debug, Clone)]
pub struct ChainedWrappingObjectStore {
    wrappers: Vec<Arc<dyn WrappingObjectStore>>,
}

impl ChainedWrappingObjectStore {
    pub fn new(wrappers: Vec<Arc<dyn WrappingObjectStore>>) -> Self {
        Self { wrappers }
    }

    pub fn add_wrapper(&mut self, wrapper: Arc<dyn WrappingObjectStore>) {
        self.wrappers.push(wrapper);
    }
}

impl WrappingObjectStore for ChainedWrappingObjectStore {
    fn wrap(&self, store_prefix: &str, original: Arc<dyn OSObjectStore>) -> Arc<dyn OSObjectStore> {
        self.wrappers
            .iter()
            .fold(original, |acc, wrapper| wrapper.wrap(store_prefix, acc))
    }
}

/// Parameters to create an [ObjectStore]
///
#[derive(Debug, Clone)]
pub struct ObjectStoreParams {
    pub block_size: Option<usize>,
    #[deprecated(note = "Implement an ObjectStoreProvider instead")]
    pub object_store: Option<(Arc<DynObjectStore>, Url)>,
    /// Refresh offset for AWS credentials when using the legacy AWS credentials path.
    /// For StorageOptionsAccessor, use `refresh_offset_millis` storage option instead.
    pub s3_credentials_refresh_offset: Duration,
    #[cfg(feature = "aws")]
    pub aws_credentials: Option<AwsCredentialProvider>,
    pub object_store_wrapper: Option<Arc<dyn WrappingObjectStore>>,
    /// Unified storage options accessor with caching and automatic refresh
    ///
    /// Provides storage options and optionally a dynamic provider for automatic
    /// credential refresh. Use `StorageOptionsAccessor::with_static_options()` for static
    /// options or `StorageOptionsAccessor::with_initial_and_provider()` for dynamic refresh.
    pub storage_options_accessor: Option<Arc<StorageOptionsAccessor>>,
    /// Use constant size upload parts for multipart uploads. Only necessary
    /// for Cloudflare R2, which doesn't support variable size parts. When this
    /// is false, max upload size is 2.5TB. When this is true, the max size is
    /// 50GB.
    pub use_constant_size_upload_parts: bool,
    pub list_is_lexically_ordered: Option<bool>,
}

impl Default for ObjectStoreParams {
    fn default() -> Self {
        #[allow(deprecated)]
        Self {
            object_store: None,
            block_size: None,
            s3_credentials_refresh_offset: Duration::from_secs(60),
            #[cfg(feature = "aws")]
            aws_credentials: None,
            object_store_wrapper: None,
            storage_options_accessor: None,
            use_constant_size_upload_parts: false,
            list_is_lexically_ordered: None,
        }
    }
}

impl ObjectStoreParams {
    /// Get the StorageOptionsAccessor from the params
    pub fn get_accessor(&self) -> Option<Arc<StorageOptionsAccessor>> {
        self.storage_options_accessor.clone()
    }

    /// Get storage options from the accessor, if any
    ///
    /// Returns the initial storage options from the accessor without triggering refresh.
    pub fn storage_options(&self) -> Option<&HashMap<String, String>> {
        self.storage_options_accessor
            .as_ref()
            .and_then(|a| a.initial_storage_options())
    }

    /// Resolve these params for a single base path scope.
    ///
    /// Storage options may carry base-scoped entries (`base_<id>.<key>`) that
    /// apply only to one registered base path; see
    /// [`StorageOptionsAccessor::scoped_to_base`]. Returns the params unchanged
    /// when the storage options contain no base-scoped entries.
    pub fn scoped_to_base(&self, base_id: Option<u32>) -> Cow<'_, Self> {
        let Some(accessor) = &self.storage_options_accessor else {
            return Cow::Borrowed(self);
        };
        let scoped = accessor.scoped_to_base(base_id);
        if Arc::ptr_eq(&scoped, accessor) {
            Cow::Borrowed(self)
        } else {
            Cow::Owned(Self {
                storage_options_accessor: Some(scoped),
                ..self.clone()
            })
        }
    }
}

fn wrapper_allocation_ptr(wrapper: &Arc<dyn WrappingObjectStore>) -> *const () {
    // Trait object pointers include vtable metadata, which is not stable across codegen units.
    // Cache identity must follow the Arc allocation instead.
    Arc::as_ptr(wrapper) as *const ()
}

// We implement hash for caching
impl std::hash::Hash for ObjectStoreParams {
    #[allow(deprecated)]
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // For hashing, we use pointer values for ObjectStore, S3 credentials, wrapper
        self.block_size.hash(state);
        if let Some((store, url)) = &self.object_store {
            Arc::as_ptr(store).hash(state);
            url.hash(state);
        }
        self.s3_credentials_refresh_offset.hash(state);
        #[cfg(feature = "aws")]
        if let Some(aws_credentials) = &self.aws_credentials {
            Arc::as_ptr(aws_credentials).hash(state);
        }
        if let Some(wrapper) = &self.object_store_wrapper {
            wrapper_allocation_ptr(wrapper).hash(state);
        }
        if let Some(accessor) = &self.storage_options_accessor {
            accessor.accessor_id().hash(state);
        }
        self.use_constant_size_upload_parts.hash(state);
        self.list_is_lexically_ordered.hash(state);
    }
}

// We implement eq for caching
impl Eq for ObjectStoreParams {}
impl PartialEq for ObjectStoreParams {
    #[allow(deprecated)]
    fn eq(&self, other: &Self) -> bool {
        #[cfg(feature = "aws")]
        if self.aws_credentials.is_some() != other.aws_credentials.is_some() {
            return false;
        }

        // For equality, we use pointer comparison for ObjectStore, S3 credentials, wrapper
        // For accessor, we use accessor_id() for semantic equality
        self.block_size == other.block_size
            && self
                .object_store
                .as_ref()
                .map(|(store, url)| (Arc::as_ptr(store), url))
                == other
                    .object_store
                    .as_ref()
                    .map(|(store, url)| (Arc::as_ptr(store), url))
            && self.s3_credentials_refresh_offset == other.s3_credentials_refresh_offset
            && self
                .object_store_wrapper
                .as_ref()
                .map(wrapper_allocation_ptr)
                == other
                    .object_store_wrapper
                    .as_ref()
                    .map(wrapper_allocation_ptr)
            && self
                .storage_options_accessor
                .as_ref()
                .map(|a| a.accessor_id())
                == other
                    .storage_options_accessor
                    .as_ref()
                    .map(|a| a.accessor_id())
            && self.use_constant_size_upload_parts == other.use_constant_size_upload_parts
            && self.list_is_lexically_ordered == other.list_is_lexically_ordered
    }
}

/// Convert a URI string or local path to a URL
///
/// This function handles both proper URIs (with schemes like `file://`, `s3://`, etc.)
/// and plain local filesystem paths. On Windows, it correctly handles drive letters
/// that might be parsed as URL schemes.
///
/// # Examples
///
/// ```
/// # use lance_io::object_store::uri_to_url;
/// // URIs are preserved
/// let url = uri_to_url("s3://bucket/path").unwrap();
/// assert_eq!(url.scheme(), "s3");
///
/// // Local paths are converted to file:// URIs
/// # #[cfg(unix)]
/// let url = uri_to_url("/tmp/data").unwrap();
/// # #[cfg(unix)]
/// assert_eq!(url.scheme(), "file");
/// ```
pub fn uri_to_url(uri: &str) -> Result<Url> {
    match Url::parse(uri) {
        Ok(url) if url.scheme().len() == 1 && cfg!(windows) => {
            // On Windows, the drive is parsed as a scheme
            local_path_to_url(uri)
        }
        Ok(url) => Ok(url),
        Err(_) => local_path_to_url(uri),
    }
}

fn expand_path(str_path: impl AsRef<str>) -> Result<std::path::PathBuf> {
    let str_path = str_path.as_ref();
    let expanded = expand_tilde_path(str_path).unwrap_or_else(|| str_path.into());

    let mut expanded_path = path_abs::PathAbs::new(expanded)
        .unwrap()
        .as_path()
        .to_path_buf();
    // path_abs::PathAbs::new(".") returns an empty string.
    if let Some(s) = expanded_path.as_path().to_str()
        && s.is_empty()
    {
        expanded_path = std::env::current_dir()?;
    }

    Ok(expanded_path)
}

fn expand_tilde_path(path: &str) -> Option<std::path::PathBuf> {
    let home_dir = std::env::home_dir()?;
    if path == "~" {
        return Some(home_dir);
    }
    if let Some(stripped) = path.strip_prefix("~/") {
        return Some(home_dir.join(stripped));
    }
    #[cfg(windows)]
    if let Some(stripped) = path.strip_prefix("~\\") {
        return Some(home_dir.join(stripped));
    }

    None
}

fn local_path_to_url(str_path: &str) -> Result<Url> {
    let expanded_path = expand_path(str_path)?;

    Url::from_directory_path(expanded_path).map_err(|_| {
        Error::invalid_input_source(format!("Invalid table location: '{}'", str_path).into())
    })
}

#[cfg(feature = "huggingface")]
fn parse_hf_repo_id(url: &Url) -> Result<String> {
    // Accept forms with repo type prefix (models/datasets/spaces) or legacy without.
    let mut segments: Vec<String> = Vec::new();
    if let Some(host) = url.host_str() {
        segments.push(host.to_string());
    }
    segments.extend(
        url.path()
            .trim_start_matches('/')
            .split('/')
            .map(|s| s.to_string()),
    );

    if segments.len() < 2 {
        return Err(Error::invalid_input(
            "Huggingface URL must contain at least owner and repo",
        ));
    }

    let repo_type_candidates = ["models", "datasets", "spaces"];
    let (owner, repo_with_rev) = if repo_type_candidates.contains(&segments[0].as_str()) {
        if segments.len() < 3 {
            return Err(Error::invalid_input(
                "Huggingface URL missing owner/repo after repo type",
            ));
        }
        (segments[1].as_str(), segments[2].as_str())
    } else {
        (segments[0].as_str(), segments[1].as_str())
    };

    let repo = repo_with_rev
        .split_once('@')
        .map(|(r, _)| r)
        .unwrap_or(repo_with_rev);
    Ok(format!("{owner}/{repo}"))
}

impl ObjectStore {
    /// Parse from a string URI.
    ///
    /// Returns the ObjectStore instance and the absolute path to the object.
    ///
    /// This uses the default [ObjectStoreRegistry] to find the object store. To
    /// allow for potential re-use of object store instances, it's recommended to
    /// create a shared [ObjectStoreRegistry] and pass that to [Self::from_uri_and_params].
    pub async fn from_uri(uri: &str) -> Result<(Arc<Self>, Path)> {
        let registry = Arc::new(ObjectStoreRegistry::default());

        Self::from_uri_and_params(registry, uri, &ObjectStoreParams::default()).await
    }

    /// Parse from a string URI.
    ///
    /// Returns the ObjectStore instance and the absolute path to the object.
    pub async fn from_uri_and_params(
        registry: Arc<ObjectStoreRegistry>,
        uri: &str,
        params: &ObjectStoreParams,
    ) -> Result<(Arc<Self>, Path)> {
        #[allow(deprecated)]
        if let Some((store, path)) = params.object_store.as_ref() {
            let mut inner = store.clone();
            let store_prefix =
                registry.calculate_object_store_prefix(uri, params.storage_options())?;

            let mut io_tracker = IOTracker::default();
            meter_store(&mut inner, &mut io_tracker, &store_prefix);

            if let Some(wrapper) = params.object_store_wrapper.as_ref() {
                inner = wrapper.wrap(&store_prefix, inner);
            }

            // Always wrap with IO tracking
            let tracked_store = io_tracker.wrap("", inner);

            let store = Self {
                inner: tracked_store,
                scheme: path.scheme().to_string(),
                block_size: params.block_size.unwrap_or(64 * 1024),
                max_iop_size: *DEFAULT_MAX_IOP_SIZE,
                use_constant_size_upload_parts: params.use_constant_size_upload_parts,
                list_is_lexically_ordered: params.list_is_lexically_ordered.unwrap_or_default(),
                io_parallelism: DEFAULT_CLOUD_IO_PARALLELISM,
                download_retry_count: DEFAULT_DOWNLOAD_RETRY_COUNT,
                io_tracker,
                store_prefix,
            };
            let path = Path::parse(path.path())?;
            return Ok((Arc::new(store), path));
        }
        let url = uri_to_url(uri)?;

        let store = registry.get_store(url.clone(), params).await?;
        // We know the scheme is valid if we got a store back.
        let provider = registry.get_provider(url.scheme()).expect_ok()?;
        let path = provider.extract_path(&url)?;

        Ok((store, path))
    }

    /// Extract the path component from a URI without initializing the object store.
    ///
    /// This is a synchronous operation that only parses the URI and extracts the path,
    /// without creating or initializing any object store instance.
    ///
    /// # Arguments
    ///
    /// * `registry` - The object store registry to get the provider
    /// * `uri` - The URI to extract the path from
    ///
    /// # Returns
    ///
    /// The extracted path component
    pub fn extract_path_from_uri(registry: Arc<ObjectStoreRegistry>, uri: &str) -> Result<Path> {
        let url = uri_to_url(uri)?;
        let provider = registry
            .get_provider(url.scheme())
            .ok_or_else(|| Error::invalid_input(format!("Unknown scheme: {}", url.scheme())))?;
        provider.extract_path(&url)
    }

    #[deprecated(note = "Use `from_uri` instead")]
    pub fn from_path(str_path: &str) -> Result<(Arc<Self>, Path)> {
        Self::from_uri_and_params(
            Arc::new(ObjectStoreRegistry::default()),
            str_path,
            &Default::default(),
        )
        .now_or_never()
        .unwrap()
    }

    /// Local object store.
    pub fn local() -> Self {
        let provider = FileStoreProvider;
        provider
            .new_store(Url::parse("file:///").unwrap(), &Default::default())
            .now_or_never()
            .unwrap()
            .unwrap()
    }

    /// Create a in-memory object store directly for testing.
    pub fn memory() -> Self {
        let provider = MemoryStoreProvider;
        provider
            .new_store(Url::parse("memory:///").unwrap(), &Default::default())
            .now_or_never()
            .unwrap()
            .unwrap()
    }

    /// Returns true if the object store pointed to a local file system.
    pub fn is_local(&self) -> bool {
        self.scheme == "file" || self.scheme == "file+uring"
    }

    pub fn is_cloud(&self) -> bool {
        if self.is_local() || self.scheme == "memory" || self.scheme == "shared-memory" {
            return false;
        }
        true
    }

    /// Whether this object store prefers the lite scheduler.
    ///
    /// The lite scheduler is designed for backends like io_uring where
    /// tasks should only be polled when the consumer polls them.
    pub fn prefers_lite_scheduler(&self) -> bool {
        self.scheme == "file+uring"
    }

    pub fn scheme(&self) -> &str {
        &self.scheme
    }

    pub fn block_size(&self) -> usize {
        self.block_size
    }

    pub fn max_iop_size(&self) -> u64 {
        self.max_iop_size
    }

    /// The amount of parallelism to use for I/O operations.
    ///
    /// Honors the `LANCE_IO_THREADS` override when set, otherwise the store's configured value.
    /// Always at least 1: callers feed this straight into `buffered` / `buffer_unordered`, and a
    /// window of 0 makes those streams never poll their input — e.g. a metadata-only `count_rows`
    /// would hang rather than return.
    pub fn io_parallelism(&self) -> usize {
        std::env::var("LANCE_IO_THREADS")
            .map(|val| val.parse::<usize>().unwrap())
            .unwrap_or(self.io_parallelism)
            .max(1)
    }

    /// Get the IO tracker for this object store
    ///
    /// The IO tracker can be used to get statistics about read/write operations
    /// performed on this object store.
    pub fn io_tracker(&self) -> &IOTracker {
        &self.io_tracker
    }

    /// Get a snapshot of current IO statistics without resetting counters
    ///
    /// Returns the current IO statistics without modifying the internal state.
    /// Use this when you need to check stats without resetting them.
    pub fn io_stats_snapshot(&self) -> IoStats {
        self.io_tracker.stats()
    }

    /// Get incremental IO statistics since the last call to this method
    ///
    /// Returns the accumulated statistics since the last call and resets the
    /// counters to zero. This is useful for tracking IO operations between
    /// different stages of processing.
    pub fn io_stats_incremental(&self) -> IoStats {
        self.io_tracker.incremental_stats()
    }

    /// Open a file for path.
    ///
    /// Parameters
    /// - ``path``: Absolute path to the file.
    pub async fn open(&self, path: &Path) -> Result<Box<dyn Reader>> {
        match self.scheme.as_str() {
            "file" => {
                LocalObjectReader::open_with_tracker(
                    path,
                    self.block_size,
                    None,
                    Arc::new(self.io_tracker.clone()),
                )
                .await
            }
            #[cfg(target_os = "linux")]
            "file+uring" => {
                // Check if current-thread mode enabled
                let use_current_thread = std::env::var("LANCE_URING_CURRENT_THREAD")
                    .map(|v| str_is_truthy(&v))
                    .unwrap_or(false);

                if use_current_thread {
                    UringCurrentThreadReader::open(
                        path,
                        self.block_size,
                        None,
                        Arc::new(self.io_tracker.clone()),
                    )
                    .await
                } else {
                    UringReader::open(
                        path,
                        self.block_size,
                        None,
                        Arc::new(self.io_tracker.clone()),
                    )
                    .await
                }
            }
            _ => Ok(Box::new(CloudObjectReader::new(
                self.inner.clone(),
                path.clone(),
                self.block_size,
                None,
                self.download_retry_count,
            )?)),
        }
    }

    /// Open a reader for a file with known size.
    ///
    /// This size may either have been retrieved from a list operation or
    /// cached metadata. By passing in the known size, we can skip a HEAD / metadata
    /// call.
    pub async fn open_with_size(&self, path: &Path, known_size: usize) -> Result<Box<dyn Reader>> {
        // If we know the file is really small, we can read the whole thing
        // as a single request.
        if known_size <= self.block_size {
            return Ok(Box::new(SmallReader::new(
                self.inner.clone(),
                path.clone(),
                self.download_retry_count,
                known_size,
            )));
        }

        match self.scheme.as_str() {
            "file" => {
                LocalObjectReader::open_with_tracker(
                    path,
                    self.block_size,
                    Some(known_size),
                    Arc::new(self.io_tracker.clone()),
                )
                .await
            }
            #[cfg(target_os = "linux")]
            "file+uring" => {
                // Check if current-thread mode enabled
                let use_current_thread = std::env::var("LANCE_URING_CURRENT_THREAD")
                    .map(|v| str_is_truthy(&v))
                    .unwrap_or(false);

                if use_current_thread {
                    UringCurrentThreadReader::open(
                        path,
                        self.block_size,
                        Some(known_size),
                        Arc::new(self.io_tracker.clone()),
                    )
                    .await
                } else {
                    UringReader::open(
                        path,
                        self.block_size,
                        Some(known_size),
                        Arc::new(self.io_tracker.clone()),
                    )
                    .await
                }
            }
            _ => Ok(Box::new(CloudObjectReader::new(
                self.inner.clone(),
                path.clone(),
                self.block_size,
                Some(known_size),
                self.download_retry_count,
            )?)),
        }
    }

    /// Create an [ObjectWriter] from local [std::path::Path]
    pub async fn create_local_writer(path: &std::path::Path) -> Result<ObjectWriter> {
        let object_store = Self::local();
        let absolute_path = expand_path(path.to_string_lossy())?;
        let os_path = Path::from_absolute_path(absolute_path)?;
        ObjectWriter::new(&object_store, &os_path).await
    }

    /// Open an [Reader] from local [std::path::Path]
    pub async fn open_local(path: &std::path::Path) -> Result<Box<dyn Reader>> {
        let object_store = Self::local();
        let absolute_path = expand_path(path.to_string_lossy())?;
        let os_path = Path::from_absolute_path(absolute_path)?;
        object_store.open(&os_path).await
    }

    /// Create a new file.
    pub async fn create(&self, path: &Path) -> Result<Box<dyn Writer>> {
        match self.scheme.as_str() {
            "file" => {
                let local_path = super::local::to_local_path(path);
                let local_path = std::path::PathBuf::from(&local_path);
                if let Some(parent) = local_path.parent() {
                    tokio::fs::create_dir_all(parent).await?;
                }
                let parent = local_path
                    .parent()
                    .expect("file path must have parent")
                    .to_owned();
                let named_temp =
                    tokio::task::spawn_blocking(move || tempfile::NamedTempFile::new_in(parent))
                        .await
                        .map_err(|e| Error::io(format!("spawn_blocking failed: {}", e)))??;
                let (std_file, temp_path) = named_temp.into_parts();
                let file = tokio::fs::File::from_std(std_file);
                Ok(Box::new(LocalWriter::new(
                    file,
                    path.clone(),
                    temp_path,
                    Arc::new(self.io_tracker.clone()),
                )))
            }
            _ => Ok(Box::new(ObjectWriter::new(self, path).await?)),
        }
    }

    /// A helper function to create a file and write content to it.
    pub async fn put(&self, path: &Path, content: &[u8]) -> Result<WriteResult> {
        let mut writer = self.create(path).await?;
        writer.write_all(content).await?;
        Writer::shutdown(writer.as_mut()).await
    }

    pub async fn delete(&self, path: &Path) -> Result<()> {
        self.inner.delete(path).await?;
        Ok(())
    }

    /// AWS S3 and GCS reject a single-shot server-side copy whose source is
    /// larger than this; such sources are streamed through a multipart write.
    const MAX_SINGLE_COPY_BYTES: u64 = 5 * 1024 * 1024 * 1024; // 5 GiB

    pub async fn copy(&self, from: &Path, to: &Path) -> Result<()> {
        // S3 and GCS cap single-shot server-side copies at 5 GiB and object_store
        // does not fall back to a multipart copy for larger sources
        // (https://github.com/apache/arrow-rs-object-store/issues/563). Azure and
        // other blob stores don't have this limit, so we only pay for the fallback
        // (an extra size lookup) on S3 and GCS.
        let multipart_copy_fallback = matches!(self.scheme.as_str(), "s3" | "s3+ddb" | "gs");
        self.copy_impl(
            from,
            to,
            multipart_copy_fallback,
            Self::MAX_SINGLE_COPY_BYTES,
        )
        .await
    }

    /// Copy `from` to `to`. When `multipart_copy_fallback` is set, a source
    /// larger than `max_single_copy` is streamed through a multipart write
    /// instead of a single-shot server-side copy. Both are parameters so tests
    /// can drive the streaming path without a multi-gigabyte fixture or an S3
    /// endpoint.
    async fn copy_impl(
        &self,
        from: &Path,
        to: &Path,
        multipart_copy_fallback: bool,
        max_single_copy: u64,
    ) -> Result<()> {
        if self.is_local() {
            // Use std::fs::copy for local filesystem to support cross-filesystem copies
            let metrics = self.io_tracker.begin_io("copy");
            let result = super::local::copy_file(from, to);
            metrics.record(&result, 0);
            return result;
        }
        if multipart_copy_fallback {
            // Reuse the reader for both the size lookup (a single cached HEAD)
            // and the streamed copy, avoiding a separate HEAD request.
            let reader = self.open(from).await?;
            if reader.size().await? as u64 > max_single_copy {
                let mut writer = self.create(to).await?;
                writer.copy_from_reader(reader.as_ref()).await?;
                Writer::shutdown(writer.as_mut()).await?;
                return Ok(());
            }
        }
        Ok(self.inner.copy(from, to).await?)
    }

    /// Read a directory (start from base directory) and returns all sub-paths in the directory.
    pub async fn read_dir(&self, dir_path: impl Into<Path>) -> Result<Vec<String>> {
        let path = dir_path.into();
        let path = Path::parse(&path)?;
        let output = self.inner.list_with_delimiter(Some(&path)).await?;
        Ok(output
            .common_prefixes
            .iter()
            .chain(output.objects.iter().map(|o| &o.location))
            .filter_map(|s| s.filename().map(|f| f.to_string()))
            .collect())
    }

    /// Non-recursive, path-segment delimited list of a single directory level.
    ///
    /// Unlike [`Self::list`], which recurses into the entire subtree, this returns
    /// only the immediate children of `prefix`: the child "directories" as
    /// [`ListResult::common_prefixes`] and the direct child files as
    /// [`ListResult::objects`].
    pub async fn list_with_delimiter(&self, prefix: Option<&Path>) -> Result<ListResult> {
        Ok(self.inner.list_with_delimiter(prefix).await?)
    }

    pub fn list(
        &self,
        path: Option<Path>,
    ) -> Pin<Box<dyn Stream<Item = Result<ObjectMeta>> + Send>> {
        Box::pin(ListRetryStream::new(self.inner.clone(), path, 5).map(|m| m.map_err(|e| e.into())))
    }

    /// Read all files (start from base directory) recursively
    ///
    /// unmodified_since can be specified to only return files that have not been modified since the given time.
    pub fn read_dir_all<'a, 'b>(
        &'a self,
        dir_path: impl Into<&'b Path> + Send,
        unmodified_since: Option<DateTime<Utc>>,
    ) -> BoxStream<'a, Result<ObjectMeta>> {
        self.inner.read_dir_all(dir_path, unmodified_since)
    }

    /// Remove a directory recursively.
    pub async fn remove_dir_all(&self, dir_path: impl Into<Path>) -> Result<()> {
        let path = dir_path.into();
        let path = Path::parse(&path)?;

        if self.is_local() {
            // The local file system provider needs to delete both files and directories.
            // Counted as a single delete request, matching how `delete_stream`
            // counts one batched request regardless of how many paths it removes.
            let metrics = self.io_tracker.begin_io("delete");
            let result = super::local::remove_dir_all(&path);
            metrics.record(&result, 0);
            return result;
        }
        let sub_entries = self
            .inner
            .list(Some(&path))
            .map(|m| m.map(|meta| meta.location))
            .boxed();
        self.inner
            .delete_stream(sub_entries)
            .try_collect::<Vec<_>>()
            .await?;
        if self.scheme == "file-object-store" {
            // file-object-store tries to do everything as similarly as possible to the remote
            // object stores. But we still have to delete the directory entries afterwards.
            return super::local::remove_dir_all(&path);
        }
        Ok(())
    }

    pub fn remove_stream<'a>(
        &'a self,
        locations: BoxStream<'a, Result<Path>>,
    ) -> BoxStream<'a, Result<Path>> {
        let store = Arc::clone(&self.inner);
        locations
            .and_then(move |location| {
                let store = Arc::clone(&store);
                async move {
                    store.delete(&location).await?;
                    Ok(location)
                }
            })
            .boxed()
    }

    /// Check a file exists.
    pub async fn exists(&self, path: &Path) -> Result<bool> {
        match self.inner.head(path).await {
            Ok(_) => Ok(true),
            Err(object_store::Error::NotFound { path: _, source: _ }) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Get file size.
    pub async fn size(&self, path: &Path) -> Result<u64> {
        Ok(self.inner.head(path).await?.size)
    }

    /// Convenience function to open a reader and read all the bytes
    pub async fn read_one_all(&self, path: &Path) -> Result<Bytes> {
        let reader = self.open(path).await?;
        Ok(reader.get_all().await?)
    }

    /// Convenience function open a reader and make a single request
    ///
    /// If you will be making multiple requests to the path it is more efficient to call [`Self::open`]
    /// and then call [`Reader::get_range`] multiple times.
    pub async fn read_one_range(&self, path: &Path, range: Range<usize>) -> Result<Bytes> {
        let reader = self.open(path).await?;
        Ok(reader.get_range(range).await?)
    }
}

/// Options that can be set for multiple object stores
#[derive(PartialEq, Eq, Hash, Clone, Debug, Copy)]
pub enum LanceConfigKey {
    /// Number of times to retry a download that fails
    DownloadRetryCount,
}

impl FromStr for LanceConfigKey {
    type Err = Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "download_retry_count" => Ok(Self::DownloadRetryCount),
            _ => Err(Error::invalid_input_source(
                format!("Invalid LanceConfigKey: {}", s).into(),
            )),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct StorageOptions(pub HashMap<String, String>);

impl StorageOptions {
    /// Create a new instance of [`StorageOptions`]
    pub fn new(options: HashMap<String, String>) -> Self {
        let mut options = options;
        if let Ok(value) = std::env::var("AZURE_STORAGE_ALLOW_HTTP") {
            options.insert("allow_http".into(), value);
        }
        if let Ok(value) = std::env::var("AZURE_STORAGE_USE_HTTP") {
            options.insert("allow_http".into(), value);
        }
        if let Ok(value) = std::env::var("AWS_ALLOW_HTTP") {
            options.insert("allow_http".into(), value);
        }
        if let Ok(value) = std::env::var("OBJECT_STORE_CLIENT_MAX_RETRIES") {
            options.insert("client_max_retries".into(), value);
        }
        if let Ok(value) = std::env::var("OBJECT_STORE_CLIENT_RETRY_TIMEOUT") {
            options.insert("client_retry_timeout".into(), value);
        }
        Self(options)
    }

    /// Denotes if unsecure connections via http are allowed
    pub fn allow_http(&self) -> bool {
        self.0.iter().any(|(key, value)| {
            key.to_ascii_lowercase().contains("allow_http") & str_is_truthy(value)
        })
    }

    /// Number of times to retry a download that fails
    pub fn download_retry_count(&self) -> usize {
        self.0
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("download_retry_count"))
            .map(|(_, value)| value.parse::<usize>().unwrap_or(3))
            .unwrap_or(3)
    }

    /// Max retry times to set in RetryConfig for object store client
    pub fn client_max_retries(&self) -> usize {
        self.0
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("client_max_retries"))
            .and_then(|(_, value)| value.parse::<usize>().ok())
            .unwrap_or(3)
    }

    /// Seconds of timeout to set in RetryConfig for object store client
    pub fn client_retry_timeout(&self) -> u64 {
        self.0
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case("client_retry_timeout"))
            .and_then(|(_, value)| value.parse::<u64>().ok())
            .unwrap_or(180)
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0.get(key)
    }

    /// Build [`ClientOptions`] with default headers extracted from `headers.*` keys.
    ///
    /// Keys prefixed with `headers.` are parsed into HTTP headers. For example,
    /// `headers.x-ms-version = 2023-11-03` results in a default header
    /// `x-ms-version: 2023-11-03`.
    ///
    /// Returns an error if any `headers.*` key has an invalid header name or value.
    #[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
    pub fn client_options(&self) -> Result<ClientOptions> {
        let mut headers = HeaderMap::new();
        for (key, value) in &self.0 {
            if let Some(header_name) = key.strip_prefix("headers.") {
                let name = header_name
                    .parse::<http::header::HeaderName>()
                    .map_err(|e| {
                        Error::invalid_input(format!("invalid header name '{header_name}': {e}"))
                    })?;
                let val = HeaderValue::from_str(value).map_err(|e| {
                    Error::invalid_input(format!("invalid header value for '{header_name}': {e}"))
                })?;
                headers.insert(name, val);
            }
        }
        let mut client_options = ClientOptions::default();
        if !headers.is_empty() {
            client_options = client_options.with_default_headers(headers);
        }
        Ok(client_options)
    }

    /// Get the expiration time in milliseconds since epoch, if present
    pub fn expires_at_millis(&self) -> Option<u64> {
        self.0
            .get(EXPIRES_AT_MILLIS_KEY)
            .and_then(|s| s.parse::<u64>().ok())
    }
}

impl From<HashMap<String, String>> for StorageOptions {
    fn from(value: HashMap<String, String>) -> Self {
        Self::new(value)
    }
}

static DEFAULT_OBJECT_STORE_REGISTRY: std::sync::LazyLock<ObjectStoreRegistry> =
    std::sync::LazyLock::new(ObjectStoreRegistry::default);

impl ObjectStore {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        mut store: Arc<DynObjectStore>,
        location: Url,
        block_size: Option<usize>,
        wrapper: Option<Arc<dyn WrappingObjectStore>>,
        use_constant_size_upload_parts: bool,
        list_is_lexically_ordered: bool,
        io_parallelism: usize,
        download_retry_count: usize,
        storage_options: Option<&HashMap<String, String>>,
    ) -> Self {
        let scheme = location.scheme();
        let block_size = block_size.unwrap_or_else(|| infer_block_size(scheme));
        let store_prefix = match DEFAULT_OBJECT_STORE_REGISTRY.get_provider(scheme) {
            Some(provider) => provider
                .calculate_object_store_prefix(&location, storage_options)
                .unwrap(),
            None => {
                let store_prefix = format!("{}${}", location.scheme(), location.authority());
                log::warn!(
                    "Guessing that object store prefix is {}, since object store scheme is not found in registry.",
                    store_prefix
                );
                store_prefix
            }
        };
        let mut io_tracker = IOTracker::default();
        meter_store(&mut store, &mut io_tracker, &store_prefix);

        let store = match wrapper {
            Some(wrapper) => wrapper.wrap(&store_prefix, store),
            None => store,
        };

        // Always wrap with IO tracking
        let tracked_store = io_tracker.wrap("", store);

        Self {
            inner: tracked_store,
            scheme: scheme.into(),
            block_size,
            max_iop_size: *DEFAULT_MAX_IOP_SIZE,
            use_constant_size_upload_parts,
            list_is_lexically_ordered,
            io_parallelism,
            download_retry_count,
            io_tracker,
            store_prefix,
        }
    }
}

/// Wrap `inner` so its operations publish metrics labelled by `store_prefix`,
/// and label `io_tracker` with the same prefix so the local reads and writes
/// that bypass `inner` publish under it too.
///
/// The two go together on purpose: a store metered on one path but not the other
/// would report a partial picture that reads like a complete one. Every
/// constructor that hands an [`ObjectStore`] to a caller must route its `inner`
/// through here, or through nothing at all.
#[cfg(feature = "metrics")]
fn meter_store(inner: &mut Arc<dyn OSObjectStore>, io_tracker: &mut IOTracker, store_prefix: &str) {
    use crate::object_store::metrics::ObjectStoreMetricsExt;
    io_tracker.set_metrics_base(store_prefix);
    *inner = inner.clone().metered(store_prefix.to_owned());
}

#[cfg(not(feature = "metrics"))]
fn meter_store(
    _inner: &mut Arc<dyn OSObjectStore>,
    _io_tracker: &mut IOTracker,
    _store_prefix: &str,
) {
}

fn infer_block_size(scheme: &str) -> usize {
    // Block size: On local file systems, we use 4KB block size. On cloud
    // object stores, we use 64KB block size. This is generally the largest
    // block size where we don't see a latency penalty.
    match scheme {
        "file" => 4 * 1024,
        _ => 64 * 1024,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use bytes::Bytes;
    use lance_core::utils::tempfile::{TempStdDir, TempStdFile, TempStrDir};
    use object_store::memory::InMemory;
    use object_store::{
        CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, PutMultipartOptions,
        PutOptions, PutPayload, PutResult, Result as OSResult,
    };
    use rstest::rstest;
    use std::env::set_current_dir;
    use std::fmt::{Display, Formatter};
    use std::fs::{create_dir_all, write};
    use std::ops::Range;
    use std::path::Path as StdPath;
    use std::sync::atomic::{AtomicBool, Ordering};

    /// Write test content to file.
    fn write_to_file(path_str: &str, contents: &str) -> std::io::Result<()> {
        let path = expand_path(path_str).map_err(std::io::Error::other)?;
        std::fs::create_dir_all(path.parent().unwrap())?;
        write(path, contents)
    }

    async fn read_from_store(store: &ObjectStore, path: &Path) -> Result<String> {
        let test_file_store = store.open(path).await.unwrap();
        let size = test_file_store.size().await.unwrap();
        let bytes = test_file_store.get_range(0..size).await.unwrap();
        let contents = String::from_utf8(bytes.to_vec()).unwrap();
        Ok(contents)
    }

    #[test]
    fn test_io_parallelism_clamped_to_nonzero() {
        // `io_parallelism()` feeds `buffered`/`buffer_unordered` windows; a value of 0 makes those
        // streams never poll, hanging callers (e.g. a metadata-only `count_rows`). It must clamp.
        let store = ObjectStore::local();

        // SAFETY: process-global env var, set and restored within this test. `io_parallelism()`
        // only reads it, and a concurrent reader observes a valid clamped value, never 0.
        unsafe { std::env::set_var("LANCE_IO_THREADS", "0") };
        assert_eq!(
            store.io_parallelism(),
            1,
            "LANCE_IO_THREADS=0 must clamp to 1"
        );

        unsafe { std::env::set_var("LANCE_IO_THREADS", "8") };
        assert_eq!(
            store.io_parallelism(),
            8,
            "a positive override must pass through unchanged"
        );

        unsafe { std::env::remove_var("LANCE_IO_THREADS") };
        assert!(
            store.io_parallelism() >= 1,
            "the configured default parallelism must be at least 1"
        );
    }

    #[tokio::test]
    async fn test_absolute_paths() {
        let tmp_path = TempStrDir::default();
        write_to_file(
            &format!("{tmp_path}/bar/foo.lance/test_file"),
            "TEST_CONTENT",
        )
        .unwrap();

        // test a few variations of the same path
        for uri in &[
            format!("{tmp_path}/bar/foo.lance"),
            format!("{tmp_path}/./bar/foo.lance"),
            format!("{tmp_path}/bar/foo.lance/../foo.lance"),
        ] {
            let (store, path) = ObjectStore::from_uri(uri).await.unwrap();
            let contents = read_from_store(store.as_ref(), &path.clone().join("test_file"))
                .await
                .unwrap();
            assert_eq!(contents, "TEST_CONTENT");
        }
    }

    #[tokio::test]
    async fn test_cloud_paths() {
        let uri = "s3://bucket/foo.lance";
        let (store, path) = ObjectStore::from_uri(uri).await.unwrap();
        assert_eq!(store.scheme, "s3");
        assert_eq!(path.to_string(), "foo.lance");

        let (store, path) = ObjectStore::from_uri("s3+ddb://bucket/foo.lance")
            .await
            .unwrap();
        assert_eq!(store.scheme, "s3");
        assert_eq!(path.to_string(), "foo.lance");

        let (store, path) = ObjectStore::from_uri("gs://bucket/foo.lance")
            .await
            .unwrap();
        assert_eq!(store.scheme, "gs");
        assert_eq!(path.to_string(), "foo.lance");

        let (store, path) =
            ObjectStore::from_uri("abfss://filesystem@account.dfs.core.windows.net/foo.lance")
                .await
                .unwrap();
        assert_eq!(store.scheme, "abfss");
        assert_eq!(path.to_string(), "foo.lance");
    }

    async fn test_block_size_used_test_helper(
        uri: &str,
        storage_options: Option<HashMap<String, String>>,
        default_expected_block_size: usize,
    ) {
        // Test the default
        let registry = Arc::new(ObjectStoreRegistry::default());
        let accessor = storage_options
            .clone()
            .map(|opts| Arc::new(StorageOptionsAccessor::with_static_options(opts)));
        let params = ObjectStoreParams {
            storage_options_accessor: accessor.clone(),
            ..ObjectStoreParams::default()
        };
        let (store, _) = ObjectStore::from_uri_and_params(registry, uri, &params)
            .await
            .unwrap();
        assert_eq!(store.block_size, default_expected_block_size);

        // Ensure param is used
        let registry = Arc::new(ObjectStoreRegistry::default());
        let params = ObjectStoreParams {
            block_size: Some(1024),
            storage_options_accessor: accessor,
            ..ObjectStoreParams::default()
        };
        let (store, _) = ObjectStore::from_uri_and_params(registry, uri, &params)
            .await
            .unwrap();
        assert_eq!(store.block_size, 1024);
    }

    #[rstest]
    #[case("s3://bucket/foo.lance", None)]
    #[case("gs://bucket/foo.lance", None)]
    #[case("az://account/bucket/foo.lance",
      Some(HashMap::from([
            (String::from("account_name"), String::from("account")),
            (String::from("container_name"), String::from("container"))
           ])))]
    #[case("abfss://filesystem@account.dfs.core.windows.net/foo.lance",
      Some(HashMap::from([
            (String::from("account_name"), String::from("account")),
            (String::from("container_name"), String::from("filesystem"))
           ])))]
    #[tokio::test]
    async fn test_block_size_used_cloud(
        #[case] uri: &str,
        #[case] storage_options: Option<HashMap<String, String>>,
    ) {
        test_block_size_used_test_helper(uri, storage_options, 64 * 1024).await;
    }

    #[rstest]
    #[case("file")]
    #[case("file-object-store")]
    #[case("memory:///bucket/foo.lance")]
    #[tokio::test]
    async fn test_block_size_used_file(#[case] prefix: &str) {
        let tmp_path = TempStrDir::default();
        let path = format!("{tmp_path}/bar/foo.lance/test_file");
        write_to_file(&path, "URL").unwrap();
        let uri = format!("{prefix}:///{path}");
        test_block_size_used_test_helper(&uri, None, 4 * 1024).await;
    }

    #[tokio::test]
    async fn test_relative_paths() {
        let tmp_path = TempStrDir::default();
        write_to_file(
            &format!("{tmp_path}/bar/foo.lance/test_file"),
            "RELATIVE_URL",
        )
        .unwrap();

        set_current_dir(StdPath::new(tmp_path.as_ref())).expect("Error changing current dir");
        let (store, path) = ObjectStore::from_uri("./bar/foo.lance").await.unwrap();

        let contents = read_from_store(store.as_ref(), &path.clone().join("test_file"))
            .await
            .unwrap();
        assert_eq!(contents, "RELATIVE_URL");
    }

    #[tokio::test]
    async fn test_tilde_expansion() {
        let uri = "~/foo.lance";
        write_to_file(&format!("{uri}/test_file"), "TILDE").unwrap();
        let (store, path) = ObjectStore::from_uri(uri).await.unwrap();
        let contents = read_from_store(store.as_ref(), &path.clone().join("test_file"))
            .await
            .unwrap();
        assert_eq!(contents, "TILDE");
    }

    #[tokio::test]
    async fn test_read_directory() {
        let path = TempStdDir::default();
        create_dir_all(path.join("foo").join("bar")).unwrap();
        create_dir_all(path.join("foo").join("zoo")).unwrap();
        create_dir_all(path.join("foo").join("zoo").join("abc")).unwrap();
        write_to_file(
            path.join("foo").join("test_file").to_str().unwrap(),
            "read_dir",
        )
        .unwrap();
        let (store, base) = ObjectStore::from_uri(path.to_str().unwrap()).await.unwrap();

        let sub_dirs = store.read_dir(base.clone().join("foo")).await.unwrap();
        assert_eq!(sub_dirs, vec!["bar", "zoo", "test_file"]);
    }

    #[tokio::test]
    async fn test_delete_directory_local_store() {
        test_delete_directory("").await;
    }

    #[tokio::test]
    async fn test_delete_directory_file_object_store() {
        test_delete_directory("file-object-store").await;
    }

    async fn test_delete_directory(scheme: &str) {
        let path = TempStdDir::default();
        create_dir_all(path.join("foo").join("bar")).unwrap();
        create_dir_all(path.join("foo").join("zoo")).unwrap();
        create_dir_all(path.join("foo").join("zoo").join("abc")).unwrap();
        write_to_file(
            path.join("foo")
                .join("bar")
                .join("test_file")
                .to_str()
                .unwrap(),
            "delete",
        )
        .unwrap();
        let file_url = Url::from_directory_path(&path).unwrap();
        let url = if scheme.is_empty() {
            file_url
        } else {
            let mut url = Url::parse(&format!("{scheme}:///")).unwrap();
            // Use the file:// URL's normalized path so this works on Windows too.
            url.set_path(file_url.path());
            url
        };
        let (store, base) = ObjectStore::from_uri(url.as_ref()).await.unwrap();
        store
            .remove_dir_all(base.clone().join("foo"))
            .await
            .unwrap();

        assert!(!path.join("foo").exists());
    }

    #[derive(Debug)]
    struct TestWrapper {
        called: AtomicBool,

        return_value: Arc<dyn OSObjectStore>,
    }

    impl WrappingObjectStore for TestWrapper {
        fn wrap(
            &self,
            _store_prefix: &str,
            _original: Arc<dyn OSObjectStore>,
        ) -> Arc<dyn OSObjectStore> {
            self.called.store(true, Ordering::Relaxed);

            // return a mocked value so we can check if the final store is the one we expect
            self.return_value.clone()
        }
    }

    impl TestWrapper {
        fn called(&self) -> bool {
            self.called.load(Ordering::Relaxed)
        }
    }

    #[tokio::test]
    async fn test_wrapper_identity_is_stable_across_tasks() {
        let wrapper = Arc::new(TestWrapper {
            called: AtomicBool::new(false),
            return_value: Arc::new(InMemory::new()),
        });
        let initial_params = ObjectStoreParams {
            object_store_wrapper: Some(wrapper.clone()),
            ..ObjectStoreParams::default()
        };
        let task_params = tokio::spawn(async move {
            ObjectStoreParams {
                object_store_wrapper: Some(wrapper),
                ..ObjectStoreParams::default()
            }
        })
        .await
        .unwrap();

        assert_eq!(initial_params, task_params);

        let mut initial_hasher = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(&initial_params, &mut initial_hasher);
        let mut task_hasher = std::hash::DefaultHasher::new();
        std::hash::Hash::hash(&task_params, &mut task_hasher);
        assert_eq!(
            std::hash::Hasher::finish(&initial_hasher),
            std::hash::Hasher::finish(&task_hasher)
        );
    }

    #[tokio::test]
    async fn test_wrapping_object_store_option_is_used() {
        // Make a store for the inner store first
        let mock_inner_store: Arc<dyn OSObjectStore> = Arc::new(InMemory::new());
        let registry = Arc::new(ObjectStoreRegistry::default());

        assert_eq!(Arc::strong_count(&mock_inner_store), 1);

        let wrapper = Arc::new(TestWrapper {
            called: AtomicBool::new(false),
            return_value: mock_inner_store.clone(),
        });

        let params = ObjectStoreParams {
            object_store_wrapper: Some(wrapper.clone()),
            ..ObjectStoreParams::default()
        };

        // not called yet
        assert!(!wrapper.called());

        let _ = ObjectStore::from_uri_and_params(registry, "memory:///", &params)
            .await
            .unwrap();

        // called after construction
        assert!(wrapper.called());

        // hard to compare two trait pointers as the point to vtables
        // using the ref count as a proxy to make sure that the store is correctly kept
        assert_eq!(Arc::strong_count(&mock_inner_store), 2);
    }

    #[tokio::test]
    async fn test_local_paths() {
        let file_path = TempStdFile::default();
        let mut writer = ObjectStore::create_local_writer(&file_path).await.unwrap();
        writer.write_all(b"LOCAL").await.unwrap();
        Writer::shutdown(&mut writer).await.unwrap();

        let reader = ObjectStore::open_local(&file_path).await.unwrap();
        let buf = reader.get_range(0..5).await.unwrap();
        assert_eq!(buf.as_ref(), b"LOCAL");
    }

    #[tokio::test]
    async fn test_read_one() {
        let file_path = TempStdFile::default();
        let mut writer = ObjectStore::create_local_writer(&file_path).await.unwrap();
        writer.write_all(b"LOCAL").await.unwrap();
        Writer::shutdown(&mut writer).await.unwrap();

        let file_path_os = object_store::path::Path::parse(file_path.to_str().unwrap()).unwrap();
        let obj_store = ObjectStore::local();
        let buf = obj_store.read_one_all(&file_path_os).await.unwrap();
        assert_eq!(buf.as_ref(), b"LOCAL");

        let buf = obj_store.read_one_range(&file_path_os, 0..5).await.unwrap();
        assert_eq!(buf.as_ref(), b"LOCAL");
    }

    #[tokio::test]
    #[cfg(windows)]
    async fn test_windows_paths() {
        use std::path::Component;
        use std::path::Prefix;
        use std::path::Prefix::*;

        fn get_path_prefix(path: &StdPath) -> Prefix<'_> {
            match path.components().next().unwrap() {
                Component::Prefix(prefix_component) => prefix_component.kind(),
                _ => panic!(),
            }
        }

        fn get_drive_letter(prefix: Prefix) -> String {
            match prefix {
                Disk(bytes) => String::from_utf8(vec![bytes]).unwrap(),
                _ => panic!(),
            }
        }

        let tmp_path = TempStdFile::default();
        let prefix = get_path_prefix(&tmp_path);
        let drive_letter = get_drive_letter(prefix);

        write_to_file(
            &(format!("{drive_letter}:/test_folder/test.lance") + "/test_file"),
            "WINDOWS",
        )
        .unwrap();

        for uri in &[
            format!("{drive_letter}:/test_folder/test.lance"),
            format!("{drive_letter}:\\test_folder\\test.lance"),
        ] {
            let (store, base) = ObjectStore::from_uri(uri).await.unwrap();
            let contents = read_from_store(store.as_ref(), &base.clone().join("test_file"))
                .await
                .unwrap();
            assert_eq!(contents, "WINDOWS");
        }
    }

    #[tokio::test]
    async fn test_cross_filesystem_copy() {
        // Create two temporary directories that simulate different filesystems
        let source_dir = TempStdDir::default();
        let dest_dir = TempStdDir::default();

        // Create a test file in the source directory
        let source_file_name = "test_file.txt";
        let source_file = source_dir.join(source_file_name);
        std::fs::write(&source_file, b"test content").unwrap();

        // Create ObjectStore for local filesystem
        let (store, base_path) = ObjectStore::from_uri(source_dir.to_str().unwrap())
            .await
            .unwrap();

        // Create paths relative to the ObjectStore base
        let from_path = base_path.clone().join(source_file_name);

        // Use object_store::Path::parse for the destination
        let dest_file = dest_dir.join("copied_file.txt");
        let dest_str = dest_file.to_str().unwrap();
        let to_path = object_store::path::Path::parse(dest_str).unwrap();

        // Perform the copy operation
        store.copy(&from_path, &to_path).await.unwrap();

        // Verify the file was copied correctly
        assert!(dest_file.exists());
        let copied_content = std::fs::read(&dest_file).unwrap();
        assert_eq!(copied_content, b"test content");
    }

    #[tokio::test]
    async fn test_copy_creates_parent_directories() {
        let source_dir = TempStdDir::default();
        let dest_dir = TempStdDir::default();

        // Create a test file in the source directory
        let source_file_name = "test_file.txt";
        let source_file = source_dir.join(source_file_name);
        std::fs::write(&source_file, b"test content").unwrap();

        // Create ObjectStore for local filesystem
        let (store, base_path) = ObjectStore::from_uri(source_dir.to_str().unwrap())
            .await
            .unwrap();

        // Create paths
        let from_path = base_path.clone().join(source_file_name);

        // Create destination with nested directories that don't exist yet
        let dest_file = dest_dir.join("nested").join("dirs").join("copied_file.txt");
        let dest_str = dest_file.to_str().unwrap();
        let to_path = object_store::path::Path::parse(dest_str).unwrap();

        // Perform the copy operation - should create parent directories
        store.copy(&from_path, &to_path).await.unwrap();

        // Verify the file was copied correctly and directories were created
        assert!(dest_file.exists());
        assert!(dest_file.parent().unwrap().exists());
        let copied_content = std::fs::read(&dest_file).unwrap();
        assert_eq!(copied_content, b"test content");
    }

    /// Inner store that forwards everything to `InMemory` except single-shot
    /// server-side copy (`copy_opts`), which always fails. This lets a test
    /// prove that `ObjectStore::copy` fell back to a streaming multipart copy
    /// for an oversized source rather than issuing a single `CopyObject`.
    #[derive(Debug)]
    struct CopyFailingStore {
        inner: InMemory,
    }

    impl Display for CopyFailingStore {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            write!(f, "CopyFailingStore")
        }
    }

    #[async_trait]
    impl OSObjectStore for CopyFailingStore {
        async fn put_opts(
            &self,
            location: &Path,
            bytes: PutPayload,
            opts: PutOptions,
        ) -> OSResult<PutResult> {
            self.inner.put_opts(location, bytes, opts).await
        }
        async fn put_multipart_opts(
            &self,
            location: &Path,
            opts: PutMultipartOptions,
        ) -> OSResult<Box<dyn MultipartUpload>> {
            self.inner.put_multipart_opts(location, opts).await
        }
        async fn get_opts(&self, location: &Path, options: GetOptions) -> OSResult<GetResult> {
            self.inner.get_opts(location, options).await
        }
        async fn get_ranges(&self, location: &Path, ranges: &[Range<u64>]) -> OSResult<Vec<Bytes>> {
            self.inner.get_ranges(location, ranges).await
        }
        fn delete_stream(
            &self,
            locations: BoxStream<'static, OSResult<Path>>,
        ) -> BoxStream<'static, OSResult<Path>> {
            self.inner.delete_stream(locations)
        }
        fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list(prefix)
        }
        fn list_with_offset(
            &self,
            prefix: Option<&Path>,
            offset: &Path,
        ) -> BoxStream<'static, OSResult<ObjectMeta>> {
            self.inner.list_with_offset(prefix, offset)
        }
        async fn list_with_delimiter(&self, prefix: Option<&Path>) -> OSResult<ListResult> {
            self.inner.list_with_delimiter(prefix).await
        }
        async fn copy_opts(&self, _from: &Path, _to: &Path, _opts: CopyOptions) -> OSResult<()> {
            Err(object_store::Error::Generic {
                store: "CopyFailingStore",
                source: "single-shot copy disabled in test".into(),
            })
        }
    }

    #[tokio::test]
    async fn test_copy_streams_objects_larger_than_threshold() {
        // memory:// is non-local but isn't an S3/GCS scheme, so copy() wouldn't
        // enable the fallback on its own. Drive copy_impl directly with
        // multipart_copy_fallback = true to exercise the streaming path. The
        // inner store rejects any single-shot copy, so a successful copy can only
        // have gone through the streaming branch.
        let mut store = ObjectStore::memory();
        store.inner = Arc::new(CopyFailingStore {
            inner: InMemory::new(),
        });

        let from = Path::from("source.bin");
        let contents = b"streaming multipart copy payload well past the tiny threshold";
        store.put(&from, contents).await.unwrap();

        // Source size (61 bytes) exceeds the threshold -> must stream via a
        // multipart write rather than a single-shot server-side copy.
        let streamed = Path::from("streamed.bin");
        store.copy_impl(&from, &streamed, true, 8).await.unwrap();
        let copied = store.read_one_all(&streamed).await.unwrap();
        assert_eq!(copied.as_ref(), contents.as_slice());

        // Source size below the threshold -> single-shot copy, which the inner
        // store rejects, confirming that the streaming branch (not native copy)
        // is what made the first copy succeed.
        let native = Path::from("native.bin");
        assert!(
            store
                .copy_impl(&from, &native, true, u64::MAX)
                .await
                .is_err()
        );
    }

    #[test]
    #[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
    fn test_client_options_extracts_headers() {
        let opts = StorageOptions(HashMap::from([
            ("headers.x-custom-foo".to_string(), "bar".to_string()),
            ("headers.x-ms-version".to_string(), "2023-11-03".to_string()),
            ("region".to_string(), "us-west-2".to_string()),
        ]));
        let client_options = opts.client_options().unwrap();

        // Verify non-header keys are not consumed as headers by creating
        // another StorageOptions with no headers.* keys.
        let opts_no_headers = StorageOptions(HashMap::from([(
            "region".to_string(),
            "us-west-2".to_string(),
        )]));
        opts_no_headers.client_options().unwrap();

        // Smoke test: the client_options with headers should be usable
        // in a builder (we can't inspect the headers directly, but building
        // should not fail).
        #[cfg(feature = "gcp")]
        {
            use object_store::gcp::GoogleCloudStorageBuilder;
            let _builder = GoogleCloudStorageBuilder::new()
                .with_client_options(client_options)
                .with_url("gs://test-bucket");
        }
    }

    #[test]
    #[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
    fn test_client_options_rejects_invalid_header_name() {
        let opts = StorageOptions(HashMap::from([(
            "headers.bad header".to_string(),
            "value".to_string(),
        )]));
        let err = opts.client_options().unwrap_err();
        assert!(err.to_string().contains("invalid header name"));
    }

    #[test]
    #[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
    fn test_client_options_rejects_invalid_header_value() {
        let opts = StorageOptions(HashMap::from([(
            "headers.x-good-name".to_string(),
            "bad\x01value".to_string(),
        )]));
        let err = opts.client_options().unwrap_err();
        assert!(err.to_string().contains("invalid header value"));
    }

    #[test]
    #[cfg(any(feature = "aws", feature = "azure", feature = "gcp"))]
    fn test_client_options_empty_when_no_header_keys() {
        let opts = StorageOptions(HashMap::from([
            ("region".to_string(), "us-east-1".to_string()),
            ("access_key_id".to_string(), "AKID".to_string()),
        ]));
        opts.client_options().unwrap();
    }
}
