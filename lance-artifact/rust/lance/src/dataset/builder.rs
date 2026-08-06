// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors
use std::{collections::HashMap, sync::Arc, time::Duration};

use lance_core::cache::CacheBackend;

use super::refs::{Branches, Ref, Refs, check_valid_branch, normalize_branch, standardize_branch};
use super::{DEFAULT_INDEX_CACHE_SIZE, DEFAULT_METADATA_CACHE_SIZE, ReadParams, WriteParams};
use crate::dataset::branch_location::BranchLocation;
use crate::io::commit::namespace_manifest::LanceNamespaceExternalManifestStore;
use crate::{Dataset, Error, Result, session::Session};
use futures::FutureExt;
use lance_core::utils::tracing::{DATASET_LOADING_EVENT, TRACE_DATASET_EVENTS};
use lance_file::reader::FileReaderOptions;
use lance_io::object_store::{
    DEFAULT_CLOUD_IO_PARALLELISM, LanceNamespaceStorageOptionsProvider, ObjectStore,
    ObjectStoreParams, StorageOptions, StorageOptionsAccessor,
};
use lance_namespace::LanceNamespace;
use lance_namespace::models::DescribeTableRequest;
use lance_table::{
    format::{Manifest, populate_manifest_schema_dictionaries},
    io::commit::external_manifest::ExternalManifestCommitHandler,
    io::commit::{CommitHandler, commit_handler_from_url},
};
#[cfg(feature = "aws")]
use object_store::aws::AwsCredentialProvider;
use object_store::{DynObjectStore, path::Path};
use prost::Message;
use tracing::{info, instrument};
use url::Url;

/// builder for loading a [`Dataset`].
#[derive(Clone)]
pub struct DatasetBuilder {
    /// Cache size for index cache. If it is zero, index cache is disabled.
    index_cache_size_bytes: usize,
    /// Metadata cache size for the fragment metadata. If it is zero, metadata
    /// cache is disabled.
    metadata_cache_size_bytes: usize,
    /// Custom index cache backend. If set, overrides `index_cache_size_bytes`.
    index_cache_backend: Option<Arc<dyn CacheBackend>>,
    /// Optional pre-loaded manifest to avoid loading it again.
    manifest: Option<Manifest>,
    session: Option<Arc<Session>>,
    commit_handler: Option<Arc<dyn CommitHandler>>,
    options: ObjectStoreParams,
    version: Option<Ref>,
    table_uri: String,
    file_reader_options: Option<FileReaderOptions>,
    /// Storage options that override user-provided options (e.g., from namespace client)
    storage_options_override: Option<HashMap<String, String>>,
    /// Runtime-only exact object store bindings keyed by base path URI.
    base_store_params: HashMap<String, ObjectStoreParams>,
    /// Namespace-managed table info `(client, table_id)`, set by `from_namespace`
    /// when the table uses managed versioning. The commit handler is built in
    /// `build_object_store`, rooted at the resolved table path; the branch a
    /// namespace request targets is derived per call from the base path the
    /// handler is handed.
    namespace_managed: Option<(Arc<dyn LanceNamespace>, Vec<String>)>,
}

impl std::fmt::Debug for DatasetBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DatasetBuilder")
            .field("index_cache_size_bytes", &self.index_cache_size_bytes)
            .field("metadata_cache_size_bytes", &self.metadata_cache_size_bytes)
            .field("manifest", &self.manifest.is_some())
            .field("session", &self.session.is_some())
            .field("commit_handler", &self.commit_handler.is_some())
            .field("version", &self.version)
            .field("table_uri", &self.table_uri)
            .field("file_reader_options", &self.file_reader_options)
            .field(
                "storage_options_override",
                &self.storage_options_override.is_some(),
            )
            .field("base_store_params", &!self.base_store_params.is_empty())
            .field("namespace_managed", &self.namespace_managed.is_some())
            .finish()
    }
}

impl DatasetBuilder {
    pub fn from_uri<T: AsRef<str>>(table_uri: T) -> Self {
        Self {
            index_cache_size_bytes: DEFAULT_INDEX_CACHE_SIZE,
            metadata_cache_size_bytes: DEFAULT_METADATA_CACHE_SIZE,
            index_cache_backend: None,
            table_uri: table_uri.as_ref().to_string(),
            options: ObjectStoreParams::default(),
            commit_handler: None,
            session: None,
            version: None,
            manifest: None,
            file_reader_options: None,
            storage_options_override: None,
            base_store_params: HashMap::new(),
            namespace_managed: None,
        }
    }

    /// Create a DatasetBuilder from a LanceNamespace client
    ///
    /// This will automatically fetch the table location and storage options from the namespace
    /// client via `describe_table()`.
    ///
    /// Storage options from the namespace client will override any user-provided storage options
    /// set via `.with_storage_options()`. This ensures the namespace client is always the source
    /// of truth for storage options.
    ///
    /// # Arguments
    /// * `namespace_client` - The namespace client implementation to fetch table info from
    /// * `table_id` - The table identifier (e.g., vec!["my_table"])
    ///
    /// # Example
    /// ```ignore
    /// use lance_namespace_impls::ConnectBuilder;
    /// use lance::dataset::DatasetBuilder;
    ///
    /// // Connect to a REST namespace
    /// let namespace_client = ConnectBuilder::new("rest")
    ///     .property("uri", "http://localhost:8080")
    ///     .connect()
    ///     .await?;
    ///
    /// // Load a dataset using storage options from namespace client
    /// let dataset = DatasetBuilder::from_namespace(
    ///     namespace_client,
    ///     vec!["my_table".to_string()],
    /// )
    /// .await?
    /// .load()
    /// .await?;
    /// ```
    #[allow(deprecated)]
    pub async fn from_namespace(
        namespace_client: Arc<dyn LanceNamespace>,
        table_id: Vec<String>,
    ) -> Result<Self> {
        let request = DescribeTableRequest {
            id: Some(table_id.clone()),
            ..Default::default()
        };

        let response = namespace_client
            .describe_table(request)
            .await
            .map_err(|e| Error::namespace_source(Box::new(e)))?;

        let table_uri = response.location.ok_or_else(|| {
            Error::namespace_source(Box::new(std::io::Error::other(
                "Table location not found in namespace response",
            )))
        })?;

        let mut builder = Self::from_uri(&table_uri);

        // Defer building the commit handler to load(): the manifest store is
        // rooted at the resolved table path, which is only known once the
        // object store is built.
        if response.managed_versioning == Some(true) {
            builder.namespace_managed = Some((namespace_client.clone(), table_id.clone()));
        }

        // Use namespace storage options if available
        let namespace_storage_options = response.storage_options;

        builder.storage_options_override = namespace_storage_options.clone();

        if let Some(initial_opts) = namespace_storage_options {
            let provider: Arc<dyn lance_io::object_store::StorageOptionsProvider> = Arc::new(
                LanceNamespaceStorageOptionsProvider::new(namespace_client, table_id),
            );
            builder.options.storage_options_accessor = Some(Arc::new(
                StorageOptionsAccessor::with_initial_and_provider(initial_opts, provider),
            ));
        }

        Ok(builder)
    }
}

// Much of this builder is directly inspired from the to delta-rs table builder implementation
// https://github.com/delta-io/delta-rs/main/crates/deltalake-core/src/table/builder.rs
impl DatasetBuilder {
    /// Set the cache size for indices. Set to zero, to disable the cache.
    pub fn with_index_cache_size_bytes(mut self, cache_size: usize) -> Self {
        self.index_cache_size_bytes = cache_size;
        self
    }

    /// Use a custom index cache backend.
    ///
    /// When set, this overrides `with_index_cache_size_bytes` — the custom
    /// backend is responsible for its own capacity management.
    pub fn with_index_cache_backend(mut self, backend: Arc<dyn CacheBackend>) -> Self {
        self.index_cache_backend = Some(backend);
        self
    }

    /// Set the cache size for indices. Set to zero, to disable the cache.
    #[deprecated(since = "0.30.0", note = "Use `with_index_cache_size_bytes` instead")]
    pub fn with_index_cache_size(mut self, cache_size: usize) -> Self {
        let assumed_entry_size = 20 * 1024 * 1024; // 20 MiB per entry
        self.index_cache_size_bytes = cache_size * assumed_entry_size;
        self
    }

    /// Size of the metadata cache in bytes. This cache stores metadata in memory
    /// for faster open table and scans. The default is 1 GiB.
    pub fn with_metadata_cache_size_bytes(mut self, cache_size: usize) -> Self {
        self.metadata_cache_size_bytes = cache_size;
        self
    }

    /// Set the cache size for the file metadata. Set to zero to disable this cache.
    #[deprecated(
        since = "0.30.0",
        note = "Use `with_metadata_cache_size_bytes` instead"
    )]
    pub fn with_metadata_cache_size(mut self, cache_size: usize) -> Self {
        let assumed_entry_size = 10 * 1024 * 1024; // 10MB per entry
        self.metadata_cache_size_bytes = cache_size * assumed_entry_size;
        self
    }

    /// The block size passed to the underlying Object Store reader.
    ///
    /// This is used to control the minimal request size.
    /// Defaults to 4KB for local files and 64KB for others
    pub fn with_block_size(mut self, block_size: usize) -> Self {
        self.options.block_size = Some(block_size);
        self
    }

    /// Sets `version` for the builder using a version number
    pub fn with_version(mut self, version: u64) -> Self {
        self.version = Some(Ref::from(version));
        self
    }

    /// Sets `version` for the builder using a branch and optional version number
    /// If version_number is null, checkout the latest version
    pub fn with_branch(mut self, branch: &str, version_number: Option<u64>) -> Self {
        self.version = Some(Ref::from((branch, version_number)));
        self
    }

    /// Sets `version` for the builder using a tag
    pub fn with_tag(mut self, tag: &str) -> Self {
        self.version = Some(Ref::from(tag));
        self
    }

    pub fn with_commit_handler(mut self, commit_handler: Arc<dyn CommitHandler>) -> Self {
        self.commit_handler = Some(commit_handler);
        self
    }

    /// Sets the s3 credentials refresh.
    /// This only applies to s3 storage.
    pub fn with_s3_credentials_refresh_offset(mut self, offset: Duration) -> Self {
        self.options.s3_credentials_refresh_offset = offset;
        self
    }

    /// Sets the aws credentials provider.
    /// This only applies to aws object store.
    #[cfg(feature = "aws")]
    pub fn with_aws_credentials_provider(mut self, credentials: AwsCredentialProvider) -> Self {
        self.options.aws_credentials = Some(credentials);
        self
    }

    /// Directly set the object store to use.
    #[deprecated(note = "Implement an ObjectStoreProvider instead")]
    #[allow(deprecated)]
    pub fn with_object_store(
        mut self,
        object_store: Arc<DynObjectStore>,
        location: Url,
        commit_handler: Arc<dyn CommitHandler>,
    ) -> Self {
        self.options.object_store = Some((object_store, location));
        self.commit_handler = Some(commit_handler);
        self
    }

    /// Use a serialized manifest instead of loading it from the object store.
    ///
    /// This is common when transferring a dataset across IPC boundaries.
    pub fn with_serialized_manifest(mut self, manifest: &[u8]) -> Result<Self> {
        let manifest = Manifest::try_from(lance_table::format::pb::Manifest::decode(manifest)?)?;
        self.manifest = Some(manifest);
        Ok(self)
    }

    /// Set options used to initialize storage backend
    ///
    /// Options may be passed in the HashMap or set as environment variables. See documentation of
    /// underlying object store implementation for details.
    ///
    /// - [Azure options](https://docs.rs/object_store/latest/object_store/azure/enum.AzureConfigKey.html#variants)
    /// - [S3 options](https://docs.rs/object_store/latest/object_store/aws/enum.AmazonS3ConfigKey.html#variants)
    /// - [Google options](https://docs.rs/object_store/latest/object_store/gcp/enum.GoogleConfigKey.html#variants)
    ///
    /// For datasets with additional registered base paths, a key of the form
    /// `base_<id>.<key>` applies `<key>` only to the base path with that
    /// manifest id, overriding the unscoped options that every base inherits.
    /// For example `base_1.account_key = abc` makes base 1 use
    /// `account_key = abc` while all other options are shared. Exact per-base
    /// bindings set via [`Self::with_base_store_params`] take precedence over
    /// base-scoped keys.
    pub fn with_storage_options(mut self, storage_options: HashMap<String, String>) -> Self {
        // Merge with existing options if accessor exists, otherwise create new static accessor
        if let Some(existing) = self.options.storage_options_accessor.take() {
            let mut merged = existing
                .initial_storage_options()
                .cloned()
                .unwrap_or_default();
            merged.extend(storage_options);
            if let Some(provider) = existing.provider().cloned() {
                self.options.storage_options_accessor = Some(Arc::new(
                    StorageOptionsAccessor::with_initial_and_provider(merged, provider),
                ));
            } else {
                self.options.storage_options_accessor = Some(Arc::new(
                    StorageOptionsAccessor::with_static_options(merged),
                ));
            }
        } else {
            self.options.storage_options_accessor = Some(Arc::new(
                StorageOptionsAccessor::with_static_options(storage_options),
            ));
        }
        self
    }

    /// Set a single option used to initialize storage backend
    /// For example, to set the region for S3, you can use:
    ///
    /// ```ignore
    /// let builder = DatasetBuilder::from_uri("s3://bucket/path")
    ///     .with_storage_option("region", "us-east-1");
    /// ```
    pub fn with_storage_option(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Self {
        let mut storage_options = self.options.storage_options().cloned().unwrap_or_default();
        storage_options.insert(key.as_ref().to_string(), value.as_ref().to_string());

        // Merge with existing accessor if present
        if let Some(existing) = self.options.storage_options_accessor.take() {
            if let Some(provider) = existing.provider().cloned() {
                self.options.storage_options_accessor = Some(Arc::new(
                    StorageOptionsAccessor::with_initial_and_provider(storage_options, provider),
                ));
            } else {
                self.options.storage_options_accessor = Some(Arc::new(
                    StorageOptionsAccessor::with_static_options(storage_options),
                ));
            }
        } else {
            self.options.storage_options_accessor = Some(Arc::new(
                StorageOptionsAccessor::with_static_options(storage_options),
            ));
        }
        self
    }

    /// Enable credential vending from a LanceNamespace client
    ///
    /// Credentials will be automatically refreshed from the namespace client
    /// before they expire. The namespace client should return `expires_at_millis`
    /// in the storage_options from `describe_table()`.
    ///
    /// Use `with_s3_credentials_refresh_offset()` to configure how early
    /// credentials should be refreshed before they expire (default is 5 minutes).
    ///
    /// # Arguments
    /// * `provider` - The storage options provider to fetch credentials from
    ///
    /// # Example
    /// ```ignore
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use lance_namespace_impls::ConnectBuilder;
    /// use lance_io::object_store::{StorageOptionsProvider, LanceNamespaceStorageOptionsProvider};
    ///
    /// // Connect to a REST namespace
    /// let namespace_client = ConnectBuilder::new("rest")
    ///     .property("uri", "http://localhost:8080")
    ///     .connect()
    ///     .await?;
    ///
    /// // Create a storage options provider from namespace client
    /// let provider = Arc::new(LanceNamespaceStorageOptionsProvider::new(
    ///     namespace_client,
    ///     vec!["my_table".to_string()],
    /// ));
    ///
    /// // With default settings (5 minute refresh offset)
    /// let dataset = DatasetBuilder::from_uri("s3://bucket/table.lance")
    ///     .with_storage_options_provider(provider)
    ///     .load()
    ///     .await?;
    /// ```
    ///
    /// // With custom refresh offset (refresh 10 minutes before expiration)
    /// let dataset = DatasetBuilder::from_uri("s3://bucket/table.lance")
    ///     .with_storage_options_provider(provider.clone())
    ///     .with_s3_credentials_refresh_offset(Duration::from_secs(600))
    ///     .load()
    ///     .await?;
    pub fn with_storage_options_provider(
        mut self,
        provider: Arc<dyn lance_io::object_store::StorageOptionsProvider>,
    ) -> Self {
        // Preserve existing storage options if any
        if let Some(existing) = self.options.storage_options_accessor.take() {
            if let Some(initial) = existing.initial_storage_options().cloned() {
                self.options.storage_options_accessor = Some(Arc::new(
                    StorageOptionsAccessor::with_initial_and_provider(initial, provider),
                ));
            } else {
                self.options.storage_options_accessor =
                    Some(Arc::new(StorageOptionsAccessor::with_provider(provider)));
            }
        } else {
            self.options.storage_options_accessor =
                Some(Arc::new(StorageOptionsAccessor::with_provider(provider)));
        }
        self
    }

    /// Set a unified storage options accessor for credential management
    ///
    /// The accessor bundles static storage options with an optional dynamic provider,
    /// handling all caching and refresh logic internally.
    ///
    /// # Arguments
    /// * `accessor` - The storage options accessor
    ///
    /// # Example
    /// ```ignore
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// use lance_io::object_store::StorageOptionsAccessor;
    ///
    /// // Create an accessor with a dynamic provider
    /// let accessor = Arc::new(StorageOptionsAccessor::with_provider(
    ///     provider,
    ///     Duration::from_secs(300), // 5 minute refresh offset
    /// ));
    ///
    /// let dataset = DatasetBuilder::from_uri("s3://bucket/table.lance")
    ///     .with_storage_options_accessor(accessor)
    ///     .load()
    ///     .await?;
    /// ```
    pub fn with_storage_options_accessor(mut self, accessor: Arc<StorageOptionsAccessor>) -> Self {
        self.options.storage_options_accessor = Some(accessor);
        self
    }

    /// Set runtime-only object store params for a specific registered base path.
    ///
    /// These params are not persisted in the manifest. They are used as-is
    /// whenever the dataset resolves an object store for the given
    /// `BasePath.path`, taking precedence over `base_<id>.<key>` storage
    /// options (see [`Self::with_storage_options`]). Dataset-level store params
    /// remain the fallback for bases without an explicit binding.
    pub fn with_base_store_params(
        mut self,
        base_path: impl AsRef<str>,
        store_params: ObjectStoreParams,
    ) -> Self {
        self.base_store_params
            .insert(base_path.as_ref().to_string(), store_params);
        self
    }

    /// Set options based on [ReadParams].
    pub fn with_read_params(mut self, read_params: ReadParams) -> Self {
        self = self
            .with_index_cache_size_bytes(read_params.index_cache_size_bytes)
            .with_metadata_cache_size_bytes(read_params.metadata_cache_size_bytes);

        if let Some(options) = read_params.store_options {
            self.options = options;
        }

        if let Some(session) = read_params.session {
            self.session = Some(session);
        }

        if let Some(commit_handler) = read_params.commit_handler {
            self.commit_handler = Some(commit_handler);
        }

        if let Some(file_reader_options) = read_params.file_reader_options {
            self.file_reader_options = Some(file_reader_options);
        }

        self
    }

    /// Set options based on [WriteParams].
    pub fn with_write_params(mut self, write_params: WriteParams) -> Self {
        if let Some(options) = write_params.store_params {
            self.options = options;
        }

        if let Some(commit_handler) = write_params.commit_handler {
            self.commit_handler = Some(commit_handler);
        }

        self
    }

    /// Re-use an existing session.
    ///
    /// The session holds caches for index and metadata.
    ///
    /// If this is set, then `with_index_cache_size` and `with_metadata_cache_size` are ignored.
    pub fn with_session(mut self, session: Arc<Session>) -> Self {
        self.session = Some(session);
        self
    }

    /// Set exact object store params used as the dataset-level default binding.
    pub fn with_store_params(mut self, store_params: ObjectStoreParams) -> Self {
        self.options = store_params;
        self
    }

    /// Build a lance object store for the given config
    pub async fn build_object_store(
        mut self,
    ) -> Result<(Arc<ObjectStore>, Path, Arc<dyn CommitHandler>)> {
        let storage_options = self
            .options
            .storage_options()
            .cloned()
            .map(StorageOptions::new)
            .unwrap_or_default();
        let download_retry_count = storage_options.download_retry_count();

        let store_registry = self
            .session
            .as_ref()
            .map(|s| s.store_registry())
            .unwrap_or_default();

        #[allow(deprecated)]
        let (object_store, base_path) = match &self.options.object_store {
            Some(store) => (
                Arc::new(ObjectStore::new(
                    store.0.clone(),
                    store.1.clone(),
                    self.options.block_size,
                    self.options.object_store_wrapper.clone(),
                    self.options.use_constant_size_upload_parts,
                    store.1.scheme() != "file",
                    // If user supplied an object store then we just assume it's probably
                    // cloud-like
                    DEFAULT_CLOUD_IO_PARALLELISM,
                    download_retry_count,
                    None, // No storage_options available here
                )),
                Path::from(store.1.path()),
            ),
            None => {
                ObjectStore::from_uri_and_params(store_registry, &self.table_uri, &self.options)
                    .await?
            }
        };

        // Resolve the commit handler: an explicitly set one wins; otherwise a
        // namespace-managed table builds a manifest store rooted at the resolved
        // table path (the branch a request targets is derived per call from the
        // base path the handler is handed); otherwise fall back to the default
        // for the uri. Resolving here (not in load) keeps this pub method
        // consistent for every caller.
        let commit_handler: Arc<dyn CommitHandler> =
            if let Some(commit_handler) = self.commit_handler.take() {
                commit_handler
            } else if let Some((namespace_client, table_id)) = self.namespace_managed.take() {
                Arc::new(ExternalManifestCommitHandler {
                    external_manifest_store: Arc::new(LanceNamespaceExternalManifestStore::new(
                        namespace_client,
                        table_id,
                        base_path.clone(),
                    )),
                })
            } else {
                commit_handler_from_url(&self.table_uri, &Some(self.options.clone())).await?
            };

        Ok((object_store, base_path, commit_handler))
    }

    #[instrument(skip_all)]
    pub async fn load(self) -> Result<Dataset> {
        let uri = self.table_uri.clone();
        let target_ref = self.version.clone();
        match self.load_impl().boxed().await {
            Ok(dataset) => {
                info!(target: TRACE_DATASET_EVENTS, event=DATASET_LOADING_EVENT, uri=uri, target_ref = ?target_ref, version=dataset.manifest.version, status="success");
                Ok(dataset)
            }
            Err(e) => {
                info!(target: TRACE_DATASET_EVENTS, event=DATASET_LOADING_EVENT, uri=uri, target_ref = ?target_ref, status="error");
                Err(e)
            }
        }
    }

    // Runtime per-base overrides are supplied as storage options, but the dataset
    // ultimately resolves object stores from ObjectStoreParams. Normalize once in
    // the builder so reads only need to look up the prepared params by base path.
    fn merge_store_params_with_storage_options(
        params: &ObjectStoreParams,
        override_options: &HashMap<String, String>,
    ) -> ObjectStoreParams {
        if override_options.is_empty() {
            return params.clone();
        }

        let mut merged_params = params.clone();
        let mut merged_options = merged_params.storage_options().cloned().unwrap_or_default();
        merged_options.extend(override_options.clone());

        let storage_options_accessor = match merged_params
            .storage_options_accessor
            .as_ref()
            .and_then(|accessor| accessor.provider().cloned())
        {
            Some(provider) => Arc::new(StorageOptionsAccessor::with_initial_and_provider(
                merged_options,
                provider,
            )),
            None => Arc::new(StorageOptionsAccessor::with_static_options(merged_options)),
        };
        merged_params.storage_options_accessor = Some(storage_options_accessor);
        merged_params
    }

    async fn load_impl(mut self) -> Result<Dataset> {
        // Apply storage_options_override to merge namespace client options with any existing accessor
        if let Some(override_opts) = self.storage_options_override.take() {
            self.options =
                Self::merge_store_params_with_storage_options(&self.options, &override_opts);
        }

        let index_cache_backend = self.index_cache_backend.take();
        let session = match self.session.as_ref() {
            Some(session) => session.clone(),
            None => match index_cache_backend {
                Some(backend) => Arc::new(Session::with_index_cache_backend(
                    backend,
                    self.metadata_cache_size_bytes,
                    Default::default(),
                )),
                None => Arc::new(Session::new(
                    self.index_cache_size_bytes,
                    self.metadata_cache_size_bytes,
                    Default::default(),
                )),
            },
        };

        let target_ref = self.version.clone();
        let table_uri = self.table_uri.clone();

        // How do we detect which version scheme is in use?
        let manifest = self.manifest.take();

        let file_reader_options = self.file_reader_options.clone();
        let store_params = self.options.clone();
        let base_store_params = (!self.base_store_params.is_empty())
            .then(|| Arc::new(std::mem::take(&mut self.base_store_params)));

        // A namespace-managed table is always addressed at its root uri, so the
        // effective branch is resolvable before loading: the base path is
        // qualified up front and the manifest store derives the branch from it.
        // An explicitly supplied commit handler opts out of the managed flow.
        let managed_store_active =
            self.namespace_managed.is_some() && self.commit_handler.is_none();

        let (object_store, base_path, commit_handler) = self.build_object_store().await?;

        // Two cases that need to check out after loading the manifest:
        // 1. If the target is configured as a branch, we need to check the branch field in the manifest
        // and reload the right branch in case the uri is not the right one.
        // 2. If the target is configured as a tag, and we don't find the tag under the table_uri,
        // we need to get the root_location after loading the manifest and get the right version.
        // In practice, we should try best to use the right uri and avoid double loading.
        let mut need_delay_checkout = false;
        let (mut branch, mut version_number) = match target_ref.clone() {
            Some(Ref::Version(branch, version_number)) => {
                if branch.is_some() && !managed_store_active {
                    need_delay_checkout = true;
                }
                (branch, version_number)
            }
            // We don't have a current branch context, just specify the branch as main
            // But the real branch will be specified by uri
            Some(Ref::VersionNumber(version_number)) => (None, Some(version_number)),
            // Here we assume the uri and path is the root.
            // If tag not found, we need to delay checkout after loading by uri
            Some(Ref::Tag(tag_name)) => {
                let refs = Refs::new(
                    object_store.clone(),
                    commit_handler.clone(),
                    BranchLocation {
                        path: base_path.clone(),
                        uri: table_uri.clone(),
                        branch: None,
                    },
                );
                match refs.tags().get(&tag_name).await {
                    Ok(tag_content) => {
                        if tag_content.branch.is_some() && !managed_store_active {
                            // The tag's chain lives under a different base path
                            // and the unmanaged handler resolves versions by
                            // base path only, so load the root's latest first
                            // and check the tag's branch/version out from it.
                            need_delay_checkout = true;
                            (tag_content.branch, None)
                        } else {
                            (tag_content.branch.clone(), Some(tag_content.version))
                        }
                    }
                    Err(e) => {
                        // A managed table is always rooted at the namespace
                        // location, so a tag missing here is missing.
                        if managed_store_active {
                            return Err(e);
                        }
                        need_delay_checkout = true;
                        (None, None)
                    }
                }
            }
            None => (None, None),
        };

        // Reject malformed branch names at the boundary (mirroring the branch
        // CRUD paths) so they fail as InvalidRef instead of resolving oddly
        if let Some(branch_name) = branch.as_deref()
            && !Branches::is_main_branch(Some(branch_name))
        {
            check_valid_branch(branch_name)?;
        }

        // For a managed table the branch is known before loading; point the base
        // path and uri at the branch chain so the loaded dataset is rooted there
        // (data placement, refs and the path-derived store branch all follow the
        // base path).
        let (base_path, table_uri) = if managed_store_active && branch.is_some() {
            let branch_location = BranchLocation {
                path: base_path,
                uri: table_uri,
                branch: None,
            }
            .find_branch(branch.as_deref())?;
            (branch_location.path, branch_location.uri)
        } else {
            (base_path, table_uri)
        };

        let dataset = Self::load_by_uri(
            session,
            manifest,
            file_reader_options,
            table_uri,
            version_number,
            object_store,
            base_path,
            commit_handler,
            Some(store_params),
            base_store_params,
        )
        .await?;

        if managed_store_active {
            // The base path was qualified above, so the loaded manifest must
            // already be on the requested branch; a mismatch means the namespace
            // resolved another chain.
            let requested_branch = branch.as_deref().and_then(standardize_branch);
            if dataset.manifest.branch.as_deref() != requested_branch.as_deref() {
                return Err(Error::internal(format!(
                    "open of branch '{}' resolved a manifest belonging to branch '{}'",
                    normalize_branch(branch.as_deref()),
                    normalize_branch(dataset.manifest.branch.as_deref()),
                )));
            }
        }

        if need_delay_checkout {
            if let Some(Ref::Tag(tag_name)) = target_ref {
                let tag_content = dataset.tags().get(tag_name.as_str()).await?;
                branch = tag_content.branch.clone();
                version_number = Some(tag_content.version);
            }

            if branch.as_deref() != dataset.manifest.branch.as_deref() {
                return dataset
                    .checkout_version((branch.as_deref(), version_number))
                    .await;
            }
        }
        if let Some(version_number) = version_number
            && version_number != dataset.manifest.version
        {
            return Err(Error::VersionNotFound {
                message: format!("version {} not found", version_number),
            });
        }
        Ok(dataset)
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn load_by_uri(
        session: Arc<Session>,
        manifest: Option<Manifest>,
        file_reader_options: Option<FileReaderOptions>,
        table_uri: String,
        version_number: Option<u64>,
        object_store: Arc<ObjectStore>,
        base_path: Path,
        commit_handler: Arc<dyn CommitHandler>,
        store_params: Option<ObjectStoreParams>,
        base_store_params: Option<Arc<HashMap<String, ObjectStoreParams>>>,
    ) -> Result<Dataset> {
        let (manifest, location) = if let Some(mut manifest) = manifest {
            let location = commit_handler
                .resolve_version_location(&base_path, manifest.version, &object_store.inner)
                .await?;
            if manifest.schema.has_dictionary_types() {
                let reader = object_store.open(&location.path).await?;
                populate_manifest_schema_dictionaries(&mut manifest, reader.as_ref()).await?;
            }
            (Arc::new(manifest), location)
        } else {
            let manifest_location = match version_number {
                Some(version) => {
                    let target_manifest_result = commit_handler
                        .resolve_version_location(&base_path, version, &object_store.inner)
                        .await;
                    // This may fail due to the uri is not the right branch
                    // In this case we should try to load the latest version and checkout the right branch and version_number
                    match target_manifest_result {
                        Ok(manifest_location) => manifest_location,
                        Err(e) => {
                            if let Error::VersionNotFound { message: _ } = e {
                                // If the version is not found, we need to try to load the latest version.
                                commit_handler
                                    .resolve_latest_location(&base_path, &object_store)
                                    .await?
                            } else {
                                return Err(e);
                            }
                        }
                    }
                }
                None => commit_handler
                    .resolve_latest_location(&base_path, &object_store)
                    .await
                    .map_err(|e| match &e {
                        Error::NotFound { .. } => {
                            Error::dataset_not_found(base_path.to_string(), Box::new(e))
                        }
                        _ => e,
                    })?,
            };

            let manifest = Dataset::get_manifest(
                &object_store,
                &manifest_location,
                &table_uri,
                session.as_ref(),
            )
            .await?;
            (manifest, manifest_location)
        };

        Dataset::checkout_manifest(
            object_store,
            base_path,
            table_uri,
            manifest,
            location,
            session,
            commit_handler,
            file_reader_options,
            store_params,
            base_store_params,
        )
    }
}
