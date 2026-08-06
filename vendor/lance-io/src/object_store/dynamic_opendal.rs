// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashMap;
use std::fmt;
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream, stream::BoxStream};
use object_store::path::Path;
use object_store::{
    CopyOptions, GetOptions, GetResult, ListResult, MultipartUpload, ObjectMeta,
    ObjectStore as OSObjectStore, PutMultipartOptions, PutOptions, PutPayload, PutResult,
    RenameOptions,
};
use object_store_opendal::OpendalStore;
use tokio::sync::RwLock;

use crate::object_store::StorageOptionsAccessor;
use lance_core::Result;

type NormalizeConfigFn = fn(&HashMap<String, String>) -> Result<HashMap<String, String>>;
type BuildStoreFn = fn(HashMap<String, String>) -> Result<OpendalStore>;
type FilterDynamicOptionsFn = fn(&HashMap<String, String>) -> HashMap<String, String>;

#[derive(Debug, Clone)]
struct CachedOpenDalStore {
    config: HashMap<String, String>,
    store: Arc<OpendalStore>,
}

#[derive(Clone)]
pub(in crate::object_store) struct DynamicOpenDalStore {
    name: Arc<str>,
    base_options: Arc<HashMap<String, String>>,
    accessor: Arc<StorageOptionsAccessor>,
    normalize_config: NormalizeConfigFn,
    build_store: BuildStoreFn,
    filter_dynamic_options: Option<FilterDynamicOptionsFn>,
    atomic_key_groups: Vec<Vec<&'static str>>,
    protected_keys: Vec<&'static str>,
    cache: Arc<RwLock<Option<CachedOpenDalStore>>>,
}

impl fmt::Debug for DynamicOpenDalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicOpenDalStore")
            .field("name", &self.name)
            .field("accessor", &self.accessor)
            .finish()
    }
}

impl DynamicOpenDalStore {
    pub(in crate::object_store) fn new(
        name: impl Into<Arc<str>>,
        base_options: HashMap<String, String>,
        accessor: Arc<StorageOptionsAccessor>,
        normalize_config: NormalizeConfigFn,
        build_store: BuildStoreFn,
    ) -> Self {
        Self {
            name: name.into(),
            base_options: Arc::new(base_options),
            accessor,
            normalize_config,
            build_store,
            filter_dynamic_options: None,
            atomic_key_groups: Vec::new(),
            protected_keys: Vec::new(),
            cache: Arc::new(RwLock::new(None)),
        }
    }

    #[allow(dead_code)]
    pub(in crate::object_store) fn with_protected_keys(
        mut self,
        keys: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        self.protected_keys = keys.into_iter().collect();
        self
    }

    /// Restrict provider-vended updates before they are merged into the fixed store config.
    pub(in crate::object_store) fn with_dynamic_options_filter(
        mut self,
        filter: FilterDynamicOptionsFn,
    ) -> Self {
        self.filter_dynamic_options = Some(filter);
        self
    }

    /// Treat a set of related options as one authority when provider values are present.
    pub(in crate::object_store) fn with_atomic_key_group(
        mut self,
        keys: impl IntoIterator<Item = &'static str>,
    ) -> Self {
        self.atomic_key_groups.push(keys.into_iter().collect());
        self
    }

    fn merge_options(
        &self,
        mut dynamic_options: HashMap<String, String>,
    ) -> HashMap<String, String> {
        if let Some(filter) = self.filter_dynamic_options {
            dynamic_options = filter(&dynamic_options);
        }
        for key in &self.protected_keys {
            dynamic_options.remove(*key);
        }
        let mut merged = self.base_options.as_ref().clone();
        for group in &self.atomic_key_groups {
            if group.iter().any(|key| dynamic_options.contains_key(*key)) {
                for key in group {
                    merged.remove(*key);
                }
            }
        }
        merged.extend(dynamic_options);
        merged
    }

    pub(in crate::object_store) async fn current_store(&self) -> Result<Arc<OpendalStore>> {
        let merged_options = self.merge_options(self.accessor.get_storage_options().await?.0);
        let config = (self.normalize_config)(&merged_options)?;

        // Cache reuse depends on exact normalized config equality. Providers
        // should return stable, canonicalized values for semantically identical
        // configurations to avoid unnecessary store rebuilds.
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.as_ref()
                && cached.config == config
            {
                return Ok(cached.store.clone());
            }
        }

        let store = Arc::new((self.build_store)(config.clone())?);
        let mut cache = self.cache.write().await;
        if let Some(cached) = cache.as_ref()
            && cached.config == config
        {
            return Ok(cached.store.clone());
        }

        *cache = Some(CachedOpenDalStore {
            config,
            store: store.clone(),
        });
        Ok(store)
    }

    fn map_store_error(&self, error: lance_core::Error) -> object_store::Error {
        object_store::Error::Generic {
            store: "DynamicOpenDalStore",
            source: Box::new(error),
        }
    }
}

impl fmt::Display for DynamicOpenDalStore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "DynamicOpenDalStore({})", self.name)
    }
}

#[async_trait::async_trait]
impl OSObjectStore for DynamicOpenDalStore {
    async fn put_opts(
        &self,
        location: &Path,
        payload: PutPayload,
        opts: PutOptions,
    ) -> object_store::Result<PutResult> {
        self.current_store()
            .await
            .map_err(|e| self.map_store_error(e))?
            .put_opts(location, payload, opts)
            .await
    }

    async fn put_multipart_opts(
        &self,
        location: &Path,
        opts: PutMultipartOptions,
    ) -> object_store::Result<Box<dyn MultipartUpload>> {
        self.current_store()
            .await
            .map_err(|e| self.map_store_error(e))?
            .put_multipart_opts(location, opts)
            .await
    }

    async fn get_opts(
        &self,
        location: &Path,
        options: GetOptions,
    ) -> object_store::Result<GetResult> {
        self.current_store()
            .await
            .map_err(|e| self.map_store_error(e))?
            .get_opts(location, options)
            .await
    }

    async fn get_ranges(
        &self,
        location: &Path,
        ranges: &[Range<u64>],
    ) -> object_store::Result<Vec<Bytes>> {
        self.current_store()
            .await
            .map_err(|e| self.map_store_error(e))?
            .get_ranges(location, ranges)
            .await
    }

    fn delete_stream(
        &self,
        locations: BoxStream<'static, object_store::Result<Path>>,
    ) -> BoxStream<'static, object_store::Result<Path>> {
        let this = self.clone();
        stream::once(async move {
            let store = this
                .current_store()
                .await
                .map_err(|e| this.map_store_error(e))?;
            Ok::<_, object_store::Error>((store, locations))
        })
        .map_ok(|(store, locations)| store.delete_stream(locations))
        .try_flatten()
        .boxed()
    }

    fn list(&self, prefix: Option<&Path>) -> BoxStream<'static, object_store::Result<ObjectMeta>> {
        let prefix = prefix.cloned();
        let this = self.clone();
        stream::once(async move {
            this.current_store()
                .await
                .map_err(|e| this.map_store_error(e))
        })
        .map_ok(move |store| store.list(prefix.as_ref()))
        .try_flatten()
        .boxed()
    }

    async fn list_with_delimiter(&self, prefix: Option<&Path>) -> object_store::Result<ListResult> {
        self.current_store()
            .await
            .map_err(|e| self.map_store_error(e))?
            .list_with_delimiter(prefix)
            .await
    }

    async fn copy_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: CopyOptions,
    ) -> object_store::Result<()> {
        self.current_store()
            .await
            .map_err(|e| self.map_store_error(e))?
            .copy_opts(from, to, opts)
            .await
    }

    async fn rename_opts(
        &self,
        from: &Path,
        to: &Path,
        opts: RenameOptions,
    ) -> object_store::Result<()> {
        self.current_store()
            .await
            .map_err(|e| self.map_store_error(e))?
            .rename_opts(from, to, opts)
            .await
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use opendal::{Operator, services::Memory};

    use super::*;
    use crate::object_store::test_utils::StaticMockStorageOptionsProvider;

    #[tokio::test]
    async fn test_dynamic_store_caches_by_normalized_config() {
        let accessor = Arc::new(StorageOptionsAccessor::with_provider(Arc::new(
            StaticMockStorageOptionsProvider {
                options: HashMap::from([("token".to_string(), "value".to_string())]),
            },
        )));

        let store = DynamicOpenDalStore::new(
            "memory",
            HashMap::new(),
            accessor,
            |options| Ok(options.clone()),
            |_| {
                let operator = Operator::new(Memory::default()).map_err(|e| {
                    lance_core::Error::invalid_input(format!(
                        "Failed to create memory operator: {e:?}"
                    ))
                })?;
                Ok(OpendalStore::new(operator))
            },
        );

        let first = store
            .current_store()
            .await
            .expect("first store should build");
        let second = store
            .current_store()
            .await
            .expect("second store should reuse cache");

        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn test_merge_options_preserves_protected_base_keys() {
        let accessor = Arc::new(StorageOptionsAccessor::with_provider(Arc::new(
            StaticMockStorageOptionsProvider {
                options: HashMap::new(),
            },
        )));
        let store = DynamicOpenDalStore::new(
            "memory",
            HashMap::from([
                ("bucket".to_string(), "url-bucket".to_string()),
                ("root".to_string(), "/".to_string()),
                ("token".to_string(), "base-token".to_string()),
            ]),
            accessor,
            |options| Ok(options.clone()),
            |_| {
                let operator = Operator::new(Memory::default()).map_err(|e| {
                    lance_core::Error::invalid_input(format!(
                        "Failed to create memory operator: {e:?}"
                    ))
                })?;
                Ok(OpendalStore::new(operator))
            },
        )
        .with_protected_keys(["bucket", "root"]);

        let merged = store.merge_options(HashMap::from([
            ("bucket".to_string(), "provider-bucket".to_string()),
            ("root".to_string(), "/provider-root".to_string()),
            ("token".to_string(), "provider-token".to_string()),
        ]));

        assert_eq!(merged.get("bucket").unwrap(), "url-bucket");
        assert_eq!(merged.get("root").unwrap(), "/");
        assert_eq!(merged.get("token").unwrap(), "provider-token");
    }
}
