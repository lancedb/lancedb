// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::collections::HashSet;
use std::sync::Arc;

use dashmap::DashMap;
use datafusion::catalog::{CatalogProvider, CatalogProviderList, SchemaProvider};
use datafusion::error::Result;

#[allow(unused_imports)]
use crate::SessionBuilder;
use crate::namespace_level::NamespaceLevel;
use crate::schema::LanceSchemaProvider;

/// A dynamic [`CatalogProviderList`] that maps Lance namespaces to catalogs.
///
/// The underlying namespace must be a four-level namespace. It is explicitly configured
/// via [`SessionBuilder::with_root`], and each child namespace under this root is
/// automatically registered as a [`LanceCatalogProvider`].
///
/// This `CatalogProviderList` is optional when building a DataFusion `SessionContext`.
/// If not provided, you can still configure catalogs using
/// [`SessionBuilder::add_catalog`] or set a default catalog via
/// [`SessionBuilder::with_default_catalog`].
#[derive(Debug, Clone)]
pub struct LanceCatalogProviderList {
    /// Root Lance namespace used to resolve catalogs / schemas / tables.
    #[allow(dead_code)]
    ns_level: NamespaceLevel,
    /// Catalogs that have been loaded from the root namespace.
    ///
    /// Note: The values in this map may become stale over time, as there is currently
    /// no mechanism to automatically refresh or invalidate cached catalog providers.
    catalogs: DashMap<String, Arc<dyn CatalogProvider>>,
}

impl LanceCatalogProviderList {
    pub async fn try_new(namespace: NamespaceLevel) -> Result<Self> {
        let catalogs = DashMap::new();
        for child_namespace in namespace.children().await? {
            let catalog_name = child_namespace.name().to_string();
            let catalog_provider = Arc::new(LanceCatalogProvider::try_new(child_namespace).await?);
            catalogs.insert(catalog_name, catalog_provider as Arc<dyn CatalogProvider>);
        }

        Ok(Self {
            ns_level: namespace,
            catalogs,
        })
    }
}

impl CatalogProviderList for LanceCatalogProviderList {
    /// Adds a new catalog to this catalog list.
    /// If a catalog of the same name existed before, it is replaced in the list and returned.
    fn register_catalog(
        &self,
        name: String,
        catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs.insert(name, catalog)
    }

    fn catalog_names(&self) -> Vec<String> {
        self.catalogs
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        self.catalogs
            .get(name)
            .map(|entry| Arc::clone(entry.value()))
    }
}

/// A dynamic [`CatalogProvider`] that exposes the immediate child namespaces
/// of a Lance namespace as database schemas.
///
/// The underlying namespace must be a three-level namespace. It is either explicitly
/// registered via [`SessionBuilder::add_catalog`], or automatically created as part of
/// the catalog hierarchy when [`SessionBuilder::with_root`] is used.
/// Child namespaces are automatically loaded as [`LanceSchemaProvider`] instances.
#[derive(Debug, Clone)]
pub struct LanceCatalogProvider {
    #[allow(dead_code)]
    ns_level: NamespaceLevel,
    /// Note: The values in this map may become stale over time, as there is currently
    /// no mechanism to automatically refresh or invalidate cached schema providers.
    schemas: DashMap<String, Arc<dyn SchemaProvider>>,
}

impl LanceCatalogProvider {
    pub async fn try_new(namespace: NamespaceLevel) -> Result<Self> {
        let schemas = DashMap::new();
        for child_namespace in namespace.children().await? {
            let schema_name = child_namespace.name().to_string();
            let schema_provider = Arc::new(LanceSchemaProvider::try_new(child_namespace).await?);
            schemas.insert(schema_name, schema_provider as Arc<dyn SchemaProvider>);
        }

        Ok(Self {
            ns_level: namespace,
            schemas,
        })
    }
}

impl CatalogProvider for LanceCatalogProvider {
    fn schema_names(&self) -> Vec<String> {
        self.schemas
            .iter()
            .map(|entry| entry.key().clone())
            .collect::<HashSet<_>>()
            .into_iter()
            .collect()
    }

    fn schema(&self, schema_name: &str) -> Option<Arc<dyn SchemaProvider>> {
        self.schemas
            .get(schema_name)
            .map(|entry| Arc::clone(entry.value()))
    }

    fn register_schema(
        &self,
        name: &str,
        schema: Arc<dyn SchemaProvider>,
    ) -> Result<Option<Arc<dyn SchemaProvider>>> {
        Ok(self.schemas.insert(name.to_string(), schema))
    }
}
