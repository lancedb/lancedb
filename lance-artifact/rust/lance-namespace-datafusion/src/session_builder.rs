// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use datafusion::catalog::{CatalogProvider, SchemaProvider};
use datafusion::error::Result;
use datafusion::execution::context::{SessionConfig, SessionContext};
use std::sync::Arc;

use crate::LanceCatalogProvider;
use crate::catalog::LanceCatalogProviderList;
use crate::namespace_level::NamespaceLevel;

/// Builder for configuring a `SessionContext` with Lance namespaces.
#[derive(Clone, Debug, Default)]
pub struct SessionBuilder {
    /// Optional root namespace exposed via a dynamic
    /// `LanceCatalogProviderList`.
    root: Option<NamespaceLevel>,
    /// Explicit catalogs to register by name.
    catalogs: Vec<(String, NamespaceLevel)>,
    /// Optional DataFusion session configuration.
    config: Option<SessionConfig>,
    /// Optional default catalog name.
    /// It will override the default catalog name in [`SessionBuilder::config`] if set
    default_catalog: Option<String>,
    /// Optional default catalog provider.
    default_catalog_provider: Option<Arc<dyn CatalogProvider>>,
    /// Optional default schema name.
    /// It will override the default schema name in [`SessionBuilder::config`] if set
    default_schema: Option<String>,
    /// Optional default schema provider.
    default_schema_provider: Option<Arc<dyn SchemaProvider>>,
}

impl SessionBuilder {
    /// Create a new builder with no namespaces or configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Attach a root `LanceNamespace` that is exposed as a dynamic
    /// catalog list via `LanceCatalogProviderList`.
    pub fn with_root(mut self, ns: NamespaceLevel) -> Self {
        self.root = Some(ns);
        self
    }

    /// Register an additional catalog backed by the given namespace.
    ///
    /// The catalog is identified by `name` and can later be combined
    /// with schemas via `SessionBuilder::add_schema` using the same
    /// namespace.
    pub fn add_catalog(mut self, name: &str, ns: NamespaceLevel) -> Self {
        self.catalogs.push((name.to_string(), ns));
        self
    }

    /// Provide an explicit `SessionConfig` for the underlying
    /// `SessionContext`.
    pub fn with_config(mut self, config: SessionConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Override the default catalog name used by the session.
    pub fn with_default_catalog(
        mut self,
        name: &str,
        catalog_provider: Option<Arc<dyn CatalogProvider>>,
    ) -> Self {
        self.default_catalog = Some(name.to_string());
        self.default_catalog_provider = catalog_provider;
        self
    }

    /// Override the default schema name used by the session.
    pub fn with_default_schema(
        mut self,
        name: &str,
        schema_provider: Option<Arc<dyn SchemaProvider>>,
    ) -> Self {
        self.default_schema = Some(name.to_string());
        self.default_schema_provider = schema_provider;
        self
    }

    /// Build a `SessionContext` with all configured namespaces.
    pub async fn build(self) -> Result<SessionContext> {
        self.check_params_valid()?;
        let config = self.config.unwrap_or_default();
        let options = config.options();
        let default_catalog = self
            .default_catalog
            .unwrap_or_else(|| options.catalog.default_catalog.clone());
        let default_schema = self
            .default_schema
            .unwrap_or_else(|| options.catalog.default_schema.clone());

        let ctx = SessionContext::new_with_config(
            config
                .with_default_catalog_and_schema(default_catalog.as_str(), default_schema.as_str()),
        );

        if let Some(root) = self.root {
            let catalog_list = Arc::new(LanceCatalogProviderList::try_new(root).await?);
            ctx.register_catalog_list(catalog_list);
        }

        for (catalog_name, namespace) in self.catalogs {
            ctx.register_catalog(
                catalog_name,
                Arc::new(LanceCatalogProvider::try_new(namespace).await?),
            );
        }
        if let Some(catalog_provider) = self.default_catalog_provider {
            if let Some(schema_provider) = self.default_schema_provider {
                catalog_provider.register_schema(default_schema.as_str(), schema_provider)?;
            }
            ctx.register_catalog(default_catalog.as_str(), catalog_provider);
        }

        Ok(ctx)
    }

    fn check_params_valid(&self) -> Result<()> {
        if let (None, Some(schema)) = (&self.default_catalog, &self.default_schema) {
            return Err(datafusion::error::DataFusionError::Internal(format!(
                "Default SchemaProvider {} must be used together with a default CatalogProvider",
                schema
            )));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::SessionBuilder;
    use std::sync::Arc;

    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::catalog::SchemaProvider;
    use datafusion::catalog::memory::{MemoryCatalogProvider, MemorySchemaProvider};
    use datafusion::common::record_batch;
    use datafusion::datasource::MemTable;
    use datafusion::error::Result;

    #[tokio::test]
    async fn default_catalog_and_schema_are_used_for_sql_queries() -> Result<()> {
        // Construct a simple in-memory orders table using the same style as tests/sql.rs.
        let batch = record_batch!(
            ("order_id", Int32, vec![101, 102, 103]),
            ("customer_id", Int32, vec![1, 2, 3]),
            ("amount", Int32, vec![100, 200, 300])
        )?;
        let schema = batch.schema();
        let table = Arc::new(MemTable::try_new(schema, vec![vec![batch]])?);

        // Create DataFusion's in-memory schema and catalog providers.
        let sales_schema = Arc::new(MemorySchemaProvider::new());
        let retail_catalog = Arc::new(MemoryCatalogProvider::new());
        sales_schema.register_table("orders".to_string(), table)?;

        // Build a SessionContext that uses the memory catalog/schema as defaults.
        let ctx = SessionBuilder::new()
            .with_default_catalog("retail", Some(retail_catalog))
            .with_default_schema("sales", Some(sales_schema))
            .build()
            .await?;

        let extract_count = |batches: &[RecordBatch]| -> i64 {
            let batch = &batches[0];
            let array = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("COUNT should return Int64Array");
            assert_eq!(array.len(), 1);
            array.value(0)
        };

        // Query using explicit schema name.
        let df_with_schema = ctx.sql("SELECT COUNT(*) AS c FROM sales.orders").await?;
        let batches_with_schema = df_with_schema.collect().await?;

        // Query relying on default catalog and schema.
        let df_without_schema = ctx.sql("SELECT COUNT(*) AS c FROM orders").await?;
        let batches_without_schema = df_without_schema.collect().await?;

        let count_with_schema = extract_count(&batches_with_schema);
        let count_without_schema = extract_count(&batches_without_schema);

        assert_eq!(count_with_schema, 3);
        assert_eq!(count_without_schema, 3);
        assert_eq!(count_with_schema, count_without_schema);

        Ok(())
    }
}
