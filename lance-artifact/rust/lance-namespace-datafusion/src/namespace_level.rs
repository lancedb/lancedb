// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::sync::Arc;

use lance::dataset::builder::DatasetBuilder;
use lance::{Dataset, Result};
use lance_namespace::LanceNamespace;
use lance_namespace::models::{ListNamespacesRequest, ListTablesRequest};

const DEFAULT_NAMESPACE_NAME: &str = "lance";

/// Lightweight wrapper around a Lance namespace handle and identifier.
#[derive(Debug, Clone)]
pub struct NamespaceLevel {
    root: Arc<dyn LanceNamespace>,
    /// Full namespace identifier, e.g. [catalog, schema].
    namespace_id: Option<Vec<String>>,
}

impl From<Arc<dyn LanceNamespace>> for NamespaceLevel {
    fn from(lance_namespace: Arc<dyn LanceNamespace>) -> Self {
        Self::from_root(Arc::clone(&lance_namespace))
    }
}

impl From<(Arc<dyn LanceNamespace>, String)> for NamespaceLevel {
    fn from(lance_namespace: (Arc<dyn LanceNamespace>, String)) -> Self {
        Self::from_namespace(Arc::clone(&lance_namespace.0), vec![lance_namespace.1])
    }
}

impl From<(Arc<dyn LanceNamespace>, Vec<String>)> for NamespaceLevel {
    fn from(lance_namespace: (Arc<dyn LanceNamespace>, Vec<String>)) -> Self {
        Self::from_namespace(Arc::clone(&lance_namespace.0), lance_namespace.1)
    }
}

impl NamespaceLevel {
    /// Construct a namespace rooted at the top-level Lance namespace.
    pub fn from_root(root: Arc<dyn LanceNamespace>) -> Self {
        Self {
            root,
            namespace_id: None,
        }
    }

    /// Construct a namespace for a specific child identifier under the root.
    pub fn from_namespace(root: Arc<dyn LanceNamespace>, namespace_id: Vec<String>) -> Self {
        Self {
            root,
            namespace_id: Some(namespace_id),
        }
    }

    /// Return the full namespace identifier.
    pub fn id(&self) -> Vec<String> {
        self.namespace_id.clone().unwrap_or_default()
    }

    /// Name for this namespace (last component or default).
    pub fn name(&self) -> &str {
        self.namespace_id
            .as_deref()
            .and_then(|v| v.last())
            .map_or(DEFAULT_NAMESPACE_NAME, |relative_name| {
                relative_name.as_str()
            })
    }

    fn child_id(&self, child_name: String) -> Vec<String> {
        match &self.namespace_id {
            Some(namespace_id) => {
                let mut child_namespace = namespace_id.clone();
                child_namespace.push(child_name);
                child_namespace
            }
            None => vec![child_name],
        }
    }

    /// List direct child namespaces.
    pub async fn children(&self) -> Result<Vec<Self>> {
        let root = Arc::clone(&self.root);
        let namespace_id = self.namespace_id.clone().unwrap_or_default();
        let request = ListNamespacesRequest {
            id: Some(namespace_id.clone()),
            page_token: None,
            limit: None,
            ..Default::default()
        };

        let namespaces = root.list_namespaces(request).await?.namespaces;

        Ok(namespaces
            .into_iter()
            .map(|relative_ns_id| {
                Self::from_namespace(Arc::clone(&self.root), self.child_id(relative_ns_id))
            })
            .collect())
    }

    /// List table names under this namespace.
    pub async fn tables(&self) -> Result<Vec<String>> {
        let root = Arc::clone(&self.root);
        let namespace_id = self.namespace_id.clone().unwrap_or_default();
        let request = ListTablesRequest {
            id: Some(namespace_id),
            page_token: None,
            limit: None,
            ..Default::default()
        };

        root.list_tables(request).await.map(|resp| resp.tables)
    }

    /// Load a Lance dataset for the given table name in this namespace.
    pub async fn load_dataset(&self, table_name: &str) -> Result<Dataset> {
        DatasetBuilder::from_namespace(
            Arc::clone(&self.root),
            self.child_id(table_name.to_string()),
        )
        .await?
        .load()
        .await
    }
}
