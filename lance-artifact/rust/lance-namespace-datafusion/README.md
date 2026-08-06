# Lance Namespace-DataFusion Integration

This crate provides a bridge between Lance Namespaces and Apache DataFusion, allowing Lance tables to be queried as if they were native DataFusion catalogs, schemas, and tables.

It exposes a `SessionBuilder` that constructs a DataFusion `SessionContext` with `CatalogProvider` and `SchemaProvider` implementations backed by a `lance_namespace::LanceNamespace` instance.

## Features

- **Dynamic Catalogs**: Maps top-level Lance namespaces to DataFusion catalogs.
- **Dynamic Schemas**: Maps child namespaces to DataFusion schemas.
- **Lazy Table Loading**: Tables are loaded on-demand from the namespace when queried.
- **Read-Only**: This integration focuses solely on providing read access (SQL `SELECT`) to Lance datasets. DML operations are not included.

## Usage

First, build a `LanceNamespace` (e.g., from a directory), then use the `SessionBuilder` to create a `SessionContext`.

```rust,ignore
use std::sync::Arc;
use datafusion::prelude::SessionContext;
use lance_namespace_datafusion::SessionBuilder;
use lance_namespace::LanceNamespace;
use lance_namespace_impls::DirectoryNamespaceBuilder;

async fn run_query() {
    // 1. Create a Lance Namespace
    let temp_dir = tempfile::tempdir().unwrap();
    let ns: Arc<dyn LanceNamespace> = Arc::new(
        DirectoryNamespaceBuilder::new(temp_dir.path().to_string_lossy().to_string())
            .build()
            .await
            .unwrap(),
    );

    // 2. Build a DataFusion SessionContext
    let ctx = SessionBuilder::new()
        .with_root(ns.into())
        .build()
        .await
        .unwrap();

    // 3. Run a SQL query
    let df = ctx.sql("SELECT * FROM my_catalog.my_schema.my_table").await.unwrap();
    df.show().await.unwrap();
}
```
