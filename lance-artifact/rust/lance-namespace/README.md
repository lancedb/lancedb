# lance-namespace

Lance Namespace Core APIs for managing namespaces and tables.

## Overview

This crate provides the core APIs and trait definitions for Lance namespaces, including:

- `LanceNamespace` trait - The main interface for namespace operations
- Schema conversion utilities for Arrow schemas
- Models and APIs for namespace operations (via lance-namespace-reqwest-client)

**Note**: For actual namespace implementations (REST, Directory, etc.), see the `lance-namespace-impls` crate.

## Features

The namespace API supports:

- Creating and managing namespaces
- Creating and managing tables within namespaces
- Listing namespaces and tables
- Schema management
- Multiple backend implementations (REST, directory-based, etc.)

## Usage

```rust
use lance_namespace::LanceNamespace;

// For implementations, use lance-namespace-impls:
// use lance_namespace_impls::connect;
// let namespace = connect("rest", properties).await?;
// let namespace = connect("dir", properties).await?;

// Then use the trait methods:
async fn example(namespace: &dyn LanceNamespace) {
    // List tables in the namespace
    let tables = namespace.list_tables(Default::default()).await;
}
```

## Documentation

For more information about Lance and its namespace system, see the [Lance Namespace documentation](https://lance.org/format/namespace).
