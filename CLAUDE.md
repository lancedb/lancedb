LanceDB is a database designed for retrieval, including vector, full-text, and hybrid search.
It is a wrapper around Lance. There are two backends: local (in-process like SQLite) and
remote (against LanceDB Cloud).

The core of LanceDB is written in Rust. There are bindings in Python, Typescript, and Java.

Project layout:

* `rust/lancedb`: The LanceDB core Rust implementation.
* `python`: The Python bindings, using PyO3.
* `nodejs`: The Typescript bindings, using napi-rs
* `java`: The Java bindings

(`rust/ffi` and `node/` are for a deprecated package. You can ignore them.)

Common commands:

* Check for compiler errors: `cargo check --features remote --tests --examples`
* Run tests: `cargo test --features remote --tests`
* Run specific test: `cargo test --features remote -p <package_name> --test <test_name>`
* Lint: `cargo clippy --features remote --tests --examples`
* Format: `cargo fmt --all`

Before committing changes, run formatting.
