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

* Check for compiler errors: `cargo check --quiet --features remote --tests --examples`
* Run tests: `cargo test --quiet --features remote --tests`
* Run specific test: `cargo test --quiet --features remote -p <package_name> --test <test_name>`
* Lint: `cargo clippy --quiet --features remote --tests --examples`
* Format: `cargo fmt --all`

Before committing changes, run formatting.

## Coding tips

* When writing Rust doctests for things that require a connection or table reference,
  write them as a function instead of a fully executable test. This allows type checking
  to run but avoids needing a full test environment. For example:
    ```rust
    /// ```
    /// use lance_index::scalar::FullTextSearchQuery;
    /// use lancedb::query::{QueryBase, ExecutableQuery};
    ///
    /// # use lancedb::Table;
    /// # async fn query(table: &Table) -> Result<(), Box<dyn std::error::Error>> {
    /// let results = table.query()
    ///     .full_text_search(FullTextSearchQuery::new("hello world".into()))
    ///     .execute()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ```

## Example plan: adding a new method on Table

Adding a new method involves first adding it to the Rust core, then exposing it
in the Python and TypeScript bindings. There are both local and remote tables.
Remote tables are implemented via a HTTP API and require the `remote` cargo
feature flag to be enabled. Python has both sync and async methods.

Rust core changes:

1. Add method on `Table` struct in `rust/lancedb/src/table.rs` (calls `BaseTable` trait).
2. Add method to `BaseTable` trait in `rust/lancedb/src/table.rs`.
3. Implement new trait method on `NativeTable` in `rust/lancedb/src/table.rs`.
    * Test with unit test in `rust/lancedb/src/table.rs`.
4. Implement new trait method on `RemoteTable` in `rust/lancedb/src/remote/table.rs`.
    * Test with unit test in `rust/lancedb/src/remote/table.rs` against mocked endpoint.

Python bindings changes:

1. Add PyO3 method binding in `python/src/table.rs`. Run `make develop` to compile bindings.
2. Add types for PyO3 method in `python/python/lancedb/_lancedb.pyi`.
3. Add method to `AsyncTable` class in `python/python/lancedb/table.py`.
4. Add abstract method to `Table` abstract base class in `python/python/lancedb/table.py`.
5. Add concrete sync method to `LanceTable` class in `python/python/lancedb/table.py`.
    * Should use `LOOP.run()` to call the corresponding `AsyncTable` method.
6. Add concrete sync method to `RemoteTable` class in `python/python/lancedb/remote/table.py`.
7. Add unit test in `python/tests/test_table.py`.

TypeScript bindings changes:

1. Add napi-rs method binding on `Table` in `nodejs/src/table.rs`.
2. Run `npm run build` to generate TypeScript definitions.
3. Add typescript method on abstract class `Table` in `nodejs/src/table.ts`.
4. Add concrete method on `LocalTable` class in `nodejs/src/native_table.ts`.
    * Note: despite the name, this class is also used for remote tables.
5. Add test in `nodejs/__test__/table.test.ts`.
6. Run `npm run docs` to generate TypeScript documentation.
