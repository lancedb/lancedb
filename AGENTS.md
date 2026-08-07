LanceDB is a database designed for retrieval, including vector, full-text, and hybrid search.
It is a wrapper around Lance. There are two backends: local (in-process like SQLite) and
remote (against LanceDB Cloud).

The core of LanceDB is written in Rust. There are bindings in Python, Typescript, and Java.

Project layout:

* `rust/lancedb`: The LanceDB core Rust implementation.
* `python`: The Python bindings, using PyO3.
* `nodejs`: The Typescript bindings, using napi-rs
* `java`: The Java bindings

Common commands:

* Check for compiler errors: `cargo check --quiet --features remote --tests --examples`
* Run tests: `cargo test --quiet --features remote --tests`
* Run specific test: `cargo test --quiet --features remote -p <package_name> --test <test_name>`
* Lint: `cargo clippy --quiet --features remote --tests --examples`
* Format Rust: `cargo fmt --all`
* Format Python: `ruff format .`
* Lint Python: `ruff check .`
* Bootstrap Python dev env: `cd python && uv run --extra tests --extra dev maturin develop --extras tests,dev`
* Run Python tests: `cd python && uv run --extra tests pytest python/tests -vv --durations=10 -m "not slow and not s3_test"`
* Run specific Python test: `cd python && uv run --extra tests pytest python/tests/<test_file>.py::<test_name> -q`

For Python validation, prefer the uv-managed environment declared by `python/uv.lock`.
Do not treat system `python`, global `pytest`, or missing editable-install errors as
final blockers; bootstrap or enter the uv environment instead. If `lancedb._lancedb`
is missing or stale, or if Rust/PyO3 binding code changed, rebuild the Python
extension with the bootstrap command above before running tests.

Before committing changes, run formatting for every language you touched. At minimum:

* Rust changes: run `cargo fmt --all`.
* Python changes: run `ruff format .` and `ruff check .` from the repository root,
  and run targeted tests through `cd python && uv run ...`.
* TypeScript changes: run the relevant `npm`/`pnpm` lint, format, build, and docs commands in `nodejs`.

Before creating a PR, the exact value passed to `gh pr create --title` must follow
Conventional Commits, such as `fix: support nested field paths in native index creation`
or `feat(python): add dataset multiprocessing support`. Do not use a plain natural
language summary like `Support nested field paths in native index creation` as the PR
title. The semantic-release check uses the PR title and body as the merge commit message,
so a non-conventional PR title will fail CI. After creating a PR, read the remote PR title
back and fix it immediately if it is not conventional.

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
8. If you added a new public class or module-level function (not just a method on an
   existing class), expose it in the API reference. See "Python API reference" below.

TypeScript bindings changes:

1. Add napi-rs method binding on `Table` in `nodejs/src/table.rs`.
2. Run `npm run build` to generate TypeScript definitions.
3. Add typescript method on abstract class `Table` in `nodejs/src/table.ts`.
4. Add concrete method on `LocalTable` class in `nodejs/src/native_table.ts`.
    * Note: despite the name, this class is also used for remote tables.
5. Add test in `nodejs/__test__/table.test.ts`.
6. Run `npm run docs` to generate TypeScript documentation.

## Python API reference

`docs/src/python/python.md` is the entire Python API reference. It is maintained by
hand, and anything not listed there is not rendered at all, so new public classes and
module-level functions have to be added explicitly. How depends on the module:

* `lancedb.index`, `lancedb.embeddings`, `lancedb.remote`, and `lancedb.rerankers` are
  rendered by a single directive each, driven by the module's `__all__`. Add the new
  name to `__all__` and it appears; forget, and it is silently omitted.
* Everything else (`lancedb`, `lancedb.table`, `lancedb.query`, `lancedb.db`, ...) is
  listed symbol by symbol. Add a `::: lancedb.<module>.<Name>` line to the matching
  section, and remember that the page separates synchronous and asynchronous APIs.

Deliberately undocumented: concrete implementations reached through an abstract base
(`LanceTable`, `LanceDBConnection`, `RemoteDBConnection`), query base classes already
covered by `inherited_members`, and internal helpers.

Cross-references in docstrings use mkdocstrings syntax, `[text][lancedb.table.Table]`.
Plain relative links such as `[Table](Table)` do not resolve. To check your work:

```shell
pip install -r docs/requirements.txt
cd docs && PYTHONPATH=. mkdocs build
```

The docs site only builds on pushes to `main`, so this is not covered by PR CI.

## Review Guidelines

Please consider the following when reviewing code contributions.

### Rust API design
* Design public APIs so they can be evolved easily in the future without breaking
  changes. Often this means using builder patterns or options structs instead of
  long argument lists.
* For public APIs, prefer inputs that use `Into<T>` or `AsRef<T>` traits to allow
  more flexible inputs. For example, use `name: Into<String>` instead of `name: String`,
  so we don't have to write `func("my_string".to_string())`.

### Testing
* Ensure all new public APIs have documentation and examples.
* Ensure that all bugfixes and features have corresponding tests. **We do not merge
  code without tests.**

### Documentation
* New features must include updates to the rust documentation comments. Link to
  relevant structs and methods to increase the value of documentation.

## Cursor Cloud specific instructions

The VM snapshot already has the Rust `1.97.0` toolchain (auto-selected by
`rust-toolchain.toml`), `protoc`, `uv` (on `PATH` via `~/.bashrc`), the Rust
debug build artifacts, the Python editable extension, and `nodejs/node_modules`.
The startup update script only refreshes dependencies (`uv sync` for Python and
`pnpm install` for Node); it deliberately does NOT rebuild the native
extensions. After changing Rust or PyO3/napi binding code you must rebuild the
affected binding yourself (see per-binding rebuild commands below).

Non-obvious caveats discovered during setup:

* The documented Python bootstrap `uv run --extra tests --extra dev maturin
  develop --extras tests,dev` does not work as-is here: `maturin` is not
  installed as a CLI in the uv environment, and `maturin develop --extras`
  runs its own dependency resolution that cannot find the prerelease
  `pylance==9.0.0rc1` (it lacks the extra package index that `uv` uses via
  `uv.lock`). Because `uv run --extra tests --extra dev` already installs those
  extras, the working command is:
  `cd python && uv run --extra tests --extra dev --with maturin maturin develop`
  (note: `--with maturin`, and no `--extras`). This is the Python binding
  rebuild command.
* Rust core, the Python extension (maturin), and the Node addon (napi) all
  compile into the SHARED `/workspace/target`. Cargo feature unification differs
  between `maturin develop` and `pnpm build`, so alternating between building
  the Python and Node bindings forces a full recompile of shared crates
  (`lancedb`, `datafusion`, `lance-*`) — roughly 6-7 min each way on this
  4-core VM. Build one binding at a time to avoid the churn.
* The `_lancedb` release build (triggered when `uv run`/`uv sync` installs the
  `lancedb` project itself) uses `lto = "fat"` + `opt-level = 3`, needs ~11 GB
  RAM, and takes ~20 min cold on this VM. To avoid it, the update script uses
  `uv sync --no-install-project --inexact` (the `--inexact` flag is required so
  the sync does not uninstall the editable extension). Prefer the debug
  `maturin develop` (~6 min cold, seconds when warm) for iteration.
* `cargo check` only produces metadata, so the first `cargo run --example ...`
  or `cargo test` after a check triggers a large codegen/link compile.
* Node binding rebuild: `cd nodejs && pnpm build` (napi debug build + `tsc`).
  The native addon lands at `nodejs/dist/lancedb.linux-x64-gnu.node`.

Verified working (local backend, no cloud credentials needed):

* Rust: `cargo check/clippy --features remote --tests --examples`,
  `cargo test --features remote -p lancedb --lib`, `cargo run --features remote
  --example simple`.
* Python: `cd python && uv run --extra tests pytest python/tests/test_table.py`,
  `uv run --directory python --extra dev ruff check python`.
* Node: `cd nodejs && pnpm lint`, `pnpm test __test__/connection.test.ts`.

Java (`java/`) is optional; its integration tests need LanceDB Cloud
credentials (`LANCEDB_DB`, `LANCEDB_API_KEY`) and were not set up here.
