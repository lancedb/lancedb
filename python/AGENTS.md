These are the Python bindings of LanceDB.
The core Rust library is in the `../rust/lancedb` directory, the rust binding
code is in the `src/` directory and the Python bindings are in the `lancedb/` directory.

Common commands:

* Bootstrap dev env: `uv run --extra tests --extra dev maturin develop --extras tests,dev`
* Build: `make develop`
* Format: `make format`
* Lint: `make check`
* Fix lints: `make fix`
* Test: `uv run --extra tests pytest python/tests -vv --durations=10 -m "not slow and not s3_test"`
* Run specific test: `uv run --extra tests pytest python/tests/<test_file>.py::<test_name> -q`
* Doc test: `uv run --extra tests pytest --doctest-modules python/lancedb`

Use the uv-managed environment declared by `uv.lock` for Python validation. Do
not treat system `python`, global `pytest`, or missing editable-install errors
as final blockers; bootstrap or enter the uv environment instead. `make test`
and `make doctest` assume the development environment is already prepared.

Before committing changes, run lints and then formatting.

When you change the Rust code, PyO3 binding code, or see a missing/stale
`lancedb._lancedb`, recompile the Python bindings with
`uv run --extra tests --extra dev maturin develop --extras tests,dev` before
running tests.

When you export new types from Rust to Python, you must manually update `python/lancedb/_lancedb.pyi`
with the corresponding type hints. You can run `pyright` to check for type errors in the Python code.
