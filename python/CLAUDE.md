These are the Python bindings of LanceDB.
The core Rust library is in the `../rust/lancedb` directory, the rust binding
code is in the `src/` directory and the Python bindings are in the `lancedb/` directory.

Common commands:

* Build: `make develop`
* Format: `make format`
* Lint: `make check`
* Fix lints: `make fix`
* Test: `make test`
* Doc test: `make doctest`

Before committing changes, run lints and then formatting.

When you change the Rust code, you will need to recompile the Python bindings: `make develop`.
