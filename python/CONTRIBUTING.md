# Contributing to LanceDB Python

This documents outlines the process for contributing to LanceDB Python.
For general contribution guidelines, see [CONTRIBUTING.md](../CONTRIBUTING.md).

## Project layout

The Python package is a wrapper around the Rust library, `lancedb`. We use
[pyo3](https://pyo3.rs/) to create the bindings between Rust and Python.

* `src/`: Rust bindings source code
* `python/lancedb`: Python package source code
* `python/tests`: Unit tests

## Development environment

To set up your development environment, you will need to install the following:

1. Python 3.9 or later
2. Cargo (Rust's package manager). Use [rustup](https://rustup.rs/) to install.
3. [protoc](https://grpc.io/docs/protoc-installation/) (Protocol Buffers compiler)

Create a virtual environment to work in:

```bash
python -m venv venv
source venv/bin/activate
pip install maturin
```

### Commit Hooks

It is **highly recommended** to install the pre-commit hooks to ensure that your
code is formatted correctly and passes basic checks before committing:

```bash
make develop # this will install pre-commit itself
pre-commit install
```

## Development

Most common development commands can be run using the Makefile.

Build the package

```shell
make develop
```

Format:

```shell
make format
```

Run tests:

```shell
make test
make doctest
```

To run a single test, you can use the `pytest` command directly. Provide the path
to the test file, and optionally the test name after `::`.

```shell
# Single file: test_table.py
pytest -vv python/tests/test_table.py
# Single test: test_basic in test_table.py
pytest -vv python/tests/test_table.py::test_basic
```

To see all commands, run:

```shell
make help
```
