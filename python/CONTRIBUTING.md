# Contributing to LanceDB Python

This documents outlines the process for contributing to LanceDB Python.
For general contribution guidelines, see [CONTRIBUTING.md](../CONTRIBUTING.md).

## Development

LanceDb is based on the rust crate `lancedb` and is built with maturin.  In order to build with maturin
you will either need a conda environment or a virtual environment (venv).

```bash
python -m venv venv
. ./venv/bin/activate
```

Install the necessary packages:

```bash
export PIP_EXTRA_INDEX_URL=https://pypi.fury.io/lancedb/
python -m pip install .[tests,dev]
```

Our repo often references preview releases of `pylance`, which is hosted on `fury.io`.

To build the python package you can use maturin:

```bash
# This will build the rust bindings and place them in the appropriate place
# in your venv or conda environment
maturin develop
```

To run the unit tests:

```bash
pytest
```

To run the doc tests:

```bash
pytest --doctest-modules python/lancedb
```

To run linter and automatically fix all errors:

```bash
ruff format python
ruff --fix python
```

If any packages are missing, install them with:

```bash
pip install <PACKAGE_NAME>
```

___
For **Windows** users, there may be errors when installing packages, so these commands may be helpful:

Activate the virtual environment:

```bash
. .\venv\Scripts\activate
```

You may need to run the installs separately:

```bash
pip install -e .[tests]
pip install -e .[dev]
```

`tantivy` requires `rust` to be installed, so install it with `conda`, as it doesn't support windows installation:

```bash
pip install wheel
pip install cargo
conda install rust
pip install tantivy
```
