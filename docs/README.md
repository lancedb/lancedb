# LanceDB Documentation

LanceDB docs are deployed to https://lancedb.github.io/lancedb/.

Documentation is built and deployed automatically by [Github Actions](../.github/workflows/docs.yml)
whenever a commit is pushed to the `main` branch. So it is possible for the docs to show
unreleased features.

## Building the docs

### Setup
1. Install LanceDB Python. See setup in [Python contributing guide](../python/CONTRIBUTING.md).
   Run `make develop` to install the Python package.
2. Install documentation dependencies. From LanceDB repo root: `pip install -r docs/requirements.txt`

### Preview the docs

```shell
cd docs
mkdocs serve
```

If you want to just generate the HTML files:

```shell
PYTHONPATH=. mkdocs build -f docs/mkdocs.yml
```

If successful, you should see a `docs/site` directory that you can verify locally.

## Adding examples

To make sure examples are correct, we put examples in test files so they can be
run as part of our test suites.

You can see the tests are at:

* Python: `python/python/tests/docs`
* Typescript: `nodejs/examples/`

### Checking python examples

```shell
cd python
pytest -vv python/tests/docs
```

### Checking typescript examples

The `@lancedb/lancedb` package must be built before running the tests:

```shell
pushd nodejs
npm ci
npm run build
popd
```

Then you can run the examples by going to the `nodejs/examples` directory and
running the tests like a normal npm package:

```shell
pushd nodejs/examples
npm ci
npm test
popd
```

## API documentation

### Python

The Python API documentation is organized based on the file `docs/src/python/python.md`.
We manually add entries there so we can control the organization of the reference page.
**However, this means any new types must be manually added to the file.** No additional
steps are needed to generate the API documentation.

### Typescript

The typescript API documentation is generated from the typescript source code using [typedoc](https://typedoc.org/).

When new APIs are added, you must manually re-run the typedoc command to update the API documentation.
The new files should be checked into the repository.

```shell
pushd nodejs
npm run docs
popd
```
