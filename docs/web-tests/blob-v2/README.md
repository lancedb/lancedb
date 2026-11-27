# Blob v2 documentation tests

`docs/web-tests/ts` pins `@lancedb/lancedb` 0.31.0 and `docs/web-tests/rs` pins `lancedb` 0.31.0. Those releases do not include `blob()`. Run the Blob v2 examples here.

## Python

`lancedb==0.41.0b10` from the LanceDB package index, without pylance. PyPI `lancedb` 0.40.0 does not provide `lancedb.blob()`.

```shell
python3.13 -m venv .venv
.venv/bin/pip install -r docs/web-tests/blob-v2/py/requirements.txt
.venv/bin/python -m pytest docs/web-tests/blob-v2/py/test_multimodal.py
```

`test_blob_pylance.py` checks custom thresholds and `to_pandas()` in `lazy` and `bytes` mode. Those calls import pylance. Use a second virtualenv. `test_multimodal.py` fails if `lance` can be imported. Snippets from this file are written to `docs/web/snippets/blob_pylance.mdx`.

This install is for the optional tests only. The public guide does not tell users to bypass the dependency resolver. PyPI `pylance` 13.0.0 requires `lance-namespace<0.12`, and `lancedb==0.41.0b10` requires `lance-namespace>=0.13`, so `lancedb[pylance]` does not resolve. The three tests passed with pylance installed after the base requirements, without its own dependency pin.

```shell
python3.13 -m venv .venv-pylance
.venv-pylance/bin/pip install -r docs/web-tests/blob-v2/py/requirements.txt
.venv-pylance/bin/pip install --no-deps pylance==13.0.0
.venv-pylance/bin/python -m pytest docs/web-tests/blob-v2/py/test_blob_pylance.py
```

## TypeScript

`@lancedb/lancedb@0.41.0-beta.8`. Node.js 22.6 or newer. `npm test` passes `--experimental-strip-types`, which Node added in 22.6.0.

```shell
cd docs/web-tests/blob-v2/ts
npm ci
npm test
```

## Rust

crates.io `lancedb` 0.40.0 does not include `blob()`. This example builds the workspace crate, `lancedb` 0.41.0-beta.10. Pass `--features remote` so this checkout compiles. The example uses a local database. The Rust workflow runs this example. It is not a default feature, so a workspace build without `remote` does not turn that feature on for the rest of the workspace.

```shell
cargo run -p lancedb-blob-v2-docs --features remote --example multimodal
```

## CI

Python and TypeScript above install preview packages. Pre-release builds are not guaranteed to stay available for more than 6 months, so those runs are not required PR checks. Run them when the guide's pins change. The Rust example is the in-repo check.

Regenerate snippets from the repository root:

```shell
python3 docs/web-tests/mdx_snippets_gen.py \
  -s docs/web-tests/py -s docs/web-tests/ts -s docs/web-tests/rs \
  -s docs/web-tests/blob-v2/py -s docs/web-tests/blob-v2/ts -s docs/web-tests/blob-v2/rs \
  -o docs/web/snippets
```
