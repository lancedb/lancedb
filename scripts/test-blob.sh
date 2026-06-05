#!/usr/bin/env bash
# Run every blob-related check end to end.
#
# Use this when working on blob-column support. It verifies:
#   - the Rust core compiles, formats clean, and passes blob unit + integration tests
#   - the Python PyO3 bindings compile
#   - the Python extension is rebuilt from current source (never trusts a stale .so)
#   - the gated Python integration tests pass
#   - the end-to-end user-acceptance scenarios pass
#
# Run from the repo root:
#   ./scripts/test-blob.sh

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

say() { printf '\n=== %s ===\n' "$1"; }

say "cargo fmt --check"
cargo fmt --all -- --check

say "clippy on lancedb (tests)"
cargo clippy --quiet --features remote -p lancedb --tests -- -D warnings

say "cargo test (blob unit tests)"
cargo test --quiet --features remote -p lancedb --lib blob

say "cargo test (blob integration tests)"
cargo test --quiet --features remote -p lancedb --test blob_integration

say "cargo check on lancedb-python"
cargo check -p lancedb-python

# Always rebuild the Python extension from current source so the gate cannot
# pass against a stale .so. maturin rebuilds in place; uv handles the venv.
say "rebuild Python extension from current source"
(cd python && uv run --extra tests --extra dev --with maturin maturin develop --quiet)

# Sanity check: the freshly-built .so must expose the methods this branch
# claims to ship. If a method is missing here, the source and the binary are
# out of sync and the gate must fail loudly.
say "verify .so exposes claimed methods"
(cd python && .venv/bin/python -c "
import lancedb._lancedb as m
required = ('blob_columns', 'take_blobs', 'take_blob_files')
missing = [name for name in required if not hasattr(m.Table, name)]
if missing:
    raise SystemExit(f'stale extension: Table is missing {missing}')
if not hasattr(m, 'BlobFile') or not hasattr(m.BlobFile, 'read'):
    raise SystemExit('stale extension: BlobFile.read missing')
print('ok')
")

say "Python blob unit tests"
(cd python && LANCEDB_BLOB_INTEGRATION=1 .venv/bin/python -m pytest \
    python/tests/test_blob.py -q)

say "Python end-to-end UAT scenarios"
(cd python && LANCEDB_BLOB_INTEGRATION=1 .venv/bin/python \
    python/tests/blob_uat.py)

printf '\nall blob checks green.\n'
