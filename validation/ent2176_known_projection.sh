#!/usr/bin/env bash
set -u

ROOT=$(cd "$(dirname "$0")/.." && pwd)
MODE=${1:-all}
RUN_ID=${RUN_ID:-all}
EVIDENCE=/home/ec2-user/.cache/sophon-task-tmp/ent2176/known-projection/$RUN_ID
PYTHON_ENV=/home/ec2-user/Code/wt/ent-2073-sdk/python/.venv
PYTHON_BIN=/home/ec2-user/.local/share/uv/python/cpython-3.12.16-linux-x86_64-gnu/bin/python3.12
PYTHON_SITE=$PYTHON_ENV/lib/python3.12/site-packages
RUFF=$PYTHON_ENV/bin/ruff
EXTENSION_SOURCE=/home/ec2-user/Code/wt/ent-2073-sdk/python/python/lancedb/_lancedb.abi3.so
EXTENSION_TARGET="$ROOT/python/python/lancedb/_lancedb.abi3.so"

export TMPDIR=/home/ec2-user/.cache/sophon-task-tmp/ent2176
mkdir -p "$TMPDIR" "$EVIDENCE"
touch "$TMPDIR/.write-probe"
rm "$TMPDIR/.write-probe"

ln -sfn "$EXTENSION_SOURCE" "$EXTENSION_TARGET"

git -C "$ROOT" rev-parse HEAD >"$EVIDENCE/source-head.txt"
sha256sum "$EXTENSION_SOURCE" "$ROOT/python/python/lancedb/functions.py" \
  "$ROOT/python/python/tests/test_first_class_function_slice1.py" \
  >"$EVIDENCE/source-and-extension.sha256"
PYTHONPATH="$ROOT/python/python:$PYTHON_SITE" "$PYTHON_BIN" -c \
  'import lancedb, lancedb._lancedb; print(lancedb.__file__); print(lancedb._lancedb.__file__)' \
  >"$EVIDENCE/runtime-paths.txt" 2>&1
PYTHON_PATH_STATUS=$?
printf '%s\n' "$PYTHON_PATH_STATUS" >"$EVIDENCE/runtime-paths.exit"

overall=$PYTHON_PATH_STATUS
run_logged() {
  name=$1
  workdir=$2
  shift 2
  (cd "$workdir" && "$@") >"$EVIDENCE/$name.log" 2>&1
  status=$?
  printf '%s\n' "$status" >"$EVIDENCE/$name.exit"
  printf '%-42s exit=%s\n' "$name" "$status"
  if [ "$status" -ne 0 ]; then
    overall=1
  fi
}

if [ "$MODE" = all ]; then
  run_logged rust-fmt "$ROOT" cargo fmt --all -- --check
  run_logged rust-slice1 "$ROOT/rust" mbx +1.97.1 test --locked -p lancedb \
    --features remote --test first_class_function_slice1
  run_logged rust-clippy "$ROOT/rust" mbx +1.97.1 clippy --locked -p lancedb \
    --features remote --test first_class_function_slice1 -- -D warnings
fi
run_logged ruff-format "$ROOT" "$RUFF" format --check \
  "python/python/lancedb/functions.py" python/python/tests/test_first_class_function_slice1.py
run_logged ruff-check "$ROOT" "$RUFF" check \
  "python/python/lancedb/functions.py" python/python/tests/test_first_class_function_slice1.py
run_logged python-slice1 "$ROOT" env PYTHONPATH="$ROOT/python/python:$PYTHON_SITE" \
  "$PYTHON_BIN" -m pytest -q python/python/tests/test_first_class_function_slice1.py
run_logged python-ent2073-sync-async "$ROOT" env PYTHONPATH="$ROOT/python/python:$PYTHON_SITE" \
  "$PYTHON_BIN" -m pytest -q \
  python/python/tests/test_first_class_function_slice2.py::test_async_catalog_path_preserves_execution_controls \
  python/python/tests/test_first_class_function_slice2.py::test_sync_catalog_path_preserves_execution_controls

printf '%s\n' "$overall" >"$EVIDENCE/aggregate.exit"
exit "$overall"
