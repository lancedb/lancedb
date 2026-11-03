#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <rust|python|lint> <output_dir>" >&2
  exit 2
fi

suite=$1
out=$2
expected_sha=${EXPECTED_SHA:?EXPECTED_SHA is required}
repo=$(cd "$(dirname "$0")/.." && pwd)
cd "$repo"

actual_sha=$(git rev-parse HEAD)
if [[ $actual_sha != "$expected_sha" ]]; then
  echo "source mismatch: expected $expected_sha, found $actual_sha" >&2
  exit 3
fi

mkdir -p "$out"
trap 'status=$?; printf "%s\n" "$status" > "$out/run.exit.tmp"; mv "$out/run.exit.tmp" "$out/run.exit"; exit "$status"' EXIT
export TMPDIR=/var/tmp
export RUSTFLAGS=""
export RUSTUP_TOOLCHAIN=1.97.1

run() {
  local name=$1
  shift
  local log="$out/$name.log"
  local exit_file="$out/$name.exit"
  local status
  printf '\n===== %s =====\n' "$name"
  printf 'sha=%s\ncommand=' "$actual_sha"
  printf '%q ' "$@"
  printf '\n'
  mbx +1.97.1 rustc --version
  mbx +1.97.1 cargo --version
  set +e
  "$@" 2>&1 | tee "$log"
  status=${PIPESTATUS[0]}
  set -e
  printf '%s\n' "$status" > "$exit_file.tmp"
  mv "$exit_file.tmp" "$exit_file"
  if [[ $status -ne 0 ]]; then
    echo "$name failed with exit $status" >&2
    exit "$status"
  fi
}

case "$suite" in
  rust)
    run rust-list mbx +1.97.1 test -p lancedb --features remote \
      wait_progress_observes_the_existing_poll_loop -- --list
    grep -Fq 'wait_progress_observes_the_existing_poll_loop' "$out/rust-list.log"
    run rust-test mbx +1.97.1 test -p lancedb --features remote \
      wait_progress_observes_the_existing_poll_loop -- --nocapture
    grep -Eq 'test result: ok\. [1-9][0-9]* passed' "$out/rust-test.log"
    ;;
  python)
    cd python
    run python-collect uv run --frozen --python 3.12 --extra tests --extra dev \
      pytest --collect-only -q tests/test_job.py -k 'progress or wait_progress'
    grep -Fq 'test_wait_progress_true_updates_and_closes_only_the_owned_bar' \
      "$out/python-collect.log"
    run python-build uv run --frozen --python 3.12 --extra tests --extra dev \
      maturin develop --extras tests,dev
    run python-provenance uv run --frozen --python 3.12 --extra tests --extra dev \
      python -c \
      "import lancedb; print(lancedb.__file__); assert lancedb.__file__.startswith('$repo/python/')"
    run python-job-tests uv run --frozen --python 3.12 --extra tests --extra dev \
      pytest -q tests/test_job.py -k 'progress or wait_progress'
    grep -Eq '[1-9][0-9]* passed' "$out/python-job-tests.log"
    run python-remote-test uv run --frozen --python 3.12 --extra tests --extra dev \
      pytest -q tests/test_remote_db.py::test_remote_connection_jobs_surface
    grep -Eq '[1-9][0-9]* passed' "$out/python-remote-test.log"
    ;;
  lint)
    run clippy mbx +1.97.1 clippy -p lancedb --features remote --no-deps --tests \
      --benches --locked -- -D warnings --no-deps
    run rust-fmt rustup run 1.97.1 cargo fmt --all -- --check
    cd python
    run ruff-format uv run --frozen --python 3.12 --extra tests --extra dev \
      ruff format --check .
    run ruff-check uv run --frozen --python 3.12 --extra tests --extra dev \
      ruff check .
    ;;
  *)
    echo "unsupported suite: $suite" >&2
    exit 2
    ;;
esac

printf 'completed=%s\nverified_head=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$actual_sha"
