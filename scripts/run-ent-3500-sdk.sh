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
venv=/home/ec2-user/Code/wt/ent-3246-sdk/python/.venv
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
  rustc --version
  cargo --version
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
    run rust-list cargo test -p lancedb --features remote \
      wait_progress_observes_the_existing_poll_loop -- --list
    grep -Fq 'wait_progress_observes_the_existing_poll_loop' "$out/rust-list.log"
    run rust-test cargo test -p lancedb --features remote \
      wait_progress_observes_the_existing_poll_loop -- --nocapture
    grep -Eq 'test result: ok\. [1-9][0-9]* passed' "$out/rust-test.log"
    ;;
  python)
    source "$venv/bin/activate"
    run python-collect "$venv/bin/pytest" --collect-only -q \
      python/tests/test_job.py -k 'progress or wait_progress'
    grep -Fq 'test_wait_progress_true_updates_and_closes_only_the_owned_bar' \
      "$out/python-collect.log"
    run python-build "$venv/bin/maturin" develop
    run python-provenance "$venv/bin/python" -c \
      "import lancedb; print(lancedb.__file__); assert lancedb.__file__.startswith('$repo/python/')"
    run python-job-tests "$venv/bin/pytest" -q python/tests/test_job.py \
      -k 'progress or wait_progress'
    grep -Eq '[1-9][0-9]* passed' "$out/python-job-tests.log"
    run python-remote-test "$venv/bin/pytest" -q \
      python/tests/test_remote_db.py::test_remote_connection_jobs_surface
    grep -Eq '[1-9][0-9]* passed' "$out/python-remote-test.log"
    ;;
  lint)
    run clippy cargo clippy -p lancedb --features remote --no-deps --tests \
      --benches --locked -- -D warnings --no-deps
    run rust-fmt cargo fmt --all -- --check
    source "$venv/bin/activate"
    run ruff-format "$venv/bin/ruff" format --check python
    run ruff-check "$venv/bin/ruff" check python
    ;;
  *)
    echo "unsupported suite: $suite" >&2
    exit 2
    ;;
esac

printf 'completed=%s\nverified_head=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$actual_sha"
