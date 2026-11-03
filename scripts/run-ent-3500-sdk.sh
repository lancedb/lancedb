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
suite_completed=0
trap 'status=$?; if [[ $status -eq 0 && $suite_completed -ne 1 ]]; then status=1; fi; printf "%s\n" "$status" > "$out/run.exit.tmp"; mv "$out/run.exit.tmp" "$out/run.exit"; exit "$status"' EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
trap 'exit 129' HUP
export TMPDIR=/var/tmp
export RUSTFLAGS=""
toolchain=1.97.0
export RUSTUP_TOOLCHAIN=$toolchain

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
  rustup run "$toolchain" rustc --version
  rustup run "$toolchain" cargo --version
  set +e
  "$@" 2>&1 | tee "$log"
  pipeline_status=("${PIPESTATUS[@]}")
  set -e
  status=${pipeline_status[0]}
  if [[ $status -eq 0 && ${pipeline_status[1]} -ne 0 ]]; then
    status=${pipeline_status[1]}
  fi
  printf '%s\n' "$status" > "$exit_file.tmp"
  mv "$exit_file.tmp" "$exit_file"
  if [[ $status -ne 0 ]]; then
    echo "$name failed with exit $status" >&2
    exit "$status"
  fi
}

case "$suite" in
  rust)
    run rust-list mbx +$toolchain test -p lancedb --features remote \
      wait_progress_observes_the_existing_poll_loop -- --list
    grep -Fq 'wait_progress_observes_the_existing_poll_loop' "$out/rust-list.log"
    run rust-list-paused mbx +$toolchain test -p lancedb --features remote \
      describe_progress_reports_stable_unpaused_state -- --list
    grep -Fq 'describe_progress_reports_stable_unpaused_state' \
      "$out/rust-list-paused.log"
    run rust-test mbx +$toolchain test -p lancedb --features remote \
      wait_progress_observes_the_existing_poll_loop -- --nocapture
    grep -Eq 'test result: ok\. [1-9][0-9]* passed' "$out/rust-test.log"
    run rust-test-paused mbx +$toolchain test -p lancedb --features remote \
      describe_progress_reports_stable_unpaused_state -- --nocapture
    grep -Eq 'test result: ok\. [1-9][0-9]* passed' \
      "$out/rust-test-paused.log"
    ;;
  python)
    cd python
    run python-build uv run --frozen --python 3.12 --extra tests --extra dev \
      maturin develop --extras tests,dev
    run python-provenance uv run --frozen --python 3.12 --extra tests --extra dev \
      python -c \
      "import lancedb; print(lancedb.__file__); assert lancedb.__file__.startswith('$repo/python/')"
    run python-collect uv run --frozen --python 3.12 --extra tests --extra dev \
      pytest --collect-only -q python/tests/test_job.py -k 'progress or wait_progress'
    grep -Fq 'test_wait_progress_true_updates_and_closes_only_the_owned_bar' \
      "$out/python-collect.log"
    grep -Fq 'test_wait_progress_bar_resumes_caller_owned_bar_after_timeout' \
      "$out/python-collect.log"
    grep -Fq 'test_wait_progress_bar_resets_display_for_new_plan_or_epoch' \
      "$out/python-collect.log"
    run python-job-tests uv run --frozen --python 3.12 --extra tests --extra dev \
      pytest -q python/tests/test_job.py -k 'progress or wait_progress'
    grep -Eq '[1-9][0-9]* passed' "$out/python-job-tests.log"
    run python-remote-test uv run --frozen --python 3.12 --extra tests --extra dev \
      pytest -q python/tests/test_remote_db.py::test_remote_connection_jobs_surface
    grep -Eq '[1-9][0-9]* passed' "$out/python-remote-test.log"
    ;;
  lint)
    run clippy mbx +$toolchain clippy -p lancedb --features remote --no-deps --tests \
      --benches --locked -- -D warnings --no-deps
    run rust-fmt rustup run "$toolchain" cargo fmt --all -- --check
    cd python
    run ruff-format uvx --from ruff==0.9.9 ruff format --check .
    run ruff-check uvx --from ruff==0.9.9 ruff check .
    ;;
  *)
    echo "unsupported suite: $suite" >&2
    exit 2
    ;;
esac

printf 'completed=%s\nverified_head=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$actual_sha"
suite_completed=1
