#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <rust|python|lint> <output_dir>" >&2
  exit 2
fi

suite=$1
out=$2
expected_sha=${EXPECTED_SHA:-5bfcbe77bf9595b05ed1a5f06a803bdce8fe79a8}
if [[ -n ${SOURCE_DIR:-} ]]; then
  repo=$(cd "$SOURCE_DIR" && pwd)
else
  repo=$(cd "$(dirname "$0")/.." && pwd)
fi
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
    run rust-test mbx +$toolchain test -p lancedb --features remote \
      remote::job::tests::wait_progress_observes_the_existing_poll_loop \
      -- --exact --nocapture
    grep -Fq 'test result: ok. 1 passed;' "$out/rust-test.log"
    run rust-test-paused mbx +$toolchain test -p lancedb --features remote \
      remote::job::tests::describe_progress_reports_stable_unpaused_state \
      -- --exact --nocapture
    grep -Fq 'test result: ok. 1 passed;' "$out/rust-test-paused.log"
    ;;
  python)
    cd python
    run python-build uv run --frozen --python 3.12 --extra tests --extra dev \
      maturin develop --extras tests,dev
    run python-provenance uv run --frozen --python 3.12 --extra tests --extra dev \
      python -c \
      "import lancedb; print(lancedb.__file__); assert lancedb.__file__.startswith('$repo/python/')"
    python_job_tests=(
      python/tests/test_job.py::test_progress_is_additive_to_string_status_and_returns_none_locally
      python/tests/test_job.py::test_progress_eta_uses_two_stable_advancing_same_epoch_samples
      python/tests/test_job.py::test_progress_retains_eta_from_same_handle_observations
      python/tests/test_job.py::test_wait_progress_callback_observes_backend_poll_updates
      python/tests/test_job.py::test_wait_progress_callback_uses_the_public_eta_enrichment
      python/tests/test_job.py::test_wait_progress_true_updates_and_closes_only_the_owned_bar
      python/tests/test_job.py::test_wait_progress_bar_resumes_caller_owned_bar_after_timeout
      python/tests/test_job.py::test_wait_progress_bar_resets_display_for_new_plan_or_epoch
      python/tests/test_job.py::test_wait_progress_bar_uses_remaining_rows_when_computation_is_repeated
      python/tests/test_job.py::test_wait_progress_bar_tracks_late_planning_without_regressing
      python/tests/test_job.py::test_wait_progress_false_keeps_legacy_inner_wait_call
    )
    run python-job-tests uv run --frozen --python 3.12 --extra tests --extra dev \
      pytest -q "${python_job_tests[@]}"
    grep -Fq '11 passed' "$out/python-job-tests.log"
    run python-remote-test uv run --frozen --python 3.12 --extra tests --extra dev \
      pytest -q python/tests/test_remote_db.py::test_remote_connection_jobs_surface
    grep -Fq '1 passed' "$out/python-remote-test.log"
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
