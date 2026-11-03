#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 2 ]]; then
  echo "usage: $0 <node> <output_dir>" >&2
  exit 2
fi

suite=$1
out=$2
expected_sha=${EXPECTED_SHA:-0a4ed51f59e3843ee8070e05bd72dd2e27b5775e}
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
export RUSTUP_TOOLCHAIN=1.97.0
export PATH="/home/ec2-user/tools/node-v24.22.2-linux-x64/bin:$PATH"

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
  node --version
  pnpm --version
  mbx +1.97.0 cargo --version
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
  node)
    cd nodejs
    run pnpm-install pnpm install --frozen-lockfile
    run pnpm-build pnpm build
    run node-tests pnpm test -- __test__/remote.test.ts __test__/connection.test.ts
    run node-lint pnpm lint
    run node-docs pnpm docs
    cd ..
    run rust-clippy mbx +1.97.0 clippy -p lancedb-nodejs --no-deps --tests \
      --locked -- -D warnings --no-deps
    run rust-fmt rustup run 1.97.0 cargo fmt --all -- --check
    ;;
  *)
    echo "unsupported suite: $suite" >&2
    exit 2
    ;;
esac

printf 'completed=%s\nverified_head=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$actual_sha"
suite_completed=1
