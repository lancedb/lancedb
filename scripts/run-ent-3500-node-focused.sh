#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <output_dir>" >&2
  exit 2
fi

out=$1
expected_sha=${EXPECTED_SHA:-506689c7614abecf16f14b1ea624e46521fe54be}
repo=${SOURCE_DIR:?SOURCE_DIR is required}
repo=$(cd "$repo" && pwd)
cd "$repo"

actual_sha=$(git rev-parse HEAD)
if [[ $actual_sha != "$expected_sha" ]]; then
  echo "source mismatch: expected $expected_sha, found $actual_sha" >&2
  exit 3
fi

mkdir -p "$out"
suite_completed=0
trap 'status=$?; if [[ $status -eq 0 && $suite_completed -ne 1 ]]; then status=1; fi; printf "%s\n" "$status" > "$out/run.exit.tmp"; mv "$out/run.exit.tmp" "$out/run.exit"; exit "$status"' EXIT
export TMPDIR=/var/tmp
export RUSTFLAGS=""
export RUSTUP_TOOLCHAIN=1.97.0
export PATH="/home/ec2-user/tools/node-v24.22.2-linux-x64/bin:$PATH"
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0

run() {
  local name=$1
  shift
  local status
  printf '\n===== %s =====\n' "$name"
  printf 'sha=%s\ncommand=' "$actual_sha"
  printf '%q ' "$@"
  printf '\n'
  set +e
  "$@" 2>&1 | tee "$out/$name.log"
  pipeline_status=("${PIPESTATUS[@]}")
  set -e
  status=${pipeline_status[0]}
  if [[ $status -eq 0 && ${pipeline_status[1]} -ne 0 ]]; then
    status=${pipeline_status[1]}
  fi
  printf '%s\n' "$status" > "$out/$name.exit"
  if [[ $status -ne 0 ]]; then
    exit "$status"
  fi
}

cd nodejs
run pnpm-install pnpm install --frozen-lockfile
run pnpm-build pnpm build
run focused-tests pnpm test -- __test__/remote.test.ts --runInBand -t \
  "observes structured progress through the existing wait poller|does not derive ETA across a zero-rate, epoch, or pause boundary|keeps a thrown progress observer from escaping wait"
run node-lint pnpm lint
run node-docs pnpm run docs

printf 'completed=%s\nverified_head=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$actual_sha"
suite_completed=1
