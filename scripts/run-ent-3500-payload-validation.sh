#!/usr/bin/env bash
set -u

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <output_dir>" >&2
  exit 2
fi

out=$1
expected_sha=${EXPECTED_SHA:?EXPECTED_SHA is required}
repo=${SOURCE_DIR:?SOURCE_DIR is required}
repo=$(cd "$repo" && pwd)
cd "$repo"

actual_sha=$(git rev-parse HEAD)
if [[ "$actual_sha" != "$expected_sha" ]]; then
  echo "source mismatch: expected $expected_sha, found $actual_sha" >&2
  exit 3
fi

mkdir -p "$out"
export TMPDIR=/var/tmp
export RUSTUP_TOOLCHAIN=1.97.0
export PATH="/home/ec2-user/tools/node-v24.22.2-linux-x64/bin:$PATH"
export COREPACK_ENABLE_DOWNLOAD_PROMPT=0

cd nodejs
pnpm test -- __test__/remote.test.ts --runInBand -t \
  "observes structured progress through the existing wait poller" \
  >"$out/payload-test.log" 2>&1
rc=$?
printf '%s\n' "$rc" >"$out/payload-test.exit"
exit "$rc"
