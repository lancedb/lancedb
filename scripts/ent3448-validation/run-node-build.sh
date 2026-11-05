#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

expected_source=$1
evidence_dir=$2
cd "$(dirname "$0")/../.."
actual_source=$(git rev-parse HEAD)
[[ "$actual_source" == "$expected_source" ]]
mkdir -p "$evidence_dir"
printf '%s\n' "$actual_source" > "$evidence_dir/source.sha"
git status --porcelain > "$evidence_dir/source.status"
trap 'result=$?; printf "%s\n" "$result" > "$evidence_dir/suite.exit"' EXIT

export PATH="/home/ec2-user/.local/node/node-v24.11.1-linux-x64/bin:$PATH"
export RUSTUP_TOOLCHAIN=1.97.1
export CARGO_BUILD_JOBS=4
export TMPDIR=/home/ec2-user/.cache/sophon-task-tmp/ent3448/node
mkdir -p "$TMPDIR"
node --version > "$evidence_dir/node.version"
pnpm --version > "$evidence_dir/pnpm.version"

run_check() {
    local check_name=$1
    shift
    set +e
    "$@" > "$evidence_dir/$check_name.log" 2>&1
    local result=$?
    set -e
    printf '%s\n' "$result" > "$evidence_dir/$check_name.exit"
    return "$result"
}

cd nodejs
run_check install pnpm install --frozen-lockfile
run_check build pnpm build
sha256sum dist/*.node > "$evidence_dir/native.sha256"
