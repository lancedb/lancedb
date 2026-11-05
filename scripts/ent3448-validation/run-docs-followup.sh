#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
set -euo pipefail

expected_source=$1
evidence_dir=$2
venv=$3
cd "$(dirname "$0")/../.."
[[ "$(git rev-parse HEAD)" == "$expected_source" ]]
test -z "$(git status --porcelain)"
mkdir -p "$evidence_dir"
git rev-parse HEAD > "$evidence_dir/source.sha"
trap 'result=$?; printf "%s\n" "$result" > "$evidence_dir/suite.exit"' EXIT

export RUSTUP_TOOLCHAIN=1.97.0
export CARGO_BUILD_JOBS=4
export TMPDIR=/home/ec2-user/.cache/sophon-task-tmp/ent3448/docs
export PATH="/home/ec2-user/.local/node/node-v24.11.1-linux-x64/bin:$PATH"
mkdir -p "$TMPDIR"

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

run_check cargo-format cargo fmt --all -- --check
run_check python-format uvx --from ruff==0.17.3 ruff format --check python/python/lancedb/_lancedb.pyi
run_check python-lint uvx --from ruff==0.17.3 ruff check python/python/lancedb/_lancedb.pyi
cd nodejs
run_check node-build pnpm build
run_check node-docs pnpm run docs
cd ..

git diff --name-only -- docs/src/js > "$evidence_dir/changed-js-docs.txt"
cat > "$evidence_dir/expected-js-docs.txt" <<'EOF'
docs/src/js/interfaces/FunctionErrorRecord.md
docs/src/js/interfaces/JobFailureInfo.md
EOF
diff -u "$evidence_dir/expected-js-docs.txt" "$evidence_dir/changed-js-docs.txt"
