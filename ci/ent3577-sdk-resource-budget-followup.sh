#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

set -u

expected_head="${1:?expected commit is required}"
evidence_dir="${2:-/home/ec2-user/ent3577-evidence/${expected_head}}"
repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
node_bin="/home/ec2-user/tools/node-v24.22.2-linux-x64/bin"

export PATH="${node_bin}:${PATH}"
export NO_COLOR=1
export PYTHONUNBUFFERED=1

mkdir -p "${evidence_dir}"
exec > >(tee -a "${evidence_dir}/runner.log") 2>&1

finish() {
    local code="$1"
    printf '%s\n' "${code}" >"${evidence_dir}/runner.exit"
    printf 'finished_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" |
        tee -a "${evidence_dir}/metadata.txt"
    exit "${code}"
}

run() {
    local name="$1"
    local directory="$2"
    shift 2
    local status

    printf '%s\tstarted\t%s\t%s\n' "${name}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${directory}" |
        tee -a "${evidence_dir}/status.tsv"
    (
        cd "${directory}" &&
            "$@"
    ) >"${evidence_dir}/${name}.log" 2>&1
    status=$?
    printf '%s\n' "${status}" >"${evidence_dir}/${name}.exit"
    printf '%s\tfinished\t%s\tstatus=%s\n' \
        "${name}" "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "${status}" |
        tee -a "${evidence_dir}/status.tsv"
    if [[ "${status}" -ne 0 ]]; then
        finish "${status}"
    fi
}

actual_head="$(git -C "${repo_root}" rev-parse HEAD)"
if [[ "${actual_head}" != "${expected_head}" ]]; then
    printf 'head mismatch: expected=%s actual=%s\n' "${expected_head}" "${actual_head}" \
        >"${evidence_dir}/preflight.log"
    finish 2
fi

{
    printf 'started_at=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'host=%s\n' "$(hostname)"
    printf 'repo_root=%s\n' "${repo_root}"
    printf 'head=%s\n' "${actual_head}"
    printf 'tree=%s\n' "$(git -C "${repo_root}" rev-parse HEAD^{tree})"
    printf 'origin_main=%s\n' "$(git -C "${repo_root}" rev-parse origin/main)"
    printf 'node=%s\n' "$(node --version)"
    printf 'pnpm=%s\n' "$(pnpm --version)"
    printf 'uv=%s\n' "$(uv --version)"
    printf 'cargo=%s\n' "$(cargo --version)"
    printf 'nproc=%s\n' "$(nproc)"
    free -h
    df -h "${repo_root}" /tmp
} >"${evidence_dir}/metadata.txt" 2>&1

git -C "${repo_root}" status --short >"${evidence_dir}/source-status-before.txt"

run source_format "${repo_root}" cargo fmt --all -- --check
run source_diff_check "${repo_root}" git diff --check

run python_build "${repo_root}/python" \
    uv run --extra tests --extra dev maturin develop --extras tests,dev
run python_remote_refresh "${repo_root}/python" \
    uv run --extra tests pytest \
    python/tests/test_remote_db.py::test_remote_refresh_async_returns_typed_terminal_result \
    -q
run python_remote_invalid "${repo_root}/python" \
    uv run --extra tests pytest \
    python/tests/test_remote_db.py::test_remote_refresh_rejects_invalid_options_before_request \
    -q
run python_local_reject "${repo_root}/python" \
    uv run --extra tests pytest \
    python/tests/test_table.py::test_local_refresh_rejects_enterprise_options \
    -q
run python_ruff_format "${repo_root}/python" \
    uv run --extra dev ruff format --check \
    python/lancedb/table.py \
    python/lancedb/remote/table.py \
    python/lancedb/_lancedb.pyi \
    python/tests/test_remote_db.py \
    python/tests/test_table.py
run python_ruff_check "${repo_root}/python" \
    uv run --extra dev ruff check \
    python/lancedb/table.py \
    python/lancedb/remote/table.py \
    python/lancedb/_lancedb.pyi \
    python/tests/test_remote_db.py \
    python/tests/test_table.py
run python_types "${repo_root}/python" \
    uv run --extra dev pyright \
    python/lancedb/table.py \
    python/lancedb/remote/table.py \
    python/lancedb/_lancedb.pyi

run node_install "${repo_root}/nodejs" pnpm install --frozen-lockfile
run node_build "${repo_root}/nodejs" pnpm build
run node_lint "${repo_root}/nodejs" pnpm lint-ci
run node_remote_refresh "${repo_root}/nodejs" \
    pnpm test __test__/remote.test.ts \
    -t "forwards refresh options to the backfill request" --runInBand
run node_local_reject "${repo_root}/nodejs" \
    pnpm test __test__/table.test.ts \
    -t "rejects Enterprise refresh options on local tables" --runInBand
run node_docs "${repo_root}/nodejs" pnpm docs
run node_docs_clean "${repo_root}/nodejs" \
    git diff --exit-code -- ../docs/src/js

sha256sum \
    "${repo_root}/python/src/table.rs" \
    "${repo_root}/python/python/lancedb/table.py" \
    "${repo_root}/python/python/lancedb/remote/table.py" \
    "${repo_root}/nodejs/src/table.rs" \
    "${repo_root}/nodejs/lancedb/table.ts" \
    "${repo_root}/nodejs/__test__/remote.test.ts" \
    "${repo_root}/docs/web/feature-engineering/refresh-column.mdx" \
    >"${evidence_dir}/source-sha256.txt"
git -C "${repo_root}" status --short >"${evidence_dir}/source-status-after.txt"

if [[ -s "${evidence_dir}/source-status-before.txt" ]] ||
    [[ -s "${evidence_dir}/source-status-after.txt" ]]; then
    printf 'source tree is not clean before or after verification\n' \
        >"${evidence_dir}/cleanliness.log"
    finish 3
fi

printf 'follow-up runner completed successfully\n'
finish 0
