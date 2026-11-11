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
} >"${evidence_dir}/metadata.txt" 2>&1

git -C "${repo_root}" status --short >"${evidence_dir}/source-status-before.txt"

run node_docs "${repo_root}/nodejs" pnpm run docs
run node_docs_clean "${repo_root}/nodejs" \
    git diff --exit-code -- ../docs/src/js

sha256sum \
    "${repo_root}/nodejs/src/table.rs" \
    "${repo_root}/nodejs/lancedb/table.ts" \
    "${repo_root}/nodejs/__test__/remote.test.ts" \
    "${repo_root}/docs/src/js/classes/Table.md" \
    "${repo_root}/docs/src/js/interfaces/FunctionWorkerResourceBudget.md" \
    "${repo_root}/docs/src/js/interfaces/RefreshColumnOptions.md" \
    "${repo_root}/docs/web/feature-engineering/refresh-column.mdx" \
    >"${evidence_dir}/source-sha256.txt"
git -C "${repo_root}" status --short >"${evidence_dir}/source-status-after.txt"

if [[ -s "${evidence_dir}/source-status-before.txt" ]] ||
    [[ -s "${evidence_dir}/source-status-after.txt" ]]; then
    printf 'source tree is not clean before or after verification\n' \
        >"${evidence_dir}/cleanliness.log"
    finish 3
fi

printf 'Node documentation verification completed successfully\n'
finish 0
