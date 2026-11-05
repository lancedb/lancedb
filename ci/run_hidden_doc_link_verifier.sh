#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

set -euo pipefail

usage() {
    echo "usage: $0 {start|run} <40-char-commit> <label>" >&2
    exit 2
}

[[ $# -eq 3 ]] || usage

mode="$1"
commit="$2"
label="$3"

[[ "$commit" =~ ^[0-9a-f]{40}$ ]] || usage
[[ "$label" =~ ^[a-z0-9][a-z0-9._-]*$ ]] || usage

evidence_root="${SDK_HIDDEN_DOC_LINK_EVIDENCE_ROOT:-$HOME/codex-evidence/ENT-3373-typed-errors/sdk-hidden-doc-link-ci}"
repo="$evidence_root/lancedb"
log="$evidence_root/$label.log"
metadata="$evidence_root/$label.metadata"
exit_file="$evidence_root/$label.exit"
done_file="$evidence_root/$label.done"
session="sdk-doc-link-$label"

run() {
    mkdir -p "$evidence_root"
    rm -f "$log.tmp" "$metadata.tmp" "$exit_file" "$done_file"

    finish() {
        status=$?
        if [[ ! -f "$exit_file" ]]; then
            printf '%s\n' "$status" > "$exit_file"
        fi
        touch "$done_file"
        exit "$status"
    }
    trap finish EXIT

    if [[ -n "${CARGO_TARGET_DIR:-}" || -n "${CARGO_BUILD_BUILD_DIR:-}" ]]; then
        echo "refusing to run with a custom Cargo target directory" >&2
        return 64
    fi

    if [[ ! -d "$repo/.git" ]]; then
        git clone --filter=blob:none --no-checkout \
            https://github.com/lancedb/lancedb.git "$repo"
    fi

    git -C "$repo" fetch --no-tags origin
    git -C "$repo" switch --detach "$commit"

    actual_commit="$(git -C "$repo" rev-parse HEAD)"
    if [[ "$actual_commit" != "$commit" ]]; then
        echo "checked out $actual_commit instead of $commit" >&2
        return 65
    fi

    cd "$repo"
    {
        printf 'label=%s\n' "$label"
        printf 'requested_commit=%s\n' "$commit"
        printf 'actual_commit=%s\n' "$actual_commit"
        printf 'started_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
        printf 'cargo=%s\n' "$(command -v cargo)"
        printf 'rustup=%s\n' "$(command -v rustup)"
        printf 'CARGO_TARGET_DIR=%s\n' "${CARGO_TARGET_DIR:-unset}"
        printf 'CARGO_BUILD_BUILD_DIR=%s\n' "${CARGO_BUILD_BUILD_DIR:-unset}"
        cargo --version
        rustc --version
        rustup show active-toolchain
        git -C "$repo" status --short
    } > "$metadata.tmp"
    mv "$metadata.tmp" "$metadata"

    cd "$repo/rust/lancedb"
    set +e
    cargo test \
        --profile ci \
        --features remote \
        --locked \
        --lib \
        docs::tests::test_all_doc_links_resolve_in_repo_docs \
        -- \
        --exact \
        --nocapture \
        2>&1 | tee "$log.tmp"
    status=${PIPESTATUS[0]}
    set -e

    mv "$log.tmp" "$log"
    printf '%s\n' "$status" > "$exit_file"
    exit "$status"
}

case "$mode" in
    start)
        if tmux has-session -t "$session" 2>/dev/null; then
            echo "tmux session already exists: $session" >&2
            exit 1
        fi
        printf -v command '%q run %q %q' "$0" "$commit" "$label"
        tmux new-session -d -s "$session" "$command"
        echo "$session"
        ;;
    run)
        run
        ;;
    *)
        usage
        ;;
esac
