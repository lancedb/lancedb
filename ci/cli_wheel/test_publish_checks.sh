#!/usr/bin/env bash
set -euo pipefail

# Exercise the release checks without a GitHub token or a PyPI upload.
stable='{"tag_name":"v0.42.0","draft":false,"prerelease":false,"published_at":"2026-01-01T00:00:00Z"}'
protected='{"protection_rules":[{"type":"required_reviewers","reviewers":[{"type":"Team"}]}]}'
gh() {
    case "$2" in
        repos/lancedb/lancedb/releases/tags/*) [[ -n $release_json ]] && echo "$release_json" ;;
        repos/lancedb/lancedb/environments/pypi-cli) [[ -n $environment_json ]] && echo "$environment_json" ;;
        *) return 1 ;;
    esac
}
export -f gh
check() {
    release_json=$2 environment_json=$3 bash ci/cli_wheel/check_publish.sh "$1"
}
reject() {
    if check "$@" 2>/dev/null; then
        echo "Unexpectedly accepted: $*" >&2
        exit 1
    fi
}

check v0.42.0 "$stable" "$protected"
reject v0.42.0-beta.30 "${stable/v0.42.0/v0.42.0-beta.30}" "$protected"
reject v0.42.1 "$stable" "$protected"
reject v0.42.0 "" "$protected"
reject v0.42.0 "$(jq '.draft = true' <<<"$stable")" "$protected"
reject v0.42.0 "$(jq '.prerelease = true' <<<"$stable")" "$protected"
reject v0.42.0 "$(jq '.published_at = null' <<<"$stable")" "$protected"
reject v0.42.0 "$stable" '{"protection_rules":[]}'
reject v0.42.0 "$stable" ""
reject v0.42.0 "$stable" '{"protection_rules":[{"type":"required_reviewers","reviewers":[]}]}'
