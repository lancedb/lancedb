#!/usr/bin/env bash
set -euo pipefail

tag=${1:?pass the stable release tag}
number='(0|[1-9][0-9]*)'
if ! [[ $tag =~ ^v${number}\.${number}\.${number}$ ]]; then
    echo "Expected a stable release tag such as v0.42.0, got $tag" >&2
    exit 1
fi

if ! gh api "repos/lancedb/lancedb/releases/tags/$tag" |
    jq -e --arg tag "$tag" '
        .tag_name == $tag and .draft == false and .prerelease == false and
        (.published_at | type == "string" and length > 0)
    ' >/dev/null; then
    echo "Expected a published stable GitHub release for $tag" >&2
    exit 1
fi

# Referencing a missing GitHub environment would create it without protection.
if ! gh api repos/lancedb/lancedb/environments/pypi-cli |
    jq -e 'any(.protection_rules[]; .type == "required_reviewers" and (.reviewers | length > 0))' >/dev/null; then
    echo "Configure a required reviewer on the pypi-cli environment" >&2
    exit 1
fi
