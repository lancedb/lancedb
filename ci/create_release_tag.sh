#!/usr/bin/env bash
set -euo pipefail

RELEASE_VERSION=$(python -c 'import tomllib; print(tomllib.load(open(".bumpversion.toml", "rb"))["tool"]["bumpversion"]["current_version"])')
RELEASE_TAG="v${RELEASE_VERSION}"

if git rev-parse --quiet --verify "refs/tags/${RELEASE_TAG}" >/dev/null; then
  echo "Release tag ${RELEASE_TAG} already exists" >&2
  exit 1
fi

git tag --annotate "$RELEASE_TAG" --message "Release ${RELEASE_TAG}"

HEAD_SHA=$(git rev-parse HEAD)
TAG_SHA=$(git rev-parse "refs/tags/${RELEASE_TAG}^{}")
if [[ "$TAG_SHA" != "$HEAD_SHA" ]]; then
  echo "Release tag ${RELEASE_TAG} points to ${TAG_SHA}, expected ${HEAD_SHA}" >&2
  exit 1
fi

echo "Created ${RELEASE_TAG} at ${HEAD_SHA}"
