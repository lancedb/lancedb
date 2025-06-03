#!/usr/bin/env bash
set -euo pipefail

# This updates the lockfile without building
cargo metadata > /dev/null

pushd nodejs || exit 1
npm install --package-lock-only
popd
pushd node || exit 1
npm install --package-lock-only
popd

if git diff --quiet --exit-code; then
  echo "No lockfile changes to commit; skipping amend."
else
  git add Cargo.lock nodejs/package-lock.json node/package-lock.json
  git commit --amend --no-edit
fi
