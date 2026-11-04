#!/usr/bin/env bash
set -uo pipefail
source_sha=$1
evidence=$2
mode=$3
mkdir -p "$evidence"
export PATH="$HOME/.cargo/bin:$HOME/.local/bin:$PATH"
export CARGO_BUILD_JOBS=4
actual=$(git rev-parse HEAD)
printf '%s\n' "$actual" > "$evidence/source.txt"
if [[ "$actual" != "$source_sha" ]]; then exit 2; fi
git status --porcelain > "$evidence/status.txt"
if [[ -s "$evidence/status.txt" ]]; then exit 3; fi
mbx +1.97.1 --version > "$evidence/toolchain.txt" 2>&1
if [[ "$mode" == red ]]; then
    mbx +1.97.1 test --locked -p lancedb --lib repeated_row_repair_intent_fences_old_completion -- --exact table::freshness::tests::repeated_row_repair_intent_fences_old_completion --nocapture > "$evidence/regression.log" 2>&1
    result=$?
    printf '%s\n' "$result" > "$evidence/regression.exit"
    printf '%s\n' "$result" > "$evidence/overall.exit"
    exit "$result"
fi
mbx +1.97.1 fmt --all -- --check > "$evidence/fmt.log" 2>&1
result=$?
printf '%s\n' "$result" > "$evidence/fmt.exit"
if [[ "$result" != 0 ]]; then printf '%s\n' "$result" > "$evidence/overall.exit"; exit "$result"; fi
mbx +1.97.1 test --locked -p lancedb --lib table::freshness::tests -- --nocapture > "$evidence/freshness.log" 2>&1
result=$?
printf '%s\n' "$result" > "$evidence/freshness.exit"
if [[ "$result" != 0 ]]; then printf '%s\n' "$result" > "$evidence/overall.exit"; exit "$result"; fi
mbx +1.97.1 clippy --locked -p lancedb --tests -- -D warnings > "$evidence/clippy.log" 2>&1
result=$?
printf '%s\n' "$result" > "$evidence/clippy.exit"
printf '%s\n' "$result" > "$evidence/overall.exit"
exit "$result"
