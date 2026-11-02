#!/usr/bin/env bash
set -uo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
evidence=${1:-/home/ec2-user/Code/wt/ent-3561-evidence}
mkdir -p "$evidence"
head=$(git -C "$root" rev-parse --short HEAD)
log="$evidence/validate-$head.log"
exit_file="$evidence/validate-$head.exit"

{
  echo "started=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "head=$(git -C "$root" rev-parse HEAD)"
  echo "toolchain=uv-python-3.12.16"
} | tee "$log"

bash "$root/scripts/ent-3561-validate.sh" > >(tee -a "$log") 2>&1
status=$?
{
  echo "finished=$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  echo "exit=$status"
} | tee -a "$log"
printf '%s\n' "$status" > "$exit_file"
exit "$status"
