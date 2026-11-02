#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
venv=/home/ec2-user/Code/wt/ent-3246-sdk/python/.venv
python="$venv/bin/python"
extension=/home/ec2-user/Code/wt/ent-3246-sdk/python/python/lancedb/_lancedb.abi3.so
docs_packages=/home/ec2-user/Code/wt/ent-3469-docs-pkgs

ln -sfn "$extension" "$root/python/python/lancedb/_lancedb.abi3.so"
export PYTHONPATH="$root/python/python"

echo "source=$(git -C "$root" rev-parse HEAD)"
echo "python=$($python --version 2>&1)"
echo "extension_sha256=$(sha256sum "$extension" | cut -d' ' -f1)"

"$python" "$root/scripts/ent-3561-validate.py"
"$python" -m pytest -q \
    "$root/python/python/tests/test_function_packaging.py" \
    "$root/python/python/tests/test_blob_reader.py"

rm -rf "$root/docs/site"
(cd "$root/docs" && PYTHONPATH="$docs_packages:$root/python/python" "$python" -m mkdocs build --clean)

page="$root/docs/site/python/python/index.html"
"$python" "$root/scripts/ent-3561-check-docs.py" "$page"
