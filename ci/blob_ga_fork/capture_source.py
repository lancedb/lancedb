# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Reject product drift from the exact feature source before the hosted build."""

import hashlib
import json
import subprocess
import sys
from pathlib import Path

SOURCE = "27f4e5168a985f6eee2abc8ea4f2bf3a3fc10fb4"
WORKFLOW = ".github/workflows/blob-ga-fork-diagnostic.yml"


def git(*args):
    return subprocess.check_output(["git", *args])


out = Path(sys.argv[1])
out.mkdir(parents=True, exist_ok=True)
changed = git("diff", "--name-only", SOURCE, "HEAD").decode().splitlines()
assert all(path == WORKFLOW or path.startswith("ci/blob_ga_fork/") for path in changed)
assert not git("status", "--porcelain", "--untracked-files=no"), "dirty tracked source"
(out / "source-tree.txt").write_bytes(git("ls-tree", "-r", SOURCE))
(out / "diagnostic.patch").write_bytes(git("diff", "--binary", SOURCE, "HEAD"))
paths = [
    "Cargo.toml",
    "Cargo.lock",
    "rust-toolchain.toml",
    ".cargo/config.toml",
    "python/pyproject.toml",
    "python/uv.lock",
    "python/src/runtime.rs",
    "python/python/lancedb/background_loop.py",
    "python/python/tests/test_remote_db.py",
    ".github/workflows/python.yml",
    ".github/workflows/run_tests/action.yml",
    ".github/workflows/build_linux_wheel/action.yml",
    "docker-compose.yml",
    *changed,
]
identity = {
    "source": SOURCE,
    "source_tree": git("rev-parse", f"{SOURCE}^{{tree}}").decode().strip(),
    "candidate": git("rev-parse", "HEAD").decode().strip(),
    "candidate_tree": git("rev-parse", "HEAD^{tree}").decode().strip(),
    "changed_paths": changed,
    "sha256": {
        path: hashlib.sha256(Path(path).read_bytes()).hexdigest()
        for path in paths
        if Path(path).is_file()
    },
}
(out / "source-identity.json").write_text(json.dumps(identity, indent=2) + "\n")
print(json.dumps(identity, indent=2))
