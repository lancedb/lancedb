# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Bind installed imports to the retained wheel, with no source-path override."""

import hashlib
import importlib
import json
import os
import platform
import sys
import sysconfig
import zipfile
from importlib import metadata
from pathlib import Path


def digest(path):
    with Path(path).open("rb") as stream:
        return hash_stream(stream)


def hash_stream(stream):
    value = hashlib.sha256()
    for block in iter(lambda: stream.read(1024 * 1024), b""):
        value.update(block)
    return value.hexdigest()


def main():
    output, wheel_path = map(Path, sys.argv[1:])
    modules = {}
    errors = []
    with zipfile.ZipFile(wheel_path) as wheel:
        for name in (
            "lancedb",
            "lancedb._lancedb",
            "lancedb.background_loop",
            "lancedb.remote.db",
            "lance",
            "pyarrow",
        ):
            module = importlib.import_module(name)
            path = Path(module.__file__).resolve()
            actual = digest(path)
            entry = {"path": str(path), "sha256": actual, "size": path.stat().st_size}
            if name.startswith("lancedb"):
                purelib = Path(sysconfig.get_path("purelib")).resolve()
                platlib = Path(sysconfig.get_path("platlib")).resolve()
                if not any(root in path.parents for root in (purelib, platlib)):
                    errors.append(f"{name} imported outside installed site-packages")
                suffix = str(path).split("/lancedb/", 1)[1]
                member = f"lancedb/{suffix}"
                with wheel.open(member) as stream:
                    expected = hash_stream(stream)
                entry.update(wheel_member=member, wheel_sha256=expected)
                if actual != expected:
                    errors.append(f"{name} does not match retained wheel")
            modules[name] = entry
    packages = []
    for dist in metadata.distributions():
        # RECORD binds package file hashes without retaining pip URLs or credentials.
        packages.append(
            {
                "name": dist.metadata["Name"],
                "version": dist.version,
                "metadata_sha256": hashlib.sha256(
                    (dist.read_text("METADATA") or "").encode()
                ).hexdigest(),
                "record_sha256": hashlib.sha256(
                    (dist.read_text("RECORD") or "").encode()
                ).hexdigest(),
            }
        )
    identity = {
        "executable": sys.executable,
        "executable_sha256": digest(sys.executable),
        "version": sys.version,
        "cwd": os.getcwd(),
        "libc": platform.libc_ver(),
        "platform": platform.platform(),
        "modules": modules,
        "packages": sorted(packages, key=lambda item: item["name"].lower()),
        "wheel": {"path": str(wheel_path), "sha256": digest(wheel_path)},
        "cpu_count": os.cpu_count(),
        "errors": errors,
    }
    output.write_text(json.dumps(identity, indent=2) + "\n")
    print(json.dumps(identity, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
