# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Bind installed imports to the retained wheel, with no source-path override."""

import hashlib
import importlib
import json
import os
import platform
import re
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


def canonical_name(name):
    return re.sub(r"[-_.]+", "-", name).lower()


def compare_reference(identity, reference):
    """Reject changed test inputs; retain installation and host differences."""
    errors = identity["errors"]
    differences = {}
    for key in ("version", "executable_sha256", "libc", "platform", "cpu_count"):
        if identity[key] != reference[key]:
            differences[key] = {"reference": reference[key], "actual": identity[key]}
    if identity["version"].split()[0] != reference["version"].split()[0]:
        errors.append("Python version differs from retained experiment")
    if identity["wheel"]["sha256"] != reference["wheel"]["sha256"]:
        errors.append("wheel differs from retained experiment")
    for name, expected in reference["modules"].items():
        if identity["modules"][name]["sha256"] != expected["sha256"]:
            errors.append(f"{name} differs from retained experiment")
    expected = {canonical_name(p["name"]): p for p in reference["packages"]}
    actual = {canonical_name(p["name"]): p for p in identity["packages"]}
    if actual.keys() != expected.keys():
        errors.append("installed package set differs from retained experiment")
        differences["package_set"] = {
            "missing": sorted(expected.keys() - actual.keys()),
            "extra": sorted(actual.keys() - expected.keys()),
        }
    for name in sorted(actual.keys() & expected.keys()):
        for key in ("version", "metadata_sha256", "record_sha256"):
            if actual[name][key] != expected[name][key]:
                differences[f"package:{name}:{key}"] = {
                    "reference": expected[name][key],
                    "actual": actual[name][key],
                }
                # RECORD includes generated scripts and installation paths.
                if key != "record_sha256":
                    errors.append(f"{name} {key} differs from retained experiment")
    identity["reference_differences"] = differences


def main():
    if sys.argv[1] == "--requirements":
        reference = json.loads(Path(sys.argv[2]).read_text())
        # Install every observed distribution, without resolving newer dependencies.
        packages = reference["packages"]
        assert all(re.fullmatch(r"[A-Za-z0-9_.+-]+", p["name"]) for p in packages)
        assert all(re.fullmatch(r"[A-Za-z0-9_.+!-]+", p["version"]) for p in packages)
        Path(sys.argv[3]).write_text(
            "".join(
                f"{p['name']}=={p['version']}\n"
                for p in packages
                if canonical_name(p["name"]) != "lancedb"
            )
        )
        return
    output, wheel_path = map(Path, sys.argv[1:3])
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
    if len(sys.argv) == 4:
        compare_reference(identity, json.loads(Path(sys.argv[3]).read_text()))
    output.write_text(json.dumps(identity, indent=2) + "\n")
    print(json.dumps(identity, indent=2))
    if errors:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
