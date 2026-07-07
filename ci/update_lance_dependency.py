#!/usr/bin/env python3
"""Prepare a Lance dependency update for LanceDB."""

from __future__ import annotations

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path
from typing import Sequence

try:
    from check_lance_release import parse_semver
except ModuleNotFoundError:
    # Supports importing as ci.update_lance_dependency from tests or ad hoc checks.
    from ci.check_lance_release import parse_semver  # type: ignore


def normalize_version(raw: str) -> str:
    value = raw.strip()
    value = value.removeprefix("refs/tags/")
    value = value.removeprefix("v")
    try:
        parse_semver(value)
    except ValueError:
        raise ValueError(f"Unsupported Lance version or tag: {raw}")
    return value


def normalized_tag(version: str) -> str:
    return f"v{version}"


def branch_name(version: str) -> str:
    suffix = re.sub(r"[^a-zA-Z0-9]+", "-", version).strip("-")
    suffix = re.sub(r"-+", "-", suffix)
    return f"codex/update-lance-{suffix}"


def commit_type(version: str) -> str:
    prerelease = version.split("-", maxsplit=1)[1] if "-" in version else ""
    return "chore" if "beta" in prerelease or "rc" in prerelease else "feat"


def metadata_for(version: str) -> dict[str, str]:
    kind = commit_type(version)
    message = f"{kind}: update lance dependency to v{version}"
    return {
        "version": version,
        "tag": normalized_tag(version),
        "branch_name": branch_name(version),
        "commit_type": kind,
        "commit_message": message,
        "pr_title": message,
    }


def run_command(cmd: Sequence[str], *, cwd: Path) -> None:
    subprocess.run(cmd, cwd=cwd, check=True)


def update_java_lance_core_version(repo_root: Path, version: str) -> None:
    pom_path = repo_root / "java" / "pom.xml"
    contents = pom_path.read_text(encoding="utf-8")
    updated, count = re.subn(
        r"(<lance-core\.version>)[^<]+(</lance-core\.version>)",
        rf"\g<1>{version}\g<2>",
        contents,
        count=1,
    )
    if count != 1:
        raise RuntimeError(
            "Expected exactly one <lance-core.version> entry in java/pom.xml"
        )
    pom_path.write_text(updated, encoding="utf-8")


def write_github_outputs(path: str | None, payload: dict[str, str]) -> None:
    if not path:
        return
    with open(path, "a", encoding="utf-8") as output:
        for key, value in payload.items():
            output.write(f"{key}={value}\n")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "tag_or_version",
        help="Lance tag or version, for example refs/tags/v7.2.0-beta.1 or 7.2.0",
    )
    parser.add_argument(
        "--repo-root",
        type=Path,
        default=Path(__file__).resolve().parents[1],
        help="Path to the lancedb repository root",
    )
    parser.add_argument(
        "--github-output",
        default=None,
        help="Optional GitHub Actions output file to receive metadata fields",
    )
    parser.add_argument(
        "--metadata-only",
        action="store_true",
        help="Only print derived metadata; do not modify dependency files",
    )
    args = parser.parse_args(argv)

    repo_root = args.repo_root.resolve()
    version = normalize_version(args.tag_or_version)
    payload = metadata_for(version)

    if not args.metadata_only:
        run_command([sys.executable, "ci/set_lance_version.py", version], cwd=repo_root)
        update_java_lance_core_version(repo_root, version)

    write_github_outputs(args.github_output, payload)
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
