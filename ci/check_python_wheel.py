# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Validate the ABI, platform, and contents of a built LanceDB wheel."""

from __future__ import annotations

import argparse
import re
import sys
from email.parser import BytesParser
from pathlib import Path
from zipfile import BadZipFile, ZipFile

PROFILES = {
    "abi3": ("cp310", "abi3"),
    "cp315t": ("cp315", "cp315t"),
}


def validate_wheel(wheel_path: Path, profile: str, platform_fragment: str) -> None:
    expected_python, expected_abi = PROFILES[profile]
    match = re.fullmatch(
        r"lancedb-[^-]+-(?P<python>[^-]+)-(?P<abi>[^-]+)-"
        r"(?P<platform>[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*)\.whl",
        wheel_path.name,
    )
    if match is None:
        raise ValueError(f"unexpected LanceDB wheel filename: {wheel_path.name}")

    python_tag, abi_tag, platform_tag = match.groups()
    if (python_tag, abi_tag) != (expected_python, expected_abi):
        raise ValueError(
            f"{wheel_path.name} has {python_tag}-{abi_tag}; expected "
            f"{expected_python}-{expected_abi}"
        )
    if platform_fragment not in platform_tag:
        raise ValueError(
            f"{wheel_path.name} has platform {platform_tag!r}; expected "
            f"it to include {platform_fragment!r}"
        )

    try:
        with ZipFile(wheel_path) as archive:
            names = archive.namelist()
            wheel_metadata_paths = [
                name for name in names if name.endswith(".dist-info/WHEEL")
            ]
            metadata_paths = [
                name for name in names if name.endswith(".dist-info/METADATA")
            ]
            if len(wheel_metadata_paths) != 1 or len(metadata_paths) != 1:
                raise ValueError("wheel must contain one WHEEL and one METADATA file")

            wheel_metadata = BytesParser().parsebytes(
                archive.read(wheel_metadata_paths[0])
            )
            distribution_metadata = BytesParser().parsebytes(
                archive.read(metadata_paths[0])
            )
            tags = wheel_metadata.get_all("Tag", [])
            expected_tags = {
                f"{expected_python}-{expected_abi}-{platform}"
                for platform in platform_tag.split(".")
            }
            if set(tags) != expected_tags or len(tags) != len(expected_tags):
                raise ValueError(
                    f"WHEEL metadata has tags {tags!r}; expected {sorted(expected_tags)!r}"
                )

            distribution_name = (distribution_metadata.get("Name") or "").lower()
            if distribution_name.replace("_", "-") != "lancedb":
                raise ValueError(
                    f"wheel metadata names {distribution_name!r}, expected 'lancedb'"
                )
            if not (distribution_metadata.get("Requires-Python") or "").startswith(
                ">=3.10"
            ):
                raise ValueError(
                    "wheel metadata does not preserve Requires-Python >=3.10"
                )
            if not any(
                name.startswith("lancedb/_lancedb.")
                and name.endswith((".pyd", ".so", ".dylib"))
                for name in names
            ):
                raise ValueError("wheel is missing the lancedb native extension")
    except BadZipFile as exc:
        raise ValueError(f"not a valid wheel archive: {wheel_path}") from exc


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("profile", choices=PROFILES)
    parser.add_argument("--platform-fragment", required=True)
    parser.add_argument("wheel", type=Path)
    args = parser.parse_args()

    try:
        validate_wheel(args.wheel, args.profile, args.platform_fragment)
    except (OSError, ValueError) as exc:
        print(f"wheel validation failed: {exc}", file=sys.stderr)
        return 1

    print(f"validated {args.profile} wheel: {args.wheel.name}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
