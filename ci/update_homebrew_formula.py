"""Update the tap formula from a stable CLI release's SHA256SUMS."""

import argparse
from pathlib import Path
import re

from ci.package_cli import TARGETS, archive_stem


def update_formula(formula: str, tag: str, manifest: str) -> str:
    stable_version = r"[0-9]+\.[0-9]+\.[0-9]+"
    if not re.fullmatch(f"v{stable_version}", tag):
        raise ValueError(f"Expected a stable CLI release tag, got {tag!r}")
    version = tag[1:]

    version_line = rf'^  version "({stable_version})(?:-beta\.[0-9]+)?"$'
    current = re.findall(version_line, formula, re.MULTILINE)
    if len(current) != 1:
        raise ValueError("Expected one CLI version declaration in the tap formula")
    if tuple(map(int, current[0].split("."))) > tuple(map(int, version.split("."))):
        # A maintainer may be retrying an older release after a newer tap PR merged.
        return formula

    archives = {}
    for line in manifest.splitlines():
        entry = re.fullmatch(r"([0-9a-f]{64})  (\S+)", line)
        if not entry or entry[2] in archives:
            raise ValueError(f"Invalid or duplicate entry in SHA256SUMS: {line!r}")
        archives[entry[2]] = entry[1]

    filenames = {t: f"{archive_stem(version, t)}.tar.gz" for t in TARGETS}
    if archives.keys() != set(filenames.values()):
        raise ValueError(f"Expected SHA256SUMS for exactly the CLI archives for {tag}")

    updated = re.sub(
        version_line, f'  version "{version}"', formula, flags=re.MULTILINE
    )
    for target, filename in filenames.items():
        # Match the URL and its following checksum together; platform order can change.
        url = re.escape(f'url "#{{archive}}-{target}.tar.gz"')
        pattern = rf'^([ \t]+{url}\n[ \t]+sha256 ")[0-9a-f]{{64}}"$'
        updated, count = re.subn(
            pattern, rf'\g<1>{archives[filename]}"', updated, flags=re.MULTILINE
        )
        if count != 1:
            raise ValueError(
                f"Expected one URL and checksum for {target} in the formula"
            )
    return updated


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument("checksums", type=Path)
    parser.add_argument("formula", type=Path)
    args = parser.parse_args()
    original = args.formula.read_text()
    updated = update_formula(original, args.tag, args.checksums.read_text())
    if updated != original:
        args.formula.write_text(updated)


if __name__ == "__main__":
    main()
