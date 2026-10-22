"""Fail if the lancedb sdist lacks a git-tracked file of the Python package.

Usage: python ci/check_sdist.py <sdist>   (run from the repository root)
"""

import subprocess
import sys
import tarfile
from pathlib import Path

# Where git tracks the package, and where maturin puts it under the sdist root.
TRACKED_PACKAGE = "python/python/lancedb"
SDIST_PACKAGE = "python/lancedb"


def main() -> None:
    sdist = Path(sys.argv[1])
    root = sdist.name.removesuffix(".tar.gz")
    listing = subprocess.run(
        ["git", "ls-files", "-z", "--", TRACKED_PACKAGE],
        check=True,
        capture_output=True,
        text=True,
    ).stdout
    tracked = [p for p in listing.split("\0") if p]
    if not tracked:
        sys.exit(f"error: git tracks no files under {TRACKED_PACKAGE}")

    with tarfile.open(sdist) as tar:
        members = {m.name for m in tar if m.isfile()}
    missing = [
        p
        for p in tracked
        if f"{root}/{SDIST_PACKAGE}{p.removeprefix(TRACKED_PACKAGE)}" not in members
    ]
    if missing:
        sys.exit(f"error: {sdist.name} lacks tracked sources: {', '.join(missing)}")
    print(f"{sdist.name} OK: all {len(tracked)} tracked sources present")


if __name__ == "__main__":
    main()
