"""Build lancedb-tools wheels from a complete, checksummed CLI release."""

import argparse
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
from pathlib import Path

from ci.package_cli import NOTICES, archive_stem, verify_checksums

PLATFORMS = {
    "aarch64-apple-darwin": "macosx_14_0_arm64",
    "aarch64-unknown-linux-gnu": "manylinux_2_28_aarch64",
    "x86_64-unknown-linux-gnu": "manylinux_2_28_x86_64",
}


def build_wheels(tag: str, archives: Path, output: Path):
    if not re.fullmatch(r"v[0-9]+\.[0-9]+\.[0-9]+(?:-beta\.[0-9]+)?", tag):
        raise ValueError(f"Expected a stable or beta CLI release tag, got {tag!r}")
    version = tag[1:]
    wheel_version = version.replace("-beta.", "b")
    verify_checksums(archives, version)
    output.mkdir(parents=True, exist_ok=True)

    for target, platform in PLATFORMS.items():
        stem = archive_stem(version, target)
        with tempfile.TemporaryDirectory(dir=output) as tmp:
            stage = Path(tmp)
            with tarfile.open(archives / f"{stem}.tar.gz", "r:gz") as archive:
                archive.extractall(stage, filter="data")
            root = stage / "wheel"
            package = root / "lancedb_tools"
            package.mkdir(parents=True)
            (stage / stem / "lancedb").rename(package / "lancedb")
            shutil.copyfile(
                Path(__file__).parent / "cli_wheel/launcher.py", package / "__init__.py"
            )

            info = root / f"lancedb_tools-{wheel_version}.dist-info"
            licenses = info / "licenses"
            licenses.mkdir(parents=True)
            for notice in NOTICES:
                (stage / stem / notice).rename(licenses / notice)
            (info / "METADATA").write_text(
                "Metadata-Version: 2.4\n"
                "Name: lancedb-tools\n"
                f"Version: {wheel_version}\n"
                "Summary: Command-line client for LanceDB SQL, profiles, and authentication\n"
                "Requires-Python: >=3.8\n"
                "License-Expression: Apache-2.0\n"
                + "".join(f"License-File: {notice}\n" for notice in NOTICES)
                + "Project-URL: Source, https://github.com/lancedb/lancedb\n"
                "Description-Content-Type: text/plain\n\n"
                "Run LanceDB SQL, manage connection profiles, and authenticate to a deployment.\n"
                "Use pipx install lancedb-tools or uvx --from lancedb-tools lancedb --help.\n"
                "Supports Apple Silicon on macOS 14+ and Linux ARM64 or x86-64 with glibc 2.28+.\n"
                "Linux x86-64 requires AVX2, FMA, and F16C.\n"
            )
            (info / "WHEEL").write_text(
                "Wheel-Version: 1.0\nGenerator: ci.build_cli_wheels\n"
                f"Root-Is-Purelib: false\nTag: py3-none-{platform}\n"
            )
            (info / "entry_points.txt").write_text(
                "[console_scripts]\nlancedb = lancedb_tools:main\n"
            )
            subprocess.run(
                [sys.executable, "-m", "wheel", "pack", str(root), "-d", str(stage)],
                check=True,
            )
            (wheel,) = stage.glob("*.whl")
            wheel.replace(output / wheel.name)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("tag")
    parser.add_argument(
        "archives",
        type=Path,
        help="Directory with the three release archives and SHA256SUMS",
    )
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    build_wheels(args.tag, args.archives, args.output)
