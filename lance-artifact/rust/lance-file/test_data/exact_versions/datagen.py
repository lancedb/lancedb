# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The Lance Authors

import argparse
import hashlib
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

BASELINE_COMMIT = "3a72f8a61e14613f517dded6816d4bfc77817c93"
FIXTURE_NAMES = (
    "v1.lance",
    "v2_0.lance",
    "v2_1.lance",
    "v2_2.lance",
    "v2_0_self_described.lance",
    "v2_0_mini.lance",
)


def git_output(source: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", source, *args],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


def generate(source: Path, generator: Path, output: Path, target: Path) -> None:
    examples = source / "rust/lance-file/examples"
    target_generator = examples / "exact_version_fixture_generator.rs"
    created_examples = not examples.exists()
    examples.mkdir(parents=True, exist_ok=True)
    if target_generator.exists():
        raise RuntimeError(f"refusing to replace existing {target_generator}")

    shutil.copyfile(generator, target_generator)
    try:
        env = os.environ.copy()
        env["CARGO_TARGET_DIR"] = str(target)
        subprocess.run(
            [
                "cargo",
                "run",
                "-p",
                "lance-file",
                "--example",
                "exact_version_fixture_generator",
                "--",
                str(output),
            ],
            cwd=source,
            env=env,
            check=True,
        )
    finally:
        target_generator.unlink(missing_ok=True)
        if created_examples:
            examples.rmdir()


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Reproduce exact-version fixtures with the locked baseline writers."
    )
    parser.add_argument(
        "--source",
        type=Path,
        required=True,
        help=f"clean Lance checkout at {BASELINE_COMMIT}",
    )
    parser.add_argument(
        "--write",
        action="store_true",
        help="replace the checked-in fixtures after both baseline runs agree",
    )
    args = parser.parse_args()

    source = args.source.resolve()
    if git_output(source, "rev-parse", "HEAD") != BASELINE_COMMIT:
        raise RuntimeError(
            f"{source} must be checked out at baseline commit {BASELINE_COMMIT}"
        )
    if git_output(source, "status", "--porcelain"):
        raise RuntimeError(f"{source} must be clean before fixture generation")

    fixture_dir = Path(__file__).resolve().parent
    generator = fixture_dir / "datagen.rs"
    with (
        tempfile.TemporaryDirectory(prefix="lance-exact-fixtures-a-") as first_dir,
        tempfile.TemporaryDirectory(prefix="lance-exact-fixtures-b-") as second_dir,
        tempfile.TemporaryDirectory(
            prefix="lance-exact-fixtures-target-"
        ) as target_dir,
    ):
        first = Path(first_dir)
        second = Path(second_dir)
        target = Path(target_dir)
        generate(source, generator, first, target)
        generate(source, generator, second, target)

        for name in FIXTURE_NAMES:
            first_path = first / name
            second_path = second / name
            if first_path.read_bytes() != second_path.read_bytes():
                raise RuntimeError(f"separate baseline runs disagree for {name}")

            checked_path = fixture_dir / name
            if args.write:
                shutil.copyfile(first_path, checked_path)
            elif (
                not checked_path.exists()
                or checked_path.read_bytes() != first_path.read_bytes()
            ):
                raise RuntimeError(
                    f"{name} differs from the reproducible baseline; rerun with --write"
                )
            print(f"{name}: {digest(first_path)}")


if __name__ == "__main__":
    main()
