# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Link all CI test/bench targets after the workflow removes Cargo.lock.

This job never runs the test binaries. Discard their completed
executables while retaining normal binaries, libraries and build-script outputs.
"""

import json
import shutil
import subprocess
import sys
from pathlib import Path


def discard_test_executable(message: dict, target_dir: Path) -> bool:
    if message.get("reason") != "compiler-artifact":
        return False
    target = message["target"]
    kinds = target["kind"]
    # profile.test describes this build; target.test is only a manifest capability.
    # Ordinary binaries may still be needed through CARGO_BIN_EXE by later targets.
    if "custom-build" in kinds or not (
        message["profile"]["test"] or "test" in kinds or "bench" in kinds
    ):
        return False
    executable = message.get("executable")
    if executable is None:
        return False
    path = Path(executable)
    if not path.resolve().is_relative_to(target_dir.resolve()):
        raise ValueError(f"executable {path} is outside target directory {target_dir}")
    if not path.is_file():
        return False
    size = path.stat().st_size
    path.unlink()
    print(
        json.dumps(
            {
                "removed_test_executable": str(path),
                "target": target["name"],
                "logical_bytes": size,
                "filesystem_free_bytes": shutil.disk_usage(path.parent).free,
            }
        ),
        flush=True,
    )
    return True


def main() -> int:
    metadata = subprocess.run(
        ["cargo", "metadata", "--format-version=1", "--no-deps", "--all-features"],
        stdout=subprocess.PIPE,
        text=True,
    )
    if metadata.returncode:
        return metadata.returncode
    target_dir = Path(json.loads(metadata.stdout)["target_directory"])
    command = [
        "cargo",
        "build",
        "--profile",
        "ci",
        "--benches",
        "--all-features",
        "--tests",
        # Bound simultaneous compiler/linker temporary outputs as well as retained files.
        "--jobs",
        "1",
        "--message-format=json,json-diagnostic-rendered-ansi",
    ]
    cleanup_failed = False
    with subprocess.Popen(command, stdout=subprocess.PIPE, text=True) as process:
        assert process.stdout is not None
        for line in process.stdout:
            try:
                message = json.loads(line)
            except json.JSONDecodeError:
                print(line, end="", flush=True)
                continue
            if not isinstance(message, dict):
                print(line, end="", flush=True)
                continue
            if message.get("reason") == "compiler-message":
                diagnostic = message["message"]
                sys.stderr.write(
                    diagnostic.get("rendered") or json.dumps(diagnostic) + "\n"
                )
                sys.stderr.flush()
            elif message.get("reason") == "compiler-artifact":
                try:
                    discard_test_executable(message, target_dir)
                except (OSError, ValueError) as error:
                    print(
                        f"Cannot discard completed test executable: {error}",
                        file=sys.stderr,
                    )
                    cleanup_failed = True
            elif message.get("reason") not in {
                "build-script-executed",
                "build-finished",
            }:
                print(line, end="", flush=True)
        result = process.wait()
    return result if result else int(cleanup_failed)


if __name__ == "__main__":
    sys.exit(main())
