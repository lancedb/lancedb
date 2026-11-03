import os
import subprocess
import tempfile
import unittest
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[1] / "build_cli_linux.sh"


class BuildCliLinuxTest(unittest.TestCase):
    def test_toolchain_failures_stop_before_downloads_and_cargo(self):
        # Start with controlled tools so the host's compilers cannot mask a
        # missing container dependency. No command can download or build code.
        for failed_tool, exit_code in (
            ("dnf", 42),
            ("clang", 127),
            ("clang++", 127),
            ("ld.lld", 127),
        ):
            with (
                self.subTest(failed_tool=failed_tool),
                tempfile.TemporaryDirectory() as tmp,
            ):
                workdir = Path(tmp)
                bin_dir = workdir / "bin"
                bin_dir.mkdir()
                log = workdir / "commands.log"
                for name in (
                    "dnf",
                    "clang",
                    "clang++",
                    "ld.lld",
                    "git",
                    "curl",
                    "cargo",
                ):
                    status = exit_code if name == failed_tool else 0
                    if name in ("git", "curl", "cargo"):
                        status = 99
                    command = bin_dir / name
                    command.write_text(
                        f'#!/bin/sh\necho "{name}" >> "$CLI_TEST_LOG"\nexit {status}\n'
                    )
                    command.chmod(0o755)
                env = dict(os.environ, PATH=str(bin_dir), CLI_TEST_LOG=str(log))
                result = subprocess.run(
                    ["/bin/bash", str(SCRIPT)],
                    check=False,
                    cwd=workdir,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                self.assertEqual(result.returncode, exit_code, result.stderr)
                commands = log.read_text().splitlines()
                self.assertEqual(commands[0], "dnf")
                self.assertEqual(commands[-1], failed_tool)
                self.assertNotIn("git", commands)
                self.assertNotIn("curl", commands)
                self.assertNotIn("cargo", commands)


if __name__ == "__main__":
    unittest.main()
