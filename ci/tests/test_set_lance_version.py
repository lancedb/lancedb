import os
import stat
import subprocess
import sys
import tempfile
import textwrap
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
SCRIPT = REPO_ROOT / "ci" / "set_lance_version.py"
LANCE_GIT_URL = "https://github.com/lance-format/lance.git"

CARGO_TOML = """\
[workspace.dependencies]
lance = { "version" = "=1.0.0", default-features = false, "features" = ["dynamodb"] }
lance-core = "1.0.0"
lance_datafusion = {
    "version" = "=1.0.0",
    "features" = ["substrait"]
}
lancedb = { path = "rust/lancedb", default-features = false }
lancedb-common = { path = "rust/lancedb-common" }
lancewood = "1.0.0"
my-lance = "1.0.0"
"""

UNTOUCHED_DEPENDENCIES = """\
lancedb = { path = "rust/lancedb", default-features = false }
lancedb-common = { path = "rust/lancedb-common" }
lancewood = "1.0.0"
my-lance = "1.0.0"
"""


class SetLanceVersionTest(unittest.TestCase):
    def test_supported_update_modes_only_rewrite_lance_dependencies(self):
        cases = {
            "stable": (
                """\
lance = { "version" = "=9.9.9", default-features = false, "features" = ["dynamodb"] }
lance-core = "=9.9.9"
lance_datafusion = { "version" = "=9.9.9", "features" = ["substrait"] }
""",
                ["cargo info lance", "cargo metadata"],
            ),
            "preview": (
                f"""\
lance = {{ "version" = "=10.0.0-beta.3", default-features = false, "features" = ["dynamodb"], "tag" = "v10.0.0-beta.3", "git" = "{LANCE_GIT_URL}" }}
lance-core = {{ "version" = "=10.0.0-beta.3", "tag" = "v10.0.0-beta.3", "git" = "{LANCE_GIT_URL}" }}
lance_datafusion = {{ "version" = "=10.0.0-beta.3", "features" = ["substrait"], "tag" = "v10.0.0-beta.3", "git" = "{LANCE_GIT_URL}" }}
""",
                ["git ls-remote --tags", "cargo metadata"],
            ),
            "local": (
                """\
lance = { "path" = "../lance/rust/lance", default-features = false, "features" = ["dynamodb"] }
lance-core = { "path" = "../lance/rust/lance-core" }
lance_datafusion = { "path" = "../lance/rust/lance_datafusion", "features" = ["substrait"] }
""",
                ["cargo metadata"],
            ),
            "v8.1.2": (
                """\
lance = { "version" = "=8.1.2", default-features = false, "features" = ["dynamodb"] }
lance-core = "=8.1.2"
lance_datafusion = { "version" = "=8.1.2", "features" = ["substrait"] }
""",
                ["cargo metadata"],
            ),
            "v8.2.0-beta.4": (
                f"""\
lance = {{ "version" = "=8.2.0-beta.4", default-features = false, "features" = ["dynamodb"], "tag" = "v8.2.0-beta.4", "git" = "{LANCE_GIT_URL}" }}
lance-core = {{ "version" = "=8.2.0-beta.4", "tag" = "v8.2.0-beta.4", "git" = "{LANCE_GIT_URL}" }}
lance_datafusion = {{ "version" = "=8.2.0-beta.4", "features" = ["substrait"], "tag" = "v8.2.0-beta.4", "git" = "{LANCE_GIT_URL}" }}
""",
                ["cargo metadata"],
            ),
        }

        for version, (updated_dependencies, expected_commands) in cases.items():
            with self.subTest(version=version), tempfile.TemporaryDirectory() as tmp:
                workdir = Path(tmp)
                (workdir / "Cargo.toml").write_text(CARGO_TOML)
                command_log = workdir / "commands.log"
                fake_bin = workdir / "bin"
                fake_bin.mkdir()
                self._write_fake_executables(fake_bin)
                self._write_fake_python_dependencies(workdir)

                env = os.environ.copy()
                env["PATH"] = os.pathsep.join([str(fake_bin), env["PATH"]])
                env["FAKE_COMMAND_LOG"] = str(command_log)
                env["PYTHONPATH"] = os.pathsep.join(
                    filter(None, [str(workdir), env.get("PYTHONPATH")])
                )
                result = subprocess.run(
                    [sys.executable, str(SCRIPT), version],
                    cwd=workdir,
                    env=env,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )

                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertEqual(
                    (workdir / "Cargo.toml").read_text(),
                    "[workspace.dependencies]\n"
                    + updated_dependencies
                    + UNTOUCHED_DEPENDENCIES,
                )
                commands = command_log.read_text().splitlines()
                for command in expected_commands:
                    self.assertTrue(
                        any(line.startswith(command) for line in commands),
                        f"{command!r} not found in {commands!r}",
                    )

    def _write_fake_executables(self, fake_bin: Path) -> None:
        cargo = fake_bin / "cargo"
        cargo.write_text(
            textwrap.dedent(
                """\
                #!/bin/sh
                printf 'cargo %s\\n' "$*" >> "$FAKE_COMMAND_LOG"
                case "$1" in
                  info)
                    printf '%s\\n' 'version: 8.8.8 (latest 9.9.9)'
                    ;;
                  metadata)
                    ;;
                  *)
                    exit 2
                    ;;
                esac
                """
            )
        )
        cargo.chmod(cargo.stat().st_mode | stat.S_IXUSR)

        git = fake_bin / "git"
        git.write_text(
            textwrap.dedent(
                """\
                #!/bin/sh
                printf 'git %s\\n' "$*" >> "$FAKE_COMMAND_LOG"
                if [ "$1" != "ls-remote" ]; then
                  exit 2
                fi
                printf '%s\\n' \\
                  '111111 refs/tags/v9.9.9' \\
                  '222222 refs/tags/v10.0.0-beta.1' \\
                  '333333 refs/tags/v10.0.0-beta.3'
                """
            )
        )
        git.chmod(git.stat().st_mode | stat.S_IXUSR)

    def _write_fake_python_dependencies(self, workdir: Path) -> None:
        packaging = workdir / "packaging"
        packaging.mkdir()
        (packaging / "__init__.py").write_text("")
        (packaging / "version.py").write_text(
            textwrap.dedent(
                """\
                class Version:
                    def __init__(self, value):
                        release, _, prerelease = value.partition("-beta.")
                        self._key = (
                            tuple(int(part) for part in release.split(".")),
                            not prerelease,
                            int(prerelease or 0),
                        )

                    def __lt__(self, other):
                        return self._key < other._key
                """
            )
        )


if __name__ == "__main__":
    unittest.main()
