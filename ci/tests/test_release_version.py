# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os
import re
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]


class ReleaseVersionTests(unittest.TestCase):
    def check_bump(self, part, expected):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = (REPO_ROOT / ".bumpversion.toml").read_text()
            config = config.split("[[tool.bumpversion.files]]", 1)[0]
            config = re.sub(
                r'^current_version = ".*"$',
                'current_version = "0.40.0-beta.12"',
                config,
                flags=re.MULTILINE,
            )
            config += '\n[[tool.bumpversion.files]]\nfilename = "version.txt"\n'
            (root / ".bumpversion.toml").write_text(config)
            (root / "version.txt").write_text("0.40.0-beta.12\n")
            (root / "java").mkdir()
            (root / "java" / "pom.xml").write_text("0.40.0-beta.12\n")
            (root / "bin").mkdir()
            maven = root / "bin" / "mvn"
            # Record the effective Maven version while exercising the real
            # bump-my-version parser, serialization, hook environment, and git hook.
            maven.write_text(
                '#!/bin/sh\nfor arg in "$@"; do\n'
                '  case "$arg" in\n'
                '    -DnewVersion=*) printf "%s\\n" "${arg#-DnewVersion=}" > pom.xml;;\n'
                "  esac\ndone\n"
            )
            maven.chmod(0o755)
            env = dict(
                os.environ, PATH=f"{root / 'bin'}{os.pathsep}{os.environ['PATH']}"
            )

            def run(*args):
                return subprocess.run(
                    args,
                    cwd=root,
                    env=env,
                    text=True,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    check=True,
                )

            run("git", "init", "-q")
            run("git", "config", "user.name", "Release Test")
            run("git", "config", "user.email", "release-test@example.invalid")
            run("git", "config", "commit.gpgsign", "false")
            run("git", "add", ".")
            run("git", "commit", "-qm", "Initial version")
            run("bump-my-version", "bump", part)
            self.assertEqual((root / "version.txt").read_text().strip(), expected)
            self.assertEqual((root / "java" / "pom.xml").read_text().strip(), expected)
            self.assertEqual(
                run("git", "describe", "--exact-match").stdout.strip(), f"v{expected}"
            )

    def test_stable_maven_version_matches_release(self):
        self.check_bump("pre_l", "0.40.0")

    def test_preview_maven_version_matches_release(self):
        self.check_bump("pre_n", "0.40.0-beta.13")


if __name__ == "__main__":
    unittest.main()
