# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import json
import os
import re
import shutil
import subprocess
import tempfile
import textwrap
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FREE_THREADED_JOBS = (
    "free-threaded-linux",
    "free-threaded-mac",
    "free-threaded-windows",
)


def reporter_needs():
    workflow = (ROOT / ".github/workflows/pypi-publish.yml").read_text()
    reporter = workflow.split("\n  report-failure:\n", 1)[1]
    match = re.search(r"^    needs: \[([^\n]+)\]$", reporter, re.MULTILINE)
    if match is None:
        raise AssertionError("report-failure must declare direct job dependencies")
    return [job.strip() for job in match.group(1).split(",")]


class PyPIPublishFailureTest(unittest.TestCase):
    def test_reporter_has_direct_free_threaded_dependencies(self):
        self.assertTrue(set(FREE_THREADED_JOBS).issubset(reporter_needs()))

    @unittest.skipUnless(
        os.name == "posix" and shutil.which("bash") and shutil.which("jq"),
        "failure reporter requires bash and jq",
    )
    def test_isolated_free_threaded_failures_are_reported(self):
        action = (ROOT / ".github/actions/create-failure-issue/action.yml").read_text()
        script = textwrap.dedent(action.split("      run: |\n", 1)[1])
        with tempfile.TemporaryDirectory() as directory:
            gh = Path(directory) / "gh"
            gh.write_text(
                '#!/bin/sh\ncase "$1 $2" in\n'
                '  "issue list") echo "[]" ;;\n'
                '  "issue create") printf "%s\\n" "$@" ;;\n'
                '  *) exit 1 ;;\nesac\n'
            )
            gh.chmod(0o755)
            for failed_job in FREE_THREADED_JOBS + (None,):
                with self.subTest(failed_job=failed_job):
                    results = {
                        job: {
                            "result": "failure" if job == failed_job else "success"
                        }
                        for job in reporter_needs()
                    }
                    if failed_job:
                        results["publish"] = {"result": "skipped"}
                    result = subprocess.run(
                        ["bash", "-e", "-c", script],
                        env={
                            **os.environ,
                            "PATH": directory + os.pathsep + os.environ["PATH"],
                            "GH_TOKEN": "",
                            "JOB_RESULTS": json.dumps(results),
                            "WORKFLOW_NAME": "PyPI Publish",
                            "RUN_URL": "https://example.invalid/run",
                        },
                        capture_output=True,
                        text=True,
                        check=True,
                    )
                    if failed_job:
                        self.assertIn(
                            f"PyPI Publish Failed ({failed_job})", result.stdout
                        )
                        self.assertIn("Issue created successfully", result.stdout)
                    else:
                        self.assertIn("No job failures detected", result.stdout)
                        self.assertNotIn("Issue created successfully", result.stdout)


if __name__ == "__main__":
    unittest.main()
