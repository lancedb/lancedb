# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import subprocess
import sys


def test_import_lancedb_without_pylance():
    script = """
import sys


class BlockLanceImports:
    def find_spec(self, fullname, path=None, target=None):
        if fullname == "lance" or fullname.startswith("lance."):
            raise ModuleNotFoundError(f"blocked optional dependency: {fullname}")
        return None


sys.meta_path.insert(0, BlockLanceImports())
import lancedb
"""

    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
