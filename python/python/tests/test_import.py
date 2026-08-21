# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os
import re
import shutil
import subprocess
import sys

import lancedb
import lancedb._lancedb as _lancedb
import pytest


@pytest.mark.skipif(sys.platform != "linux", reason="ldd is Linux-specific")
def test_native_extension_does_not_link_openssl():
    """OpenSSL-linked wheels abort when imported on RHEL hosts in FIPS mode."""
    ldd = shutil.which("ldd")
    if ldd is None:
        pytest.skip("ldd is not installed")

    result = subprocess.run(
        [ldd, _lancedb.__file__],
        check=True,
        capture_output=True,
        text=True,
    )
    openssl_libraries = re.findall(
        r"^\s*(lib(?:crypto|ssl)\S*)\s+=>", result.stdout, flags=re.MULTILINE
    )

    assert not openssl_libraries, (
        "the LanceDB native extension must use rustls instead of linking OpenSSL: "
        f"{openssl_libraries}"
    )


def test_top_level_version_comes_from_native_module():
    child_code = """
import importlib.metadata

orig_version = importlib.metadata.version

def fail_version_lookup(_: str) -> str:
    if _ in {"lancedb", "lancedb-compat"}:
        raise AssertionError("lancedb import should not query distribution metadata")
    return orig_version(_)

importlib.metadata.version = fail_version_lookup

import lancedb
import lancedb._lancedb as native_module
import lancedb.db

assert lancedb.__version__ == native_module.__version__
"""
    env = os.environ.copy()
    env["PYTHONPATH"] = os.pathsep.join(path for path in sys.path if path)

    result = subprocess.run(
        [sys.executable, "-c", child_code],
        check=False,
        capture_output=True,
        text=True,
        env=env,
    )

    assert result.returncode == 0, result.stderr or result.stdout
