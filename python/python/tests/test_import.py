# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import re
import shutil
import subprocess
import sys

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
