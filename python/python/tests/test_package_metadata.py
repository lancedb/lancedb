# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import importlib
import re
import sys
from pathlib import Path

import pytest


def test_pyo3_abi_matches_minimum_supported_python():
    project_dir = Path(__file__).parents[2]
    pyproject = (project_dir / "pyproject.toml").read_text()
    cargo_manifest = (project_dir / "Cargo.toml").read_text()

    minimum_python = re.search(
        r'^requires-python\s*=\s*">=(\d+)\.(\d+)"$', pyproject, re.MULTILINE
    )
    assert minimum_python is not None

    major, minor = minimum_python.groups()
    expected_abi = f"abi3-py{major}{minor}"
    configured_abis = re.findall(r'"(abi3-py\d+)"', cargo_manifest)

    assert configured_abis == [expected_abi, expected_abi], (
        "the pyo3 runtime and build ABI features must both match requires-python"
    )


@pytest.mark.skipif(sys.platform != "win32", reason="Windows wheel regression test")
def test_windows_wheel_tag_and_native_import():
    project_dir = Path(__file__).parents[2]
    wheels = list((project_dir.parent / "target" / "wheels").glob("lancedb-*.whl"))
    if not wheels:
        pytest.skip("no wheel artifact is available in this development environment")

    assert len(wheels) == 1
    assert wheels[0].name.endswith("-cp310-abi3-win_amd64.whl")

    native_module = importlib.import_module("lancedb._lancedb")
    assert Path(native_module.__file__).suffix == ".pyd"
