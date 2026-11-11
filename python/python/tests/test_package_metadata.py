# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import importlib
import importlib.metadata
import re
import sys
from pathlib import Path

import pytest


@pytest.mark.parametrize("distribution", ["lancedb", "lancedb-compat"])
def test_version_matches_package_metadata(distribution):
    import lancedb

    try:
        package_version = importlib.metadata.version(distribution)
    except importlib.metadata.PackageNotFoundError:
        pytest.skip(f"{distribution} is not installed")

    assert lancedb.__version__ == package_version


@pytest.mark.parametrize("name", ["JobInfo", "JobDescription", "JobFailureInfo"])
def test_job_metadata_types_have_resolvable_modules(name):
    """Documentation tools resolve re-exports through each type's module."""
    public_type = getattr(importlib.import_module("lancedb.job"), name)
    defining_module = importlib.import_module(public_type.__module__)

    assert getattr(defining_module, public_type.__name__, None) is public_type


def test_gpu_class_reexport_has_resolvable_module():
    import lancedb

    public_type = lancedb.GpuClass
    defining_module = importlib.import_module(public_type.__module__)

    assert getattr(defining_module, public_type.__name__, None) is public_type


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
    configured_abis = re.findall(r'"(?:pyo3/)?(abi3-py\d+)"', cargo_manifest)

    # PyO3 0.29 configures the ABI through the runtime dependency; its
    # build-config crate no longer exposes ABI features.
    assert configured_abis == [expected_abi], (
        "the pyo3 runtime ABI feature must match requires-python"
    )


def test_cargo_profiles_preserve_abi3_and_enable_native_free_threading():
    project_dir = Path(__file__).parents[2]
    cargo_manifest = (project_dir / "Cargo.toml").read_text()
    features_section = re.search(
        r"(?ms)^\[features\]\s*(.*?)(?=^\[[^]]+\]|\Z)", cargo_manifest
    )
    assert features_section is not None

    def feature_values(name):
        match = re.search(
            rf"(?ms)^{re.escape(name)}\s*=\s*\[(.*?)\]", features_section.group(1)
        )
        assert match is not None, f"missing Cargo feature {name!r}"
        return re.findall(r'"([^"]+)"', match.group(1))

    default_python_features = {
        "remote",
        "lancedb/aws",
        "lancedb/gcs",
        "lancedb/azure",
        "lancedb/dynamodb",
        "lancedb/oss",
        "lancedb/huggingface",
        "lancedb/cos",
        "lancedb/goosefs",
        "lancedb/metrics-otel",
    }
    assert set(feature_values("default-python")) == default_python_features
    assert set(feature_values("default")) == {"stable-abi", "default-python"}
    assert feature_values("stable-abi") == ["pyo3/abi3-py310"]
    assert feature_values("free-threaded") == ["default-python"]

    pyo3_dependency = re.search(r"(?m)^pyo3\s*=\s*\{([^}]*)\}", cargo_manifest)
    assert pyo3_dependency is not None
    assert "abi3-py310" not in pyo3_dependency.group(1)


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
