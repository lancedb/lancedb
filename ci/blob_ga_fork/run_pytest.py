# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Load diagnostics by filename; never put checkout product code on PYTHONPATH."""

import importlib.util
import sys
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location(
    "fork_probe", Path(__file__).with_name("fork_probe.py")
)
probe = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = probe
spec.loader.exec_module(probe)
raise SystemExit(pytest.main(sys.argv[1:], plugins=[probe]))
