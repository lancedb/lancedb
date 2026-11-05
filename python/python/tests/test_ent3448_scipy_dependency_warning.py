# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""One-off proof for the original scipy.ndimage registration warning."""

import importlib.util

import pytest

from lancedb.functions import function


def uses_scipy_ndimage(value: int) -> float:
    import scipy.ndimage

    return float(scipy.ndimage.gaussian_filter1d([float(value)], sigma=1.0)[0])


def test_scipy_ndimage_without_declared_dependency_warns():
    assert importlib.util.find_spec("scipy") is None

    with pytest.warns(
        UserWarning,
        match=r"imports 'scipy'.*Remote availability cannot be verified",
    ):
        definition = function(pip=())(uses_scipy_ndimage)

    assert definition.registration_request.runtime.environment.packages == ()
