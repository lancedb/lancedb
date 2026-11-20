# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
"""Deprecated result imports. Use :mod:`lancedb.optimize` instead."""

from .optimize import OptimizeResult

CompactionResult = OptimizeResult

__all__ = ["CompactionResult"]
