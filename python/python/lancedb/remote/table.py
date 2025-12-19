# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
Backwards compatibility alias for RemoteTable.

RemoteTable is now consolidated with LanceTable. Cloud tables are created
using LanceTable._from_async() and detected via the is_cloud property.
"""

from ..table import LanceTable

# RemoteTable is now an alias for LanceTable
# Cloud vs local behavior is determined by the is_cloud property
RemoteTable = LanceTable

__all__ = ["RemoteTable"]
