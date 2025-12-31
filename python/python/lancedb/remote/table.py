# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
Backwards compatibility alias for RemoteTable.

RemoteTable is now consolidated with Table. Cloud tables are created
using Table._from_async() and detected via the is_cloud property.
"""

from ..table import Table

# RemoteTable is now an alias for Table
# Cloud vs local behavior is determined by the is_cloud property
RemoteTable = Table

__all__ = ["RemoteTable"]
