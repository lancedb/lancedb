# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Models for namespace operations.

Re-exports models from lance_namespace_urllib3_client.
"""

from lance_namespace_urllib3_client.models import (
    CreateNamespaceResponse,
    DescribeNamespaceResponse,
    DropNamespaceResponse,
    ListNamespacesResponse,
    ListTablesResponse,
)

__all__ = [
    "ListNamespacesResponse",
    "CreateNamespaceResponse",
    "DropNamespaceResponse",
    "DescribeNamespaceResponse",
    "ListTablesResponse",
]
