# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Models for namespace operations."""

from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional


class NamespaceMode(str, Enum):
    """Mode for creating namespaces."""

    CREATE = "create"
    EXIST_OK = "exist_ok"
    OVERWRITE = "overwrite"


class DropMode(str, Enum):
    """Mode for dropping namespaces."""

    SKIP = "skip"
    FAIL = "fail"


class DropBehavior(str, Enum):
    """Behavior for dropping namespaces."""

    RESTRICT = "restrict"
    CASCADE = "cascade"


@dataclass
class ListNamespacesResponse:
    """Response from listing namespaces.

    Attributes
    ----------
    namespaces : List[str]
        List of namespace names
    page_token : Optional[str]
        Token for pagination, if there are more results
    """

    namespaces: List[str]
    page_token: Optional[str] = None


@dataclass
class CreateNamespaceResponse:
    """Response from creating a namespace.

    Attributes
    ----------
    properties : Optional[Dict[str, str]]
        Properties of the created namespace
    """

    properties: Optional[Dict[str, str]] = None


@dataclass
class DropNamespaceResponse:
    """Response from dropping a namespace.

    Attributes
    ----------
    properties : Optional[Dict[str, str]]
        Properties of the dropped namespace
    transaction_id : Optional[List[str]]
        Transaction ID for long-running operations
    """

    properties: Optional[Dict[str, str]] = None
    transaction_id: Optional[List[str]] = None


@dataclass
class DescribeNamespaceResponse:
    """Response from describing a namespace.

    Attributes
    ----------
    properties : Optional[Dict[str, str]]
        Properties of the namespace
    """

    properties: Optional[Dict[str, str]] = None


@dataclass
class ListTablesResponse:
    """Response from listing tables.

    Attributes
    ----------
    tables : List[str]
        List of table names
    page_token : Optional[str]
        Token for pagination, if there are more results
    """

    tables: List[str]
    page_token: Optional[str] = None
