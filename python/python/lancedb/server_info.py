# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Remote server build information returned as JSON-compatible dictionaries."""

from typing import Optional, TypedDict


class ServerBuildInfo(TypedDict):
    """Immutable identity compiled into a server executable."""

    release: str
    git_sha: str
    build_date: Optional[str]


class ServerInfo(TypedDict):
    """The responding query node and its configured Function runtime.

    ``query_node`` can be ``None`` when the executable has no build metadata.
    ``build_date`` is a UTC RFC 3339 timestamp when known. During a rolling
    upgrade, separate requests can reach different query-node builds.
    Configured images do not establish the version of a running job.
    """

    query_node: Optional[ServerBuildInfo]
    lance_version: str
    lancedb_version: str
    function_worker_protocol_version: int
    configured_query_node_image: Optional[str]
    configured_job_worker_image: Optional[str]
    configured_sql_job_image: Optional[str]
    configured_gpu_worker_image: Optional[str]
