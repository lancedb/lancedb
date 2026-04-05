# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import importlib.metadata
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Dict, Optional, Union, Any, List
import warnings

__version__ = importlib.metadata.version("lancedb")

from ._lancedb import connect as lancedb_connect
from .common import URI, sanitize_uri
from urllib.parse import urlparse
from .db import AsyncConnection, DBConnection, LanceDBConnection
from .remote import ClientConfig
from .remote.db import RemoteDBConnection
from .expr import Expr, col, lit, func
from .schema import vector
from .table import AsyncTable, Table
from ._lancedb import Session
from .namespace import (
    connect_namespace,
    connect_namespace_async,
    LanceNamespaceDBConnection,
    AsyncLanceNamespaceDBConnection,
)


def _check_s3_bucket_with_dots(
    uri: str, storage_options: Optional[Dict[str, str]]
) -> None:
    """
    Check if an S3 URI has a bucket name containing dots and warn if no region
    is specified. S3 buckets with dots cannot use virtual-hosted-style URLs,
    which breaks automatic region detection.

    See: https://github.com/lancedb/lancedb/issues/1898
    """
    if not isinstance(uri, str) or not uri.startswith("s3://"):
        return

    parsed = urlparse(uri)
    bucket = parsed.netloc

    if "." not in bucket:
        return

    # Check if region is provided in storage_options
    region_keys = {"region", "aws_region"}
    has_region = storage_options and any(k in storage_options for k in region_keys)

    if not has_region:
        raise ValueError(
            f"S3 bucket name '{bucket}' contains dots, which prevents automatic "
            f"region detection. Please specify the region explicitly via "
            f"storage_options={{'region': '<your-region>'}} or "
            f"storage_options={{'aws_region': '<your-region>'}}. "
            f"See https://github.com/lancedb/lancedb/issues/1898 for details."
        )


def connect(
    uri: Optional[URI] = None,
    *,
    api_key: Optional[str] = None,
    region: str = "us-east-1",
    host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
    request_thread_pool: Optional[Union[int, ThreadPoolExecutor]] = None,
    client_config: Union[ClientConfig, Dict[str, Any], None] = None,
    storage_options: Optional[Dict[str, str]] = None,
    session: Optional[Session] = None,
    namespace_client_impl: Optional[str] = None,
    namespace_client_properties: Optional[Dict[str, str]] = None,
    namespace_client_pushdown_operations: Optional[List[str]] = None,
    **kwargs: Any,
) -> DBConnection:
    """Connect to a LanceDB database.

    Parameters
    ----------
    uri: str or Path, optional
        The uri of the database. When ``namespace_client_impl`` is provided you may
        omit ``uri`` and connect through a namespace client instead.
    api_key: str, optional
        If presented, connect to LanceDB cloud.
        Otherwise, connect to a database on file system or cloud storage.
        Can be set via environment variable `LANCEDB_API_KEY`.
    region: str, default "us-east-1"
        The region to use for LanceDB Cloud.
    host_override: str, optional
        The override url for LanceDB Cloud.
    read_consistency_interval: timedelta, default None
        (For LanceDB OSS only)
        The interval at which to check for updates to the table from other
        processes. If None, then consistency is not checked. For performance
        reasons, this is the default. For strong consistency, set this to
        zero seconds. Then every read will check for updates from other
        processes. As a compromise, you can set this to a non-zero timedelta
        for eventual consistency. If more than that interval has passed since
        the last check, then the table will be checked for updates. Note: this
        consistency only applies to read operations. Write operations are
        always consistent.
    client_config: ClientConfig or dict, optional
        Configuration options for the LanceDB Cloud HTTP client. If a dict, then
        the keys are the attributes of the ClientConfig class. If None, then the
        default configuration is used.
    storage_options: dict, optional
        Additional options for the storage backend. See available options at
        <https://lancedb.com/docs/storage/>
    session: Session, optional
        (For LanceDB OSS only)
        A session to use for this connection. Sessions allow you to configure
        cache sizes for index and metadata caches, which can significantly
        impact memory use and performance. They can also be re-used across
        multiple connections to share the same cache state.
    namespace_client_impl : str, optional
        When provided along with ``namespace_client_properties``, ``connect``
        returns a namespace-backed connection by delegating to
        :func:`connect_namespace`. The value identifies which namespace
        implementation to load (e.g., ``"dir"`` or ``"rest"``).
    namespace_client_properties : dict, optional
        Configuration to pass to the namespace client implementation. Required
        when ``namespace_client_impl`` is set.
    namespace_client_pushdown_operations : list[str], optional
        Only used when ``namespace_client_properties`` is provided. Forwards to
        :func:`connect_namespace` to control which operations are executed on the
        namespace service (e.g., ``["QueryTable", "CreateTable"]``).

    Examples
    --------

    For a local directory, provide a path for the database:

    >>> import lancedb
    >>> db = lancedb.connect("~/.lancedb")

    For object storage, use a URI prefix:

    >>> db = lancedb.connect("s3://my-bucket/lancedb",
    ...                      storage_options={"aws_access_key_id": "***"})

    Connect to LanceDB cloud:

    >>> db = lancedb.connect("db://my_database", api_key="ldb_...",
    ...                      client_config={"retry_config": {"retries": 5}})

    Connect to a namespace-backed database:

    >>> db = lancedb.connect(namespace_client_impl="dir",
    ...                      namespace_client_properties={"root": "/tmp/ns"})

    Returns
    -------
    conn : DBConnection
        A connection to a LanceDB database.
    """
    if namespace_client_impl is not None or namespace_client_properties is not None:
        if namespace_client_impl is None or namespace_client_properties is None:
            raise ValueError(
                "Both namespace_client_impl and "
                "namespace_client_properties must be provided"
            )
        if kwargs:
            raise ValueError(f"Unknown keyword arguments: {kwargs}")
        return connect_namespace(
            namespace_client_impl,
            namespace_client_properties,
            read_consistency_interval=read_consistency_interval,
            storage_options=storage_options,
            session=session,
            namespace_client_pushdown_operations=namespace_client_pushdown_operations,
        )

    if namespace_client_pushdown_operations is not None:
        raise ValueError(
            "namespace_client_pushdown_operations is only valid when "
            "connecting through a namespace"
        )
    if uri is None:
        raise ValueError(
            "uri is required when not connecting through a namespace client"
        )
    if isinstance(uri, str) and uri.startswith("db://"):
        if api_key is None:
            api_key = os.environ.get("LANCEDB_API_KEY")
        if api_key is None:
            raise ValueError(f"api_key is required to connect to LanceDB cloud: {uri}")
        if isinstance(request_thread_pool, int):
            request_thread_pool = ThreadPoolExecutor(request_thread_pool)
        return RemoteDBConnection(
            uri,
            api_key,
            region,
            host_override,
            # TODO: remove this (deprecation warning downstream)
            request_thread_pool=request_thread_pool,
            client_config=client_config,
            storage_options=storage_options,
            **kwargs,
        )
    _check_s3_bucket_with_dots(str(uri), storage_options)

    if kwargs:
        raise ValueError(f"Unknown keyword arguments: {kwargs}")

    return LanceDBConnection(
        uri,
        read_consistency_interval=read_consistency_interval,
        storage_options=storage_options,
        session=session,
    )


async def connect_async(
    uri: URI,
    *,
    api_key: Optional[str] = None,
    region: str = "us-east-1",
    host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
    client_config: Optional[Union[ClientConfig, Dict[str, Any]]] = None,
    storage_options: Optional[Dict[str, str]] = None,
    session: Optional[Session] = None,
) -> AsyncConnection:
    """Connect to a LanceDB database.

    Parameters
    ----------
    uri: str or Path
        The uri of the database.
    api_key: str, optional
        If present, connect to LanceDB cloud.
        Otherwise, connect to a database on file system or cloud storage.
        Can be set via environment variable `LANCEDB_API_KEY`.
    region: str, default "us-east-1"
        The region to use for LanceDB Cloud.
    host_override: str, optional
        The override url for LanceDB Cloud.
    read_consistency_interval: timedelta, default None
        (For LanceDB OSS only)
        The interval at which to check for updates to the table from other
        processes. If None, then consistency is not checked. For performance
        reasons, this is the default. For strong consistency, set this to
        zero seconds. Then every read will check for updates from other
        processes. As a compromise, you can set this to a non-zero timedelta
        for eventual consistency. If more than that interval has passed since
        the last check, then the table will be checked for updates. Note: this
        consistency only applies to read operations. Write operations are
        always consistent.
    client_config: ClientConfig or dict, optional
        Configuration options for the LanceDB Cloud HTTP client. If a dict, then
        the keys are the attributes of the ClientConfig class. If None, then the
        default configuration is used.
    storage_options: dict, optional
        Additional options for the storage backend. See available options at
        <https://lancedb.com/docs/storage/>
    session: Session, optional
        (For LanceDB OSS only)
        A session to use for this connection. Sessions allow you to configure
        cache sizes for index and metadata caches, which can significantly
        impact memory use and performance. They can also be re-used across
        multiple connections to share the same cache state.

    Examples
    --------

    >>> import lancedb
    >>> async def doctest_example():
    ...     # For a local directory, provide a path to the database
    ...     db = await lancedb.connect_async("~/.lancedb")
    ...     # For object storage, use a URI prefix
    ...     db = await lancedb.connect_async("s3://my-bucket/lancedb",
    ...                                      storage_options={
    ...                                          "aws_access_key_id": "***"})
    ...     # Connect to LanceDB cloud
    ...     db = await lancedb.connect_async("db://my_database", api_key="ldb_...",
    ...                                      client_config={
    ...                                          "retry_config": {"retries": 5}})

    Returns
    -------
    conn : AsyncConnection
        A connection to a LanceDB database.
    """
    if read_consistency_interval is not None:
        read_consistency_interval_secs = read_consistency_interval.total_seconds()
    else:
        read_consistency_interval_secs = None

    if isinstance(client_config, dict):
        client_config = ClientConfig(**client_config)

    _check_s3_bucket_with_dots(str(uri), storage_options)

    return AsyncConnection(
        await lancedb_connect(
            sanitize_uri(uri),
            api_key,
            region,
            host_override,
            read_consistency_interval_secs,
            client_config,
            storage_options,
            session,
        )
    )


__all__ = [
    "connect",
    "connect_async",
    "connect_namespace",
    "connect_namespace_async",
    "AsyncConnection",
    "AsyncLanceNamespaceDBConnection",
    "AsyncTable",
    "col",
    "Expr",
    "func",
    "lit",
    "URI",
    "sanitize_uri",
    "vector",
    "DBConnection",
    "LanceDBConnection",
    "LanceNamespaceDBConnection",
    "RemoteDBConnection",
    "Session",
    "Table",
    "__version__",
]


def __warn_on_fork():
    warnings.warn(
        "lance is not fork-safe. If you are using multiprocessing, use spawn instead.",
    )


if hasattr(os, "register_at_fork"):
    os.register_at_fork(before=__warn_on_fork)  # type: ignore[attr-defined]
