# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import importlib.metadata
import os
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from typing import Dict, Optional, Union, Any
import warnings

__version__ = importlib.metadata.version("lancedb")

from ._lancedb import connect as lancedb_connect
from .common import URI, sanitize_uri
from .db import AsyncConnection, DBConnection, LanceDBConnection
from .remote import ClientConfig
from .remote.db import RemoteDBConnection
from .schema import vector
from .table import AsyncTable


def connect(
    uri: URI,
    *,
    api_key: Optional[str] = None,
    region: str = "us-east-1",
    host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
    request_thread_pool: Optional[Union[int, ThreadPoolExecutor]] = None,
    client_config: Union[ClientConfig, Dict[str, Any], None] = None,
    storage_options: Optional[Dict[str, str]] = None,
    **kwargs: Any,
) -> DBConnection:
    """Connect to a LanceDB database.

    Parameters
    ----------
    uri: str or Path
        The uri of the database.
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
        <https://lancedb.github.io/lancedb/guides/storage/>

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

    Returns
    -------
    conn : DBConnection
        A connection to a LanceDB database.
    """
    if isinstance(uri, str) and uri.startswith("db://"):
        if api_key is None:
            api_key = os.environ.get("LANCEDB_API_KEY")
        if api_key is None:
            raise ValueError(f"api_key is required to connected LanceDB cloud: {uri}")
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

    if kwargs:
        raise ValueError(f"Unknown keyword arguments: {kwargs}")
    return LanceDBConnection(
        uri,
        read_consistency_interval=read_consistency_interval,
        storage_options=storage_options,
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
        <https://lancedb.github.io/lancedb/guides/storage/>

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

    return AsyncConnection(
        await lancedb_connect(
            sanitize_uri(uri),
            api_key,
            region,
            host_override,
            read_consistency_interval_secs,
            client_config,
            storage_options,
        )
    )


__all__ = [
    "connect",
    "connect_async",
    "AsyncConnection",
    "AsyncTable",
    "URI",
    "sanitize_uri",
    "vector",
    "DBConnection",
    "LanceDBConnection",
    "RemoteDBConnection",
    "__version__",
]


def __warn_on_fork():
    warnings.warn(
        "lance is not fork-safe. If you are using multiprocessing, use spawn instead.",
    )


if hasattr(os, "register_at_fork"):
    os.register_at_fork(before=__warn_on_fork)
