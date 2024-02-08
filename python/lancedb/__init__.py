#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import importlib.metadata
import os
from datetime import timedelta
from typing import Optional

__version__ = importlib.metadata.version("lancedb")

from .common import URI
from .db import DBConnection, LanceDBConnection
from .remote.db import RemoteDBConnection
from .schema import vector  # noqa: F401
from .utils import sentry_log  # noqa: F401


def connect(
    uri: URI,
    *,
    api_key: Optional[str] = None,
    region: str = "us-east-1",
    host_override: Optional[str] = None,
    read_consistency_interval: Optional[timedelta] = None,
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


    Examples
    --------

    For a local directory, provide a path for the database:

    >>> import lancedb
    >>> db = lancedb.connect("~/.lancedb")

    For object storage, use a URI prefix:

    >>> db = lancedb.connect("s3://my-bucket/lancedb")

    Connect to LancdDB cloud:

    >>> db = lancedb.connect("db://my_database", api_key="ldb_...")

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
        return RemoteDBConnection(uri, api_key, region, host_override)
    return LanceDBConnection(uri, read_consistency_interval=read_consistency_interval)
