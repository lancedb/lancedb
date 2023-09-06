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

from typing import Optional

from .db import URI, DBConnection, LanceDBConnection
from .remote.db import RemoteDBConnection
from .schema import vector


def connect(
    uri: URI,
    *,
    api_key: Optional[str] = None,
    region: str = "us-west-2",
    host_override: Optional[str] = None,
) -> DBConnection:
    """Connect to a LanceDB database.

    Parameters
    ----------
    uri: str or Path
        The uri of the database.
    api_key: str, optional
        If presented, connect to LanceDB cloud.
        Otherwise, connect to a database on file system or cloud storage.
    region: str, default "us-west-2"
        The region to use for LanceDB Cloud.
    host_override: str, optional
        The override url for LanceDB Cloud.

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
            raise ValueError(f"api_key is required to connected LanceDB cloud: {uri}")
        return RemoteDBConnection(uri, api_key, region, host_override)
    return LanceDBConnection(uri)
