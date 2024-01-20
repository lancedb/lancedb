#  Copyright 2024 LanceDB Developers
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

# This module contains an adapter that will close connections if they have not been
# used before a certain timeout. This is necessary because some load balancers will
# close connections after a certain amount of time, but the request module may not yet
# have received the FIN/ACK and will try to reuse the connection.
#
# TODO some of the code here can be simplified if/when this PR is merged:
# https://github.com/urllib3/urllib3/pull/3275

import datetime
import logging
import os

from requests.adapters import HTTPAdapter
from urllib3.connection import HTTPSConnection
from urllib3.connectionpool import HTTPSConnectionPool
from urllib3.poolmanager import PoolManager


def get_client_connection_timeout() -> int:
    return int(os.environ.get("LANCE_CLIENT_CONNECTION_TIMEOUT", "300"))


class LanceDBHTTPSConnection(HTTPSConnection):
    """
    HTTPSConnection that tracks the last time it was used.
    """

    idle_timeout: datetime.timedelta
    last_activity: datetime.datetime

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.last_activity = datetime.datetime.now()

    def request(self, *args, **kwargs):
        self.last_activity = datetime.datetime.now()
        super().request(*args, **kwargs)

    def is_expired(self):
        return datetime.datetime.now() - self.last_activity > self.idle_timeout


def LanceDBHTTPSConnectionPoolFactory(client_idle_timeout: int):
    """
    Creates a connection pool class that can be used to close idle connections.
    """

    class LanceDBHTTPSConnectionPool(HTTPSConnectionPool):
        # override the connection class
        ConnectionCls = LanceDBHTTPSConnection

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)

        def _get_conn(self, timeout: float | None = None):
            logging.debug("Getting https connection")
            conn = super()._get_conn(timeout)
            if conn.is_expired():
                logging.debug("Closing expired connection")
                conn.close()

            return conn

        def _new_conn(self):
            conn = super()._new_conn()
            conn.idle_timeout = datetime.timedelta(seconds=client_idle_timeout)
            return conn

    return LanceDBHTTPSConnectionPool


class LanceDBClientPoolManager(PoolManager):
    def __init__(
        self, client_idle_timeout: int, num_pools: int, maxsize: int, **kwargs
    ):
        super().__init__(num_pools=num_pools, maxsize=maxsize, **kwargs)
        # inject our connection pool impl
        connection_pool_class = LanceDBHTTPSConnectionPoolFactory(
            client_idle_timeout=client_idle_timeout
        )
        self.pool_classes_by_scheme["https"] = connection_pool_class


def LanceDBClientHTTPAdapterFactory():
    """
    Creates an HTTPAdapter class that can be used to close idle connections
    """

    # closure over the timeout
    client_idle_timeout = get_client_connection_timeout()

    class LanceDBClientRequestHTTPAdapter(HTTPAdapter):
        def init_poolmanager(self, connections, maxsize, block=False):
            # inject our pool manager impl
            self.poolmanager = LanceDBClientPoolManager(
                client_idle_timeout=client_idle_timeout,
                num_pools=connections,
                maxsize=maxsize,
                block=block,
            )

    return LanceDBClientRequestHTTPAdapter
