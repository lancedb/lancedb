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

from dataclasses import dataclass, field
from datetime import timedelta
from typing import List, Optional

from lancedb import __version__

__all__ = ["TimeoutConfig", "RetryConfig", "ClientConfig"]


@dataclass
class TimeoutConfig:
    """Timeout configuration for remote HTTP client.

    Attributes
    ----------
    connect_timeout: Optional[timedelta]
        The timeout for establishing a connection. Default is 120 seconds (2 minutes).
        This can also be set via the environment variable
        `LANCE_CLIENT_CONNECT_TIMEOUT`, as an integer number of seconds.
    read_timeout: Optional[timedelta]
        The timeout for reading data from the server. Default is 300 seconds
        (5 minutes). This can also be set via the environment variable
        `LANCE_CLIENT_READ_TIMEOUT`, as an integer number of seconds.
    pool_idle_timeout: Optional[timedelta]
        The timeout for keeping idle connections in the connection pool. Default
        is 300 seconds (5 minutes). This can also be set via the environment variable
        `LANCE_CLIENT_CONNECTION_TIMEOUT`, as an integer number of seconds.
    """

    connect_timeout: Optional[timedelta] = None
    read_timeout: Optional[timedelta] = None
    pool_idle_timeout: Optional[timedelta] = None

    @staticmethod
    def __to_timedelta(value) -> Optional[timedelta]:
        if value is None:
            return None
        elif isinstance(value, timedelta):
            return value
        elif isinstance(value, (int, float)):
            return timedelta(seconds=value)
        else:
            raise ValueError(
                f"Invalid value for timeout: {value}, must be a timedelta "
                "or number of seconds"
            )

    def __post_init__(self):
        self.connect_timeout = self.__to_timedelta(self.connect_timeout)
        self.read_timeout = self.__to_timedelta(self.read_timeout)
        self.pool_idle_timeout = self.__to_timedelta(self.pool_idle_timeout)


@dataclass
class RetryConfig:
    """Retry configuration for the remote HTTP client.

    Attributes
    ----------
    retries: Optional[int]
        The maximum number of retries for a request. Default is 3. You can also set this
        via the environment variable `LANCE_CLIENT_MAX_RETRIES`.
    connect_retries: Optional[int]
        The maximum number of retries for connection errors. Default is 3. You can also
        set this via the environment variable `LANCE_CLIENT_CONNECT_RETRIES`.
    read_retries: Optional[int]
        The maximum number of retries for read errors. Default is 3. You can also set
        this via the environment variable `LANCE_CLIENT_READ_RETRIES`.
    backoff_factor: Optional[float]
        The backoff factor to apply between retries. Default is 0.25. Between each retry
        the client will wait for the amount of seconds:
        `{backoff factor} * (2 ** ({number of previous retries}))`. So for the default
        of 0.25, the first retry will wait 0.25 seconds, the second retry will wait 0.5
        seconds, the third retry will wait 1 second, etc.

        You can also set this via the environment variable
        `LANCE_CLIENT_RETRY_BACKOFF_FACTOR`.
    backoff_jitter: Optional[float]
        The jitter to apply to the backoff factor, in seconds. Default is 0.25.

        A random value between 0 and `backoff_jitter` will be added to the backoff
        factor in seconds. So for the default of 0.25 seconds, between 0 and 250
        milliseconds will be added to the sleep between each retry.

        You can also set this via the environment variable
        `LANCE_CLIENT_RETRY_BACKOFF_JITTER`.
    statuses: Optional[List[int]
        The HTTP status codes for which to retry the request. Default is
        [429, 500, 502, 503].

        You can also set this via the environment variable
        `LANCE_CLIENT_RETRY_STATUSES`. Use a comma-separated list of integers.
    """

    retries: Optional[int] = None
    connect_retries: Optional[int] = None
    read_retries: Optional[int] = None
    backoff_factor: Optional[float] = None
    backoff_jitter: Optional[float] = None
    statuses: Optional[List[int]] = None


@dataclass
class ClientConfig:
    user_agent: str = f"LanceDB-Python-Client/{__version__}"
    retry_config: RetryConfig = field(default_factory=RetryConfig)
    timeout_config: Optional[TimeoutConfig] = field(default_factory=TimeoutConfig)

    def __post_init__(self):
        if isinstance(self.retry_config, dict):
            self.retry_config = RetryConfig(**self.retry_config)
        if isinstance(self.timeout_config, dict):
            self.timeout_config = TimeoutConfig(**self.timeout_config)
