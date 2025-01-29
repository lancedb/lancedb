# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import Optional


class LanceDBClientError(RuntimeError):
    """An error that occurred in the LanceDB client.

    Attributes
    ----------
    message: str
        The error message.
    request_id: str
        The id of the request that failed. This can be provided in error reports
        to help diagnose the issue.
    status_code: int
        The HTTP status code of the response. May be None if the request
        failed before the response was received.
    """

    def __init__(
        self, message: str, request_id: str, status_code: Optional[int] = None
    ):
        super().__init__(message)
        self.request_id = request_id
        self.status_code = status_code


class HttpError(LanceDBClientError):
    """An error that occurred during an HTTP request.

    Attributes
    ----------
    message: str
        The error message.
    request_id: str
        The id of the request that failed. This can be provided in error reports
        to help diagnose the issue.
    status_code: int
        The HTTP status code of the response. May be None if the request
        failed before the response was received.
    """

    pass


class RetryError(LanceDBClientError):
    """An error that occurs when the client has exceeded the maximum number of retries.

    The retry strategy can be adjusted by setting the
    [retry_config](lancedb.remote.ClientConfig.retry_config) in the client
    configuration. This is passed in the `client_config` argument of
    [connect](lancedb.connect) and [connect_async](lancedb.connect_async).

    The __cause__ attribute of this exception will be the last exception that
    caused the retry to fail. It will be an
    [HttpError][lancedb.remote.errors.HttpError] instance.

    Attributes
    ----------
    message: str
        The retry error message, which will describe which retry limit was hit.
    request_id: str
        The id of the request that failed. This can be provided in error reports
        to help diagnose the issue.
    request_failures: int
        The number of request failures.
    connect_failures: int
        The number of connect failures.
    read_failures: int
        The number of read failures.
    max_request_failures: int
        The maximum number of request failures.
    max_connect_failures: int
        The maximum number of connect failures.
    max_read_failures: int
        The maximum number of read failures.
    status_code: int
        The HTTP status code of the last response. May be None if the request
        failed before the response was received.
    """

    def __init__(
        self,
        message: str,
        request_id: str,
        request_failures: int,
        connect_failures: int,
        read_failures: int,
        max_request_failures: int,
        max_connect_failures: int,
        max_read_failures: int,
        status_code: Optional[int],
    ):
        super().__init__(message, request_id, status_code)
        self.request_failures = request_failures
        self.connect_failures = connect_failures
        self.read_failures = read_failures
        self.max_request_failures = max_request_failures
        self.max_connect_failures = max_connect_failures
        self.max_read_failures = max_read_failures
