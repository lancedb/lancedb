# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import Optional

from lance_namespace.errors import TableNotFoundError as _NamespaceTableNotFoundError


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

    def __reduce__(self) -> tuple[type, tuple]:
        return (self.__class__, (str(self), self.request_id, self.status_code))


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
    error_code: str
        A machine-readable error code reported by the server, if available.
    resource_kind: str
        The kind of resource reported by the server, if available.
    resource_name: str
        The name of the resource reported by the server, if available.
    namespace_path: tuple[str, ...]
        The parent namespace path of the resource, if available.
    """

    def __new__(
        cls,
        message: str,
        request_id: str,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
        resource_kind: Optional[str] = None,
        resource_name: Optional[str] = None,
        namespace_path: Optional[tuple[str, ...]] = None,
    ):
        if cls is HttpError:
            mapped_cls = _http_error_class(error_code, resource_kind)
            if mapped_cls is not None:
                cls = mapped_cls
        return super().__new__(cls)

    def __init__(
        self,
        message: str,
        request_id: str,
        status_code: Optional[int] = None,
        error_code: Optional[str] = None,
        resource_kind: Optional[str] = None,
        resource_name: Optional[str] = None,
        namespace_path: Optional[tuple[str, ...]] = None,
    ):
        super().__init__(message, request_id, status_code)
        self.error_code = error_code
        self.resource_kind = resource_kind
        self.resource_name = resource_name
        self.namespace_path = (
            tuple(namespace_path) if namespace_path is not None else None
        )

    def __reduce__(self) -> tuple[type, tuple]:
        return (
            self.__class__,
            (
                str(self),
                self.request_id,
                self.status_code,
                self.error_code,
                self.resource_kind,
                self.resource_name,
                self.namespace_path,
            ),
        )


def _http_error_class(
    error_code: Optional[str], resource_kind: Optional[str]
) -> Optional[type[HttpError]]:
    if error_code == "already_exists":
        return AlreadyExistsError
    if error_code != "not_found":
        return None
    return {
        "secret": SecretNotFoundError,
        "function": FunctionNotFoundError,
        "namespace": NamespaceNotFoundError,
    }.get(resource_kind)


class SecretNotFoundError(HttpError):
    """A remote secret was not found."""


class FunctionNotFoundError(HttpError):
    """A remote function was not found."""


class NamespaceNotFoundError(HttpError):
    """A remote namespace was not found."""


class AlreadyExistsError(HttpError):
    """A remote resource already exists."""


class TableNotFoundError(HttpError, ValueError, _NamespaceTableNotFoundError):
    """A remote table was not found.

    This is also a ``lance_namespace.errors.TableNotFoundError`` and a
    ``ValueError``, so existing missing-table handlers continue to work.
    Like [HttpError][lancedb.remote.errors.HttpError], it carries the
    ``request_id`` and ``status_code`` of the failed request.

    Examples
    --------
    >>> error = TableNotFoundError("Table 'missing' was not found", "req-123", 404)
    >>> isinstance(error, ValueError)
    True
    >>> error.request_id
    'req-123'
    """

    pass


class RetryError(LanceDBClientError):
    """An error that occurs when the client has exceeded the maximum number of retries.

    The retry strategy can be adjusted by setting the
    [retry_config][lancedb.remote.ClientConfig.retry_config] in the client
    configuration. This is passed in the `client_config` argument of
    [connect][lancedb.connect] and [connect_async][lancedb.connect_async].

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

    def __reduce__(self) -> tuple[type, tuple]:
        return (
            self.__class__,
            (
                str(self),
                self.request_id,
                self.request_failures,
                self.connect_failures,
                self.read_failures,
                self.max_request_failures,
                self.max_connect_failures,
                self.max_read_failures,
                self.status_code,
            ),
        )
