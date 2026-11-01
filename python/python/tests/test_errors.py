# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pickle

import pytest
from lance_namespace.errors import ErrorCode
from lance_namespace.errors import TableNotFoundError as NamespaceTableNotFoundError

from lancedb.db import DBConnection
from lancedb.remote.errors import (
    AlreadyExistsError,
    FunctionNotFoundError,
    HttpError,
    LanceDBClientError,
    NamespaceNotFoundError,
    RetryError,
    SecretNotFoundError,
    TableNotFoundError,
)
from lancedb.table import Table


@pytest.mark.parametrize(
    "base, method",
    [
        (Table, "to_lance"),
        (Table, "to_polars"),
        (DBConnection, "drop_all_tables"),
        (DBConnection, "drop_database"),
    ],
)
def test_base_unsupported_methods_have_messages(base, method):
    with pytest.raises(NotImplementedError) as exc_info:
        getattr(base, method)(None)
    message = str(exc_info.value)
    assert method in message
    assert "not supported" in message


def test_pickle_lancedb_client_error():
    err = LanceDBClientError("something went wrong", "req-123", 400)
    restored = pickle.loads(pickle.dumps(err))
    assert str(restored) == "something went wrong"
    assert restored.request_id == "req-123"
    assert restored.status_code == 400


def test_pickle_lancedb_client_error_no_status_code():
    err = LanceDBClientError("fail", "req-456")
    restored = pickle.loads(pickle.dumps(err))
    assert str(restored) == "fail"
    assert restored.request_id == "req-456"
    assert restored.status_code is None


def test_pickle_http_error():
    err = HttpError("not found", "req-789", 404)
    restored = pickle.loads(pickle.dumps(err))
    assert isinstance(restored, HttpError)
    assert str(restored) == "not found"
    assert restored.request_id == "req-789"
    assert restored.status_code == 404
    assert restored.is_transport_error is False


def test_pickle_http_transport_error():
    err = HttpError("connection reset", "req-789", is_transport_error=True)
    restored = pickle.loads(pickle.dumps(err))
    assert isinstance(restored, HttpError)
    assert restored.is_transport_error is True
    assert restored.request_id == "req-789"


@pytest.mark.parametrize(
    "error_code,resource_kind,status_code,expected_type",
    [
        ("not_found", "secret", 404, SecretNotFoundError),
        ("not_found", "function", 404, FunctionNotFoundError),
        ("not_found", "namespace", 404, NamespaceNotFoundError),
        ("already_exists", "secret", 409, AlreadyExistsError),
    ],
)
def test_http_error_structured_resource_error(
    error_code, resource_kind, status_code, expected_type
):
    err = HttpError(
        "resource request failed",
        "req-structured",
        status_code,
        error_code,
        resource_kind,
        "resource-name",
        ["parent", "child"],
    )

    assert type(err) is expected_type
    assert isinstance(err, HttpError)
    assert err.error_code == error_code
    assert err.resource_kind == resource_kind
    assert err.resource_name == "resource-name"
    assert err.namespace_path == ("parent", "child")
    assert err.request_id == "req-structured"
    assert err.status_code == status_code
    assert err.is_transport_error is False


@pytest.mark.parametrize(
    "error_code,resource_kind",
    [
        (None, None),
        ("not_found", None),
        ("not_found", "unknown"),
        ("invalid", "secret"),
    ],
)
def test_http_error_without_known_structured_fields_stays_generic(
    error_code, resource_kind
):
    err = HttpError(
        "not found",
        "req-generic",
        404,
        error_code,
        resource_kind,
        "resource-name",
        [],
    )

    assert type(err) is HttpError
    assert err.error_code == error_code
    assert err.resource_kind == resource_kind


@pytest.mark.parametrize(
    "exception_type,error_code,resource_kind",
    [
        (SecretNotFoundError, "not_found", "secret"),
        (FunctionNotFoundError, "not_found", "function"),
        (NamespaceNotFoundError, "not_found", "namespace"),
        (AlreadyExistsError, "already_exists", "namespace"),
    ],
)
@pytest.mark.parametrize("is_transport_error", [False, True])
def test_pickle_structured_http_error(
    exception_type, error_code, resource_kind, is_transport_error
):
    err = HttpError(
        "resource request failed",
        "req-pickle",
        409 if error_code == "already_exists" else 404,
        error_code,
        resource_kind,
        "resource-name",
        ["parent"],
        is_transport_error=is_transport_error,
    )
    assert type(err) is exception_type

    restored = pickle.loads(pickle.dumps(err))

    assert type(restored) is exception_type
    assert restored.error_code == error_code
    assert restored.resource_kind == resource_kind
    assert restored.resource_name == "resource-name"
    assert restored.namespace_path == ("parent",)
    assert restored.request_id == "req-pickle"
    assert restored.status_code == err.status_code
    assert restored.is_transport_error is is_transport_error


def test_pickle_table_not_found_error():
    err = TableNotFoundError("Table 'missing' was not found", "req-missing", 404)
    restored = pickle.loads(pickle.dumps(err))
    assert isinstance(restored, TableNotFoundError)
    assert isinstance(restored, NamespaceTableNotFoundError)
    assert isinstance(restored, ValueError)
    assert isinstance(restored, HttpError)
    assert restored.code == ErrorCode.TABLE_NOT_FOUND
    assert str(restored) == "Table 'missing' was not found"
    assert restored.request_id == "req-missing"
    assert restored.status_code == 404


def test_pickle_retry_error():
    err = RetryError(
        "max retries exceeded",
        "req-abc",
        request_failures=3,
        connect_failures=1,
        read_failures=2,
        max_request_failures=5,
        max_connect_failures=3,
        max_read_failures=3,
        status_code=503,
    )
    restored = pickle.loads(pickle.dumps(err))
    assert isinstance(restored, RetryError)
    assert str(restored) == "max retries exceeded"
    assert restored.request_id == "req-abc"
    assert restored.request_failures == 3
    assert restored.connect_failures == 1
    assert restored.read_failures == 2
    assert restored.max_request_failures == 5
    assert restored.max_connect_failures == 3
    assert restored.max_read_failures == 3
    assert restored.status_code == 503
