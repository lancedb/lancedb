# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pickle

from lancedb.remote.errors import HttpError, LanceDBClientError, RetryError


def _assert_roundtrip(exc, attrs):
    out = pickle.loads(pickle.dumps(exc))
    assert type(out) is type(exc)
    assert str(out) == str(exc)
    for name in attrs:
        assert getattr(out, name) == getattr(exc, name)


def test_lancedb_client_error_pickle_roundtrip():
    e = LanceDBClientError("connection reset", request_id="abc-123", status_code=503)
    _assert_roundtrip(e, ["request_id", "status_code"])


def test_http_error_pickle_roundtrip():
    e = HttpError("500 internal", request_id="req-1", status_code=500)
    _assert_roundtrip(e, ["request_id", "status_code"])


def test_retry_error_pickle_roundtrip():
    e = RetryError(
        "retries exhausted",
        request_id="req-2",
        request_failures=3,
        connect_failures=0,
        read_failures=0,
        max_request_failures=3,
        max_connect_failures=3,
        max_read_failures=3,
        status_code=503,
    )
    _assert_roundtrip(
        e,
        [
            "request_id",
            "request_failures",
            "connect_failures",
            "read_failures",
            "max_request_failures",
            "max_connect_failures",
            "max_read_failures",
            "status_code",
        ],
    )
