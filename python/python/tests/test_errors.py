# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pickle

from lancedb.remote.errors import HttpError, LanceDBClientError, RetryError


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
