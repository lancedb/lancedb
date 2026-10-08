# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import inspect
from urllib.parse import parse_qs, urlparse

import lancedb
import pyarrow as pa
import pytest

from test_remote_db import mock_lancedb_connection, mock_lancedb_connection_async


@pytest.mark.parametrize("uri", ["memory://", "db://dev"])
def test_unknown_connect_options(uri):
    with pytest.raises(ValueError, match="Unknown keyword arguments.*bogus"):
        lancedb.connect(uri, api_key="fake", bogus=1)


@pytest.mark.parametrize("option", ["session", "manifest_enabled"])
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.asyncio
async def test_remote_connect_rejects_local_options(option, asynchronous):
    value = lancedb.Session() if option == "session" else True
    with pytest.raises(NotImplementedError, match=option):
        if asynchronous:
            await lancedb.connect_async("db://dev", api_key="fake", **{option: value})
        else:
            lancedb.connect("db://dev", api_key="fake", **{option: value})


def test_remote_create_signature():
    from lancedb.db import DBConnection
    from lancedb.remote.db import RemoteDBConnection

    expected = inspect.signature(DBConnection.create_table).parameters
    actual = inspect.signature(RemoteDBConnection.create_table).parameters
    assert list(actual) == list(expected)
    for name in expected:
        assert actual[name].kind == expected[name].kind
        assert actual[name].default == expected[name].default


def test_remote_create_positional_mode():
    modes = []

    def handler(request):
        assert urlparse(request.path).path == "/v1/table/ct_pos/create/"
        modes.append(parse_qs(urlparse(request.path).query)["mode"])
        request.send_response(200)
        request.end_headers()

    with mock_lancedb_connection(handler) as db:
        data = [{"value": 1}]
        db.create_table("ct_pos", data, None, "overwrite")
        db.create_table("ct_pos", data, None, "overwrite")
        db.create_table("ct_pos", data, None, "create", True)
        with pytest.raises(ValueError, match="Invalid mode"):
            db.create_table("ct_pos", data, None, "drop")
    assert modes == [["overwrite"], ["overwrite"], ["exist_ok"]]


CREATE_OPTIONS = [
    {"new_table_data_storage_version": "2.0"},
    {"new_table_enable_v2_manifest_paths": "false"},
    {"bogus_option_xyz": "1"},
]


@pytest.mark.parametrize("storage_options", CREATE_OPTIONS)
@pytest.mark.parametrize("empty", [False, True])
@pytest.mark.asyncio
async def test_remote_create_rejects_write_options(storage_options, empty):
    requests = []

    def handler(request):
        requests.append(request.path)
        request.send_response(200)
        request.end_headers()

    kwargs = {"schema": pa.schema([("value", pa.int64())])}
    if not empty:
        kwargs["data"] = [{"value": 1}]
    async with mock_lancedb_connection_async(handler) as db:
        with pytest.raises(NotImplementedError, match="write_options.*remote"):
            await db.create_table("t", storage_options=storage_options, **kwargs)
    assert requests == []


@pytest.mark.parametrize(
    "options",
    [
        {"data_storage_version": "2.0"},
        {"enable_v2_manifest_paths": False},
        {"storage_options": {"x": "1"}},
    ],
)
def test_sync_remote_create_rejects_write_options(options):
    requests = []

    def handler(request):
        requests.append(request.path)
        request.send_response(200)
        request.end_headers()

    with mock_lancedb_connection(handler) as db:
        with pytest.raises(NotImplementedError, match="remote"):
            db.create_table("t", [{"value": 1}], **options)
    assert requests == []


@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize(
    "options", [{"index_cache_size": 10}, {"storage_options": {"x": "y"}}]
)
@pytest.mark.asyncio
async def test_remote_open_rejects_local_options(options, asynchronous):
    requests = []

    def handler(request):
        requests.append(request.path)
        request.send_response(200)
        request.end_headers()
        request.wfile.write(b'{"version": 1, "schema": {"fields": []}}')

    if asynchronous:
        async with mock_lancedb_connection_async(handler) as db:
            with pytest.raises(NotImplementedError, match="remote"):
                await db.open_table("t", **options)
    else:
        with mock_lancedb_connection(handler) as db:
            with pytest.raises(NotImplementedError, match="remote"):
                db.open_table("t", **options)
    assert requests == []


@pytest.mark.asyncio
async def test_remote_open_rejects_location():
    requests = []

    def handler(request):
        requests.append(request.path)
        request.send_response(200)
        request.end_headers()
        request.wfile.write(b'{"version": 1, "schema": {"fields": []}}')

    async with mock_lancedb_connection_async(handler) as db:
        with pytest.raises(NotImplementedError, match="location.*remote"):
            await db.open_table("t", location="/tmp/zzz")
    assert requests == []
