# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from datetime import timedelta

import lancedb
import pytest
from lance_namespace import connect as connect_namespace_client
from lancedb import _lancedb
from lancedb.namespace import LanceNamespaceDBConnection


@pytest.mark.parametrize("seconds", [-1.0, -0.000001])
@pytest.mark.parametrize("kind", ["sync", "namespace", "namespace_async", "client"])
def test_negative_read_consistency_interval(tmp_path, seconds, kind):
    interval = timedelta(seconds=seconds)
    with pytest.raises(ValueError, match="read_consistency_interval"):
        if kind == "sync":
            lancedb.connect(tmp_path, read_consistency_interval=interval)
        elif kind == "client":
            client = connect_namespace_client("dir", {"root": str(tmp_path)})
            LanceNamespaceDBConnection(client, read_consistency_interval=interval)
        else:
            connect = (
                lancedb.connect_namespace
                if kind == "namespace"
                else lancedb.connect_namespace_async
            )
            connect("dir", {"root": str(tmp_path)}, read_consistency_interval=interval)


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [-1.0, -0.000001])
async def test_negative_async_read_consistency_interval(tmp_path, seconds):
    with pytest.raises(ValueError, match="read_consistency_interval"):
        await lancedb.connect_async(
            tmp_path, read_consistency_interval=timedelta(seconds=seconds)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("seconds", [float("nan"), float("inf"), -float("inf"), 1e30])
@pytest.mark.parametrize("kind", ["connect", "namespace", "client"])
async def test_binding_rejects_invalid_read_consistency_interval(
    tmp_path, seconds, kind
):
    with pytest.raises(ValueError, match="read_consistency_interval"):
        if kind == "connect":
            await _lancedb.connect(str(tmp_path), read_consistency_interval=seconds)
        elif kind == "namespace":
            _lancedb.connect_namespace(
                "dir", {"root": str(tmp_path)}, read_consistency_interval=seconds
            )
        else:
            client = connect_namespace_client("dir", {"root": str(tmp_path)})
            _lancedb.connect_namespace_client(client, read_consistency_interval=seconds)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "interval", [None, timedelta(0), timedelta(microseconds=1), timedelta(seconds=5)]
)
async def test_valid_read_consistency_intervals(tmp_path, interval):
    db = lancedb.connect(tmp_path / "sync", read_consistency_interval=interval)
    assert db.read_consistency_interval == interval
    db.close()

    db = await lancedb.connect_async(
        tmp_path / "async", read_consistency_interval=interval
    )
    assert await db.get_read_consistency_interval() == interval
    db.close()

    db = lancedb.connect_namespace(
        "dir", {"root": str(tmp_path / "namespace")}, read_consistency_interval=interval
    )
    assert db.read_consistency_interval == interval
    db.close()

    client = connect_namespace_client("dir", {"root": str(tmp_path / "client")})
    db = LanceNamespaceDBConnection(client, read_consistency_interval=interval)
    assert db.read_consistency_interval == interval
    db.close()
