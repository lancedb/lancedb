# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest

import lancedb


@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
async def test_clone_exist_ok_preserves_target(tmp_path, asynchronous):
    source_uri = str(tmp_path / "source.lance")
    if asynchronous:
        db = await lancedb.connect_async(tmp_path)
        await db.create_table("source", [{"id": 1}])
        table = await db.clone_table("target", source_uri, exist_ok=True)
        assert (await table.to_arrow()).to_pylist() == [{"id": 1}]
        await table.add([{"id": 2}])
        version = await table.version()
        with pytest.raises(ValueError, match="already exists"):
            await db.clone_table("target", source_uri)
        kept = await db.clone_table("target", "missing.lance", exist_ok=True)
        assert await kept.version() == version
        assert (await kept.to_arrow()).to_pylist() == [{"id": 1}, {"id": 2}]
    else:
        db = lancedb.connect(tmp_path)
        db.create_table("source", [{"id": 1}])
        table = db.clone_table("target", source_uri, exist_ok=True)
        assert table.to_arrow().to_pylist() == [{"id": 1}]
        table.add([{"id": 2}])
        version = table.version
        with pytest.raises(ValueError, match="already exists"):
            db.clone_table("target", source_uri)
        kept = db.clone_table("target", "missing.lance", exist_ok=True)
        assert kept.version == version
        assert kept.to_arrow().to_pylist() == [{"id": 1}, {"id": 2}]
