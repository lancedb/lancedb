# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest

import lancedb


@pytest.mark.experimental_functions
@pytest.mark.asyncio
@pytest.mark.parametrize("asynchronous", [False, True])
@pytest.mark.parametrize("background", [False, True])
@pytest.mark.parametrize("namespace", [False, True])
async def test_exist_ok_preserves_materialized_data(
    tmp_path, asynchronous, background, namespace
):
    options = {"storage_options": {"new_table_enable_stable_row_ids": "true"}}
    if asynchronous:
        db = (
            lancedb.connect_namespace_async("dir", {"root": str(tmp_path)}, **options)
            if namespace
            else await lancedb.connect_async(tmp_path, **options)
        )
        source = await db.create_table("source", [{"id": 1}])
        original = await db.create_materialized_view("saved", "source", exist_ok=True)
        definition = await original.definition()
        version = await original.table.version()
        await source.add([{"id": 2}])
        if background:
            job = await db.create_materialized_view_async(
                "saved", "missing", exist_ok=True
            )
            await job.wait()
            kept = await db.open_materialized_view("saved")
        else:
            kept = await db.create_materialized_view("saved", "missing", exist_ok=True)
        assert (await kept.definition()).query == definition.query
        assert await kept.table.version() == version
        assert (await kept.table.to_arrow()).to_pylist() == [{"id": 1}]
        with pytest.raises(ValueError, match="already exists"):
            await db.create_materialized_view("saved", "source")
    else:
        db = (
            lancedb.connect_namespace("dir", {"root": str(tmp_path)}, **options)
            if namespace
            else lancedb.connect(tmp_path, **options)
        )
        source = db.create_table("source", [{"id": 1}])
        original = db.create_materialized_view("saved", "source", exist_ok=True)
        definition = original.definition
        version = original.table.version
        source.add([{"id": 2}])
        if background:
            job = db.create_materialized_view_async("saved", "missing", exist_ok=True)
            job.wait()
            kept = db.open_materialized_view("saved")
        else:
            kept = db.create_materialized_view("saved", "missing", exist_ok=True)
        assert kept.definition.query == definition.query
        assert kept.table.version == version
        assert kept.table.to_arrow().to_pylist() == [{"id": 1}]
        with pytest.raises(ValueError, match="already exists"):
            db.create_materialized_view("saved", "source")
