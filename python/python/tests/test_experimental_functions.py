# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb
import pytest

pytestmark = pytest.mark.skipif(
    lancedb._lancedb._experimental_functions_enabled,
    reason="tests the default build without experimental-functions",
)


@pytest.mark.parametrize("namespace", [False, True])
def test_default_build_rejects_local_functions(tmp_path, namespace):
    db = (
        lancedb.connect_namespace("dir", {"root": str(tmp_path)})
        if namespace
        else lancedb.connect(tmp_path)
    )
    table = db.create_table("source", [{"x": 1}, {"x": 2}])
    version = table.version
    operations = [
        lambda: table.add_columns(computed={"doubled": "x * 2"}),
        lambda: table.refresh_column("x"),
        lambda: table.refresh_column_async("x"),
        lambda: db.create_materialized_view("blocked", "source"),
        lambda: db.create_materialized_view("blocked", "source", with_no_data=True),
        lambda: db.create_materialized_view_async("blocked", "source"),
    ]
    for operation in operations:
        with pytest.raises(NotImplementedError, match="experimental-functions"):
            operation()
    assert table.version == version
    assert db.table_names() == ["source"]
    table.add_columns({"eager": "x * 2"})
    assert table.to_arrow()["eager"].to_pylist() == [2, 4]


@pytest.mark.asyncio
async def test_async_default_build_rejects_local_functions(tmp_path):
    db = await lancedb.connect_async(tmp_path)
    table = await db.create_table("source", [{"x": 1}, {"x": 2}])
    version = await table.version()
    operations = [
        lambda: table.add_columns(computed={"doubled": "x * 2"}),
        lambda: table.refresh_column("x"),
        lambda: table.refresh_column_async("x"),
        lambda: db.create_materialized_view("blocked", "source"),
        lambda: db.create_materialized_view("blocked", "source", with_no_data=True),
        lambda: db.create_materialized_view_async("blocked", "source"),
    ]
    for operation in operations:
        with pytest.raises(NotImplementedError, match="experimental-functions"):
            await operation()
    assert await table.version() == version
    assert await db.table_names() == ["source"]
    await table.add_columns({"eager": "x * 2"})
    assert (await table.query().to_arrow())["eager"].to_pylist() == [2, 4]
