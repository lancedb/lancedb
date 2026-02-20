import shutil
from pathlib import Path

import lancedb
import lancedb.pydantic
import pandas as pd
import pytest

from lancedb.index import BTree

class TestSchema(lancedb.pydantic.LanceModel):
    pk: str
    txt: str

@pytest.fixture(scope="function")
def lancedb_test_path(tmp_path_factory: pytest.TempPathFactory):
    temp_dir = tmp_path_factory.mktemp("lancedb_test")
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.mark.asyncio
async def test_delete_preserves_scalar_indices(lancedb_test_path):
    db = await lancedb.connect_async(str(lancedb_test_path))
    table = await db.create_table("test_table", schema=TestSchema, mode="create")

    # Create index on 'txt'
    await table.create_index(column="txt", config=BTree())

    # Confirm index was created
    indices_before = await table.list_indices()
    assert len(indices_before) == 1
    assert indices_before[0].columns == ["txt"]
    assert indices_before[0].index_type == "BTree"

    # Add data
    df = pd.DataFrame({"pk": [str(i) for i in range(5)], "txt": [str(i) for i in range(5)]})
    await table.add(data=df)

    # Delete one row
    await table.delete(where="txt = '2'")

    # Confirm row count changed
    row_count = await table.count_rows()
    assert row_count == 4

    # Confirm index still exists
    indices_after = await table.list_indices()
    assert len(indices_after) == 1
    assert indices_after[0].columns == ["txt"]
    assert indices_after[0].index_type == "BTree"
