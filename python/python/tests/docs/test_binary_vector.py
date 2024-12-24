import shutil
import lancedb
import numpy as np
import pytest

shutil.rmtree("data/binary-lancedb", ignore_errors=True)


def test_binary_vector():
    # --8<-- [start:sync_binary_vector]
    db = lancedb.connect("data/binary-lancedb")
    data = [
        {
            "id": i,
            "vector": np.random.randint(0, 256, size=16),
        }
        for i in range(1024)
    ]
    tbl = db.create_table("my_binary_vectors", data=data)
    query = np.random.randint(0, 256, size=16)
    tbl.search(query).to_arrow()
    # --8<-- [end:sync_binary_vector]


@pytest.mark.asyncio
async def test_binary_vector_async():
    # --8<-- [start:async_binary_vector]
    db = await lancedb.connect_async("data/binary-lancedb")
    data = [
        {
            "id": i,
            "vector": np.random.randint(0, 256, size=16),
        }
        for i in range(1024)
    ]
    tbl = await db.create_table("my_binary_vectors", data=data)
    query = np.random.randint(0, 256, size=16)
    await tbl.query().nearest_to(query).to_arrow()
    # --8<-- [end:async_binary_vector]
