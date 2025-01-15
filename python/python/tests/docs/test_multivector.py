import shutil
import pytest

# --8<-- [start:imports]
import lancedb
import numpy as np
# --8<-- [end:imports]

shutil.rmtree("data/multivector_demo", ignore_errors=True)


def test_binary_vector():
    # --8<-- [start:sync_multivector]
    db = lancedb.connect("data/multivector_demo")
    data = [
        {
            "id": i,
            "vector": np.random.random(size=(2, 256)),
        }
        for i in range(1024)
    ]
    tbl = db.create_table("my_table", data=data)

    # query with single vector
    query = np.random.random(256)
    tbl.search(query).to_arrow()

    # query with multiple vectors
    query = np.random.random(size=(2, 256))
    tbl.search(query).to_arrow()

    # --8<-- [end:sync_multivector]
    db.drop_table("my_table")


@pytest.mark.asyncio
async def test_binary_vector_async():
    # --8<-- [start:async_multivector]
    db = await lancedb.connect_async("data/multivector_demo")
    data = [
        {
            "id": i,
            "vector": np.random.random(size=(2, 256)),
        }
        for i in range(1024)
    ]
    tbl = await db.create_table("my_table", data=data)

    # query with single vector
    query = np.random.random(256)
    await tbl.query().nearest_to(query).to_arrow()

    # query with multiple vectors
    query = np.random.random(size=(2, 256))

    # --8<-- [end:async_multivector]
    await db.drop_table("my_table")
