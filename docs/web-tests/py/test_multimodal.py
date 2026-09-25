# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest
try:
    import lancedb
    import numpy as np
    import pyarrow as pa
    import io
    from PIL import Image
except ImportError:
    pass

# --8<-- [start:multimodal_imports]
import lancedb
import pyarrow as pa
import pandas as pd
import numpy as np
import io
from PIL import Image
# --8<-- [end:multimodal_imports]

def test_multimodal_ingestion(db_path_factory):
    # Ensure dependencies are available
    pytest.importorskip("PIL")
    pytest.importorskip("lancedb")
    pytest.importorskip("numpy")

    # --8<-- [start:create_dummy_data]
    # Create some dummy images
    def create_dummy_image(color):
        img = Image.new('RGB', (100, 100), color=color)
        buf = io.BytesIO()
        img.save(buf, format='PNG')
        return buf.getvalue()

    # Create dataset with metadata, vectors, and image blobs
    data = [
        {
            "id": 1,
            "filename": "red_square.png",
            "vector": np.random.rand(128).astype(np.float32),
            "image_blob": create_dummy_image('red'),
            "label": "red"
        },
        {
            "id": 2,
            "filename": "blue_square.png",
            "vector": np.random.rand(128).astype(np.float32),
            "image_blob": create_dummy_image('blue'),
            "label": "blue"
        }
    ]
    # --8<-- [end:create_dummy_data]

    # --8<-- [start:define_schema]
    # Define schema explictly to ensure image_blob is treated as binary
    schema = pa.schema([
        pa.field("id", pa.int32()),
        pa.field("filename", pa.string()),
        pa.field("vector", pa.list_(pa.float32(), 128)),
        pa.field("image_blob", pa.binary()), # Important: Use pa.binary() for blobs
        pa.field("label", pa.string())
    ])
    # --8<-- [end:define_schema]

    db_uri = db_path_factory("multimodal_db")
    db = lancedb.connect(db_uri)

    # --8<-- [start:ingest_data]
    tbl = db.create_table("images", data=data, schema=schema, mode="overwrite")
    # --8<-- [end:ingest_data]
   
    assert len(tbl) == 2

    # --8<-- [start:search_data]
    # Search for similar images
    query_vector = np.random.rand(128).astype(np.float32)
    results = tbl.search(query_vector).limit(1).to_pandas()
    # --8<-- [end:search_data]

    # --8<-- [start:process_results]
    # Convert back to PIL Image
    for _, row in results.iterrows():
        image_bytes = row['image_blob']
        image = Image.open(io.BytesIO(image_bytes))
        print(f"Retrieved image: {row['filename']}, Size: {image.size}")
        # You can now use 'image' with other libraries or display it
    # --8<-- [end:process_results]
   
    assert len(results) == 1

def test_blob_api_definition(db_path_factory):
    # --8<-- [start:blob_api_schema]
    import pyarrow as pa

    # Define schema with Blob API metadata for lazy loading
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field(
            "video", 
            pa.large_binary(), 
            metadata={"lance-encoding:blob": "true"} # Enable Blob API
        ),
    ])
    # --8<-- [end:blob_api_schema]

    # --8<-- [start:blob_api_ingest]
    import lancedb

    db = lancedb.connect(db_path_factory("blob_db"))
    
    # Create sample data
    data = [
        {"id": 1, "video": b"fake_video_bytes_1"},
        {"id": 2, "video": b"fake_video_bytes_2"}
    ]
    
    # Create the table
    tbl = db.create_table("videos", data=data, schema=schema)
    # --8<-- [end:blob_api_ingest]
    assert len(tbl) == 2


def test_blob_api_to_pandas(db_path_factory):
    db = lancedb.connect(db_path_factory("blob_to_pandas_db"))
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field(
            "video",
            pa.large_binary(),
            metadata={"lance-encoding:blob": "true"},
        ),
    ])
    tbl = db.create_table(
        "videos",
        data=[
            {"id": 1, "video": b"fake_video_bytes_1"},
            {"id": 2, "video": b"fake_video_bytes_2"},
        ],
        schema=schema,
        mode="overwrite",
    )

    # --8<-- [start:blob_api_to_pandas]
    # Default: blob columns come back lazily
    df_lazy = tbl.to_pandas()

    # Materialize blob bytes eagerly
    df_bytes = tbl.to_pandas(blob_mode="bytes")

    # Return descriptors instead of payloads
    df_desc = tbl.to_pandas(blob_mode="descriptions")

    # Forward extra kwargs to PyArrow's to_pandas
    df_typed = tbl.to_pandas(split_blocks=True, self_destruct=True)
    # --8<-- [end:blob_api_to_pandas]

    assert len(df_lazy) == 2
    assert isinstance(df_bytes["video"].iloc[0], bytes)
    assert df_bytes["video"].tolist() == [
        b"fake_video_bytes_1",
        b"fake_video_bytes_2",
    ]
    assert len(df_desc) == 2
    assert len(df_typed) == 2


@pytest.mark.asyncio
async def test_query_to_pandas_kwargs(db_path_factory):
    schema = pa.schema([
        pa.field("id", pa.int64()),
        pa.field("vector", pa.list_(pa.float32(), 128)),
        pa.field(
            "video",
            pa.large_binary(),
            metadata={"lance-encoding:blob": "true"},
        ),
    ])
    data = [
        {
            "id": i,
            "vector": np.random.rand(128).astype(np.float32),
            "video": f"fake_video_bytes_{i}".encode(),
        }
        for i in range(10)
    ]

    db = lancedb.connect(db_path_factory("query_to_pandas_db"))
    tbl = db.create_table("search_demo", data=data, schema=schema, mode="overwrite")

    async_db = await lancedb.connect_async(
        str(db_path_factory("query_to_pandas_async_db"))
    )
    tbl_async = await async_db.create_table(
        "search_demo", data=data, schema=schema, mode="overwrite"
    )

    query_vector = np.random.rand(128).astype(np.float32)

    # --8<-- [start:query_to_pandas_kwargs]
    # Plain scan query: blob_mode is supported end to end
    df_lazy = (
        tbl.search()
        .where("id = 1")
        .select(["id", "video"])
        .to_pandas(blob_mode="lazy")
    )

    # Same call shape works on async query builders
    df_bytes = await (
        tbl_async.query()
        .where("id = 1")
        .select(["id", "video"])
        .to_pandas(blob_mode="bytes")
    )

    # Vector / FTS / hybrid queries can't materialize blob columns,
    # so omit them from the projection
    df_vec = (
        tbl.search(query_vector)
        .limit(10)
        .select(["id", "vector"])
        .to_pandas(split_blocks=True, self_destruct=True)
    )
    # --8<-- [end:query_to_pandas_kwargs]

    assert len(df_lazy) == 1
    assert len(df_bytes) == 1
    assert df_bytes["video"].iloc[0] == b"fake_video_bytes_1"
    assert len(df_vec) == 10
    assert "video" not in df_vec.columns
