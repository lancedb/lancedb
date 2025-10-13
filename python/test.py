import requests
from lancedb.pydantic import LanceModel, Vector
import importlib
import io
import os

import lancedb
import numpy as np
import pandas as pd
import pyarrow as pa
import pytest
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector, MultiVector

db = lancedb.connect("~/.db")
registry = get_registry()
func = registry.get("multimodal-late-interaction").create(
    model_name="vidore/colQwen2.5-v0.2",
    device="auto",
    batch_size=1,
)

class MediaItems(LanceModel):
    text: str
    image_uri: str = func.SourceField()
    image_bytes: bytes = func.SourceField()
    image_vectors: MultiVector(func.ndims()) = func.VectorField()

table = db.create_table("media", schema=MediaItems, mode="overwrite")

texts = [
    "a cute cat playing with yarn",
    "a puppy in a flower field",
    "a red sports car on the highway",
]

uris = [
    "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg",
    "http://farm1.staticflickr.com/134/332220238_da527d8140_z.jpg",
    "http://farm5.staticflickr.com/4092/5017326486_1f46057f5f_z.jpg",
]

image_bytes = [requests.get(uri).content for uri in uris]

table.add(
    pd.DataFrame({"text": texts, "image_uri": uris, "image_bytes": image_bytes})
)

result = (
    table.search("fluffy companion", vector_column_name="image_vectors")
    .limit(1)
    .to_pydantic(MediaItems)[0]
)
assert any(keyword in result.text.lower() for keyword in ("cat", "puppy"))

first_row = table.to_arrow().to_pylist()[0]
assert len(first_row["image_vectors"]) > 1
assert len(first_row["image_vectors"][0]) == func.ndims()