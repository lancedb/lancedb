#  Copyright (c) 2023. LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
import io

import numpy as np
import pandas as pd
import pytest
import requests

import lancedb
from lancedb.embeddings import EmbeddingFunctionRegistry
from lancedb.pydantic import LanceModel, Vector

# These are integration tests for embedding functions.
# They are slow because they require downloading models
# or connection to external api


@pytest.mark.slow
@pytest.mark.parametrize("alias", ["sentence-transformers", "openai"])
def test_sentence_transformer(alias, tmp_path):
    db = lancedb.connect(tmp_path)
    registry = EmbeddingFunctionRegistry.get_instance()
    func = registry.get(alias).create()

    class Words(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    table = db.create_table("words", schema=Words)
    table.add(
        pd.DataFrame(
            {
                "text": [
                    "hello world",
                    "goodbye world",
                    "fizz",
                    "buzz",
                    "foo",
                    "bar",
                    "baz",
                ]
            }
        )
    )

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]

    vec = func.compute_query_embeddings(query)[0]
    expected = table.search(vec).limit(1).to_pydantic(Words)[0]
    assert actual.text == expected.text
    assert actual.text == "hello world"


@pytest.mark.slow
def test_openclip(tmp_path):
    from PIL import Image

    db = lancedb.connect(tmp_path)
    registry = EmbeddingFunctionRegistry.get_instance()
    func = registry.get("open-clip").create()

    class Images(LanceModel):
        label: str
        image_uri: str = func.SourceField()
        image_bytes: bytes = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()
        vec_from_bytes: Vector(func.ndims()) = func.VectorField()

    table = db.create_table("images", schema=Images)
    labels = ["cat", "cat", "dog", "dog", "horse", "horse"]
    uris = [
        "http://farm1.staticflickr.com/53/167798175_7c7845bbbd_z.jpg",
        "http://farm1.staticflickr.com/134/332220238_da527d8140_z.jpg",
        "http://farm9.staticflickr.com/8387/8602747737_2e5c2a45d4_z.jpg",
        "http://farm5.staticflickr.com/4092/5017326486_1f46057f5f_z.jpg",
        "http://farm9.staticflickr.com/8216/8434969557_d37882c42d_z.jpg",
        "http://farm6.staticflickr.com/5142/5835678453_4f3a4edb45_z.jpg",
    ]
    # get each uri as bytes
    image_bytes = [requests.get(uri).content for uri in uris]
    table.add(
        pd.DataFrame({"label": labels, "image_uri": uris, "image_bytes": image_bytes})
    )

    # text search
    actual = table.search("man's best friend").limit(1).to_pydantic(Images)[0]
    assert actual.label == "dog"
    frombytes = (
        table.search("man's best friend", vector_column_name="vec_from_bytes")
        .limit(1)
        .to_pydantic(Images)[0]
    )
    assert actual.label == frombytes.label
    assert np.allclose(actual.vector, frombytes.vector)

    # image search
    query_image_uri = "http://farm1.staticflickr.com/200/467715466_ed4a31801f_z.jpg"
    image_bytes = requests.get(query_image_uri).content
    query_image = Image.open(io.BytesIO(image_bytes))
    actual = table.search(query_image).limit(1).to_pydantic(Images)[0]
    assert actual.label == "dog"
    other = (
        table.search(query_image, vector_column_name="vec_from_bytes")
        .limit(1)
        .to_pydantic(Images)[0]
    )
    assert actual.label == other.label

    arrow_table = table.search().select(["vector", "vec_from_bytes"]).to_arrow()
    assert np.allclose(
        arrow_table["vector"].combine_chunks().values.to_numpy(),
        arrow_table["vec_from_bytes"].combine_chunks().values.to_numpy(),
    )
