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
import importlib
import io
import os

import lancedb
import numpy as np
import pandas as pd
import pytest
import requests
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector

# These are integration tests for embedding functions.
# They are slow because they require downloading models
# or connection to external api


try:
    if importlib.util.find_spec("mlx.core") is not None:
        _mlx = True
    else:
        _mlx = None
except Exception:
    _mlx = None

try:
    if importlib.util.find_spec("imagebind") is not None:
        _imagebind = True
    else:
        _imagebind = None
except Exception:
    _imagebind = None


@pytest.mark.slow
@pytest.mark.parametrize(
    "alias", ["sentence-transformers", "openai", "huggingface", "ollama"]
)
def test_basic_text_embeddings(alias, tmp_path):
    db = lancedb.connect(tmp_path)
    registry = get_registry()
    func = registry.get(alias).create(max_retries=0)
    func2 = registry.get(alias).create(max_retries=0)

    class Words(LanceModel):
        text: str = func.SourceField()
        text2: str = func2.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()
        vector2: Vector(func2.ndims()) = func2.VectorField()

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
                ],
                "text2": [
                    "to be or not to be",
                    "that is the question",
                    "for whether tis nobler",
                    "in the mind to suffer",
                    "the slings and arrows",
                    "of outrageous fortune",
                    "or to take arms",
                ],
            }
        )
    )

    query = "greeting"
    actual = (
        table.search(query, vector_column_name="vector").limit(1).to_pydantic(Words)[0]
    )

    vec = func.compute_query_embeddings(query)[0]
    expected = (
        table.search(vec, vector_column_name="vector").limit(1).to_pydantic(Words)[0]
    )
    assert actual.text == expected.text
    assert actual.text == "hello world"
    assert not np.allclose(actual.vector, actual.vector2)

    actual = (
        table.search(query, vector_column_name="vector2").limit(1).to_pydantic(Words)[0]
    )
    assert actual.text != "hello world"
    assert not np.allclose(actual.vector, actual.vector2)


@pytest.mark.slow
def test_openclip(tmp_path):
    from PIL import Image

    db = lancedb.connect(tmp_path)
    registry = get_registry()
    func = registry.get("open-clip").create(max_retries=0)

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
    actual = (
        table.search("man's best friend", vector_column_name="vector")
        .limit(1)
        .to_pydantic(Images)[0]
    )
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
    actual = (
        table.search(query_image, vector_column_name="vector")
        .limit(1)
        .to_pydantic(Images)[0]
    )
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


@pytest.mark.skipif(
    _imagebind is None,
    reason="skip if imagebind not installed.",
)
@pytest.mark.slow
def test_imagebind(tmp_path):
    import os
    import shutil
    import tempfile

    import pandas as pd
    import requests

    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    with tempfile.TemporaryDirectory() as temp_dir:
        print(f"Created temporary directory {temp_dir}")

        def download_images(image_uris):
            downloaded_image_paths = []
            for uri in image_uris:
                try:
                    response = requests.get(uri, stream=True)
                    if response.status_code == 200:
                        # Extract image name from URI
                        image_name = os.path.basename(uri)
                        image_path = os.path.join(temp_dir, image_name)
                        with open(image_path, "wb") as out_file:
                            shutil.copyfileobj(response.raw, out_file)
                        downloaded_image_paths.append(image_path)
                except Exception as e:  # noqa: PERF203
                    print(f"Failed to download {uri}. Error: {e}")
            return temp_dir, downloaded_image_paths

        db = lancedb.connect(tmp_path)
        registry = get_registry()
        func = registry.get("imagebind").create(max_retries=0)

        class Images(LanceModel):
            label: str
            image_uri: str = func.SourceField()
            vector: Vector(func.ndims()) = func.VectorField()

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
        temp_dir, downloaded_images = download_images(uris)
        table.add(pd.DataFrame({"label": labels, "image_uri": downloaded_images}))
        # text search
        actual = (
            table.search("man's best friend", vector_column_name="vector")
            .limit(1)
            .to_pydantic(Images)[0]
        )
        assert actual.label == "dog"

        # image search
        query_image_uri = [
            "https://live.staticflickr.com/65535/33336453970_491665f66e_h.jpg"
        ]
        temp_dir, downloaded_images = download_images(query_image_uri)
        query_image_uri = downloaded_images[0]
        actual = (
            table.search(query_image_uri, vector_column_name="vector")
            .limit(1)
            .to_pydantic(Images)[0]
        )
        assert actual.label == "dog"

    if os.path.isdir(temp_dir):
        shutil.rmtree(temp_dir)
        print(f"Deleted temporary directory {temp_dir}")


@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("COHERE_API_KEY") is None, reason="COHERE_API_KEY not set"
)  # also skip if cohere not installed
def test_cohere_embedding_function():
    cohere = (
        get_registry()
        .get("cohere")
        .create(name="embed-multilingual-v2.0", max_retries=0)
    )

    class TextModel(LanceModel):
        text: str = cohere.SourceField()
        vector: Vector(cohere.ndims()) = cohere.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect("~/lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    assert len(tbl.to_pandas()["vector"][0]) == cohere.ndims()


@pytest.mark.slow
def test_instructor_embedding(tmp_path):
    model = get_registry().get("instructor").create(max_retries=0)

    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect(tmp_path)
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    assert len(tbl.to_pandas()["vector"][0]) == model.ndims()


@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("GOOGLE_API_KEY") is None, reason="GOOGLE_API_KEY not set"
)
def test_gemini_embedding(tmp_path):
    model = get_registry().get("gemini-text").create(max_retries=0)

    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect(tmp_path)
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    assert len(tbl.to_pandas()["vector"][0]) == model.ndims()
    assert tbl.search("hello").limit(1).to_pandas()["text"][0] == "hello world"


@pytest.mark.skipif(
    _mlx is None,
    reason="mlx tests only required for apple users.",
)
@pytest.mark.slow
def test_gte_embedding(tmp_path):
    model = get_registry().get("gte-text").create()

    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect(tmp_path)
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    assert len(tbl.to_pandas()["vector"][0]) == model.ndims()
    assert tbl.search("hello").limit(1).to_pandas()["text"][0] == "hello world"


def aws_setup():
    try:
        import boto3

        sts = boto3.client("sts")
        sts.get_caller_identity()
        return True
    except Exception:
        return False


@pytest.mark.slow
@pytest.mark.skipif(
    not aws_setup(), reason="AWS credentials not set or libraries not installed"
)
def test_bedrock_embedding(tmp_path):
    for name in [
        "amazon.titan-embed-text-v1",
        "cohere.embed-english-v3",
        "cohere.embed-multilingual-v3",
    ]:
        model = get_registry().get("bedrock-text").create(max_retries=0, name=name)

        class TextModel(LanceModel):
            text: str = model.SourceField()
            vector: Vector(model.ndims()) = model.VectorField()

        df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
        db = lancedb.connect(tmp_path)
        tbl = db.create_table("test", schema=TextModel, mode="overwrite")

        tbl.add(df)
        assert len(tbl.to_pandas()["vector"][0]) == model.ndims()


@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("OPENAI_API_KEY") is None, reason="OPENAI_API_KEY not set"
)
def test_openai_embedding(tmp_path):
    def _get_table(model):
        class TextModel(LanceModel):
            text: str = model.SourceField()
            vector: Vector(model.ndims()) = model.VectorField()

        db = lancedb.connect(tmp_path)
        tbl = db.create_table("test", schema=TextModel, mode="overwrite")

        return tbl

    model = get_registry().get("openai").create(max_retries=0)
    tbl = _get_table(model)
    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})

    tbl.add(df)
    assert len(tbl.to_pandas()["vector"][0]) == model.ndims()
    assert tbl.search("hello").limit(1).to_pandas()["text"][0] == "hello world"

    model = (
        get_registry()
        .get("openai")
        .create(max_retries=0, name="text-embedding-3-large")
    )
    tbl = _get_table(model)

    tbl.add(df)
    assert len(tbl.to_pandas()["vector"][0]) == model.ndims()
    assert tbl.search("hello").limit(1).to_pandas()["text"][0] == "hello world"

    model = (
        get_registry()
        .get("openai")
        .create(max_retries=0, name="text-embedding-3-large", dim=1024)
    )
    tbl = _get_table(model)

    tbl.add(df)
    assert len(tbl.to_pandas()["vector"][0]) == model.ndims()
    assert tbl.search("hello").limit(1).to_pandas()["text"][0] == "hello world"


@pytest.mark.slow
@pytest.mark.skipif(
    os.environ.get("WATSONX_API_KEY") is None
    or os.environ.get("WATSONX_PROJECT_ID") is None,
    reason="WATSONX_API_KEY and WATSONX_PROJECT_ID not set",
)
def test_watsonx_embedding(tmp_path):
    from lancedb.embeddings import WatsonxEmbeddings

    for name in WatsonxEmbeddings.model_names():
        model = get_registry().get("watsonx").create(max_retries=0, name=name)

        class TextModel(LanceModel):
            text: str = model.SourceField()
            vector: Vector(model.ndims()) = model.VectorField()

        db = lancedb.connect("~/.lancedb")
        tbl = db.create_table("watsonx_test", schema=TextModel, mode="overwrite")
        df = pd.DataFrame({"text": ["hello world", "goodbye world"]})

        tbl.add(df)
        assert len(tbl.to_pandas()["vector"][0]) == model.ndims()
        assert tbl.search("hello").limit(1).to_pandas()["text"][0] == "hello world"
