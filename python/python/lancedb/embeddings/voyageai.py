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

import os
from typing import ClassVar, TYPE_CHECKING, List, Union, Any

from pathlib import Path
from urllib.parse import urlparse
from io import BytesIO

import numpy as np
import pyarrow as pa
import requests

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import api_key_not_found_help, IMAGES, TEXT

if TYPE_CHECKING:
    import PIL


def is_valid_url(text):
    try:
        parsed = urlparse(text)
        return bool(parsed.scheme) and bool(parsed.netloc)
    except Exception:
        return False


def transform_input(input: Union[str, bytes, Path]):
    PIL = attempt_import_or_raise("PIL", "pillow")
    if isinstance(input, str):
        if is_valid_url(input):
            try:
                response = requests.get(input)
                return PIL.Image.open(BytesIO(response.content))
            except Exception as e:
                return input
        else:
            return input
    elif isinstance(input, PIL.Image.Image):
        return input
    elif isinstance(input, bytes):
        return PIL.Image.open(BytesIO(input))
    elif isinstance(input, Path):
        return PIL.Image.open(input)
    else:
        raise ValueError(f"Each input should be either str, bytes, Path or Image.")


def sanitize_multmodal_input(
        inputs: Union[TEXT, IMAGES]
) -> List[Any]:
    """
    Sanitize the input to the embedding function.
    """
    PIL = attempt_import_or_raise("PIL", "pillow")
    if isinstance(inputs, (str, bytes, Path, PIL.Image.Image)):
        inputs = [inputs]
    elif isinstance(inputs, pa.Array):
        inputs = inputs.to_pylist()
    elif isinstance(inputs, pa.ChunkedArray):
        inputs = inputs.combine_chunks().to_pylist()
    else:
        raise ValueError(f"Input type {type(inputs)} not allowed with multimodal model.")

    if not all(isinstance(x, (str, bytes, Path, PIL.Image.Image)) for x in inputs):
        raise ValueError(f"Each input should be either str, bytes, Path or Image.")

    return [[transform_input(i)] for i in inputs]


def sanitize_text_input(
        inputs: TEXT
) -> List[Any]:
    """
    Sanitize the input to the embedding function.
    """
    if isinstance(inputs, str):
        inputs = [inputs]
    elif isinstance(inputs, pa.Array):
        inputs = inputs.to_pylist()
    elif isinstance(inputs, pa.ChunkedArray):
        inputs = inputs.combine_chunks().to_pylist()
    else:
        raise ValueError(f"Input type {type(inputs)} not allowed with text model.")

    if not all(isinstance(x, str) for x in inputs):
        raise ValueError(f"Each input should be str.")

    return inputs


@register("voyageai")
class VoyageAIEmbeddingFunction(EmbeddingFunction):
    """
    An embedding function that uses the VoyageAI API

    https://docs.voyageai.com/docs/embeddings

    Parameters
    ----------
    name: str
        The name of the model to use. List of acceptable models:

            * voyage-3
            * voyage-3-lite
            * voyage-multimodal-3
            * voyage-finance-2
            * voyage-multilingual-2
            * voyage-law-2
            * voyage-code-2


    Examples
    --------
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import EmbeddingFunctionRegistry

    voyageai = EmbeddingFunctionRegistry
        .get_instance()
        .get("voyageai")
        .create(name="voyage-3")

    class TextModel(LanceModel):
        text: str = voyageai.SourceField()
        vector: Vector(voyageai.ndims()) =  voyageai.VectorField()

    data = [ { "text": "hello world" },
            { "text": "goodbye world" }]

    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)

    """

    name: str
    client: ClassVar = None
    text_embedding_models: list = [
        "voyage-3",
        "voyage-3-lite",
        "voyage-finance-2",
        "voyage-law-2",
        "voyage-code-2",
    ]
    multimodal_embedding_models: list = ["voyage-multimodal-3"]

    def _is_multimodal_model(self, model_name: str):
        return model_name in self.multimodal_embedding_models or 'multmodal' in model_name

    def ndims(self):
        if self.name == "voyage-3-lite":
            return 512
        elif self.name == "voyage-code-2":
            return 1536
        elif self.name in [
            "voyage-3",
            "voyage-multimodal-3",
            "voyage-finance-2",
            "voyage-multilingual-2",
            "voyage-law-2",
            "voyage-multimodal-3",
        ]:
            return 1024
        else:
            raise ValueError(f"Model {self.name} not supported")

    def compute_query_embeddings(
            self, query: Union[str, "PIL.Image.Image"], *args, **kwargs
    ) -> List[np.ndarray]:
        """
        Compute the embeddings for a given user query

        Parameters
        ----------
        query : Union[str, PIL.Image.Image]
            The query to embed. A query can be either text or an image.
        """
        client = VoyageAIEmbeddingFunction._get_client()
        if self._is_multimodal_model(self.name):
            result = client.multimodal_embed(
                inputs=[[query]],
                model=self.name,
                input_type="query",
                **kwargs
            )
        else:
            result = client.embed(
                texts=[query],
                model=self.name,
                input_type="query",
                **kwargs
            )

        return [result.embeddings[0]]

    def compute_source_embeddings(
            self, inputs: Union[TEXT, IMAGES], *args, **kwargs
    ) -> List[np.array]:
        """
        Compute the embeddings for a given user query

        Parameters
        ----------
        inputs : Union[TEXT, IMAGES]
            The inputs to embed. The input can be either a str or list[str] or .
        """
        client = VoyageAIEmbeddingFunction._get_client()
        if self._is_multimodal_model(self.name):
            inputs = sanitize_multmodal_input(inputs)
            result = client.multimodal_embed(
                inputs=inputs,
                model=self.name,
                input_type="document",
                **kwargs
            )
        else:
            inputs = sanitize_text_input(inputs)
            result = client.embed(
                texts=inputs,
                model=self.name,
                input_type="document",
                **kwargs
            )

        return result.embeddings

    @staticmethod
    def _get_client():
        if VoyageAIEmbeddingFunction.client is None:
            voyageai = attempt_import_or_raise("voyageai")
            if os.environ.get("VOYAGE_API_KEY") is None:
                api_key_not_found_help("voyageai")
            VoyageAIEmbeddingFunction.client = voyageai.Client(
                os.environ["VOYAGE_API_KEY"]
            )
        return VoyageAIEmbeddingFunction.client
