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
from typing import ClassVar, TYPE_CHECKING, List, Union

import numpy as np
import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import api_key_not_found_help, IMAGES

if TYPE_CHECKING:
    import PIL


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
        ]:
            return 1024
        else:
            raise ValueError(f"Model {self.name} not supported")

    def sanitize_input(self, images: IMAGES) -> Union[List[bytes], np.ndarray]:
        """
        Sanitize the input to the embedding function.
        """
        if isinstance(images, (str, bytes)):
            images = [images]
        elif isinstance(images, pa.Array):
            images = images.to_pylist()
        elif isinstance(images, pa.ChunkedArray):
            images = images.combine_chunks().to_pylist()
        return images

    def generate_text_embeddings(self, text: str, **kwargs) -> np.ndarray:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        input_type: Optional[str]

        truncation: Optional[bool]
        """
        client = VoyageAIEmbeddingFunction._get_client()
        if self.name in self.text_embedding_models:
            rs = client.embed(texts=[text], model=self.name, **kwargs)
        elif self.name in self.multimodal_embedding_models:
            rs = client.multimodal_embed(inputs=[[text]], model=self.name, **kwargs)
        else:
            raise ValueError(
                f"Model {self.name} not supported to generate text embeddings"
            )

        return rs.embeddings[0]

    def generate_image_embedding(
        self, image: "PIL.Image.Image", **kwargs
    ) -> np.ndarray:
        rs = VoyageAIEmbeddingFunction._get_client().multimodal_embed(
            inputs=[[image]], model=self.name, **kwargs
        )
        return rs.embeddings[0]

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
        if isinstance(query, str):
            return [self.generate_text_embeddings(query, input_type="query")]
        else:
            PIL = attempt_import_or_raise("PIL", "pillow")
            if isinstance(query, PIL.Image.Image):
                return [self.generate_image_embedding(query, input_type="query")]
            else:
                raise TypeError("Only text PIL images supported as query")

    def compute_source_embeddings(
        self, images: IMAGES, *args, **kwargs
    ) -> List[np.array]:
        images = self.sanitize_input(images)
        return [
            self.generate_image_embedding(img, input_type="document") for img in images
        ]

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
