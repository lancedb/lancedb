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
from typing import ClassVar, List, Union

import numpy as np

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import api_key_not_found_help, TEXT


@register("voyageai")
class VoyageAIEmbeddingFunction(TextEmbeddingFunction):
    """
    An embedding function that uses the VoyageAI API

    https://docs.voyageai.com/docs/embeddings

    Parameters
    ----------
    name: str
        The name of the model to use. List of acceptable models:

            * voyage-3
            * voyage-3-lite
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

    def ndims(self):
        if self.name == "voyage-3-lite":
            return 512
        elif self.name == "voyage-code-2":
            return 1536
        elif self.name in [
            "voyage-3",
            "voyage-finance-2",
            "voyage-multilingual-2",
            "voyage-law-2",
        ]:
            return 1024
        else:
            raise ValueError(f"Model {self.name} not supported")

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        return self.compute_source_embeddings(query, input_type="query")

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        input_type = (
            kwargs.get("input_type") or "document"
        )  # assume source input type if not passed by `compute_query_embeddings`
        return self.generate_embeddings(texts, input_type=input_type)

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray], *args, **kwargs
    ) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        input_type: Optional[str]

        truncation: Optional[bool]
        """
        VoyageAIEmbeddingFunction._init_client()
        rs = VoyageAIEmbeddingFunction.client.embed(
            texts=texts, model=self.name, **kwargs
        )

        return [emb for emb in rs.embeddings]

    @staticmethod
    def _init_client():
        if VoyageAIEmbeddingFunction.client is None:
            voyageai = attempt_import_or_raise("voyageai")
            if os.environ.get("VOYAGE_API_KEY") is None:
                api_key_not_found_help("voyageai")
            VoyageAIEmbeddingFunction.client = voyageai.Client(
                os.environ["VOYAGE_API_KEY"]
            )
