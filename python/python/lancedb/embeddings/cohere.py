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
from .utils import api_key_not_found_help


@register("cohere")
class CohereEmbeddingFunction(TextEmbeddingFunction):
    """
    An embedding function that uses the Cohere API

    https://docs.cohere.com/docs/multilingual-language-models

    Parameters
    ----------
    name: str, default "embed-multilingual-v2.0"
        The name of the model to use. See the Cohere documentation for
        a list of available models.

    Examples
    --------
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import EmbeddingFunctionRegistry

    cohere = EmbeddingFunctionRegistry
        .get_instance()
        .get("cohere")
        .create(name="embed-multilingual-v2.0")

    class TextModel(LanceModel):
        text: str = cohere.SourceField()
        vector: Vector(cohere.ndims()) =  cohere.VectorField()

    data = [ { "text": "hello world" },
            { "text": "goodbye world" }]

    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)

    """

    name: str = "embed-multilingual-v2.0"
    client: ClassVar = None

    def ndims(self):
        # TODO: fix hardcoding
        return 768

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray]
    ) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
        # TODO retry, rate limit, token limit
        self._init_client()
        rs = CohereEmbeddingFunction.client.embed(texts=texts, model=self.name)

        return [emb for emb in rs.embeddings]

    def _init_client(self):
        cohere = attempt_import_or_raise("cohere")
        if CohereEmbeddingFunction.client is None:
            if os.environ.get("COHERE_API_KEY") is None:
                api_key_not_found_help("cohere")
            CohereEmbeddingFunction.client = cohere.Client(os.environ["COHERE_API_KEY"])
