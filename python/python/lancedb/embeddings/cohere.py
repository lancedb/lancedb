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


@register("cohere")
class CohereEmbeddingFunction(TextEmbeddingFunction):
    """
    An embedding function that uses the Cohere API

    https://docs.cohere.com/docs/multilingual-language-models

    Parameters
    ----------
    name: str, default "embed-multilingual-v2.0"
        The name of the model to use. List of acceptable models:

            * embed-english-v3.0
            * embed-multilingual-v3.0
            * embed-english-light-v3.0
            * embed-multilingual-light-v3.0
            * embed-english-v2.0
            * embed-english-light-v2.0
            * embed-multilingual-v2.0

    source_input_type: str, default "search_document"
        The input type for the source column in the database

    query_input_type: str, default "search_query"
        The input type for the query column in the database

    Cohere supports following input types:

    | Input Type               | Description                          |
    |-------------------------|---------------------------------------|
    | "`search_document`"     | Used for embeddings stored in a vector|
    |                         | database for search use-cases.        |
    | "`search_query`"        | Used for embeddings of search queries |
    |                         | run against a vector DB               |
    | "`semantic_similarity`" | Specifies the given text will be used |
    |                         | for Semantic Textual Similarity (STS) |
    | "`classification`"      | Used for embeddings passed through a  |
    |                         | text classifier.                      |
    | "`clustering`"          | Used for the embeddings run through a |
    |                         | clustering algorithm                  |

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
    source_input_type: str = "search_document"
    query_input_type: str = "search_query"
    client: ClassVar = None

    def ndims(self):
        # TODO: fix hardcoding
        if self.name in [
            "embed-english-v3.0",
            "embed-multilingual-v3.0",
            "embed-english-light-v2.0",
        ]:
            return 1024
        elif self.name in ["embed-english-light-v3.0", "embed-multilingual-light-v3.0"]:
            return 384
        elif self.name == "embed-english-v2.0":
            return 4096
        elif self.name == "embed-multilingual-v2.0":
            return 768
        else:
            raise ValueError(f"Model {self.name} not supported")

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        return self.compute_source_embeddings(query, input_type=self.query_input_type)

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        input_type = (
            kwargs.get("input_type") or self.source_input_type
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
        """
        self._init_client()
        rs = CohereEmbeddingFunction.client.embed(
            texts=texts, model=self.name, **kwargs
        )

        return [emb for emb in rs.embeddings]

    def _init_client(self):
        cohere = attempt_import_or_raise("cohere")
        if CohereEmbeddingFunction.client is None:
            if os.environ.get("COHERE_API_KEY") is None:
                api_key_not_found_help("cohere")
            CohereEmbeddingFunction.client = cohere.Client(os.environ["COHERE_API_KEY"])
