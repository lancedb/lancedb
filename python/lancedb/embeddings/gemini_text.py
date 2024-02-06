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
from functools import cached_property
from typing import List, Union

import numpy as np

from lancedb.pydantic import PYDANTIC_VERSION

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import TEXT, api_key_not_found_help


@register("gemini-text")
class GeminiText(TextEmbeddingFunction):
    """
    An embedding function that uses the Google's Gemini API. Requires GOOGLE_API_KEY to
    be set.

    https://ai.google.dev/docs/embeddings_guide

    Supports various tasks types:
    | Task Type               | Description                                            |
    |-------------------------|--------------------------------------------------------|
    | "`retrieval_query`"     | Specifies the given text is a query in a               |
    |                         | search/retrieval setting.                              |
    | "`retrieval_document`"  | Specifies the given text is a document in a            |
    |                         | search/retrieval setting. Using this task type         |
    |                         | requires a title but is automatically provided by      |
    |                         | Embeddings API                                         |
    | "`semantic_similarity`" | Specifies the given text will be used for Semantic     |
    |                         | Textual Similarity (STS).                              |
    | "`classification`"      | Specifies that the embeddings will be used for         |
    |                         | classification.                                        |
    | "`clustering`"          | Specifies that the embeddings will be used for         |
    |                         | clustering.                                            |

    Note: The supported task types might change in the Gemini API, but as long as a
          supported task type and its argument set is provided, those will be delegated
          to the API calls.

    Parameters
    ----------
    name: str, default "models/embedding-001"
        The name of the model to use. See the Gemini documentation for a list of
        available models.

    query_task_type: str, default "retrieval_query"
        Sets the task type for the queries.
    source_task_type: str, default "retrieval_document"
        Sets the task type for ingestion.

    Examples
    --------
    import lancedb
    import pandas as pd
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import get_registry

    model = get_registry().get("gemini-text").create()

    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    rs = tbl.search("hello").limit(1).to_pandas()

    """

    name: str = "models/embedding-001"
    query_task_type: str = "retrieval_query"
    source_task_type: str = "retrieval_document"

    if PYDANTIC_VERSION < (2, 0):  # Pydantic 1.x compat

        class Config:
            keep_untouched = (cached_property,)

    def ndims(self):
        # TODO: fix hardcoding
        return 768

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        return self.compute_source_embeddings(query, task_type=self.query_task_type)

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        task_type = (
            kwargs.get("task_type") or self.source_task_type
        )  # assume source task type if not passed by `compute_query_embeddings`
        return self.generate_embeddings(texts, task_type=task_type)

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
        if (
            kwargs.get("task_type") == "retrieval_document"
        ):  # Provide a title to use existing API design
            title = "Embedding of a document"
            kwargs["title"] = title

        return [
            self.client.embed_content(model=self.name, content=text, **kwargs)[
                "embedding"
            ]
            for text in texts
        ]

    @cached_property
    def client(self):
        genai = attempt_import_or_raise("google.generativeai", "google.generativeai")

        if not os.environ.get("GOOGLE_API_KEY"):
            api_key_not_found_help("google")
        return genai
