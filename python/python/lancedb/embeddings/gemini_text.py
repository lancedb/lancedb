# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from functools import cached_property
from typing import List, Optional, Union

import numpy as np

from lancedb.pydantic import PYDANTIC_VERSION

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import TEXT, api_key_not_found_help

EMBEDDING_BATCH_SIZE = 100


@register("gemini-text")
class GeminiText(TextEmbeddingFunction):
    """
    An embedding function that uses Google's Gemini API. Requires GOOGLE_API_KEY to
    be set.

    https://ai.google.dev/gemini-api/docs/embeddings

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
    name: str, default "gemini-embedding-001"
        The name of the model to use. Supported models include:
        - "gemini-embedding-001" (768 dimensions)

        Note: The legacy "models/embedding-001" format is also supported but
        "gemini-embedding-001" is recommended.

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

    name: str = "gemini-embedding-001"
    dim: Optional[int] = None
    query_task_type: str = "retrieval_query"
    source_task_type: str = "retrieval_document"

    if PYDANTIC_VERSION.major < 2:  # Pydantic 1.x compat

        class Config:
            keep_untouched = (cached_property,)
    else:
        model_config = dict()
        model_config["ignored_types"] = (cached_property,)

    def ndims(self):
        if self.dim:
            return self.dim
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
        from google.genai import types

        task_type = kwargs.get("task_type")

        # Build content objects for embed_content
        contents = []
        for text in texts:
            if task_type == "retrieval_document":
                # Provide a title for retrieval_document task
                contents.append(
                    {"parts": [{"text": "Embedding of a document"}, {"text": text}]}
                )
            else:
                contents.append({"parts": [{"text": text}]})

        # Build config
        config_kwargs = {"output_dimensionality": self.ndims()}
        if task_type:
            config_kwargs["task_type"] = task_type.upper()  # API expects uppercase

        config = types.EmbedContentConfig(**config_kwargs) if config_kwargs else None

        # Call embed_content in groups of at most EMBEDDING_BATCH_SIZE docs at a time
        embeddings = []
        for i in range(0, len(contents), EMBEDDING_BATCH_SIZE):
            chunk = contents[i : i + EMBEDDING_BATCH_SIZE]
            response = self.client.models.embed_content(
                model=self.name,
                contents=chunk,
                config=config,
            )
            embeddings.extend([np.array(e.values) for e in response.embeddings])

        return embeddings

    @cached_property
    def client(self):
        attempt_import_or_raise("google.genai", "google-genai")

        if not os.environ.get("GOOGLE_API_KEY"):
            api_key_not_found_help("google")

        from google import genai as genai_module
        from lancedb import __version__

        return genai_module.Client(
            api_key=os.environ.get("GOOGLE_API_KEY"),
            http_options={
                "headers": {
                    "x-goog-api-client": f"lancedb/{__version__}",
                }
            },
        )
