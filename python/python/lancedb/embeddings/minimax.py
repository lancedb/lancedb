# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from typing import ClassVar, List, Optional, Union

import numpy as np

from .base import TextEmbeddingFunction
from .registry import register
from .utils import api_key_not_found_help

API_URL = "https://api.minimax.io/v1/embeddings"


@register("minimax")
class MiniMaxEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the MiniMax API

    https://platform.minimaxi.com/document/Embeddings

    Parameters
    ----------
    name: str, default "embo-01"
        The name of the embedding model to use.

    api_key: str, default None
        The API key for MiniMax. If not provided, the MINIMAX_API_KEY
        environment variable will be used.

    Examples
    --------
    >>> import lancedb
    >>> from lancedb.pydantic import LanceModel, Vector
    >>> from lancedb.embeddings import get_registry
    >>> minimax = get_registry().get("minimax").create()
    >>> class TextModel(LanceModel):
    ...     text: str = minimax.SourceField()
    ...     vector: Vector(minimax.ndims()) = minimax.VectorField()
    >>> data = [{"text": "hello world"}, {"text": "goodbye world"}]
    >>> db = lancedb.connect("~/.lancedb")
    >>> tbl = db.create_table("minimax_test", schema=TextModel, mode="overwrite")
    >>> tbl.add(data)
    """

    name: str = "embo-01"
    api_key: Optional[str] = None
    _session: ClassVar = None

    def ndims(self):
        if self.name == "embo-01":
            return 1536
        else:
            raise ValueError(f"Unknown MiniMax embedding model: {self.name}")

    @staticmethod
    def sensitive_keys() -> List[str]:
        return ["api_key"]

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(query)
        return self._generate_embeddings(texts, embedding_type="query")

    def compute_source_embeddings(self, texts, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        return self._generate_embeddings(texts, embedding_type="db")

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray], *args, **kwargs
    ) -> List[np.array]:
        return self._generate_embeddings(texts, embedding_type="db")

    def _generate_embeddings(
        self, texts: List[str], embedding_type: str = "db"
    ) -> List[np.array]:
        """
        Get embeddings from the MiniMax API.

        Parameters
        ----------
        texts : List[str]
            The texts to embed.
        embedding_type : str
            The type of embedding: "db" for storage, "query" for search queries.
        """
        self._init_client()

        valid_texts = []
        valid_indices = []
        for idx, text in enumerate(texts):
            if text:
                valid_texts.append(text)
                valid_indices.append(idx)

        if not valid_texts:
            return [None] * len(texts)

        resp = MiniMaxEmbeddings._session.post(
            API_URL,
            json={
                "model": self.name,
                "texts": valid_texts,
                "type": embedding_type,
            },
        ).json()

        base_resp = resp.get("base_resp", {})
        status_code = base_resp.get("status_code", 0)
        if status_code != 0:
            raise RuntimeError(
                f"MiniMax API error (code {status_code}): "
                f"{base_resp.get('status_msg', 'unknown error')}"
            )

        vectors = resp.get("vectors")
        if vectors is None:
            raise RuntimeError("MiniMax API returned no vectors")

        valid_embeddings = {
            idx: vec for idx, vec in zip(valid_indices, vectors)
        }
        return [valid_embeddings.get(idx, None) for idx in range(len(texts))]

    def _init_client(self):
        import requests

        if MiniMaxEmbeddings._session is None:
            if self.api_key is None and os.environ.get("MINIMAX_API_KEY") is None:
                api_key_not_found_help("minimax")
            api_key = self.api_key or os.environ.get("MINIMAX_API_KEY")
            MiniMaxEmbeddings._session = requests.Session()
            MiniMaxEmbeddings._session.headers.update(
                {
                    "Authorization": f"Bearer {api_key}",
                    "Content-Type": "application/json",
                }
            )
