# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from functools import cached_property
from typing import TYPE_CHECKING, List, Optional, Union

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register

if TYPE_CHECKING:
    import numpy as np
    import ollama


@register("ollama")
class OllamaEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses Ollama

    https://github.com/ollama/ollama/blob/main/docs/api.md#generate-embeddings
    https://ollama.com/blog/embedding-models
    """

    name: str = "nomic-embed-text"
    host: str = "http://localhost:11434"
    options: Optional[dict] = None  # type = ollama.Options
    keep_alive: Optional[Union[float, str]] = None
    ollama_client_kwargs: Optional[dict] = {}

    def ndims(self):
        return len(self.generate_embeddings(["foo"])[0])

    def _compute_embedding(self, text) -> Union["np.array", None]:
        return (
            self._ollama_client.embeddings(
                model=self.name,
                prompt=text,
                options=self.options,
                keep_alive=self.keep_alive,
            )["embedding"]
            or None
        )

    def generate_embeddings(
        self, texts: Union[List[str], "np.ndarray"]
    ) -> list[Union["np.array", None]]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
        # TODO retry, rate limit, token limit
        embeddings = [self._compute_embedding(text) for text in texts]
        return embeddings

    @cached_property
    def _ollama_client(self) -> "ollama.Client":
        ollama = attempt_import_or_raise("ollama")
        # ToDo explore ollama.AsyncClient
        return ollama.Client(host=self.host, **self.ollama_client_kwargs)
