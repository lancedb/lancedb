# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from functools import cached_property
from typing import TYPE_CHECKING, Any, List, Optional, Sequence, Union

import numpy as np

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register

if TYPE_CHECKING:
    import ollama


DEFAULT_OLLAMA_HOST = "http://localhost:11434"


@register("ollama")
class OllamaEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses Ollama

    https://github.com/ollama/ollama/blob/main/docs/api.md#generate-embeddings
    https://ollama.com/blog/embedding-models
    """

    name: str = "nomic-embed-text"
    host: str = DEFAULT_OLLAMA_HOST
    options: Optional[dict] = None  # type = ollama.Options
    keep_alive: Optional[Union[float, str]] = None
    ollama_client_kwargs: Optional[dict] = {}

    def endpoints(self) -> List[str]:
        urls = []
        if self.host.rstrip("/") != DEFAULT_OLLAMA_HOST:
            urls.append(self.host)
        # These kwargs go to the underlying httpx client, where a proxy would
        # see every request.
        for key in ("proxy", "proxies"):
            proxy = (self.ollama_client_kwargs or {}).get(key)
            if isinstance(proxy, str):
                urls.append(proxy)
            elif isinstance(proxy, dict):
                urls.extend(v for v in proxy.values() if isinstance(v, str))
        return urls

    def ndims(self) -> int:
        return len(self.generate_embeddings(["foo"])[0])

    def _compute_embedding(self, text: Sequence[str]) -> Sequence[Sequence[float]]:
        response = self._ollama_client.embed(
            model=self.name,
            input=text,
            options=self.options,
            keep_alive=self.keep_alive,
        )
        return response.embeddings

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray]
    ) -> list[Union[np.array, None]]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
        # TODO retry, rate limit, token limit
        if isinstance(texts, np.ndarray):
            texts = texts.tolist()
        embeddings = self._compute_embedding(texts)
        return list(embeddings)

    def __getstate__(self) -> dict[str, Any]:
        state = super().__getstate__()
        state["__dict__"] = {
            k: v for k, v in state["__dict__"].items() if k != "_ollama_client"
        }
        return state

    def __setstate__(self, state: dict[str, Any]) -> None:
        super().__setstate__(state)

    @cached_property
    def _ollama_client(self) -> "ollama.Client":
        ollama = attempt_import_or_raise("ollama")
        # ToDo explore ollama.AsyncClient
        return ollama.Client(host=self.host, **self.ollama_client_kwargs)
