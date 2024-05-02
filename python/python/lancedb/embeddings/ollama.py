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
from functools import cached_property
from typing import TYPE_CHECKING, List, Optional, Union

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register

if TYPE_CHECKING:
    import numpy as np


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

    def _compute_embedding(self, text):
        return self._ollama_client.embeddings(
            model=self.name,
            prompt=text,
            options=self.options,
            keep_alive=self.keep_alive,
        )["embedding"]

    def generate_embeddings(
        self, texts: Union[List[str], "np.ndarray"]
    ) -> List["np.array"]:
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
    def _ollama_client(self):
        ollama = attempt_import_or_raise("ollama")
        # ToDo explore ollama.AsyncClient
        return ollama.Client(host=self.host, **self.ollama_client_kwargs)
