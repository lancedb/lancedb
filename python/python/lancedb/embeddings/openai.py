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
from typing import List, Optional, Union

import numpy as np

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import api_key_not_found_help


@register("openai")
class OpenAIEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the OpenAI API

    https://platform.openai.com/docs/guides/embeddings

    This can also be used for open source models that
    are compatible with the OpenAI API.

    Notes
    -----
    If you're running an Ollama server locally,
    you can just override the `base_url` parameter
    and provide the Ollama embedding model you want
    to use (https://ollama.com/library):

    ```python
    from lancedb.embeddings import get_registry
    openai = get_registry().get("openai")
    embedding_function = openai.create(
        name="<ollama-embedding-model-name>",
        base_url="http://localhost:11434",
        )
    ```

    """

    name: str = "text-embedding-ada-002"
    dim: Optional[int] = None
    base_url: Optional[str] = None
    default_headers: Optional[dict] = None
    organization: Optional[str] = None
    api_key: Optional[str] = None

    def ndims(self):
        return self._ndims

    @staticmethod
    def model_names():
        return [
            "text-embedding-ada-002",
            "text-embedding-3-large",
            "text-embedding-3-small",
        ]

    @cached_property
    def _ndims(self):
        if self.name == "text-embedding-ada-002":
            return 1536
        elif self.name == "text-embedding-3-large":
            return self.dim or 3072
        elif self.name == "text-embedding-3-small":
            return self.dim or 1536
        else:
            raise ValueError(f"Unknown model name {self.name}")

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
        if self.name == "text-embedding-ada-002":
            rs = self._openai_client.embeddings.create(input=texts, model=self.name)
        else:
            kwargs = {
                "input": texts,
                "model": self.name,
            }
            if self.dim:
                kwargs["dimensions"] = self.dim
            rs = self._openai_client.embeddings.create(**kwargs)
        return [v.embedding for v in rs.data]

    @cached_property
    def _openai_client(self):
        openai = attempt_import_or_raise("openai")

        if self.base_url is not None:
            if self.api_key is None and not os.environ.get("OPENAI_API_KEY"):
                api_key_not_found_help("openai")

        kwargs = {}
        if self.base_url:
            kwargs["base_url"] = self.base_url
        if self.default_headers:
            kwargs["default_headers"] = self.default_headers
        if self.organization:
            kwargs["organization"] = self.organization
        if self.api_key:
            kwargs["api_key"] = self
        return openai.OpenAI(**kwargs)
