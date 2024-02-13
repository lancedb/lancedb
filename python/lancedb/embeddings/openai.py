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
    """

    name: str = "text-embedding-ada-002"
    dim: Optional[int] = None

    def ndims(self):
        return self._ndims

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
            rs = self._openai_client.embeddings.create(
                input=texts, model=self.name, dimensions=self.ndims()
            )
        return [v.embedding for v in rs.data]

    @cached_property
    def _openai_client(self):
        openai = attempt_import_or_raise("openai")

        if not os.environ.get("OPENAI_API_KEY"):
            api_key_not_found_help("openai")
        return openai.OpenAI()
