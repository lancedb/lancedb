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
from typing import List, Union

import numpy as np

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import weak_lru


@register("sentence-transformers")
class SentenceTransformerEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the sentence-transformers library

    https://huggingface.co/sentence-transformers

    Parameters
    ----------
    name: str, default "all-MiniLM-L6-v2"
        The name of the model to use.
    device: str, default "cpu"
        The device to use for the model
    normalize: bool, default True
        Whether to normalize the embeddings
    trust_remote_code: bool, default True
        Whether to trust the remote code
    """

    name: str = "all-MiniLM-L6-v2"
    device: str = "cpu"
    normalize: bool = True
    trust_remote_code: bool = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ndims = None

    @property
    def embedding_model(self):
        """
        Get the sentence-transformers embedding model specified by the
        name, device, and trust_remote_code. This is cached so that the
        model is only loaded once per process.
        """
        return self.get_embedding_model()

    def ndims(self):
        if self._ndims is None:
            self._ndims = len(self.generate_embeddings("foo")[0])
        return self._ndims

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
        return self.embedding_model.encode(
            list(texts),
            convert_to_numpy=True,
            normalize_embeddings=self.normalize,
        ).tolist()

    @weak_lru(maxsize=1)
    def get_embedding_model(self):
        """
        Get the sentence-transformers embedding model specified by the
        name, device, and trust_remote_code. This is cached so that the
        model is only loaded once per process.

        TODO: use lru_cache instead with a reasonable/configurable maxsize
        """
        sentence_transformers = attempt_import_or_raise(
            "sentence_transformers", "sentence-transformers"
        )
        return sentence_transformers.SentenceTransformer(
            self.name, device=self.device, trust_remote_code=self.trust_remote_code
        )
