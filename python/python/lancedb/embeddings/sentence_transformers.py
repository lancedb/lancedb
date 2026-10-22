# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import List, Optional, Tuple, Union

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
    name : str, default "all-MiniLM-L6-v2"
        The name of the model to use.
    device : str, default "cpu"
        The device to use for the model
    normalize : bool, default True
        Whether to normalize the embeddings
    trust_remote_code : bool, default False
        Whether to run code from the model's repository. Only enable this for
        repositories you trust. When restored from table metadata, this is only
        enabled for models allowed with
        [allow_remote_code][lancedb.embeddings.EmbeddingFunctionRegistry.allow_remote_code].
    revision : Optional[str], default None
        The model revision to load, such as a commit hash.
    """

    name: str = "all-MiniLM-L6-v2"
    device: str = "cpu"
    normalize: bool = True
    trust_remote_code: bool = False
    revision: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ndims = None

    def remote_code_model(self) -> Optional[Tuple[str, Optional[str]]]:
        return (self.name, self.revision)

    @classmethod
    def _legacy_trust_remote_code_default(cls) -> bool:
        return True

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
            self.name,
            device=self.device,
            trust_remote_code=self.trust_remote_code,
            revision=self.revision,
        )
