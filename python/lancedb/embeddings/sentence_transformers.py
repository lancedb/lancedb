from typing import List, Union

import numpy as np
from cachetools import cached

from .base import TextEmbeddingFunction
from .registry import register


@register("sentence-transformers")
class SentenceTransformerEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the sentence-transformers library

    https://huggingface.co/sentence-transformers
    """

    name: str = "all-MiniLM-L6-v2"
    device: str = "cpu"
    normalize: bool = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ndims = None

    @property
    def embedding_model(self):
        """
        Get the sentence-transformers embedding model specified by the
        name and device. This is cached so that the model is only loaded
        once per process.
        """
        return self.__class__.get_embedding_model(self.name, self.device)

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

    @classmethod
    @cached(cache={})
    def get_embedding_model(cls, name, device):
        """
        Get the sentence-transformers embedding model specified by the
        name and device. This is cached so that the model is only loaded
        once per process.

        Parameters
        ----------
        name : str
            The name of the model to load
        device : str
            The device to load the model on

        TODO: use lru_cache instead with a reasonable/configurable maxsize
        """
        sentence_transformers = cls.safe_import(
            "sentence_transformers", "sentence-transformers"
        )
        return sentence_transformers.SentenceTransformer(name, device=device)
