from typing import List, Union

import numpy as np

from .base import TextEmbeddingFunction
from .registry import register


@register("openai")
class OpenAIEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the OpenAI API

    https://platform.openai.com/docs/guides/embeddings
    """

    name: str = "text-embedding-ada-002"

    def ndims(self):
        # TODO don't hardcode this
        return 1536

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
        openai = self.safe_import("openai")
        rs = openai.Embedding.create(input=texts, model=self.name)["data"]
        return [v["embedding"] for v in rs]
