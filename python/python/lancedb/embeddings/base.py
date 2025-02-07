# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from abc import ABC, abstractmethod
from typing import List, Union

import numpy as np
import pyarrow as pa
from pydantic import BaseModel, Field, PrivateAttr

from .utils import TEXT, retry_with_exponential_backoff


class EmbeddingFunction(BaseModel, ABC):
    """
    An ABC for embedding functions.

    All concrete embedding functions must implement the following:
    1. compute_query_embeddings() which takes a query and returns a list of embeddings
    2. get_source_embeddings() which returns a list of embeddings for the source column
    For text data, the two will be the same. For multi-modal data, the source column
    might be images and the vector column might be text.
    3. ndims method which returns the number of dimensions of the vector column
    """

    __slots__ = ("__weakref__",)  # pydantic 1.x compatibility
    max_retries: int = (
        7  # Setting 0 disables retires. Maybe this should not be enabled by default,
    )
    _ndims: int = PrivateAttr()

    @classmethod
    def create(cls, **kwargs):
        """
        Create an instance of the embedding function
        """
        return cls(**kwargs)

    @abstractmethod
    def compute_query_embeddings(self, *args, **kwargs) -> list[Union[np.array, None]]:
        """
        Compute the embeddings for a given user query

        Returns
        -------
        A list of embeddings for each input. The embedding of each input can be None
        when the embedding is not valid.
        """
        pass

    @abstractmethod
    def compute_source_embeddings(self, *args, **kwargs) -> list[Union[np.array, None]]:
        """Compute the embeddings for the source column in the database

        Returns
        -------
        A list of embeddings for each input. The embedding of each input can be None
        when the embedding is not valid.
        """
        pass

    def compute_query_embeddings_with_retry(
        self, *args, **kwargs
    ) -> list[Union[np.array, None]]:
        """Compute the embeddings for a given user query with retries

        Returns
        -------
        A list of embeddings for each input. The embedding of each input can be None
        when the embedding is not valid.
        """
        return retry_with_exponential_backoff(
            self.compute_query_embeddings, max_retries=self.max_retries
        )(
            *args,
            **kwargs,
        )

    def compute_source_embeddings_with_retry(
        self, *args, **kwargs
    ) -> list[Union[np.array, None]]:
        """Compute the embeddings for the source column in the database with retries.

        Returns
        -------
        A list of embeddings for each input. The embedding of each input can be None
        when the embedding is not valid.
        """
        return retry_with_exponential_backoff(
            self.compute_source_embeddings, max_retries=self.max_retries
        )(*args, **kwargs)

    def sanitize_input(self, texts: TEXT) -> Union[List[str], np.ndarray]:
        """
        Sanitize the input to the embedding function.
        """
        if isinstance(texts, str):
            texts = [texts]
        elif isinstance(texts, pa.Array):
            texts = texts.to_pylist()
        elif isinstance(texts, pa.ChunkedArray):
            texts = texts.combine_chunks().to_pylist()
        return texts

    def safe_model_dump(self):
        from ..pydantic import PYDANTIC_VERSION

        if PYDANTIC_VERSION.major < 2:
            return {k: v for k, v in self.__dict__.items() if not k.startswith("_")}
        return self.model_dump(
            exclude={
                field_name
                for field_name in self.model_fields
                if field_name.startswith("_")
            }
        )

    @abstractmethod
    def ndims(self) -> int:
        """
        Return the dimensions of the vector column
        """
        pass

    def SourceField(self, **kwargs):
        """
        Creates a pydantic Field that can automatically annotate
        the source column for this embedding function
        """
        return Field(json_schema_extra={"source_column_for": self}, **kwargs)

    def VectorField(self, **kwargs):
        """
        Creates a pydantic Field that can automatically annotate
        the target vector column for this embedding function
        """
        return Field(json_schema_extra={"vector_column_for": self}, **kwargs)

    def __eq__(self, __value: object) -> bool:
        if not hasattr(__value, "__dict__"):
            return False
        return vars(self) == vars(__value)

    def __hash__(self) -> int:
        return hash(frozenset(vars(self).items()))


class EmbeddingFunctionConfig(BaseModel):
    """
    This model encapsulates the configuration for a embedding function
    in a lancedb table. It holds the embedding function, the source column,
    and the vector column
    """

    vector_column: str
    source_column: str
    function: EmbeddingFunction


class TextEmbeddingFunction(EmbeddingFunction):
    """
    A callable ABC for embedding functions that take text as input
    """

    def compute_query_embeddings(
        self, query: str, *args, **kwargs
    ) -> list[Union[np.array, None]]:
        return self.compute_source_embeddings(query, *args, **kwargs)

    def compute_source_embeddings(
        self, texts: TEXT, *args, **kwargs
    ) -> list[Union[np.array, None]]:
        texts = self.sanitize_input(texts)
        return self.generate_embeddings(texts)

    @abstractmethod
    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray], *args, **kwargs
    ) -> list[Union[np.array, None]]:
        """Generate the embeddings for the given texts"""
        pass
