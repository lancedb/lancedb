# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import math
import os
from functools import cached_property
from typing import List, Literal, Optional

from pydantic import Field, model_validator

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import TEXT, MissingApiKeyError


NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
MODEL_DIMENSIONS = {
    "nvidia/nemotron-3.5-embed-1b": 2048,
    "nvidia/nemotron-3.5-embed-8b": 4096,
}


@register("nvidia")
class NvidiaEmbeddings(TextEmbeddingFunction):
    """Text embeddings using NVIDIA Nemotron 3.5 Embed 1B or 8B.

    Uses the optional ``openai`` package for NVIDIA-hosted and self-hosted NIM
    ``/v1/embeddings`` endpoints. Documents use ``input_type="passage"`` and
    queries use ``input_type="query"``. Image inputs are not supported.

    Parameters
    ----------
    name : str
        Hosted model ID, or the served model ID from a NIM ``/v1/models`` response.
    dim : int, optional
        Requested output dimensions. Defaults to 2048 for the hosted 1B model
        and 4096 for the hosted 8B model. Required for other served model IDs;
        use a dimension supported by your deployment.
    base_url : str
        API base URL including ``/v1``. Defaults to NVIDIA's hosted API.
    api_key : str, optional
        Registry variable reference, such as ``$var:nvidia-key``. Otherwise uses
        ``NVIDIA_API_KEY``. Custom endpoints can run without authentication.
    batch_size : int
        Maximum inputs per request (default 32). Reduce for deployment limits.
    truncate : str
        ``END`` (default) truncates overlong text; ``NONE`` rejects it.
    max_retries : int
        Inherited LanceDB retry limit. The OpenAI client's own retries are disabled.

    Examples
    --------
    Set ``NVIDIA_API_KEY`` before using the hosted API:

    >>> from lancedb.embeddings import get_registry
    >>> from lancedb.pydantic import LanceModel, Vector
    >>> func = get_registry().get("nvidia").create()
    >>> class Document(LanceModel):
    ...     text: str = func.SourceField()
    ...     vector: Vector(func.ndims()) = func.VectorField()

    Embedding runs client-side for both local and remote tables. For a custom
    endpoint restored from table metadata in a new process, first call
    [allow_endpoint][lancedb.embeddings.EmbeddingFunctionRegistry.allow_endpoint].
    """

    name: str = "nvidia/nemotron-3.5-embed-1b"
    dim: Optional[int] = Field(default=None, gt=0)
    base_url: str = NVIDIA_BASE_URL
    api_key: Optional[str] = Field(default=None, repr=False)
    batch_size: int = Field(default=32, gt=0)
    truncate: Literal["END", "NONE"] = "END"

    @model_validator(mode="after")
    def _validate_dimensions(self):
        native_dim = MODEL_DIMENSIONS.get(self.name)
        if native_dim is None and self.dim is None:
            raise ValueError("Set dim explicitly for self-hosted NIM model IDs")
        if native_dim is not None and self.dim is not None:
            minimum = 128 if native_dim == 2048 else 1
            if not minimum <= self.dim <= native_dim:
                raise ValueError(
                    f"{self.name} supports dimensions from {minimum} to {native_dim}"
                )
        return self

    @staticmethod
    def sensitive_keys():
        return ["api_key"]

    def endpoints(self) -> List[str]:
        return [] if self.base_url.rstrip("/") == NVIDIA_BASE_URL else [self.base_url]

    def ndims(self) -> int:
        return self.dim if self.dim is not None else MODEL_DIMENSIONS[self.name]

    def compute_query_embeddings(
        self, query: str, *args, **kwargs
    ) -> List[List[float]]:
        return self._embed(query, input_type="query")

    def generate_embeddings(self, texts: TEXT) -> List[List[float]]:
        return self._embed(texts, input_type="passage")

    def _embed(
        self, texts: TEXT, *, input_type: Literal["passage", "query"]
    ) -> List[List[float]]:
        texts = list(self.sanitize_input(texts))
        if any(not isinstance(text, str) for text in texts):
            raise ValueError("NVIDIA embeddings support text strings only")
        embeddings = []
        for start in range(0, len(texts), self.batch_size):
            batch = texts[start : start + self.batch_size]
            response = self._client.embeddings.create(
                model=self.name,
                input=batch,
                dimensions=self.ndims(),
                encoding_format="float",
                extra_body={
                    "input_type": input_type,
                    "modality": "text",
                    "truncate": self.truncate,
                },
            )
            data = getattr(response, "data", None)
            if not isinstance(data, list) or len(data) != len(batch):
                raise ValueError("NVIDIA returned an unexpected number of embeddings")
            indices = [getattr(item, "index", None) for item in data]
            if any(type(index) is not int for index in indices) or sorted(
                indices
            ) != list(range(len(batch))):
                raise ValueError("NVIDIA returned invalid embedding indices")
            for item in sorted(data, key=lambda item: item.index):
                vector = getattr(item, "embedding", None)
                if (
                    not isinstance(vector, list)
                    or len(vector) != self.ndims()
                    or any(
                        type(value) not in (int, float) or not math.isfinite(value)
                        for value in vector
                    )
                ):
                    raise ValueError(
                        "NVIDIA returned an invalid embedding: expected "
                        f"{self.ndims()} finite numeric values"
                    )
                embeddings.append(vector)
        return embeddings

    @cached_property
    def _client(self):
        openai = attempt_import_or_raise("openai")
        api_key = self.api_key or os.environ.get("NVIDIA_API_KEY")
        if not api_key and not self.endpoints():
            raise MissingApiKeyError(
                "Set NVIDIA_API_KEY or provide api_key via a registry variable"
            )
        return openai.OpenAI(
            base_url=self.base_url,
            api_key=api_key or "not-required",
            max_retries=0,
        )
