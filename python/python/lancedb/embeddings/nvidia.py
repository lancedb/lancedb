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
from .utils import TEXT, MissingApiKeyError, retry_with_exponential_backoff


NVIDIA_BASE_URL = "https://integrate.api.nvidia.com/v1"
# Hosted model ID -> (default dimensions, minimum dimensions)
MODEL_DIMENSIONS = {
    "nvidia/nemotron-3.5-embed-1b": (2048, 128),
    "nvidia/nemotron-3.5-embed-8b": (4096, 1),
}


@register("nvidia")
class NvidiaEmbeddings(TextEmbeddingFunction):
    """Text embeddings using NVIDIA Nemotron 3.5 Embed 1B or 8B.

    Uses the optional ``openai`` package for NVIDIA-hosted and self-hosted NIM
    ``/v1/embeddings`` endpoints. Documents use ``input_type="passage"`` and
    queries use ``input_type="query"``. Null and empty strings get a null
    embedding. Image inputs are not supported.

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
        Registry variable reference, such as ``$var:nvidia-key``. Otherwise the
        hosted API uses ``NVIDIA_API_KEY``. Custom endpoints only receive an
        explicit ``api_key`` and can run without authentication.
    batch_size : int
        Maximum inputs per request (default 32). Reduce for deployment limits.
    truncate : str
        ``END`` (default) truncates overlong text; ``NONE`` rejects it.
    max_retries : int
        Inherited LanceDB retry limit, applied to each request. The OpenAI
        client's own retries are disabled.

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
        dimensions = MODEL_DIMENSIONS.get(self.name)
        if dimensions is None and self.dim is None:
            raise ValueError("Set dim explicitly for self-hosted NIM model IDs")
        if dimensions is not None and self.dim is not None:
            native_dim, minimum = dimensions
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
        return self.dim if self.dim is not None else MODEL_DIMENSIONS[self.name][0]

    def compute_query_embeddings(
        self, query: str, *args, **kwargs
    ) -> List[Optional[List[float]]]:
        return self._embed(query, input_type="query")

    def generate_embeddings(self, texts: TEXT) -> List[Optional[List[float]]]:
        return self._embed(texts, input_type="passage")

    # Retry each request rather than the whole call, so a transient failure does
    # not resend batches that already succeeded, and invalid inputs or responses
    # fail at once instead of being retried.
    def compute_query_embeddings_with_retry(
        self, query: str, *args, **kwargs
    ) -> List[Optional[List[float]]]:
        return self._embed(query, input_type="query", max_retries=self.max_retries)

    def compute_source_embeddings_with_retry(
        self, texts: TEXT, *args, **kwargs
    ) -> List[Optional[List[float]]]:
        return self._embed(texts, input_type="passage", max_retries=self.max_retries)

    def _embed(
        self,
        texts: TEXT,
        *,
        input_type: Literal["passage", "query"],
        max_retries: int = 0,
    ) -> List[Optional[List[float]]]:
        texts = list(self.sanitize_input(texts))
        if any(text is not None and not isinstance(text, str) for text in texts):
            raise ValueError("NVIDIA embeddings support text strings only")
        request = self._request
        if max_retries > 0:
            request = retry_with_exponential_backoff(request, max_retries=max_retries)
        embeddings = [None] * len(texts)
        valid = [index for index, text in enumerate(texts) if text]
        for start in range(0, len(valid), self.batch_size):
            rows = valid[start : start + self.batch_size]
            data = request([texts[row] for row in rows], input_type)
            if not isinstance(data, list) or len(data) != len(rows):
                raise ValueError("NVIDIA returned an unexpected number of embeddings")
            indices = [getattr(item, "index", None) for item in data]
            if any(type(index) is not int for index in indices) or sorted(
                indices
            ) != list(range(len(rows))):
                raise ValueError("NVIDIA returned invalid embedding indices")
            for row, item in zip(rows, sorted(data, key=lambda item: item.index)):
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
                embeddings[row] = vector
        return embeddings

    def _request(self, batch: List[str], input_type: str):
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
        return getattr(response, "data", None)

    @cached_property
    def _client(self):
        openai = attempt_import_or_raise("openai")
        hosted = not self.endpoints()
        # Never send the hosted API key from the environment to a custom endpoint
        api_key = self.api_key or (os.environ.get("NVIDIA_API_KEY") if hosted else None)
        if not api_key and hosted:
            raise MissingApiKeyError(
                "Set NVIDIA_API_KEY or provide api_key via a registry variable"
            )
        return openai.OpenAI(
            base_url=self.base_url,
            api_key=api_key or "not-required",
            max_retries=0,
        )
