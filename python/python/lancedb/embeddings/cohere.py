# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from functools import cached_property
from importlib.metadata import version
from typing import ClassVar, List, Literal, Optional, Union
from urllib.parse import urlsplit

import numpy as np
from packaging.version import Version
from pydantic import Field, model_validator

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import (
    TEXT,
    _get_http_status,
    api_key_not_found_help,
    retry_with_exponential_backoff,
)


_COHERE_V2_MIN_VERSION = "5.15.0"
_COHERE_V2_MAX_TEXTS = 96


def _is_redirect_error(error: Exception) -> bool:
    status = _get_http_status(error)
    return status is not None and 300 <= status < 400


@register("cohere")
class CohereEmbeddingFunction(TextEmbeddingFunction):
    """
    An embedding function for Cohere text models, including Cohere Embed v4
    deployed on Azure AI Foundry through the native Cohere Embed API v2.

    https://docs.cohere.com/v2/reference/embed

    Parameters
    ----------
    name : str, default "embed-multilingual-v3.0"
        The model name. For a custom ``base_url``, supply the deployed model name
        explicitly. The default Cohere API supports these models:

            * embed-english-v3.0
            * embed-multilingual-v3.0
            * embed-english-light-v3.0
            * embed-multilingual-light-v3.0

        The v2.0 models (embed-english-v2.0, embed-english-light-v2.0 and
        embed-multilingual-v2.0) were retired by Cohere on April 4, 2026.
        The previous default produced 768-dimensional vectors, while the new
        default produces 1024-dimensional vectors. Existing tables containing
        vectors from a v2.0 model must be re-embedded with an active model into
        a table whose vector column has the matching dimension.

    source_input_type : str, default "search_document"
        The input type for the source column in the database

    query_input_type : str, default "search_query"
        The input type for the query column in the database.

    base_url : str, optional
        Native Cohere API v2 base URL. For Foundry, use
        ``https://<resource>.services.ai.azure.com/providers/cohere``. The Cohere
        SDK appends ``/v2/embed``. This path requires ``cohere>=5.15.0`` and uses
        ``search_document`` and ``search_query`` for text retrieval.

    output_dimension : int, optional
        Required with ``base_url``. Choose an output dimension supported by the
        deployed model. This also sets the vector column dimension.

    api_key : str, optional
        Required with ``base_url``. Use a registry variable reference such as
        ``$var:foundry-cohere-key`` and set its value with ``registry.set_var``.
        To restore a table in a new process, set the variable again and trust
        the endpoint with
        [allow_endpoint][lancedb.embeddings.EmbeddingFunctionRegistry.allow_endpoint].

    Notes
    -----
    Cohere supports following input types:

    | Input Type               | Description                          |
    |-------------------------|---------------------------------------|
    | "`search_document`"     | Used for embeddings stored in a vector|
    |                         | database for search use-cases.        |
    | "`search_query`"        | Used for embeddings of search queries |
    |                         | run against a vector DB               |
    | "`semantic_similarity`" | Specifies the given text will be used |
    |                         | for Semantic Textual Similarity (STS) |
    | "`classification`"      | Used for embeddings passed through a  |
    |                         | text classifier.                      |
    | "`clustering`"          | Used for the embeddings run through a |
    |                         | clustering algorithm                  |

    Examples
    --------
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import EmbeddingFunctionRegistry

    cohere = EmbeddingFunctionRegistry
        .get_instance()
        .get("cohere")
        .create(name="embed-multilingual-v3.0")

    class TextModel(LanceModel):
        text: str = cohere.SourceField()
        vector: Vector(cohere.ndims()) =  cohere.VectorField()

    data = [ { "text": "hello world" },
            { "text": "goodbye world" }]

    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)

    """

    name: str = "embed-multilingual-v3.0"
    source_input_type: str = "search_document"
    query_input_type: str = "search_query"
    client: ClassVar = None
    base_url: Optional[str] = None
    output_dimension: Optional[int] = Field(default=None, gt=0)
    api_key: Optional[str] = Field(default=None, repr=False)

    @classmethod
    def create(cls, **kwargs):
        api_key = kwargs.get("api_key")
        if (
            isinstance(api_key, str)
            and api_key.startswith("$var:")
            and ":" in api_key[len("$var:") :]
        ):
            raise ValueError(
                "Cohere api_key must use a registry variable without a default"
            )
        return super().create(**kwargs)

    @model_validator(mode="after")
    def _validate_endpoint_config(self):
        if self.base_url is None:
            if self.output_dimension is not None or self.api_key is not None:
                raise ValueError("Cohere output_dimension and api_key require base_url")
            return self

        url = urlsplit(self.base_url)
        if (
            url.scheme not in ("http", "https")
            or not url.hostname
            or url.username is not None
            or url.query
            or url.fragment
        ):
            raise ValueError(
                "Cohere base_url must be an HTTP(S) URL without credentials, "
                "a query string, or a fragment"
            )
        if "name" not in self.model_fields_set or not self.name.strip():
            raise ValueError(
                "Set name to the deployed Cohere model when using base_url"
            )
        if self.output_dimension is None or not self.api_key:
            raise ValueError("Cohere base_url requires output_dimension and api_key")
        if (
            self.source_input_type != "search_document"
            or self.query_input_type != "search_query"
        ):
            raise ValueError(
                "Cohere base_url requires search_document for sources "
                "and search_query for queries"
            )
        return self

    @staticmethod
    def sensitive_keys() -> List[str]:
        return ["api_key"]

    def endpoints(self) -> List[str]:
        return [] if self.base_url is None else [self.base_url]

    def ndims(self):
        if self.output_dimension is not None:
            return self.output_dimension
        # TODO: fix hardcoding
        if self.name in [
            "embed-english-v3.0",
            "embed-multilingual-v3.0",
            "embed-english-light-v2.0",
        ]:
            return 1024
        elif self.name in ["embed-english-light-v3.0", "embed-multilingual-light-v3.0"]:
            return 384
        elif self.name == "embed-english-v2.0":
            return 4096
        elif self.name == "embed-multilingual-v2.0":
            return 768
        else:
            raise ValueError(f"Model {self.name} not supported")

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        if self.base_url is not None:
            return self._embed_v2(query, "search_query")
        return self.compute_source_embeddings(query, input_type=self.query_input_type)

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        input_type = (
            kwargs.get("input_type") or self.source_input_type
        )  # assume source input type if not passed by `compute_query_embeddings`
        return self.generate_embeddings(texts, input_type=input_type)

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray], *args, **kwargs
    ) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
        if self.base_url is not None:
            return self._embed_v2(texts, "search_document")
        self._init_client()
        rs = CohereEmbeddingFunction.client.embed(
            texts=texts, model=self.name, **kwargs
        )

        return [emb for emb in rs.embeddings]

    def compute_source_embeddings_with_retry(self, texts: TEXT, *args, **kwargs):
        if self.base_url is None:
            return super().compute_source_embeddings_with_retry(texts, *args, **kwargs)
        return self._embed_v2(texts, "search_document", max_retries=self.max_retries)

    def compute_query_embeddings_with_retry(self, query: str, *args, **kwargs):
        if self.base_url is None:
            return super().compute_query_embeddings_with_retry(query, *args, **kwargs)
        return self._embed_v2(query, "search_query", max_retries=self.max_retries)

    def _embed_v2(
        self,
        texts: TEXT,
        input_type: Literal["search_document", "search_query"],
        *,
        max_retries: int = 0,
    ) -> List[List[float]]:
        texts = self.sanitize_input(texts)
        if any(not isinstance(text, str) for text in texts):
            raise ValueError("Cohere API v2 text retrieval requires string inputs")
        if len(texts) == 0:
            return []

        # Retry each HTTP request without resending earlier successful batches.
        # The client refuses redirects, so repeating a redirected request cannot help.
        request = self._v2_client.embed
        if max_retries > 0:
            request = retry_with_exponential_backoff(
                request, max_retries=max_retries, do_not_retry=_is_redirect_error
            )
        embeddings = []
        for start in range(0, len(texts), _COHERE_V2_MAX_TEXTS):
            batch = list(texts[start : start + _COHERE_V2_MAX_TEXTS])
            response = request(
                texts=batch,
                model=self.name,
                input_type=input_type,
                output_dimension=self.ndims(),
                embedding_types=["float"],
                request_options={"max_retries": 0},
            )
            vectors = response.embeddings.float
            if vectors is None or len(vectors) != len(batch):
                raise ValueError(
                    "Cohere API v2 returned an unexpected number of vectors"
                )
            if any(len(vector) != self.ndims() for vector in vectors):
                raise ValueError(
                    f"Cohere API v2 returned a vector with an unexpected dimension "
                    f"(expected {self.ndims()})"
                )
            embeddings.extend(vectors)
        return embeddings

    @cached_property
    def _v2_client(self):
        cohere = attempt_import_or_raise(
            "cohere", mitigation=f"cohere>={_COHERE_V2_MIN_VERSION}"
        )
        if Version(version("cohere")) < Version(_COHERE_V2_MIN_VERSION):
            raise ImportError(
                f"Cohere base_url requires cohere>={_COHERE_V2_MIN_VERSION}"
            )
        httpx = attempt_import_or_raise("httpx")
        # Older SDK versions override the HTTPX client's timeout on each request.
        timeout = 300
        # The registry trusts only this endpoint. The SDK follows redirects by
        # default, including redirects that can resend the document or query.
        return cohere.ClientV2(
            base_url=self.base_url,
            api_key=self.api_key,
            timeout=timeout,
            httpx_client=httpx.Client(follow_redirects=False, timeout=timeout),
        )

    def _init_client(self):
        cohere = attempt_import_or_raise("cohere")
        if CohereEmbeddingFunction.client is None:
            if os.environ.get("COHERE_API_KEY") is None:
                api_key_not_found_help("cohere")
            CohereEmbeddingFunction.client = cohere.Client(os.environ["COHERE_API_KEY"])
