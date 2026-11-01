# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
from functools import cached_property
from typing import Any, List, Literal, Optional, Union

import numpy as np
from pydantic import ConfigDict

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import TEXT

# The Generative AI embed-text API accepts at most 96 inputs per request:
# https://docs.oracle.com/en-us/iaas/api/#/en/generative-ai-inference/latest/EmbedTextResult/EmbedText
EMBEDDING_BATCH_SIZE = 96

# A ``name`` that is a dedicated AI cluster endpoint OCID is routed with
# DedicatedServingMode instead of the on-demand model catalog.
_DEDICATED_ENDPOINT_PREFIX = "ocid1.generativeaiendpoint"

_SERVICE_ERROR_HELP = """
    Common causes:
      - Expired session token (auth_type="SECURITY_TOKEN"): run
        `oci session authenticate --profile-name <profile>`.
      - Missing IAM policy: the caller needs
        `allow group <group> to use generative-ai-family in compartment <name>`.
      - Model not offered in the region behind `region` / `service_endpoint`, or
        a wrong `name` / `compartment_id`. List models with
        `oci generative-ai model-collection list-models --compartment-id <ocid>`.
"""


class OCIGenAIServiceError(ValueError):
    """Raised when the Generative AI service rejects an ``embed_text`` request.

    ``status_code`` mirrors the HTTP status so LanceDB's retry helper treats
    401/403 responses as permanent instead of retrying them.
    """

    def __init__(self, message: str, status_code: Optional[int] = None):
        super().__init__(message)
        self.status_code = status_code


@register("oci-genai")
class OCIGenAIEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the Oracle Cloud Infrastructure (OCI)
    Generative AI service.

    https://docs.oracle.com/en-us/iaas/Content/generative-ai/embed-models.htm

    Requires the OCI Python SDK (``pip install oci``). No credentials are stored
    on this object: requests are signed by the SDK using an API-key or
    session-token profile from the OCI config file, or the instance / resource
    principal of the host, so there are no sensitive keys to protect with
    ``$var:``.

    Cohere embedding models are asymmetric: the source column is embedded with
    ``input_type="SEARCH_DOCUMENT"`` and queries with ``"SEARCH_QUERY"``.
    Inputs are sent in batches of at most 96 texts, the service limit.

    Parameters
    ----------
    name : str, default "cohere.embed-v4.0"
        Model ID of an on-demand embedding model: ``cohere.embed-v4.0``
        (1536 dims), ``cohere.embed-english-v3.0`` /
        ``cohere.embed-multilingual-v3.0`` (1024 dims) or their ``-light-v3.0``
        variants (384 dims). A dedicated AI cluster endpoint OCID
        (``ocid1.generativeaiendpoint...``) is also accepted.
    compartment_id : str, optional
        OCID of the compartment the requests are authorized in and billed to.
        The service requires it; when not passed, the ``OCI_COMPARTMENT_ID``
        environment variable is used.
    region : str, default "us-chicago-1"
        OCI region used to derive the inference endpoint when
        ``service_endpoint`` is not set.
    service_endpoint : str, optional
        Full inference endpoint, e.g.
        ``https://inference.generativeai.us-chicago-1.oci.oraclecloud.com``.
        Takes precedence over ``region``; needed for realms with a different
        endpoint template.
    auth_type : str, default "API_KEY"
        One of ``"API_KEY"``, ``"SECURITY_TOKEN"``, ``"INSTANCE_PRINCIPAL"`` or
        ``"RESOURCE_PRINCIPAL"``. The first two read profile ``auth_profile``
        from the OCI config file ``auth_file_location``; ``SECURITY_TOKEN``
        additionally uses the profile's ``security_token_file`` written by
        ``oci session authenticate``. The principal types use the identity of
        the host (Compute instance, OKE workload, Functions, ...) and ignore
        the config file.
    auth_profile : str, default "DEFAULT"
        Profile name in the OCI config file.
    auth_file_location : str, default "~/.oci/config"
        Path of the OCI config file.
    truncate : str, default "END"
        How inputs longer than the model context are handled: ``"NONE"``
        (fail), ``"START"`` or ``"END"`` (drop tokens from that side).
    source_input_type : str, default "SEARCH_DOCUMENT"
        ``input_type`` sent when embedding the source column.
    query_input_type : str, default "SEARCH_QUERY"
        ``input_type`` sent when embedding queries.
    output_dimensions : int, optional
        Embedding size for models that support it (``cohere.embed-v4.0``:
        256, 512, 1024 or 1536). When set, ``ndims()`` returns it directly;
        otherwise the size is discovered with one probe request and cached.

    Examples
    --------
    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    model = get_registry().get("oci-genai").create(
        name="cohere.embed-v4.0",
        compartment_id="ocid1.compartment.oc1..<your-compartment>",
        auth_profile="DEFAULT",
    )

    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("docs", schema=TextModel, mode="overwrite")
    tbl.add([{"text": "hello world"}, {"text": "goodbye world"}])
    rs = tbl.search("hello").limit(1).to_pandas()
    """

    name: str = "cohere.embed-v4.0"
    compartment_id: Optional[str] = None
    region: str = "us-chicago-1"
    service_endpoint: Optional[str] = None
    auth_type: Literal[
        "API_KEY", "SECURITY_TOKEN", "INSTANCE_PRINCIPAL", "RESOURCE_PRINCIPAL"
    ] = "API_KEY"
    auth_profile: str = "DEFAULT"
    auth_file_location: str = "~/.oci/config"
    truncate: Literal["NONE", "START", "END"] = "END"
    source_input_type: str = "SEARCH_DOCUMENT"
    query_input_type: str = "SEARCH_QUERY"
    output_dimensions: Optional[int] = None

    model_config = ConfigDict(ignored_types=(cached_property,))

    @staticmethod
    def model_names() -> List[str]:
        """On-demand text embedding models offered by the service."""
        return [
            "cohere.embed-v4.0",
            "cohere.embed-english-v3.0",
            "cohere.embed-multilingual-v3.0",
            "cohere.embed-english-light-v3.0",
            "cohere.embed-multilingual-light-v3.0",
        ]

    def ndims(self) -> int:
        return self._resolved_ndims

    @cached_property
    def _resolved_ndims(self) -> int:
        if self.output_dimensions is not None:
            return self.output_dimensions
        # embed-v4.0 is variable-width and the catalog changes, so probe the
        # service once instead of hardcoding a model -> dimensions table.
        return len(self._embed_batch(["lancedb"], self.query_input_type)[0])

    def compute_query_embeddings(
        self, query: str, *args, **kwargs
    ) -> List[Union[List[float], None]]:
        return self.compute_source_embeddings(query, input_type=self.query_input_type)

    def compute_source_embeddings(
        self, texts: TEXT, *args, **kwargs
    ) -> List[Union[List[float], None]]:
        texts = self.sanitize_input(texts)
        # assume the source input type unless `compute_query_embeddings` set one
        kwargs["input_type"] = kwargs.get("input_type") or self.source_input_type
        return self.generate_embeddings(texts, **kwargs)

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray], *args, **kwargs
    ) -> List[Union[List[float], None]]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed

        Returns
        -------
        list[list[float] | None]
            One embedding per input, in order. Empty inputs are not sent to
            the service (it rejects them) and come back as ``None``.
        """
        input_type = kwargs.get("input_type") or self.source_input_type
        texts = list(texts)
        valid = [(idx, text) for idx, text in enumerate(texts) if text]
        results: List[Union[List[float], None]] = [None] * len(texts)
        for start in range(0, len(valid), EMBEDDING_BATCH_SIZE):
            batch = valid[start : start + EMBEDDING_BATCH_SIZE]
            embeddings = self._embed_batch([text for _, text in batch], input_type)
            for (idx, _), embedding in zip(batch, embeddings):
                results[idx] = embedding
        return results

    def _embed_batch(self, texts: List[str], input_type: str) -> List[List[float]]:
        """Send one embed_text request (at most EMBEDDING_BATCH_SIZE inputs)."""
        oci = attempt_import_or_raise("oci")
        models = oci.generative_ai_inference.models
        if self.name.startswith(_DEDICATED_ENDPOINT_PREFIX):
            serving_mode = models.DedicatedServingMode(endpoint_id=self.name)
        else:
            serving_mode = models.OnDemandServingMode(model_id=self.name)
        details = {
            "inputs": texts,
            "serving_mode": serving_mode,
            "compartment_id": self._resolve_compartment_id(),
            "truncate": self.truncate,
            "input_type": input_type,
        }
        if self.output_dimensions is not None:
            details["output_dimensions"] = self.output_dimensions
        try:
            response = self._client.embed_text(models.EmbedTextDetails(**details))
        except oci.exceptions.ServiceError as e:
            raise OCIGenAIServiceError(
                f"OCI Generative AI embed_text failed with HTTP {e.status} "
                f"({e.code}): {e.message}\n{_SERVICE_ERROR_HELP}",
                status_code=e.status,
            ) from e
        return response.data.embeddings

    def _resolve_compartment_id(self) -> str:
        compartment_id = self.compartment_id or os.environ.get("OCI_COMPARTMENT_ID")
        if not compartment_id:
            raise ValueError(
                "compartment_id is required by the OCI Generative AI service. "
                "Pass `compartment_id=` to OCIGenAIEmbeddings or set the "
                "OCI_COMPARTMENT_ID environment variable."
            )
        return compartment_id

    def _resolve_service_endpoint(self) -> str:
        if self.service_endpoint:
            return self.service_endpoint
        return f"https://inference.generativeai.{self.region}.oci.oraclecloud.com"

    def _build_auth(self, oci) -> tuple[dict, Any]:
        """Return the ``(config, signer)`` pair for the configured ``auth_type``."""
        if self.auth_type in ("API_KEY", "SECURITY_TOKEN"):
            config = oci.config.from_file(
                file_location=self.auth_file_location, profile_name=self.auth_profile
            )
            token_file = config.get("security_token_file")
            if self.auth_type == "API_KEY":
                if token_file:
                    raise ValueError(
                        f"Profile '{self.auth_profile}' in {self.auth_file_location} "
                        "is a session-token profile; use auth_type='SECURITY_TOKEN'."
                    )
                return config, None
            if not token_file:
                raise ValueError(
                    f"Profile '{self.auth_profile}' in {self.auth_file_location} "
                    "has no security_token_file; run `oci session authenticate` "
                    "or use auth_type='API_KEY'."
                )
            with open(os.path.expanduser(token_file)) as f:
                token = f.read().strip()
            private_key = oci.signer.load_private_key_from_file(
                os.path.expanduser(config["key_file"]), config.get("pass_phrase")
            )
            return config, oci.auth.signers.SecurityTokenSigner(token, private_key)
        if self.auth_type == "INSTANCE_PRINCIPAL":
            return {}, oci.auth.signers.InstancePrincipalsSecurityTokenSigner()
        if self.auth_type == "RESOURCE_PRINCIPAL":
            return {}, oci.auth.signers.get_resource_principals_signer()
        raise ValueError(f"Unsupported auth_type '{self.auth_type}'")

    @cached_property
    def _client(self):
        oci = attempt_import_or_raise("oci")
        config, signer = self._build_auth(oci)
        kwargs = {
            "config": config,
            "service_endpoint": self._resolve_service_endpoint(),
            # LanceDB already retries with exponential backoff (`max_retries`),
            # so the SDK's own retry loop is disabled to avoid compounding it.
            "retry_strategy": oci.retry.NoneRetryStrategy(),
            "timeout": (10, 240),
        }
        if signer is not None:
            kwargs["signer"] = signer
        return oci.generative_ai_inference.GenerativeAiInferenceClient(**kwargs)

    def __getstate__(self) -> dict[str, Any]:
        # The SDK client holds an HTTP session and cannot be pickled.
        state = super().__getstate__()
        state["__dict__"] = {
            k: v for k, v in state["__dict__"].items() if k != "_client"
        }
        return state
