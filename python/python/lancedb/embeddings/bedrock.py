# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import base64
import io
import json
from functools import cached_property
from pathlib import Path
from typing import TYPE_CHECKING, List, Literal, Union

import numpy as np
from pydantic import ConfigDict

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction, TextEmbeddingFunction
from .registry import register
from .utils import IMAGES, TEXT, retry_with_exponential_backoff

if TYPE_CHECKING:
    from PIL.Image import Image


class _BedrockEmbeddingFunction(EmbeddingFunction):
    name: str
    region: str = "us-east-1"

    model_config = ConfigDict(ignored_types=(cached_property,))

    def _invoke_model(self, body: str, *, max_retries: int = 0) -> dict:
        invoke = self.client.invoke_model
        if max_retries > 0:
            invoke = retry_with_exponential_backoff(invoke, max_retries=max_retries)
        response = invoke(
            body=body,
            modelId=self.name,
            accept="application/json",
            contentType="application/json",
        )
        return json.loads(response["body"].read())

    @cached_property
    def client(self):
        """The boto3 Bedrock Runtime client."""
        boto3 = attempt_import_or_raise("boto3")
        return self._runtime_client(boto3.Session())

    def _runtime_client(self, session, **credentials):
        botocore = attempt_import_or_raise("botocore")
        retry_config = botocore.config.Config(
            region_name=self.region,
            # The embedding function handles retries.
            retries={"max_attempts": 0, "mode": "standard"},
        )
        return session.client(
            service_name="bedrock-runtime",
            config=retry_config,
            region_name=self.region,
            **credentials,
        )


@register("bedrock-text")
class BedRockText(_BedrockEmbeddingFunction, TextEmbeddingFunction):
    """
    Parameters
    ----------
    name : str, default "amazon.titan-embed-text-v1"
        The model ID of the bedrock model to use. Supported models for are:
        - amazon.titan-embed-text-v1
        - cohere.embed-english-v3
        - cohere.embed-multilingual-v3
    region : str, default "us-east-1"
        Optional name of the AWS Region in which the service should be called.
    profile_name : str, default None
        Optional name of the AWS profile to use for calling the Bedrock service.
        If not specified, the default profile will be used.
    assumed_role : str, default None
        Optional ARN of an AWS IAM role to assume for calling the Bedrock service.
        If not specified, the current active credentials will be used.
    role_session_name : str, default "lancedb-embeddings"
        Optional name of the AWS IAM role session to use for calling the Bedrock
        service. If not specified, "lancedb-embeddings" name will be used.

    Examples
    --------
    import lancedb
    import pandas as pd
    from lancedb.pydantic import LanceModel, Vector

    model = get_registry().get("bedrock-text").create()

    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect("tmp_path")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)

    rs = tbl.search("hello").limit(1).to_pandas()
    """

    name: str = "amazon.titan-embed-text-v1"
    assumed_role: Union[str, None] = None
    profile_name: Union[str, None] = None
    role_session_name: str = "lancedb-embeddings"
    source_input_type: str = "search_document"
    query_input_type: str = "search_query"

    @cached_property
    def client(self):
        """The boto3 Bedrock Runtime client."""
        boto3 = attempt_import_or_raise("boto3")
        session = (
            boto3.Session(region_name=self.region, profile_name=self.profile_name)
            if self.profile_name
            else boto3.Session()
        )
        if not self.assumed_role:
            return self._runtime_client(session)

        sts = session.client("sts")
        credentials = sts.assume_role(
            RoleArn=self.assumed_role,
            RoleSessionName=self.role_session_name,
        )["Credentials"]
        return self._runtime_client(
            session,
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

    def ndims(self):
        # return len(self._generate_embedding("test"))
        # TODO: fix hardcoding
        if self.name == "amazon.titan-embed-text-v1":
            return 1536
        elif self.name in [
            "amazon.titan-embed-text-v2:0",
            "cohere.embed-english-v3",
            "cohere.embed-multilingual-v3",
        ]:
            # TODO: "amazon.titan-embed-text-v2:0" model supports dynamic ndims
            return 1024
        else:
            raise ValueError(f"Model {self.name} not supported")

    def compute_query_embeddings(
        self, query: str, *args, **kwargs
    ) -> List[List[float]]:
        return self.compute_source_embeddings(query, input_type=self.query_input_type)

    def compute_source_embeddings(
        self, texts: TEXT, *args, **kwargs
    ) -> List[List[float]]:
        texts = self.sanitize_input(texts)
        # assume source input type if not passed by `compute_query_embeddings`
        kwargs["input_type"] = kwargs.get("input_type") or self.source_input_type

        return self.generate_embeddings(texts, **kwargs)

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray], *args, **kwargs
    ) -> List[List[float]]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed

        Returns
        -------
        list[list[float]]
            The embeddings for the given texts
        """
        results = []
        for text in texts:
            response = self._generate_embedding(text, *args, **kwargs)
            results.append(response)
        return results

    def _generate_embedding(self, text: str, *args, **kwargs) -> List[float]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: str
            The texts to embed

        Returns
        -------
        list[float]
            The embeddings for the given texts
        """
        # format input body for provider
        provider = self.name.split(".")[0]
        input_body = {**kwargs}
        if provider == "cohere":
            input_body["texts"] = [text]
        else:
            # includes common provider == "amazon"
            input_body.pop("input_type", None)
            input_body["inputText"] = text
        body = json.dumps(input_body)

        try:
            response_body = self._invoke_model(body)
            if provider == "cohere":
                return response_body.get("embeddings")[0]
            else:
                # includes common provider == "amazon"
                return response_body.get("embedding")
        except Exception as e:
            help_txt = """
                boto3 client failed to invoke the bedrock API. In case of
                AWS credentials error:
                    - Please check your AWS credentials and ensure that you have access.
                    You can set up aws credentials using `aws configure` command and
                    verify by running `aws sts get-caller-identity` in your terminal.
                """
            raise ValueError(f"Error raised by boto3 client: {e}. \n {help_txt}")


@register("bedrock-image")
class BedrockImage(_BedrockEmbeddingFunction):
    """Embed images and search them by text or Pillow image using Amazon Bedrock.

    Install `boto3` and `pillow` to use this embedding function. Use a binary
    source column (`bytes`) for encoded image bytes or a string
    source column (`str`) for file paths readable by the process that adds rows.
    Pillow detects the image format from the file contents. Null source values
    produce null embeddings.

    Both models accept PNG and JPEG sources. `cohere.embed-v4:0` also accepts
    WebP and GIF. For queries, pass a text string or a Pillow image. Pillow
    queries are converted to RGB PNGs before calling Bedrock. With
    [AsyncTable.search][lancedb.table.AsyncTable.search], pass
    `query_type="vector"` for Pillow image queries.

    This function uses the boto3 default credential chain configured by your
    application, including `AWS_PROFILE` or the application's IAM role.

    Parameters
    ----------
    name : str, default "amazon.titan-embed-image-v1"
        Model ID to invoke on Amazon Bedrock. `amazon.titan-embed-image-v1`
        returns 1024 dimensions. `cohere.embed-v4:0` returns 1536 dimensions.
    region : str, default "us-east-1"
        AWS Region where the model is available.
    max_retries : int, default 7
        Maximum retries per Bedrock request. Set to `0` to disable retries.

    Examples
    --------
    ```python
    from pathlib import Path

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    from PIL import Image

    model = get_registry().get("bedrock-image").create()

    class Photo(LanceModel):
        image: bytes = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    db = lancedb.connect("photos_db")
    table = db.create_table("photos", schema=Photo)
    table.add([{"image": Path("sunset.jpg").read_bytes()}])

    matches = table.search("sunset over the ocean").limit(5).to_list()
    with Image.open("query.png") as query:
        similar = table.search(query).limit(5).to_list()
    ```
    """

    # Table metadata can select a model and region. Credentials come from the
    # application, so reject settings such as a profile name or role ARN.
    model_config = ConfigDict(extra="forbid")

    name: Literal["amazon.titan-embed-image-v1", "cohere.embed-v4:0"] = (
        "amazon.titan-embed-image-v1"
    )

    def ndims(self) -> int:
        return 1024 if self.name == "amazon.titan-embed-image-v1" else 1536

    def compute_source_embeddings(self, images: IMAGES) -> list[list[float] | None]:
        return self._embed_sources(images, max_retries=0)

    def compute_query_embeddings(self, query: "str | Image") -> list[list[float]]:
        return self._embed_query(query, max_retries=0)

    # A transient failure can retry its own request without replaying earlier
    # images or repeating local image encoding and response parsing.
    def compute_source_embeddings_with_retry(
        self, images: IMAGES, *args, **kwargs
    ) -> list[list[float] | None]:
        return self._embed_sources(images, max_retries=self.max_retries)

    def compute_query_embeddings_with_retry(
        self, query: "str | Image", *args, **kwargs
    ) -> list[list[float]]:
        return self._embed_query(query, max_retries=self.max_retries)

    def _embed_sources(
        self, images: IMAGES, *, max_retries: int
    ) -> list[list[float] | None]:
        values = [images] if isinstance(images, bytes) else self.sanitize_input(images)
        embeddings = []
        for image in values:
            if image is None:
                embeddings.append(None)
                continue
            image_bytes = Path(image).read_bytes() if isinstance(image, str) else image
            image_module = attempt_import_or_raise("PIL.Image", "pillow")
            with image_module.open(io.BytesIO(image_bytes)) as decoded:
                payload = self._image_payload(image_bytes, decoded.format)
            embeddings.append(
                self._embed(
                    payload, input_type="search_document", max_retries=max_retries
                )
            )
        return embeddings

    def _embed_query(
        self, query: "str | Image", *, max_retries: int
    ) -> list[list[float]]:
        if isinstance(query, str):
            payload = (
                {"inputText": query}
                if self.name == "amazon.titan-embed-image-v1"
                else {"texts": [query]}
            )
        else:
            image_module = attempt_import_or_raise("PIL.Image", "pillow")
            if not isinstance(query, image_module.Image):
                raise TypeError("Bedrock image queries must be text or a Pillow image")
            with io.BytesIO() as buffer:
                query.convert("RGB").save(buffer, format="PNG")
                payload = self._image_payload(buffer.getvalue(), "PNG")
        return [
            self._embed(payload, input_type="search_query", max_retries=max_retries)
        ]

    def _image_payload(self, image_bytes: bytes, image_format: str | None) -> dict:
        encoded = base64.b64encode(image_bytes).decode("ascii")
        if self.name == "amazon.titan-embed-image-v1":
            if image_format not in ("JPEG", "PNG"):
                raise ValueError(
                    "Titan Multimodal Embeddings accepts PNG and JPEG images"
                )
            return {"inputImage": encoded}

        if image_format not in ("JPEG", "PNG", "WEBP", "GIF"):
            raise ValueError("Cohere Embed v4 accepts PNG, JPEG, WebP, and GIF images")
        return {"images": [f"data:image/{image_format.lower()};base64,{encoded}"]}

    def _embed(
        self, payload: dict, *, input_type: str, max_retries: int
    ) -> list[float]:
        is_cohere = self.name == "cohere.embed-v4:0"
        if is_cohere:
            payload["input_type"] = input_type
            payload["embedding_types"] = ["float"]

        response = self._invoke_model(json.dumps(payload), max_retries=max_retries)
        if is_cohere:
            return response["embeddings"]["float"][0]
        if response.get("message"):
            raise ValueError(response["message"])
        return response["embedding"]
