# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import json
from functools import cached_property
from typing import List, Union

import numpy as np

from lancedb.pydantic import PYDANTIC_VERSION

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import TEXT


@register("bedrock-text")
class BedRockText(TextEmbeddingFunction):
    """
    Parameters
    ----------
    name: str, default "amazon.titan-embed-text-v1"
        The model ID of the bedrock model to use. Supported models for are:
        - amazon.titan-embed-text-v1
        - cohere.embed-english-v3
        - cohere.embed-multilingual-v3
    region: str, default "us-east-1"
        Optional name of the AWS Region in which the service should be called.
    profile_name: str, default None
        Optional name of the AWS profile to use for calling the Bedrock service.
        If not specified, the default profile will be used.
    assumed_role: str, default None
        Optional ARN of an AWS IAM role to assume for calling the Bedrock service.
        If not specified, the current active credentials will be used.
    role_session_name: str, default "lancedb-embeddings"
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
    region: str = "us-east-1"
    assumed_role: Union[str, None] = None
    profile_name: Union[str, None] = None
    role_session_name: str = "lancedb-embeddings"
    source_input_type: str = "search_document"
    query_input_type: str = "search_query"

    if PYDANTIC_VERSION.major < 2:  # Pydantic 1.x compat

        class Config:
            keep_untouched = (cached_property,)
    else:
        model_config = dict()
        model_config["ignored_types"] = (cached_property,)

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
            # invoke bedrock API
            response = self.client.invoke_model(
                body=body,
                modelId=self.name,
                accept="application/json",
                contentType="application/json",
            )

            # format output based on provider
            response_body = json.loads(response.get("body").read())
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

    @cached_property
    def client(self):
        """Create a boto3 client for Amazon Bedrock service

        Returns
        -------
        boto3.client
            The boto3 client for Amazon Bedrock service
        """
        botocore = attempt_import_or_raise("botocore")
        boto3 = attempt_import_or_raise("boto3")

        session_kwargs = {"region_name": self.region}
        client_kwargs = {**session_kwargs}

        if self.profile_name:
            session_kwargs["profile_name"] = self.profile_name

        retry_config = botocore.config.Config(
            region_name=self.region,
            retries={
                "max_attempts": 0,  # disable this as retries retries are handled
                "mode": "standard",
            },
        )
        session = (
            boto3.Session(**session_kwargs) if self.profile_name else boto3.Session()
        )
        if self.assumed_role:  # if not using default credentials
            sts = session.client("sts")
            response = sts.assume_role(
                RoleArn=str(self.assumed_role),
                RoleSessionName=self.role_session_name,
            )
            client_kwargs["aws_access_key_id"] = response["Credentials"]["AccessKeyId"]
            client_kwargs["aws_secret_access_key"] = response["Credentials"][
                "SecretAccessKey"
            ]
            client_kwargs["aws_session_token"] = response["Credentials"]["SessionToken"]

        service_name = "bedrock-runtime"

        bedrock_client = session.client(
            service_name=service_name, config=retry_config, **client_kwargs
        )

        return bedrock_client
