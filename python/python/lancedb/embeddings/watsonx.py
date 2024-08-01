#  Copyright (c) 2023. LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from functools import cached_property
from typing import List, Optional, Dict, Union

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register

import numpy as np


@register("watsonx")
class WatsonxEmbeddings(TextEmbeddingFunction):
    """
    API Docs:
    ---------
    https://cloud.ibm.com/apidocs/watsonx-ai#text-embeddings

    Supported embedding models:
    ---------------------------
    https://dataplatform.cloud.ibm.com/docs/content/wsj/analyze-data/fm-models-embed.html?context=wx
    """

    name: str = "ibm/slate-125m-english-rtrvr"
    api_key: Optional[str] = None
    project_id: Optional[str] = None
    url: Optional[str] = None
    params: Optional[Dict] = None

    @staticmethod
    def model_names():
        return [
            "ibm/slate-125m-english-rtrvr",
            "ibm/slate-30m-english-rtrvr",
            "sentence-transformers/all-minilm-l12-v2",
            "intfloat/multilingual-e5-large",
        ]

    def ndims(self):
        return self._ndims

    @cached_property
    def _ndims(self):
        if self.name == "ibm/slate-125m-english-rtrvr":
            return 768
        elif self.name == "ibm/slate-30m-english-rtrvr":
            return self.dim or 384
        elif self.name == "sentence-transformers/all-minilm-l12-v2":
            return self.dim or 384
        elif self.name == "intfloat/multilingual-e5-large":
            return self.dim or 1024
        else:
            raise ValueError(f"Unknown model name {self.name}")

    def generate_embeddings(
        self,
        texts: Union[List[str], np.ndarray],
        *args,
        **kwargs,
    ) -> List[List[float]]:
        return self._watsonx_client.embed_documents(
            texts=list(texts),
            *args,
            **kwargs,
        )

    @cached_property
    def _watsonx_client(self):
        ibm_watsonx_ai = attempt_import_or_raise("ibm_watsonx_ai")

        kwargs = {"model_id": self.name}
        if self.project_id:
            kwargs["project_id"] = self.project_id
        if self.params:
            kwargs["params"] = self.params

        creds_kwargs = {}
        if self.api_key:
            creds_kwargs["api_key"] = self.api_key
        if self.url:
            creds_kwargs["url"] = self.url

        if creds_kwargs:
            kwargs["credentials"] = ibm_watsonx_ai.Credentials(**creds_kwargs)

        return ibm_watsonx_ai.foundation_models.Embeddings(**kwargs)
