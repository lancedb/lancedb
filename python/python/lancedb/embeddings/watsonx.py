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
import os
from functools import cached_property
from typing import List, Optional, Dict, Union

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register

import numpy as np

DEFAULT_WATSONX_URL = "https://us-south.ml.cloud.ibm.com"

MODELS_DIMS = {
    "ibm/slate-125m-english-rtrvr": 768,
    "ibm/slate-30m-english-rtrvr": 384,
    "sentence-transformers/all-minilm-l12-v2": 384,
    "intfloat/multilingual-e5-large": 1024,
}


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
        if self.name not in MODELS_DIMS:
            raise ValueError(f"Unknown model name {self.name}")
        return MODELS_DIMS[self.name]

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
        ibm_watsonx_ai_foundation_models = attempt_import_or_raise(
            "ibm_watsonx_ai.foundation_models"
        )

        kwargs = {"model_id": self.name}
        if self.params:
            kwargs["params"] = self.params
        if self.project_id:
            kwargs["project_id"] = self.project_id
        elif "WATSONX_PROJECT_ID" in os.environ:
            kwargs["project_id"] = os.environ["WATSONX_PROJECT_ID"]
        else:
            raise ValueError("WATSONX_PROJECT_ID must be set or passed")

        creds_kwargs = {}
        if self.api_key:
            creds_kwargs["api_key"] = self.api_key
        elif "WATSONX_API_KEY" in os.environ:
            creds_kwargs["api_key"] = os.environ["WATSONX_API_KEY"]
        else:
            raise ValueError("WATSONX_API_KEY must be set or passed")
        if self.url:
            creds_kwargs["url"] = self.url
        else:
            creds_kwargs["url"] = DEFAULT_WATSONX_URL
        kwargs["credentials"] = ibm_watsonx_ai.Credentials(**creds_kwargs)

        return ibm_watsonx_ai_foundation_models.Embeddings(**kwargs)
