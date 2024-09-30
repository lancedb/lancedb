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
from typing import List, Any

import numpy as np

from pydantic import PrivateAttr
from lancedb.pydantic import PYDANTIC_VERSION

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import TEXT


@register("huggingface")
class TransformersEmbeddingFunction(EmbeddingFunction):
    """
    An embedding function that can use any model from the transformers library.

    Parameters:
    ----------
    name : str
        The name of the model to use. This should be a model name that can be loaded
        by transformers.AutoModel.from_pretrained. For example, "bert-base-uncased".
        default: "colbert-ir/colbertv2.0""
    device : str
        The device to use for the model. Default is "cpu".
    show_progress_bar : bool
        Whether to show a progress bar when loading the model. Default is True.
    trust_remote_code : bool
        Whether or not to allow for custom models defined on the HuggingFace
        Hub in their own modeling files. This option should only be set to True
        for repositories you trust and in which you have read the code, as it
        will execute code present on the Hub on your local machine.

    to download package, run :
        `pip install transformers`
    you may need to install pytorch as well - `https://pytorch.org/get-started/locally/`

    """

    name: str = "colbert-ir/colbertv2.0"
    device: str = "cpu"
    trust_remote_code: bool = False
    _tokenizer: Any = PrivateAttr()
    _model: Any = PrivateAttr()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ndims = None
        transformers = attempt_import_or_raise("transformers")
        self._tokenizer = transformers.AutoTokenizer.from_pretrained(self.name)
        self._model = transformers.AutoModel.from_pretrained(
            self.name, trust_remote_code=self.trust_remote_code
        )
        self._model.to(self.device)

    if PYDANTIC_VERSION.major < 2:  # Pydantic 1.x compat

        class Config:
            keep_untouched = (cached_property,)
    else:
        model_config = dict()
        model_config["ignored_types"] = (cached_property,)

    def ndims(self):
        self._ndims = self._model.config.hidden_size
        return self._ndims

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        return self.compute_source_embeddings(query)

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        embedding = []
        for text in texts:
            encoding = self._tokenizer(
                text, return_tensors="pt", padding=True, truncation=True
            ).to(self.device)
            emb = self._model(**encoding).last_hidden_state.mean(dim=1).squeeze()
            embedding.append(emb.tolist())

        return embedding


@register("colbert")
class ColbertEmbeddings(TransformersEmbeddingFunction):
    """
    An embedding function that uses the colbert model from the huggingface library.

    Parameters:
    ----------
    name : str
        The name of the model to use. This should be a model name that can be loaded
        by transformers.AutoModel.from_pretrained. For example, "bert-base-uncased".
        default: "colbert-ir/colbertv2.0""

    to download package, run :
        `pip install transformers`
    you may need to install pytorch as well - `https://pytorch.org/get-started/locally/`

    """

    name: str = "colbert-ir/colbertv2.0"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
