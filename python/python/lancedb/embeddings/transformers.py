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
from typing import List

import numpy as np

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

    to download package, run :
        `pip install transformers`
    you may need to install pytorch as well - `https://pytorch.org/get-started/locally/`

    """

    name: str = "colbert-ir/colbertv2.0"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    @cached_property
    def _model(self):
        transformers = attempt_import_or_raise("transformers")
        tokenizer = transformers.AutoTokenizer.from_pretrained(self.name)
        model = transformers.AutoModel.from_pretrained(self.name)
        return tokenizer, model

    def ndims(self):
        _, model = self._model
        return model.config.hidden_size

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        return self.compute_source_embeddings(query)

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        tokenizer, model = self._model
        embedding = []
        for text in texts:
            encoding = tokenizer(
                text, return_tensors="pt", padding=True, truncation=True
            )
            emb = model(**encoding).last_hidden_state.mean(dim=1).squeeze()
            embedding.append(emb.detach().numpy())

        return embedding
