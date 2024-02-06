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
from typing import List, Union

import numpy as np

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import weak_lru


@register("gte-text")
class GteEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses GTE-LARGE MLX format(for Apple silicon devices only)
    as well as the standard cpu/gpu version from: https://huggingface.co/thenlper/gte-large.

    For Apple users, you will need the mlx package insalled, which can be done with:
        pip install mlx

    Parameters
    ----------
    name: str, default "thenlper/gte-large"
        The name of the model to use.
    device: str, default "cpu"
        Sets the device type for the model.
    normalize: str, default "True"
        Controls normalize param in encode function for the transformer.
    mlx: bool, default False
        Controls which model to use. False for gte-large,True for the mlx version.

    Examples
    --------
    import lancedb
    import lancedb.embeddings.gte
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector
    import pandas as pd

    model = get_registry().get("gte-text").create() # mlx=True for Apple silicon
    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hi hello sayonara", "goodbye world"]})
    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    rs = tbl.search("hello").limit(1).to_pandas()

    """

    name: str = "thenlper/gte-large"
    device: str = "cpu"
    normalize: bool = True
    mlx: bool = False

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ndims = None
        if kwargs:
            self.mlx = kwargs.get("mlx", False)
            if self.mlx is True:
                self.name = "gte-mlx"

    @property
    def embedding_model(self):
        """
        Get the embedding model specified by the flag,
        name and device. This is cached so that the model is only loaded
        once per process.
        """
        return self.get_embedding_model()

    def ndims(self):
        if self.mlx is True:
            self._ndims = self.embedding_model.dims
        if self._ndims is None:
            self._ndims = len(self.generate_embeddings("foo")[0])
        return self._ndims

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray]
    ) -> List[np.array]:
        """
        Get the embeddings for the given texts.

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
        if self.mlx is True:
            return self.embedding_model.run(list(texts)).tolist()

        return self.embedding_model.encode(
            list(texts),
            convert_to_numpy=True,
            normalize_embeddings=self.normalize,
        ).tolist()

    @weak_lru(maxsize=1)
    def get_embedding_model(self):
        """
        Get the embedding model specified by the flag,
        name and device. This is cached so that the model is only loaded
        once per process.
        """
        if self.mlx is True:
            from .gte_mlx_model import Model

            return Model()
        else:
            sentence_transformers = attempt_import_or_raise(
                "sentence_transformers", "sentence-transformers"
            )
            return sentence_transformers.SentenceTransformer(
                self.name, device=self.device
            )
