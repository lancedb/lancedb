# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
import os
from typing import ClassVar, List, Union
import numpy as np

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .registry import register
from .utils import api_key_not_found_help, TEXT


@register("azure-ai-text")
class AzureAITextEmbeddingFunction(TextEmbeddingFunction):
    """
    An embedding function that uses the AzureAI API

    https://learn.microsoft.com/en-us/python/api/overview/azure/ai-inference-readme?view=azure-python-preview

    - AZURE_AI_ENDPOINT: The endpoint URL for the AzureAI service.
    - AZURE_AI_API_KEY: The API key for the AzureAI service.

    Parameters
    ----------
    - name: str
        The name of the model to use. This should be set to the model you want to use for embeddings.
    - _ndims: int
        The number of dimensions of the embeddings. This is required to create the vector column in LanceDB.


    Examples
    --------
    import lancedb
    import pandas as pd
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import get_registry

    model = get_registry().get("azure-ai-text").create(name="embed-v-4-0", ndims=1536)

    class TextModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect("lance_example")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    rs = tbl.search("hello").limit(1).to_pandas()
    #           text                                             vector  _distance
    # 0  hello world  [-0.018188477, 0.0134887695, -0.013000488, 0.0...   0.841431
    """

    name: str
    _ndims: int
    client: ClassVar = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ndims = kwargs.get("ndims")
        if self._ndims is None:
            raise ValueError(
                "AzureAIEmbeddingFunction requires ndims to be set. "
                "Please provide the number of dimensions for the embeddings."
            )

    def ndims(self):
        return self._ndims

    def compute_query_embeddings(self, query: str, *args, **kwargs) -> List[np.array]:
        return self.compute_source_embeddings(query, input_type="query")

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        input_type = (
            kwargs.get("input_type") or "document"
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
        input_type: Optional[str]

        truncation: Optional[bool]
        """
        AzureAITextEmbeddingFunction._init_client()

        if isinstance(texts, np.ndarray):
            if texts.dtype != object:
                raise ValueError(
                    "AzureAIEmbeddingFunction only supports input of type `object` (i.e., list of strings) for numpy arrays."
                )
            texts = texts.tolist()

        # batch process so that no more than 96 texts are sent at once.
        batch_size = 96
        embeddings = []
        for i in range(0, len(texts), batch_size):
            rs = AzureAITextEmbeddingFunction.client.embed(
                input=texts[i : i + batch_size],
                model=self.name,
                dimensions=self._ndims,
                **kwargs,
            )
            embeddings.extend(emb.embedding for emb in rs.data)
        return embeddings

    @staticmethod
    def _init_client():
        if AzureAITextEmbeddingFunction.client is None:
            if os.environ.get("AZURE_AI_API_KEY") is None:
                api_key_not_found_help("AZURE_AI")
            if os.environ.get("AZURE_AI_ENDPOINT") is None:
                api_key_not_found_help("AZURE_AI", "ENDPOINT")

            inference = attempt_import_or_raise(
                "azure.ai.inference", "azure-ai-inference"
            )

            credentials = attempt_import_or_raise(
                "azure.core.credentials", "azure-ai-inference"
            )

            AzureAITextEmbeddingFunction.client = inference.EmbeddingsClient(
                endpoint=os.environ["AZURE_AI_ENDPOINT"],
                credential=credentials.AzureKeyCredential(
                    os.environ["AZURE_AI_API_KEY"]
                ),
            )
