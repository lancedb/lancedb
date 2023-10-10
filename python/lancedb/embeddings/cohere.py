import os
from typing import ClassVar
from typing import List, Union
import numpy as np

from .functions import register, TextEmbeddingFunction


@register("cohere")
class CohereEmbeddingFunction(TextEmbeddingFunction):
    """
    An embedding function that uses the Cohere API

    https://docs.cohere.com/docs/multilingual-language-models

    Parameters
    ----------
    name: str, default "embed-multilingual-v2.0"
        The name of the model to use. See the Cohere documentation for a list of available models.
    
    Examples
    --------
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import EmbeddingFunctionRegistry
    import pandas as pd

    cohere = EmbeddingFunctionRegistry.get_instance().get("cohere").create(name="embed-multilingual-v2.0")

    class TextModel(LanceModel):
        text: str = cohere.SourceField()
        vector: Vector(cohere.ndims()) =  cohere.VectorField()

    df = pd.DataFrame({"text": ["hello world", "goodbye world"]})
    db = lancedb.connect("~/lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(df)
    print(tbl.to_pandas())
    """

    name: str = "embed-multilingual-v2.0"
    client: ClassVar = None

    def ndims(self):
        # TODO: This throws pydantic error when using an instance var to store the ndims. Investigate and make dynamic
        return 768

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray]
    ) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
        # TODO retry, rate limit, token limit
        self._init_client()    
        rs = CohereEmbeddingFunction.client.embed(texts=texts, model=self.name)
    
        return [emb for emb in rs.embeddings]
    
    def _init_client(self):
        cohere = self.safe_import("cohere")
        if CohereEmbeddingFunction.client is None:
            if os.environ.get("COHERE_API_KEY") is None:
                raise ValueError("Please set the COHERE_API_KEY environment variable")
            CohereEmbeddingFunction.client = cohere.Client(os.environ["COHERE_API_KEY"])
