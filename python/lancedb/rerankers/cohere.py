from typing import Union
from functools import cached_property
from .base import Reranker
from ..embeddings.utils import api_key_not_found_help
from ..utils.general import safe_import
import numpy as np
import lancedb
import pyarrow as pa
import os


class CohereReranker(Reranker):
    """
    Reranks the results using cohere rerank api.

    Parameters
    ----------
    model_name : str, default "rerank-multilingual-v2.0"
        The name of the cross encoder model to use. Available cohere models are:
        - rerank-english-v2.0
        - rerank-multilingual-v2.0
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    top_k : str, default None
        The number of results to return. If None, will return all results.
    """

    def __init__(
        self,
        model_name: str = "rerank-multilingual-v2.0",
        column: str = "text",
        top_n: Union[int, None] = None,
    ):
        self.model_name = model_name
        self.column = column
        self.top_n = top_n

    @cached_property
    def _client(self):
        cohere = safe_import("cohere")
        if os.environ.get("COHERE_API_KEY") is None:
            self.api_key_not_found_help("cohere")
        return cohere.Client(os.environ["COHERE_API_KEY"])

    import numpy as np

    def rerank_hybrid(
        self,
        query_builder: "lancedb.HybridQueryBuilder",
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        combined_results = self.merge_results(vector_results, fts_results)
        docs = combined_results[self.column].to_pylist()
        results = self._client.rerank(
            query=query_builder._query,
            documents=docs,
            top_n=self.top_n,
            model=self.model_name,
        )
        results = [(result.index, result.relevance_score) for result in results]
        # sort by score
        scores = np.array([result[1] for result in results])
        sorted_indices = np.argsort(scores)[::-1]
        # sort the results by the sorted indices
        combined_results = combined_results.take(sorted_indices)
        # add the scores
        combined_results = combined_results.set_column(
            combined_results.column_names.index("_score"),
            "_score",
            pa.array(scores[sorted_indices], type=pa.float32()),
        )

        return combined_results
