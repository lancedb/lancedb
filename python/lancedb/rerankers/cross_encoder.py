import typing
from functools import cached_property
from typing import Union

import pyarrow as pa

from ..util import safe_import
from .base import Reranker

if typing.TYPE_CHECKING:
    import lancedb


class CrossEncoderReranker(Reranker):
    """
    Reranks the results using a cross encoder model. The cross encoder model is
    used to score the query and each result. The results are then sorted by the score.

    Parameters
    ----------
    model : str, default "cross-encoder/ms-marco-TinyBERT-L-6"
        The name of the cross encoder model to use. See the sentence transformers
        documentation for a list of available models.
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    device : str, default None
        The device to use for the cross encoder model. If None, will use "cuda"
        if available, otherwise "cpu".
    """

    def __init__(
        self,
        model_name: str = "cross-encoder/ms-marco-TinyBERT-L-6",
        column: str = "text",
        device: Union[str, None] = None,
    ):
        torch = safe_import("torch")
        self.model_name = model_name
        self.column = column
        self.device = device
        if self.device is None:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"

    @cached_property
    def model(self):
        sbert = safe_import("sentence_transformers")
        cross_encoder = sbert.CrossEncoder(self.model_name)

        return cross_encoder

    def rerank_hybrid(
        self,
        query_builder: "lancedb.HybridQueryBuilder",
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        combined_results = self.merge_results(vector_results, fts_results)
        passages = combined_results[self.column].to_pylist()
        cross_inp = [[query_builder._query, passage] for passage in passages]
        cross_scores = self.model.predict(cross_inp)
        # inver scores - Relevance <> distance
        cross_scores = 1 - cross_scores
        combined_results = combined_results.set_column(
            combined_results.column_names.index("_score"),
            "_score",
            pa.array(cross_scores, type=pa.float32()),
        )
        # sort the results by _score
        combined_results = combined_results.sort_by("_score")
        return combined_results
