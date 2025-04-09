# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from functools import cached_property
from typing import Union

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker


class CrossEncoderReranker(Reranker):
    """
    Reranks the results using a cross encoder model. The cross encoder model is
    used to score the query and each result. The results are then sorted by the score.

    Parameters
    ----------
    model_name : str, default "cross-encoder/ms-marco-TinyBERT-L-6"
        The name of the cross encoder model to use. See the sentence transformers
        documentation for a list of available models.
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    device : str, default None
        The device to use for the cross encoder model. If None, will use "cuda"
        if available, otherwise "cpu".
    return_score : str, default "relevance"
        options are "relevance" or "all". Only "relevance" is supported for now.
    trust_remote_code : bool, default True
        If True, will trust the remote code to be safe. If False, will not trust
        the remote code and will not run it
    """

    def __init__(
        self,
        model_name: str = "cross-encoder/ms-marco-TinyBERT-L-6",
        column: str = "text",
        device: Union[str, None] = None,
        return_score="relevance",
        trust_remote_code: bool = True,
    ):
        super().__init__(return_score)
        torch = attempt_import_or_raise("torch")
        self.model_name = model_name
        self.column = column
        self.device = device
        self.trust_remote_code = trust_remote_code
        if self.device is None:
            self.device = "cuda" if torch.cuda.is_available() else "cpu"

    @cached_property
    def model(self):
        sbert = attempt_import_or_raise("sentence_transformers")
        # Allows overriding the automatically selected device
        cross_encoder = sbert.CrossEncoder(
            self.model_name,
            device=self.device,
            trust_remote_code=self.trust_remote_code,
        )

        return cross_encoder

    def _rerank(self, result_set: pa.Table, query: str):
        result_set = self._handle_empty_results(result_set)
        if len(result_set) == 0:
            return result_set
        passages = result_set[self.column].to_pylist()
        cross_inp = [[query, passage] for passage in passages]
        cross_scores = self.model.predict(cross_inp)
        result_set = result_set.append_column(
            "_relevance_score", pa.array(cross_scores, type=pa.float32())
        )

        return result_set

    def rerank_hybrid(
        self,
        query: str,
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        combined_results = self.merge_results(vector_results, fts_results)
        combined_results = self._rerank(combined_results, query)
        # sort the results by _score
        if self.score == "relevance":
            combined_results = self._keep_relevance_score(combined_results)
        elif self.score == "all":
            raise NotImplementedError(
                "return_score='all' not implemented for CrossEncoderReranker"
            )
        combined_results = combined_results.sort_by(
            [("_relevance_score", "descending")]
        )

        return combined_results

    def rerank_vector(self, query: str, vector_results: pa.Table):
        vector_results = self._rerank(vector_results, query)
        if self.score == "relevance":
            vector_results = vector_results.drop_columns(["_distance"])

        vector_results = vector_results.sort_by([("_relevance_score", "descending")])
        return vector_results

    def rerank_fts(self, query: str, fts_results: pa.Table):
        fts_results = self._rerank(fts_results, query)
        if self.score == "relevance":
            fts_results = fts_results.drop_columns(["_score"])

        fts_results = fts_results.sort_by([("_relevance_score", "descending")])
        return fts_results
