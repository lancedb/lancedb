# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import List

import pyarrow as pa
from .base import Reranker, RerankableResult
from ..util import attempt_import_or_raise


class AnswerdotaiRerankers(Reranker):
    """
    Reranks the results using the Answerdotai Rerank API.
    All supported reranker model types can be found here:
    - https://github.com/AnswerDotAI/rerankers


    Parameters
    ----------
    model_type : str, default "colbert"
        The type of the model to use.
    model_name : str, default "rerank-english-v2.0"
        The name of the model to use from the given model type.
    column : str, default "text"
        The name of the column to use as input to the cross encoder model.
    return_score : str, default "relevance"
        options are "relevance" or "all". Only "relevance" is supported for now.
    **kwargs
        Additional keyword arguments to pass to the model. For example, 'device'.
        See AnswerDotAI/rerankers for more information.
    """

    def __init__(
        self,
        model_type="colbert",
        model_name: str = "answerdotai/answerai-colbert-small-v1",
        column: str = "text",
        return_score="relevance",
        **kwargs,
    ):
        super().__init__(return_score)
        self.column = column
        rerankers = attempt_import_or_raise(
            "rerankers"
        )  # import here for faster ops later
        self.model_name = model_name
        self.model_type = model_type
        self.reranker = rerankers.Reranker(
            model_name=model_name, model_type=model_type, **kwargs
        )

    def __str__(self):
        return (
            f"AnswerdotaiRerankers(model_type={self.model_type}, "
            f"model_name={self.model_name})"
        )

    def needs_columns(self):
        return [self.column]

    def compute_scores(
        self,
        query: str,
        results: List[RerankableResult],
    ) -> List[pa.Array]:
        tables = [r.data for r in results]
        merged = pa.concat_tables(tables, **self._concat_tables_args)
        if "_rowid" in merged.column_names:
            merged = self._deduplicate(merged)

        if len(merged) == 0:
            return [pa.array([], type=pa.float32()) for _ in results]

        docs = merged[self.column].to_pylist()
        doc_ids = list(range(len(docs)))
        ranked = self.reranker.rank(query, docs, doc_ids=doc_ids)
        merged_scores = {
            docs[i]: float(ranked.get_result_by_docid(i).score) for i in doc_ids
        }

        score_arrays = []
        for result in results:
            texts = result.data[self.column].to_pylist()
            scores = [merged_scores.get(t, 0.0) for t in texts]
            score_arrays.append(pa.array(scores, type=pa.float32()))

        return score_arrays
