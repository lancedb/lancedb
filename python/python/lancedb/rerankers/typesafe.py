# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from concurrent.futures import ThreadPoolExecutor
from functools import cached_property
from itertools import chain
from typing import Any, Dict, List, Mapping, Optional

import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import Reranker

DEFAULT_INSTRUCTIONS = (
    "Does the document contain information that answers or directly addresses "
    "the query?"
)
DEFAULT_CRITERIA = {
    "true": "The document answers the query or states facts the query asks about.",
    "false": "The document is off-topic, or only shares keywords or a general "
    "subject with the query without addressing it.",
}
# The key TypeSafe returns the answer under. It is not sent to the model.
_QUESTION_ID = "relevance"


class TypeSafeReranker(Reranker):
    """
    Reranks the results using the TypeSafe System One API.
    https://docs.typesafe.ai/cookbooks/rerank_typesafe

    Each result is scored independently: TypeSafe reads the query together with
    the result's ``column`` value and answers a yes/no ("noul") question about
    whether the document is relevant. The probability of "yes", between 0 and 1,
    becomes the ``_relevance_score``. Because every score is an absolute
    probability rather than a position in the list, scores are comparable
    across queries and can be thresholded. They are model estimates, though,
    and can vary slightly between identical calls, so results with close scores
    may swap places when the same search is repeated.

    By default, one request is sent per non-null result. Set ``batch_size=40``
    to score up to 40 documents per request, with at most ``max_concurrency``
    requests in flight. Each question sees only its own document and the shared
    query. Null documents keep a score of zero without an API request; empty
    strings are scored normally. API and response-validation errors propagate.

    Parameters
    ----------
    model_name : str, default "jev-latest"
        The TypeSafe model to use.
    column : str, default "text"
        The name of the column holding the document text to score.
    instructions : str, optional
        The yes/no question asked about each query and document pair. The state
        TypeSafe reads with ``batch_size=1`` is
        ``{"query": <query>, "document": <column value>}``.
        Defaults to a generic relevance question. See ``batch_size`` for the
        different location of the document in batched requests.
    criteria : Mapping[str, str], optional
        What a "yes" and a "no" mean, as a mapping with the keys ``"true"`` and
        ``"false"``. Domain-specific criteria usually rank better than the
        generic default. Pass an empty mapping to send no criteria.
    return_score : str, default "relevance"
        Options are "relevance" or "all". If "all", keeps the vector and FTS
        scores alongside the relevance score.
    api_key : str, optional
        The API key to use. If None, the TypeSafe SDK reads the
        ``TYPESAFE_API_KEY`` environment variable.
    max_concurrency : int, default 8
        The maximum number of TypeSafe requests in flight for one rerank call.
    batch_size : int, default 1
        Positive integer limiting non-null documents per request. The default
        preserves the original request format. With a value greater than one,
        state is ``{"query": <query>}`` and each question's instructions are
        ``{"question": <instructions>, "document": <column value>}``.
        Instructions and criteria are preserved verbatim. Custom prompts that
        explicitly reference the document in request state (for example,
        ``state.document``) must be adapted before opting into batching.
        The TypeSafe SDK handles retries in both modes.

    Examples
    --------
    >>> reranker = TypeSafeReranker(batch_size=40, max_concurrency=8)
    """

    def __init__(
        self,
        model_name: str = "jev-latest",
        column: str = "text",
        instructions: Optional[str] = None,
        criteria: Optional[Mapping[str, str]] = None,
        return_score: str = "relevance",
        api_key: Optional[str] = None,
        max_concurrency: int = 8,
        batch_size: int = 1,
    ):
        super().__init__(return_score)
        if max_concurrency < 1:
            raise ValueError("max_concurrency must be at least 1")
        if (
            isinstance(batch_size, bool)
            or not isinstance(batch_size, int)
            or batch_size < 1
        ):
            raise ValueError("batch_size must be a positive integer")
        criteria = DEFAULT_CRITERIA if criteria is None else dict(criteria)
        unknown = set(criteria) - {"true", "false"}
        if unknown:
            raise ValueError(
                f"criteria keys must be 'true' or 'false', got {sorted(unknown)}"
            )
        self.model_name = model_name
        self.column = column
        self.instructions = instructions or DEFAULT_INSTRUCTIONS
        self.criteria = criteria
        self.api_key = api_key
        self.max_concurrency = max_concurrency
        self.batch_size = batch_size

    def __str__(self):
        return f"TypeSafeReranker(model_name={self.model_name})"

    @cached_property
    def _client(self):
        typesafe_sdk = attempt_import_or_raise("typesafe_sdk", "typesafe-sdk")
        return typesafe_sdk.TypeSafeClient(api_key=self.api_key)

    @cached_property
    def _question(self) -> Dict[str, Any]:
        question: Dict[str, Any] = {"type": "noul", "instructions": self.instructions}
        if self.criteria:
            question["criteria"] = self.criteria
        return question

    def _score_batch(self, query: str, documents: List[str]) -> List[float]:
        if self.batch_size == 1:
            state = {"query": query, "document": documents[0]}
            questions = {_QUESTION_ID: self._question}
        else:
            state = {"query": query}
            # IDs only need to be unique within this request, even for duplicates.
            questions = {
                str(index): {
                    **self._question,
                    "instructions": {
                        "question": self.instructions,
                        "document": document,
                    },
                }
                for index, document in enumerate(documents)
            }
        response = self._client.system_one(
            state=state,
            questions=questions,
            model=self.model_name,
        )
        expected = set(questions)
        actual = set(response.answers)
        if actual != expected:
            raise ValueError(
                "TypeSafe returned mismatched answer IDs: "
                f"missing {sorted(expected - actual)}, "
                f"unexpected {sorted(actual - expected)}"
            )
        scores = []
        for question_id in questions:
            score = getattr(response.answers[question_id], "noul", None)
            if (
                isinstance(score, bool)
                or not isinstance(score, (int, float))
                or not 0 <= score <= 1
            ):
                raise ValueError(
                    "TypeSafe returned an invalid relevance probability "
                    f"for answer {question_id!r}: {score!r}"
                )
            scores.append(float(score))
        return scores

    def _rerank(self, result_set: pa.Table, query: str) -> pa.Table:
        result_set = self._handle_empty_results(result_set)
        if len(result_set) == 0:
            return result_set
        docs = result_set[self.column].to_pylist()
        documents = [doc for doc in docs if doc is not None]
        batches = [
            documents[start : start + self.batch_size]
            for start in range(0, len(documents), self.batch_size)
        ]
        # Rerankers are also called synchronously from inside the async query
        # APIs, so the requests run on threads rather than on an event loop.
        with ThreadPoolExecutor(max_workers=self.max_concurrency) as pool:
            batch_scores = pool.map(
                lambda batch: self._score_batch(query, batch), batches
            )
            # Both map and _score_batch preserve input order.
            scored_documents = chain.from_iterable(batch_scores)
            scores = [0.0 if doc is None else next(scored_documents) for doc in docs]
        result_set = result_set.append_column(
            "_relevance_score", pa.array(scores, type=pa.float32())
        )
        return result_set.sort_by([("_relevance_score", "descending")])

    def rerank_hybrid(
        self,
        query: str,
        vector_results: pa.Table,
        fts_results: pa.Table,
    ):
        if self.score == "all":
            combined_results = self._merge_and_keep_scores(vector_results, fts_results)
        else:
            combined_results = self.merge_results(vector_results, fts_results)
        combined_results = self._rerank(combined_results, query)
        if self.score == "relevance":
            combined_results = self._keep_relevance_score(combined_results)
        return combined_results

    def rerank_vector(self, query: str, vector_results: pa.Table):
        vector_results = self._rerank(vector_results, query)
        if self.score == "relevance":
            vector_results = vector_results.drop_columns(["_distance"])
        return vector_results

    def rerank_fts(self, query: str, fts_results: pa.Table):
        fts_results = self._rerank(fts_results, query)
        if self.score == "relevance":
            fts_results = fts_results.drop_columns(["_score"])
        return fts_results
