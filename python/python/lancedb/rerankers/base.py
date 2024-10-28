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

from abc import ABC, abstractmethod
from packaging.version import Version
from typing import Union, List, TYPE_CHECKING

import numpy as np
import pyarrow as pa

if TYPE_CHECKING:
    from ..table import LanceVectorQueryBuilder

ARROW_VERSION = Version(pa.__version__)


class Reranker(ABC):
    def __init__(self, return_score: str = "relevance"):
        """
        Interface for a reranker. A reranker is used to rerank the results from a
        vector and FTS search. This is useful for combining the results from both
        search methods.

        Parameters
        ----------
        return_score : str, default "relevance"
            opntions are "relevance" or "all"
            The type of score to return. If "relevance", will return only the relevance
            score. If "all", will return all scores from the vector and FTS search along
            with the relevance score.

        """
        if return_score not in ["relevance", "all"]:
            raise ValueError("score must be either 'relevance' or 'all'")
        self.score = return_score
        # Set the merge args based on the arrow version here to avoid checking it at
        # each query
        self._concat_tables_args = {"promote_options": "default"}
        if ARROW_VERSION.major <= 13:
            self._concat_tables_args = {"promote": True}

    def rerank_vector(
        self,
        query: str,
        vector_results: pa.Table,
    ):
        """
        Rerank function receives the result from the vector search.
        This isn't mandatory to implement

        Parameters
        ----------
        query : str
            The input query
        vector_results : pa.Table
            The results from the vector search

        Returns
        -------
        pa.Table
            The reranked results
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement rerank_vector"
        )

    def rerank_fts(
        self,
        query: str,
        fts_results: pa.Table,
    ):
        """
        Rerank function receives the result from the FTS search.
        This isn't mandatory to implement

        Parameters
        ----------
        query : str
            The input query
        fts_results : pa.Table
            The results from the FTS search

        Returns
        -------
        pa.Table
            The reranked results
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement rerank_fts"
        )

    @abstractmethod
    def rerank_hybrid(
        self,
        query: str,
        vector_results: pa.Table,
        fts_results: pa.Table,
    ) -> pa.Table:
        """
        Rerank function receives the individual results from the vector and FTS search
        results. You can choose to use any of the results to generate the final results,
        allowing maximum flexibility. This is mandatory to implement

        Parameters
        ----------
        query : str
            The input query
        vector_results : pa.Table
            The results from the vector search
        fts_results : pa.Table
            The results from the FTS search

        Returns
        -------
        pa.Table
            The reranked results
        """
        pass

    def merge_results(self, vector_results: pa.Table, fts_results: pa.Table):
        """
        Merge the results from the vector and FTS search. This is a vanilla merging
        function that just concatenates the results and removes the duplicates.

        NOTE: This doesn't take score into account. It'll keep the instance that was
        encountered first. This is designed for rerankers that don't use the score.
        In case you want to use the score, or support `return_scores="all"` you'll
        have to implement your own merging function.

        Parameters
        ----------
        vector_results : pa.Table
            The results from the vector search
        fts_results : pa.Table
            The results from the FTS search
        """
        combined = pa.concat_tables(
            [vector_results, fts_results], **self._concat_tables_args
        )

        # deduplicate
        combined = self._deduplicate(combined)

        return combined

    def rerank_multivector(
        self,
        vector_results: Union[List[pa.Table], List["LanceVectorQueryBuilder"]],
        query: Union[str, None],  # Some rerankers might not need the query
        deduplicate: bool = False,
    ):
        """
        This is a rerank function that receives the results from multiple
        vector searches. For example, this can be used to combine the
        results of two vector searches with different embeddings.

        Parameters
        ----------
        vector_results : List[pa.Table] or List[LanceVectorQueryBuilder]
            The results from the vector search. Either accepts the query builder
            if the results haven't been executed yet or the results in arrow format.
        query : str or None,
            The input query. Some rerankers might not need the query to rerank.
            In that case, it can be set to None explicitly. This is inteded to
            be handled by the reranker implementations.
        deduplicate : bool, optional
            Whether to deduplicate the results based on the `_rowid` column,
            by default False. Requires `_rowid` to be present in the results.

        Returns
        -------
        pa.Table
            The reranked results
        """
        vector_results = (
            [vector_results] if not isinstance(vector_results, list) else vector_results
        )

        # Make sure all elements are of the same type
        if not all(isinstance(v, type(vector_results[0])) for v in vector_results):
            raise ValueError(
                "All elements in vector_results should be of the same type"
            )

        # avoids circular import
        if type(vector_results[0]).__name__ == "LanceVectorQueryBuilder":
            vector_results = [result.to_arrow() for result in vector_results]
        elif not isinstance(vector_results[0], pa.Table):
            raise ValueError(
                "vector_results should be a list of pa.Table or LanceVectorQueryBuilder"
            )

        combined = pa.concat_tables(vector_results, **self._concat_tables_args)

        reranked = self.rerank_vector(query, combined)

        # TODO: Allow custom deduplicators here.
        # currently, this'll just keep the first instance.
        if deduplicate:
            if "_rowid" not in combined.column_names:
                raise ValueError(
                    "'_rowid' is required for deduplication. \
                    add _rowid to search results like this: \
                    `search().with_row_id(True)`"
                )
            reranked = self._deduplicate(reranked)

        return reranked

    def _deduplicate(self, table: pa.Table):
        """
        Deduplicate the table based on the `_rowid` column.
        """
        row_id = table.column("_rowid")

        # deduplicate
        mask = np.full((table.shape[0]), False)
        _, mask_indices = np.unique(np.array(row_id), return_index=True)
        mask[mask_indices] = True
        deduped_table = table.filter(mask=mask)

        return deduped_table

    def _keep_relevance_score(self, combined_results: pa.Table):
        if self.score == "relevance":
            if "_score" in combined_results.column_names:
                combined_results = combined_results.drop_columns(["_score"])
            if "_distance" in combined_results.column_names:
                combined_results = combined_results.drop_columns(["_distance"])
        return combined_results
