# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from abc import ABC
from dataclasses import dataclass
from packaging.version import Version
from typing import List, Literal

import numpy as np
import pyarrow as pa

ARROW_VERSION = Version(pa.__version__)


@dataclass
class RerankableResult:
    """Base class for results passed to a reranker's ``compute_scores`` method."""

    data: pa.Table


@dataclass
class FtsResult(RerankableResult):
    """FTS search results.

    ``data`` may contain the following columns:

    - ``_rowid``: the rowid of the matching document
    - ``text``: the text of the matching document
    - ``_score``: the relevance score according to FTS
    """

    pass


@dataclass
class VectorResult(RerankableResult):
    """Vector (ANN) search results.

    ``data`` may contain the following columns:

    - ``_rowid``: the rowid of the matching document
    - ``_distance``: the distance according to ANN
    - ``text``: the text of the matching document
    """

    pass


class Reranker(ABC):
    """
    Base class for rerankers.

    A reranker computes a ``_relevance_score`` for query results coming from
    ANN and/or FTS searches.  Subclasses must override :meth:`compute_scores`
    and optionally :meth:`needs_columns`.

    Parameters
    ----------
    return_score : str, default "relevance"
        Options are "relevance" or "all".
        If "relevance", only the ``_relevance_score`` column is kept.
        If "all", the original ``_distance`` and ``_score`` columns are
        preserved alongside ``_relevance_score``.
    """

    def __init__(self, return_score: str = "relevance"):
        if return_score not in ["relevance", "all"]:
            raise ValueError("score must be either 'relevance' or 'all'")
        self.score = return_score
        # Set the merge args based on the arrow version here to avoid checking
        # it at each query
        self._concat_tables_args = {"promote_options": "default"}
        if ARROW_VERSION.major <= 13:
            self._concat_tables_args = {"promote": True}

    def __str__(self):
        return self.__class__.__name__

    def needs_columns(
        self,
    ) -> List[Literal["_distance", "_score", "text", "_rowid"]]:
        """Return the columns this reranker needs to see in the input tables.

        The query pipeline uses this to avoid loading unnecessary fields.
        Override in subclasses to declare requirements.  The default
        implementation returns ``["text"]``.
        """
        return ["text"]

    def compute_scores(
        self,
        query: str,
        results: List[RerankableResult],
    ) -> List[pa.Array]:
        """Compute relevance scores for each result set.

        Subclasses must override this method.

        Parameters
        ----------
        query : str
            The search query.
        results : List[RerankableResult]
            One or more result sets, each wrapped as a :class:`VectorResult`
            or :class:`FtsResult`.  It is possible for there to be only one
            input (e.g. ANN-only or FTS-only), or multiple of the same type
            (multi-vector).

        Returns
        -------
        List[pa.Array]
            One ``float32`` array per input result set.  Each array has the
            same length as the corresponding ``result.data`` table and
            contains the computed relevance score for every row.
        """
        raise NotImplementedError(
            f"{self.__class__.__name__} does not implement compute_scores"
        )

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
