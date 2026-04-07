# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from typing import List

import pyarrow as pa


def check_reranker_result(result):
    """Validate the final result table from a reranker."""
    if not isinstance(result, pa.Table):  # Enforce type
        raise TypeError(
            f"reranker must return a pyarrow.Table, got {type(result)}"
        )

    # Enforce that `_relevance_score` column is present
    if "_relevance_score" not in result.column_names:
        raise ValueError(
            "reranker must return a pyarrow.Table with a column "
            "named `_relevance_score`"
        )


def check_compute_scores_result(
    score_arrays: List[pa.Array], num_results: int
):
    """Validate the return value of ``compute_scores``.

    Parameters
    ----------
    score_arrays : List[pa.Array]
        The arrays returned by ``compute_scores``.
    num_results : int
        The number of result sets that were passed to ``compute_scores``.
    """
    if not isinstance(score_arrays, list):
        raise TypeError(
            f"compute_scores must return a list of pa.Array, got {type(score_arrays)}"
        )
    if len(score_arrays) != num_results:
        raise ValueError(
            f"compute_scores returned {len(score_arrays)} arrays "
            f"but {num_results} result sets were provided"
        )
    for i, arr in enumerate(score_arrays):
        if not isinstance(arr, pa.Array):
            raise TypeError(
                f"compute_scores[{i}] must be a pa.Array, got {type(arr)}"
            )
