# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Typed previews and results of Enterprise Optimize data rewrites."""

from dataclasses import dataclass, fields
import json
from typing import List, Literal, Optional


@dataclass(frozen=True)
class OptimizeResult:
    """Actual committed outcome of one Enterprise Optimize data rewrite.

    ``rows_failed`` counts planned live rows that were not committed, including
    partial progress. ``more_work`` means a bounded run left eligible work for
    another invocation. File/fragment counts are None if an older worker did
    not report them; they are never inferred from unrelated table versions.
    File counts include deletion files as well as data files.
    """

    partial_commit: bool
    rows_succeeded: int
    rows_failed: int
    completion_percentage: float
    more_work: bool = False
    files_added: Optional[int] = None
    files_removed: Optional[int] = None
    fragments_added: Optional[int] = None
    fragments_removed: Optional[int] = None

    @classmethod
    def from_json(cls, raw: str) -> "OptimizeResult":
        """Decode a server result, ignoring additive fields from newer servers."""
        value = json.loads(raw)
        names = {field.name for field in fields(cls)}
        return cls(**{key: item for key, item in value.items() if key in names})


@dataclass(frozen=True)
class OptimizeResolvedOptions:
    """Effective rewrite settings after Enterprise policy resolution."""

    target_rows_per_fragment: int
    max_source_rows: Optional[int] = None
    io_buffer_size: Optional[int] = None
    batch_size: Optional[int] = None


@dataclass(frozen=True)
class OptimizePlan:
    """Data-rewrite preview without submitting a maintenance job.

    Counts describe planned work, not committed results, memory use, or runtime.
    A later submission plans its own snapshot and may select different work.
    """

    operation: Literal["rewrite_data"]
    table: str
    namespace: List[str]
    branch: str
    source_version: int
    effective_options: OptimizeResolvedOptions
    planned_groups: int
    planned_fragments: int
    planned_source_rows: int
    more_work: bool

    @classmethod
    def from_json(cls, raw: str) -> "OptimizePlan":
        # The native decoder validates the versioned server contract first.
        value = json.loads(raw)
        option_names = {field.name for field in fields(OptimizeResolvedOptions)}
        value["effective_options"] = OptimizeResolvedOptions(
            **{
                key: item
                for key, item in value["effective_options"].items()
                if key in option_names
            }
        )
        names = {field.name for field in fields(cls)}
        return cls(**{key: item for key, item in value.items() if key in names})
