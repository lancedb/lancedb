# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Typed metadata previews and results of data-cache prewarm jobs."""

from dataclasses import dataclass, fields
import json
from typing import Optional


@dataclass(frozen=True)
class PrewarmDataResult:
    """Reported executor work for a pinned snapshot, not guaranteed cache residency.

    Counts omitted by older workers remain None. I/O counts are unknown when
    executors do not report them. Partial completions are a subset of completed
    executors and may reflect cache pressure.
    """

    table_version: int
    executors_targeted: Optional[int] = None
    executors_completed: Optional[int] = None
    executors_partial: Optional[int] = None
    fragments_selected: Optional[int] = None
    ranges_completed: Optional[int] = None
    bytes_read: Optional[int] = None

    @classmethod
    def from_json(cls, raw: str) -> "PrewarmDataResult":
        value = json.loads(raw)
        names = {field.name for field in fields(cls)}
        return cls(**{key: item for key, item in value.items() if key in names})


@dataclass(frozen=True)
class PrewarmPruningSummary:
    """Conservative metadata pruning; selected ranges are not exact row matches.

    ``mode`` is a server-reported string, including future pruning modes.
    Unknown interval counts and fallback reasons remain None.
    """

    mode: str
    input_fragments: int
    selected_fragments: int
    pruned_fragments: int
    selected_row_ranges: Optional[int] = None
    fallback_reason: Optional[str] = None


@dataclass(frozen=True)
class PrewarmDataPlan:
    """Metadata-only preview for a snapshot, without a warming job.

    A later submission chooses its own snapshot and can produce a different
    plan. ``plan`` is readable display text; use ``pruning`` for structured facts.
    """

    table_version: int
    plan: str
    pruning: PrewarmPruningSummary

    @classmethod
    def from_json(cls, raw: str) -> "PrewarmDataPlan":
        # The native SDK validates the wire format before returning this value.
        value = json.loads(raw)
        summary = value["pruning"]
        names = {field.name for field in fields(PrewarmPruningSummary)}
        return cls(
            table_version=value["table_version"],
            plan=value["plan"],
            pruning=PrewarmPruningSummary(
                **{key: item for key, item in summary.items() if key in names}
            ),
        )
