# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Results of durable remote compaction jobs."""

from dataclasses import dataclass, fields
import json
from typing import Optional


@dataclass(frozen=True)
class CompactionResult:
    """Actual committed outcome of one remote compaction invocation.

    ``rows_failed`` counts planned live rows that were not committed, including
    partial progress. ``more_work`` means a bounded run left eligible work for
    another invocation. File/fragment counts are None if an older worker did
    not report them; they are never inferred from unrelated table versions.
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
    def from_json(cls, raw: str) -> "CompactionResult":
        """Decode a server result, ignoring additive fields from newer servers."""
        value = json.loads(raw)
        names = {field.name for field in fields(cls)}
        return cls(**{key: item for key, item in value.items() if key in names})
