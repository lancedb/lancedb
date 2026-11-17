# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Results of remote data-cache prewarm jobs."""

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
