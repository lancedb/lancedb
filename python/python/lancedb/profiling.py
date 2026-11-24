# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Typed snapshots of on-demand Enterprise Function worker captures."""

import json
from dataclasses import dataclass
from typing import Literal, Optional

FunctionProfileKind = Literal["cpu", "gpu", "memory"]
FunctionProfileStatus = Literal["pending", "capturing", "complete", "failed"]

__all__ = ["FunctionProfile", "FunctionProfileKind", "FunctionProfileStatus"]


@dataclass(frozen=True)
class FunctionProfile:
    """A capture's identity, current status and artifact retention bounds.

    Retain ``profile_id`` to retrieve the capture through the parent Job,
    including after its refresh completes. ``status`` and ``error`` describe
    the capture independently of the parent refresh job. Timestamp fields are
    milliseconds since the Unix epoch. ``expires_at_millis`` is an upper bound:
    the server may reclaim job scratch or evict an older capture sooner.

    CPU evidence contains Python stacks weighted by thread CPU time. Memory
    evidence contains Python allocations made during capture and retained at
    its end. GPU evidence contains PyTorch CUDA activities; it excludes Python
    operator attribution and GPU allocation stacks.
    """

    profile_id: str
    job_id: str
    worker_id: str
    execution_epoch: int
    session_slot: int
    kind: FunctionProfileKind
    duration_seconds: float
    status: FunctionProfileStatus
    requested_at_millis: int
    deadline_at_millis: int
    expires_at_millis: int
    completed_at_millis: Optional[int] = None
    error: Optional[str] = None
    artifact_id: Optional[str] = None

    @classmethod
    def _from_json(cls, value: str) -> "FunctionProfile":
        return cls(**json.loads(value))
