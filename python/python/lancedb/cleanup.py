# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Results of durable remote version cleanup."""

from dataclasses import dataclass, fields
import json
from typing import Optional


@dataclass(frozen=True)
class CleanupResult:
    """Version-deletion outcome, excluding best-effort auxiliary sweeps.

    Unknown totals after retry/recovery are None, never fabricated zeroes.
    Completion does not guarantee that every orphaned file was reclaimed.
    """

    format_version: int
    min_age_seconds: int
    statistics_complete: bool
    old_versions: Optional[int] = None
    bytes_removed: Optional[int] = None
    failed_deletes: Optional[int] = None

    @classmethod
    def from_json(cls, raw: str) -> "CleanupResult":
        """Decode the versioned contract, preserving unknown measurements."""
        value = json.loads(raw)
        if (
            not isinstance(value, dict)
            or type(value.get("format_version")) is not int
            or value["format_version"] != 1
        ):
            raise ValueError("unsupported cleanup result format")
        if type(value.get("statistics_complete")) is not bool:
            raise ValueError("invalid cleanup result statistics_complete")
        for key in (
            "min_age_seconds",
            "old_versions",
            "bytes_removed",
            "failed_deletes",
        ):
            count = value.get(key)
            if count is None and key != "min_age_seconds":
                continue
            if type(count) is not int or count < 0:
                raise ValueError(f"invalid cleanup result {key}")
        counts = [value.get("old_versions"), value.get("bytes_removed")]
        if not value["statistics_complete"] and value.get("failed_deletes") is not None:
            raise ValueError("inconsistent cleanup failed-deletion measurement")
        if (
            value["statistics_complete"] and any(count is None for count in counts)
        ) or (
            not value["statistics_complete"]
            and any(count is not None for count in counts)
        ):
            raise ValueError("inconsistent cleanup result statistics")
        names = {field.name for field in fields(cls)}
        return cls(**{key: item for key, item in value.items() if key in names})


@dataclass(frozen=True)
class CleanupPlan:
    """Read-only version-deletion estimates, excluding auxiliary sweeps.

    No job or deletion snapshot is reserved. Execution plans current state again.
    """

    format_version: int
    source_version: int
    min_age_seconds: int
    cutoff_millis: int
    versions_to_remove: int
    bytes_to_remove: int

    @classmethod
    def from_json(cls, raw: str) -> "CleanupPlan":
        value = json.loads(raw)
        if not isinstance(value, dict) or value.get("format_version") != 1:
            raise ValueError("unsupported cleanup preview format")
        names = {field.name for field in fields(cls)}
        for name in names:
            item = value.get(name)
            if type(item) is not int or (name != "cutoff_millis" and item < 0):
                raise ValueError(f"invalid cleanup preview {name}")
        return cls(**{key: item for key, item in value.items() if key in names})


def _validate_min_age_seconds(value: Optional[int]) -> None:
    if value is not None and (type(value) is not int or value < 0):
        raise ValueError("min_age_seconds must be a nonnegative integer or None")
