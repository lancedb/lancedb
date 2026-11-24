# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import re
from typing import Any, Optional, Union


def _validate_gpu_class_name(value: Any) -> str:
    if not isinstance(value, str) or not re.fullmatch(
        r"[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?", value
    ):
        raise ValueError(
            "gpu_class must be a lowercase name of at most 63 letters, digits, or "
            "hyphens, and cannot start or end with a hyphen"
        )
    return value


def _resource_options(
    cpu: Optional[int],
    memory: Optional[int],
    gpu: Optional[int],
    *,
    gpu_class: Optional[str] = None,
    gpu_count: Optional[int] = None,
) -> dict[str, Union[int, str]]:
    result: dict[str, Union[int, str]] = {}
    for name, value, maximum, minimum in (
        ("total_cpu_millis", cpu, 2**64 - 1, 1),
        ("total_memory_bytes", memory, 2**64 - 1, 1),
        ("total_gpu_count", gpu, 2**32 - 1, 0),
        ("gpu_count", gpu_count, 2**32 - 1, 1),
    ):
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{name} must be an integer")
        if not minimum <= value <= maximum:
            raise ValueError(f"{name} must be between {minimum} and {maximum}")
        result[name] = value
    if gpu_class is not None:
        result["gpu_class"] = _validate_gpu_class_name(gpu_class)
    if gpu is not None and (
        (gpu_count is not None and gpu_count > gpu)
        or (gpu_class is not None and gpu == 0)
    ):
        raise ValueError("total_gpu_count cannot fit one Function worker")
    return result
