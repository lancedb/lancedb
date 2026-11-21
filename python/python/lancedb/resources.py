# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from typing import Optional


def _resource_options(
    cpu: Optional[int], memory: Optional[int], gpu: Optional[int], *, prefix: str = ""
) -> dict[str, int]:
    result = {}
    for name, value, maximum, minimum in (
        ("total_cpu_millis", cpu, 2**64 - 1, 1),
        ("total_memory_bytes", memory, 2**64 - 1, 1),
        ("total_gpu_count", gpu, 2**32 - 1, 0),
    ):
        if value is None:
            continue
        if isinstance(value, bool) or not isinstance(value, int):
            raise ValueError(f"{prefix}{name} must be an integer")
        if not minimum <= value <= maximum:
            raise ValueError(f"{prefix}{name} must be between {minimum} and {maximum}")
        result[prefix + name] = value
    return result
