# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

# These tests assert the default resource values for Geneva KubeRay cluster nodes.
# If any of these values change, update the tables in:
#   docs/geneva/jobs/performance.mdx (the "Geneva defaults" section)

import pytest


def test_head_node_defaults():
    # If these change, update the Head Node table in performance.mdx
    from geneva.cluster.builder import KubeRayClusterBuilder

    builder = KubeRayClusterBuilder()
    assert builder._head_cpus == 4
    assert builder._head_memory == "8Gi"
    assert builder._head_gpus == 0
    assert builder._head_node_selector == {"geneva.lancedb.com/ray-head": "true"}
    assert builder._head_service_account == "geneva-service-account"


def test_cpu_worker_defaults():
    # If these change, update the CPU Workers table in performance.mdx
    from geneva.cluster.builder import CpuWorkerBuilder

    worker = CpuWorkerBuilder()
    assert worker._num_cpus == 4
    assert worker._memory == "8Gi"
    assert worker._node_selector == {"geneva.lancedb.com/ray-worker-cpu": "true"}
    assert worker._replicas == 1
    assert worker._min_replicas == 0
    assert worker._max_replicas == 100
    assert worker._idle_timeout_seconds == 60

    # Confirm build produces 0 GPUs
    config = worker.build()
    assert config.num_gpus == 0


def test_gpu_worker_defaults():
    # If these change, update the GPU Workers table in performance.mdx
    from geneva.cluster.builder import GpuWorkerBuilder

    worker = GpuWorkerBuilder()
    assert worker._num_cpus == 8
    assert worker._memory == "16Gi"
    assert worker._num_gpus == 1
    assert worker._node_selector == {"geneva.lancedb.com/ray-worker-gpu": "true"}
    assert worker._replicas == 1
    assert worker._min_replicas == 0
    assert worker._max_replicas == 100
    assert worker._idle_timeout_seconds == 60
