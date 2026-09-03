# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os

import pytest


def test_quick_fix_manifest():
    # --8<-- [start:quick_fix_manifest]
    from geneva.manifest.builder import PipManifestBuilder

    manifest = PipManifestBuilder.create("fix").pip(["numpy==1.26.4"]).build()
    # --8<-- [end:quick_fix_manifest]
    assert manifest.pip == ["numpy==1.26.4"]


def test_env_vars_via_cluster(monkeypatch):
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key-id")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")

    # --8<-- [start:env_vars_via_cluster]
    from geneva.cluster.builder import KubeRayClusterBuilder
    import os

    cluster = (
        KubeRayClusterBuilder.create("my-cluster")
        .ray_init_kwargs({
            "runtime_env": {
                "env_vars": {
                    "AWS_ACCESS_KEY_ID": os.environ["AWS_ACCESS_KEY_ID"],
                    "AWS_SECRET_ACCESS_KEY": os.environ["AWS_SECRET_ACCESS_KEY"],
                }
            }
        })
        .build()
    )
    # --8<-- [end:env_vars_via_cluster]
    assert cluster.kuberay.ray_init_kwargs["runtime_env"]["env_vars"]["AWS_ACCESS_KEY_ID"] == "test-key-id"


def test_pip_manifest(monkeypatch):
    import geneva
    from unittest.mock import MagicMock, create_autospec
    mock_conn = create_autospec(geneva.db.Connection, instance=True)
    monkeypatch.setattr("geneva.connect", MagicMock(return_value=mock_conn))

    # --8<-- [start:pip_manifest]
    import geneva
    from geneva.manifest.builder import PipManifestBuilder

    manifest = (
        PipManifestBuilder.create("my-manifest")
        .pip([
            "numpy==1.26.4",
            "torch==2.0.1",
            "attrs==23.2.0",
        ])
        .build()
    )

    conn = geneva.connect("s3://my-bucket/my-db")
    conn.define_manifest("my-manifest", manifest)
    with conn.context(cluster="my-cluster", manifest="my-manifest"):
        conn.open_table("my-table").backfill("my-column")
    # --8<-- [end:pip_manifest]
    assert "numpy==1.26.4" in manifest.pip


def test_conda_cluster_path():
    # --8<-- [start:conda_cluster_path]
    from geneva.cluster.builder import KubeRayClusterBuilder

    cluster = (
        KubeRayClusterBuilder.create("my-cluster")
        .ray_init_kwargs({
            "runtime_env": {"conda": "environment.yml"}
        })
        .build()
    )
    # --8<-- [end:conda_cluster_path]
    assert cluster.kuberay.ray_init_kwargs["runtime_env"]["conda"] == "environment.yml"


def test_conda_cluster_inline():
    # --8<-- [start:conda_cluster_inline]
    from geneva.cluster.builder import KubeRayClusterBuilder

    cluster = (
        KubeRayClusterBuilder.create("my-cluster")
        .ray_init_kwargs({
            "runtime_env": {
                "conda": {
                    "channels": ["conda-forge"],
                    "dependencies": [
                        "python=3.10",
                        "ffmpeg<8",
                        "torchvision=0.22.1",
                    ],
                },
                "config": {"eager_install": True},
            }
        })
        .build()
    )
    # --8<-- [end:conda_cluster_inline]
    assert cluster.kuberay.ray_init_kwargs["runtime_env"]["conda"]["channels"] == ["conda-forge"]
