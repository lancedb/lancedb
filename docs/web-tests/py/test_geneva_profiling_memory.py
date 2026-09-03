# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Snippets for docs/geneva/udfs/profiling-memory.mdx."""

from unittest.mock import MagicMock


def test_stateful_udf_class():
    import geneva
    import pyarrow as pa

    load_model = MagicMock(return_value=MagicMock(embed=MagicMock(return_value=[0.1] * 512)))

    # --8<-- [start:stateful_udf_class]
    @geneva.udf(data_type=pa.list_(pa.float32(), 512))
    class MyEmbedding:
        def __init__(self):
            self.model = None

        def setup(self):
            self.model = load_model()  # allocated once per actor

        def __call__(self, text: str) -> list[float]:
            if self.model is None:
                self.setup()
            return self.model.embed(text)
    # --8<-- [end:stateful_udf_class]

    assert MyEmbedding is not None


def test_memray_tracker_udf(monkeypatch):
    import sys

    load_model = MagicMock(return_value=MagicMock(embed=MagicMock(return_value=[0.1] * 512)))
    monkeypatch.setitem(sys.modules, "memray", MagicMock())

    # --8<-- [start:memray_tracker_udf]
    import os, pathlib, uuid
    from typing import Any
    import memray
    import geneva
    import pyarrow as pa

    _MEMRAY_OUT_DIR_ENV = "MY_UDF_MEMRAY_OUT_DIR"


    @geneva.udf(data_type=pa.list_(pa.float32(), 512))
    class MyEmbedding:
        def __init__(self):
            self.model = None
            self._tracker: Any = None  # memray.Tracker, when profiling is on

        def setup(self):
            # Open a memray tracker per worker process, if requested. Each
            # worker writes its own .bin file so traces don't collide.
            out_dir = os.environ.get(_MEMRAY_OUT_DIR_ENV)
            if out_dir:
                pathlib.Path(out_dir).mkdir(parents=True, exist_ok=True)
                bin_path = pathlib.Path(out_dir) / (
                    f"memray-{os.getpid()}-{uuid.uuid4().hex}.bin"
                )
                self._tracker = memray.Tracker(
                    str(bin_path), native_traces=False, follow_fork=False
                )
                self._tracker.__enter__()
            self.model = load_model()

        def __call__(self, text: str) -> list[float]:
            if self.model is None:
                self.setup()
            return self.model.embed(text)
    # --8<-- [end:memray_tracker_udf]

    assert MyEmbedding is not None


def test_ray_cluster_profile(monkeypatch):
    from contextlib import contextmanager

    table = MagicMock()

    @contextmanager
    def _mock_cluster(*args, **kwargs):
        yield

    monkeypatch.setattr("geneva.runners.ray._mgr.ray_cluster", _mock_cluster)

    # --8<-- [start:ray_cluster_profile]
    from geneva.runners.ray._mgr import ray_cluster

    with ray_cluster(
        local=True,
        extra_env={"MY_UDF_MEMRAY_OUT_DIR": "/tmp/my-udf-profile"},
    ):
        table.backfill("embedding", concurrency=1)
    # --8<-- [end:ray_cluster_profile]

    table.backfill.assert_called_once_with("embedding", concurrency=1)


def test_log_memory(capsys):
    # --8<-- [start:log_memory]
    import resource, pyarrow as pa

    def log_memory(seq: int) -> None:
        rss_bytes = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        # ru_maxrss is bytes on macOS, KiB on Linux:
        import sys
        if sys.platform != "darwin":
            rss_bytes *= 1024
        arrow_live = pa.total_allocated_bytes()
        print(
            f"seq={seq} "
            f"rss_mb={rss_bytes // 1024**2} "
            f"arrow_live_mb={arrow_live // 1024**2} "
            f"gap_mb={(rss_bytes - arrow_live) // 1024**2}",
            flush=True,
        )
    # --8<-- [end:log_memory]

    log_memory(0)
    captured = capsys.readouterr()
    assert "seq=0" in captured.out
    assert "rss_mb=" in captured.out


def test_leaky_cache():
    # --8<-- [start:leaky_cache]
    class BadEmbedding:
        def __init__(self):
            self.cache: dict[str, list[float]] = {}

        def __call__(self, text: str) -> list[float]:
            if text not in self.cache:
                self.cache[text] = self.model.embed(text)
            return self.cache[text]
    # --8<-- [end:leaky_cache]

    obj = BadEmbedding()
    obj.model = MagicMock(embed=MagicMock(return_value=[0.1, 0.2]))
    obj("hello")
    obj("hello")
    assert obj.model.embed.call_count == 1  # second call served from cache
    assert len(obj.cache) == 1


def test_bounded_cache():
    load_model = MagicMock(return_value=MagicMock(embed=MagicMock(return_value=[0.1, 0.2])))

    # --8<-- [start:bounded_cache]
    from functools import lru_cache

    class GoodEmbedding:
        def __init__(self):
            self._embed = None

        def setup(self):
            model = load_model()
            self._embed = lru_cache(maxsize=1024)(model.embed)

        def __call__(self, text: str) -> list[float]:
            if self._embed is None:
                self.setup()
            return self._embed(text)
    # --8<-- [end:bounded_cache]

    obj = GoodEmbedding()
    assert obj("hello") == [0.1, 0.2]
    load_model.assert_called_once()


def test_leaky_aggregator():
    import pyarrow as pa

    # --8<-- [start:leaky_aggregator]
    class BadAggregator:
        def __init__(self):
            self.history = []

        def __call__(self, batch: pa.RecordBatch) -> pa.Array:
            self.history.append(batch)  # holds every batch ever processed
            ...
    # --8<-- [end:leaky_aggregator]

    obj = BadAggregator()
    assert isinstance(obj.history, list)


def test_leaky_closure():
    import pyarrow as pa

    def expensive(x):
        return x

    # --8<-- [start:leaky_closure]
    class BadDeferred:
        def __init__(self):
            self.work_queue = []

        def __call__(self, x: pa.Array) -> pa.Array:
            # Lambda captures `x` by reference — the whole Array stays alive
            self.work_queue.append(lambda: expensive(x))
            ...
    # --8<-- [end:leaky_closure]

    obj = BadDeferred()
    assert isinstance(obj.work_queue, list)


def test_torch_inference_mode(monkeypatch):
    import sys

    mock_torch = MagicMock()
    monkeypatch.setitem(sys.modules, "torch", mock_torch)

    class TorchUDF:
        def __init__(self):
            self.model = MagicMock(encode=MagicMock(return_value=[0.1]))

        # --8<-- [start:torch_inference_mode]
        def __call__(self, text: str) -> list[float]:
            with torch.inference_mode():            # <-- prevents autograd graph retention
                return self.model.encode(text)
        # --8<-- [end:torch_inference_mode]

    import torch  # resolves to monkeypatched mock above
    obj = TorchUDF()
    assert obj("hello") == [0.1]


def test_confidence_check():
    class DebuggingUDF:
        def __init__(self):
            self._scratches = []

        # --8<-- [start:confidence_check]
        def __call__(self, x):
            scratch = bytearray(8 * 1024 * 1024)  # 8 MiB
            self._scratches.append(scratch)        # <-- deliberate leak
            return ...
        # --8<-- [end:confidence_check]

    obj = DebuggingUDF()
    obj(None)
    assert len(obj._scratches) == 1
    assert len(obj._scratches[0]) == 8 * 1024 * 1024
