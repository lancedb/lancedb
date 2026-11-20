# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import signal
import os
import subprocess
import sys
import threading
from datetime import timedelta

import pyarrow as pa
import pytest

import lancedb
from lancedb import background_loop
from lancedb.index import BTree, FTS, IvfFlat


def _direct_query_table(tmp_path):
    table = lancedb.connect(tmp_path).create_table(
        "direct",
        pa.table(
            {
                "id": pa.array([1, 2, 3, 4], pa.int64()),
                "text": ["alpha beta", "alpha gamma", "beta gamma", "delta epsilon"],
                "vector": pa.array(
                    [
                        [1.0, 0.0, 0.0, 0.0],
                        [0.0, 1.0, 0.0, 0.0],
                        [0.0, 0.0, 1.0, 0.0],
                        [0.0, 0.0, 0.0, 1.0],
                    ],
                    pa.list_(pa.float32(), 4),
                ),
            }
        ),
    )
    table.create_index("id", config=BTree())
    table.create_index("vector", config=IvfFlat(distance_type="l2", num_partitions=1))
    table.create_index("text", config=FTS(with_position=True))
    return table


def test_direct_sync_queries_do_not_use_background_loop(tmp_path, monkeypatch):
    table = _direct_query_table(tmp_path)
    expected_empty_schema = table.search().where("id < 0").output_schema()

    def fail(*_args, **_kwargs):
        raise AssertionError("LOOP.run must not be used by direct sync queries")

    monkeypatch.setattr(background_loop.LOOP, "run", fail)

    point = table.search().where("id = 1").select(["id", "text"]).to_arrow()
    assert point.to_pylist() == [{"id": 1, "text": "alpha beta"}]

    with_row_id = (
        table.search().where("id = 1").select(["id"]).with_row_id(True).to_arrow()
    )
    assert with_row_id["_rowid"].to_pylist() == [0]

    # The vector column is inferred from the sync schema binding.
    vector = table.search([0.9, 0.1, 0.0, 0.0]).limit(2).to_arrow()
    assert vector["id"].to_pylist() == [1, 2]
    assert "_distance" in vector.column_names

    fts = table.search("alpha", query_type="fts").select(["id"]).to_arrow()
    assert sorted(fts["id"].to_pylist()) == [1, 2]
    assert "_score" in fts.column_names

    phrase = table.search('"alpha beta"', query_type="fts").select(["id"]).to_arrow()
    assert phrase["id"].to_pylist() == [1]

    empty = table.search().where("id < 0").to_arrow()
    assert empty.num_rows == 0
    assert empty.schema == expected_empty_schema

    with pytest.raises(Exception, match="Query timeout"):
        table.search([1.0, 0.0, 0.0, 0.0]).to_arrow(timeout=timedelta(0))


@pytest.mark.skipif(sys.platform != "linux", reason="needs sched_setaffinity")
def test_direct_sync_query_from_single_worker_progress_callback(tmp_path):
    script = r"""
import os
import sys

cpus = sorted(os.sched_getaffinity(0))
if not cpus:
    print("SKIP")
    sys.exit(0)
os.sched_setaffinity(0, {cpus[0]})

import lancedb

db = lancedb.connect("memory:///")
target = db.create_table(
    "target",
    [{"id": 1, "value": "one"}, {"id": 2, "value": "two"}],
)
source = db.create_table("source", [{"id": 1}])
seen = []

def progress(_update):
    if not seen:
        seen.append(
            target.search()
            .where("id = 1")
            .select(["value"])
            .to_arrow()
            .to_pylist()
        )

source.add([{"id": 2}], progress=progress)
assert seen == [[{"value": "one"}]], seen
print("OK")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if "SKIP" in result.stdout:
        pytest.skip("sched_setaffinity did not leave a usable CPU")
    assert result.returncode == 0, result.stdout + result.stderr
    assert "OK" in result.stdout


@pytest.mark.slow
def test_direct_sync_query_sigint_aborts_and_reuses_runtime(tmp_path):
    rows = 500_000
    table = lancedb.connect(tmp_path).create_table(
        "sigint",
        pa.table(
            {
                "id": pa.array(range(rows), pa.int64()),
                "text": pa.array([f"row-{row}" for row in range(rows)]),
            }
        ),
    )
    # Warm the runtime and page cache so the timed call is long enough to observe.
    table.search().select(["id"]).to_arrow()

    timer = threading.Timer(0.001, os.kill, args=(os.getpid(), signal.SIGINT))
    timer.start()
    try:
        with pytest.raises(KeyboardInterrupt):
            table.search().select(["id", "text"]).to_arrow()
    finally:
        timer.cancel()
        timer.join()

    # The shared runtime and its generation counter remain usable.
    after = table.search().where("id = 1").select(["id", "text"]).to_arrow()
    assert after.to_pylist() == [{"id": 1, "text": "row-1"}]
