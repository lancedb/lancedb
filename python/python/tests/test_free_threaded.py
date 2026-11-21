# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import sys
import sysconfig
import threading
from concurrent.futures import ThreadPoolExecutor

import pytest


def _require_free_threaded_runtime():
    if not sysconfig.get_config_var("Py_GIL_DISABLED"):
        pytest.skip("requires a free-threaded CPython interpreter")
    assert not sys._is_gil_enabled(), "the GIL was enabled before the test"


def test_native_import_does_not_enable_the_gil():
    _require_free_threaded_runtime()

    import lancedb
    import lancedb._lancedb

    assert not sys._is_gil_enabled()
    assert lancedb.__version__
    assert lancedb._lancedb.__version__


def test_free_threaded_database_operations_preserve_concurrent_appends(tmp_path):
    _require_free_threaded_runtime()

    import lancedb

    assert not sys._is_gil_enabled()
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "records",
        [{"id": -1, "vector": [1.0, 0.0], "payload": "seed"}],
    )

    worker_count = 12
    batches_per_worker = 10
    rows_per_batch = 5
    start = threading.Barrier(worker_count)

    def append_and_read(worker_id):
        start.wait(timeout=30)
        for batch_id in range(batches_per_worker):
            first_id = worker_id * batches_per_worker * rows_per_batch
            first_id += batch_id * rows_per_batch
            table.add(
                [
                    {
                        "id": first_id + offset,
                        "vector": [float(worker_id), 1.0],
                        "payload": f"worker-{worker_id}",
                    }
                    for offset in range(rows_per_batch)
                ]
            )
            assert table.count_rows() >= 1

    with ThreadPoolExecutor(max_workers=worker_count) as pool:
        list(pool.map(append_and_read, range(worker_count)))

    assert not sys._is_gil_enabled()
    assert table.count_rows() == 1 + worker_count * batches_per_worker * rows_per_batch
    assert table.search([1.0, 0.0]).limit(1).to_list()[0]["id"] == -1

    rows = table.to_arrow().to_pylist()
    ids = {row["id"] for row in rows}
    assert ids == {-1, *range(worker_count * batches_per_worker * rows_per_batch)}

    table.update(where="id = -1", values={"payload": "updated"})
    table.delete("id = 0")
    db.close()

    reopened = lancedb.connect(tmp_path)
    persisted = reopened.open_table("records")
    persisted_rows = persisted.to_arrow().to_pylist()
    persisted_by_id = {row["id"]: row for row in persisted_rows}

    assert len(persisted_rows) == worker_count * batches_per_worker * rows_per_batch
    assert persisted_by_id[-1]["payload"] == "updated"
    assert 0 not in persisted_by_id
    assert not sys._is_gil_enabled()
    reopened.close()
