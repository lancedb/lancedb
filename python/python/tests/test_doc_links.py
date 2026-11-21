# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb
from lancedb._lancedb import __version__ as _core_version, _append_doc_url, _doc_url
from lancedb.db import DBConnection
from lancedb.job import AsyncJob, Job
import pytest


def test_python_sdk_utm_content_and_doc_links(tmp_path):
    compaction_url = _doc_url("compaction")
    expected_content = f"utm_content=python-{_core_version}"
    assert (
        compaction_url
        == f"https://docs.lancedb.com/maintenance/compaction?utm_source=lancedb_sdk&utm_medium=warning&utm_campaign=compaction&{expected_content}#when"
    )

    db = lancedb.connect(tmp_path)
    with pytest.raises(NotImplementedError) as sql_exc:
        db.execute_query("SELECT 1")
    assert _doc_url("sql") in str(sql_exc.value)

    with pytest.raises(NotImplementedError) as gpu_exc:
        db.list_gpu_classes()
    assert _doc_url("gpu_classes") in str(gpu_exc.value)

    with pytest.raises(NotImplementedError) as job_exc:
        db.open_job("job-1")
    assert _doc_url("jobs") in str(job_exc.value)

    with pytest.raises(NotImplementedError) as secret_exc:
        db.create_secret("s", "v")
    assert _doc_url("secrets") in str(secret_exc.value)

    with pytest.raises(NotImplementedError) as view_exc:
        db.create_view("v", "SELECT 1")
    assert _doc_url("views") in str(view_exc.value)

    with pytest.raises(NotImplementedError) as mv_exc:
        DBConnection.open_materialized_view(db, "mv")
    assert _doc_url("materialized_views") in str(mv_exc.value)

    with pytest.raises(NotImplementedError) as create_mv_exc:
        DBConnection.create_materialized_view(db, "mv", "SELECT 1")
    assert _doc_url("materialized_views") in str(create_mv_exc.value)

    local_job = Job(AsyncJob(None))
    with pytest.raises(NotImplementedError) as events_exc:
        local_job.events()
    assert _doc_url("jobs") in str(events_exc.value)

    appended = _append_doc_url("Something failed.", "sql")
    assert appended.endswith(f"See {_doc_url('sql')}")
