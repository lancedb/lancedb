# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from __future__ import annotations

import pyarrow as pa

from lancedb import udf


_RUNNING_DEADLINE_SECONDS = 30


@udf(
    inputs={"value": pa.int64()},
    output=pa.int64(),
    python="3.12",
    packages=["pyarrow==24.0.0"],
    output_nullable=True,
)
def reliable_double(value):
    if value is None:
        return None
    return value * 2


@udf(
    inputs={"value": pa.int64()},
    output=pa.int64(),
    python="3.12",
    packages=["pyarrow==24.0.0"],
    output_nullable=True,
)
def terminate_worker_on_input(value):
    if value is None:
        return None
    try:
        if len(value) == 0:
            return value
    except TypeError:
        pass

    import os

    os._exit(73)


@udf(
    inputs={"value": pa.int64()},
    output=pa.int64(),
    python="3.12",
    packages=["pyarrow==24.0.0"],
    output_nullable=False,
)
def slow_triple(value):
    import time

    time.sleep(0.02)
    return value * 3


def _require_live() -> str:
    import os

    import pytest

    host = os.environ.get("LANCEDB_FCF_E2E_HOST")
    if not host:
        pytest.skip(
            "LANCEDB_FCF_E2E_HOST is required for live enterprise reliability tests"
        )
    return host


def _job_timeout():
    from datetime import timedelta

    return timedelta(minutes=5)


def _query_timeout():
    from datetime import timedelta

    return timedelta(seconds=30)


def _connect():
    import os

    import lancedb

    return lancedb.connect(
        os.environ.get("LANCEDB_FCF_E2E_DB_URI", "db://fcf-e2e-local"),
        api_key=os.environ.get("LANCEDB_FCF_E2E_API_KEY", "fake"),
        host_override=_require_live(),
    )


def _run_names(case: str) -> tuple[str, str]:
    import uuid

    suffix = uuid.uuid4().hex[:12]
    return f"fcf_rel_{case}_{suffix}", f"fcf_rel.{case}_{suffix}"


def _read_rows(table, columns: list[str], row_count: int) -> list[dict]:
    return sorted(
        table.search()
        .select(columns)
        .limit(row_count)
        .to_list(timeout=_query_timeout()),
        key=lambda row: row["row_id"],
    )


def _emit_evidence(case: str, evidence: dict) -> None:
    import json

    print(
        json.dumps(
            {"case": case, **evidence},
            sort_keys=True,
            separators=(",", ":"),
        )
    )


def test_enterprise_reliability_core_lifecycle():
    import pytest

    import lancedb
    from lancedb.exceptions import FunctionError
    from lancedb.expr import col

    _require_live()
    table_name, function_name = _run_names("lifecycle")
    setup_db = _connect()
    setup_db.create_table(
        table_name,
        data=pa.Table.from_pylist(
            [
                {"row_id": 1, "value": 2},
                {"row_id": 2, "value": 5},
                {"row_id": 3, "value": None},
            ],
            schema=pa.schema(
                [
                    pa.field("row_id", pa.int64(), nullable=False),
                    pa.field("value", pa.int64(), nullable=True),
                ]
            ),
        ),
    )

    registration_job = setup_db.functions.register(function_name, reliable_double)
    registration_job_id = registration_job.id
    assert isinstance(registration_job_id, str) and registration_job_id
    registered = registration_job.wait(timeout=_job_timeout())
    assert type(registered) is lancedb.Function
    assert isinstance(registered.id, str) and registered.id
    with pytest.raises(AttributeError):
        registered.id = "mutated"

    catalog_reader = _connect()
    by_name = catalog_reader.functions.get(function_name)
    by_id = catalog_reader.functions.get_by_id(registered.id)
    expected_identity = (
        registered.id,
        (("value", pa.int64()),),
        pa.int64(),
        True,
    )
    for function in (registered, by_name, by_id):
        assert type(function) is lancedb.Function
        assert (
            function.id,
            function.parameters,
            function.output_type,
            function.output_nullable,
        ) == expected_identity

    table = catalog_reader.open_table(table_name)
    create_job = table.add_generated_column(
        "derived",
        registered(value=col("value")),
    )
    create_job_id = create_job.id
    assert isinstance(create_job_id, str) and create_job_id
    assert create_job.wait(timeout=_job_timeout()) is None

    complete_reader = _connect().open_table(table_name)
    complete_status = complete_reader.generated_column_status("derived")
    assert complete_status == "complete"
    initial_rows = _read_rows(
        complete_reader,
        ["row_id", "value", "derived"],
        3,
    )
    assert initial_rows == [
        {"row_id": 1, "value": 2, "derived": 4},
        {"row_id": 2, "value": 5, "derived": 10},
        {"row_id": 3, "value": None, "derived": None},
    ]

    complete_reader.update(where="row_id = 2", values={"value": 7})

    incomplete_reader = _connect().open_table(table_name)
    changed_rows = _read_rows(incomplete_reader, ["row_id", "value"], 3)
    assert changed_rows == [
        {"row_id": 1, "value": 2},
        {"row_id": 2, "value": 7},
        {"row_id": 3, "value": None},
    ]
    incomplete_status = incomplete_reader.generated_column_status("derived")
    assert incomplete_status == "incomplete"
    with pytest.raises(FunctionError) as raised:
        (
            incomplete_reader.search()
            .select(["row_id", "derived"])
            .limit(3)
            .to_list(timeout=_query_timeout())
        )
    assert raised.value.code == "generated_column_incomplete"

    refresh_job = incomplete_reader.refresh_generated_column("derived")
    refresh_job_id = refresh_job.id
    assert isinstance(refresh_job_id, str) and refresh_job_id
    assert refresh_job.wait(timeout=_job_timeout()) is None

    refreshed_reader = _connect().open_table(table_name)
    refreshed_status = refreshed_reader.generated_column_status("derived")
    assert refreshed_status == "complete"
    final_rows = _read_rows(
        refreshed_reader,
        ["row_id", "value", "derived"],
        3,
    )
    assert final_rows == [
        {"row_id": 1, "value": 2, "derived": 4},
        {"row_id": 2, "value": 7, "derived": 14},
        {"row_id": 3, "value": None, "derived": None},
    ]

    _emit_evidence(
        "core_lifecycle",
        {
            "final_rows": final_rows,
            "function_id": registered.id,
            "job_ids": {
                "create": create_job_id,
                "refresh": refresh_job_id,
                "register": registration_job_id,
            },
            "status": [
                complete_status,
                incomplete_status,
                refreshed_status,
            ],
            "table": table_name,
        },
    )


def test_enterprise_reliability_failure_atomicity_and_worker_recovery():
    import pytest

    import lancedb
    from lancedb.exceptions import JobFailedError
    from lancedb.expr import col

    _require_live()
    table_name, failing_function_name = _run_names("worker_failure")
    _, healthy_function_name = _run_names("worker_recovery")
    row_count = 4
    setup_db = _connect()
    setup_db.create_table(
        table_name,
        data=pa.table(
            {
                "row_id": list(range(row_count)),
                "value": [1, 2, 3, 4],
            }
        ),
    )

    registration_job = setup_db.functions.register(
        failing_function_name,
        terminate_worker_on_input,
    )
    failing_function = registration_job.wait(timeout=_job_timeout())
    assert type(failing_function) is lancedb.Function

    table = setup_db.open_table(table_name)
    failed_create_job = table.add_generated_column(
        "must_not_publish",
        failing_function(value=col("value")),
    )
    failed_job_id = failed_create_job.id
    assert isinstance(failed_job_id, str) and failed_job_id
    with pytest.raises(JobFailedError) as raised:
        failed_create_job.wait(timeout=_job_timeout())
    assert raised.value.error_code == "udf_execution_failure"

    first_description = _connect().get_job(failed_job_id)
    second_description = _connect().get_job(failed_job_id)
    for description in (first_description, second_description):
        assert description is not None
        assert description.job_id == failed_job_id
        assert description.state == "failed"
        assert description.failure is not None
        assert description.failure.error_code == "udf_execution_failure"

    atomic_reader = _connect().open_table(table_name)
    assert "must_not_publish" not in atomic_reader.schema.names
    assert _read_rows(atomic_reader, ["row_id", "value"], row_count) == [
        {"row_id": 0, "value": 1},
        {"row_id": 1, "value": 2},
        {"row_id": 2, "value": 3},
        {"row_id": 3, "value": 4},
    ]

    healthy_registration_job = setup_db.functions.register(
        healthy_function_name,
        reliable_double,
    )
    healthy_function = healthy_registration_job.wait(timeout=_job_timeout())
    assert type(healthy_function) is lancedb.Function
    recovery_job = atomic_reader.add_generated_column(
        "recovered",
        healthy_function(value=col("value")),
    )
    recovery_job_id = recovery_job.id
    assert isinstance(recovery_job_id, str) and recovery_job_id
    assert recovery_job.wait(timeout=_job_timeout()) is None

    recovered_reader = _connect().open_table(table_name)
    assert "must_not_publish" not in recovered_reader.schema.names
    assert recovered_reader.generated_column_status("recovered") == "complete"
    recovered_rows = _read_rows(
        recovered_reader,
        ["row_id", "value", "recovered"],
        row_count,
    )
    assert recovered_rows == [
        {"row_id": 0, "value": 1, "recovered": 2},
        {"row_id": 1, "value": 2, "recovered": 4},
        {"row_id": 2, "value": 3, "recovered": 6},
        {"row_id": 3, "value": 4, "recovered": 8},
    ]

    _emit_evidence(
        "failure_atomicity_and_worker_recovery",
        {
            "failure_code": first_description.failure.error_code,
            "failed_job_id": failed_job_id,
            "recovered_rows": recovered_rows,
            "recovery_job_id": recovery_job_id,
            "table": table_name,
        },
    )


def test_enterprise_reliability_concurrent_refresh_fencing():
    import time

    import pytest

    import lancedb
    from lancedb.exceptions import FunctionError, JobFailedError
    from lancedb.expr import col

    _require_live()
    table_name, function_name = _run_names("refresh_fencing")
    row_count = 1024
    setup_db = _connect()
    setup_db.create_table(
        table_name,
        data=pa.table(
            {
                "row_id": list(range(row_count)),
                "value": list(range(row_count)),
            }
        ),
    )

    registration_job = setup_db.functions.register(function_name, slow_triple)
    function = registration_job.wait(timeout=_job_timeout())
    assert type(function) is lancedb.Function

    table = setup_db.open_table(table_name)
    create_job = table.add_generated_column(
        "derived",
        function(value=col("value")),
    )
    assert create_job.wait(timeout=_job_timeout()) is None
    initial_reader = _connect().open_table(table_name)
    assert initial_reader.generated_column_status("derived") == "complete"

    initial_reader.update(where="row_id = 0", values={"value": 10_000})
    incomplete_reader = _connect().open_table(table_name)
    assert incomplete_reader.generated_column_status("derived") == "incomplete"

    refresh_job = incomplete_reader.refresh_generated_column("derived")
    refresh_job_id = refresh_job.id
    assert isinstance(refresh_job_id, str) and refresh_job_id
    deadline = time.monotonic() + _RUNNING_DEADLINE_SECONDS
    observed_states = []
    running_observations = 0
    while running_observations < 2:
        state = refresh_job.status()
        if not observed_states or observed_states[-1] != state:
            observed_states.append(state)
        if state == "running":
            running_observations += 1
        else:
            running_observations = 0
            assert state not in {"finished", "failed", "cancelled"}
        assert time.monotonic() < deadline
        if running_observations < 2:
            time.sleep(0.05)

    concurrent_writer = _connect().open_table(table_name)
    concurrent_writer.update(where="row_id = 1", values={"value": 20_000})
    with pytest.raises(JobFailedError) as raised:
        refresh_job.wait(timeout=_job_timeout())
    assert raised.value.error_code == "stale_or_conflicting_input"

    stale_job = _connect().get_job(refresh_job_id)
    assert stale_job is not None
    assert stale_job.job_id == refresh_job_id
    assert stale_job.state == "failed"
    assert stale_job.failure is not None
    assert stale_job.failure.error_code == raised.value.error_code
    if observed_states[-1] != stale_job.state:
        observed_states.append(stale_job.state)

    stale_reader = _connect().open_table(table_name)
    stale_rows = _read_rows(stale_reader, ["row_id", "value"], row_count)
    assert len(stale_rows) == row_count
    for row_id, row in enumerate(stale_rows):
        expected_value = 10_000 if row_id == 0 else 20_000 if row_id == 1 else row_id
        assert (row["row_id"], row["value"]) == (row_id, expected_value)
    assert stale_reader.generated_column_status("derived") == "incomplete"
    with pytest.raises(FunctionError) as incomplete:
        (
            stale_reader.search()
            .select(["row_id", "derived"])
            .limit(row_count)
            .to_list(timeout=_query_timeout())
        )
    assert incomplete.value.code == "generated_column_incomplete"

    resubmitted_job = stale_reader.refresh_generated_column("derived")
    resubmitted_job_id = resubmitted_job.id
    assert isinstance(resubmitted_job_id, str) and resubmitted_job_id
    assert resubmitted_job.wait(timeout=_job_timeout()) is None

    final_reader = _connect().open_table(table_name)
    final_status = final_reader.generated_column_status("derived")
    assert final_status == "complete"
    final_rows = _read_rows(
        final_reader,
        ["row_id", "value", "derived"],
        row_count,
    )
    assert len(final_rows) == row_count
    final_checksum = 0
    for row_id, row in enumerate(final_rows):
        expected_value = 10_000 if row_id == 0 else 20_000 if row_id == 1 else row_id
        assert (row["row_id"], row["value"], row["derived"]) == (
            row_id,
            expected_value,
            expected_value * 3,
        )
        final_checksum += row["derived"]

    _emit_evidence(
        "concurrent_refresh_fencing",
        {
            "failure_code": stale_job.failure.error_code,
            "final_checksum": final_checksum,
            "final_status": final_status,
            "observed_states": observed_states,
            "resubmitted_job_id": resubmitted_job_id,
            "row_count": row_count,
            "stale_job_id": refresh_job_id,
            "table": table_name,
        },
    )
