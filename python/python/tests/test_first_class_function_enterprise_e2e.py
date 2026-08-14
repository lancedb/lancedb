# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from __future__ import annotations

import pyarrow as pa

from lancedb import udf


@udf(
    inputs={"value": pa.int64()},
    output=pa.int64(),
    python="3.12",
    packages=["pyarrow==24.0.0"],
    output_nullable=True,
)
def double_nullable(value):
    if value is None:
        return None
    return value * 2


def test_first_class_function_enterprise_lifecycle():
    import json
    import os
    import uuid
    from datetime import timedelta

    import pytest

    import lancedb
    from lancedb.exceptions import FunctionError
    from lancedb.expr import col

    host = os.environ.get("LANCEDB_FCF_E2E_HOST")
    if not host:
        pytest.skip("LANCEDB_FCF_E2E_HOST is required for the live enterprise test")

    database_uri = os.environ.get("LANCEDB_FCF_E2E_DB_URI", "db://fcf-e2e-local")
    api_key = os.environ.get("LANCEDB_FCF_E2E_API_KEY", "fake")
    run_suffix = uuid.uuid4().hex[:12]
    table_name = f"fcf_e2e_{run_suffix}"
    function_name = f"fcf_e2e.double_{run_suffix}"
    job_timeout = timedelta(minutes=5)
    query_timeout = timedelta(seconds=30)

    def connect():
        return lancedb.connect(
            database_uri,
            api_key=api_key,
            host_override=host,
        )

    setup_db = connect()
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

    registration_job = setup_db.functions.register(function_name, double_nullable)
    registration_job_id = registration_job.id
    assert isinstance(registration_job_id, str) and registration_job_id
    registered_function = registration_job.wait(timeout=job_timeout)
    assert type(registered_function) is lancedb.Function
    assert isinstance(registered_function.id, str) and registered_function.id
    with pytest.raises(AttributeError):
        registered_function.id = "mutated"

    catalog_reader = connect()
    function_by_name = catalog_reader.functions.get(function_name)
    function_by_id = catalog_reader.functions.get_by_id(registered_function.id)
    expected_signature = ((("value", pa.int64()),), pa.int64(), True)
    expected_identity = (
        registered_function.id,
        *expected_signature,
    )
    for function in (registered_function, function_by_name, function_by_id):
        assert type(function) is lancedb.Function
        assert (
            function.id,
            function.parameters,
            function.output_type,
            function.output_nullable,
        ) == expected_identity

    generated_column_table = catalog_reader.open_table(table_name)
    generated_column_job = generated_column_table.add_generated_column(
        "derived",
        registered_function(value=col("value")),
    )
    generated_column_job_id = generated_column_job.id
    assert isinstance(generated_column_job_id, str) and generated_column_job_id
    assert generated_column_job.wait(timeout=job_timeout) is None

    complete_reader = connect().open_table(table_name)
    complete_status = complete_reader.generated_column_status("derived")
    assert complete_status == "complete"
    initial_rows = sorted(
        complete_reader.search()
        .select(["row_id", "value", "derived"])
        .limit(3)
        .to_list(timeout=query_timeout),
        key=lambda row: row["row_id"],
    )
    assert initial_rows == [
        {"row_id": 1, "value": 2, "derived": 4},
        {"row_id": 2, "value": 5, "derived": 10},
        {"row_id": 3, "value": None, "derived": None},
    ]

    update_result = complete_reader.update(
        where="row_id = 2",
        values={"value": 7},
    )
    assert update_result.rows_updated == 1

    incomplete_reader = connect().open_table(table_name)
    incomplete_status = incomplete_reader.generated_column_status("derived")
    assert incomplete_status == "incomplete"
    with pytest.raises(FunctionError) as raised:
        (
            incomplete_reader.search()
            .select(["row_id", "derived"])
            .limit(3)
            .to_list(timeout=query_timeout)
        )
    assert raised.value.code == "generated_column_incomplete"

    refresh_job = incomplete_reader.refresh_generated_column("derived")
    refresh_job_id = refresh_job.id
    assert isinstance(refresh_job_id, str) and refresh_job_id
    assert refresh_job.wait(timeout=job_timeout) is None

    refreshed_reader = connect().open_table(table_name)
    refreshed_status = refreshed_reader.generated_column_status("derived")
    assert refreshed_status == "complete"
    final_rows = sorted(
        refreshed_reader.search()
        .select(["row_id", "value", "derived"])
        .limit(3)
        .to_list(timeout=query_timeout),
        key=lambda row: row["row_id"],
    )
    assert final_rows == [
        {"row_id": 1, "value": 2, "derived": 4},
        {"row_id": 2, "value": 7, "derived": 14},
        {"row_id": 3, "value": None, "derived": None},
    ]

    evidence = {
        "run_suffix": run_suffix,
        "database": database_uri.removeprefix("db://"),
        "table": table_name,
        "function": function_name,
        "function_id": registered_function.id,
        "job_ids": {
            "register": registration_job_id,
            "add_generated_column": generated_column_job_id,
            "refresh_generated_column": refresh_job_id,
        },
        "status_transitions": [
            complete_status,
            incomplete_status,
            refreshed_status,
        ],
        "final_rows": final_rows,
    }
    print(json.dumps(evidence, sort_keys=True, separators=(",", ":")))
