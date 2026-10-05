# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Examples for docs/web/management/function.mdx.

Registering a Function and filling its column need a LanceDB Enterprise
deployment, so `test_function_enterprise` runs only when the connection
variables the Enterprise examples already use are set: LANCEDB_URI,
LANCEDB_API_KEY and LANCEDB_HOST_OVERRIDE, plus LANCEDB_REGION if the
deployment needs one. It creates the table `function_demo` and a version of
the Function `double` in that database, and drops both when it finishes.

The other tests run locally and check the client side of the same code: what
`@udf` sends for registration, how a registered version binds a column, and
where a local connection stops. They do not run a Function remotely.
"""

import json
import os
import sys
from pathlib import Path

import lancedb
import pytest
from lancedb import col
from lancedb.functions import FunctionApplication, FunctionVersion

# --8<-- [start:function_define]
from lancedb import udf


@udf
def double(value: float) -> float:
    return value * 2


# --8<-- [end:function_define]

# A Function version as the service returns it after registration: the SDK's
# shared golden reply, with the name and signature of the definition above.
SERVICE_REPLY = (
    Path(__file__).parents[3]
    / "rust"
    / "lancedb"
    / "tests"
    / "fixtures"
    / "first_class_functions"
    / "v1"
    / "remote_function_job.json"
)


def registered_version(definition) -> FunctionVersion:
    request = definition.registration_request
    reply = json.loads(SERVICE_REPLY.read_text())["result"]
    reply["name"] = request.name
    reply["signature"] = json.loads(request.signature.to_canonical_json())
    return FunctionVersion.from_json(json.dumps(reply))


def require_env(var_name: str) -> str:
    """Skip the test unless the required environment variable is provided."""
    value = os.environ.get(var_name)
    if not value:
        pytest.skip(f"Set {var_name} to run this Enterprise example")
    return value


def test_function_runs_locally_as_python():
    # --8<-- [start:function_local_call]
    print(double(1.5))
    # 3.0
    # --8<-- [end:function_local_call]
    assert double(1.5) == 3.0


def test_function_registration_request():
    request = double.registration_request
    assert request.name == "double"
    assert [
        (parameter.name, parameter.arrow_type, parameter.nullable)
        for parameter in request.signature.inputs
    ] == [("value", "float64", False)]
    output = request.signature.output
    assert (output.kind, output.arrow_type, output.nullable) == (
        "scalar",
        "float64",
        False,
    )
    version = request.runtime.python_version
    assert version == f"{sys.version_info.major}.{sys.version_info.minor}"
    assert request.runtime.environment.packages == ()


def test_function_registration_is_remote(tmp_path):
    db = lancedb.connect(tmp_path)
    with pytest.raises(NotImplementedError):
        db.create_function(double)


def test_function_version_binds_a_column(tmp_path):
    version = registered_version(double)

    application = version(value=col("value"))

    assert isinstance(application, FunctionApplication)
    assert application.function.name == "double"
    assert application.function.version == version.version
    assert [
        (binding.parameter, binding.kind, binding.value["path"])
        for binding in application.inputs
    ] == [("value", "column", "value")]
    with pytest.raises(TypeError, match="direct col"):
        version(value=1.5)
    with pytest.raises(TypeError, match="missing"):
        version(x=col("value"))

    # The binding is accepted by add_columns, which a local table refuses:
    # a Function column needs LanceDB Cloud or Enterprise.
    table = lancedb.connect(tmp_path).create_table("t", [{"value": 1.0}])
    with pytest.raises(NotImplementedError, match="Cloud and Enterprise"):
        table.add_columns({"doubled": application})


def test_function_enterprise():
    require_env("LANCEDB_URI")
    require_env("LANCEDB_API_KEY")
    require_env("LANCEDB_HOST_OVERRIDE")

    # --8<-- [start:function_connect]
    import os

    import lancedb

    db = lancedb.connect(
        uri=os.environ["LANCEDB_URI"],  # db://your-database
        api_key=os.environ["LANCEDB_API_KEY"],
        region=os.environ.get("LANCEDB_REGION", "us-east-1"),
        host_override=os.environ["LANCEDB_HOST_OVERRIDE"],
    )
    # --8<-- [end:function_connect]

    table = None
    version = None
    try:
        # --8<-- [start:function_create_table]
        import pyarrow as pa

        schema = pa.schema([pa.field("value", pa.float64(), nullable=False)])
        table = db.create_table(
            "function_demo",
            [{"value": 1.0}, {"value": 2.0}, {"value": 3.0}],
            schema=schema,
            mode="overwrite",
        )
        # --8<-- [end:function_create_table]

        # --8<-- [start:function_register]
        job = db.create_function_async(double)
        version = job.wait()
        # --8<-- [end:function_register]
        assert version.name == "double"

        # --8<-- [start:function_apply]
        from lancedb import col

        table.add_columns({"doubled": version(value=col("value"))})
        # --8<-- [end:function_apply]

        # --8<-- [start:function_refresh]
        refresh = table.refresh_column_async("doubled")
        result = refresh.wait()
        print(result.rows_assigned)
        # 3
        # --8<-- [end:function_refresh]
        assert result.rows_assigned == 3
        assert result.rows_failed == 0

        # --8<-- [start:function_inspect]
        rows = table.search().select(["value", "doubled"]).to_list()
        print(sorted((row["value"], row["doubled"]) for row in rows))
        # [(1.0, 2.0), (2.0, 4.0), (3.0, 6.0)]
        # --8<-- [end:function_inspect]
        assert sorted((row["value"], row["doubled"]) for row in rows) == [
            (1.0, 2.0),
            (2.0, 4.0),
            (3.0, 6.0),
        ]
    finally:
        if table is not None:
            db.drop_table("function_demo")
        if version is not None:
            db.drop_function(version.name, version=version.version)
