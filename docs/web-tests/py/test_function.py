# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Examples for docs/web/management/function.mdx.

Registering a Function and filling its column need a LanceDB Enterprise
deployment, so `test_function_enterprise` runs only when
LANCEDB_DOCS_TEST_DEPLOYMENT=1 asks for one, with the connection variables the
Enterprise examples already use: LANCEDB_URI, LANCEDB_API_KEY and
LANCEDB_HOST_OVERRIDE, plus LANCEDB_REGION if the deployment needs one. The
walkthrough calls its table `function_demo` and its Function `double`; the test
creates both under names unique to its run, never replaces an existing object,
and drops only what it created.

The other tests run locally and check the client side of the same code: what
`@udf` sends for registration, how a registered version binds a column, where a
local connection stops, and when the Enterprise test connects. They do not run
a Function remotely.
"""

import json
import os
import sys
import uuid
from pathlib import Path

import lancedb
import pyarrow as pa
import pytest
from lancedb import col
from lancedb.functions import FunctionApplication, FunctionVersion

# --8<-- [start:function_define]
from lancedb import udf


@udf
def double(value: float) -> float:
    return value * 2


# --8<-- [end:function_define]

# The definition the walkthrough registers. The Enterprise test registers a copy
# of it under a name that only its run uses.
WALKTHROUGH_DOUBLE = double


def describe(value: float):
    return {"half": value / 2, "note": None}


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


DEPLOYMENT = ("LANCEDB_URI", "LANCEDB_HOST_OVERRIDE", "LANCEDB_API_KEY")
# The connection settings alone never send this test to a deployment.
OPT_IN = "LANCEDB_DOCS_TEST_DEPLOYMENT"


def require_deployment():
    if os.environ.get(OPT_IN) != "1":
        pytest.skip(f"set {OPT_IN}=1 and {', '.join(DEPLOYMENT)} to use a deployment")
    missing = [name for name in DEPLOYMENT if not os.environ.get(name)]
    if missing:
        pytest.fail(f"{OPT_IN}=1 needs {', '.join(missing)}")


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


def test_function_output_nullability():
    inputs = pa.schema([pa.field("value", pa.float64(), nullable=False)])

    # A named-struct output may declare nullable fields.
    fields = [
        pa.field("half", pa.float64(), nullable=False),
        pa.field("note", pa.string(), nullable=True),
    ]
    struct = udf(input_schema=inputs, output_schema=pa.schema(fields))(describe)
    output = struct.registration_request.signature.output
    assert output.kind == "named_struct"
    assert [(field.name, field.nullable) for field in output.fields] == [
        ("half", False),
        ("note", True),
    ]

    # A scalar output, and a struct declared as one nullable field, may not be.
    for nullable in (
        pa.field("result", pa.float64(), nullable=True),
        pa.field("result", pa.struct(fields), nullable=True),
    ):
        with pytest.raises(ValueError, match="non-nullable"):
            udf(input_schema=inputs, output_schema=nullable)(describe)


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


def test_function_enterprise(monkeypatch):
    require_deployment()

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

    # In a shared database `function_demo` or `double` may already exist, so
    # this run creates both under names only it uses, and drops only those.
    run = uuid.uuid4().hex[:12]
    table_name = f"function_demo_{run}"
    double = udf(name=f"double_{run}")(WALKTHROUGH_DOUBLE.__wrapped__)
    create_table = db.create_table

    def create_run_table(name, *args, **kwargs):
        assert name == "function_demo"
        return create_table(table_name, *args, **kwargs)

    monkeypatch.setattr(db, "create_table", create_run_table)

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
        )
        # --8<-- [end:function_create_table]

        # --8<-- [start:function_register]
        job = db.create_function_async(double)
        version = job.wait()
        # --8<-- [end:function_register]
        assert version.name == f"double_{run}"

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
            db.drop_table(table_name)
        if version is not None:
            db.drop_function(version.name, version=version.version)


class ReachedConnection(Exception):
    pass


SETTINGS = {
    "LANCEDB_URI": "db://docs-test",
    "LANCEDB_HOST_OVERRIDE": "https://deployment.invalid",
    "LANCEDB_API_KEY": "docs-test-key",
}


@pytest.fixture
def connect_attempts(monkeypatch):
    """`lancedb.connect`, replaced so that it records each call and stops."""
    attempts = []

    def connect(**kwargs):
        attempts.append(kwargs)
        raise ReachedConnection

    monkeypatch.setattr(lancedb, "connect", connect)
    for name in (OPT_IN, *DEPLOYMENT, "LANCEDB_REGION"):
        monkeypatch.delenv(name, raising=False)
    return attempts


def enterprise_outcome(monkeypatch):
    try:
        test_function_enterprise(monkeypatch)
    except (pytest.skip.Exception, pytest.fail.Exception, ReachedConnection) as outcome:
        return outcome


@pytest.mark.parametrize(
    "opt_in",
    [None, "", "0", "true", "TRUE", "yes", "on", " 1", "1 ", "01", "1.0", "11"],
)
def test_function_enterprise_needs_the_exact_opt_in(
    monkeypatch, connect_attempts, opt_in
):
    for name, value in SETTINGS.items():
        monkeypatch.setenv(name, value)
    if opt_in is not None:
        monkeypatch.setenv(OPT_IN, opt_in)
    outcome = enterprise_outcome(monkeypatch)
    assert isinstance(outcome, pytest.skip.Exception), outcome
    assert str(outcome).startswith(f"set {OPT_IN}=1 ")
    assert connect_attempts == []


@pytest.mark.parametrize("blank", ["unset", "empty"])
@pytest.mark.parametrize(
    "missing", [*([name] for name in DEPLOYMENT), list(DEPLOYMENT)]
)
def test_function_enterprise_fails_when_asked_without_settings(
    monkeypatch, connect_attempts, missing, blank
):
    monkeypatch.setenv(OPT_IN, "1")
    for name, value in SETTINGS.items():
        if name not in missing:
            monkeypatch.setenv(name, value)
        elif blank == "empty":
            monkeypatch.setenv(name, "")
    outcome = enterprise_outcome(monkeypatch)
    assert isinstance(outcome, pytest.fail.Exception), outcome
    assert str(outcome) == f"{OPT_IN}=1 needs {', '.join(missing)}"
    assert connect_attempts == []


@pytest.mark.parametrize("region", [None, "eu-west-1"])
def test_function_enterprise_connects_when_asked(monkeypatch, connect_attempts, region):
    monkeypatch.setenv(OPT_IN, "1")
    for name, value in SETTINGS.items():
        monkeypatch.setenv(name, value)
    if region:
        monkeypatch.setenv("LANCEDB_REGION", region)
    outcome = enterprise_outcome(monkeypatch)
    assert isinstance(outcome, ReachedConnection), outcome
    assert connect_attempts == [
        {
            "uri": "db://docs-test",
            "api_key": "docs-test-key",
            "region": region or "us-east-1",
            "host_override": "https://deployment.invalid",
        }
    ]
