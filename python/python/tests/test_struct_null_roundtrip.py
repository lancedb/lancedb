"""Tests for nullable struct round-trip (issue #3105).

Verifies that nullable struct fields preserve None on read-back instead
of being inflated to dicts with default values.
"""

import pyarrow as pa
from lancedb.arrow import table_to_pylist


def test_top_level_null_struct():
    """A top-level nullable struct column should round-trip None correctly."""
    struct_type = pa.struct(
        [pa.field("name", pa.utf8(), False), pa.field("value", pa.utf8(), False)]
    )
    col = pa.array(
        [None, {"name": "hello", "value": "world"}], type=struct_type
    )
    table = pa.table({"id": [1, 2], "obj": col})

    result = table_to_pylist(table)
    assert result[0]["obj"] is None
    assert result[1]["obj"] == {"name": "hello", "value": "world"}


def test_nested_null_struct():
    """A struct with a nullable inner struct should preserve None on the inner."""
    inner_type = pa.struct([pa.field("value", pa.utf8(), False)])
    outer_type = pa.struct(
        [
            pa.field("name", pa.utf8(), False),
            pa.field("inner", inner_type, True),
        ]
    )
    col = pa.array(
        [{"name": "has inner", "inner": {"value": "x"}}, {"name": "no inner", "inner": None}],
        type=outer_type,
    )
    table = pa.table({"data": col})

    result = table_to_pylist(table)
    assert result[0]["data"]["inner"] == {"value": "x"}
    assert result[1]["data"]["inner"] is None


def test_list_of_nullable_structs():
    """A list column whose items are nullable structs should preserve None items."""
    struct_type = pa.struct(
        [pa.field("name", pa.utf8(), False)]
    )
    list_type = pa.list_(pa.field("item", struct_type, True))
    col = pa.array(
        [[{"name": "a"}, None, {"name": "b"}], [None]],
        type=list_type,
    )
    table = pa.table({"items": col})

    result = table_to_pylist(table)
    assert result[0]["items"][0] == {"name": "a"}
    assert result[0]["items"][1] is None
    assert result[0]["items"][2] == {"name": "b"}
    assert result[1]["items"][0] is None


def test_full_scenario_from_issue():
    """Reproduce the exact scenario from issue #3105."""
    inner_type = pa.struct([pa.field("value", pa.utf8(), False)])
    nested_type = pa.struct(
        [
            pa.field("name", pa.utf8(), False),
            pa.field("inner", inner_type, True),
        ]
    )
    list_type = pa.list_(pa.field("item", nested_type, True))

    table = pa.table(
        {
            "test_id": ["All null", "Nested null"],
            "object": pa.array([None, {"name": "null inner", "inner": None}], type=nested_type),
            "list": pa.array([None, [None]], type=list_type),
            "primitive_field": pa.array([None, 1], type=pa.int64()),
        }
    )

    result = table_to_pylist(table)

    # "All null" row
    assert result[0]["test_id"] == "All null"
    assert result[0]["object"] is None
    assert result[0]["list"] is None
    assert result[0]["primitive_field"] is None

    # "Nested null" row
    assert result[1]["test_id"] == "Nested null"
    assert result[1]["object"] == {"name": "null inner", "inner": None}
    assert result[1]["list"] == [None]
    assert result[1]["primitive_field"] == 1


def test_no_struct_columns_passthrough():
    """Tables without struct columns should use fast path (plain to_pylist)."""
    table = pa.table({"a": [1, 2], "b": ["x", "y"]})
    result = table_to_pylist(table)
    assert result == [{"a": 1, "b": "x"}, {"a": 2, "b": "y"}]
