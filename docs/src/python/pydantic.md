# Pydantic

[Pydantic](https://docs.pydantic.dev/latest/) is a data validation library in Python.
LanceDB integrates with Pydantic for schema inference, data ingestion, and query result casting.

## Schema

LanceDB supports to create Apache Arrow Schema from a
[Pydantic BaseModel](https://docs.pydantic.dev/latest/api/main/#pydantic.main.BaseModel)
via [pydantic_to_schema()](python.md#lancedb.pydantic.pydantic_to_schema) method.

::: lancedb.pydantic.pydantic_to_schema

## Vector Field

LanceDB provides a [`Vector(dim)`](python.md#lancedb.pydantic.Vector) method to define a
vector Field in a Pydantic Model.

::: lancedb.pydantic.Vector

## Type Conversion

LanceDB automatically convert Pydantic fields to
[Apache Arrow DataType](https://arrow.apache.org/docs/python/generated/pyarrow.DataType.html#pyarrow.DataType).

Current supported type conversions:

| Pydantic Field Type | PyArrow Data Type |
| ------------------- | ----------------- |
| `int`               | `pyarrow.int64`   |
| `float`              | `pyarrow.float64`  |
| `bool`              | `pyarrow.bool`    |
| `str`               | `pyarrow.utf8()`    |
| `list`              | `pyarrow.List`    |
| `BaseModel`         | `pyarrow.Struct`    |
| `Vector(n)`         | `pyarrow.FixedSizeList(float32, n)` |
