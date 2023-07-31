# Pydantic

[Pydantic](https://docs.pydantic.dev/latest/) is a data validation library in Python.
LanceDB integrates with Pydantic for schema inference, data ingestion, and query result casting.

## Quick example

```python
import lancedb
from lancedb.pydantic import LanceModel, vector

# Define model

class TestModel(LanceModel):
    name: str
    vector: vector(2)
    
db = lancedb.connect("/tmp")

# Create table
table = db.create_table("test", schema=TestModel.to_arrow_schema())

# Add data
table.add([
    TestModel(name="test", vector=[1.0, 2.0])
])

# Convert results
table.search([0., 0.]).limit(1).to(TestModel)

# >>> table.search([0., 0.]).limit(1).to(TestModel)
# [TestModel(name='test', vector=FixedSizeList(dim=2))]
```

## Schema

LanceDB supports to create Apache Arrow Schema from a
[Pydantic BaseModel](https://docs.pydantic.dev/latest/api/main/#pydantic.main.BaseModel)
via [pydantic_to_schema()](python.md##lancedb.pydantic.pydantic_to_schema) method.

::: lancedb.pydantic.pydantic_to_schema

## Vector Field

LanceDB provides a [`vector(dim)`](python.md#lancedb.pydantic.vector) method to define a
vector Field in a Pydantic Model.

::: lancedb.pydantic.vector

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
| `vector(n)`         | `pyarrow.FixedSizeList(float32, n)` |
