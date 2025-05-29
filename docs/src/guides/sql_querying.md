You can use DuckDB and Apache Datafusion to query your LanceDB tables using SQL.
This guide will show how to query Lance tables them using both.

We will re-use the dataset [created previously](./pandas_and_pyarrow.md):

```python
import lancedb

db = lancedb.connect("data/sample-lancedb")
data = [
    {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
    {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}
]
table = db.create_table("pd_table", data=data)
```

## Querying a LanceDB Table with DuckDb

The `to_lance` method converts the LanceDB table to a `LanceDataset`, which is accessible to DuckDB through the Arrow compatibility layer.
To query the resulting Lance dataset in DuckDB, all you need to do is reference the dataset by the same name in your SQL query.

```python
import duckdb

arrow_table = table.to_lance()

duckdb.query("SELECT * FROM arrow_table")
```

```
┌─────────────┬─────────┬────────┐
│   vector    │  item   │ price  │
│   float[]   │ varchar │ double │
├─────────────┼─────────┼────────┤
│ [3.1, 4.1]  │ foo     │   10.0 │
│ [5.9, 26.5] │ bar     │   20.0 │
└─────────────┴─────────┴────────┘
```

## Querying a LanceDB Table with Apache Datafusion

Have the required imports before doing any querying.

=== "Python"
    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-lancedb"
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-session-context"
    --8<-- "python/python/tests/docs/test_guide_tables.py:import-ffi-dataset"
    ```

Register the table created with the Datafusion session context.

=== "Python"
    ```python
    --8<-- "python/python/tests/docs/test_guide_tables.py:lance_sql_basic"
    ```

```
┌─────────────┬─────────┬────────┐
│   vector    │  item   │ price  │
│   float[]   │ varchar │ double │
├─────────────┼─────────┼────────┤
│ [3.1, 4.1]  │ foo     │   10.0 │
│ [5.9, 26.5] │ bar     │   20.0 │
└─────────────┴─────────┴────────┘
```
