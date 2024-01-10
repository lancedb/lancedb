# DuckDB

LanceDB is very well-integrated with [DuckDB](https://duckdb.org/), an in-process SQL OLAP database. This integration is done via [Arrow](https://duckdb.org/docs/guides/python/sql_on_arrow) .

We can demonstrate this by first installing `duckdb` and `lancedb`.

```shell
pip install duckdb lancedb
```

We will re-use the dataset [created previously](./pandas_and_pyarrow.md):

```python
import lancedb

db = lancedb.connect("data/sample-lancedb")
data = [
    {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
    {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}
]
table = db.create_table("pd_table", data=data)
arrow_table = table.to_arrow()
```

DuckDB can directly query the `pyarrow.Table` object:

```python
import duckdb

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

You can very easily run any other DuckDB SQL queries on your data.

```py
duckdb.query("SELECT mean(price) FROM arrow_table")
```

```
┌─────────────┐
│ mean(price) │
│   double    │
├─────────────┤
│        15.0 │
└─────────────┘
```