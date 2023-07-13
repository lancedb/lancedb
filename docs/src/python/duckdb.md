# DuckDB

`LanceDB` works with `DuckDB` via [PyArrow integration](https://duckdb.org/docs/guides/python/sql_on_arrow).

Let us start with installing `duckdb` and `lancedb`.

```shell
pip install duckdb lancedb
```

We will re-use [the dataset created previously](./arrow.md):

```python
import lancedb

db = lancedb.connect("data/sample-lancedb")
table = db.open_table("pd_table")
arrow_table = table.to_arrow()
```

`DuckDB` can directly query the `arrow_table`:

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