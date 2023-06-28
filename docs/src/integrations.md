# Integrations

Built on top of Apache Arrow, `LanceDB` is easy to integrate with the Python ecosystem, including Pandas, PyArrow and DuckDB.

## Pandas and PyArrow

First, we need to connect to a `LanceDB` database.

```py

import lancedb

db = lancedb.connect("data/sample-lancedb")
```

And write a `Pandas DataFrame` to LanceDB directly.

```py
import pandas as pd

data = pd.DataFrame({
    "vector": [[3.1, 4.1], [5.9, 26.5]],
    "item": ["foo", "bar"],
    "price": [10.0, 20.0]
})
table = db.create_table("pd_table", data=data)
```

You will find detailed instructions of creating dataset and index in [Basic Operations](basic.md) and [Indexing](ann_indexes.md)
sections.


We can now perform similarity searches via `LanceDB`.

```py
# Open the table previously created.
table = db.open_table("pd_table")

query_vector = [100, 100]
# Pandas DataFrame
df = table.search(query_vector).limit(1).to_df()
print(df)
```

```
    vector     item  price        score
0  [5.9, 26.5]  bar   20.0  14257.05957
```

If you have a simple filter, it's faster to provide a where clause to `LanceDB`'s search query.
If you have more complex criteria, you can always apply the filter to the resulting pandas `DataFrame` from the search query.

```python

# Apply the filter via LanceDB
results = table.search([100, 100]).where("price < 15").to_df()
assert len(results) == 1
assert results["item"].iloc[0] == "foo"

# Apply the filter via Pandas
df = results = table.search([100, 100]).to_df()
results = df[df.price < 15]
assert len(results) == 1
assert results["item"].iloc[0] == "foo"
```

## DuckDB

`LanceDB` works with `DuckDB` via [PyArrow integration](https://duckdb.org/docs/guides/python/sql_on_arrow).

Let us start with installing `duckdb` and `lancedb`.

```shell
pip install duckdb lancedb
```

We will re-use the dataset created previously

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
```python
duckdb.query("SELECT mean(price) FROM arrow_table")
```

```
Out[16]:
┌─────────────┐
│ mean(price) │
│   double    │
├─────────────┤
│        15.0 │
└─────────────┘
```