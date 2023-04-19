# Integrations

Built on top of Apache Arrow, `LanceDB` is easy to integrate with the Python ecosystem, including Pandas, Polars, and DuckDB.

## Pandas and PyArrow

``` sh
pip install pandas lancedb
```

First, we need to connect to a LanceDB instance. This instance could be a local file directory.

``` py

import lancedb

db = lancedb.connect("/tmp/lancedb")
```

And write `Pandas DataFrame` to LanceDB directly.

```py
import pandas as pd

data = pd.DataFrame({
    "vector": [[3.1, 4.1], [5.9, 26.5]],
    "item": ["foo", "bar"],
    "price": [10.0, 20.0]
})
table = db.create_table("pd_table", data=data)

# Optionally, create a IVF_PQ index
table.create_index(num_partitions=256, num_sub_vectors=96)
```

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

We can apply the filter via `LancdDB`, or apply the filter on `pd.DataFrame` later.

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

# DuckDB

Let us start with installing `duckdb` and `lancedb`.

```shell
pip install duckdb lancedb
```

We will re-use the dataset created previously

```python
import lancedb

db = lancedb.connect("/tmp/lancedb")
table = db.open_table("pd_table")
arrow_table = table.to_arrow()
```

Now we can use `DuckDB` to query the `arrow_table`:

```python
In [15]: duckdb.query("SELECT * FROM t")
Out[15]:
┌─────────────┬─────────┬────────┐
│   vector    │  item   │ price  │
│   float[]   │ varchar │ double │
├─────────────┼─────────┼────────┤
│ [3.1, 4.1]  │ foo     │   10.0 │
│ [5.9, 26.5] │ bar     │   20.0 │
└─────────────┴─────────┴────────┘

In [16]: duckdb.query("SELECT mean(price) FROM t")
Out[16]:
┌─────────────┐
│ mean(price) │
│   double    │
├─────────────┤
│        15.0 │
└─────────────┘
```