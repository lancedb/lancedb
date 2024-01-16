# Polars

LanceDB supports [Polars](https://github.com/pola-rs/polars), a blazingly fast DataFrame library for Python written in Rust. Just like in Pandas, the Polars integration is enabled by PyArrow under the hood. A deeper integration between Lance Tables and Polars DataFrames is in progress, but at the moment, you can read a Polars DataFrame into LanceDB and output the search results from a query to a Polars DataFrame.

## Create dataset

First, we need to connect to a LanceDB database.

```py

import lancedb

db = lancedb.connect("data/polars-lancedb")
```

We can load a Polars `DataFrame` to LanceDB directly.

```py
import polars as pl

data = pl.DataFrame({
    "vector": [[3.1, 4.1], [5.9, 26.5]],
    "item": ["foo", "bar"],
    "price": [10.0, 20.0]
})
table = db.create_table("pl_table", data=data)
```

## Vector search

We can now perform similarity search via the LanceDB Python API.

```py
query = [3.1, 4.1]
result = table.search(query).limit(1).to_polars()
assert len(result) == 1
assert result["item"][0] == "foo"
```

In addition to the selected columns, LanceDB also returns a vector
and also the `_distance` column which is the distance between the query
vector and the returned vector.

```
shape: (1, 4)
┌───────────────┬──────┬───────┬───────────┐
│ vector        ┆ item ┆ price ┆ _distance │
│ ---           ┆ ---  ┆ ---   ┆ ---       │
│ array[f32, 2] ┆ str  ┆ f64   ┆ f32       │
╞═══════════════╪══════╪═══════╪═══════════╡
│ [3.1, 4.1]    ┆ foo  ┆ 10.0  ┆ 0.0       │
└───────────────┴──────┴───────┴───────────┘
```