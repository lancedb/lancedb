# Polars

LanceDB supports [Polars](https://github.com/pola-rs/polars), a blazingly fast DataFrame library for Python written in Rust. Just like in Pandas, the Polars integration is enabled by PyArrow under the hood. A deeper integration between Lance Tables and Polars DataFrames is in progress, but at the moment, you can read a Polars DataFrame into LanceDB and output the search results from a query to a Polars DataFrame.

## Create & Query LanceDB Table

### From Polars DataFrame

First, we connect to a LanceDB database.

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

We can now perform similarity search via the LanceDB Python API.

```py
query = [3.0, 4.0]
result = table.search(query).limit(1).to_polars()
print(result)
print(type(result))
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
<class 'polars.dataframe.frame.DataFrame'>
```

Note that the type of the result from a table search is a Polars DataFrame.

### From Pydantic Models

Alternately, we can create an empty LanceDB Table using a Pydantic schema and populate it with a Polars DataFrame.

```py
import polars as pl
from lancedb.pydantic import Vector, LanceModel


class Item(LanceModel):
    vector: Vector(2)
    item: str
    price: float

data = {
    "vector": [[3.1, 4.1]],
    "item": "foo",
    "price": 10.0,
}

table = db.create_table("test_table", schema=Item)
df = pl.DataFrame(data)
# Add Polars DataFrame to table
table.add(df)
```

The table can now be queried as usual.

```py
result = table.search([3.0, 4.0]).limit(1).to_polars()
print(result)
print(type(result))
```

```
shape: (1, 4)
┌───────────────┬──────┬───────┬───────────┐
│ vector        ┆ item ┆ price ┆ _distance │
│ ---           ┆ ---  ┆ ---   ┆ ---       │
│ array[f32, 2] ┆ str  ┆ f64   ┆ f32       │
╞═══════════════╪══════╪═══════╪═══════════╡
│ [3.1, 4.1]    ┆ foo  ┆ 10.0  ┆ 0.02      │
└───────────────┴──────┴───────┴───────────┘
<class 'polars.dataframe.frame.DataFrame'>
```

This result is the same as the previous one, with a DataFrame returned.

## Dump Table to LazyFrame

As you iterate on your application, you'll likely need to work with the whole table's data pretty frequently.
LanceDB tables can also be converted directly into a polars LazyFrame for further processing.

```python
ldf = table.to_polars()
print(type(ldf))
```

Unlike the search result from a query, we can see that the type of the result is a LazyFrame.

```
<class 'polars.lazyframe.frame.LazyFrame'>
```

We can now work with the LazyFrame as we would in Polars, and collect the first result.

```python
print(ldf.first().collect())
```

```
shape: (1, 3)
┌───────────────┬──────┬───────┐
│ vector        ┆ item ┆ price │
│ ---           ┆ ---  ┆ ---   │
│ array[f32, 2] ┆ str  ┆ f64   │
╞═══════════════╪══════╪═══════╡
│ [3.1, 4.1]    ┆ foo  ┆ 10.0  │
└───────────────┴──────┴───────┘
```

The reason it's beneficial to not convert the LanceDB Table
to a DataFrame is because the table can potentially be way larger
than memory, and Polars LazyFrames allow us to work with such
larger-than-memory datasets by not loading it into memory all at once.

