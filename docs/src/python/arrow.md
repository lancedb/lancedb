# Pandas and PyArrow


Built on top of [Apache Arrow](https://arrow.apache.org/),
`LanceDB` is easy to integrate with the Python ecosystem, including [Pandas](https://pandas.pydata.org/)
and PyArrow.

## Create dataset

First, we need to connect to a `LanceDB` database.

```py

import lancedb

db = lancedb.connect("data/sample-lancedb")
```

Afterwards, we write a `Pandas DataFrame` to LanceDB directly.

```py
import pandas as pd

data = pd.DataFrame({
    "vector": [[3.1, 4.1], [5.9, 26.5]],
    "item": ["foo", "bar"],
    "price": [10.0, 20.0]
})
table = db.create_table("pd_table", data=data)
```

Similar to [`pyarrow.write_dataset()`](https://arrow.apache.org/docs/python/generated/pyarrow.dataset.write_dataset.html),
[db.create_table()](../python/#lancedb.db.DBConnection.create_table) accepts a wide-range of forms of data.

For example, if you have a dataset that is larger than memory size, you can create table with `Iterator[pyarrow.RecordBatch]`,
to lazily generate data:

```py

from typing import Iterable
import pyarrow as pa

def make_batches() -> Iterable[pa.RecordBatch]:
    for i in range(5):
        yield pa.RecordBatch.from_arrays(
            [
                pa.array([[3.1, 4.1], [5.9, 26.5]]),
                pa.array(["foo", "bar"]),
                pa.array([10.0, 20.0]),
            ],
            ["vector", "item", "price"])

schema=pa.schema([
    pa.field("vector", pa.list_(pa.float32())),
    pa.field("item", pa.utf8()),
    pa.field("price", pa.float32()),
])

table = db.create_table("iterable_table", data=make_batches(), schema=schema)
```

You will find detailed instructions of creating dataset in
[Basic Operations](../basic.md) and [API](../python/#lancedb.db.DBConnection.create_table)
sections.

## Vector Search

We can now perform similarity search via `LanceDB` Python API.

```py
# Open the table previously created.
table = db.open_table("pd_table")

query_vector = [100, 100]
# Pandas DataFrame
df = table.search(query_vector).limit(1).to_pandas()
print(df)
```

```
    vector     item  price    _distance
0  [5.9, 26.5]  bar   20.0  14257.05957
```

If you have a simple filter, it's faster to provide a `where clause` to `LanceDB`'s search query.
If you have more complex criteria, you can always apply the filter to the resulting Pandas `DataFrame`.

```python

# Apply the filter via LanceDB
results = table.search([100, 100]).where("price < 15").to_pandas()
assert len(results) == 1
assert results["item"].iloc[0] == "foo"

# Apply the filter via Pandas
df = results = table.search([100, 100]).to_pandas()
results = df[df.price < 15]
assert len(results) == 1
assert results["item"].iloc[0] == "foo"
```