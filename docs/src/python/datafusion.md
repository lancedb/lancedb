# Apache Datafusion

In Python, LanceDB tables can also be queried with [Apache Datafusion](https://datafusion.apache.org/), an extensible query engine written in Rust that uses Apache Arrow as its in-memory format. This means you can write complex SQL queries to analyze your data in LanceDB.

This integration is done via [Datafusion FFI](https://docs.rs/datafusion-ffi/latest/datafusion_ffi/), which provides a native integration between LanceDB and Datafusion.
The Datafusion FFI allows to pass down column selections and basic filters to LanceDB, reducing the amount of scanned data when executing your query. Additionally, the integration allows streaming data from LanceDB tables which allows to do aggregation larger-than-memory.

We can demonstrate this by first installing `datafusion` and `lancedb`.

```shell
pip install datafusion lancedb
```

We will re-use the dataset [created previously](./pandas_and_pyarrow.md):

```python
import lancedb

from datafusion import SessionContext
from lance import FFILanceTableProvider

db = lancedb.connect("data/sample-lancedb")
data = [
    {"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
    {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}
]
lance_table = db.create_table("lance_table", data)

ctx = SessionContext()

ffi_lance_table = FFILanceTableProvider(
    lance_table.to_lance(), with_row_id=True, with_row_addr=True
)
ctx.register_table_provider("ffi_lance_table", ffi_lance_table)
```

The `to_lance` method converts the LanceDB table to a `LanceDataset`, which is accessible to Datafusion through the Datafusion FFI integration layer.
To query the resulting Lance dataset in Datafusion, you first need to register the dataset with Datafusion and then just reference it by the same name in your SQL query.

```python
ctx.table("ffi_lance_table")
ctx.sql("SELECT * FROM ffi_lance_table")
```

```
┌─────────────┬─────────┬────────┬─────────────────┬─────────────────┐
│   vector    │  item   │ price  │ _rowid          │ _rowaddr        │
│   float[]   │ varchar │ double │ bigint unsigned │ bigint unsigned │
├─────────────┼─────────┼────────┼─────────────────┼─────────────────┤
│ [3.1, 4.1]  │ foo     │   10.0 │               0 │               0 │
│ [5.9, 26.5] │ bar     │   20.0 │               1 │               1 │
└─────────────┴─────────┴────────┴─────────────────┴─────────────────┘
```
