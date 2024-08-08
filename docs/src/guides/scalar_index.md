# Building Scalar Index

Similar to many SQL databases, LanceDB supports several types of Scalar indices to accelerate search
over scalar columns.

- `BTREE`: The most common type is BTREE. This index is inspired by the btree data structure
  although only the first few layers of the btree are cached in memory.
  It will perform well on columns with a large number of unique values and few rows per value.
- `BITMAP`: this index stores a bitmap for each unique value in the column.
  This index is useful for columns with a finite number of unique values and many rows per value.
  For example, columns that represent "categories", "labels", or "tags"
- `LABEL_LIST`: a special index that is used to index list columns whose values have a finite set of possibilities.
  For example, a column that contains lists of tags (e.g. `["tag1", "tag2", "tag3"]`) can be indexed with a `LABEL_LIST` index.

| Data Type                                     | Filter                           | Index Type   |
| --------------------------------------------- | -------------------------------- | ------------ |
| Numeric, String, Temporal                      | `<`, `=`, `>`, `in`, `between`, `is null`                    | `BTREE`      |
| Boolean, numbers or strings with finite range of values         | `<`, `=`, `>`, `in`, `between`, `is null`   | `BITMAP`     |
| List of low cardinality of numbers or strings | `array_has_any`, `array_has_all` | `LABEL_LIST` |

=== "Python"

    ```python
    import lancedb
    data = [
      {"my_col": 1, "category": "A", "vector": [1, 2]},
      {"my_col": 2, "category": "B", "vector": [3, 4]},
      {"my_col": 3, "category": "C", "vector": [5, 6]}
    ]

    db = lancedb.connect("./db")
    table = db.create_table("my_table", data)
    table.create_scalar_index("my_col")  # BTree by default
    table.create_scalar_index("category", index_type="BITMAP")
    ```

=== "Typescript"

    Only `BTree` index is supported today. `BITMAP` and `LABEL_LIST` will be added soon.

    Follow [Github issue #1511](https://github.com/lancedb/lancedb/issues/1511) for updates.

    ```js
    const db = await lancedb.connect("data/sample-lancedb");
    const tbl = await db.openTable("my_vectors");

    await tbl.create_index("my_float_column");
    ```

For example, the following scan will be faster if the column `my_col` has a scalar index:

=== "Python"

    ```python
    import lancedb

    table = db.open_table("my_table")
    my_df = table.search().where("my_col = 2").to_pandas()
    ```

=== "Typescript"

    === "@lancedb/lancedb"

        ```js
        const db = await lancedb.connect("data/lance");
        const tbl = await db.openTable("images");

        await tbl
          .query()
          .where("my_col != 7")
          .limit(10)
          .toArray();
        ```

Scalar indices can also speed up scans containing a vector search or full text search, and a prefilter:

=== "Python"

    ```python
    import lancedb

    data = [
      {"my_col": 1, "vector": [1, 2]},
      {"my_col": 2, "vector": [3, 4]},
      {"my_col": 3, "vector": [5, 6]}
    ]
    table = db.create_table("vecs", data)
    (
        table.search([1, 2])
        .where("my_col != 3", prefilter=True)
        .to_pandas()
    )
    ```

=== "Typescript"

    === "@lancedb/lancedb"

        ```js
        const db = await lancedb.connect("data/lance");
        const tbl = await db.openTable("images");

        await tbl.search(Array(1536).fill(1.2))
          .where("my_col != 7")  // prefilter is default behavior.
          .limit(10)
          .toArray();
        ```
