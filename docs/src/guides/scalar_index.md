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

| Data Type                                                       | Filter                                    | Index Type   |
| --------------------------------------------------------------- | ----------------------------------------- | ------------ |
| Numeric, String, Temporal                                       | `<`, `=`, `>`, `in`, `between`, `is null` | `BTREE`      |
| Boolean, numbers or strings with fewer than 1,000 unique values | `<`, `=`, `>`, `in`, `between`, `is null` | `BITMAP`     |
| List of low cardinality of numbers or strings                   | `array_has_any`, `array_has_all`          | `LABEL_LIST` |

=== "Python"

    ```python
    import lancedb
    books = [
      {"book_id": 1, "publisher": "plenty of books", "tags": ["fantasy", "adventure"]},
      {"book_id": 2, "publisher": "book town", "tags": ["non-fiction"]},
      {"book_id": 3, "publisher": "oreilly", "tags": ["textbook"]}
    ]

    db = lancedb.connect("./db")
    table = db.create_table("books", books)
    table.create_scalar_index("book_id")  # BTree by default
    table.create_scalar_index("publisher", index_type="BITMAP")
    ```

=== "Typescript"

    === "@lancedb/lancedb"

        ```js
        const db = await lancedb.connect("data");
        const tbl = await db.openTable("my_vectors");

        await tbl.create_index("book_id");
        await tlb.create_index("publisher", { config: lancedb.Index.bitmap() })
        ```

For example, the following scan will be faster if the column `my_col` has a scalar index:

=== "Python"

    ```python
    import lancedb

    table = db.open_table("books")
    my_df = table.search().where("book_id = 2").to_pandas()
    ```

=== "Typescript"

    === "@lancedb/lancedb"

        ```js
        const db = await lancedb.connect("data");
        const tbl = await db.openTable("books");

        await tbl
          .query()
          .where("book_id = 2")
          .limit(10)
          .toArray();
        ```

Scalar indices can also speed up scans containing a vector search or full text search, and a prefilter:

=== "Python"

    ```python
    import lancedb

    data = [
      {"book_id": 1, "vector": [1, 2]},
      {"book_id": 2, "vector": [3, 4]},
      {"book_id": 3, "vector": [5, 6]}
    ]
    table = db.create_table("book_with_embeddings", data)

    (
        table.search([1, 2])
        .where("book_id != 3", prefilter=True)
        .to_pandas()
    )
    ```

=== "Typescript"

    === "@lancedb/lancedb"

        ```js
        const db = await lancedb.connect("data/lance");
        const tbl = await db.openTable("book_with_embeddings");

        await tbl.search(Array(1536).fill(1.2))
          .where("book_id != 3")  // prefilter is default behavior.
          .limit(10)
          .toArray();
        ```
