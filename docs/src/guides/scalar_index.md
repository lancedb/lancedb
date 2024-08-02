# Building Scalar Index

Similar to many SQL databases, LanceDB supports several types of Scalar indices to accelerate search
over scalar columns.

- `BTREE`: The most common type is BTREE. This index is inspired by the btree data structure
  although only the first few layers of the btree are cached in memory.
  It will perform well on columns with a large number of unique values and few rows per value.
- `BITMAP`: this index stores a bitmap for each unique value in the column.
  This index is useful for columns with a small number of unique values and many rows per value.
  For example, a column presents `categories`. In Apache Arrow terminalogy, a column can be presented
  by `DictionaryType`.
- `LABEL_LIST`: a special index that is used to index list columns whose values have small cardinality.
  For example, a column that contains lists of tags (e.g. `["tag1", "tag2", "tag3"]`) can be indexed with a `LABEL_LIST` index.
  This index can only speedup queries with `array_has_any()` or `array_has_all()` filters.

=== "Python"

    ```python
    import lancedb
    db = lancedb.connect("./db")
    table = db.open_table("my_table")
    table.create_scalar_index("category", index_type="BITMAP")
    ```

=== "Typescript"

    Only `BTree` index is supported today. `BITMAP` and `LABEL_LIST` will be added soon.

    ```js
    const db = await lancedb.connect("data/sample-lancedb");
    const tbl = await db.openTable("my_vectors");

    await tbl.create_index("my_float_column");
    ```

For example, the following scan will be faster if the column `my_col` has a scalar index:

=== "Python"

    ```python
    import lancedb

    db = lancedb.connect("/data/lance")
    img_table = db.open_table("images")
    my_df = img_table.search().where("my_col = 7").to_pandas()
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

    db = lancedb.connect("/data/lance")
    img_table = db.open_table("images")
    img_table.search([1, 2, 3, 4], vector_column_name="vector")
        .where("my_col != 7", prefilter=True)
        .to_pandas()
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
