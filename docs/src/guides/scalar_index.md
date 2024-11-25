# Building a Scalar Index

Scalar indices organize data by scalar attributes (e.g. numbers, categorical values), enabling fast filtering of vector data. In vector databases, scalar indices accelerate the retrieval of scalar data associated with vectors, thus enhancing the query performance when searching for vectors that meet certain scalar criteria. 

Similar to many SQL databases, LanceDB supports several types of scalar indices to accelerate search
over scalar columns.

- `BTREE`: The most common type is BTREE. The index stores a copy of the
  column in sorted order. This sorted copy allows a binary search to be used to
  satisfy queries.
- `BITMAP`: this index stores a bitmap for each unique value in the column. It 
  uses a series of bits to indicate whether a value is present in a row of a table
- `LABEL_LIST`: a special index that can be used on `List<T>` columns to
  support queries with `array_contains_all` and `array_contains_any`
  using an underlying bitmap index.
  For example, a column that contains lists of tags (e.g. `["tag1", "tag2", "tag3"]`) can be indexed with a `LABEL_LIST` index.

!!! tips "How to choose the right scalar index type"

    `BTREE`: This index is good for scalar columns with mostly distinct values and does best when the query is highly selective.
    
    `BITMAP`: This index works best for low-cardinality numeric or string columns, where the number of unique values is small (i.e., less than a few thousands).
    
    `LABEL_LIST`: This index should be used for columns containing list-type data.

| Data Type                                                       | Filter                                    | Index Type   |
| --------------------------------------------------------------- | ----------------------------------------- | ------------ |
| Numeric, String, Temporal                                       | `<`, `=`, `>`, `in`, `between`, `is null` | `BTREE`      |
| Boolean, numbers or strings with fewer than 1,000 unique values | `<`, `=`, `>`, `in`, `between`, `is null` | `BITMAP`     |
| List of low cardinality of numbers or strings                   | `array_has_any`, `array_has_all`          | `LABEL_LIST` |

### Create a scalar index
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

The following scan will be faster if the column `book_id` has a scalar index:

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
### Update a scalar index
Updating the table data (adding, deleting, or modifying records) requires that you also update the scalar index. This can be done by calling `optimize`, which will trigger an update to the existing scalar index.
=== "Python"

    ```python
    table.add([{"vector": [7, 8], "book_id": 4}])
    table.optimize()
    ```

=== "TypeScript"

    ```typescript
    await tbl.add([{ vector: [7, 8], book_id: 4 }]);
    await tbl.optimize();
    ```

=== "Rust"

    ```rust
    let more_data: Box<dyn RecordBatchReader + Send> = create_some_records()?;
    tbl.add(more_data).execute().await?;
    tbl.optimize(OptimizeAction::All).execute().await?;
    ```

!!! note

    New data added after creating the scalar index will still appear in search results if optimize is not used, but with increased latency due to a flat search on the unindexed portion. LanceDB Cloud automates the optimize process, minimizing the impact on search speed.