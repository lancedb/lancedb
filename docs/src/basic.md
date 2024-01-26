# Quick start

!!! info "LanceDB can be run in a number of ways:"

    * Embedded within an existing backend (like your Django, Flask, Node.js or FastAPI application)
    * Connected to directly from a client application like a Jupyter notebook for analytical workloads
    * Deployed as a remote serverless database

![](assets/lancedb_embedded_explanation.png)

## Installation

=== "Python"

      ```shell
      pip install lancedb
      ```

=== "Javascript"

      ```shell
      npm install vectordb
      ```

=== "Rust"

    ```shell
    cargo install vectordb
    ```

    !!! info "Rust crate is installed as source. You need install protobuf."

    === "macOS"

        ```shell
        brew install protobuf
        ```

    === "Ubuntu/Debian"

        ```shell
        sudo apt install -y protobuf-compiler libssl-dev
        ```


## How to connect to a database

=== "Python"

      ```python
      import lancedb
      uri = "data/sample-lancedb"
      db = lancedb.connect(uri)
      ```

=== "Javascript"

      ```javascript
      const lancedb = require("vectordb");

      const uri = "data/sample-lancedb";
      const db = await lancedb.connect(uri);
      ```

=== "Rust"

    ```rust
    use vectordb::connect;

    let uri = "data/sample-lancedb";
    let db = connect(&uri).await.unwrap();
    ```

LanceDB will create the directory if it doesn't exist (including parent directories).

If you need a reminder of the uri, you can call `db.uri()`.

## How to create a table

=== "Python"

    ```python
    tbl = db.create_table("my_table",
                    data=[{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                          {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
    ```

    If the table already exists, LanceDB will raise an error by default.
    If you want to overwrite the table, you can pass in `mode="overwrite"`
    to the `create_table` method.

    You can also pass in a pandas DataFrame directly:

    ```python
    import pandas as pd
    df = pd.DataFrame([{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
                       {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}])
    tbl = db.create_table("table_from_df", data=df)
    ```

=== "Javascript"

    ```javascript
    const tb = await db.createTable(
        "myTable",
        [{"vector": [3.1, 4.1], "item": "foo", "price": 10.0},
         {"vector": [5.9, 26.5], "item": "bar", "price": 20.0}]
    )
    ```

    If the table already exists, LanceDB will raise an error by default.
    If you want to overwrite the table, you can pass in `mode="overwrite"`
    to the `createTable` function.

=== "Rust"

    ```rust
    use arrow_schema::{DataType, Schema, Field};
    use arrow_array::{RecordBatch, RecordBatchIterator};

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int32, false),
        Field::new("vector", DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Float32, true)), 128), true),
    ]));
    // Create a RecordBatch stream.
    let batches = RecordBatchIterator::new(vec![
        RecordBatch::try*new(schema.clone(),
        vec![
            Arc::new(Int32Array::from_iter_values(0..10)),
            Arc::new(FixedSizeListArray::from_iter_primitive::<Float32Type, *, _>(
                (0..10).map(|_| Some(vec![Some(1.0); 128])), 128)),
            ]).unwrap()
        ].into_iter().map(Ok),
        schema.clone());
    db.create_table("my_table", Box::new(batches), None).await.unwrap();
    ```

    If the table already exists, LanceDB will raise an error by default.

!!! info "Under the hood, LanceDB is converting the input data into an Apache Arrow table and persisting it to disk in [Lance format](https://www.github.com/lancedb/lance)."

### Creating an empty table

Sometimes you may not have the data to insert into the table at creation time.
In this case, you can create an empty table and specify the schema.

=== "Python"

      ```python
      import pyarrow as pa
      schema = pa.schema([pa.field("vector", pa.list_(pa.float32(), list_size=2))])
      tbl = db.create_table("empty_table", schema=schema)
      ```

## How to open an existing table

Once created, you can open a table using the following code:

=== "Python"

    ```python
    tbl = db.open_table("my_table")
    ```

=== "Javascript"

    ```typescript
    const tbl = await db.openTable("myTable");
    ```

=== "Rust"

    ```rust
    const tbl = db.open_table_with_params("myTable", None).await.unwrap();
    ```

If you forget the name of your table, you can always get a listing of all table names:

=== "Python"

    ```python
    print(db.table_names())
    ```

=== "Javascript"

    ```javascript
    console.log(await db.tableNames());
    ```

=== "Rust"

    ```rust
    println!("{:?}", db.table_names().await.unwrap());
    ```

## How to add data to a table

After a table has been created, you can always add more data to it using

=== "Python"

    ```python

    # Option 1: Add a list of dicts to a table
    data = [{"vector": [1.3, 1.4], "item": "fizz", "price": 100.0},
            {"vector": [9.5, 56.2], "item": "buzz", "price": 200.0}]
    tbl.add(data)

    # Option 2: Add a pandas DataFrame to a table
    df = pd.DataFrame(data)
    tbl.add(data)
    ```

=== "Javascript"

    ```javascript
    await tbl.add([{vector: [1.3, 1.4], item: "fizz", price: 100.0},
                    {vector: [9.5, 56.2], item: "buzz", price: 200.0}])
    ```

=== "Rust"

    ```rust
    let batches = RecordBatchIterator::new(...);
    tbl.add(Box::new(batches), None).await.unwrap();
    ```

## How to search for (approximate) nearest neighbors

Once you've embedded the query, you can find its nearest neighbors using the following code:

=== "Python"

    ```python
    tbl.search([100, 100]).limit(2).to_pandas()
    ```

    This returns a pandas DataFrame with the results.

=== "Javascript"

    ```javascript
    const query = await tbl.search([100, 100]).limit(2).execute();
    ```

=== "Rust"

    ```rust
    use arrow_array::RecordBatch;
    use futures::TryStreamExt;

    let results: Vec<RecordBatch> = tbl
        .search(&[100.0, 100.0])
        .execute_stream()
        .await
        .unwrap()
        .try_collect();
    ```

By default, LanceDB runs a brute-force scan over dataset to find the K nearest neighbours (KNN).
users can speed up the query by creating vector indices over the vector columns.

=== "Python"

    ```python
    tbl.create_index()
    ```

=== "Javascript"

    ```javascript
    await tbl.createIndex({})
    ```

=== "Rust"

    ```rust
    tbl.create_index(&["vector"]).build().await.unwrap()
    ```

Check [Approximate Nearest Neighbor (ANN) Indexes](/ann_indices.md) section for more details.

## How to delete rows from a table

Use the `delete()` method on tables to delete rows from a table. To choose
which rows to delete, provide a filter that matches on the metadata columns.
This can delete any number of rows that match the filter.

=== "Python"

    ```python
    tbl.delete('item = "fizz"')
    ```

=== "Javascript"

    ```javascript
    await tbl.delete('item = "fizz"')
    ```

=== "Rust"

    ```rust
    tbl.delete("item = \"fizz\"").await.unwrap();
    ```

The deletion predicate is a SQL expression that supports the same expressions
as the `where()` clause on a search. They can be as simple or complex as needed.
To see what expressions are supported, see the [SQL filters](sql.md) section.

=== "Python"

      Read more: [lancedb.table.Table.delete][]

=== "Javascript"

      Read more: [vectordb.Table.delete](javascript/interfaces/Table.md#delete)

## How to remove a table

Use the `drop_table()` method on the database to remove a table.

=== "Python"

      ```python
      db.drop_table("my_table")
      ```

      This permanently removes the table and is not recoverable, unlike deleting rows.
      By default, if the table does not exist an exception is raised. To suppress this,
      you can pass in `ignore_missing=True`.

=== "JavaScript"

      ```javascript
      await db.dropTable('myTable')
      ```

      This permanently removes the table and is not recoverable, unlike deleting rows.
      If the table does not exist an exception is raised.

=== "Rust"

    ```rust
    db.drop_table("my_table").await.unwrap()
    ```

!!! note "Bundling `vectordb` apps with Webpack"

    If you're using the `vectordb` module in JavaScript, since LanceDB contains a prebuilt Node binary, you must configure `next.config.js` to exclude it from webpack. This is required for both using Next.js and deploying a LanceDB app on Vercel.

    ```javascript
    /** @type {import('next').NextConfig} */
    module.exports = ({
    webpack(config) {
        config.externals.push({ vectordb: 'vectordb' })
        return config;
    }
    })
    ```

## What's next

This section covered the very basics of using LanceDB. If you're learning about vector databases for the first time, you may want to read the page on [indexing](concepts/index_ivfpq.md) to get familiar with the concepts.

If you've already worked with other vector databases, you may want to read the [guides](guides/tables.md) to learn how to work with LanceDB in more detail.
