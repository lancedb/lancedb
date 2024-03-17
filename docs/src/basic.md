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

=== "Typescript"

      ```shell
      npm install vectordb
      ```

=== "Rust"

    !!! warning "Rust SDK is experimental, might introduce breaking changes in the near future"

    ```shell
    cargo add vectordb
    ```

    !!! info "To use the vectordb create, you first need to install protobuf."

    === "macOS"

        ```shell
        brew install protobuf
        ```

    === "Ubuntu/Debian"

        ```shell
        sudo apt install -y protobuf-compiler libssl-dev
        ```

    !!! info "Please also make sure you're using the same version of Arrow as in the [vectordb crate](https://github.com/lancedb/lancedb/blob/main/Cargo.toml)"

## How to connect to a database

=== "Python"

      ```python
      import lancedb
      uri = "data/sample-lancedb"
      db = lancedb.connect(uri)
      ```

=== "Typescript"

    ```typescript
    --8<-- "docs/src/basic_legacy.ts:import"

    --8<-- "docs/src/basic_legacy.ts:open_db"
    ```

=== "Rust"

    ```rust
    #[tokio::main]
    async fn main() -> Result<()> {
        --8<-- "rust/vectordb/examples/simple.rs:connect"
    }
    ```

    !!! info "See [examples/simple.rs](https://github.com/lancedb/lancedb/tree/main/rust/vectordb/examples/simple.rs) for a full working example."

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

=== "Typescript"

    ```typescript
    --8<-- "docs/src/basic_legacy.ts:create_table"
    ```

    If the table already exists, LanceDB will raise an error by default.
    If you want to overwrite the table, you can pass in `mode="overwrite"`
    to the `createTable` function.

=== "Rust"

    ```rust
    use arrow_schema::{DataType, Schema, Field};
    use arrow_array::{RecordBatch, RecordBatchIterator};

    --8<-- "rust/vectordb/examples/simple.rs:create_table"
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

=== "Typescript"

    ```typescript
    --8<-- "docs/src/basic_legacy.ts:create_empty_table"
    ```

=== "Rust"

    ```rust
    --8<-- "rust/vectordb/examples/simple.rs:create_empty_table"
    ```

## How to open an existing table

Once created, you can open a table using the following code:

=== "Python"

    ```python
    tbl = db.open_table("my_table")
    ```

=== "Typescript"

    ```typescript
    const tbl = await db.openTable("myTable");
    ```

=== "Rust"

    ```rust
    --8<-- "rust/vectordb/examples/simple.rs:open_with_existing_file"
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
    --8<-- "rust/vectordb/examples/simple.rs:list_names"
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

=== "Typescript"

    ```typescript
    --8<-- "docs/src/basic_legacy.ts:add"
    ```

=== "Rust"

    ```rust
    --8<-- "rust/vectordb/examples/simple.rs:add"
    ```

## How to search for (approximate) nearest neighbors

Once you've embedded the query, you can find its nearest neighbors using the following code:

=== "Python"

    ```python
    tbl.search([100, 100]).limit(2).to_pandas()
    ```

    This returns a pandas DataFrame with the results.

=== "Typescript"

    ```typescript
    --8<-- "docs/src/basic_legacy.ts:search"
    ```

=== "Rust"

    ```rust
    use futures::TryStreamExt;

    --8<-- "rust/vectordb/examples/simple.rs:search"
    ```

By default, LanceDB runs a brute-force scan over dataset to find the K nearest neighbours (KNN).
For tables with more than 50K vectors, creating an ANN index is recommended to speed up search performance.

=== "Python"

    ```py
    tbl.create_index()
    ```

=== "Typescript"

    ```{.typescript .ignore}
    --8<-- "docs/src/basic_legacy.ts:create_index"
    ```

=== "Rust"

    ```rust
     --8<-- "rust/vectordb/examples/simple.rs:create_index"
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

=== "Typescript"

    ```typescript
    --8<-- "docs/src/basic_legacy.ts:delete"
    ```

=== "Rust"

    ```rust
    --8<-- "rust/vectordb/examples/simple.rs:delete"
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

=== "Typescript"

      ```typescript
      --8<-- "docs/src/basic_legacy.ts:drop_table"
      ```

      This permanently removes the table and is not recoverable, unlike deleting rows.
      If the table does not exist an exception is raised.

=== "Rust"

    ```rust
    --8<-- "rust/vectordb/examples/simple.rs:drop_table"
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
