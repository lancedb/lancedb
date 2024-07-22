# Rust-backed Client Migration Guide

In an effort to ensure all clients have the same set of capabilities we have begun migrating the
python and node clients onto a common Rust base library. In python, this new client is part of
the same lancedb package, exposed as an asynchronous client. Once the asynchronous client has
reached full functionality we will begin migrating the synchronous library to be a thin wrapper
around the asynchronous client.

This guide describes the differences between the two APIs and will hopefully assist users
that would like to migrate to the new API.

## Python
### Closeable Connections

The Connection now has a `close` method. You can call this when
you are done with the connection to eagerly free resources. Currently
this is limited to freeing/closing the HTTP connection for remote
connections. In the future we may add caching or other resources to
native connections so this is probably a good practice even if you
aren't using remote connections.

In addition, the connection can be used as a context manager which may
be a more convenient way to ensure the connection is closed.

```python
import lancedb

async def my_async_fn():
    with await lancedb.connect_async("my_uri") as db:
        print(await db.table_names())
```

It is not mandatory to call the `close` method. If you do not call it
then the connection will be closed when the object is garbage collected.

### Closeable Table

The Table now also has a `close` method, similar to the connection. This
can be used to eagerly free the cache used by a Table object. Similar to
the connection, it can be used as a context manager and it is not mandatory
to call the `close` method.

#### Changes to Table APIs

- Previously `Table.schema` was a property. Now it is an async method.
- The method `Table.__len__` was removed and `len(table)` will no longer
  work. Use `Table.count_rows` instead.

#### Creating Indices

The `Table.create_index` method is now used for creating both vector indices
and scalar indices. It currently requires a column name to be specified (the
column to index). Vector index defaults are now smarter and scale better with
the size of the data.

To specify index configuration details you will need to specify which kind of
index you are using.

#### Querying

The `Table.search` method has been renamed to `AsyncTable.vector_search` for
clarity.

### Features not yet supported

The following features are not yet supported by the asynchronous API. However,
we plan to support them soon.

- You cannot specify an embedding function when creating or opening a table.
  You must calculate embeddings yourself if using the asynchronous API
- The merge insert operation is not supported in the asynchronous API
- Cleanup / compact / optimize indices are not supported in the asynchronous API
- add / alter columns is not supported in the asynchronous API
- The asynchronous API does not yet support any full text search or reranking
  search
- Remote connections to LanceDb Cloud are not yet supported.
- The method Table.head is not yet supported.

## TypeScript/JavaScript

For JS/TS users, we offer a brand new SDK [@lancedb/lancedb](https://www.npmjs.com/package/@lancedb/lancedb)

We tried to keep the API as similar as possible to the previous version, but there are a few small changes. Here are the most important ones:

### Creating Tables

[CreateTableOptions.writeOptions.writeMode](./javascript/interfaces/WriteOptions.md#writemode) has been replaced with [CreateTableOptions.mode](./js/interfaces/CreateTableOptions.md#mode)

=== "vectordb (deprecated)"

    ```ts
    db.createTable(tableName, data, { writeMode: lancedb.WriteMode.Overwrite });
    ```

=== "@lancedb/lancedb"

    ```ts
    db.createTable(tableName, data, { mode: "overwrite" })
    ```

### Changes to Table APIs

Previously `Table.schema` was a property. Now it is an async method.

#### Creating Indices

The `Table.createIndex` method is now used for creating both vector indices
and scalar indices. It currently requires a column name to be specified (the
column to index). Vector index defaults are now smarter and scale better with
the size of the data.

=== "vectordb (deprecated)"

    ```ts
    await tbl.createIndex({
      column: "vector", // default
      type: "ivf_pq",
      num_partitions: 2,
      num_sub_vectors: 2,
    });
    ```

=== "@lancedb/lancedb"

    ```ts
    await table.createIndex("vector", {
      config: lancedb.Index.ivfPq({
        numPartitions: 2,
        numSubVectors: 2,
      }),
    });
    ```

### Embedding Functions

The embedding API has been completely reworked, and it now more closely resembles the Python API, including the new [embedding registry](./js/classes/embedding.EmbeddingFunctionRegistry.md)

=== "vectordb (deprecated)"

    ```ts

    const embeddingFunction = new lancedb.OpenAIEmbeddingFunction('text', API_KEY)
    const data = [
        { id: 1, text: 'Black T-Shirt', price: 10 },
        { id: 2, text: 'Leather Jacket', price: 50 }
    ]
    const table = await db.createTable('vectors', data, embeddingFunction)
    ```

=== "@lancedb/lancedb"

    ```ts
    import * as lancedb from "@lancedb/lancedb";
    import * as arrow from "apache-arrow";
    import { LanceSchema, getRegistry } from "@lancedb/lancedb/embedding";

    const func = getRegistry().get("openai").create({apiKey: API_KEY});

    const data = [
        { id: 1, text: 'Black T-Shirt', price: 10 },
        { id: 2, text: 'Leather Jacket', price: 50 }
    ]

    const table = await db.createTable('vectors', data, {
        embeddingFunction: {
            sourceColumn: "text",
            function: func,
        }
    })

    ```

You can also use a schema driven approach, which parallels the Pydantic integration in our Python SDK:

```ts
const func = getRegistry().get("openai").create({apiKey: API_KEY});

const data = [
    { id: 1, text: 'Black T-Shirt', price: 10 },
    { id: 2, text: 'Leather Jacket', price: 50 }
]
const schema = LanceSchema({
    id: new arrow.Int32(),
    text: func.sourceField(new arrow.Utf8()),
    price: new arrow.Float64(),
    vector: func.vectorField()
})

const table = await db.createTable('vectors', data, {schema})

```
