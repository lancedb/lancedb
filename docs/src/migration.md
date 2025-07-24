# Rust-backed Client Migration Guide

In an effort to ensure all clients have the same set of capabilities we have
migrated the Python and Node clients onto a common Rust base library. In Python,
both the synchronous and asynchronous clients are based on this implementation.
In Node, the new client is available as `@lancedb/lancedb`, which replaces
the existing `vectordb` package.

This guide describes the differences between the two Node APIs and will assist users
that would like to migrate to the new API.

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

The embedding API has been completely reworked, and it now more closely resembles the Python API, including the new [embedding registry](./js/classes/embedding.EmbeddingFunctionRegistry.md):

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
