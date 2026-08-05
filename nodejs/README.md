# LanceDB JavaScript SDK

A JavaScript library for [LanceDB](https://github.com/lancedb/lancedb).

## Installation

```bash
npm install @lancedb/lancedb
```

This will download the appropriate native library for your platform. We currently
support:

- Linux (x86_64 and aarch64 on glibc and musl)
- MacOS (Intel and ARM/M1/M2)
- Windows (x86_64 and aarch64)

## Usage

### Basic Example

```javascript
import * as lancedb from "@lancedb/lancedb";
const db = await lancedb.connect("data/sample-lancedb");
const table = await db.createTable("my_table", [
  { id: 1, vector: [0.1, 1.0], item: "foo", price: 10.0 },
  { id: 2, vector: [3.9, 0.5], item: "bar", price: 20.0 },
]);
const results = await table.vectorSearch([0.1, 0.3]).limit(20).toArray();
console.log(results);
```

### Use an Existing Table with LangChain

When wrapping an existing table with `@langchain/community`, open the table
with LanceDB and pass the resulting table handle to LangChain. The LangChain
`uri` and `tableName` options are used when creating a table; they do not open
an existing table for search.

```javascript
import { LanceDB as LangChainLanceDB } from "@langchain/community/vectorstores/lancedb";
import * as lancedb from "@lancedb/lancedb";

const db = await lancedb.connect("data/sample-lancedb");
const table = await db.openTable("my_table");
const vectorStore = new LangChainLanceDB(embeddings, { table });

const results = await vectorStore.similaritySearchVectorWithScore(
  queryVector,
  5,
);
```

The [quickstart](https://docs.lancedb.com/quickstart/) contains more complete examples.

## Development

See [CONTRIBUTING.md](./CONTRIBUTING.md) for information on how to contribute to LanceDB.
