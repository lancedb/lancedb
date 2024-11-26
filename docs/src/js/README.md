**@lancedb/lancedb** • **Docs**

***

# LanceDB JavaScript SDK

A JavaScript library for [LanceDB](https://github.com/lancedb/lancedb).

## Installation

```bash
npm install @lancedb/lancedb
```

This will download the appropriate native library for your platform. We currently
support:

- Linux (x86_64 and aarch64)
- MacOS (Intel and ARM/M1/M2)
- Windows (x86_64 only)

We do not yet support musl-based Linux (such as Alpine Linux) or aarch64 Windows.

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

The [quickstart](../basic.md) contains a more complete example.

## Development

```sh
npm run build
npm run test
```

### Running lint / format

LanceDb uses [biome](https://biomejs.dev/) for linting and formatting. if you are using VSCode you will need to install the official [Biome](https://marketplace.visualstudio.com/items?itemName=biomejs.biome) extension.
To manually lint your code you can run:

```sh
npm run lint
```

to automatically fix all fixable issues:

```sh
npm run lint-fix
```

If you do not have your workspace root set to the `nodejs` directory, unfortunately the extension will not work. You can still run the linting and formatting commands manually.

### Generating docs

```sh
npm run docs

cd ../docs
# Asssume the virtual environment was created
# python3 -m venv venv
# pip install -r requirements.txt
. ./venv/bin/activate
mkdocs build
```

## Namespaces

- [embedding](namespaces/embedding/README.md)

## Enumerations

- [WriteMode](enumerations/WriteMode.md)

## Classes

- [Connection](classes/Connection.md)
- [Index](classes/Index.md)
- [MakeArrowTableOptions](classes/MakeArrowTableOptions.md)
- [Query](classes/Query.md)
- [QueryBase](classes/QueryBase.md)
- [RecordBatchIterator](classes/RecordBatchIterator.md)
- [Table](classes/Table.md)
- [VectorColumnOptions](classes/VectorColumnOptions.md)
- [VectorQuery](classes/VectorQuery.md)

## Interfaces

- [AddColumnsSql](interfaces/AddColumnsSql.md)
- [AddDataOptions](interfaces/AddDataOptions.md)
- [ClientConfig](interfaces/ClientConfig.md)
- [ColumnAlteration](interfaces/ColumnAlteration.md)
- [ConnectionOptions](interfaces/ConnectionOptions.md)
- [CreateTableOptions](interfaces/CreateTableOptions.md)
- [ExecutableQuery](interfaces/ExecutableQuery.md)
- [IndexConfig](interfaces/IndexConfig.md)
- [IndexOptions](interfaces/IndexOptions.md)
- [IndexStatistics](interfaces/IndexStatistics.md)
- [IvfPqOptions](interfaces/IvfPqOptions.md)
- [OptimizeOptions](interfaces/OptimizeOptions.md)
- [RetryConfig](interfaces/RetryConfig.md)
- [TableNamesOptions](interfaces/TableNamesOptions.md)
- [TimeoutConfig](interfaces/TimeoutConfig.md)
- [UpdateOptions](interfaces/UpdateOptions.md)
- [WriteOptions](interfaces/WriteOptions.md)

## Type Aliases

- [Data](type-aliases/Data.md)

## Functions

- [connect](functions/connect.md)
- [makeArrowTable](functions/makeArrowTable.md)
