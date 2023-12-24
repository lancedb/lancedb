[vectordb](README.md) / Exports

# JavaScript API Reference (OSS)

This section contains the API reference for the OSS JavaScript / Node.js API.

## Installation

```bash
npm install vectordb
```

This will download the appropriate native library for your platform. We currently
support x86_64 Linux, aarch64 Linux, Intel MacOS, and ARM (M1/M2) MacOS. We do not
yet support Windows or musl-based Linux (such as Alpine Linux).

## Usage

### Basic Example

```javascript
const lancedb = require('vectordb');
const db = await lancedb.connect('data/sample-lancedb');
const table = await db.createTable("my_table",
      [{ id: 1, vector: [0.1, 1.0], item: "foo", price: 10.0 },
      { id: 2, vector: [3.9, 0.5], item: "bar", price: 20.0 }])
const results = await table.search([0.1, 0.3]).limit(20).execute();
console.log(results);
```

The [examples](../examples/index.md) folder contains complete examples.

## Development

To build everything fresh:

```bash
npm install
npm run tsc
npm run build
```

Then you should be able to run the tests with:

```bash
npm test
```

### Rebuilding Rust library

```bash
npm run build
```

### Rebuilding Typescript

```bash
npm run tsc
```

### Fix lints

To run the linter and have it automatically fix all errors

```bash
npm run lint -- --fix
```

To build documentation

```bash
npx typedoc --plugin typedoc-plugin-markdown --out ../docs/src/javascript src/index.ts
```

## Classes

- [DefaultWriteOptions](classes/DefaultWriteOptions.md)
- [LocalConnection](classes/LocalConnection.md)
- [LocalTable](classes/LocalTable.md)
- [OpenAIEmbeddingFunction](classes/OpenAIEmbeddingFunction.md)
- [Query](classes/Query.md)

## Enumerations

- [MetricType](enums/MetricType.md)
- [WriteMode](enums/WriteMode.md)

## Interfaces

- [AwsCredentials](interfaces/AwsCredentials.md)
- [CleanupStats](interfaces/CleanupStats.md)
- [CompactionMetrics](interfaces/CompactionMetrics.md)
- [CompactionOptions](interfaces/CompactionOptions.md)
- [Connection](interfaces/Connection.md)
- [ConnectionOptions](interfaces/ConnectionOptions.md)
- [CreateTableOptions](interfaces/CreateTableOptions.md)
- [EmbeddingFunction](interfaces/EmbeddingFunction.md)
- [IndexStats](interfaces/IndexStats.md)
- [IvfPQIndexConfig](interfaces/IvfPQIndexConfig.md)
- [Table](interfaces/Table.md)
- [UpdateArgs](interfaces/UpdateArgs.md)
- [UpdateSqlArgs](interfaces/UpdateSqlArgs.md)
- [VectorIndex](interfaces/VectorIndex.md)
- [WriteOptions](interfaces/WriteOptions.md)

### Functions

- [connect](modules.md#connect)
- [isWriteOptions](modules.md#iswriteoptions)

## Type Aliases

### VectorIndexParams

Ƭ **VectorIndexParams**: [`IvfPQIndexConfig`](interfaces/IvfPQIndexConfig.md)

#### Defined in

[index.ts:755](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L755)

## Functions

### connect

▸ **connect**(`uri`): `Promise`\<[`Connection`](interfaces/Connection.md)\>

Connect to a LanceDB instance at the given URI

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `uri` | `string` | The uri of the database. |

#### Returns

`Promise`\<[`Connection`](interfaces/Connection.md)\>

#### Defined in

[index.ts:95](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L95)

▸ **connect**(`opts`): `Promise`\<[`Connection`](interfaces/Connection.md)\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `opts` | `Partial`\<[`ConnectionOptions`](interfaces/ConnectionOptions.md)\> |

#### Returns

`Promise`\<[`Connection`](interfaces/Connection.md)\>

#### Defined in

[index.ts:96](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L96)

___

### isWriteOptions

▸ **isWriteOptions**(`value`): value is WriteOptions

#### Parameters

| Name | Type |
| :------ | :------ |
| `value` | `any` |

#### Returns

value is WriteOptions

#### Defined in

[index.ts:781](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L781)
