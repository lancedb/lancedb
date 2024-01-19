[vectordb](README.md) / Exports

# vectordb

## Table of contents

### Enumerations

- [MetricType](enums/MetricType.md)
- [WriteMode](enums/WriteMode.md)

### Classes

- [DefaultWriteOptions](classes/DefaultWriteOptions.md)
- [LocalConnection](classes/LocalConnection.md)
- [LocalTable](classes/LocalTable.md)
- [OpenAIEmbeddingFunction](classes/OpenAIEmbeddingFunction.md)
- [Query](classes/Query.md)

### Interfaces

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

### Type Aliases

- [VectorIndexParams](modules.md#vectorindexparams)

### Functions

- [connect](modules.md#connect)
- [isWriteOptions](modules.md#iswriteoptions)

## Type Aliases

### VectorIndexParams

Ƭ **VectorIndexParams**: [`IvfPQIndexConfig`](interfaces/IvfPQIndexConfig.md)

#### Defined in

[index.ts:996](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L996)

## Functions

### connect

▸ **connect**(`uri`): `Promise`\<[`Connection`](interfaces/Connection.md)\>

Connect to a LanceDB instance at the given URI.

Accpeted formats:

- `/path/to/database` - local database
- `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
- `db://host:port` - remote database (SaaS)

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `uri` | `string` | The uri of the database. If the database uri starts with `db://` then it connects to a remote database. |

#### Returns

`Promise`\<[`Connection`](interfaces/Connection.md)\>

**`See`**

[ConnectionOptions](interfaces/ConnectionOptions.md) for more details on the URI format.

#### Defined in

[index.ts:141](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L141)

▸ **connect**(`opts`): `Promise`\<[`Connection`](interfaces/Connection.md)\>

Connect to a LanceDB instance with connection options.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `opts` | `Partial`\<[`ConnectionOptions`](interfaces/ConnectionOptions.md)\> | The [ConnectionOptions](interfaces/ConnectionOptions.md) to use when connecting to the database. |

#### Returns

`Promise`\<[`Connection`](interfaces/Connection.md)\>

#### Defined in

[index.ts:147](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L147)

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

[index.ts:1022](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L1022)
