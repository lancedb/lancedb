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
