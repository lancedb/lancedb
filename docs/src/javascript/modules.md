[vectordb](README.md) / Exports

# vectordb

## Table of contents

### Enumerations

- [MetricType](enums/MetricType.md)
- [WriteMode](enums/WriteMode.md)

### Classes

- [LocalConnection](classes/LocalConnection.md)
- [LocalTable](classes/LocalTable.md)
- [OpenAIEmbeddingFunction](classes/OpenAIEmbeddingFunction.md)
- [Query](classes/Query.md)

### Interfaces

- [AwsCredentials](interfaces/AwsCredentials.md)
- [Connection](interfaces/Connection.md)
- [ConnectionOptions](interfaces/ConnectionOptions.md)
- [EmbeddingFunction](interfaces/EmbeddingFunction.md)
- [IvfPQIndexConfig](interfaces/IvfPQIndexConfig.md)
- [Table](interfaces/Table.md)

### Type Aliases

- [VectorIndexParams](modules.md#vectorindexparams)

### Functions

- [connect](modules.md#connect)

## Type Aliases

### VectorIndexParams

Ƭ **VectorIndexParams**: [`IvfPQIndexConfig`](interfaces/IvfPQIndexConfig.md)

#### Defined in

[index.ts:431](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L431)

## Functions

### connect

▸ **connect**(`uri`): `Promise`<[`Connection`](interfaces/Connection.md)\>

Connect to a LanceDB instance at the given URI

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `uri` | `string` | The uri of the database. |

#### Returns

`Promise`<[`Connection`](interfaces/Connection.md)\>

#### Defined in

[index.ts:47](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L47)

▸ **connect**(`opts`): `Promise`<[`Connection`](interfaces/Connection.md)\>

#### Parameters

| Name | Type |
| :------ | :------ |
| `opts` | `Partial`<[`ConnectionOptions`](interfaces/ConnectionOptions.md)\> |

#### Returns

`Promise`<[`Connection`](interfaces/Connection.md)\>

#### Defined in

[index.ts:48](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/index.ts#L48)
