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

- [Connection](interfaces/Connection.md)
- [EmbeddingFunction](interfaces/EmbeddingFunction.md)
- [Table](interfaces/Table.md)

### Type Aliases

- [VectorIndexParams](modules.md#vectorindexparams)

### Functions

- [connect](modules.md#connect)

## Type Aliases

### VectorIndexParams

Ƭ **VectorIndexParams**: `IvfPQIndexConfig`

#### Defined in

[index.ts:364](https://github.com/lancedb/lancedb/blob/20281c7/node/src/index.ts#L364)

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

[index.ts:34](https://github.com/lancedb/lancedb/blob/20281c7/node/src/index.ts#L34)
