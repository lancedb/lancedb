[vectordb](README.md) / Exports

# vectordb

## Table of contents

### Enumerations

- [MetricType](enums/MetricType.md)
- [WriteMode](enums/WriteMode.md)

### Classes

- [Connection](classes/Connection.md)
- [OpenAIEmbeddingFunction](classes/OpenAIEmbeddingFunction.md)
- [Query](classes/Query.md)
- [Table](classes/Table.md)

### Interfaces

- [EmbeddingFunction](interfaces/EmbeddingFunction.md)

### Type Aliases

- [VectorIndexParams](modules.md#vectorindexparams)

### Functions

- [connect](modules.md#connect)

## Type Aliases

### VectorIndexParams

Ƭ **VectorIndexParams**: `IvfPQIndexConfig`

#### Defined in

[index.ts:224](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L224)

## Functions

### connect

▸ **connect**(`uri`): `Promise`<[`Connection`](classes/Connection.md)\>

Connect to a LanceDB instance at the given URI

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `uri` | `string` | The uri of the database. |

#### Returns

`Promise`<[`Connection`](classes/Connection.md)\>

#### Defined in

[index.ts:34](https://github.com/lancedb/lancedb/blob/31dab97/node/src/index.ts#L34)
