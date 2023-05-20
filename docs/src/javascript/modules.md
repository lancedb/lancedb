[vectordb](README.md) / Exports

# vectordb

## Table of contents

### Enumerations

- [WriteMode](enums/WriteMode.md)

### Classes

- [Connection](classes/Connection.md)
- [Query](classes/Query.md)
- [Table](classes/Table.md)

### Functions

- [connect](modules.md#connect)

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

[index.ts:30](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L30)
