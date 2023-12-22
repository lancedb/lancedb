[vectordb](../README.md) / [Exports](../saas-modules.md) / RemoteTable

# Class: RemoteTable<T\>

A LanceDB Table is the collection of Records. Each Record has one or more vector fields.

## Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

## Implements

- [`Table`](../interfaces/Table.md)<`T`\>

## Table of contents

### Constructors

- [constructor](RemoteTable.md#constructor)

### Properties

- [\_name](RemoteTable.md#_name)
- [\_client](RemoteTable.md#_client)
- [\_embeddings](RemoteTable.md#_embeddings)

### Accessors

- [name](RemoteTable.md#name)

### Methods

- [add](RemoteTable.md#add)
- [countRows](RemoteTable.md#countrows)
- [createIndex](RemoteTable.md#createindex)
- [delete](RemoteTable.md#delete)
- [listIndices](classes/RemoteTable.md#listindices)
- [indexStats](classes/RemoteTable.md#liststats)
- [overwrite](RemoteTable.md#overwrite)
- [search](RemoteTable.md#search)
- [schema](classes/RemoteTable.md#schema)
- [update](RemoteTable.md#update)

## Constructors

### constructor

• **new RemoteTable**<`T`\>(`client`, `name`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type |
| :------ | :------ |
| `client` | `HttpLancedbClient` |
| `name` | `string` |

#### Defined in

[remote/index.ts:186](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L186)

• **new RemoteTable**<`T`\>(`client`, `name`, `embeddings`)

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `number`[] |

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `client` | `HttpLancedbClient` |  |
| `name` | `string` |  |
| `embeddings` | [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`T`\> | An embedding function to use when interacting with this table |

#### Defined in

[remote/index.ts:187](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L187)

## Accessors

### name

• `get` **name**(): `string`

#### Returns

`string`

#### Implementation of

[Table](../interfaces/Table.md).[name](../interfaces/Table.md#name)

#### Defined in

[remote/index.ts:194](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L194)

## Methods

### add

▸ **add**(`data`): `Promise`<`number`\>

Insert records into this Table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[add](../interfaces/Table.md#add)

#### Defined in

[remote/index.ts:293](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L293)

___

### countRows

▸ **countRows**(): `Promise`<`number`\>

Returns the number of rows in this table.

#### Returns

`Promise`<`number`\>

#### Implementation of

[Table](../interfaces/Table.md).[countRows](../interfaces/Table.md#countrows)

#### Defined in

[remote/index.ts:290](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L290)

___

### createIndex

▸ **createIndex**(`metric_type`, `column`): `Promise`<`any`\>

Create an ANN index on this Table vector index.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `metric_type` | `string` | distance metric type, L2 or cosine or dot |
| `column` | `string` | the name of the column to be indexed |

#### Returns

`Promise`<`any`\>

#### Implementation of

[Table](../interfaces/Table.md).[createIndex](../interfaces/Table.md#createindex)

#### Defined in

[remote/index.ts:249](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L249)

___

### delete

▸ **delete**(`filter`): `Promise`<`void`\>

Delete rows from this table.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `filter` | `string` | A filter in the same format used by a sql WHERE clause. |

#### Returns

`Promise`<`void`\>

#### Implementation of

[Table](../interfaces/Table.md).[delete](../interfaces/Table.md#delete)

#### Defined in

[remote/index.ts:295](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L295)

___

### overwrite

▸ **overwrite**(`data`): `Promise`<`number`\>

Insert records into this Table, replacing its contents.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`<`string`, `unknown`\>[] | Records to be inserted into the Table |

#### Returns

`Promise`<`number`\>

The number of rows added to the table

#### Implementation of

[Table](../interfaces/Table.md).[overwrite](../interfaces/Table.md#overwrite)

#### Defined in

[remote/index.ts:231](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L231)

___

### search

▸ **search**(`query`): [`Query`](Query.md)<`T`\>

Creates a search query to find the nearest neighbors of the given search term

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `query` | `T` | The query search term |

#### Returns

[`Query`](Query.md)<`T`\>

#### Implementation of

[Table](../interfaces/Table.md).[search](../interfaces/Table.md#search)

#### Defined in

[remote/index.ts:209](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L209)

___

### update

▸ **update**(`args`): `Promise`<`void`\>

Update zero to all rows depending on how many rows match the where clause.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `args` | `UpdateArgs` or `UpdateSqlArgs` | The query search arguments |

#### Returns

`Promise`<`any`\>

#### Implementation of

[Table](../interfaces/Table.md).[search](../interfaces/Table.md#update)

#### Defined in

[remote/index.ts:299](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L299)

___

### schema

▸ **schema**(): `Promise`<`void`\>

Get the schema of the table


#### Returns

`Promise`<`any`\>

#### Implementation of

[Table](../interfaces/Table.md).[search](../interfaces/Table.md#schema)

#### Defined in

[remote/index.ts:198](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L198)

___

### listIndices

▸ **listIndices**(): `Promise`<`void`\>

List the indices of the table


#### Returns

`Promise`<`any`\>

#### Implementation of

[Table](../interfaces/Table.md).[search](../interfaces/Table.md#listIndices)

#### Defined in

[remote/index.ts:319](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L319)

___

### indexStats

▸ **indexStats**(`indexUuid`): `Promise`<`void`\>

Get the indexed/unindexed of rows from the table

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `indexUuid` | `string`  | the uuid of the index |

#### Returns

`Promise`<`numIndexedRows`\>
`Promise`<`numUnindexedRows`\>

#### Implementation of

[Table](../interfaces/Table.md).[search](../interfaces/Table.md#indexStats)

#### Defined in

[remote/index.ts:328](https://github.com/lancedb/lancedb/blob/main/node/src/remote/index.ts#L328)