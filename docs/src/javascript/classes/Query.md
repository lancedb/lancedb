[vectordb](../README.md) / [Exports](../modules.md) / Query

# Class: Query

A builder for nearest neighbor queries for LanceDB.

## Table of contents

### Constructors

- [constructor](Query.md#constructor)

### Properties

- [\_columns](Query.md#_columns)
- [\_filter](Query.md#_filter)
- [\_limit](Query.md#_limit)
- [\_metric](Query.md#_metric)
- [\_nprobes](Query.md#_nprobes)
- [\_query\_vector](Query.md#_query_vector)
- [\_refine\_factor](Query.md#_refine_factor)
- [\_tbl](Query.md#_tbl)

### Methods

- [execute](Query.md#execute)
- [filter](Query.md#filter)
- [limit](Query.md#limit)

## Constructors

### constructor

• **new Query**(`tbl`, `queryVector`)

#### Parameters

| Name | Type |
| :------ | :------ |
| `tbl` | `any` |
| `queryVector` | `number`[] |

#### Defined in

[index.ts:131](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L131)

## Properties

### \_columns

• `Private` `Optional` `Readonly` **\_columns**: `string`[]

#### Defined in

[index.ts:127](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L127)

___

### \_filter

• `Private` `Optional` **\_filter**: `string`

#### Defined in

[index.ts:128](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L128)

___

### \_limit

• `Private` **\_limit**: `number`

#### Defined in

[index.ts:124](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L124)

___

### \_metric

• `Private` `Readonly` **\_metric**: ``"L2"``

#### Defined in

[index.ts:129](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L129)

___

### \_nprobes

• `Private` `Readonly` **\_nprobes**: `number`

#### Defined in

[index.ts:126](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L126)

___

### \_query\_vector

• `Private` `Readonly` **\_query\_vector**: `number`[]

#### Defined in

[index.ts:123](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L123)

___

### \_refine\_factor

• `Private` `Optional` `Readonly` **\_refine\_factor**: `number`

#### Defined in

[index.ts:125](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L125)

___

### \_tbl

• `Private` `Readonly` **\_tbl**: `any`

#### Defined in

[index.ts:122](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L122)

## Methods

### execute

▸ **execute**<`T`\>(): `Promise`<`T`[]\>

Execute the query and return the results as an Array of Objects

#### Type parameters

| Name | Type |
| :------ | :------ |
| `T` | `Record`<`string`, `unknown`\> |

#### Returns

`Promise`<`T`[]\>

#### Defined in

[index.ts:154](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L154)

___

### filter

▸ **filter**(`value`): [`Query`](Query.md)

#### Parameters

| Name | Type |
| :------ | :------ |
| `value` | `string` |

#### Returns

[`Query`](Query.md)

#### Defined in

[index.ts:146](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L146)

___

### limit

▸ **limit**(`value`): [`Query`](Query.md)

#### Parameters

| Name | Type |
| :------ | :------ |
| `value` | `number` |

#### Returns

[`Query`](Query.md)

#### Defined in

[index.ts:141](https://github.com/lancedb/lancedb/blob/e234a3e/node/src/index.ts#L141)
