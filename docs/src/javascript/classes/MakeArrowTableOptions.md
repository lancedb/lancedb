[vectordb](../README.md) / [Exports](../modules.md) / MakeArrowTableOptions

# Class: MakeArrowTableOptions

Options to control the makeArrowTable call.

## Table of contents

### Constructors

- [constructor](MakeArrowTableOptions.md#constructor)

### Properties

- [schema](MakeArrowTableOptions.md#schema)
- [vectorColumns](MakeArrowTableOptions.md#vectorcolumns)

## Constructors

### constructor

• **new MakeArrowTableOptions**(`values?`)

#### Parameters

| Name | Type |
| :------ | :------ |
| `values?` | `Partial`\<[`MakeArrowTableOptions`](MakeArrowTableOptions.md)\> |

#### Defined in

[arrow.ts:56](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/arrow.ts#L56)

## Properties

### schema

• `Optional` **schema**: `Schema`\<`any`\>

Provided schema.

#### Defined in

[arrow.ts:49](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/arrow.ts#L49)

___

### vectorColumns

• **vectorColumns**: `Record`\<`string`, `VectorColumnOptions`\>

Vector columns

#### Defined in

[arrow.ts:52](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/arrow.ts#L52)
