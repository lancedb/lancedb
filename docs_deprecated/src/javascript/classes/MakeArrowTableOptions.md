[vectordb](../README.md) / [Exports](../modules.md) / MakeArrowTableOptions

# Class: MakeArrowTableOptions

Options to control the makeArrowTable call.

## Table of contents

### Constructors

- [constructor](MakeArrowTableOptions.md#constructor)

### Properties

- [dictionaryEncodeStrings](MakeArrowTableOptions.md#dictionaryencodestrings)
- [embeddings](MakeArrowTableOptions.md#embeddings)
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

[arrow.ts:98](https://github.com/lancedb/lancedb/blob/92179835/node/src/arrow.ts#L98)

## Properties

### dictionaryEncodeStrings

• **dictionaryEncodeStrings**: `boolean` = `false`

If true then string columns will be encoded with dictionary encoding

Set this to true if your string columns tend to repeat the same values
often.  For more precise control use the `schema` property to specify the
data type for individual columns.

If `schema` is provided then this property is ignored.

#### Defined in

[arrow.ts:96](https://github.com/lancedb/lancedb/blob/92179835/node/src/arrow.ts#L96)

___

### embeddings

• `Optional` **embeddings**: [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)\<`any`\>

#### Defined in

[arrow.ts:85](https://github.com/lancedb/lancedb/blob/92179835/node/src/arrow.ts#L85)

___

### schema

• `Optional` **schema**: `Schema`\<`any`\>

#### Defined in

[arrow.ts:63](https://github.com/lancedb/lancedb/blob/92179835/node/src/arrow.ts#L63)

___

### vectorColumns

• **vectorColumns**: `Record`\<`string`, `VectorColumnOptions`\>

#### Defined in

[arrow.ts:81](https://github.com/lancedb/lancedb/blob/92179835/node/src/arrow.ts#L81)
