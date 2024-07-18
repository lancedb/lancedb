[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / MakeArrowTableOptions

# Class: MakeArrowTableOptions

Options to control the makeArrowTable call.

## Table of contents

### Constructors

- [constructor](MakeArrowTableOptions.md#constructor)

### Properties

- [dictionaryEncodeStrings](MakeArrowTableOptions.md#dictionaryencodestrings)
- [embeddingFunction](MakeArrowTableOptions.md#embeddingfunction)
- [embeddings](MakeArrowTableOptions.md#embeddings)
- [schema](MakeArrowTableOptions.md#schema)
- [vectorColumns](MakeArrowTableOptions.md#vectorcolumns)

## Constructors

### constructor

• **new MakeArrowTableOptions**(`values?`): [`MakeArrowTableOptions`](MakeArrowTableOptions.md)

#### Parameters

| Name | Type |
| :------ | :------ |
| `values?` | `Partial`\<[`MakeArrowTableOptions`](MakeArrowTableOptions.md)\> |

#### Returns

[`MakeArrowTableOptions`](MakeArrowTableOptions.md)

#### Defined in

[arrow.ts:259](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/arrow.ts#L259)

## Properties

### dictionaryEncodeStrings

• **dictionaryEncodeStrings**: `boolean` = `false`

If true then string columns will be encoded with dictionary encoding

Set this to true if your string columns tend to repeat the same values
often.  For more precise control use the `schema` property to specify the
data type for individual columns.

If `schema` is provided then this property is ignored.

#### Defined in

[arrow.ts:257](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/arrow.ts#L257)

___

### embeddingFunction

• `Optional` **embeddingFunction**: [`EmbeddingFunctionConfig`](../interfaces/embedding.EmbeddingFunctionConfig.md)

#### Defined in

[arrow.ts:246](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/arrow.ts#L246)

___

### embeddings

• `Optional` **embeddings**: [`EmbeddingFunction`](embedding.EmbeddingFunction.md)\<`unknown`, `FunctionOptions`\>

#### Defined in

[arrow.ts:245](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/arrow.ts#L245)

___

### schema

• `Optional` **schema**: `SchemaLike`

#### Defined in

[arrow.ts:224](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/arrow.ts#L224)

___

### vectorColumns

• **vectorColumns**: `Record`\<`string`, [`VectorColumnOptions`](VectorColumnOptions.md)\>

#### Defined in

[arrow.ts:242](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/arrow.ts#L242)
