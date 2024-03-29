[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / MakeArrowTableOptions

# Class: MakeArrowTableOptions

Options to control the makeArrowTable call.

## Table of contents

### Constructors

- [constructor](MakeArrowTableOptions.md#constructor)

### Properties

- [dictionaryEncodeStrings](MakeArrowTableOptions.md#dictionaryencodestrings)
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

[arrow.ts:100](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/arrow.ts#L100)

## Properties

### dictionaryEncodeStrings

• **dictionaryEncodeStrings**: `boolean` = `false`

If true then string columns will be encoded with dictionary encoding

Set this to true if your string columns tend to repeat the same values
often.  For more precise control use the `schema` property to specify the
data type for individual columns.

If `schema` is provided then this property is ignored.

#### Defined in

[arrow.ts:98](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/arrow.ts#L98)

___

### schema

• `Optional` **schema**: `Schema`\<`any`\>

#### Defined in

[arrow.ts:67](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/arrow.ts#L67)

___

### vectorColumns

• **vectorColumns**: `Record`\<`string`, [`VectorColumnOptions`](VectorColumnOptions.md)\>

#### Defined in

[arrow.ts:85](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/arrow.ts#L85)
