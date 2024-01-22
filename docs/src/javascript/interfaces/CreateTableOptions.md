[vectordb](../README.md) / [Exports](../modules.md) / CreateTableOptions

# Interface: CreateTableOptions\<T\>

## Type parameters

| Name |
| :------ |
| `T` |

## Table of contents

### Properties

- [data](CreateTableOptions.md#data)
- [embeddingFunction](CreateTableOptions.md#embeddingfunction)
- [name](CreateTableOptions.md#name)
- [schema](CreateTableOptions.md#schema)
- [writeOptions](CreateTableOptions.md#writeoptions)

## Properties

### data

• `Optional` **data**: `Table`\<`any`\> \| `Record`\<`string`, `unknown`\>[]

#### Defined in

[index.ts:116](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L116)

___

### embeddingFunction

• `Optional` **embeddingFunction**: [`EmbeddingFunction`](EmbeddingFunction.md)\<`T`\>

#### Defined in

[index.ts:122](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L122)

___

### name

• **name**: `string`

#### Defined in

[index.ts:113](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L113)

___

### schema

• `Optional` **schema**: `Schema`\<`any`\>

#### Defined in

[index.ts:119](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L119)

___

### writeOptions

• `Optional` **writeOptions**: [`WriteOptions`](WriteOptions.md)

#### Defined in

[index.ts:125](https://github.com/lancedb/lancedb/blob/c89d5e6/node/src/index.ts#L125)
