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

[index.ts:79](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L79)

___

### embeddingFunction

• `Optional` **embeddingFunction**: [`EmbeddingFunction`](EmbeddingFunction.md)\<`T`\>

#### Defined in

[index.ts:85](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L85)

___

### name

• **name**: `string`

#### Defined in

[index.ts:76](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L76)

___

### schema

• `Optional` **schema**: `Schema`\<`any`\>

#### Defined in

[index.ts:82](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L82)

___

### writeOptions

• `Optional` **writeOptions**: [`WriteOptions`](WriteOptions.md)

#### Defined in

[index.ts:88](https://github.com/lancedb/lancedb/blob/7856a94/node/src/index.ts#L88)
