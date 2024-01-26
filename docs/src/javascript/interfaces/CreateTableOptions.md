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

[index.ts:121](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L121)

___

### embeddingFunction

• `Optional` **embeddingFunction**: [`EmbeddingFunction`](EmbeddingFunction.md)\<`T`\>

#### Defined in

[index.ts:127](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L127)

___

### name

• **name**: `string`

#### Defined in

[index.ts:118](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L118)

___

### schema

• `Optional` **schema**: `Schema`\<`any`\>

#### Defined in

[index.ts:124](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L124)

___

### writeOptions

• `Optional` **writeOptions**: [`WriteOptions`](WriteOptions.md)

#### Defined in

[index.ts:130](https://github.com/lancedb/lancedb/blob/5228ca4/node/src/index.ts#L130)
