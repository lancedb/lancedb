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

[index.ts:163](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L163)

___

### embeddingFunction

• `Optional` **embeddingFunction**: [`EmbeddingFunction`](EmbeddingFunction.md)\<`T`\>

#### Defined in

[index.ts:169](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L169)

___

### name

• **name**: `string`

#### Defined in

[index.ts:160](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L160)

___

### schema

• `Optional` **schema**: `Schema`\<`any`\>

#### Defined in

[index.ts:166](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L166)

___

### writeOptions

• `Optional` **writeOptions**: [`WriteOptions`](WriteOptions.md)

#### Defined in

[index.ts:172](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L172)
