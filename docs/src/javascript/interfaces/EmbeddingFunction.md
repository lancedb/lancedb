[vectordb](../README.md) / [Exports](../modules.md) / EmbeddingFunction

# Interface: EmbeddingFunction<T\>

An embedding function that automatically creates vector representation for a given column.

## Type parameters

| Name |
| :------ |
| `T` |

## Implemented by

- [`OpenAIEmbeddingFunction`](../classes/OpenAIEmbeddingFunction.md)

## Table of contents

### Properties

- [embed](EmbeddingFunction.md#embed)
- [sourceColumn](EmbeddingFunction.md#sourcecolumn)

## Properties

### embed

• **embed**: (`data`: `T`[]) => `Promise`<`number`[][]\>

#### Type declaration

▸ (`data`): `Promise`<`number`[][]\>

Creates a vector representation for the given values.

##### Parameters

| Name | Type |
| :------ | :------ |
| `data` | `T`[] |

##### Returns

`Promise`<`number`[][]\>

#### Defined in

[embedding/embedding_function.ts:27](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/embedding/embedding_function.ts#L27)

___

### sourceColumn

• **sourceColumn**: `string`

The name of the column that will be used as input for the Embedding Function.

#### Defined in

[embedding/embedding_function.ts:22](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/embedding/embedding_function.ts#L22)
