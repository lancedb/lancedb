[vectordb](../README.md) / [Exports](../modules.md) / OpenAIEmbeddingFunction

# Class: OpenAIEmbeddingFunction

An embedding function that automatically creates vector representation for a given column.

## Implements

- [`EmbeddingFunction`](../interfaces/EmbeddingFunction.md)<`string`\>

## Table of contents

### Constructors

- [constructor](OpenAIEmbeddingFunction.md#constructor)

### Properties

- [\_modelName](OpenAIEmbeddingFunction.md#_modelname)
- [\_openai](OpenAIEmbeddingFunction.md#_openai)
- [sourceColumn](OpenAIEmbeddingFunction.md#sourcecolumn)

### Methods

- [embed](OpenAIEmbeddingFunction.md#embed)

## Constructors

### constructor

• **new OpenAIEmbeddingFunction**(`sourceColumn`, `openAIKey`, `modelName?`)

#### Parameters

| Name | Type | Default value |
| :------ | :------ | :------ |
| `sourceColumn` | `string` | `undefined` |
| `openAIKey` | `string` | `undefined` |
| `modelName` | `string` | `'text-embedding-ada-002'` |

#### Defined in

[embedding/openai.ts:21](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/embedding/openai.ts#L21)

## Properties

### \_modelName

• `Private` `Readonly` **\_modelName**: `string`

#### Defined in

[embedding/openai.ts:19](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/embedding/openai.ts#L19)

___

### \_openai

• `Private` `Readonly` **\_openai**: `any`

#### Defined in

[embedding/openai.ts:18](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/embedding/openai.ts#L18)

___

### sourceColumn

• **sourceColumn**: `string`

The name of the column that will be used as input for the Embedding Function.

#### Implementation of

[EmbeddingFunction](../interfaces/EmbeddingFunction.md).[sourceColumn](../interfaces/EmbeddingFunction.md#sourcecolumn)

#### Defined in

[embedding/openai.ts:50](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/embedding/openai.ts#L50)

## Methods

### embed

▸ **embed**(`data`): `Promise`<`number`[][]\>

Creates a vector representation for the given values.

#### Parameters

| Name | Type |
| :------ | :------ |
| `data` | `string`[] |

#### Returns

`Promise`<`number`[][]\>

#### Implementation of

[EmbeddingFunction](../interfaces/EmbeddingFunction.md).[embed](../interfaces/EmbeddingFunction.md#embed)

#### Defined in

[embedding/openai.ts:38](https://github.com/lancedb/lancedb/blob/b1eeb90/node/src/embedding/openai.ts#L38)
