[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / [embedding](../modules/embedding.md) / OpenAIEmbeddingFunction

# Class: OpenAIEmbeddingFunction

[embedding](../modules/embedding.md).OpenAIEmbeddingFunction

An embedding function that automatically creates vector representation for a given column.

## Implements

- [`EmbeddingFunction`](../interfaces/embedding.EmbeddingFunction.md)\<`string`\>

## Table of contents

### Constructors

- [constructor](embedding.OpenAIEmbeddingFunction.md#constructor)

### Properties

- [\_modelName](embedding.OpenAIEmbeddingFunction.md#_modelname)
- [\_openai](embedding.OpenAIEmbeddingFunction.md#_openai)
- [sourceColumn](embedding.OpenAIEmbeddingFunction.md#sourcecolumn)

### Methods

- [embed](embedding.OpenAIEmbeddingFunction.md#embed)

## Constructors

### constructor

• **new OpenAIEmbeddingFunction**(`sourceColumn`, `openAIKey`, `modelName?`): [`OpenAIEmbeddingFunction`](embedding.OpenAIEmbeddingFunction.md)

#### Parameters

| Name | Type | Default value |
| :------ | :------ | :------ |
| `sourceColumn` | `string` | `undefined` |
| `openAIKey` | `string` | `undefined` |
| `modelName` | `string` | `"text-embedding-ada-002"` |

#### Returns

[`OpenAIEmbeddingFunction`](embedding.OpenAIEmbeddingFunction.md)

#### Defined in

[embedding/openai.ts:22](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/embedding/openai.ts#L22)

## Properties

### \_modelName

• `Private` `Readonly` **\_modelName**: `string`

#### Defined in

[embedding/openai.ts:20](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/embedding/openai.ts#L20)

___

### \_openai

• `Private` `Readonly` **\_openai**: `OpenAI`

#### Defined in

[embedding/openai.ts:19](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/embedding/openai.ts#L19)

___

### sourceColumn

• **sourceColumn**: `string`

The name of the column that will be used as input for the Embedding Function.

#### Implementation of

[EmbeddingFunction](../interfaces/embedding.EmbeddingFunction.md).[sourceColumn](../interfaces/embedding.EmbeddingFunction.md#sourcecolumn)

#### Defined in

[embedding/openai.ts:61](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/embedding/openai.ts#L61)

## Methods

### embed

▸ **embed**(`data`): `Promise`\<`number`[][]\>

Creates a vector representation for the given values.

#### Parameters

| Name | Type |
| :------ | :------ |
| `data` | `string`[] |

#### Returns

`Promise`\<`number`[][]\>

#### Implementation of

[EmbeddingFunction](../interfaces/embedding.EmbeddingFunction.md).[embed](../interfaces/embedding.EmbeddingFunction.md#embed)

#### Defined in

[embedding/openai.ts:48](https://github.com/lancedb/lancedb/blob/9d178c7/nodejs/lancedb/embedding/openai.ts#L48)
