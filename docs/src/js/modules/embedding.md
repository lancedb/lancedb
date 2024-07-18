[@lancedb/lancedb](../README.md) / [Exports](../modules.md) / embedding

# Namespace: embedding

## Table of contents

### Classes

- [EmbeddingFunction](../classes/embedding.EmbeddingFunction.md)
- [EmbeddingFunctionRegistry](../classes/embedding.EmbeddingFunctionRegistry.md)
- [OpenAIEmbeddingFunction](../classes/embedding.OpenAIEmbeddingFunction.md)

### Interfaces

- [EmbeddingFunctionConfig](../interfaces/embedding.EmbeddingFunctionConfig.md)

### Type Aliases

- [OpenAIOptions](embedding.md#openaioptions)

### Functions

- [LanceSchema](embedding.md#lanceschema)
- [getRegistry](embedding.md#getregistry)
- [register](embedding.md#register)

## Type Aliases

### OpenAIOptions

Ƭ **OpenAIOptions**: `Object`

#### Type declaration

| Name | Type |
| :------ | :------ |
| `apiKey` | `string` |
| `model` | `EmbeddingCreateParams`[``"model"``] |

#### Defined in

[embedding/openai.ts:21](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/openai.ts#L21)

## Functions

### LanceSchema

▸ **LanceSchema**(`fields`): `Schema`

Create a schema with embedding functions.

#### Parameters

| Name | Type |
| :------ | :------ |
| `fields` | `Record`\<`string`, `object` \| [`object`, `Map`\<`string`, [`EmbeddingFunction`](../classes/embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\>]\> |

#### Returns

`Schema`

Schema

**`Example`**

```ts
class MyEmbeddingFunction extends EmbeddingFunction {
// ...
}
const func = new MyEmbeddingFunction();
const schema = LanceSchema({
  id: new Int32(),
  text: func.sourceField(new Utf8()),
  vector: func.vectorField(),
  // optional: specify the datatype and/or dimensions
  vector2: func.vectorField({ datatype: new Float32(), dims: 3}),
});

const table = await db.createTable("my_table", data, { schema });
```

#### Defined in

[embedding/index.ts:49](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/index.ts#L49)

___

### getRegistry

▸ **getRegistry**(): [`EmbeddingFunctionRegistry`](../classes/embedding.EmbeddingFunctionRegistry.md)

Utility function to get the global instance of the registry

#### Returns

[`EmbeddingFunctionRegistry`](../classes/embedding.EmbeddingFunctionRegistry.md)

`EmbeddingFunctionRegistry` The global instance of the registry

**`Example`**

```ts
const registry = getRegistry();
const openai = registry.get("openai").create();

#### Defined in

[embedding/registry.ts:180](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/registry.ts#L180)

___

### register

▸ **register**(`name?`): (`ctor`: `EmbeddingFunctionConstructor`\<[`EmbeddingFunction`](../classes/embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\>) => `any`

#### Parameters

| Name | Type |
| :------ | :------ |
| `name?` | `string` |

#### Returns

`fn`

▸ (`ctor`): `any`

##### Parameters

| Name | Type |
| :------ | :------ |
| `ctor` | `EmbeddingFunctionConstructor`\<[`EmbeddingFunction`](../classes/embedding.EmbeddingFunction.md)\<`any`, `FunctionOptions`\>\> |

##### Returns

`any`

#### Defined in

[embedding/registry.ts:168](https://github.com/universalmind303/lancedb/blob/833b375/nodejs/lancedb/embedding/registry.ts#L168)
