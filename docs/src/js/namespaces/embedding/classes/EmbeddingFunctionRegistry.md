[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / EmbeddingFunctionRegistry

# Class: EmbeddingFunctionRegistry

This is a singleton class used to register embedding functions
and fetch them by name. It also handles serializing and deserializing.
You can implement your own embedding function by subclassing EmbeddingFunction
or TextEmbeddingFunction and registering it with the registry

## Constructors

### new EmbeddingFunctionRegistry()

> **new EmbeddingFunctionRegistry**(): [`EmbeddingFunctionRegistry`](EmbeddingFunctionRegistry.md)

#### Returns

[`EmbeddingFunctionRegistry`](EmbeddingFunctionRegistry.md)

## Methods

### functionToMetadata()

> **functionToMetadata**(`conf`): `Record`&lt;`string`, `any`&gt;

#### Parameters

• **conf**: [`EmbeddingFunctionConfig`](../interfaces/EmbeddingFunctionConfig.md)

#### Returns

`Record`&lt;`string`, `any`&gt;

***

### get()

> **get**&lt;`T`, `Name`&gt;(`name`): `Name` *extends* `"openai"` ? `EmbeddingFunctionCreate`&lt;[`OpenAIEmbeddingFunction`](OpenAIEmbeddingFunction.md)&gt; : `undefined` \| `EmbeddingFunctionCreate`&lt;`T`&gt;

Fetch an embedding function by name

#### Type Parameters

• **T** *extends* [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`unknown`, `FunctionOptions`&gt;

• **Name** *extends* `string` = `""`

#### Parameters

• **name**: `Name` *extends* `"openai"` ? `"openai"` : `string`

The name of the function

#### Returns

`Name` *extends* `"openai"` ? `EmbeddingFunctionCreate`&lt;[`OpenAIEmbeddingFunction`](OpenAIEmbeddingFunction.md)&gt; : `undefined` \| `EmbeddingFunctionCreate`&lt;`T`&gt;

***

### getTableMetadata()

> **getTableMetadata**(`functions`): `Map`&lt;`string`, `string`&gt;

#### Parameters

• **functions**: [`EmbeddingFunctionConfig`](../interfaces/EmbeddingFunctionConfig.md)[]

#### Returns

`Map`&lt;`string`, `string`&gt;

***

### register()

> **register**&lt;`T`&gt;(`this`, `alias`?): (`ctor`) => `any`

Register an embedding function

#### Type Parameters

• **T** *extends* `EmbeddingFunctionConstructor`&lt;[`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, `FunctionOptions`&gt;&gt; = `EmbeddingFunctionConstructor`&lt;[`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, `FunctionOptions`&gt;&gt;

#### Parameters

• **this**: [`EmbeddingFunctionRegistry`](EmbeddingFunctionRegistry.md)

• **alias?**: `string`

#### Returns

`Function`

##### Parameters

• **ctor**: `T`

##### Returns

`any`

#### Throws

Error if the function is already registered

***

### reset()

> **reset**(`this`): `void`

reset the registry to the initial state

#### Parameters

• **this**: [`EmbeddingFunctionRegistry`](EmbeddingFunctionRegistry.md)

#### Returns

`void`
