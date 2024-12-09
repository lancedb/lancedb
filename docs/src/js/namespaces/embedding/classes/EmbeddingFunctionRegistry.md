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

```ts
new EmbeddingFunctionRegistry(): EmbeddingFunctionRegistry
```

#### Returns

[`EmbeddingFunctionRegistry`](EmbeddingFunctionRegistry.md)

## Methods

### functionToMetadata()

```ts
functionToMetadata(conf): Record<string, any>
```

#### Parameters

• **conf**: [`EmbeddingFunctionConfig`](../interfaces/EmbeddingFunctionConfig.md)

#### Returns

`Record`&lt;`string`, `any`&gt;

***

### get()

```ts
get<T>(name): undefined | EmbeddingFunctionCreate<T>
```

Fetch an embedding function by name

#### Type Parameters

• **T** *extends* [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`unknown`, `FunctionOptions`&gt;

#### Parameters

• **name**: `string`
  The name of the function

#### Returns

`undefined` \| `EmbeddingFunctionCreate`&lt;`T`&gt;

***

### getTableMetadata()

```ts
getTableMetadata(functions): Map<string, string>
```

#### Parameters

• **functions**: [`EmbeddingFunctionConfig`](../interfaces/EmbeddingFunctionConfig.md)[]

#### Returns

`Map`&lt;`string`, `string`&gt;

***

### length()

```ts
length(): number
```

Get the number of registered functions

#### Returns

`number`

***

### register()

```ts
register<T>(this, alias?): (ctor) => any
```

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

```ts
reset(this): void
```

reset the registry to the initial state

#### Parameters

• **this**: [`EmbeddingFunctionRegistry`](EmbeddingFunctionRegistry.md)

#### Returns

`void`
