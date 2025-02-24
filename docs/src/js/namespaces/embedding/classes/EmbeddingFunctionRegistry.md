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

* **conf**: [`EmbeddingFunctionConfig`](../interfaces/EmbeddingFunctionConfig.md)

#### Returns

`Record`&lt;`string`, `any`&gt;

***

### get()

```ts
get<T>(name): undefined | EmbeddingFunctionCreate<T>
```

Fetch an embedding function by name

#### Type Parameters

• **T** *extends* [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`unknown`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;

#### Parameters

* **name**: `string`
    The name of the function

#### Returns

`undefined` \| [`EmbeddingFunctionCreate`](../interfaces/EmbeddingFunctionCreate.md)&lt;`T`&gt;

***

### getTableMetadata()

```ts
getTableMetadata(functions): Map<string, string>
```

#### Parameters

* **functions**: [`EmbeddingFunctionConfig`](../interfaces/EmbeddingFunctionConfig.md)[]

#### Returns

`Map`&lt;`string`, `string`&gt;

***

### getVar()

```ts
getVar(name): undefined | string
```

Get a variable.

#### Parameters

* **name**: `string`

#### Returns

`undefined` \| `string`

#### See

[setVar](EmbeddingFunctionRegistry.md#setvar)

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

• **T** *extends* [`EmbeddingFunctionConstructor`](../interfaces/EmbeddingFunctionConstructor.md)&lt;[`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;&gt; = [`EmbeddingFunctionConstructor`](../interfaces/EmbeddingFunctionConstructor.md)&lt;[`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;&gt;

#### Parameters

* **this**: [`EmbeddingFunctionRegistry`](EmbeddingFunctionRegistry.md)

* **alias?**: `string`

#### Returns

`Function`

##### Parameters

* **ctor**: `T`

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

* **this**: [`EmbeddingFunctionRegistry`](EmbeddingFunctionRegistry.md)

#### Returns

`void`

***

### setVar()

```ts
setVar(name, value): void
```

Set a variable. These can be accessed in the embedding function
configuration using the syntax `$var:variable_name`. If they are not
set, an error will be thrown letting you know which key is unset. If you
want to supply a default value, you can add an additional part in the
configuration like so: `$var:variable_name:default_value`. Default values
can be used for runtime configurations that are not sensitive, such as
whether to use a GPU for inference.

The name must not contain colons. The default value can contain colons.

#### Parameters

* **name**: `string`

* **value**: `string`

#### Returns

`void`
