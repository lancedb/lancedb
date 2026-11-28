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

### allowEndpoint()

```ts
allowEndpoint(url): void
```

Trust a custom endpoint when an embedding function that sends data to it
is restored from table metadata. Trailing slashes are ignored.

Embedding functions whose metadata this process wrote are allowed to use
their endpoints automatically.

#### Parameters

* **url**: `string`

#### Returns

`void`

***

### allowFunction()

```ts
allowFunction(name): void
```

Trust any options from table metadata for an embedding function that
isn't registered with `safeToRestore: true`, such as your own embedding
functions.

LanceDB can't tell what such a function's options make it load or
contact, so when it is restored from table metadata it only computes
embeddings for options this process wrote, unless you call this. Models
and endpoints the function reports still need approval with
[allowModel](EmbeddingFunctionRegistry.md#allowmodel) and [allowEndpoint](EmbeddingFunctionRegistry.md#allowendpoint).

#### Parameters

* **name**: `string`
    The name the function was registered under.

#### Returns

`void`

***

### allowModel()

```ts
allowModel(model, revision?): void
```

Trust a model when an embedding function that loads it is restored from
table metadata.

Anyone who can write a table controls its embedding options, so table
metadata cannot pick a model other than the function's default by itself.
Embedding functions whose metadata this process wrote, for example by
creating a table with them, are allowed to load their models
automatically.

#### Parameters

* **model**: `string`
    The model name, such as `"Xenova/bge-small-en-v1.5"`.

* **revision?**: `string`
    The revision the table's options must pin. If omitted,
    any revision is trusted.

#### Returns

`void`

***

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
register<T>(
   this,
   alias?,
   options?): (ctor) => any
```

Register an embedding function

#### Type Parameters

• **T** *extends* [`EmbeddingFunctionConstructor`](../interfaces/EmbeddingFunctionConstructor.md)&lt;[`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;&gt; = [`EmbeddingFunctionConstructor`](../interfaces/EmbeddingFunctionConstructor.md)&lt;[`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;&gt;

#### Parameters

* **this**: [`EmbeddingFunctionRegistry`](EmbeddingFunctionRegistry.md)

* **alias?**: `string`

* **options?**: [`RegisterOptions`](../interfaces/RegisterOptions.md) = `{}`

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
setVar(
   name,
   value,
   options): void
```

Set a variable. These can be accessed in the embedding function
configuration using the syntax `$var:variable_name`. If they are not
set, an error will be thrown letting you know which key is unset. If you
want to supply a default value, you can add an additional part in the
configuration like so: `$var:variable_name:default_value`. Default values
can be used for runtime configurations that are not sensitive, such as
whether to use a GPU for inference.

The name must not contain colons. The default value can contain colons.

Variables are secret unless you pass `{ secret: false }`, and secret
variables can only be used in sensitive keys such as API keys. See
[SetVarOptions.secret](../interfaces/SetVarOptions.md#secret).

#### Parameters

* **name**: `string`

* **value**: `string`

* **options**: [`SetVarOptions`](../interfaces/SetVarOptions.md) = `{}`

#### Returns

`void`
