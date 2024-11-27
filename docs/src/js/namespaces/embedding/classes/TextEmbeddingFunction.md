[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / TextEmbeddingFunction

# Class: `abstract` TextEmbeddingFunction&lt;M&gt;

an abstract class for implementing embedding functions that take text as input

## Extends

- [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`string`, `M`&gt;

## Type Parameters

• **M** *extends* `FunctionOptions` = `FunctionOptions`

## Constructors

### new TextEmbeddingFunction()

```ts
new TextEmbeddingFunction<M>(): TextEmbeddingFunction<M>
```

#### Returns

[`TextEmbeddingFunction`](TextEmbeddingFunction.md)&lt;`M`&gt;

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`constructor`](EmbeddingFunction.md#constructors)

## Methods

### computeQueryEmbeddings()

```ts
computeQueryEmbeddings(data: string): Promise<number[] | Float32Array | Float64Array>
```

Compute the embeddings for a single query

#### Parameters

• **data**: `string`

#### Returns

`Promise`&lt;`number`[] \| `Float32Array` \| `Float64Array`&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`computeQueryEmbeddings`](EmbeddingFunction.md#computequeryembeddings)

***

### computeSourceEmbeddings()

```ts
computeSourceEmbeddings(data: string[]): Promise<number[][] | Float32Array[] | Float64Array[]>
```

Creates a vector representation for the given values.

#### Parameters

• **data**: `string`[]

#### Returns

`Promise`&lt;`number`[][] \| `Float32Array`[] \| `Float64Array`[]&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`computeSourceEmbeddings`](EmbeddingFunction.md#computesourceembeddings)

***

### embeddingDataType()

```ts
embeddingDataType(): Float<Floats>
```

The datatype of the embeddings

#### Returns

`Float`&lt;`Floats`&gt;

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`embeddingDataType`](EmbeddingFunction.md#embeddingdatatype)

***

### generateEmbeddings()

```ts
abstract generateEmbeddings(texts: string[], ...args: any[]): Promise<number[][] | Float32Array[] | Float64Array[]>
```

#### Parameters

• **texts**: `string`[]

• ...**args**: `any`[]

#### Returns

`Promise`&lt;`number`[][] \| `Float32Array`[] \| `Float64Array`[]&gt;

***

### init()?

```ts
optional init(): Promise<void>
```

#### Returns

`Promise`&lt;`void`&gt;

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`init`](EmbeddingFunction.md#init)

***

### ndims()

```ts
ndims(): undefined | number
```

The number of dimensions of the embeddings

#### Returns

`undefined` \| `number`

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`ndims`](EmbeddingFunction.md#ndims)

***

### sourceField()

```ts
sourceField(): [DataType<Type, any>, Map<string, EmbeddingFunction<any, FunctionOptions>>]
```

sourceField is used in combination with `LanceSchema` to provide a declarative data model

#### Returns

[`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, `FunctionOptions`&gt;&gt;]

#### See

lancedb.LanceSchema

#### Overrides

[`EmbeddingFunction`](EmbeddingFunction.md).[`sourceField`](EmbeddingFunction.md#sourcefield)

***

### toJSON()

```ts
abstract toJSON(): Partial<M>
```

Convert the embedding function to a JSON object
It is used to serialize the embedding function to the schema
It's important that any object returned by this method contains all the necessary
information to recreate the embedding function

It should return the same object that was passed to the constructor
If it does not, the embedding function will not be able to be recreated, or could be recreated incorrectly

#### Returns

`Partial`&lt;`M`&gt;

#### Example

```ts
class MyEmbeddingFunction extends EmbeddingFunction {
  constructor(options: {model: string, timeout: number}) {
    super();
    this.model = options.model;
    this.timeout = options.timeout;
  }
  toJSON() {
    return {
      model: this.model,
      timeout: this.timeout,
    };
}
```

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`toJSON`](EmbeddingFunction.md#tojson)

***

### vectorField()

```ts
vectorField(optionsOrDatatype?: DataType<Type, any> | Partial<FieldOptions<DataType<Type, any>>>): [DataType<Type, any>, Map<string, EmbeddingFunction<any, FunctionOptions>>]
```

vectorField is used in combination with `LanceSchema` to provide a declarative data model

#### Parameters

• **optionsOrDatatype?**: `DataType`&lt;`Type`, `any`&gt; \| `Partial`&lt;`FieldOptions`&lt;`DataType`&lt;`Type`, `any`&gt;&gt;&gt;

#### Returns

[`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, `FunctionOptions`&gt;&gt;]

#### See

lancedb.LanceSchema

#### Inherited from

[`EmbeddingFunction`](EmbeddingFunction.md).[`vectorField`](EmbeddingFunction.md#vectorfield)
