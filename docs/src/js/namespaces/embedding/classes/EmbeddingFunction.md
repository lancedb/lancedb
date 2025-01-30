[**@lancedb/lancedb**](../../../README.md) • **Docs**
***
[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / EmbeddingFunction
# Class: `abstract` EmbeddingFunction&lt;T, M&gt;
An embedding function that automatically creates vector representation for a given column.
## Extended by
- [`TextEmbeddingFunction`](TextEmbeddingFunction.md)
## Type Parameters
• **T** = `any`
• **M** *extends* [`FunctionOptions`](../interfaces/FunctionOptions.md) = [`FunctionOptions`](../interfaces/FunctionOptions.md)
## Constructors
### new EmbeddingFunction()
```ts
new EmbeddingFunction<T, M>(): EmbeddingFunction<T, M>
```
#### Returns
[`EmbeddingFunction`](EmbeddingFunction.md)&lt;`T`, `M`&gt;
## Methods
### computeQueryEmbeddings()
```ts
computeQueryEmbeddings(data): Promise<number[] | Float32Array | Float64Array>
```
Compute the embeddings for a single query
#### Parameters
* **data**: `T`
#### Returns
`Promise`&lt;`number`[] \| `Float32Array` \| `Float64Array`&gt;
***
### computeSourceEmbeddings()
```ts
abstract computeSourceEmbeddings(data): Promise<number[][] | Float32Array[] | Float64Array[]>
```
Creates a vector representation for the given values.
#### Parameters
* **data**: `T`[]
#### Returns
`Promise`&lt;`number`[][] \| `Float32Array`[] \| `Float64Array`[]&gt;
***
### embeddingDataType()
```ts
abstract embeddingDataType(): Float<Floats>
```
The datatype of the embeddings
#### Returns
`Float`&lt;`Floats`&gt;
***
### init()?
```ts
optional init(): Promise<void>
```
#### Returns
`Promise`&lt;`void`&gt;
***
### ndims()
```ts
ndims(): undefined | number
```
The number of dimensions of the embeddings
#### Returns
`undefined` \| `number`
***
### sourceField()
```ts
sourceField(optionsOrDatatype): [DataType<Type, any>, Map<string, EmbeddingFunction<any, FunctionOptions>>]
```
sourceField is used in combination with `LanceSchema` to provide a declarative data model
#### Parameters
* **optionsOrDatatype**: `DataType`&lt;`Type`, `any`&gt; \| `Partial`&lt;[`FieldOptions`](../interfaces/FieldOptions.md)&lt;`DataType`&lt;`Type`, `any`&gt;&gt;&gt;
    The options for the field or the datatype
#### Returns
[`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;&gt;]
#### See
[LanceSchema](../functions/LanceSchema.md)
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
***
### vectorField()
```ts
vectorField(optionsOrDatatype?): [DataType<Type, any>, Map<string, EmbeddingFunction<any, FunctionOptions>>]
```
vectorField is used in combination with `LanceSchema` to provide a declarative data model
#### Parameters
* **optionsOrDatatype?**: `DataType`&lt;`Type`, `any`&gt; \| `Partial`&lt;[`FieldOptions`](../interfaces/FieldOptions.md)&lt;`DataType`&lt;`Type`, `any`&gt;&gt;&gt;
    The options for the field
#### Returns
[`DataType`&lt;`Type`, `any`&gt;, `Map`&lt;`string`, [`EmbeddingFunction`](EmbeddingFunction.md)&lt;`any`, [`FunctionOptions`](../interfaces/FunctionOptions.md)&gt;&gt;]
#### See
[LanceSchema](../functions/LanceSchema.md)
