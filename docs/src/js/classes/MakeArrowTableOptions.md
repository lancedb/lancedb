[**@lancedb/lancedb**](../README.md) • **Docs**
***
[@lancedb/lancedb](../globals.md) / MakeArrowTableOptions
# Class: MakeArrowTableOptions
Options to control the makeArrowTable call.
## Constructors
### new MakeArrowTableOptions()
```ts
new MakeArrowTableOptions(values?): MakeArrowTableOptions
```
#### Parameters
* **values?**: `Partial`&lt;[`MakeArrowTableOptions`](MakeArrowTableOptions.md)&gt;
#### Returns
[`MakeArrowTableOptions`](MakeArrowTableOptions.md)
## Properties
### dictionaryEncodeStrings
```ts
dictionaryEncodeStrings: boolean = false;
```
If true then string columns will be encoded with dictionary encoding
Set this to true if your string columns tend to repeat the same values
often.  For more precise control use the `schema` property to specify the
data type for individual columns.
If `schema` is provided then this property is ignored.
***
### embeddingFunction?
```ts
optional embeddingFunction: EmbeddingFunctionConfig;
```
***
### embeddings?
```ts
optional embeddings: EmbeddingFunction<unknown, FunctionOptions>;
```
***
### schema?
```ts
optional schema: SchemaLike;
```
***
### vectorColumns
```ts
vectorColumns: Record<string, VectorColumnOptions>;
```
