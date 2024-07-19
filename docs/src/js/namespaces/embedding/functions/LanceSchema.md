[**@lancedb/lancedb**](../../../README.md) • **Docs**

***

[@lancedb/lancedb](../../../globals.md) / [embedding](../README.md) / LanceSchema

# Function: LanceSchema()

> **LanceSchema**(`fields`): `Schema`

Create a schema with embedding functions.

## Parameters

• **fields**: `Record`&lt;`string`, `object` \| [`object`, `Map`&lt;`string`, [`EmbeddingFunction`](../classes/EmbeddingFunction.md)&lt;`any`, `FunctionOptions`&gt;&gt;]&gt;

## Returns

`Schema`

Schema

## Example

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
