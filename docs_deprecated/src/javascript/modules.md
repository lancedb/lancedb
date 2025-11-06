[vectordb](README.md) / Exports

# vectordb

## Table of contents

### Enumerations

- [IndexStatus](enums/IndexStatus.md)
- [MetricType](enums/MetricType.md)
- [WriteMode](enums/WriteMode.md)

### Classes

- [DefaultWriteOptions](classes/DefaultWriteOptions.md)
- [LocalConnection](classes/LocalConnection.md)
- [LocalTable](classes/LocalTable.md)
- [MakeArrowTableOptions](classes/MakeArrowTableOptions.md)
- [OpenAIEmbeddingFunction](classes/OpenAIEmbeddingFunction.md)
- [Query](classes/Query.md)

### Interfaces

- [AwsCredentials](interfaces/AwsCredentials.md)
- [CleanupStats](interfaces/CleanupStats.md)
- [ColumnAlteration](interfaces/ColumnAlteration.md)
- [CompactionMetrics](interfaces/CompactionMetrics.md)
- [CompactionOptions](interfaces/CompactionOptions.md)
- [Connection](interfaces/Connection.md)
- [ConnectionOptions](interfaces/ConnectionOptions.md)
- [CreateTableOptions](interfaces/CreateTableOptions.md)
- [EmbeddingFunction](interfaces/EmbeddingFunction.md)
- [IndexStats](interfaces/IndexStats.md)
- [IvfPQIndexConfig](interfaces/IvfPQIndexConfig.md)
- [MergeInsertArgs](interfaces/MergeInsertArgs.md)
- [Table](interfaces/Table.md)
- [UpdateArgs](interfaces/UpdateArgs.md)
- [UpdateSqlArgs](interfaces/UpdateSqlArgs.md)
- [VectorIndex](interfaces/VectorIndex.md)
- [WriteOptions](interfaces/WriteOptions.md)

### Type Aliases

- [VectorIndexParams](modules.md#vectorindexparams)

### Functions

- [connect](modules.md#connect)
- [convertToTable](modules.md#converttotable)
- [isWriteOptions](modules.md#iswriteoptions)
- [makeArrowTable](modules.md#makearrowtable)

## Type Aliases

### VectorIndexParams

Ƭ **VectorIndexParams**: [`IvfPQIndexConfig`](interfaces/IvfPQIndexConfig.md)

#### Defined in

[index.ts:1336](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1336)

## Functions

### connect

▸ **connect**(`uri`): `Promise`\<[`Connection`](interfaces/Connection.md)\>

Connect to a LanceDB instance at the given URI.

Accepted formats:

- `/path/to/database` - local database
- `s3://bucket/path/to/database` or `gs://bucket/path/to/database` - database on cloud storage
- `db://host:port` - remote database (LanceDB cloud)

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `uri` | `string` | The uri of the database. If the database uri starts with `db://` then it connects to a remote database. |

#### Returns

`Promise`\<[`Connection`](interfaces/Connection.md)\>

**`See`**

[ConnectionOptions](interfaces/ConnectionOptions.md) for more details on the URI format.

#### Defined in

[index.ts:188](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L188)

▸ **connect**(`opts`): `Promise`\<[`Connection`](interfaces/Connection.md)\>

Connect to a LanceDB instance with connection options.

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `opts` | `Partial`\<[`ConnectionOptions`](interfaces/ConnectionOptions.md)\> | The [ConnectionOptions](interfaces/ConnectionOptions.md) to use when connecting to the database. |

#### Returns

`Promise`\<[`Connection`](interfaces/Connection.md)\>

#### Defined in

[index.ts:194](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L194)

___

### convertToTable

▸ **convertToTable**\<`T`\>(`data`, `embeddings?`, `makeTableOptions?`): `Promise`\<`ArrowTable`\>

#### Type parameters

| Name |
| :------ |
| `T` |

#### Parameters

| Name | Type |
| :------ | :------ |
| `data` | `Record`\<`string`, `unknown`\>[] |
| `embeddings?` | [`EmbeddingFunction`](interfaces/EmbeddingFunction.md)\<`T`\> |
| `makeTableOptions?` | `Partial`\<[`MakeArrowTableOptions`](classes/MakeArrowTableOptions.md)\> |

#### Returns

`Promise`\<`ArrowTable`\>

#### Defined in

[arrow.ts:465](https://github.com/lancedb/lancedb/blob/92179835/node/src/arrow.ts#L465)

___

### isWriteOptions

▸ **isWriteOptions**(`value`): value is WriteOptions

#### Parameters

| Name | Type |
| :------ | :------ |
| `value` | `any` |

#### Returns

value is WriteOptions

#### Defined in

[index.ts:1362](https://github.com/lancedb/lancedb/blob/92179835/node/src/index.ts#L1362)

___

### makeArrowTable

▸ **makeArrowTable**(`data`, `options?`): `ArrowTable`

An enhanced version of the makeTable function from Apache Arrow
that supports nested fields and embeddings columns.

This function converts an array of Record<String, any> (row-major JS objects)
to an Arrow Table (a columnar structure)

Note that it currently does not support nulls.

If a schema is provided then it will be used to determine the resulting array
types.  Fields will also be reordered to fit the order defined by the schema.

If a schema is not provided then the types will be inferred and the field order
will be controlled by the order of properties in the first record.

If the input is empty then a schema must be provided to create an empty table.

When a schema is not specified then data types will be inferred.  The inference
rules are as follows:

 - boolean => Bool
 - number => Float64
 - String => Utf8
 - Buffer => Binary
 - Record<String, any> => Struct
 - Array<any> => List

#### Parameters

| Name | Type | Description |
| :------ | :------ | :------ |
| `data` | `Record`\<`string`, `any`\>[] | input data |
| `options?` | `Partial`\<[`MakeArrowTableOptions`](classes/MakeArrowTableOptions.md)\> | options to control the makeArrowTable call. |

#### Returns

`ArrowTable`

**`Example`**

```ts

import { fromTableToBuffer, makeArrowTable } from "../arrow";
import { Field, FixedSizeList, Float16, Float32, Int32, Schema } from "apache-arrow";

const schema = new Schema([
  new Field("a", new Int32()),
  new Field("b", new Float32()),
  new Field("c", new FixedSizeList(3, new Field("item", new Float16()))),
 ]);
 const table = makeArrowTable([
   { a: 1, b: 2, c: [1, 2, 3] },
   { a: 4, b: 5, c: [4, 5, 6] },
   { a: 7, b: 8, c: [7, 8, 9] },
 ], { schema });
```

By default it assumes that the column named `vector` is a vector column
and it will be converted into a fixed size list array of type float32.
The `vectorColumns` option can be used to support other vector column
names and data types.

```ts

const schema = new Schema([
   new Field("a", new Float64()),
   new Field("b", new Float64()),
   new Field(
     "vector",
     new FixedSizeList(3, new Field("item", new Float32()))
   ),
 ]);
 const table = makeArrowTable([
   { a: 1, b: 2, vector: [1, 2, 3] },
   { a: 4, b: 5, vector: [4, 5, 6] },
   { a: 7, b: 8, vector: [7, 8, 9] },
 ]);
 assert.deepEqual(table.schema, schema);
```

You can specify the vector column types and names using the options as well

```typescript

const schema = new Schema([
   new Field('a', new Float64()),
   new Field('b', new Float64()),
   new Field('vec1', new FixedSizeList(3, new Field('item', new Float16()))),
   new Field('vec2', new FixedSizeList(3, new Field('item', new Float16())))
 ]);
const table = makeArrowTable([
   { a: 1, b: 2, vec1: [1, 2, 3], vec2: [2, 4, 6] },
   { a: 4, b: 5, vec1: [4, 5, 6], vec2: [8, 10, 12] },
   { a: 7, b: 8, vec1: [7, 8, 9], vec2: [14, 16, 18] }
 ], {
   vectorColumns: {
     vec1: { type: new Float16() },
     vec2: { type: new Float16() }
   }
 }
assert.deepEqual(table.schema, schema)
```

#### Defined in

[arrow.ts:198](https://github.com/lancedb/lancedb/blob/92179835/node/src/arrow.ts#L198)
