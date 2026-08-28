[**@lancedb/lancedb**](../README.md) • **Docs**

***

[@lancedb/lancedb](../globals.md) / makeJsonField

# Function: makeJsonField()

```ts
function makeJsonField(name: string, nullable?: boolean): Field
```

Create an Arrow field backed by LanceDB's JSON extension type.

## Parameters

* **name**: `string` — The field name.

* **nullable?**: `boolean` — Whether the field accepts null values. **Default** `true`

## Returns

`Field`

## Example

```ts
import { connect, makeJsonField } from "@lancedb/lancedb";
import { Schema } from "apache-arrow";

const schema = new Schema([makeJsonField("metadata")]);
const db = await connect("/path/to/database");
await db.createTable("items", [{ metadata: '{"source":"api"}' }], { schema });
```
