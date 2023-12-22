# Table of contents

## Installation

```bash
npm install vectordb
```

This will download the appropriate native library for your platform. We currently
support x86_64 Linux, aarch64 Linux, Intel MacOS, and ARM (M1/M2) MacOS. We do not
yet support Windows or musl-based Linux (such as Alpine Linux).


## Classes 
- [RemoteConnection](classes/RemoteConnection.md)
- [RemoteTable](classes/RemoteTable.md)
- [RemoteQuery](classes/RemoteQuery.md)


## Methods

- [add](classes/RemoteTable.md#add)
- [countRows](classes/RemoteTable.md#countrows)
- [createIndex](classes/RemoteTable.md#createindex)
- [createTable](classes/RemoteConnection.md#createtable)
- [delete](classes/RemoteTable.md#delete)
- [dropTable](classes/RemoteConnection.md#droptable)
- [listIndices](classes/RemoteTable.md#listindices)
- [indexStats](classes/RemoteTable.md#liststats)
- [openTable](classes/RemoteConnection.md#opentable)
- [overwrite](classes/RemoteTable.md#overwrite)
- [schema](classes/RemoteTable.md#schema)
- [search](classes/RemoteTable.md#search)
- [tableNames](classes/RemoteConnection.md#tablenames)
- [update](classes/RemoteTable.md#update)


## Example code 
```javascript
// connect to a remote DB
const lancedb = require('vectordb');
const db = await lancedb.connect({
  uri: "db://your-project-name",
  apiKey: "sk_...",
  region: "us-east-1"
});
// create a new table
const tableName = "my_table"
const table = await db.createTable(tableName,
      [
           { id: 1, vector: [0.1, 1.0], item: "foo", price: 10.0 },
           { id: 2, vector: [3.9, 0.5], item: "bar", price: 20.0 }
      ]
)
// list the table
const tableNames_1 = await db.tableNames('')
// add some data and search should be okay
const data = [
      { id: 3, vector: [10.3, 1.9], item: "test1", price: 30.0 }, 
      { id: 4, vector: [6.2, 9.2], item: "test2", price: 40.0 }
]
// create the index for the table
await table.createIndex({
      metric_type: 'L2', 
      column: 'vector'
})
let result = await table.search([2.8, 4.3]]).select(["vector", "price"]).limit(1).execute()
// update the data
await table.update({ 
      where: "id == 1", 
      values: { item: "foo1" } 
})
//drop the table
await db.dropTable(tableName)
```