# Javascript API Reference

This section contains the API reference for LanceDB Javascript API.

## Installation

```bash
npm install vectordb
```

This will download the appropriate native library for your platform. We currently
support:

* Linux (x86_64 and aarch64)
* MacOS (Intel and ARM/M1/M2)
* Windows (x86_64 only)

We do not yet support musl-based Linux (such as Alpine Linux) or arch64 Windows.

## Usage

### Basic Example
Connect to a local directory
```javascript
const lancedb = require('vectordb');
//connect to a local database
const db = await lancedb.connect('data/sample-lancedb');
```
Connect to LancdDB cloud
```javascript
connect to LanceDB Cloud
const db = await lancedb.connect({
    uri: "db://my-database",
    apiKey: "sk_...",
    region: "us-east-1"
});
```
Create a table followed by a search
```javascript
const table = await db.createTable("my_table",
      [{ id: 1, vector: [0.1, 1.0], item: "foo", price: 10.0 },
      { id: 2, vector: [3.9, 0.5], item: "bar", price: 20.0 }])
const results = await table.search([0.1, 0.3]).limit(20).execute();
console.log(results);
```

The [examples](./examples) folder contains complete examples.

## Table of contents
### Connection
Connect to a LanceDB database.

- [Connection](interfaces/Connection.md)
### Table
A Table is a collection of Records in a LanceDB Database.

- [Table](interfaces/Table.md)
### Query
The LanceDB Query

- [Query](classes/Query.md)

