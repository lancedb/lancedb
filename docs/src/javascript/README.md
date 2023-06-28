vectordb / [Exports](modules.md)

# LanceDB

A JavaScript / Node.js library for [LanceDB](https://github.com/lancedb/lancedb).

## Installation

```bash
npm install vectordb
```

## Usage

### Basic Example

```javascript
const lancedb = require('vectordb');
const db = await lancedb.connect('data/sample-lancedb');
const table = await db.createTable("my_table", 
      [{ id: 1, vector: [0.1, 1.0], item: "foo", price: 10.0 },
      { id: 2, vector: [3.9, 0.5], item: "bar", price: 20.0 }])
const results = await table.search([0.1, 0.3]).limit(20).execute();
console.log(results);
```

The [examples](./examples) folder contains complete examples.

## Development

Run the tests with

```bash
npm test
```

To run the linter and have it automatically fix all errors

```bash
npm run lint -- --fix
```

To build documentation

```bash
npx typedoc --plugin typedoc-plugin-markdown --out ../docs/src/javascript src/index.ts
```
