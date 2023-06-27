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
const db = lancedb.connect('<PATH_TO_LANCEDB_DATASET>');
const table = await db.openTable('my_table');
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
