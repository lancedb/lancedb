# LanceDB

A JavaScript / Node.js library for [LanceDB](https://github.com/lancedb/lancedb).

## Installation

```bash
npm install vectordb
```

This will download the appropriate native library for your platform. We currently
support:

* Linux (x86_64 and aarch64)
* MacOS (Intel and ARM/M1/M2)
* Windows (x86_64 only)

We do not yet support musl-based Linux (such as Alpine Linux) or aarch64 Windows.

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

To build everything fresh:

```bash
npm install
npm run tsc
npm run build
```

Then you should be able to run the tests with:

```bash
npm test
```

### Rebuilding Rust library

```bash
npm run build
```

### Rebuilding Typescript

```bash
npm run tsc
```

### Fix lints

To run the linter and have it automatically fix all errors

```bash
npm run lint -- --fix
```

To build documentation

```bash
npx typedoc --plugin typedoc-plugin-markdown --out ../docs/src/javascript src/index.ts
```
