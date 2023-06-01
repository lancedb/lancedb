# LanceDB

A JavaScript / Node.js library for [LanceDB](https://github.com/lancedb/lancedb).

## Installation

```bash
npm install vectordb
```

This will download the appropriate native library for your platform. We currently
support x86_64 Linux, aarch64 Linux, Intel MacOS, and ARM (M1/M2) MacOS. We do not
yet support Windows or musl-based Linux (such as Alpine Linux).

## Usage

### Basic Example

```javascript
const lancedb = require('vectordb');
const db = lancedb.connect('<PATH_TO_LANCEDB_DATASET>');
const table = await db.openTable('my_table');
const query = await table.search([0.1, 0.3]).setLimit(20).execute();
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
