# Contributing to LanceDB Typescript

This document outlines the process for contributing to LanceDB Typescript.
For general contribution guidelines, see [CONTRIBUTING.md](../CONTRIBUTING.md).

## Project layout

The Typescript package is a wrapper around the Rust library, `lancedb`. We use
the [napi-rs](https://napi.rs/) library to create the bindings between Rust and
Typescript.

* `src/`: Rust bindings source code
* `lancedb/`: Typescript package source code
* `__test__/`: Unit tests
* `examples/`: A pnpm package with the examples shown in the documentation

## Development environment

To set up your development environment, you will need to install the following:

1. Node.js 18 or later
2. [pnpm](https://pnpm.io/installation) 11 or later (or run via `corepack enable`,
   which uses the `packageManager` field in `package.json`)
3. Rust's package manager, Cargo. Use [rustup](https://rustup.rs/) to install.
4. [protoc](https://grpc.io/docs/protoc-installation/) (Protocol Buffers compiler)

Initial setup:

```shell
pnpm install
```

### Commit Hooks

It is **highly recommended** to install the [pre-commit](https://pre-commit.com/) hooks to ensure that your
code is formatted correctly and passes basic checks before committing:

```shell
pre-commit install
```

## Development

Most common development commands can be run using the pnpm scripts.

Build the package

```shell
pnpm install
pnpm build
```

Lint:

```shell
pnpm lint
```

Format and fix lints:

```shell
pnpm lint-fix
```

Run tests:

```shell
pnpm test
```

To run a single test:

```shell
# Single file: table.test.ts
pnpm test -- table.test.ts
# Single test: 'merge insert' in table.test.ts
pnpm test -- table.test.ts --testNamePattern=merge\ insert
```
