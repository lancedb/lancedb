# Contributing to LanceDB Typescript

This document outlines the process for contributing to LanceDB Typescript.
For general contribution guidelines, see [CONTRIBUTING.md](https://github.com/lancedb/lancedb/blob/main/CONTRIBUTING.md).

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

1. Node.js 22 or later (required by pnpm 11)
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

Local SQL computed columns and materialized-view creation/refresh are experimental
and disabled in published packages. To opt in when building from this directory:

```shell
pnpm build:debug --features experimental-functions
pnpm tsc
pnpm test --runInBand table.test.ts materialized_view.test.ts
```

Rebuild without `--features experimental-functions` to test the default package.
The tests detect the compiled feature; remote operations, metadata protections,
and ordinary eager `addColumns` transforms remain available in both builds.

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
