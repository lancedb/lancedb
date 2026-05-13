These are the typescript bindings of LanceDB.
The core Rust library is in the `../rust/lancedb` directory, the rust binding
code is in the `src/` directory and the typescript bindings are in
the `lancedb/` directory.

Whenever you change the Rust code, you will need to recompile: `pnpm build`.

Common commands:
* Build: `pnpm build`
* Lint: `pnpm lint`
* Fix lints: `pnpm lint-fix`
* Test: `pnpm test`
* Run single test file: `pnpm test __test__/arrow.test.ts`
