# Rust Guidelines

Also see [root AGENTS.md](../AGENTS.md) for cross-language standards.

## Code Style

- Use `Vec::with_capacity()` when size is known or estimable — prefer over-estimating capacity to multiple reallocations.
- Wrap large or expensive-to-clone struct fields (maps, protobuf metadata, schemas) in `Arc<T>` to avoid deep copies.
- Use `Box::pin(...)` or `.boxed()` but never both — `.boxed()` already returns `Pin<Box<...>>`.
- Remove dead code instead of adding `#[allow(dead_code)]`. Delete unused constants instead of reducing visibility.
- Use `column_by_name()` for `RecordBatch` column access in production code; use `batch["column_name"]` in tests.
- Use `PrimitiveArray::<T>::from(vec)` (zero-copy) instead of `from_iter_values(vec)` for Vec-to-PrimitiveArray conversion.
- Implement `Default` trait on config/options structs instead of standalone `default_*()` helpers.
- Place `#[cfg(test)] mod tests` as a single block at the bottom of each file — no production code after it.
- Place `use` imports at the top of the file, not inline within function bodies.
- Extract substantial new logic (bin packing, scheduling) into dedicated submodules instead of inlining into large files.
- Delete obsolete internal (`pub(crate)` / private) methods in the same PR that introduces their replacements. For public API methods, follow the deprecation path in root AGENTS.md instead.
- Choose log levels by audience: `debug!` for routine/high-frequency ops, `info!` for infrequent operator-visible state changes, `warn!` for unexpected conditions.

## Concurrency

- The closure passed to `spawn_cpu()` must only consume CPU and return — it must **never** wait on anything: **no channels** (blocking send/recv), **no I/O**, **no locks**, and no `block_on`/`.blocking_*`. The CPU pool can collapse to a single worker in resource-constrained environments (`<= 3` CPUs), so a parked closure can deadlock the whole pool with a silent 0% hang. Keep the waiting in surrounding async code and hand only the pure-CPU work to `spawn_cpu()`. Only dispatch substantial work (rule of thumb: ~100µs+ of CPU); below that the pool overhead outweighs the benefit and the work is better left inline. See the doc comment on `spawn_cpu` for the rationale.

## API Design

- Use `with_`-prefixed builder methods for optional config (e.g., `MyStruct::new(required).with_option(v)`) — don't create separate constructor variants.
- For public APIs, prefer `Into<T>` or `AsRef<T>` trait bounds for flexible inputs.
- Prefer `pub(crate)` over `pub` for crate-internal items. Use `pub use` re-exports for the actual public API surface.
- Use enums instead of magic numbers for format versions, variant types, and discriminators — leverage exhaustive `match`.
- Use strongly-typed structs instead of `HashMap<String, String>` in APIs — convert to strings only at serialization boundaries.
- Keep `RowAddr` (physical fragment+offset) and `RowId` (stable logical identifier) as distinct types — never raw `u64` for both.
- Use `RowAddress` from `lance-core/src/utils/address.rs` instead of raw bitwise operations on row addresses.
- Use `RowAddrTreeMap`/`RoaringBitmap` instead of `Vec<Range<u64>>` for physical row selections.
- Use logical row counts (`num_rows()`) instead of `physical_rows` for user-facing metrics — subtract deletions.
- Keep traits minimal — only core abstraction methods. Move helpers to standalone functions and config to struct fields.
- Get column/field types from schema metadata — never materialize data rows just to inspect types.
- Use stable, versioned serialization formats for persistent storage (e.g., index files) — avoid unstable cross-version formats.
- Use Arrow's type-safe access (`ArrayAccessor` trait bounds, `as_*_array` helpers) instead of `arrow::compute::cast` + `downcast_ref`. Prefer `_opt` variants (e.g., `as_string_opt`) unless the data type has already been verified.
- In `lance-io/`, use single-syscall writes for local filesystem I/O — don't reuse cloud multipart upload machinery.

## Error Handling

- Never use `.unwrap()`, `.expect()`, `panic!()`, or `assert!()` in library code for fallible operations — use `?` with `Result` and proper error types. Reserve `.unwrap()` for tests only.
- Avoid bare `.unwrap()`; use `if let`, `match`, `let ... else`, `?`, or combinators. Never `.is_none()` followed by `.unwrap()`. If unavoidable, use `.expect("reason")`.
- Return `LanceError::NotSupported` instead of `todo!()` or `unimplemented!()` for unsupported code paths. Test with `Result::Err` assertions, not `#[should_panic]`.
- Match `Error` variant to root cause: `Error::invalid_input` for caller data issues, `Error::corrupt_file` for format/integrity issues, `Error::not_found` for missing resources, `Error::io` for I/O failures.
- Include full context in error messages — variable names, values, sizes, types, indices. Not generic messages like `"Invalid chunk size"`.
- Use `checked_add`/`checked_mul` instead of `wrapping_add`/`wrapping_mul` for counters and IDs — return an error on overflow.
- Prefer `debug_assert!` over `assert!` for non-safety invariants; reserve `assert!` for conditions preventing data corruption. Always include descriptive messages.
- Don't silently guard against impossible conditions — use `debug_assert!`, return an explicit error, or remove the check.
- Log warnings on best-effort/cleanup failures instead of silently swallowing or propagating errors.
- Log warnings for silent no-ops (skipped operations); omit warnings before errors since the error message is sufficient.
- Avoid `unwrap_or(default)` on map lookups for required config params — use `.ok_or_else(|| Error::...)` and verify key names match between serialization and deserialization.
- Advance all parallel iterators before any `continue` branches — early exits that skip `.next()` calls cause misalignment.
- Bind `iter.next()` with `let Some(x) = iter.next() else { ... }` — never call `.next()` twice to check-then-use.

## Naming

- Reserve `_`-prefixed names for truly unused bindings — if a variable is read, drop the underscore.
- Prefix boolean variables with `is_` or `has_` instead of ambiguous `with_` or bare adjectives.
- Name booleans so `false` (zero/`Default::default()`) is the desired default — use `disable_*` instead of `enable_*` when the feature should be on by default.
- Name functions to match their actual scope — e.g., `handle_partition_system_columns` not `handle_system_columns` if only a subset is handled.

## Testing

- Use `record_batch!()` from `arrow_array` to construct `RecordBatch` in tests instead of manual Schema/Arc/try_new boilerplate.
- Use `gen_batch()` builder API (`.col()`, `.into_reader_rows()`) for test data setup instead of manual Arrow construction.
- Use `.try_into_batch()` instead of `.try_into_stream().try_collect()` for scanner results in tests.
- Use plain `"memory://"` URIs in tests — no atomic counters or unique suffixes needed.
- Assert on both error variant (`assert!(matches!(error, ErrorType::Variant { .. }))`) and message content — don't just check `is_err()`.

## Documentation

- Add doc comments to public API elements that convey semantic meaning, valid values, and effects — don't restate type signatures.
- Document enum variant doc comments with behavioral semantics, not just labels. For numeric parameters, state whether it's an id, count, index, etc.
- Add doc comments to magic constants, thresholds, and non-obvious transformation functions — explain what the value represents and why it was chosen.
- Comment fallback/guard code paths with when they trigger and why they exist.
- Ensure doc comments match actual semantics — distinguish mutates-in-place (`&mut self`) from returns-new-value.
- Use explicit forward-looking language (`TODO`, `FIXME`) in comments to distinguish current behavior from planned changes.
- Document the semantic meaning of both present and absent states for `Option<T>` fields.
- Use precise domain terminology — avoid ambiguous abbreviations (e.g., "FIXED" vs "fixed-width") or incorrect terms (e.g., "fields" when meaning "fragments").

## lance-encoding

Performance-critical encoding/decoding paths have additional requirements:

- Hoist loop-invariant conditionals out of hot loops — branch once outside, then use separate loop bodies or monomorphized variants.
- Pre-allocate single contiguous buffers. Default to `buf.resize(len, 0)` for safe initialization; reserve `Vec::with_capacity` + `unsafe { set_len() }` for measured hot paths only, with a `// SAFETY:` comment explaining why the buffer will be fully initialized before read (e.g., immediately followed by `read_exact`).
- Use `spawn_cpu()` only at the async-to-CPU boundary (e.g., FSST, decompression, batch materialization) — never nest redundant `spawn_cpu()` calls.
- Use `expect_next()` and similar utility methods instead of inlining `None`-checks with error returns.
