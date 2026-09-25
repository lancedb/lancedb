This is `rust/lance-encoding` from
[`lance-format/lance` v13.0.0-beta.14](https://github.com/lance-format/lance/tree/b80ac1d9f3c24b47bd8e297d03053ea35d216a9f/rust/lance-encoding),
with the crate's build protos copied into `protos/`. Its manifest uses explicit
dependencies so it can be built outside the Lance workspace.

The Rust sources retain the original Lance Authors attribution alongside the
LanceDB Authors header required by this repository's license check for the
vendored copy.

The only behavioral source change is in `src/compression.rs`: skip out-of-line
bitpacking when the selected width is the full width of the values. That encoder
requires positive bit savings for a partial 1024-value chunk and panics in debug builds
for signed dictionaries containing negative integers.

Remove this patch and the root Cargo patch entry once Lance releases the guard.
