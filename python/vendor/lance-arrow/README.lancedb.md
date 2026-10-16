This directory contains `rust/lance-arrow` from Lance tag `v14.0.0-beta.19`
(commit `42e593fb4ad0e1ccc22c1fa5e3c30bfe92a5384d`), licensed under Apache-2.0.

LanceDB carries a local fix in `src/lib.rs` for sliced child validity bitmaps
until a Lance release includes the fix. The patch is selected in the root
`Cargo.toml`; the rest of Lance stays on the pinned release. An explicit unsafe
block in `src/bfloat16.rs` lets this copy pass LanceDB's Rust 2024 warning gate.
