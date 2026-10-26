This directory contains `rust/lance-arrow` from Lance tag `v15.0.0-beta.1`
(commit `46a7cfe1fba5efa1f90deea4c7b14d414b991409`), licensed under Apache-2.0.

LanceDB carries a local fix in `src/lib.rs` for sliced child validity bitmaps
until a Lance release includes the fix. The patch is selected in the root
`Cargo.toml`; the rest of Lance stays on the pinned release. An explicit unsafe
block in `src/bfloat16.rs` lets this copy pass LanceDB's Rust 2024 warning gate.
