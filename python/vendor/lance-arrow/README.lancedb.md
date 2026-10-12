This directory contains `rust/lance-arrow` from Lance tag `v14.0.0-beta.13`
(commit `145fd4b933fb4a9a45831f0ae9185c88e908fe6a`), licensed under Apache-2.0.

LanceDB carries a local fix in `src/lib.rs` for sliced child validity bitmaps
until a Lance release includes the fix. The patch is selected in the root
`Cargo.toml`; the rest of Lance stays on the pinned release. An explicit unsafe
block in `src/bfloat16.rs` lets this copy pass LanceDB's Rust 2024 warning gate.
