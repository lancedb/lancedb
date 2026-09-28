This directory contains `rust/lance-arrow` from Lance tag `v13.0.0-beta.15`
(commit `db53dcf367088b32ae3a7461407d43593759ae3e`), licensed under Apache-2.0.

LanceDB carries a local fix in `src/lib.rs` for sliced child validity bitmaps
until a Lance release includes the fix. The patch is selected in the root
`Cargo.toml`; the rest of Lance stays on the pinned release. An explicit unsafe
block in `src/bfloat16.rs` lets this copy pass LanceDB's Rust 2024 warning gate.
