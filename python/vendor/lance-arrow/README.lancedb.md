This directory contains `rust/lance-arrow` from Lance tag `v15.0.0-beta.4`
(commit `77123c9aa671dc509383e1e5cec96d5cd3c3212e`), licensed under Apache-2.0.

LanceDB carries a local fix in `src/lib.rs` for sliced child validity bitmaps
until a Lance release includes the fix. The patch is selected in the root
`Cargo.toml`; the rest of Lance stays on the pinned release. An explicit unsafe
block in `src/bfloat16.rs` lets this copy pass LanceDB's Rust 2024 warning gate.
