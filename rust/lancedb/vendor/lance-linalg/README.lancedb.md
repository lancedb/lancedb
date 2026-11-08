This directory contains `rust/lance-linalg` from Lance tag `v15.0.0-beta.13`
(commit `6eef95cf4552e3f904f9222cbd3719504dede0cf`), licensed under Apache-2.0.

LanceDB patches `src/distance/cosine.rs` to recompute FLOAT32 cosine distances
in FLOAT64 when squared norms or dot products overflow or underflow. Ordinary
vectors retain the existing SIMD kernels. The root Cargo patch applies to all
Lance callers, including exact scans and cached-norm distance calculations. The
private Python binding crate also references the local path so Maturin includes
the patch in source distributions. The public Rust workspace dependency keeps
its Git pin to preserve type identity for downstream Cargo consumers.

The manifest resolves dependencies in the LanceDB workspace, disables
publication, and preserves the upstream Rust lint configuration. Other source files are unchanged. Remove
this copy and the Cargo patch once the pinned Lance release includes the fix.
