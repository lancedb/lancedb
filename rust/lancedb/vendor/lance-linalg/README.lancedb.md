This directory contains `rust/lance-linalg` from Lance tag `v15.0.0-beta.14`
(commit `3bb1550f4f27adf90948ff36c2318768e675b599`), licensed under Apache-2.0.

LanceDB patches `src/distance/cosine.rs` to recompute FLOAT32 cosine distances
in FLOAT64 when squared norms or dot products overflow or underflow. Ordinary
vectors retain the existing SIMD kernels. The root Cargo patch applies to all
Lance callers, including exact scans and cached-norm distance calculations. The
private Python binding crate also references the local path so Maturin includes
the patch in source distributions. The public Rust SDK dependency keeps its Git
pin to preserve type identity for downstream Cargo consumers. It is declared in
the SDK manifest because the workspace dependency check excludes the local patch
from its usage scan.

The manifest resolves dependencies in the LanceDB workspace, disables
publication, and preserves the upstream Rust lint configuration. Rust files add
the repository-required copyright header alongside the upstream attribution;
other than that header and the cosine fix, source files are unchanged. Remove
this copy and the Cargo patch once the pinned Lance release includes the fix.
