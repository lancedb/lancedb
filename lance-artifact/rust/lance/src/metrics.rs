// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Metrics published by Lance.
#![doc = include_str!("metrics.md")]
//!
//! The metrics themselves are emitted from the relevant subsystems (for
//! example object store I/O is instrumented in
//! [`lance_io::object_store::metrics`]); this module exists to document the
//! full catalogue of metric names, types, and labels in one place.
