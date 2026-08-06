// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Shared helpers for partition-level IVF metadata and writer initialization.
//!
//! This module centralizes common logic used by both the distributed index
//! merger and the classic IVF index builder, to avoid duplicating how we
//! initialize writers and write IVF / index metadata.

pub mod partition_merger;
pub use partition_merger::*;
