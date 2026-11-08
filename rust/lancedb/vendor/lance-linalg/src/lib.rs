// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! High-performance [Apache Arrow](https://docs.rs/arrow/latest/arrow/) native Linear Algebra algorithms.

#![deny(clippy::unused_async)]
#![cfg_attr(target_arch = "loongarch64", feature(stdarch_loongarch))]

use arrow_schema::ArrowError;

mod clustering;
pub mod distance;
pub mod kernels;
pub mod simd;

#[cfg(test)]
pub(crate) mod test_utils;

pub use clustering::Clustering;

type Error = ArrowError;
pub type Result<T> = std::result::Result<T, Error>;
