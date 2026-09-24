// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Encoding and decoding mechanisms described by [`crate::format::pb::ArrayEncoding`].
//!
//! File versions decide which mechanisms to compose and accept. This module
//! contains only the reusable implementation of that persisted grammar.

pub mod logical;
pub mod physical;
mod strategy;

pub use strategy::ArrayFieldEncodingStrategy;
