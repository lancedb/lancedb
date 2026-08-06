// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Utilities for integrating Lance into DataFusion

pub(crate) mod dataframe;
pub(crate) mod logical_plan;

pub use dataframe::LanceTableProvider;
