// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Contains the [PermutationBuilder] to create a permutation "view" of an existing table.
//!
//! A permutation view can apply a filter, divide the data into splits, and shuffle the data.
//! The permutation table only stores the split ids and row ids.  It is not a materialized copy of
//! the underlying data and can be very lightweight.
//!
//! Building a permutation table should be fairly quick and memory efficient, even for billions or
//! trillions of rows.

pub mod builder;
pub mod reader;
pub mod shuffle;
pub mod split;
pub mod util;
