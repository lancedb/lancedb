// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Dataset Traits

use std::fmt::Debug;

use arrow_array::RecordBatch;

use crate::{Result, datatypes::Schema};

/// `TakeRow` trait.
///
/// It offers a lightweight trait to use `take_rows()` over a dataset, without
/// depending on the `lance` trait.
///
/// <section class="warning">
/// Internal API
/// </section>
#[async_trait::async_trait]
pub trait DatasetTakeRows: Debug + Send + Sync {
    /// The schema of the dataset.
    fn schema(&self) -> &Schema;

    /// Take rows by the internal ROW ids.
    async fn take_rows(&self, row_ids: &[u64], projection: &Schema) -> Result<RecordBatch>;
}
