// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use async_trait::async_trait;
use datafusion::execution::SendableRecordBatchStream;
use lance_core::Result;

pub mod hilbert_sort;

#[async_trait]
pub trait Sorter {
    async fn sort(&self, data: SendableRecordBatchStream) -> Result<SendableRecordBatchStream>;
}
