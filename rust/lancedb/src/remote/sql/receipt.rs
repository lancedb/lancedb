// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use arrow_array::{Array, ArrayRef, RecordBatch, StringArray};
use futures::TryStreamExt;

use crate::sql::Query;
use crate::{Error, Result};

/// The single row returned by a SQL job submission, after its stream completed successfully.
pub(in crate::remote) struct ReceiptRow(RecordBatch);

impl ReceiptRow {
    /// Empty batches carry no receipt data. Zero or multiple rows yield `None`.
    pub(in crate::remote) async fn read(query: &Query) -> Result<Option<Self>> {
        let mut reader = query.reader().await?;
        let mut receipt = None;
        let mut rows = 0usize;
        while let Some(batch) = reader.try_next().await? {
            rows = rows.saturating_add(batch.num_rows());
            if rows == 1 && batch.num_rows() == 1 {
                receipt = Some(Self(batch));
            }
        }
        Ok(receipt.filter(|_| rows == 1))
    }

    pub(in crate::remote) fn column(&self, name: &str) -> Option<&ArrayRef> {
        self.0.column_by_name(name)
    }

    /// An absent column or an empty string can represent a completed no-op.
    /// Each command decides whether its receipt permits that outcome.
    pub(in crate::remote) fn job_id(&self) -> Result<Option<&str>> {
        let Some(column) = self.column("job_id") else {
            return Ok(None);
        };
        let ids = column
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Error::Runtime {
                message: "SQL job receipt job_id is not UTF-8".into(),
            })?;
        if ids.is_null(0) {
            return Err(Error::Runtime {
                message: "SQL job receipt contains a null job_id".into(),
            });
        }
        let id = ids.value(0);
        if id.is_empty() {
            Ok(None)
        } else if id.trim().is_empty() {
            Err(Error::Runtime {
                message: "SQL job receipt contains a blank job_id".into(),
            })
        } else {
            Ok(Some(id))
        }
    }
}
