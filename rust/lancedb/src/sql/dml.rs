// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! The result of a DML statement, as a one-row record batch.
//!
//! A DML statement has to answer over the same channel a query does, so its
//! result is carried as an ordinary [`RecordBatch`] with a fixed schema. The
//! round trip is lossless, which is what lets a caller recover the typed form
//! after the batch has crossed a transport such as Arrow Flight.

use std::fmt;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};

/// Which DML statement produced a result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DmlOperation {
    Insert,
    Update,
    Delete,
}

impl DmlOperation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Insert => "INSERT",
            Self::Update => "UPDATE",
            Self::Delete => "DELETE",
        }
    }

    fn parse(s: &str) -> Option<Self> {
        match s {
            "INSERT" => Some(Self::Insert),
            "UPDATE" => Some(Self::Update),
            "DELETE" => Some(Self::Delete),
            _ => None,
        }
    }
}

impl fmt::Display for DmlOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a DML statement did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DmlResult {
    pub table: String,
    pub operation: DmlOperation,
    pub rows_affected: i64,
    pub version: i64,
}

/// The schema every [`DmlResult`] batch carries.
pub fn dml_result_schema() -> Schema {
    Schema::new(vec![
        Field::new("table", DataType::Utf8, false),
        Field::new("operation", DataType::Utf8, false),
        Field::new("rows_affected", DataType::Int64, false),
        Field::new("version", DataType::Int64, false),
    ])
}

impl DmlResult {
    pub fn new(
        table: impl Into<String>,
        operation: DmlOperation,
        rows_affected: i64,
        version: i64,
    ) -> Self {
        Self {
            table: table.into(),
            operation,
            rows_affected,
            version,
        }
    }

    pub fn to_record_batch(&self) -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(dml_result_schema()),
            vec![
                Arc::new(StringArray::from(vec![self.table.as_str()])),
                Arc::new(StringArray::from(vec![self.operation.as_str()])),
                Arc::new(Int64Array::from(vec![self.rows_affected])),
                Arc::new(Int64Array::from(vec![self.version])),
            ],
        )
        .expect("static schema")
    }

    /// Recover a result from a batch, or `None` if the batch is not one.
    ///
    /// A query result can arrive on the same channel, so this has to be able
    /// to say "not a DML result" rather than fail.
    pub fn try_from_batch(batch: &RecordBatch) -> Option<Self> {
        if *batch.schema().as_ref() != dml_result_schema() || batch.num_rows() != 1 {
            return None;
        }
        Some(Self {
            table: batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()?
                .value(0)
                .to_string(),
            operation: DmlOperation::parse(
                batch
                    .column(1)
                    .as_any()
                    .downcast_ref::<StringArray>()?
                    .value(0),
            )?,
            rows_affected: batch
                .column(2)
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(0),
            version: batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()?
                .value(0),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip() {
        let original = DmlResult::new("foo", DmlOperation::Insert, 1, 3);
        let batch = original.to_record_batch();
        assert_eq!(DmlResult::try_from_batch(&batch), Some(original));
    }

    #[test]
    fn non_dml_returns_none() {
        let s = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(s, vec![Arc::new(Int64Array::from(vec![1]))]).unwrap();
        assert_eq!(DmlResult::try_from_batch(&batch), None);
    }
}
