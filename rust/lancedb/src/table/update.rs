// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::sync::Arc;

use lance::dataset::UpdateBuilder as LanceUpdateBuilder;
use serde::{Deserialize, Serialize};

use super::{BaseTable, NativeTable};
use crate::Error;
use crate::Result;

/// The result of an update operation
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct UpdateResult {
    #[serde(default)]
    pub rows_updated: u64,
    /// The commit version associated with the operation.
    #[serde(default)]
    pub version: u64,
}

/// A builder for configuring a [`crate::table::Table::update`] operation
#[derive(Debug, Clone)]
pub struct UpdateBuilder {
    parent: Arc<dyn BaseTable>,
    pub(crate) filter: Option<String>,
    pub(crate) columns: Vec<(String, String)>,
}

impl UpdateBuilder {
    pub(crate) fn new(parent: Arc<dyn BaseTable>) -> Self {
        Self {
            parent,
            filter: None,
            columns: Vec::new(),
        }
    }

    /// Limits the update operation to rows matching the given filter
    ///
    /// If a row does not match the filter then it will be left unchanged.
    pub fn only_if(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Specifies a column to update
    ///
    /// This method may be called multiple times to update multiple columns
    ///
    /// The `update_expr` should be an SQL expression explaining how to calculate
    /// the new value for the column.  The expression will be evaluated against the
    /// previous row's value.
    pub fn column(
        mut self,
        column_name: impl Into<String>,
        update_expr: impl Into<String>,
    ) -> Self {
        self.columns.push((column_name.into(), update_expr.into()));
        self
    }

    /// Executes the update operation.
    pub async fn execute(self) -> Result<UpdateResult> {
        if self.columns.is_empty() {
            Err(Error::InvalidInput {
                message: "at least one column must be specified in an update operation".to_string(),
            })
        } else {
            self.parent.clone().update(self).await
        }
    }
}

/// Internal implementation of the update logic
pub(crate) async fn execute_update(
    table: &NativeTable,
    update: UpdateBuilder,
) -> Result<UpdateResult> {
    table.dataset.ensure_mutable()?;

    // 1. Snapshot the current dataset
    let dataset = table.dataset.get().await?;

    // 2. Initialize the Lance Core builder
    let mut builder = LanceUpdateBuilder::new(dataset);

    // 3. Apply the filter (WHERE clause)
    if let Some(predicate) = update.filter {
        builder = builder.update_where(&predicate)?;
    }

    // 4. Apply the columns (SET clause)
    for (column, value) in update.columns {
        builder = builder.set(column, &value)?;
    }

    // 5. Execute the operation (Write new files)
    let operation = builder.build()?;
    let res = operation.execute().await?;

    // 6. Update the table's view of the latest version
    table.dataset.update(res.new_dataset.as_ref().clone());

    Ok(UpdateResult {
        rows_updated: res.rows_updated,
        version: res.new_dataset.version().version,
    })
}

#[cfg(test)]
mod tests {
    use crate::connect;
    use crate::query::QueryBase;
    use crate::query::{ExecutableQuery, Select};
    use arrow_array::{
        record_batch, Array, BooleanArray, Date32Array, FixedSizeListArray, Float32Array,
        Float64Array, Int32Array, Int64Array, LargeStringArray, RecordBatch, StringArray,
        TimestampMillisecondArray, TimestampNanosecondArray, UInt32Array,
    };
    use arrow_data::ArrayDataBuilder;
    use arrow_schema::{ArrowError, DataType, Field, Schema, TimeUnit};
    use futures::TryStreamExt;
    use std::sync::Arc;
    use std::time::Duration;

    #[tokio::test]
    async fn test_update_all_types() {
        let conn = connect("memory://")
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("int32", DataType::Int32, false),
            Field::new("int64", DataType::Int64, false),
            Field::new("uint32", DataType::UInt32, false),
            Field::new("string", DataType::Utf8, false),
            Field::new("large_string", DataType::LargeUtf8, false),
            Field::new("float32", DataType::Float32, false),
            Field::new("float64", DataType::Float64, false),
            Field::new("bool", DataType::Boolean, false),
            Field::new("date32", DataType::Date32, false),
            Field::new(
                "timestamp_ns",
                DataType::Timestamp(TimeUnit::Nanosecond, None),
                false,
            ),
            Field::new(
                "timestamp_ms",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(
                "vec_f32",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                false,
            ),
            Field::new(
                "vec_f64",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float64, true)), 2),
                false,
            ),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from_iter_values(0..10)),
                Arc::new(Int64Array::from_iter_values(0..10)),
                Arc::new(UInt32Array::from_iter_values(0..10)),
                Arc::new(StringArray::from_iter_values(vec![
                    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                ])),
                Arc::new(LargeStringArray::from_iter_values(vec![
                    "a", "b", "c", "d", "e", "f", "g", "h", "i", "j",
                ])),
                Arc::new(Float32Array::from_iter_values((0..10).map(|i| i as f32))),
                Arc::new(Float64Array::from_iter_values((0..10).map(|i| i as f64))),
                Arc::new(Into::<BooleanArray>::into(vec![
                    true, false, true, false, true, false, true, false, true, false,
                ])),
                Arc::new(Date32Array::from_iter_values(0..10)),
                Arc::new(TimestampNanosecondArray::from_iter_values(0..10)),
                Arc::new(TimestampMillisecondArray::from_iter_values(0..10)),
                Arc::new(
                    create_fixed_size_list(
                        Float32Array::from_iter_values((0..20).map(|i| i as f32)),
                        2,
                    )
                    .unwrap(),
                ),
                Arc::new(
                    create_fixed_size_list(
                        Float64Array::from_iter_values((0..20).map(|i| i as f64)),
                        2,
                    )
                    .unwrap(),
                ),
            ],
        )
        .unwrap();

        let table = conn
            .create_table("my_table", batch)
            .execute()
            .await
            .unwrap();

        // check it can do update for each type
        let updates: Vec<(&str, &str)> = vec![
            ("string", "'foo'"),
            ("large_string", "'large_foo'"),
            ("int32", "1"),
            ("int64", "1"),
            ("uint32", "1"),
            ("float32", "1.0"),
            ("float64", "1.0"),
            ("bool", "true"),
            ("date32", "1"),
            ("timestamp_ns", "1"),
            ("timestamp_ms", "1"),
            ("vec_f32", "[1.0, 1.0]"),
            ("vec_f64", "[1.0, 1.0]"),
        ];

        let mut update_op = table.update();
        for (column, value) in updates {
            update_op = update_op.column(column, value);
        }
        update_op.execute().await.unwrap();

        let mut batches = table
            .query()
            .select(Select::columns(&[
                "string",
                "large_string",
                "int32",
                "int64",
                "uint32",
                "float32",
                "float64",
                "bool",
                "date32",
                "timestamp_ns",
                "timestamp_ms",
                "vec_f32",
                "vec_f64",
            ]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        let batch = batches.pop().unwrap();

        macro_rules! assert_column {
            ($column:expr, $array_type:ty, $expected:expr) => {
                let array = $column
                    .as_any()
                    .downcast_ref::<$array_type>()
                    .unwrap()
                    .iter()
                    .collect::<Vec<_>>();
                for v in array {
                    assert_eq!(v, Some($expected));
                }
            };
        }

        assert_column!(batch.column(0), StringArray, "foo");
        assert_column!(batch.column(1), LargeStringArray, "large_foo");
        assert_column!(batch.column(2), Int32Array, 1);
        assert_column!(batch.column(3), Int64Array, 1);
        assert_column!(batch.column(4), UInt32Array, 1);
        assert_column!(batch.column(5), Float32Array, 1.0);
        assert_column!(batch.column(6), Float64Array, 1.0);
        assert_column!(batch.column(7), BooleanArray, true);
        assert_column!(batch.column(8), Date32Array, 1);
        assert_column!(batch.column(9), TimestampNanosecondArray, 1);
        assert_column!(batch.column(10), TimestampMillisecondArray, 1);

        let array = batch
            .column(11)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        for v in array {
            let v = v.unwrap();
            let f32array = v.as_any().downcast_ref::<Float32Array>().unwrap();
            for v in f32array {
                assert_eq!(v, Some(1.0));
            }
        }

        let array = batch
            .column(12)
            .as_any()
            .downcast_ref::<FixedSizeListArray>()
            .unwrap()
            .iter()
            .collect::<Vec<_>>();
        for v in array {
            let v = v.unwrap();
            let f64array = v.as_any().downcast_ref::<Float64Array>().unwrap();
            for v in f64array {
                assert_eq!(v, Some(1.0));
            }
        }
    }
    ///Two helper functions
    fn create_fixed_size_list<T: Array>(
        values: T,
        list_size: i32,
    ) -> Result<FixedSizeListArray, ArrowError> {
        let list_type = DataType::FixedSizeList(
            Arc::new(Field::new("item", values.data_type().clone(), true)),
            list_size,
        );
        let data = ArrayDataBuilder::new(list_type)
            .len(values.len() / list_size as usize)
            .add_child_data(values.into_data())
            .build()
            .unwrap();

        Ok(FixedSizeListArray::from(data))
    }

    fn make_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![Field::new("i", DataType::Int32, false)]));
        RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int32Array::from_iter_values(0..10))],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_update_with_predicate() {
        let conn = connect("memory://")
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();

        let batch = record_batch!(
            ("id", Int32, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]),
            (
                "name",
                Utf8,
                ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"]
            )
        )
        .unwrap();

        let table = conn
            .create_table("my_table", batch)
            .execute()
            .await
            .unwrap();

        table
            .update()
            .only_if("id > 5")
            .column("name", "'foo'")
            .execute()
            .await
            .unwrap();

        let mut batches = table
            .query()
            .select(Select::columns(&["id", "name"]))
            .execute()
            .await
            .unwrap()
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        while let Some(batch) = batches.pop() {
            let ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            let names = batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>();
            for (i, name) in names.iter().enumerate() {
                let id = ids[i].unwrap();
                let name = name.unwrap();
                if id > 5 {
                    assert_eq!(name, "foo");
                } else {
                    assert_eq!(name, &format!("{}", (b'a' + id as u8) as char));
                }
            }
        }
    }

    #[tokio::test]
    async fn test_update_via_expr() {
        let conn = connect("memory://")
            .read_consistency_interval(Duration::from_secs(0))
            .execute()
            .await
            .unwrap();
        let tbl = conn
            .create_table("my_table", make_test_batch())
            .execute()
            .await
            .unwrap();
        assert_eq!(1, tbl.count_rows(Some("i == 0".to_string())).await.unwrap());
        tbl.update().column("i", "i+1").execute().await.unwrap();
        assert_eq!(0, tbl.count_rows(Some("i == 0".to_string())).await.unwrap());
    }
}
