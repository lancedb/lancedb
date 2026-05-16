// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

use std::{iter::repeat_with, sync::Arc};

use arrow_array::{
    Array, ArrowNumericType, FixedSizeListArray, PrimitiveArray, RecordBatch, RecordBatchIterator,
    RecordBatchReader,
    cast::AsArray,
    types::{Float16Type, Float32Type, Float64Type, Int32Type, Int64Type},
};
use arrow_cast::{can_cast_types, cast};
use arrow_schema::{ArrowError, DataType, Field, Schema};
use futures::StreamExt;
use half::f16;
use lance_arrow::{DataTypeExt, FixedSizeListArrayExt};
use log::warn;
use num_traits::cast::AsPrimitive;

use super::inspect::{infer_dimension, infer_fsl_schema};
use crate::arrow::{SendableRecordBatchStream, SimpleRecordBatchStream};
use crate::data::scannable::{PeekedScannable, Scannable};
use crate::error::Result;

/// A [`Scannable`] wrapper that applies [`coerce_schema_batch`] to every batch,
/// converting columns to the target schema (e.g. List → FixedSizeList).
pub(crate) struct CoerceScannable {
    pub(crate) inner: Box<dyn Scannable>,
    pub(crate) schema: Arc<Schema>,
}

impl Scannable for CoerceScannable {
    fn schema(&self) -> arrow_schema::SchemaRef {
        self.schema.clone()
    }

    fn num_rows(&self) -> Option<usize> {
        self.inner.num_rows()
    }

    fn rescannable(&self) -> bool {
        self.inner.rescannable()
    }

    fn scan_as_stream(&mut self) -> SendableRecordBatchStream {
        let inner_stream = self.inner.scan_as_stream();
        let target_schema = self.schema.clone();
        let stream = inner_stream.map(move |batch_result| {
            batch_result.and_then(|batch| {
                coerce_schema_batch(batch, target_schema.clone()).map_err(crate::Error::from)
            })
        });
        Box::pin(SimpleRecordBatchStream {
            schema: self.schema.clone(),
            stream,
        })
    }
}

/// Run [`maybe_infer_fsl_scannable`] on `add.data` if the add is an Overwrite *and*
/// schema inference is enabled.
///
/// Inference is intentionally Overwrite-only: for Append we already have an existing
/// table schema and rely on the schema-cast path to align incoming data; running
/// inference again would risk producing a schema that disagrees with the table.
/// See the doc on [`crate::table::AddDataBuilder`] for the user-facing contract.
pub(crate) async fn maybe_infer_for_overwrite(
    add: &mut crate::table::AddDataBuilder,
) -> Result<()> {
    use crate::table::AddDataMode;
    if matches!(add.mode, AddDataMode::Overwrite) && add.write_options.infer_vector_columns {
        let data = std::mem::replace(&mut add.data, Box::new(Vec::<RecordBatch>::new()));
        add.data = maybe_infer_fsl_scannable(data).await?;
    }
    Ok(())
}

/// Peek at the first batch of `data` to infer FSL schema, then return a
/// [`Scannable`] that applies the conversion if the schema changed.
///
/// Variable-length list columns named "vector" or "embedding" are converted to
/// FixedSizeList when a uniform dimension can be inferred.
pub async fn maybe_infer_fsl_scannable(data: Box<dyn Scannable>) -> Result<Box<dyn Scannable>> {
    let mut peeked = PeekedScannable::new(data);
    let Some(first_batch) = peeked.peek().await else {
        return Ok(Box::new(peeked));
    };

    let original_schema = peeked.schema();
    let inferred_schema = infer_fsl_schema(&first_batch);

    if inferred_schema != original_schema {
        Ok(Box::new(CoerceScannable {
            inner: Box::new(peeked),
            schema: inferred_schema,
        }))
    } else {
        Ok(Box::new(peeked))
    }
}

fn cast_array<I: ArrowNumericType, O: ArrowNumericType>(
    arr: &PrimitiveArray<I>,
) -> Arc<PrimitiveArray<O>>
where
    I::Native: AsPrimitive<O::Native>,
{
    Arc::new(PrimitiveArray::<O>::from_iter_values(
        arr.values().iter().map(|v| (*v).as_()),
    ))
}

fn cast_float_array<I: ArrowNumericType>(
    arr: &PrimitiveArray<I>,
    dt: &DataType,
) -> std::result::Result<Arc<dyn Array>, ArrowError>
where
    I::Native: AsPrimitive<f64> + AsPrimitive<f32> + AsPrimitive<f16>,
{
    match dt {
        DataType::Float16 => Ok(cast_array::<I, Float16Type>(arr)),
        DataType::Float32 => Ok(cast_array::<I, Float32Type>(arr)),
        DataType::Float64 => Ok(cast_array::<I, Float64Type>(arr)),
        _ => Err(ArrowError::SchemaError(format!(
            "Incompatible change field: unable to coerce {:?} to {:?}",
            arr.data_type(),
            dt
        ))),
    }
}

/// Coerce a `List`/`LargeList` array to a `FixedSizeList` of the target dimension.
///
/// Returns a `SchemaError` if any row's length does not match the expected dimension.
/// This is the intended default: silently nulling out mismatched rows (which is what
/// `arrow_cast::cast` does for this conversion) loses data without telling the user.
fn coerce_list_to_fixed_size_list(
    array: &Arc<dyn Array>,
    field: &Field,
) -> std::result::Result<Arc<dyn Array>, ArrowError> {
    let DataType::FixedSizeList(_, exp_dim) = field.data_type() else {
        unreachable!("caller guarantees target is FixedSizeList");
    };

    let dim = match array.data_type() {
        DataType::List(_) => infer_dimension::<Int32Type>(array.as_list::<i32>())
            .map_err(|e| {
                ArrowError::SchemaError(format!("failed to infer dimension from list: {}", e))
            })?
            .map(|d| d as i64),
        DataType::LargeList(_) => {
            infer_dimension::<Int64Type>(array.as_list::<i64>()).map_err(|e| {
                ArrowError::SchemaError(format!("failed to infer dimension from large list: {}", e))
            })?
        }
        _ => unreachable!("caller guarantees source is List or LargeList"),
    };

    let Some(dim) = dim else {
        return Err(ArrowError::SchemaError(format!(
            "Cannot coerce column {} to {:?}: input rows have non-uniform lengths",
            field.name(),
            field.data_type(),
        )));
    };

    if dim != *exp_dim as i64 {
        return Err(ArrowError::SchemaError(format!(
            "Cannot coerce column {} to {:?}: expected dimension {} but got {}",
            field.name(),
            field.data_type(),
            exp_dim,
            dim,
        )));
    }

    // Dimension is uniform and matches the target; arrow_cast::cast will produce a
    // FixedSizeList with no row dropped, also handling the inner element type cast.
    cast(array, field.data_type())
}

fn coerce_array(
    array: &Arc<dyn Array>,
    field: &Field,
) -> std::result::Result<Arc<dyn Array>, ArrowError> {
    if array.data_type() == field.data_type() {
        return Ok(array.clone());
    }
    match (array.data_type(), field.data_type()) {
        // List/LargeList -> FixedSizeList must use the explicit branch below so that
        // rows whose length disagrees with the target dimension produce a SchemaError
        // rather than being silently nulled out by arrow_cast::cast.
        (DataType::List(_) | DataType::LargeList(_), DataType::FixedSizeList(_, _)) => {
            coerce_list_to_fixed_size_list(array, field)
        }
        // Normal cast-able types.
        (adt, dt) if can_cast_types(adt, dt) => cast(&array, dt),
        // Casting between f16/f32/f64 can be lossy.
        (adt, dt) if (adt.is_floating() || dt.is_floating()) => {
            if adt.byte_width() > dt.byte_width() {
                warn!(
                    "Coercing field {} {:?} to {:?} might lose precision",
                    field.name(),
                    adt,
                    dt
                );
            }
            match adt {
                DataType::Float16 => cast_float_array(array.as_primitive::<Float16Type>(), dt),
                DataType::Float32 => cast_float_array(array.as_primitive::<Float32Type>(), dt),
                DataType::Float64 => cast_float_array(array.as_primitive::<Float64Type>(), dt),
                _ => unreachable!(),
            }
        }
        // Cast a fixed size array with the same dimension to the expected element type.
        (DataType::FixedSizeList(_, dim), DataType::FixedSizeList(exp_field, exp_dim))
            if dim == exp_dim =>
        {
            let actual_sub = array.as_fixed_size_list();
            let values = coerce_array(actual_sub.values(), exp_field)?;
            Ok(Arc::new(FixedSizeListArray::try_new_from_values(
                values.clone(),
                *dim,
            )?) as Arc<dyn Array>)
        }
        _ => Err(ArrowError::SchemaError(format!(
            "Incompatible change field {}: unable to coerce {:?} to {:?}",
            field.name(),
            array.data_type(),
            field.data_type()
        )))?,
    }
}

pub(crate) fn coerce_schema_batch(
    batch: RecordBatch,
    schema: Arc<Schema>,
) -> std::result::Result<RecordBatch, ArrowError> {
    if batch.schema() == schema {
        return Ok(batch);
    }
    let columns = schema
        .fields()
        .iter()
        .map(|field| {
            batch
                .column_by_name(field.name())
                .ok_or_else(|| {
                    ArrowError::SchemaError(format!("Column {} not found", field.name()))
                })
                .and_then(|c| coerce_array(c, field))
        })
        .collect::<std::result::Result<Vec<_>, ArrowError>>()?;
    RecordBatch::try_new(schema, columns)
}

/// Coerce the reader (input data) to match the given [Schema].
pub fn coerce_schema(
    reader: impl RecordBatchReader + Send + 'static,
    schema: Arc<Schema>,
) -> Result<Box<dyn RecordBatchReader + Send>> {
    if reader.schema() == schema {
        return Ok(Box::new(RecordBatchIterator::new(reader, schema)));
    }
    let s = schema.clone();
    let batches = reader
        .zip(repeat_with(move || s.clone()))
        .map(|(batch, s)| coerce_schema_batch(batch?, s));
    Ok(Box::new(RecordBatchIterator::new(batches, schema)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow_array::{
        FixedSizeListArray, Float16Array, Float32Array, Float64Array, Int8Array, Int32Array,
        ListArray, RecordBatch, RecordBatchIterator, StringArray, types::Float32Type,
    };
    use arrow_schema::Field;
    use futures::TryStreamExt;
    use half::f16;
    use lance_arrow::FixedSizeListArrayExt;

    use crate::data::scannable::Scannable;

    #[test]
    fn test_coerce_list_to_fixed_size_list() {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                "fl",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 64),
                true,
            ),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float16, true),
            Field::new("i", DataType::Int32, true),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        Float32Array::from_iter_values((0..256).map(|v| v as f32)),
                        64,
                    )
                    .unwrap(),
                ),
                Arc::new(StringArray::from(vec![
                    Some("hello"),
                    Some("world"),
                    Some("from"),
                    Some("lance"),
                ])),
                Arc::new(Float16Array::from_iter_values(
                    (0..4).map(|v| f16::from_f32(v as f32)),
                )),
                Arc::new(Int32Array::from_iter_values(0..4)),
            ],
        )
        .unwrap();
        let reader =
            RecordBatchIterator::new(vec![batch.clone()].into_iter().map(Ok), schema.clone());

        let expected_schema = Arc::new(Schema::new(vec![
            Field::new(
                "fl",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float16, true)), 64),
                true,
            ),
            Field::new("s", DataType::Utf8, true),
            Field::new("f", DataType::Float64, true),
            Field::new("i", DataType::Int8, true),
        ]));
        let stream = coerce_schema(reader, expected_schema.clone()).unwrap();
        let batches = stream.collect::<Vec<_>>();
        assert_eq!(batches.len(), 1);
        let batch = batches[0].as_ref().unwrap();
        assert_eq!(batch.schema(), expected_schema);

        let expected = RecordBatch::try_new(
            expected_schema,
            vec![
                Arc::new(
                    FixedSizeListArray::try_new_from_values(
                        Float16Array::from_iter_values((0..256).map(|v| f16::from_f32(v as f32))),
                        64,
                    )
                    .unwrap(),
                ),
                Arc::new(StringArray::from(vec![
                    Some("hello"),
                    Some("world"),
                    Some("from"),
                    Some("lance"),
                ])),
                Arc::new(Float64Array::from_iter_values((0..4).map(|v| v as f64))),
                Arc::new(Int8Array::from_iter_values(0..4)),
            ],
        )
        .unwrap();
        assert_eq!(batch, &expected);
    }

    #[tokio::test]
    async fn test_coerce_scannable_multi_batch_list_to_fsl() {
        // Two batches of List<Float32> coerced to FixedSizeList<Float32, 3>.
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let make_batch = |start: f32| {
            RecordBatch::try_new(
                source_schema.clone(),
                vec![Arc::new(
                    ListArray::from_iter_primitive::<Float32Type, _, _>((0..2).map(|i| {
                        Some(vec![
                            Some(start + i as f32),
                            Some(start + i as f32 + 0.1),
                            Some(start + i as f32 + 0.2),
                        ])
                    })),
                )],
            )
            .unwrap()
        };
        let batches = vec![make_batch(0.0), make_batch(10.0)];

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            true,
        )]));

        let mut scannable = CoerceScannable {
            inner: Box::new(batches.clone()),
            schema: target_schema.clone(),
        };
        let stream = scannable.scan_as_stream();
        let results: Vec<RecordBatch> = stream.try_collect().await.unwrap();
        assert_eq!(results.len(), 2);
        for batch in &results {
            assert_eq!(batch.schema(), target_schema);
            assert_eq!(batch.num_rows(), 2);
            assert!(matches!(
                batch.column(0).data_type(),
                DataType::FixedSizeList(_, 3)
            ));
        }
    }

    #[tokio::test]
    async fn test_coerce_scannable_heterogeneous_batches_error_on_mismatched_dim() {
        // First batch has uniform length 3 (matches the inferred target). Second
        // batch has a row of length 4. The coercion must return a SchemaError
        // rather than silently nulling out the mismatched row — silent data loss
        // is user-hostile and there is no way for the caller to detect it.
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let good = RecordBatch::try_new(
            source_schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(
                    (0..2).map(|_| Some(vec![Some(1.0f32), Some(2.0), Some(3.0)])),
                ),
            )],
        )
        .unwrap();
        let bad = RecordBatch::try_new(
            source_schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(
                    (0..2).map(|_| Some(vec![Some(4.0f32), Some(5.0), Some(6.0), Some(7.0)])),
                ),
            )],
        )
        .unwrap();
        let batches = vec![good, bad];

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            true,
        )]));

        let mut scannable = CoerceScannable {
            inner: Box::new(batches),
            schema: target_schema.clone(),
        };
        let mut stream = scannable.scan_as_stream();

        let first = stream.next().await.expect("first batch present").unwrap();
        assert_eq!(first.schema(), target_schema);
        assert_eq!(first.column(0).as_fixed_size_list().null_count(), 0);

        let err = stream
            .next()
            .await
            .expect("second batch present")
            .expect_err("expected SchemaError on dimension mismatch");
        let msg = err.to_string();
        assert!(
            msg.contains("expected dimension 3") && msg.contains("got 4"),
            "error message should report expected vs actual dim, got: {msg}",
        );
    }

    #[tokio::test]
    async fn test_coerce_scannable_error_on_non_uniform_lengths_within_batch() {
        let source_schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::List(Arc::new(Field::new("item", DataType::Float32, true))),
            true,
        )]));
        let batch = RecordBatch::try_new(
            source_schema.clone(),
            vec![Arc::new(
                ListArray::from_iter_primitive::<Float32Type, _, _>(vec![
                    Some(vec![Some(1.0f32), Some(2.0), Some(3.0)]),
                    Some(vec![Some(4.0f32), Some(5.0), Some(6.0), Some(7.0)]),
                ]),
            )],
        )
        .unwrap();

        let target_schema = Arc::new(Schema::new(vec![Field::new(
            "embedding",
            DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 3),
            true,
        )]));

        let mut scannable = CoerceScannable {
            inner: Box::new(vec![batch]),
            schema: target_schema,
        };
        let mut stream = scannable.scan_as_stream();
        let err = stream
            .next()
            .await
            .expect("batch present")
            .expect_err("expected SchemaError on non-uniform lengths");
        assert!(
            err.to_string().contains("non-uniform lengths"),
            "error should mention non-uniform lengths, got: {err}",
        );
    }
}
