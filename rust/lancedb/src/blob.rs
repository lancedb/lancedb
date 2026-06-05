// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! Helpers for Lance blob v2 columns on local tables.

use std::sync::Arc;

use arrow_array::builder::LargeBinaryBuilder;
use arrow_array::{Array, ArrayRef, LargeBinaryArray, RecordBatch};
use arrow_schema::{DataType, Field, FieldRef, Schema};
use lance::blob::{BlobArrayBuilder, blob_field};
use lance::dataset::{BlobFile, Dataset, WriteParams};
use lance_arrow::schema::FieldExt;
use lance_encoding::version::LanceFileVersion;

use crate::Error;
use crate::error::Result;

/// Create an Arrow field for a Lance blob v2 column.
///
/// Blob columns use `Struct<data: LargeBinary?, uri: Utf8?>` with extension metadata
/// `lance.blob.v2`. Writes require Lance file format >= 2.2 (applied automatically on
/// table creation when a blob column is present).
pub fn blob(name: impl Into<String>, nullable: bool) -> Field {
    blob_field(name.into().as_str(), nullable)
}

/// Returns true if the field is marked as a Lance blob column (v1 metadata or v2 extension).
pub fn is_blob(field: &Field) -> bool {
    field.is_blob()
}

/// Returns true if the field is a Lance blob v2 extension column.
pub fn is_blob_v2(field: &Field) -> bool {
    field.is_blob_v2()
}

/// If the schema contains blob v2 columns, ensure write params use file format >= 2.2.
pub fn apply_blob_storage_version(schema: &Schema, write_params: &mut WriteParams) {
    let needs_v2_2 = schema.fields().iter().any(|f| f.is_blob_v2());
    if !needs_v2_2 {
        return;
    }

    let resolved = write_params
        .data_storage_version
        .unwrap_or(LanceFileVersion::Stable)
        .resolve();
    if resolved < LanceFileVersion::V2_2 {
        write_params.data_storage_version = Some(LanceFileVersion::V2_2);
    }
}

/// Coerce a record batch so blob v2 columns accept user `LargeBinary` / `Binary` input.
pub fn coerce_record_batch_for_blob_columns(
    batch: RecordBatch,
    table_schema: &Schema,
) -> Result<RecordBatch> {
    let input_schema = batch.schema();
    let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
    let mut fields: Vec<FieldRef> = input_schema.fields().to_vec();
    let mut changed = false;

    for table_field in table_schema.fields() {
        if !table_field.is_blob_v2() {
            continue;
        }
        let Ok(input_idx) = input_schema.index_of(table_field.name()) else {
            continue;
        };
        let input_field = &input_schema.fields()[input_idx];
        let column = &columns[input_idx];

        let coerced = match (input_field.data_type(), column.data_type()) {
            (DataType::LargeBinary, DataType::LargeBinary) => {
                coerce_binary_column_to_blob_v2(column, batch.num_rows())?
            }
            (DataType::Binary, DataType::Binary) => {
                let large = arrow_cast::cast(column, &DataType::LargeBinary).map_err(|e| {
                    Error::InvalidInput {
                        message: format!(
                            "failed to cast blob column '{}' to large binary: {}",
                            table_field.name(),
                            e
                        ),
                    }
                })?;
                coerce_binary_column_to_blob_v2(&large, batch.num_rows())?
            }
            (DataType::Struct(_), DataType::Struct(_)) if table_field.is_blob_v2() => {
                // Already blob-shaped input
                continue;
            }
            _ => {
                return Err(Error::InvalidInput {
                    message: format!(
                        "cannot coerce column '{}' with type {:?} into blob v2 struct",
                        table_field.name(),
                        column.data_type()
                    ),
                });
            }
        };
        columns[input_idx] = coerced;
        fields[input_idx] = Arc::new(table_field.as_ref().clone());
        changed = true;
    }

    if !changed {
        return Ok(batch);
    }

    RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).map_err(|e| Error::Runtime {
        message: format!("failed to build record batch after blob coercion: {}", e),
    })
}

fn coerce_binary_column_to_blob_v2(column: &ArrayRef, num_rows: usize) -> Result<ArrayRef> {
    let large = column
        .as_any()
        .downcast_ref::<LargeBinaryArray>()
        .ok_or_else(|| Error::InvalidInput {
            message: "expected large binary array for blob coercion".into(),
        })?;

    let mut builder = BlobArrayBuilder::new(num_rows);
    for row in 0..num_rows {
        if large.is_null(row) {
            builder.push_null().map_err(lance_to_lancedb)?;
        } else {
            builder
                .push_bytes(large.value(row))
                .map_err(lance_to_lancedb)?;
        }
    }
    builder.finish().map_err(lance_to_lancedb)
}

fn lance_to_lancedb(e: lance::Error) -> Error {
    Error::InvalidInput {
        message: e.to_string(),
    }
}

/// Names of blob v2 columns in the given Arrow schema, in declaration order.
pub fn blob_v2_column_names(schema: &Schema) -> Vec<String> {
    schema
        .fields()
        .iter()
        .filter(|f| f.is_blob_v2())
        .map(|f| f.name().clone())
        .collect()
}

fn lookup_blob_field<'a>(
    schema: &'a lance_core::datatypes::Schema,
    column: &str,
) -> Result<&'a lance_core::datatypes::Field> {
    match schema.field(column) {
        Some(field) if field.is_blob() => Ok(field),
        Some(_) => Err(Error::InvalidInput {
            message: format!("column '{column}' is not a blob column"),
        }),
        None => Err(Error::InvalidInput {
            message: format!("no column named '{column}' in this table"),
        }),
    }
}

/// Validate that `column` exists on the table and is a blob column.
///
/// Returns [`Error::InvalidInput`] for missing columns and non-blob columns so
/// callers can surface a clear error before issuing a take.
pub fn ensure_blob_column(schema: &lance_core::datatypes::Schema, column: &str) -> Result<()> {
    lookup_blob_field(schema, column).map(|_| ())
}

/// Read blob bytes for `row_ids` while preserving 1:1 alignment with the input.
///
/// Lance's `take_blobs` returns one [`BlobFile`] per non-null row, so callers
/// cannot zip the result with the row ids they passed in. This helper pre-reads
/// the blob descriptor column to identify null rows, fetches files for just the
/// non-null subset, then stitches nulls back into the output. The resulting
/// [`LargeBinaryArray`] has the same length as `row_ids`, with null entries
/// wherever the blob value was null.
///
/// Null detection mirrors `lance::dataset::blob::collect_blob_entries_v2`: a
/// null blob row has `kind == Inline (0)`, `position == 0`, and `size == 0`.
///
/// Known limitation: a zero-length non-null blob encodes identically to a null
/// (`kind == Inline`, `position == 0`, `size == 0`), so it is reported as null.
/// This originates in Lance's v2 descriptor encoding, not this helper.
pub async fn take_blobs_bytes_aligned(
    dataset: &Arc<Dataset>,
    column: &str,
    row_ids: &[u64],
) -> Result<LargeBinaryArray> {
    use arrow_array::{ArrayRef as ArrowArrayRef, StructArray, UInt8Array, UInt64Array};

    ensure_blob_column(dataset.schema(), column)?;

    if row_ids.is_empty() {
        return Ok(LargeBinaryBuilder::new().finish());
    }

    let projection = dataset.schema().project(&[column])?;
    let descriptions = dataset.take_builder(row_ids, projection)?.execute().await?;
    let desc_col = descriptions
        .column_by_name(column)
        .ok_or_else(|| Error::Runtime {
            message: format!("description column '{column}' missing from take result"),
        })?;
    let desc_struct = desc_col
        .as_any()
        .downcast_ref::<StructArray>()
        .ok_or_else(|| Error::Runtime {
            message: format!("expected struct descriptor for blob column '{column}'"),
        })?;
    let kinds = desc_struct
        .column_by_name("kind")
        .and_then(|c: &ArrowArrayRef| c.as_any().downcast_ref::<UInt8Array>())
        .ok_or_else(|| Error::Runtime {
            message: "blob descriptor 'kind' missing".into(),
        })?;
    let positions = desc_struct
        .column_by_name("position")
        .and_then(|c: &ArrowArrayRef| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| Error::Runtime {
            message: "blob descriptor 'position' missing".into(),
        })?;
    let sizes = desc_struct
        .column_by_name("size")
        .and_then(|c: &ArrowArrayRef| c.as_any().downcast_ref::<UInt64Array>())
        .ok_or_else(|| Error::Runtime {
            message: "blob descriptor 'size' missing".into(),
        })?;
    let is_null_row = |i: usize| {
        desc_struct.is_null(i)
            || (kinds.value(i) == 0 && positions.value(i) == 0 && sizes.value(i) == 0)
    };

    let non_null_row_ids: Vec<u64> = row_ids
        .iter()
        .enumerate()
        .filter_map(|(i, id)| (!is_null_row(i)).then_some(*id))
        .collect();
    let files: Vec<BlobFile> = if non_null_row_ids.is_empty() {
        Vec::new()
    } else {
        dataset.take_blobs(&non_null_row_ids, column).await?
    };
    let mut non_null_bytes: Vec<Vec<u8>> = Vec::with_capacity(files.len());
    for file in files {
        let bytes = file.read().await.map_err(|e| Error::Runtime {
            message: format!("blob read failed: {e}"),
        })?;
        non_null_bytes.push(bytes.to_vec());
    }

    let mut builder = LargeBinaryBuilder::new();
    let mut non_null_iter = non_null_bytes.into_iter();
    for i in 0..row_ids.len() {
        if is_null_row(i) {
            builder.append_null();
        } else {
            let bytes = non_null_iter.next().ok_or_else(|| Error::Runtime {
                message: "non-null blob count mismatch during alignment".to_string(),
            })?;
            builder.append_value(&bytes);
        }
    }
    Ok(builder.finish())
}

/// Read all bytes from blob files into a single large binary array (preserving order).
pub async fn blob_files_to_large_binary(files: &mut [BlobFile]) -> Result<LargeBinaryArray> {
    let mut values: Vec<Option<Vec<u8>>> = Vec::with_capacity(files.len());
    for file in files.iter_mut() {
        let bytes = file.read().await.map_err(|e| Error::Runtime {
            message: format!("failed to read blob: {}", e),
        })?;
        values.push(Some(bytes.to_vec()));
    }
    Ok(LargeBinaryArray::from_iter(values))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Int64Array;

    #[test]
    fn blob_factory_has_v2_extension() {
        let f = blob("image", true);
        assert_eq!(
            f.metadata().get("ARROW:extension:name").map(|s| s.as_str()),
            Some("lance.blob.v2")
        );
    }

    #[test]
    fn coerce_large_binary_to_blob_struct() {
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            blob("image", true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("image", DataType::LargeBinary, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(LargeBinaryArray::from_iter_values([b"hello".as_slice()])),
            ],
        )
        .unwrap();
        let coerced = coerce_record_batch_for_blob_columns(batch, &table_schema).unwrap();
        assert!(
            coerced
                .schema()
                .field_with_name("image")
                .unwrap()
                .is_blob_v2()
        );
        assert!(matches!(
            coerced
                .schema()
                .field_with_name("image")
                .unwrap()
                .data_type(),
            DataType::Struct(_)
        ));
    }

    #[test]
    fn apply_blob_storage_version_bumps_to_2_2() {
        let schema = Schema::new(vec![blob("image", true)]);
        let mut params = WriteParams::default();
        apply_blob_storage_version(&schema, &mut params);
        assert_eq!(
            params.data_storage_version.unwrap().resolve(),
            LanceFileVersion::V2_2
        );
    }

    #[test]
    fn coerce_binary_input_to_blob_struct() {
        use arrow_array::BinaryArray;
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            blob("image", true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("image", DataType::Binary, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(BinaryArray::from_iter_values([b"hi".as_slice()])),
            ],
        )
        .unwrap();
        let coerced = coerce_record_batch_for_blob_columns(batch, &table_schema).unwrap();
        assert!(
            coerced
                .schema()
                .field_with_name("image")
                .unwrap()
                .is_blob_v2()
        );
    }

    #[test]
    fn apply_blob_storage_version_noop_without_blob_columns() {
        let schema = Schema::new(vec![Field::new("id", DataType::Int64, false)]);
        let mut params = WriteParams::default();
        apply_blob_storage_version(&schema, &mut params);
        assert!(params.data_storage_version.is_none());
    }

    #[test]
    fn coerce_rejects_unsupported_input_type() {
        use arrow_array::StringArray;
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            blob("image", true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("image", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["not bytes"])),
            ],
        )
        .unwrap();
        let err = coerce_record_batch_for_blob_columns(batch, &table_schema).unwrap_err();
        assert!(matches!(err, Error::InvalidInput { .. }), "got {err:?}");
        assert!(err.to_string().contains("cannot coerce"));
    }

    #[test]
    fn coerce_binary_with_nulls_preserves_nulls() {
        use arrow_array::{BinaryArray, StructArray};
        let table_schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            blob("image", true),
        ]));
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("id", DataType::Int64, false),
                Field::new("image", DataType::Binary, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(BinaryArray::from_iter(vec![
                    Some(b"present".as_slice()),
                    None,
                ])),
            ],
        )
        .unwrap();
        let coerced = coerce_record_batch_for_blob_columns(batch, &table_schema).unwrap();
        let image = coerced.column_by_name("image").unwrap();
        let s = image.as_any().downcast_ref::<StructArray>().unwrap();
        let data = s.column_by_name("data").unwrap();
        assert_eq!(data.len(), 2);
        assert!(
            data.is_null(1),
            "null binary input coerces to a null data entry"
        );
        assert!(!data.is_null(0));
    }
}
