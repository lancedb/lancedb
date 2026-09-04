// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

//! The write-path coercion of [`super::cast`], for callers that hold batches rather than a
//! plan.
//!
//! `add` and `insert` reach the coercion as a stage of the plan they were already building.
//! Some callers instead have to hand Lance data that already matches the table: the Python
//! bindings, whose `merge_insert` and `merge` paths bypass the plan, and which used to carry
//! their own reimplementation of all of this. [`write_schema_for`] and [`coerce_batch`] let
//! them run the same code instead.
//!
//! The pair is meant to be used together: ask [`write_schema_for`] what the coerced batches
//! will look like, advertise that schema, then put every batch through [`coerce_batch`].
//! Asking the projection itself for the schema is what keeps the two answers consistent,
//! rather than a second implementation that has to be kept in step by hand.

use std::sync::Arc;

use arrow_array::RecordBatch;
use arrow_schema::{Schema, SchemaRef};
use datafusion_execution::TaskContext;
use datafusion_physical_plan::empty::EmptyExec;
use datafusion_physical_plan::{ExecutionPlan, collect};

use super::cast::cast_to_table_schema;
use super::scannable_exec::ScannableExec;
use crate::table::add_data::validate_schema;
use crate::{Error, Result};

/// The schema batches take on once [`coerce_batch`] has coerced them.
///
/// Fields keep the input's order, not the table's: Lance is indifferent to column order,
/// but the caller may go on to create a table or write an IPC stream header from this
/// schema, where the order is what the user asked for.
///
/// Errors if the input supplies a column the table does not declare, nested ones included,
/// by the same [`validate_schema`] check `add` applies. Columns the table declares and the
/// input omits are simply absent from the result, which is how a partial write
/// (`merge_insert`, `add` with a subset of columns) reaches the storage layer.
pub fn write_schema_for(input_schema: SchemaRef, table_schema: &Schema) -> Result<SchemaRef> {
    validate_schema(&input_schema, table_schema)?;
    let reordered = table_in_input_order(&input_schema, table_schema)?;
    let empty: Arc<dyn ExecutionPlan> = Arc::new(EmptyExec::new(input_schema));
    let write_schema = cast_to_table_schema(empty, &reordered)?.schema();

    // Schema-level metadata is the table's, not the input's. The projection carries the
    // input's through, and drops out altogether when the fields already line up and it is
    // skipped, either of which would lose the metadata a table is created with (the
    // embedding function definitions, among others).
    Ok(Arc::new(
        write_schema
            .as_ref()
            .clone()
            .with_metadata(table_schema.metadata().clone()),
    ))
}

/// Coerce one batch into `write_schema`, which must have come from [`write_schema_for`].
pub fn coerce_batch(batch: RecordBatch, write_schema: &Schema) -> Result<RecordBatch> {
    let num_rows = batch.num_rows();
    let input_schema = batch.schema();
    let plan: Arc<dyn ExecutionPlan> = Arc::new(ScannableExec::new(Box::new(batch), None));
    let plan = cast_to_table_schema(plan, write_schema)?;

    // The plan reads one in-memory batch and projects it, so there is nothing to await on
    // and blocking here cannot stall anyone else's IO.
    let batches = futures::executor::block_on(collect(plan, Arc::new(TaskContext::default())))?;

    let write_schema = Arc::new(write_schema.clone());
    // A projection emits one batch per input batch, but an empty input emits none.
    let coerced = match batches.len() {
        0 => return Ok(RecordBatch::new_empty(write_schema)),
        1 => batches.into_iter().next().expect("length checked"),
        n => {
            return Err(Error::Runtime {
                message: format!(
                    "coercing a batch of {num_rows} rows with schema {input_schema} produced \
                     {n} batches, expected one"
                ),
            });
        }
    };

    // The caller advertised `write_schema` for this stream, so the batch has to carry it
    // exactly, down to the metadata the projection does not thread through. `with_schema`
    // rejects a schema that is not otherwise identical, which would mean the two halves of
    // this module had drifted apart.
    Ok(coerced.with_schema(write_schema)?)
}

/// `table_schema`'s fields, in the order `input_schema` supplies them.
///
/// Only reached once [`validate_schema`] has established that every input column is one the
/// table declares.
fn table_in_input_order(input_schema: &Schema, table_schema: &Schema) -> Result<Schema> {
    let fields = input_schema
        .fields()
        .iter()
        .map(|input_field| {
            table_schema
                .field_with_name(input_field.name())
                .map(|f| Arc::new(f.clone()))
                .map_err(|e| Error::InvalidInput {
                    message: e.to_string(),
                })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(Schema::new_with_metadata(
        fields,
        table_schema.metadata().clone(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blob::blob;
    use arrow_array::{
        Array, Int32Array, Int64Array, LargeBinaryArray, StringArray, StructArray, cast::AsArray,
    };
    use arrow_schema::{DataType, Field};
    use lance_arrow::json::{is_arrow_json_field, json_field};

    fn batch_of(fields: Vec<Field>, columns: Vec<Arc<dyn Array>>) -> RecordBatch {
        RecordBatch::try_new(Arc::new(Schema::new(fields)), columns).unwrap()
    }

    #[test]
    fn write_schema_keeps_the_inputs_column_order() {
        let input = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Int32, true),
            Field::new("a", DataType::Int32, true),
        ]));
        let table = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);

        let write_schema = write_schema_for(input, &table).unwrap();
        let names: Vec<&str> = write_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, ["b", "a"]);
        assert_eq!(write_schema.field(0).data_type(), &DataType::Int64);
    }

    #[test]
    fn write_schema_omits_columns_the_input_does_not_supply() {
        let input = Arc::new(Schema::new(vec![Field::new("a", DataType::Int32, true)]));
        let table = Schema::new(vec![
            Field::new("a", DataType::Int64, true),
            Field::new("b", DataType::Int64, true),
        ]);

        let write_schema = write_schema_for(input, &table).unwrap();
        assert_eq!(write_schema.fields().len(), 1);
        assert_eq!(write_schema.field(0).name(), "a");
    }

    #[test]
    fn a_column_the_table_does_not_declare_is_rejected() {
        let input = Arc::new(Schema::new(vec![Field::new("nope", DataType::Int32, true)]));
        let table = Schema::new(vec![Field::new("a", DataType::Int64, true)]);

        let err = write_schema_for(input, &table).unwrap_err().to_string();
        assert!(
            err.contains("'nope' does not exist in table schema"),
            "{err}"
        );
    }

    /// A struct child the table does not declare would otherwise be dropped without a word.
    #[test]
    fn a_struct_child_the_table_does_not_declare_is_rejected() {
        let struct_of = |children: Vec<Field>| DataType::Struct(children.into());
        let input = Arc::new(Schema::new(vec![Field::new(
            "info",
            struct_of(vec![Field::new("nope", DataType::Int32, true)]),
            true,
        )]));
        let table = Schema::new(vec![Field::new(
            "info",
            struct_of(vec![Field::new("x", DataType::Int64, true)]),
            true,
        )]);

        let err = write_schema_for(input, &table).unwrap_err().to_string();
        assert!(
            err.contains("'nope' does not exist in table schema"),
            "{err}"
        );
    }

    /// A table's schema metadata carries its embedding function definitions, so losing it
    /// here would create a table that has forgotten how to embed.
    #[test]
    fn the_tables_schema_metadata_survives_even_when_no_column_changes() {
        let metadata = std::collections::HashMap::from([(
            "embedding_functions".to_string(),
            "[]".to_string(),
        )]);
        let table = Schema::new_with_metadata(
            vec![Arc::new(Field::new("a", DataType::Int32, true))],
            metadata.clone(),
        );
        let batch = batch_of(
            vec![Field::new("a", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(vec![1]))],
        );

        let write_schema = write_schema_for(batch.schema(), &table).unwrap();
        assert_eq!(write_schema.metadata(), &metadata);
        let coerced = coerce_batch(batch, &write_schema).unwrap();
        assert_eq!(coerced.schema(), write_schema);
    }

    /// The whole point of the pair: what [`write_schema_for`] promises has to be what
    /// [`coerce_batch`] delivers, or the caller advertises a schema its batches do not match.
    #[test]
    fn the_promised_schema_is_the_delivered_schema() {
        let table = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            json_field("doc", true),
            blob("image", true),
            Field::new(
                "info",
                DataType::Struct(
                    vec![
                        Field::new("x", DataType::Int64, true),
                        Field::new("y", DataType::Int64, true),
                    ]
                    .into(),
                ),
                true,
            ),
        ]);
        // Deliberately awkward: reordered columns, a json column as text, a blob column as
        // raw bytes, and a struct whose children are reversed and narrower.
        let batch = batch_of(
            vec![
                Field::new(
                    "info",
                    DataType::Struct(vec![Field::new("y", DataType::Int32, true)].into()),
                    true,
                ),
                Field::new("image", DataType::LargeBinary, true),
                Field::new("doc", DataType::Utf8, true),
                Field::new("id", DataType::Int32, true),
            ],
            vec![
                Arc::new(StructArray::from(vec![(
                    Arc::new(Field::new("y", DataType::Int32, true)),
                    Arc::new(Int32Array::from(vec![7])) as Arc<dyn Array>,
                )])),
                Arc::new(LargeBinaryArray::from_iter_values([b"bytes".as_slice()])),
                Arc::new(StringArray::from(vec![r#"{"k":1}"#])),
                Arc::new(Int32Array::from(vec![1])),
            ],
        );

        let write_schema = write_schema_for(batch.schema(), &table).unwrap();
        let coerced = coerce_batch(batch, &write_schema).unwrap();
        assert_eq!(coerced.schema(), write_schema);

        let names: Vec<&str> = write_schema
            .fields()
            .iter()
            .map(|f| f.name().as_str())
            .collect();
        assert_eq!(names, ["info", "image", "doc", "id"]);
        assert!(is_arrow_json_field(write_schema.field(2)));
        let id: &Int64Array = coerced.column(3).as_primitive_opt().unwrap();
        assert_eq!(id.value(0), 1);
    }

    #[test]
    fn an_empty_batch_coerces_to_an_empty_batch() {
        let table = Schema::new(vec![Field::new("a", DataType::Int64, true)]);
        let batch = batch_of(
            vec![Field::new("a", DataType::Int32, true)],
            vec![Arc::new(Int32Array::from(Vec::<i32>::new()))],
        );

        let write_schema = write_schema_for(batch.schema(), &table).unwrap();
        let coerced = coerce_batch(batch, &write_schema).unwrap();
        assert_eq!(coerced.num_rows(), 0);
        assert_eq!(coerced.schema(), write_schema);
    }

    /// Arrow's cast to a fixed-size list reads the values buffer from index zero however far
    /// into it the offsets point, so a sliced list column would land a row out of step.
    #[test]
    fn a_sliced_vector_column_keeps_its_rows_aligned() {
        use arrow_array::ListArray;
        use arrow_array::types::Int64Type;

        let table = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new(
                "vector",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 2),
                true,
            ),
        ]);
        let vectors = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![
            Some(vec![Some(1), Some(2)]),
            Some(vec![Some(3), Some(4)]),
            Some(vec![Some(5), Some(6)]),
        ]);
        let batch = batch_of(
            vec![
                Field::new("id", DataType::Int64, true),
                Field::new("vector", vectors.data_type().clone(), true),
            ],
            vec![Arc::new(Int64Array::from(vec![0, 1, 2])), Arc::new(vectors)],
        )
        .slice(1, 2);

        let write_schema = write_schema_for(batch.schema(), &table).unwrap();
        let coerced = coerce_batch(batch, &write_schema).unwrap();
        assert_eq!(coerced.schema(), write_schema);

        let vectors = coerced.column(1).as_fixed_size_list();
        let rows: Vec<Vec<f32>> = (0..vectors.len())
            .map(|i| {
                vectors
                    .value(i)
                    .as_primitive::<arrow_array::types::Float32Type>()
                    .values()
                    .to_vec()
            })
            .collect();
        assert_eq!(rows, vec![vec![3.0, 4.0], vec![5.0, 6.0]]);
    }

    /// Coercing a batch that already matches has to be a no-op rather than an error, since
    /// the caller applies [`coerce_batch`] to every batch without inspecting it.
    #[test]
    fn coercing_an_already_coerced_batch_changes_nothing() {
        let table = Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            blob("image", true),
        ]);
        let batch = batch_of(
            vec![
                Field::new("id", DataType::Int32, true),
                Field::new("image", DataType::LargeBinary, true),
            ],
            vec![
                Arc::new(Int32Array::from(vec![1])),
                Arc::new(LargeBinaryArray::from_iter_values([b"bytes".as_slice()])),
            ],
        );

        let write_schema = write_schema_for(batch.schema(), &table).unwrap();
        let once = coerce_batch(batch, &write_schema).unwrap();
        let twice = coerce_batch(once.clone(), &write_schema).unwrap();
        assert_eq!(once, twice);
    }
}
