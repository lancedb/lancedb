// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! Encoding support for complex FixedSizeList types (FSL with non-primitive children).
//!
//! Primitive FSL (e.g., `FixedSizeList<Int32>`) is handled in the physical encoding layer.
//! This module handles FSL with complex children (Struct, Map, List) which require
//! structural encoding.

use std::{ops::Range, sync::Arc};

use arrow_array::{Array, ArrayRef, GenericListArray, OffsetSizeTrait, StructArray, cast::AsArray};
use arrow_buffer::{BooleanBufferBuilder, NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::DataType;
use futures::future::BoxFuture;
use lance_arrow::deepcopy::deep_copy_nulls;
use lance_core::{Error, Result};

use crate::{
    decoder::{
        DecodedArray, FilterExpression, ScheduledScanLine, SchedulerContext,
        StructuralDecodeArrayTask, StructuralFieldDecoder, StructuralFieldScheduler,
        StructuralSchedulingJob,
    },
    encoder::{EncodeTask, FieldEncoder, OutOfLineBuffers},
    repdef::RepDefBuilder,
};

/// A structural encoder for complex fixed-size list fields
///
/// The FSL's validity is added to the rep/def builder along with the dimension
/// and the FSL array's values are passed to the child encoder.
pub struct FixedSizeListStructuralEncoder {
    keep_original_array: bool,
    child: Box<dyn FieldEncoder>,
}

impl FixedSizeListStructuralEncoder {
    pub fn new(keep_original_array: bool, child: Box<dyn FieldEncoder>) -> Self {
        Self {
            keep_original_array,
            child,
        }
    }
}

impl FieldEncoder for FixedSizeListStructuralEncoder {
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        external_buffers: &mut OutOfLineBuffers,
        mut repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        let fsl_arr = array.as_fixed_size_list_opt().ok_or_else(|| {
            Error::internal("FixedSizeList encoder used for non-fixed-size-list data".to_string())
        })?;

        let dimension = fsl_arr.value_length() as usize;
        let values = fsl_arr.values().clone();

        let validity = if self.keep_original_array {
            array.nulls().cloned()
        } else {
            deep_copy_nulls(array.nulls())
        };
        repdef.add_fsl(validity.clone(), dimension, fsl_arr.len());

        // FSL forces child elements to exist even under null rows. Normalize any
        // nested lists under null FSL rows to null empty lists.
        let values = if let Some(ref fsl_validity) = validity {
            if needs_garbage_filtering(values.data_type()) {
                let is_garbage =
                    expand_garbage_mask(&fsl_validity_to_garbage_mask(fsl_validity), dimension);
                filter_fsl_child_garbage(values, &is_garbage)
            } else {
                values
            }
        } else {
            values
        };

        self.child.maybe_encode(
            values,
            external_buffers,
            repdef,
            row_number,
            num_rows * dimension as u64,
        )
    }

    fn flush(&mut self, external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>> {
        self.child.flush(external_buffers)
    }

    fn num_columns(&self) -> u32 {
        self.child.num_columns()
    }

    fn finish(
        &mut self,
        external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<crate::encoder::EncodedColumn>>> {
        self.child.finish(external_buffers)
    }
}

/// A scheduler for complex fixed-size list fields
///
/// Scales row ranges by the FSL dimension when scheduling child rows,
/// and scales scheduled rows back when reporting to the parent.
#[derive(Debug)]
pub struct StructuralFixedSizeListScheduler {
    child: Box<dyn StructuralFieldScheduler>,
    dimension: u64,
}

impl StructuralFixedSizeListScheduler {
    pub fn new(child: Box<dyn StructuralFieldScheduler>, dimension: i32) -> Self {
        Self {
            child,
            dimension: dimension as u64,
        }
    }
}

impl StructuralFieldScheduler for StructuralFixedSizeListScheduler {
    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Box<dyn StructuralSchedulingJob + 'a>> {
        // Scale ranges by dimension for the child - each FSL row becomes `dimension` child rows
        let child_ranges: Vec<Range<u64>> = ranges
            .iter()
            .map(|r| (r.start * self.dimension)..(r.end * self.dimension))
            .collect();
        let child = self.child.schedule_ranges(&child_ranges, filter)?;
        Ok(Box::new(StructuralFixedSizeListSchedulingJob::new(
            child,
            self.dimension,
        )))
    }

    fn initialize<'a>(
        &'a mut self,
        filter: &'a FilterExpression,
        context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>> {
        self.child.initialize(filter, context)
    }
}

#[derive(Debug)]
struct StructuralFixedSizeListSchedulingJob<'a> {
    child: Box<dyn StructuralSchedulingJob + 'a>,
    dimension: u64,
}

impl<'a> StructuralFixedSizeListSchedulingJob<'a> {
    fn new(child: Box<dyn StructuralSchedulingJob + 'a>, dimension: u64) -> Self {
        Self { child, dimension }
    }
}

impl StructuralSchedulingJob for StructuralFixedSizeListSchedulingJob<'_> {
    fn schedule_next(&mut self, context: &mut SchedulerContext) -> Result<Vec<ScheduledScanLine>> {
        // Get the child's scan lines (scheduled in terms of child struct rows)
        let child_scan_lines = self.child.schedule_next(context)?;

        // Scale down rows_scheduled by dimension to convert from child rows to FSL rows
        Ok(child_scan_lines
            .into_iter()
            .map(|scan_line| ScheduledScanLine {
                decoders: scan_line.decoders,
                rows_scheduled: scan_line.rows_scheduled / self.dimension,
            })
            .collect())
    }
}

/// A decoder for complex fixed-size list fields
///
/// Drains `num_rows * dimension` from the child decoder and reconstructs
/// the FSL array with validity from the rep/def information.
#[derive(Debug)]
pub struct StructuralFixedSizeListDecoder {
    child: Box<dyn StructuralFieldDecoder>,
    data_type: DataType,
}

impl StructuralFixedSizeListDecoder {
    pub fn new(child: Box<dyn StructuralFieldDecoder>, data_type: DataType) -> Self {
        Self { child, data_type }
    }
}

impl StructuralFieldDecoder for StructuralFixedSizeListDecoder {
    fn accept_page(&mut self, child: crate::decoder::LoadedPageShard) -> Result<()> {
        self.child.accept_page(child)
    }

    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn StructuralDecodeArrayTask>> {
        // For FixedSizeList, we need to drain num_rows * dimension from the child
        let dimension = match &self.data_type {
            DataType::FixedSizeList(_, d) => *d as u64,
            _ => {
                return Err(Error::internal(
                    "FixedSizeListDecoder has non-FSL data type".to_string(),
                ));
            }
        };
        let child_task = self.child.drain(num_rows * dimension)?;
        Ok(Box::new(StructuralFixedSizeListDecodeTask::new(
            child_task,
            self.data_type.clone(),
            num_rows,
        )))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[derive(Debug)]
struct StructuralFixedSizeListDecodeTask {
    child_task: Box<dyn StructuralDecodeArrayTask>,
    data_type: DataType,
    num_rows: u64,
}

impl StructuralFixedSizeListDecodeTask {
    fn new(
        child_task: Box<dyn StructuralDecodeArrayTask>,
        data_type: DataType,
        num_rows: u64,
    ) -> Self {
        Self {
            child_task,
            data_type,
            num_rows,
        }
    }
}

impl StructuralDecodeArrayTask for StructuralFixedSizeListDecodeTask {
    fn decode(self: Box<Self>) -> Result<DecodedArray> {
        let DecodedArray {
            array,
            mut repdef,
            data_size,
        } = self.child_task.decode()?;
        match &self.data_type {
            DataType::FixedSizeList(child_field, dimension) => {
                let num_rows = self.num_rows as usize;
                let validity = repdef.unravel_fsl_validity(num_rows, *dimension as usize)?;
                let fsl_array = arrow_array::FixedSizeListArray::try_new(
                    child_field.clone(),
                    *dimension,
                    array,
                    validity,
                )?;
                Ok(DecodedArray {
                    array: Arc::new(fsl_array),
                    repdef,
                    data_size,
                })
            }
            _ => Err(Error::internal(
                "FixedSizeList decoder did not have a fixed-size list field".to_string(),
            )),
        }
    }
}

// =======================
// Garbage filtering
// =======================

/// Returns true if the data type contains any variable-length list-like types
/// (List, LargeList, ListView, LargeListView, Map) that need garbage filtering.
fn needs_garbage_filtering(data_type: &DataType) -> bool {
    match data_type {
        DataType::List(_)
        | DataType::LargeList(_)
        | DataType::ListView(_)
        | DataType::LargeListView(_)
        | DataType::Map(_, _) => true,
        DataType::Struct(fields) => fields
            .iter()
            .any(|f| needs_garbage_filtering(f.data_type())),
        DataType::FixedSizeList(field, _) => needs_garbage_filtering(field.data_type()),
        _ => false,
    }
}

/// Filters garbage (undefined data under null FSL rows) from nested list-like types.
/// Unlike variable-length lists which can remove null children entirely, FSL children
/// always exist, so we must clean any nested lists before encoding.
///
/// NB: Nested FSL is currently precluded at a higher level in our system. However, this code
/// supports and tests it.
fn filter_fsl_child_garbage(array: ArrayRef, is_garbage: &[bool]) -> ArrayRef {
    debug_assert_eq!(array.len(), is_garbage.len());

    match array.data_type() {
        DataType::List(_) => filter_list_garbage(array.as_list::<i32>(), is_garbage),
        DataType::LargeList(_) => filter_list_garbage(array.as_list::<i64>(), is_garbage),
        DataType::ListView(_) | DataType::LargeListView(_) => {
            unimplemented!("ListView inside complex FSL is not yet supported")
        }
        DataType::Map(_, _) => filter_map_garbage(array.as_map(), is_garbage),
        DataType::FixedSizeList(_, dim) => {
            filter_nested_fsl_garbage(array.as_fixed_size_list(), is_garbage, *dim as usize)
        }
        DataType::Struct(_) => filter_struct_garbage(array.as_struct(), is_garbage),
        _ => array,
    }
}

fn filter_struct_garbage(struct_arr: &StructArray, is_garbage: &[bool]) -> ArrayRef {
    let needs_filtering = struct_arr
        .fields()
        .iter()
        .any(|f| needs_garbage_filtering(f.data_type()));

    if !needs_filtering {
        return Arc::new(struct_arr.clone());
    }

    let new_columns: Vec<ArrayRef> = struct_arr
        .columns()
        .iter()
        .zip(struct_arr.fields().iter())
        .map(|(col, field)| {
            if needs_garbage_filtering(field.data_type()) {
                filter_fsl_child_garbage(col.clone(), is_garbage)
            } else {
                col.clone()
            }
        })
        .collect();

    Arc::new(StructArray::new(
        struct_arr.fields().clone(),
        new_columns,
        struct_arr.nulls().cloned(),
    ))
}

fn expand_garbage_mask(is_garbage: &[bool], dimension: usize) -> Vec<bool> {
    let mut expanded = Vec::with_capacity(is_garbage.len() * dimension);
    for &garbage in is_garbage {
        for _ in 0..dimension {
            expanded.push(garbage);
        }
    }
    expanded
}

fn fsl_validity_to_garbage_mask(fsl_validity: &NullBuffer) -> Vec<bool> {
    fsl_validity.iter().map(|valid| !valid).collect()
}

fn filter_list_garbage<O: OffsetSizeTrait>(
    list_arr: &GenericListArray<O>,
    is_garbage: &[bool],
) -> ArrayRef {
    debug_assert_eq!(
        list_arr.len(),
        is_garbage.len(),
        "list length must match garbage mask length"
    );

    let old_offsets = list_arr.offsets();
    let value_field = match list_arr.data_type() {
        DataType::List(f) | DataType::LargeList(f) => f.clone(),
        _ => unreachable!(),
    };

    let mut new_offsets: Vec<O> = Vec::with_capacity(list_arr.len() + 1);
    let mut values_to_keep: Vec<usize> = Vec::new();
    let mut validity_builder = BooleanBufferBuilder::new(list_arr.len());
    let mut current_offset = O::usize_as(0);
    new_offsets.push(current_offset);
    let old_validity = list_arr.nulls();

    for (i, &garbage) in is_garbage.iter().enumerate() {
        if garbage {
            new_offsets.push(current_offset);
            validity_builder.append(false);
        } else {
            let start = old_offsets[i].as_usize();
            let end = old_offsets[i + 1].as_usize();
            values_to_keep.extend(start..end);
            current_offset += O::usize_as(end - start);
            new_offsets.push(current_offset);
            validity_builder.append(old_validity.map(|v| v.is_valid(i)).unwrap_or(true));
        }
    }

    let new_values = if values_to_keep.is_empty() {
        list_arr.values().slice(0, 0)
    } else {
        let indices =
            arrow_array::UInt64Array::from_iter_values(values_to_keep.iter().map(|&i| i as u64));
        arrow_select::take::take(list_arr.values().as_ref(), &indices, None)
            .expect("take should succeed")
    };

    let new_values = if needs_garbage_filtering(value_field.data_type()) && !new_values.is_empty() {
        let len = new_values.len();
        filter_fsl_child_garbage(new_values, &vec![false; len])
    } else {
        new_values
    };

    let new_validity = NullBuffer::new(validity_builder.finish());
    Arc::new(GenericListArray::new(
        value_field,
        OffsetBuffer::new(ScalarBuffer::from(new_offsets)),
        new_values,
        Some(new_validity),
    ))
}

fn filter_map_garbage(map_arr: &arrow_array::MapArray, is_garbage: &[bool]) -> ArrayRef {
    debug_assert_eq!(map_arr.len(), is_garbage.len());

    let old_offsets = map_arr.offsets();
    let entries_field = match map_arr.data_type() {
        DataType::Map(field, _) => field.clone(),
        _ => unreachable!(),
    };

    let mut new_offsets: Vec<i32> = Vec::with_capacity(map_arr.len() + 1);
    let mut values_to_keep: Vec<usize> = Vec::new();
    let mut validity_builder = BooleanBufferBuilder::new(map_arr.len());
    let mut current_offset: i32 = 0;
    new_offsets.push(current_offset);
    let old_validity = map_arr.nulls();

    for (i, &garbage) in is_garbage.iter().enumerate() {
        if garbage {
            new_offsets.push(current_offset);
            validity_builder.append(false);
        } else {
            let start = old_offsets[i] as usize;
            let end = old_offsets[i + 1] as usize;
            values_to_keep.extend(start..end);
            current_offset += (end - start) as i32;
            new_offsets.push(current_offset);
            validity_builder.append(old_validity.map(|v| v.is_valid(i)).unwrap_or(true));
        }
    }

    let new_entries: ArrayRef = if values_to_keep.is_empty() {
        Arc::new(map_arr.entries().slice(0, 0))
    } else {
        let indices =
            arrow_array::UInt64Array::from_iter_values(values_to_keep.iter().map(|&i| i as u64));
        arrow_select::take::take(map_arr.entries(), &indices, None).expect("take should succeed")
    };

    let new_entries =
        if needs_garbage_filtering(entries_field.data_type()) && !new_entries.is_empty() {
            let len = new_entries.len();
            filter_fsl_child_garbage(new_entries, &vec![false; len])
        } else {
            new_entries
        };

    let new_validity = NullBuffer::new(validity_builder.finish());
    let keys_sorted = matches!(map_arr.data_type(), DataType::Map(_, true));

    Arc::new(
        arrow_array::MapArray::try_new(
            entries_field,
            OffsetBuffer::new(ScalarBuffer::from(new_offsets)),
            new_entries.as_struct().clone(),
            Some(new_validity),
            keys_sorted,
        )
        .expect("MapArray construction should succeed"),
    )
}

/// Filters garbage from nested FSL arrays that contain list-like children.
fn filter_nested_fsl_garbage(
    fsl_arr: &arrow_array::FixedSizeListArray,
    is_garbage: &[bool],
    dimension: usize,
) -> ArrayRef {
    debug_assert_eq!(fsl_arr.len(), is_garbage.len());

    let child_field = match fsl_arr.data_type() {
        DataType::FixedSizeList(field, _) => field.clone(),
        _ => unreachable!(),
    };

    if !needs_garbage_filtering(child_field.data_type()) {
        return Arc::new(fsl_arr.clone());
    }

    let child_garbage = expand_garbage_mask(is_garbage, dimension);
    let new_values = filter_fsl_child_garbage(fsl_arr.values().clone(), &child_garbage);

    Arc::new(arrow_array::FixedSizeListArray::new(
        child_field,
        dimension as i32,
        new_values,
        fsl_arr.nulls().cloned(),
    ))
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{
        Array, FixedSizeListArray,
        builder::{Int32Builder, ListBuilder},
        cast::AsArray,
    };
    use arrow_schema::{DataType, Field, Fields};
    use rstest::rstest;

    use super::filter_nested_fsl_garbage;
    use crate::{
        constants::{
            STRUCTURAL_ENCODING_FULLZIP, STRUCTURAL_ENCODING_META_KEY,
            STRUCTURAL_ENCODING_MINIBLOCK,
        },
        testing::{TestCases, check_specific_random},
    };

    fn make_fsl_struct_type(struct_fields: Fields, dimension: i32) -> DataType {
        DataType::FixedSizeList(
            Arc::new(Field::new("item", DataType::Struct(struct_fields), true)),
            dimension,
        )
    }

    fn simple_struct_fields() -> Fields {
        Fields::from(vec![
            Field::new("x", DataType::Float64, false),
            Field::new("y", DataType::Float64, false),
        ])
    }

    fn nested_struct_fields() -> Fields {
        let inner = Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]);
        Fields::from(vec![
            Field::new("outer_val", DataType::Float64, false),
            Field::new("inner", DataType::Struct(inner), true),
        ])
    }

    fn nested_struct_with_list_fields() -> Fields {
        let inner = Fields::from(vec![Field::new(
            "values",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        )]);
        Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("inner", DataType::Struct(inner), true),
        ])
    }

    fn struct_with_list_fields() -> Fields {
        Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "values",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
        ])
    }

    fn struct_with_large_list_fields() -> Fields {
        Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "values",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Int64, true))),
                true,
            ),
        ])
    }

    fn struct_with_nested_fsl_fields() -> Fields {
        Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new(
                "vectors",
                DataType::FixedSizeList(Arc::new(Field::new("item", DataType::Float32, true)), 4),
                true,
            ),
        ])
    }

    fn struct_with_map_fields() -> Fields {
        let entries_field = Arc::new(Field::new(
            "entries",
            DataType::Struct(Fields::from(vec![
                Field::new("keys", DataType::Utf8, false),
                Field::new("values", DataType::Int32, true),
            ])),
            false,
        ));
        Fields::from(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("props", DataType::Map(entries_field, false), true),
        ])
    }

    fn make_fsl_of_list() -> DataType {
        DataType::FixedSizeList(
            Arc::new(Field::new(
                "item",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
            2,
        )
    }

    fn make_fsl_of_large_list() -> DataType {
        DataType::FixedSizeList(
            Arc::new(Field::new(
                "item",
                DataType::LargeList(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            )),
            2,
        )
    }

    fn make_fsl_of_map() -> DataType {
        DataType::FixedSizeList(
            Arc::new(Field::new(
                "item",
                DataType::Map(
                    Arc::new(Field::new(
                        "entries",
                        DataType::Struct(Fields::from(vec![
                            Field::new("key", DataType::Utf8, false),
                            Field::new("value", DataType::Int32, true),
                        ])),
                        false,
                    )),
                    false,
                ),
                true,
            )),
            2,
        )
    }

    fn make_fsl_of_nested_fsl_struct() -> DataType {
        DataType::FixedSizeList(
            Arc::new(Field::new(
                "item",
                DataType::FixedSizeList(
                    Arc::new(Field::new(
                        "item",
                        DataType::Struct(Fields::from(vec![Field::new(
                            "x",
                            DataType::Int32,
                            true,
                        )])),
                        true,
                    )),
                    4,
                ),
                true,
            )),
            2,
        )
    }

    #[rstest]
    #[case::simple(simple_struct_fields(), 2)]
    #[case::nested_struct(nested_struct_fields(), 2)]
    #[case::struct_with_list(struct_with_list_fields(), 2)]
    #[case::struct_with_large_list(struct_with_large_list_fields(), 2)]
    #[case::nested_struct_with_list(nested_struct_with_list_fields(), 2)]
    #[case::struct_with_nested_fsl(struct_with_nested_fsl_fields(), 2)]
    #[case::struct_with_map(struct_with_map_fields(), 2)]
    #[test_log::test(tokio::test)]
    async fn test_fsl_struct_random(
        #[case] struct_fields: Fields,
        #[case] dimension: i32,
        #[values(STRUCTURAL_ENCODING_MINIBLOCK, STRUCTURAL_ENCODING_FULLZIP)]
        structural_encoding: &str,
    ) {
        let data_type = make_fsl_struct_type(struct_fields, dimension);
        let mut field_metadata = HashMap::new();
        field_metadata.insert(
            STRUCTURAL_ENCODING_META_KEY.to_string(),
            structural_encoding.into(),
        );
        let field = Field::new("", data_type, true).with_metadata(field_metadata);
        let test_cases = TestCases::basic().with_u32_structural_encodings();
        check_specific_random(field, test_cases).await;
    }

    #[rstest]
    #[case::list(make_fsl_of_list())]
    #[case::large_list(make_fsl_of_large_list())]
    #[case::map(make_fsl_of_map())]
    #[case::nested_fsl_struct(make_fsl_of_nested_fsl_struct())]
    fn test_unsupported_fsl_child_types_return_error(#[case] data_type: DataType) {
        let arrow_field = Field::new("test", data_type, true);
        let err = lance_core::datatypes::Field::try_from(&arrow_field).unwrap_err();
        assert!(err.to_string().contains("Unsupported data type"));
    }

    #[test]
    fn test_filter_nested_fsl_garbage() {
        // Create FSL<List<Int32>> with dimension 2: [[[1], [2]], [[3], [4]], [[5], [6]]]
        let mut list_builder = ListBuilder::new(Int32Builder::new());
        for i in 1..=6 {
            list_builder.values().append_value(i);
            list_builder.append(true);
        }
        let list_arr = list_builder.finish();

        let fsl_field = Arc::new(Field::new(
            "item",
            DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
            true,
        ));
        let fsl = FixedSizeListArray::new(fsl_field, 2, Arc::new(list_arr), None);

        // Mark second FSL row as garbage
        let result = filter_nested_fsl_garbage(&fsl, &[false, true, false], 2);
        let result = result.as_fixed_size_list();

        // Child lists at positions 2,3 (garbage row 1) should be filtered to null
        let child_list = result.values().as_list::<i32>();
        assert_eq!(
            (0..6).map(|i| child_list.is_valid(i)).collect::<Vec<_>>(),
            vec![true, true, false, false, true, true]
        );
    }

    #[test]
    fn test_filter_nested_fsl_no_list_child() {
        // FSL<Int32> - no list child, should return unchanged
        let fsl_field = Arc::new(Field::new("item", DataType::Int32, true));
        let values = arrow_array::Int32Array::from(vec![1, 2, 3, 4, 5, 6]);
        let fsl = FixedSizeListArray::new(fsl_field, 2, Arc::new(values), None);

        let result = filter_nested_fsl_garbage(&fsl, &[false, true, false], 2);
        // Should return the same array unchanged
        assert_eq!(result.len(), 3);
    }
}
