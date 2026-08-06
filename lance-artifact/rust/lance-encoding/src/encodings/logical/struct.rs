// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    collections::{BinaryHeap, VecDeque},
    ops::Range,
    sync::Arc,
};

use super::{
    fixed_size_list::StructuralFixedSizeListDecoder, list::StructuralListDecoder,
    map::StructuralMapDecoder, primitive::StructuralPrimitiveFieldDecoder,
};
use crate::{
    decoder::{
        DecodedArray, FilterExpression, LoadedPageShard, NextDecodeTask, PageEncoding,
        ScheduledScanLine, SchedulerContext, StructuralDecodeArrayTask, StructuralFieldDecoder,
        StructuralFieldScheduler, StructuralSchedulingJob,
    },
    encoder::{EncodeTask, EncodedColumn, EncodedPage, FieldEncoder, OutOfLineBuffers},
    format::pb,
    repdef::{CompositeRepDefUnraveler, RepDefBuilder},
};
use arrow_array::{Array, ArrayRef, StructArray, cast::AsArray};
use arrow_schema::{DataType, Fields};
use futures::{
    FutureExt, StreamExt, TryStreamExt,
    future::BoxFuture,
    stream::{FuturesOrdered, FuturesUnordered},
};
use itertools::Itertools;
use lance_arrow::FieldExt;
use lance_arrow::{deepcopy::deep_copy_nulls, r#struct::StructArrayExt};
use lance_core::{Error, Result};
use log::trace;

#[derive(Debug)]
struct StructuralSchedulingJobWithStatus<'a> {
    col_idx: u32,
    col_name: &'a str,
    job: Box<dyn StructuralSchedulingJob + 'a>,
    rows_scheduled: u64,
    rows_remaining: u64,
    ready_scan_lines: VecDeque<ScheduledScanLine>,
}

impl PartialEq for StructuralSchedulingJobWithStatus<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.col_idx == other.col_idx
    }
}

impl Eq for StructuralSchedulingJobWithStatus<'_> {}

impl PartialOrd for StructuralSchedulingJobWithStatus<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StructuralSchedulingJobWithStatus<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note this is reversed to make it min-heap
        other.rows_scheduled.cmp(&self.rows_scheduled)
    }
}

/// Scheduling job for struct data
///
/// The order in which we schedule the children is important.  We want to schedule the child
/// with the least amount of data first.
///
/// This allows us to decode entire rows as quickly as possible
#[derive(Debug)]
struct RepDefStructSchedulingJob<'a> {
    /// A min-heap whose key is the # of rows currently scheduled
    children: BinaryHeap<StructuralSchedulingJobWithStatus<'a>>,
    rows_scheduled: u64,
    num_rows: u64,
}

impl<'a> RepDefStructSchedulingJob<'a> {
    fn new(
        scheduler: &'a StructuralStructScheduler,
        children: Vec<Box<dyn StructuralSchedulingJob + 'a>>,
        num_rows: u64,
    ) -> Self {
        let children = children
            .into_iter()
            .enumerate()
            .map(|(idx, job)| StructuralSchedulingJobWithStatus {
                col_idx: idx as u32,
                col_name: scheduler.child_fields[idx].name(),
                job,
                rows_scheduled: 0,
                rows_remaining: num_rows,
                ready_scan_lines: VecDeque::new(),
            })
            .collect::<BinaryHeap<_>>();
        Self {
            children,
            rows_scheduled: 0,
            num_rows,
        }
    }
}

impl StructuralSchedulingJob for RepDefStructSchedulingJob<'_> {
    fn schedule_next(
        &mut self,
        mut context: &mut SchedulerContext,
    ) -> Result<Vec<ScheduledScanLine>> {
        if self.children.is_empty() {
            // Special path for empty structs
            if self.rows_scheduled == self.num_rows {
                return Ok(Vec::new());
            }
            self.rows_scheduled = self.num_rows;
            return Ok(vec![ScheduledScanLine {
                decoders: Vec::new(),
                rows_scheduled: self.num_rows,
            }]);
        }

        let mut decoders = Vec::new();
        let old_rows_scheduled = self.rows_scheduled;
        // Schedule as many children as we need to until we have scheduled at least one
        // complete row
        while old_rows_scheduled == self.rows_scheduled {
            if self.children.is_empty() {
                // Early exit when schedulers are exhausted prematurely (TODO: does this still happen?)
                return Ok(Vec::new());
            }
            let mut next_child = self.children.pop().unwrap();
            if next_child.ready_scan_lines.is_empty() {
                let scoped = context.push(next_child.col_name, next_child.col_idx);
                let child_scans = next_child.job.schedule_next(scoped.context)?;
                context = scoped.pop();
                if child_scans.is_empty() {
                    // Continue without pushing next_child back onto the heap (it is done)
                    continue;
                }
                next_child.ready_scan_lines.extend(child_scans);
            }
            let child_scan = next_child.ready_scan_lines.pop_front().unwrap();
            trace!(
                "Scheduled {} rows for child {}",
                child_scan.rows_scheduled, next_child.col_idx
            );
            next_child.rows_scheduled += child_scan.rows_scheduled;
            next_child.rows_remaining -= child_scan.rows_scheduled;
            decoders.extend(child_scan.decoders);
            self.children.push(next_child);
            self.rows_scheduled = self.children.peek().unwrap().rows_scheduled;
        }
        let struct_rows_scheduled = self.rows_scheduled - old_rows_scheduled;
        Ok(vec![ScheduledScanLine {
            decoders,
            rows_scheduled: struct_rows_scheduled,
        }])
    }
}

/// A scheduler for structs
///
/// The implementation is actually a bit more tricky than one might initially think.  We can't just
/// go through and schedule each column one after the other.  This would mean our decode can't start
/// until nearly all the data has arrived (since we need data from each column to yield a batch)
///
/// Instead, we schedule in row-major fashion
///
/// Note: this scheduler is the starting point for all decoding.  This is because we treat the top-level
/// record batch as a non-nullable struct.
#[derive(Debug)]
pub struct StructuralStructScheduler {
    children: Vec<Box<dyn StructuralFieldScheduler>>,
    child_fields: Fields,
}

impl StructuralStructScheduler {
    pub fn new(children: Vec<Box<dyn StructuralFieldScheduler>>, child_fields: Fields) -> Self {
        Self {
            children,
            child_fields,
        }
    }
}

impl StructuralFieldScheduler for StructuralStructScheduler {
    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Box<dyn StructuralSchedulingJob + 'a>> {
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum();

        let child_schedulers = self
            .children
            .iter()
            .map(|child| child.schedule_ranges(ranges, filter))
            .collect::<Result<Vec<_>>>()?;

        Ok(Box::new(RepDefStructSchedulingJob::new(
            self,
            child_schedulers,
            num_rows,
        )))
    }

    fn initialize<'a>(
        &'a mut self,
        filter: &'a FilterExpression,
        context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>> {
        let children_initialization = self
            .children
            .iter_mut()
            .map(|child| child.initialize(filter, context))
            .collect::<FuturesUnordered<_>>();
        async move {
            children_initialization
                .map(|res| res.map(|_| ()))
                .try_collect::<Vec<_>>()
                .await?;
            Ok(())
        }
        .boxed()
    }
}

#[derive(Debug)]
pub struct StructuralStructDecoder {
    children: Vec<Box<dyn StructuralFieldDecoder>>,
    data_type: DataType,
    child_fields: Fields,
    // The root decoder is slightly different because it cannot have nulls
    is_root: bool,
}

impl StructuralStructDecoder {
    pub fn new(fields: Fields, should_validate: bool, is_root: bool) -> Result<Self> {
        let children = fields
            .iter()
            .map(|field| Self::field_to_decoder(field, should_validate))
            .collect::<Result<Vec<_>>>()?;
        let data_type = DataType::Struct(fields.clone());
        Ok(Self {
            data_type,
            children,
            child_fields: fields,
            is_root,
        })
    }

    fn field_to_decoder(
        field: &Arc<arrow_schema::Field>,
        should_validate: bool,
    ) -> Result<Box<dyn StructuralFieldDecoder>> {
        match field.data_type() {
            DataType::Struct(fields) => {
                if field.is_packed_struct() || field.is_blob() {
                    let decoder =
                        StructuralPrimitiveFieldDecoder::new(&field.clone(), should_validate);
                    Ok(Box::new(decoder))
                } else {
                    Ok(Box::new(Self::new(fields.clone(), should_validate, false)?))
                }
            }
            DataType::List(child_field) | DataType::LargeList(child_field) => {
                let child_decoder = Self::field_to_decoder(child_field, should_validate)?;
                Ok(Box::new(StructuralListDecoder::new(
                    child_decoder,
                    field.data_type().clone(),
                )))
            }
            DataType::FixedSizeList(child_field, _)
                if matches!(child_field.data_type(), DataType::Struct(_)) =>
            {
                // FixedSizeList containing Struct needs structural decoding
                let child_decoder = Self::field_to_decoder(child_field, should_validate)?;
                Ok(Box::new(StructuralFixedSizeListDecoder::new(
                    child_decoder,
                    field.data_type().clone(),
                )))
            }
            DataType::Map(entries_field, keys_sorted) => {
                if *keys_sorted {
                    return Err(Error::not_supported_source(
                        "Map data type with keys_sorted=true is not supported yet"
                            .to_string()
                            .into(),
                    ));
                }
                let child_decoder = Self::field_to_decoder(entries_field, should_validate)?;
                Ok(Box::new(StructuralMapDecoder::new(
                    child_decoder,
                    field.data_type().clone(),
                )))
            }
            DataType::RunEndEncoded(_, _) => todo!(),
            DataType::ListView(_) | DataType::LargeListView(_) => todo!(),
            DataType::Union(_, _) => todo!(),
            _ => Ok(Box::new(StructuralPrimitiveFieldDecoder::new(
                field,
                should_validate,
            ))),
        }
    }

    pub fn drain_batch_task(&mut self, num_rows: u64) -> Result<NextDecodeTask> {
        let array_drain = self.drain(num_rows)?;
        Ok(NextDecodeTask {
            num_rows,
            task: Box::new(array_drain),
        })
    }
}

impl StructuralFieldDecoder for StructuralStructDecoder {
    fn accept_page(&mut self, mut child: LoadedPageShard) -> Result<()> {
        // children with empty path should not be delivered to this method
        let child_idx = child.path.pop_front().unwrap();
        // This decoder is intended for one of our children
        self.children[child_idx as usize].accept_page(child)?;
        Ok(())
    }

    fn drain(&mut self, num_rows: u64) -> Result<Box<dyn StructuralDecodeArrayTask>> {
        let child_tasks = self
            .children
            .iter_mut()
            .map(|child| child.drain(num_rows))
            .collect::<Result<Vec<_>>>()?;
        Ok(Box::new(RepDefStructDecodeTask {
            children: child_tasks,
            child_fields: self.child_fields.clone(),
            is_root: self.is_root,
            num_rows,
        }))
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[derive(Debug)]
struct RepDefStructDecodeTask {
    children: Vec<Box<dyn StructuralDecodeArrayTask>>,
    child_fields: Fields,
    is_root: bool,
    num_rows: u64,
}

impl StructuralDecodeArrayTask for RepDefStructDecodeTask {
    fn decode(self: Box<Self>) -> Result<DecodedArray> {
        if self.children.is_empty() {
            return Ok(DecodedArray {
                array: Arc::new(StructArray::new_empty_fields(self.num_rows as usize, None)),
                repdef: CompositeRepDefUnraveler::new(vec![]),
                data_size: 0,
            });
        }

        let arrays = self
            .children
            .into_iter()
            .map(|task| task.decode())
            .collect::<Result<Vec<_>>>()?;
        let mut children = Vec::with_capacity(arrays.len());
        let mut repdefs = Vec::with_capacity(arrays.len());
        let mut data_size = 0u64;
        let mut arrays_iter = arrays.into_iter();
        let first_array = arrays_iter.next().ok_or_else(|| {
            Error::internal("Struct decoder unexpectedly has no child arrays".to_string())
        })?;
        let length = first_array.array.len();

        // The repdef should be identical across all children at this point
        repdefs.push(first_array.repdef);
        data_size += first_array.data_size;
        children.push(first_array.array);

        for array in arrays_iter {
            if length != array.array.len() {
                return Err(Error::invalid_input_source(
                    format!(
                        "Struct child array length {} does not match sibling length {}",
                        array.array.len(),
                        length
                    )
                    .into(),
                ));
            }
            data_size += array.data_size;
            children.push(array.array);
            repdefs.push(array.repdef);
        }

        // Dense rep/def state can retain child-specific repetition information after a child
        // decoder finishes, so comparing dense siblings is not meaningful. If any child carries
        // sparse state, keep a sparse child as the canonical structural plan and compare it with
        // every other sparse sibling so sparse metadata is never silently discarded.
        let primary_repdef = repdefs
            .iter()
            .position(CompositeRepDefUnraveler::has_sparse)
            .unwrap_or(0);
        let mut repdef = repdefs.swap_remove(primary_repdef);
        if repdef.has_sparse() {
            for sibling in repdefs {
                if sibling.has_sparse() {
                    repdef.add_compatibility_check(sibling);
                }
            }
        }

        let validity = if self.is_root {
            repdef.ensure_exhausted()?;
            None
        } else {
            repdef.unravel_validity(length)?
        };

        let array = StructArray::try_new(self.child_fields, children, validity)
            .map_err(|e| Error::invalid_input_source(e.to_string().into()))?;
        Ok(DecodedArray {
            array: Arc::new(array),
            repdef,
            data_size,
        })
    }
}

/// A structural encoder for struct fields
///
/// The struct's validity is added to the rep/def builder
/// and the builder is cloned to all children.
pub struct StructStructuralEncoder {
    keep_original_array: bool,
    children: Vec<Box<dyn FieldEncoder>>,
}

impl StructStructuralEncoder {
    pub fn new(keep_original_array: bool, children: Vec<Box<dyn FieldEncoder>>) -> Self {
        Self {
            keep_original_array,
            children,
        }
    }
}

impl FieldEncoder for StructStructuralEncoder {
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        external_buffers: &mut OutOfLineBuffers,
        mut repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        let struct_array = array.as_struct();
        let mut struct_array = struct_array.normalize_slicing()?;
        if let Some(validity) = struct_array.nulls() {
            if self.keep_original_array {
                repdef.add_validity_bitmap(validity.clone())
            } else {
                repdef.add_validity_bitmap(deep_copy_nulls(Some(validity)).unwrap())
            }
            struct_array = struct_array.pushdown_nulls()?;
        } else {
            repdef.add_no_null(struct_array.len());
        }
        let child_tasks = self
            .children
            .iter_mut()
            .zip(struct_array.columns().iter())
            .map(|(encoder, arr)| {
                encoder.maybe_encode(
                    arr.clone(),
                    external_buffers,
                    repdef.clone(),
                    row_number,
                    num_rows,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(child_tasks.into_iter().flatten().collect::<Vec<_>>())
    }

    fn flush(&mut self, external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>> {
        self.children
            .iter_mut()
            .map(|encoder| encoder.flush(external_buffers))
            .flatten_ok()
            .collect::<Result<Vec<_>>>()
    }

    fn num_columns(&self) -> u32 {
        self.children
            .iter()
            .map(|child| child.num_columns())
            .sum::<u32>()
    }

    fn finish(
        &mut self,
        external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<crate::encoder::EncodedColumn>>> {
        let mut child_columns = self
            .children
            .iter_mut()
            .map(|child| child.finish(external_buffers))
            .collect::<FuturesOrdered<_>>();
        async move {
            let mut encoded_columns = Vec::with_capacity(child_columns.len());
            while let Some(child_cols) = child_columns.next().await {
                encoded_columns.extend(child_cols?);
            }
            Ok(encoded_columns)
        }
        .boxed()
    }
}

pub struct StructFieldEncoder {
    children: Vec<Box<dyn FieldEncoder>>,
    column_index: u32,
    num_rows_seen: u64,
}

impl StructFieldEncoder {
    pub fn new(children: Vec<Box<dyn FieldEncoder>>, column_index: u32) -> Self {
        Self {
            children,
            column_index,
            num_rows_seen: 0,
        }
    }
}

impl FieldEncoder for StructFieldEncoder {
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        external_buffers: &mut OutOfLineBuffers,
        repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        self.num_rows_seen += array.len() as u64;
        let struct_array = array.as_struct();
        let child_tasks = self
            .children
            .iter_mut()
            .zip(struct_array.columns().iter())
            .map(|(encoder, arr)| {
                encoder.maybe_encode(
                    arr.clone(),
                    external_buffers,
                    repdef.clone(),
                    row_number,
                    num_rows,
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(child_tasks.into_iter().flatten().collect::<Vec<_>>())
    }

    fn flush(&mut self, external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>> {
        let child_tasks = self
            .children
            .iter_mut()
            .map(|encoder| encoder.flush(external_buffers))
            .collect::<Result<Vec<_>>>()?;
        Ok(child_tasks.into_iter().flatten().collect::<Vec<_>>())
    }

    fn num_columns(&self) -> u32 {
        self.children
            .iter()
            .map(|child| child.num_columns())
            .sum::<u32>()
            + 1
    }

    fn finish(
        &mut self,
        external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<crate::encoder::EncodedColumn>>> {
        let mut child_columns = self
            .children
            .iter_mut()
            .map(|child| child.finish(external_buffers))
            .collect::<FuturesOrdered<_>>();
        let num_rows_seen = self.num_rows_seen;
        let column_index = self.column_index;
        async move {
            let mut columns = Vec::new();
            // Add a column for the struct header
            let mut header = EncodedColumn::default();
            header.final_pages.push(EncodedPage {
                data: Vec::new(),
                description: PageEncoding::Legacy(pb::ArrayEncoding {
                    array_encoding: Some(pb::array_encoding::ArrayEncoding::Struct(
                        pb::SimpleStruct {},
                    )),
                }),
                num_rows: num_rows_seen,
                column_idx: column_index,
                row_number: 0, // Not used by legacy encoding
            });
            columns.push(header);
            // Now run finish on the children
            while let Some(child_cols) = child_columns.next().await {
                columns.extend(child_cols?);
            }
            Ok(columns)
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {

    use std::{collections::HashMap, sync::Arc};

    use arrow_array::{
        Array, ArrayRef, Float64Array, Int32Array, Int64Array, ListArray, StructArray,
        builder::{Int32Builder, ListBuilder},
    };
    use arrow_buffer::{BooleanBuffer, NullBuffer, OffsetBuffer, ScalarBuffer};
    use arrow_schema::{DataType, Field, Fields};

    use crate::testing::{TestCases, check_basic_random, check_round_trip_encoding_of_data};

    #[test_log::test(tokio::test)]
    async fn test_simple_struct() {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("a", DataType::Int32, false),
            Field::new("b", DataType::Int32, false),
        ]));
        let field = Field::new("", data_type, false);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_nullable_struct() {
        // Test data struct<score: int32, location: struct<x: int32, y: int32>>
        // - score: null
        //   location:
        //     x: 1
        //     y: 6
        // - score: 12
        //   location:
        //     x: 2
        //     y: null
        // - score: 13
        //   location:
        //     x: 3
        //     y: 8
        // - score: 14
        //   location: null
        // - null
        //
        let inner_fields = Fields::from(vec![
            Field::new("x", DataType::Int32, false),
            Field::new("y", DataType::Int32, true),
        ]);
        let inner_struct = DataType::Struct(inner_fields.clone());
        let outer_fields = Fields::from(vec![
            Field::new("score", DataType::Int32, true),
            Field::new("location", inner_struct, true),
        ]);

        let x_vals = Int32Array::from(vec![Some(1), Some(2), Some(3), Some(4), Some(5)]);
        let y_vals = Int32Array::from(vec![Some(6), None, Some(8), Some(9), Some(10)]);
        let scores = Int32Array::from(vec![None, Some(12), Some(13), Some(14), Some(15)]);

        let location_validity = NullBuffer::from(vec![true, true, true, false, true]);
        let locations = StructArray::new(
            inner_fields,
            vec![Arc::new(x_vals), Arc::new(y_vals)],
            Some(location_validity),
        );

        let rows_validity = NullBuffer::from(vec![true, true, true, true, false]);
        let rows = StructArray::new(
            outer_fields,
            vec![Arc::new(scores), Arc::new(locations)],
            Some(rows_validity),
        );

        let test_cases = TestCases::default().with_structural_encodings();

        check_round_trip_encoding_of_data(vec![Arc::new(rows)], &test_cases, HashMap::new()).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_masked_nonempty_list() {
        // [[1, 2], [NULL], [4], [], NULL, NULL-STRUCT]
        //
        let items = Int32Array::from(vec![Some(1), Some(2), None, Some(4), Some(5), Some(6)]);
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3, 4, 4, 4, 5]));
        let list_validity = BooleanBuffer::from(vec![true, true, true, true, false, true]);
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            Arc::new(items),
            Some(NullBuffer::new(list_validity)),
        );
        let struct_validity = BooleanBuffer::from(vec![true, true, true, true, true, false]);
        let struct_array = StructArray::new(
            Fields::from(vec![Field::new(
                "inner_list",
                list_array.data_type().clone(),
                true,
            )]),
            vec![Arc::new(list_array)],
            Some(NullBuffer::new(struct_validity)),
        );
        check_round_trip_encoding_of_data(
            vec![Arc::new(struct_array)],
            &TestCases::default().with_structural_encodings(),
            HashMap::new(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_simple_struct_list() {
        // [[1, 2], [NULL], [4], [], NULL, NULL-STRUCT]
        //
        let items = Int32Array::from(vec![Some(1), Some(2), None, Some(4)]);
        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 3, 4, 4, 4, 4]));
        let list_validity = BooleanBuffer::from(vec![true, true, true, true, false, true]);
        let list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Int32, true)),
            offsets,
            Arc::new(items),
            Some(NullBuffer::new(list_validity)),
        );
        let struct_validity = BooleanBuffer::from(vec![true, true, true, true, true, false]);
        let struct_array = StructArray::new(
            Fields::from(vec![Field::new(
                "inner_list",
                list_array.data_type().clone(),
                true,
            )]),
            vec![Arc::new(list_array)],
            Some(NullBuffer::new(struct_validity)),
        );
        check_round_trip_encoding_of_data(
            vec![Arc::new(struct_array)],
            &TestCases::default().with_structural_encodings(),
            HashMap::new(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_struct_list() {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new(
                "inner_list",
                DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                true,
            ),
            Field::new("outer_int", DataType::Int32, true),
        ]));
        let field = Field::new("row", data_type, false);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_empty_struct() {
        // It's technically legal for a struct to have 0 children, need to
        // make sure we support that
        let data_type = DataType::Struct(Fields::from(Vec::<Field>::default()));
        let field = Field::new("row", data_type, false);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_complicated_struct() {
        let data_type = DataType::Struct(Fields::from(vec![
            Field::new("int", DataType::Int32, true),
            Field::new(
                "inner",
                DataType::Struct(Fields::from(vec![
                    Field::new("inner_int", DataType::Int32, true),
                    Field::new(
                        "inner_list",
                        DataType::List(Arc::new(Field::new("item", DataType::Int32, true))),
                        true,
                    ),
                ])),
                true,
            ),
            Field::new("outer_binary", DataType::Binary, true),
        ]));
        let field = Field::new("row", data_type, false);
        check_basic_random(field).await;
    }

    #[test_log::test(tokio::test)]
    async fn test_list_of_struct_with_null_struct_element() {
        // Regression: a list containing structs where most struct elements are null
        // causes a length mismatch during decoding with V2_2 encoding.
        use arrow_array::StringArray;

        let tag_array = StringArray::from(vec![
            Some("valid"),
            Some("null_struct"),
            Some("valid"),
            Some("valid"),
        ]);
        let struct_fields = Fields::from(vec![Field::new("tag", DataType::Utf8, true)]);
        // 3 out of 4 struct elements are null
        let struct_validity = NullBuffer::from(vec![false, true, false, false]);
        let struct_array = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(tag_array)],
            Some(struct_validity),
        );

        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 4]));
        let list_field = Field::new("item", DataType::Struct(struct_fields), true);
        let list_array =
            ListArray::new(Arc::new(list_field), offsets, Arc::new(struct_array), None);

        check_round_trip_encoding_of_data(
            vec![Arc::new(list_array)],
            &TestCases::default().with_u32_structural_encodings(),
            HashMap::new(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_list_of_struct_with_all_null_child() {
        use arrow_array::StringArray;

        let a_array = StringArray::from(vec![Some("w"), Some("x"), Some("y"), Some("z")]);
        let b_array = StringArray::from(vec![None::<&str>, None, None, None]);
        let struct_fields = Fields::from(vec![
            Field::new("a", DataType::Utf8, true),
            Field::new("b", DataType::Utf8, true),
        ]);
        let struct_array = StructArray::new(
            struct_fields.clone(),
            vec![Arc::new(a_array), Arc::new(b_array)],
            None,
        );

        let offsets = OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 2, 4]));
        let list_field = Field::new("item", DataType::Struct(struct_fields), true);
        let list_array =
            ListArray::new(Arc::new(list_field), offsets, Arc::new(struct_array), None);

        check_round_trip_encoding_of_data(
            vec![Arc::new(list_array)],
            &TestCases::default()
                .with_range(0..1)
                .with_range(1..2)
                .with_indices(vec![0])
                .with_indices(vec![1])
                .with_structural_encodings(),
            HashMap::new(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_list_of_struct_with_constant_child_and_empty_lists() {
        let item_fields = Fields::from(vec![
            Field::new("maneuver", DataType::Int64, true),
            Field::new("remaining_dist", DataType::Float64, true),
        ]);
        let item_array = StructArray::new(
            item_fields.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(3), Some(3), Some(3)])),
                Arc::new(Float64Array::from(vec![Some(1.0), Some(2.0), Some(3.0)])),
            ],
            None,
        );

        let list_field = Field::new("item", DataType::Struct(item_fields), true);
        let list_array = ListArray::new(
            Arc::new(list_field),
            OffsetBuffer::new(ScalarBuffer::<i32>::from(vec![0, 1, 2, 3, 3, 3, 3])),
            Arc::new(item_array),
            None,
        );

        let data_fields = Fields::from(vec![Field::new(
            "maneuvers",
            list_array.data_type().clone(),
            true,
        )]);
        let data_array = StructArray::new(data_fields.clone(), vec![Arc::new(list_array)], None);
        let row_validity = NullBuffer::from(vec![true, true, true, true, true, false]);
        let row_array = StructArray::new(
            Fields::from(vec![Field::new(
                "data",
                DataType::Struct(data_fields),
                true,
            )]),
            vec![Arc::new(data_array)],
            Some(row_validity),
        );

        check_round_trip_encoding_of_data(
            vec![Arc::new(row_array)],
            &TestCases::default().with_structural_encodings(),
            HashMap::new(),
        )
        .await;
    }

    #[test_log::test(tokio::test)]
    async fn test_ragged_scheduling() {
        // This test covers scheduling when batches straddle page boundaries

        // Create a list with 10k nulls
        let items_builder = Int32Builder::new();
        let mut list_builder = ListBuilder::new(items_builder);
        for _ in 0..10000 {
            list_builder.append_null();
        }
        let list_array = Arc::new(list_builder.finish());
        let int_array = Arc::new(Int32Array::from_iter_values(0..10000));
        let fields = vec![
            Field::new("", list_array.data_type().clone(), true),
            Field::new("", int_array.data_type().clone(), true),
        ];
        let struct_array = Arc::new(StructArray::new(
            Fields::from(fields),
            vec![list_array, int_array],
            None,
        )) as ArrayRef;
        let struct_arrays = (0..10000)
            // Intentionally skip in some randomish amount to create more ragged scheduling
            .step_by(437)
            .map(|offset| struct_array.slice(offset, 437.min(10000 - offset)))
            .collect::<Vec<_>>();
        check_round_trip_encoding_of_data(struct_arrays, &TestCases::default(), HashMap::new())
            .await;
    }
}
