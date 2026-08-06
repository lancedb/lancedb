// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{fmt::Debug, ops::Range, sync::Arc, vec};

use arrow_array::{Array, ArrayRef, cast::AsArray, make_array};
use arrow_buffer::bit_util;
use arrow_schema::DataType;
use futures::{FutureExt, future::BoxFuture};
use log::trace;

use crate::decoder::{ColumnBuffers, PageBuffers};
use crate::decoder::{FieldScheduler, LogicalPageDecoder, SchedulingJob};
use crate::encoder::ArrayEncodingStrategy;
use crate::utils::accumulation::AccumulationQueue;
use crate::{array_encoding::physical::decoder_from_array_encoding, data::DataBlock};
use lance_core::{Error, Result, datatypes::Field};

use crate::{
    decoder::{
        DecodeArrayTask, FilterExpression, MessageType, NextDecodeTask, PageEncoding, PageInfo,
        PageScheduler, PrimitivePageDecoder, PriorityRange, ScheduledScanLine, SchedulerContext,
    },
    encoder::{
        EncodeTask, EncodedColumn, EncodedPage, EncodingOptions, FieldEncoder, OutOfLineBuffers,
    },
    repdef::RepDefBuilder,
};

#[derive(Debug)]
struct PrimitivePage {
    scheduler: Box<dyn PageScheduler>,
    num_rows: u64,
    page_index: u32,
}

/// A field scheduler for primitive fields
///
/// This maps to exactly one column and it assumes that the top-level
/// encoding of each page is "basic".  The basic encoding decodes into an
/// optional buffer of validity and a fixed-width buffer of values
/// which is exactly what we need to create a primitive array.
///
/// Note: we consider booleans and fixed-size-lists of primitive types to be
/// primitive types.  This is slightly different than arrow-rs's definition
#[derive(Debug)]
pub struct PrimitiveFieldScheduler {
    data_type: DataType,
    page_schedulers: Vec<PrimitivePage>,
    num_rows: u64,
    should_validate: bool,
    column_index: u32,
}

impl PrimitiveFieldScheduler {
    pub fn new(
        column_index: u32,
        data_type: DataType,
        pages: Arc<[PageInfo]>,
        buffers: ColumnBuffers,
        should_validate: bool,
    ) -> Self {
        let page_schedulers = pages
            .iter()
            .enumerate()
            // Buggy versions of Lance could sometimes create empty pages
            .filter(|(page_index, page)| {
                log::trace!("Skipping empty page with index {}", page_index);
                page.num_rows > 0
            })
            .map(|(page_index, page)| {
                let page_buffers = PageBuffers {
                    column_buffers: buffers,
                    positions_and_sizes: &page.buffer_offsets_and_sizes,
                };
                let scheduler = decoder_from_array_encoding(
                    page.encoding.as_legacy(),
                    &page_buffers,
                    &data_type,
                );
                PrimitivePage {
                    scheduler,
                    num_rows: page.num_rows,
                    page_index: page_index as u32,
                }
            })
            .collect::<Vec<_>>();
        let num_rows = page_schedulers.iter().map(|p| p.num_rows).sum();
        Self {
            data_type,
            page_schedulers,
            num_rows,
            should_validate,
            column_index,
        }
    }
}

#[derive(Debug)]
struct PrimitiveFieldSchedulingJob<'a> {
    scheduler: &'a PrimitiveFieldScheduler,
    ranges: Vec<Range<u64>>,
    page_idx: usize,
    range_idx: usize,
    range_offset: u64,
    global_row_offset: u64,
}

impl<'a> PrimitiveFieldSchedulingJob<'a> {
    pub fn new(scheduler: &'a PrimitiveFieldScheduler, ranges: Vec<Range<u64>>) -> Self {
        Self {
            scheduler,
            ranges,
            page_idx: 0,
            range_idx: 0,
            range_offset: 0,
            global_row_offset: 0,
        }
    }
}

impl SchedulingJob for PrimitiveFieldSchedulingJob<'_> {
    fn schedule_next(
        &mut self,
        context: &mut SchedulerContext,
        priority: &dyn PriorityRange,
    ) -> Result<ScheduledScanLine> {
        debug_assert!(self.range_idx < self.ranges.len());
        // Get our current range
        let mut range = self.ranges[self.range_idx].clone();
        range.start += self.range_offset;

        let mut cur_page = &self.scheduler.page_schedulers[self.page_idx];
        trace!(
            "Current range is {:?} and current page has {} rows",
            range, cur_page.num_rows
        );
        // Skip entire pages until we have some overlap with our next range
        while cur_page.num_rows + self.global_row_offset <= range.start {
            self.global_row_offset += cur_page.num_rows;
            self.page_idx += 1;
            trace!("Skipping entire page of {} rows", cur_page.num_rows);
            cur_page = &self.scheduler.page_schedulers[self.page_idx];
        }

        // Now the cur_page has overlap with range.  Continue looping through ranges
        // until we find a range that exceeds the current page

        let mut ranges_in_page = Vec::new();
        while cur_page.num_rows + self.global_row_offset > range.start {
            range.start = range.start.max(self.global_row_offset);
            let start_in_page = range.start - self.global_row_offset;
            let end_in_page = start_in_page + (range.end - range.start);
            let end_in_page = end_in_page.min(cur_page.num_rows);
            let last_in_range = (end_in_page + self.global_row_offset) >= range.end;

            ranges_in_page.push(start_in_page..end_in_page);
            if last_in_range {
                self.range_idx += 1;
                if self.range_idx == self.ranges.len() {
                    break;
                }
                range = self.ranges[self.range_idx].clone();
            } else {
                break;
            }
        }

        let num_rows_in_next = ranges_in_page.iter().map(|r| r.end - r.start).sum();
        trace!(
            "Scheduling {} rows across {} ranges from page with {} rows (priority={}, column_index={}, page_index={})",
            num_rows_in_next,
            ranges_in_page.len(),
            cur_page.num_rows,
            priority.current_priority(),
            self.scheduler.column_index,
            cur_page.page_index,
        );

        self.global_row_offset += cur_page.num_rows;
        self.page_idx += 1;

        let physical_decoder = cur_page.scheduler.schedule_ranges(
            &ranges_in_page,
            context.io(),
            priority.current_priority(),
        );

        let logical_decoder = PrimitiveFieldDecoder {
            data_type: self.scheduler.data_type.clone(),
            column_index: self.scheduler.column_index,
            unloaded_physical_decoder: Some(physical_decoder),
            physical_decoder: None,
            rows_drained: 0,
            num_rows: num_rows_in_next,
            should_validate: self.scheduler.should_validate,
            page_index: cur_page.page_index,
        };

        let decoder = Box::new(logical_decoder);
        #[allow(deprecated)]
        let decoder_ready = context.locate_decoder(decoder);
        Ok(ScheduledScanLine {
            decoders: vec![MessageType::DecoderReady(decoder_ready)],
            rows_scheduled: num_rows_in_next,
        })
    }

    fn num_rows(&self) -> u64 {
        self.ranges.iter().map(|r| r.end - r.start).sum()
    }
}

impl FieldScheduler for PrimitiveFieldScheduler {
    fn num_rows(&self) -> u64 {
        self.num_rows
    }

    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[std::ops::Range<u64>],
        // TODO: Could potentially use filter to simplify decode, something of a micro-optimization probably
        _filter: &FilterExpression,
    ) -> Result<Box<dyn SchedulingJob + 'a>> {
        Ok(Box::new(PrimitiveFieldSchedulingJob::new(
            self,
            ranges.to_vec(),
        )))
    }

    fn initialize<'a>(
        &'a self,
        _filter: &'a FilterExpression,
        _context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>> {
        // 2.0 schedulers do not need to initialize
        std::future::ready(Ok(())).boxed()
    }
}

pub struct PrimitiveFieldDecoder {
    data_type: DataType,
    unloaded_physical_decoder: Option<BoxFuture<'static, Result<Box<dyn PrimitivePageDecoder>>>>,
    physical_decoder: Option<Arc<dyn PrimitivePageDecoder>>,
    should_validate: bool,
    num_rows: u64,
    rows_drained: u64,
    column_index: u32,
    page_index: u32,
}

impl PrimitiveFieldDecoder {
    pub fn new_from_data(
        physical_decoder: Arc<dyn PrimitivePageDecoder>,
        data_type: DataType,
        num_rows: u64,
        should_validate: bool,
    ) -> Self {
        Self {
            data_type,
            unloaded_physical_decoder: None,
            physical_decoder: Some(physical_decoder),
            should_validate,
            num_rows,
            rows_drained: 0,
            column_index: u32::MAX,
            page_index: u32::MAX,
        }
    }
}

impl Debug for PrimitiveFieldDecoder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PrimitiveFieldDecoder")
            .field("data_type", &self.data_type)
            .field("num_rows", &self.num_rows)
            .field("rows_drained", &self.rows_drained)
            .finish()
    }
}

struct PrimitiveFieldDecodeTask {
    rows_to_skip: u64,
    rows_to_take: u64,
    should_validate: bool,
    physical_decoder: Arc<dyn PrimitivePageDecoder>,
    data_type: DataType,
}

impl DecodeArrayTask for PrimitiveFieldDecodeTask {
    fn decode(self: Box<Self>) -> Result<(ArrayRef, u64)> {
        let block = self
            .physical_decoder
            .decode(self.rows_to_skip, self.rows_to_take)?;

        let array = make_array(block.into_arrow(self.data_type.clone(), self.should_validate)?);

        // This is a bit of a hack to work around https://github.com/apache/arrow-rs/issues/6302
        //
        // We change from nulls-in-dictionary (storage format) to nulls-in-indices (arrow-rs preferred
        // format)
        //
        // The calculation of logical_nulls is not free and would be good to avoid in the future
        if let DataType::Dictionary(_, _) = self.data_type {
            let dict = array.as_any_dictionary();
            if let Some(nulls) = array.logical_nulls() {
                let new_indices = dict.keys().to_data();
                let new_array = make_array(
                    new_indices
                        .into_builder()
                        .nulls(Some(nulls))
                        .add_child_data(dict.values().to_data())
                        .data_type(dict.data_type().clone())
                        .build()?,
                );
                return Ok((new_array, 0));
            }
        }
        // data_size is only tracked in the v2.1 structural decode path; the v2.0 array
        // v2.0 path does not need it so we return 0.
        Ok((array, 0))
    }
}

impl LogicalPageDecoder for PrimitiveFieldDecoder {
    // TODO: In the future, at some point, we may consider partially waiting for primitive pages by
    // breaking up large I/O into smaller I/O as a way to accelerate the "time-to-first-decode"
    fn wait_for_loaded(&mut self, loaded_need: u64) -> BoxFuture<'_, Result<()>> {
        log::trace!(
            "primitive wait for more than {} rows on column {} and page {} (page has {} rows)",
            loaded_need,
            self.column_index,
            self.page_index,
            self.num_rows
        );
        async move {
            let physical_decoder = self.unloaded_physical_decoder.take().unwrap().await?;
            self.physical_decoder = Some(Arc::from(physical_decoder));
            Ok(())
        }
        .boxed()
    }

    fn drain(&mut self, num_rows: u64) -> Result<NextDecodeTask> {
        if self.physical_decoder.as_ref().is_none() {
            return Err(lance_core::Error::internal(format!(
                "drain was called on primitive field decoder for data type {} on column {} but the decoder was never awaited",
                self.data_type, self.column_index
            )));
        }

        let rows_to_skip = self.rows_drained;
        let rows_to_take = num_rows;

        self.rows_drained += rows_to_take;

        let task = Box::new(PrimitiveFieldDecodeTask {
            rows_to_skip,
            rows_to_take,
            should_validate: self.should_validate,
            physical_decoder: self.physical_decoder.as_ref().unwrap().clone(),
            data_type: self.data_type.clone(),
        });

        Ok(NextDecodeTask {
            task,
            num_rows: rows_to_take,
        })
    }

    fn rows_loaded(&self) -> u64 {
        if self.unloaded_physical_decoder.is_some() {
            0
        } else {
            self.num_rows
        }
    }

    fn rows_drained(&self) -> u64 {
        if self.unloaded_physical_decoder.is_some() {
            0
        } else {
            self.rows_drained
        }
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

pub struct PrimitiveFieldEncoder {
    accumulation_queue: AccumulationQueue,
    array_encoding_strategy: Arc<dyn ArrayEncodingStrategy>,
    column_index: u32,
    field: Field,
    max_page_bytes: u64,
}

impl PrimitiveFieldEncoder {
    pub fn try_new(
        options: &EncodingOptions,
        array_encoding_strategy: Arc<dyn ArrayEncodingStrategy>,
        column_index: u32,
        field: Field,
    ) -> Result<Self> {
        Ok(Self {
            accumulation_queue: AccumulationQueue::new(
                options.cache_bytes_per_column,
                column_index,
                options.keep_original_array,
            ),
            column_index,
            max_page_bytes: options.max_page_bytes,
            array_encoding_strategy,
            field,
        })
    }

    fn create_encode_task(&mut self, arrays: Vec<ArrayRef>) -> Result<EncodeTask> {
        let encoder = self
            .array_encoding_strategy
            .create_array_encoder(&arrays, &self.field)?;
        let column_idx = self.column_index;
        let data_type = self.field.data_type();

        Ok(tokio::task::spawn(async move {
            let num_values = arrays.iter().map(|arr| arr.len() as u64).sum();
            let data = DataBlock::from_arrays(&arrays, num_values);
            let mut buffer_index = 0;
            let array = encoder.encode(data, &data_type, &mut buffer_index)?;
            let (data, description) = array.into_buffers();
            Ok(EncodedPage {
                data,
                description: PageEncoding::Legacy(description),
                num_rows: num_values,
                column_idx,
                row_number: 0, // v2.0 encoders do not use
            })
        })
        .map(|res_res| {
            res_res.unwrap_or_else(|err| {
                Err(Error::internal(format!(
                    "Encoding task failed with error: {:?}",
                    err
                )))
            })
        })
        .boxed())
    }

    // Creates an encode task, consuming all buffered data
    fn do_flush(&mut self, arrays: Vec<ArrayRef>) -> Result<Vec<EncodeTask>> {
        if arrays.len() == 1 {
            let array = arrays.into_iter().next().unwrap();
            let size_bytes = array.get_buffer_memory_size();
            let num_parts = bit_util::ceil(size_bytes, self.max_page_bytes as usize);
            // Can't slice it finer than 1 page per row
            let num_parts = num_parts.min(array.len());
            if num_parts <= 1 {
                // One part and it fits in a page
                Ok(vec![self.create_encode_task(vec![array])?])
            } else {
                // One part and it needs to be sliced into multiple pages

                // This isn't perfect (items in the array might not all have the same size)
                // but it's a reasonable stab for now)
                let mut tasks = Vec::with_capacity(num_parts);
                let mut offset = 0;
                let part_size = bit_util::ceil(array.len(), num_parts);
                for _ in 0..num_parts {
                    let avail = array.len() - offset;
                    if avail == 0 {
                        break;
                    }
                    let chunk_size = avail.min(part_size);
                    let part = array.slice(offset, chunk_size);
                    let task = self.create_encode_task(vec![part])?;
                    tasks.push(task);
                    offset += chunk_size;
                }
                Ok(tasks)
            }
        } else {
            // Multiple parts that (presumably) all fit in a page
            //
            // TODO: Could check here if there are any jumbo parts in the mix that need splitting
            Ok(vec![self.create_encode_task(arrays)?])
        }
    }
}

impl FieldEncoder for PrimitiveFieldEncoder {
    // Buffers data, if there is enough to write a page then we create an encode task
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        _external_buffers: &mut OutOfLineBuffers,
        _repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        if let Some(arrays) = self.accumulation_queue.insert(array, row_number, num_rows) {
            Ok(self.do_flush(arrays.0)?)
        } else {
            Ok(vec![])
        }
    }

    // If there is any data left in the buffer then create an encode task from it
    fn flush(&mut self, _external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>> {
        if let Some(arrays) = self.accumulation_queue.flush() {
            Ok(self.do_flush(arrays.0)?)
        } else {
            Ok(vec![])
        }
    }

    fn num_columns(&self) -> u32 {
        1
    }

    fn finish(
        &mut self,
        _external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<crate::encoder::EncodedColumn>>> {
        std::future::ready(Ok(vec![EncodedColumn::default()])).boxed()
    }
}
