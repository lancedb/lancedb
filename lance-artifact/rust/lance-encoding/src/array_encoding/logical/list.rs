// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    collections::VecDeque,
    ops::Range,
    sync::{Arc, OnceLock},
};

use arrow_array::{
    Array, ArrayRef, BooleanArray, Int32Array, Int64Array, LargeListArray, ListArray, UInt64Array,
    cast::AsArray,
    new_empty_array,
    types::{Int32Type, Int64Type, UInt64Type},
};
use arrow_buffer::{BooleanBuffer, BooleanBufferBuilder, Buffer, NullBuffer, OffsetBuffer};
use arrow_schema::{DataType, Field, Fields};
use futures::{FutureExt, future::BoxFuture};
use lance_core::{Error, Result, cache::LanceCache, utils::parse::str_is_truthy};
use log::trace;
use tokio::task::JoinHandle;

use crate::{
    EncodingsIo,
    array_encoding::logical::r#struct::{SimpleStructDecoder, SimpleStructScheduler},
    buffer::LanceBuffer,
    data::{BlockInfo, DataBlock, FixedWidthDataBlock},
    decoder::{
        DecodeArrayTask, DecodeBatchScheduler, FieldScheduler, FilterExpression, ListPriorityRange,
        LogicalPageDecoder, MessageType, NextDecodeTask, PageEncoding, PriorityRange,
        ScheduledScanLine, SchedulerContext, SchedulingJob,
    },
    encoder::{
        ArrayEncoder, EncodeTask, EncodedArray, EncodedColumn, EncodedPage, FieldEncoder,
        OutOfLineBuffers,
    },
    format::pb,
    repdef::RepDefBuilder,
    utils::accumulation::AccumulationQueue,
};

/// When set, indirect I/O in the 2.0 list scheduler bypasses the backpressure system.
///
/// This can be a blunt instrument to avoid deadlocks in 2.0 scenarios
/// Set LANCE_BYPASS_INDIRECT_IO_BACKPRESSURE=1 to enable.
static BYPASS_INDIRECT_IO_BACKPRESSURE: OnceLock<bool> = OnceLock::new();

fn bypass_indirect_io_backpressure() -> bool {
    *BYPASS_INDIRECT_IO_BACKPRESSURE.get_or_init(|| {
        std::env::var("LANCE_BYPASS_INDIRECT_IO_BACKPRESSURE")
            .map(|val| str_is_truthy(&val))
            .unwrap_or(false)
    })
}

// Scheduling lists is tricky.  Imagine the following scenario:
//
// * There are 2000 offsets per offsets page
// * The user requests range 8000..8500
//
// First, since 8000 matches the start of an offsets page, we don't need to read an extra offset.
//
// Since this range matches the start of a page, we know we will get an offsets array like
// [0, ...]
//
// We need to restore nulls, which relies on a null offset adjustment, which is unique to each offsets
// page.
//
// We need to map this to [X, ...] where X is the sum of the number of items in the 0-2000, 2000-4000,
// and 4000-6000 pages.
//
// This gets even trickier if a range spans multiple offsets pages.  For example, given the same
// scenario but the user requests 7999..8500.  In this case the first page read will include an
// extra offset (e.g. we need to read 7998..8000), the null adjustment will be different between the
// two, and the items offset will be different.
//
// To handle this, we take the incoming row requests, look at the page info, and then calculate
// list requests.

#[derive(Debug)]
struct ListRequest {
    /// How many lists this request maps to
    num_lists: u64,
    /// Did this request include an extra offset
    includes_extra_offset: bool,
    /// The null offset adjustment for this request
    null_offset_adjustment: u64,
    /// items offset to apply
    items_offset: u64,
}

#[derive(Debug)]
struct ListRequestsIter {
    // The bool triggers whether we need to skip an offset or not
    list_requests: VecDeque<ListRequest>,
    offsets_requests: Vec<Range<u64>>,
}

impl ListRequestsIter {
    // TODO: This logic relies on row_ranges being ordered and may be a problem when we
    // add proper support for out-of-order take
    fn new(row_ranges: &[Range<u64>], page_infos: &[OffsetPageInfo]) -> Self {
        let mut items_offset = 0;
        let mut offsets_offset = 0;
        let mut page_infos_iter = page_infos.iter();
        let mut cur_page_info = page_infos_iter.next().unwrap();
        let mut list_requests = VecDeque::new();
        let mut offsets_requests = Vec::new();

        // Each row range maps to at least one list request.  It may map to more if the
        // range spans multiple offsets pages.
        for range in row_ranges {
            let mut range = range.clone();

            // Skip any offsets pages that are before the range
            while offsets_offset + (cur_page_info.offsets_in_page) <= range.start {
                trace!("Skipping null offset adjustment chunk {:?}", offsets_offset);
                offsets_offset += cur_page_info.offsets_in_page;
                items_offset += cur_page_info.num_items_referenced_by_page;
                cur_page_info = page_infos_iter.next().unwrap();
            }

            // If the range starts at the beginning of an offsets page we don't need
            // to read an extra offset
            let mut includes_extra_offset = range.start != offsets_offset;
            if includes_extra_offset {
                offsets_requests.push(range.start - 1..range.end);
            } else {
                offsets_requests.push(range.clone());
            }

            // At this point our range overlaps the current page (cur_page_info) and
            // we can start slicing it into list requests
            while !range.is_empty() {
                // The end of the list request is the min of the end of the range
                // and the end of the current page
                let end = offsets_offset + cur_page_info.offsets_in_page;
                let last = end >= range.end;
                let end = end.min(range.end);
                list_requests.push_back(ListRequest {
                    num_lists: end - range.start,
                    includes_extra_offset,
                    null_offset_adjustment: cur_page_info.null_offset_adjustment,
                    items_offset,
                });

                includes_extra_offset = false;
                range.start = end;
                // If there is still more data in the range, we need to move to the
                // next page
                if !last {
                    offsets_offset += cur_page_info.offsets_in_page;
                    items_offset += cur_page_info.num_items_referenced_by_page;
                    cur_page_info = page_infos_iter.next().unwrap();
                }
            }
        }
        Self {
            list_requests,
            offsets_requests,
        }
    }

    // Given a page of offset data, grab the corresponding list requests
    fn next(&mut self, mut num_offsets: u64) -> Vec<ListRequest> {
        let mut list_requests = Vec::new();
        while num_offsets > 0 {
            let req = self.list_requests.front_mut().unwrap();
            // If the request did not start at zero then we need to read an extra offset
            if req.includes_extra_offset {
                num_offsets -= 1;
                debug_assert_ne!(num_offsets, 0);
            }
            if num_offsets >= req.num_lists {
                num_offsets -= req.num_lists;
                list_requests.push(self.list_requests.pop_front().unwrap());
            } else {
                let sub_req = ListRequest {
                    num_lists: num_offsets,
                    includes_extra_offset: req.includes_extra_offset,
                    null_offset_adjustment: req.null_offset_adjustment,
                    items_offset: req.items_offset,
                };

                list_requests.push(sub_req);
                req.includes_extra_offset = false;
                req.num_lists -= num_offsets;
                num_offsets = 0;
            }
        }
        list_requests
    }
}

/// Given a list of offsets and a list of requested list row ranges we need to rewrite the offsets so that
/// they appear as expected for a list array.  This involves a number of tasks:
///
///  * Nulls in the offsets are represented by oversize values and these need to be converted to
///    the appropriate length
///  * For each range we (usually) load N + 1 offsets, so if we have 5 ranges we have 5 extra values
///    and we need to drop 4 of those.
///  * Ranges may not start at 0 and, while we don't strictly need to, we want to go ahead and normalize
///    the offsets so that the first offset is 0.
///
/// Throughout the comments we will consider the following example case:
///
/// The user requests the following ranges of lists (list_row_ranges): [0..3, 5..6]
///
/// This is a total of 4 lists.  The loaded offsets are [10, 20, 120, 150, 60].  The last valid offset is 99.
/// The null_offset_adjustment will be 100.
///
/// Our desired output offsets are going to be [0, 10, 20, 20, 30] and the item ranges are [0..20] and [50..60]
/// The validity array is [true, true, false, true]
fn decode_offsets(
    offsets: &dyn Array,
    list_requests: &[ListRequest],
    null_offset_adjustment: u64,
) -> (VecDeque<Range<u64>>, Vec<u64>, BooleanBuffer) {
    // In our example this is [10, 20, 120, 50, 60]
    let numeric_offsets = offsets.as_primitive::<UInt64Type>();
    // In our example there are 4 total lists
    let total_num_lists = list_requests.iter().map(|req| req.num_lists).sum::<u64>() as u32;
    let mut normalized_offsets = Vec::with_capacity(total_num_lists as usize);
    let mut validity_buffer = BooleanBufferBuilder::new(total_num_lists as usize);
    // The first output offset is always 0 no matter what
    normalized_offsets.push(0);
    let mut last_normalized_offset = 0;
    let offsets_values = numeric_offsets.values();

    let mut item_ranges = VecDeque::new();
    let mut offsets_offset: u32 = 0;
    // All ranges should be non-empty
    debug_assert!(list_requests.iter().all(|r| r.num_lists > 0));
    for req in list_requests {
        // The # of lists in this particular range
        let num_lists = req.num_lists;

        // Because we know the first offset is always 0 we don't store that.  This means we have special
        // logic if a range starts at 0 (we didn't need to read an extra offset value in that case)
        // In our example we enter this special case on the first range (0..3) but not the second (5..6)
        // This means the first range, which has 3 lists, maps to 3 values in our offsets array [10, 20, 120]
        // However, the second range, which has 1 list, maps to 2 values in our offsets array [150, 60]
        let (items_range, offsets_to_norm_start, num_offsets_to_norm) =
            if !req.includes_extra_offset {
                // In our example items start is 0 and items_end is 20
                let first_offset_idx = 0_usize;
                let num_offsets = num_lists as usize;
                let items_start = 0;
                let items_end = offsets_values[num_offsets - 1] % null_offset_adjustment;
                let items_range = items_start..items_end;
                (items_range, first_offset_idx, num_offsets)
            } else {
                // In our example, offsets_offset will be 3, items_start will be 50, and items_end will
                // be 60
                let first_offset_idx = offsets_offset as usize;
                let num_offsets = num_lists as usize + 1;
                let items_start = offsets_values[first_offset_idx] % null_offset_adjustment;
                let items_end =
                    offsets_values[first_offset_idx + num_offsets - 1] % null_offset_adjustment;
                let items_range = items_start..items_end;
                (items_range, first_offset_idx, num_offsets)
            };

        // TODO: Maybe consider writing whether there are nulls or not as part of the
        // page description.  Then we can skip all validity work.  Not clear if that will
        // be any benefit though.

        // We calculate validity from all elements but the first (or all elements
        // if this is the special zero-start case)
        //
        // So, in our first pass through, we consider [10, 20, 120] (1 null)
        // In our second pass through we only consider [60] (0 nulls)
        // Note that the 150 is null but we only loaded it to know where the 50-60 list started
        // and it doesn't actually correspond to a list (e.g. list 4 is null but we aren't loading it
        // here)
        let validity_start = if !req.includes_extra_offset {
            0
        } else {
            offsets_to_norm_start + 1
        };
        for off in offsets_values
            .slice(validity_start, num_lists as usize)
            .iter()
        {
            validity_buffer.append(*off < null_offset_adjustment);
        }

        // In our special case we need to account for the offset 0-first_item
        if !req.includes_extra_offset {
            let first_item = offsets_values[0] % null_offset_adjustment;
            normalized_offsets.push(first_item);
            last_normalized_offset = first_item;
        }

        // Finally, we go through and shift the offsets.  If we just returned them as is (taking care of
        // nulls) we would get [0, 10, 20, 20, 60] but our last list only has 10 items, not 40 and so we
        // need to shift that 60 to a 40.
        normalized_offsets.extend(
                offsets_values
                    .slice(offsets_to_norm_start, num_offsets_to_norm)
                    .windows(2)
                    .map(|w| {
                        let start = w[0] % null_offset_adjustment;
                        let end = w[1] % null_offset_adjustment;
                        if end < start {
                            panic!("End is less than start in window {:?} with null_offset_adjustment={} we get start={} and end={}", w, null_offset_adjustment, start, end);
                        }
                        let length = end - start;
                        last_normalized_offset += length;
                        last_normalized_offset
                    }),
            );
        trace!(
            "List offsets range of {} lists maps to item range {:?}",
            num_lists, items_range
        );
        offsets_offset += num_offsets_to_norm as u32;
        if !items_range.is_empty() {
            let items_range =
                items_range.start + req.items_offset..items_range.end + req.items_offset;
            item_ranges.push_back(items_range);
        }
    }

    let validity = validity_buffer.finish();
    (item_ranges, normalized_offsets, validity)
}

/// After scheduling the offsets we immediately launch this task as a new tokio task
/// This task waits for the offsets to arrive, decodes them, and then schedules the I/O
/// for the items.
///
/// This task does not wait for the items data.  That happens on the main decode loop (unless
/// we have list of list of ... in which case it happens in the outer indirect decode loop)
#[allow(clippy::too_many_arguments)]
async fn indirect_schedule_task(
    mut offsets_decoder: Box<dyn LogicalPageDecoder>,
    list_requests: Vec<ListRequest>,
    null_offset_adjustment: u64,
    items_scheduler: Arc<dyn FieldScheduler>,
    items_type: DataType,
    io: Arc<dyn EncodingsIo>,
    cache: Arc<LanceCache>,
    priority: Box<dyn PriorityRange>,
) -> Result<IndirectlyLoaded> {
    let num_offsets = offsets_decoder.num_rows();
    // We know the offsets are a primitive array and thus will not need additional
    // pages.  We can use a dummy receiver to match the decoder API
    offsets_decoder.wait_for_loaded(num_offsets - 1).await?;
    let decode_task = offsets_decoder.drain(num_offsets)?;
    let (offsets, _) = decode_task.task.decode()?;

    let (item_ranges, offsets, validity) =
        decode_offsets(offsets.as_ref(), &list_requests, null_offset_adjustment);

    trace!(
        "Indirectly scheduling items ranges {:?} from list items column with {} rows (and priority {:?})",
        item_ranges,
        items_scheduler.num_rows(),
        priority
    );
    let offsets: Arc<[u64]> = offsets.into();

    // All requested lists are empty
    if item_ranges.is_empty() {
        debug_assert!(item_ranges.iter().all(|r| r.start == r.end));
        return Ok(IndirectlyLoaded {
            root_decoder: None,
            offsets,
            validity,
        });
    }
    let item_ranges = item_ranges.into_iter().collect::<Vec<_>>();
    let num_items = item_ranges.iter().map(|r| r.end - r.start).sum::<u64>();

    // Create a new root scheduler, which has one column, which is our items data
    let root_fields = Fields::from(vec![Field::new("item", items_type, true)]);
    let indirect_root_scheduler =
        SimpleStructScheduler::new(vec![items_scheduler], root_fields.clone(), num_items);
    #[allow(deprecated)]
    let mut indirect_scheduler = DecodeBatchScheduler::from_scheduler(
        Arc::new(indirect_root_scheduler),
        root_fields.clone(),
        cache,
    );
    let mut root_decoder = SimpleStructDecoder::new(root_fields, num_items);

    let priority = Box::new(ListPriorityRange::new(priority, offsets.clone()));

    let indirect_messages = indirect_scheduler.schedule_ranges_to_vec(
        &item_ranges,
        // Can't push filters into list items
        &FilterExpression::no_filter(),
        io,
        Some(priority),
    )?;

    for message in indirect_messages {
        for decoder in message.decoders {
            let decoder = decoder.into_array();
            if !decoder.path.is_empty() {
                root_decoder.accept_child(decoder)?;
            }
        }
    }

    Ok(IndirectlyLoaded {
        offsets,
        validity,
        root_decoder: Some(root_decoder),
    })
}

#[derive(Debug)]
struct ListFieldSchedulingJob<'a> {
    scheduler: &'a ListFieldScheduler,
    offsets: Box<dyn SchedulingJob + 'a>,
    num_rows: u64,
    list_requests_iter: ListRequestsIter,
}

impl<'a> ListFieldSchedulingJob<'a> {
    fn try_new(
        scheduler: &'a ListFieldScheduler,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Self> {
        let list_requests_iter = ListRequestsIter::new(ranges, &scheduler.offset_page_info);
        let num_rows = ranges.iter().map(|r| r.end - r.start).sum::<u64>();
        let offsets = scheduler
            .offsets_scheduler
            .schedule_ranges(&list_requests_iter.offsets_requests, filter)?;
        Ok(Self {
            scheduler,
            offsets,
            list_requests_iter,
            num_rows,
        })
    }
}

impl SchedulingJob for ListFieldSchedulingJob<'_> {
    fn schedule_next(
        &mut self,
        context: &mut SchedulerContext,
        priority: &dyn PriorityRange,
    ) -> Result<ScheduledScanLine> {
        let next_offsets = self.offsets.schedule_next(context, priority)?;
        let offsets_scheduled = next_offsets.rows_scheduled;
        let list_reqs = self.list_requests_iter.next(offsets_scheduled);
        trace!(
            "Scheduled {} offsets which maps to list requests: {:?}",
            offsets_scheduled, list_reqs
        );
        let null_offset_adjustment = list_reqs[0].null_offset_adjustment;
        // It shouldn't be possible for `list_reqs` to span more than one offsets page and so it shouldn't
        // be possible for the null_offset_adjustment to change
        debug_assert!(
            list_reqs
                .iter()
                .all(|req| req.null_offset_adjustment == null_offset_adjustment)
        );
        let num_rows = list_reqs.iter().map(|req| req.num_lists).sum::<u64>();
        // offsets is a uint64 which is guaranteed to create one decoder on each call to schedule_next
        let next_offsets_decoder = next_offsets
            .decoders
            .into_iter()
            .next()
            .unwrap()
            .into_array()
            .decoder;

        let items_scheduler = self.scheduler.items_scheduler.clone();
        let items_type = self.scheduler.items_field.data_type().clone();
        let base_io = context.io().clone();
        let io = if bypass_indirect_io_backpressure() {
            base_io.with_bypass_backpressure().unwrap_or(base_io)
        } else {
            base_io
        };
        let cache = context.cache().clone();

        // Immediately spawn the indirect scheduling
        let indirect_fut = tokio::spawn(indirect_schedule_task(
            next_offsets_decoder,
            list_reqs,
            null_offset_adjustment,
            items_scheduler,
            items_type,
            io,
            cache,
            priority.box_clone(),
        ));

        // Return a decoder
        let decoder = Box::new(ListPageDecoder {
            offsets: Arc::new([]),
            validity: BooleanBuffer::new(Buffer::from_vec(Vec::<u8>::default()), 0, 0),
            item_decoder: None,
            rows_drained: 0,
            rows_loaded: 0,
            items_field: self.scheduler.items_field.clone(),
            num_rows,
            unloaded: Some(indirect_fut),
            offset_type: self.scheduler.offset_type.clone(),
            data_type: self.scheduler.list_type.clone(),
        });
        #[allow(deprecated)]
        let decoder = context.locate_decoder(decoder);
        Ok(ScheduledScanLine {
            decoders: vec![MessageType::DecoderReady(decoder)],
            rows_scheduled: num_rows,
        })
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

/// A page scheduler for list fields that encodes offsets in one field and items in another
///
/// The list scheduler is somewhat unique because it requires indirect I/O.  We cannot know the
/// ranges we need simply by looking at the metadata.  This means that list scheduling doesn't
/// fit neatly into the two-thread schedule-loop / decode-loop model.  To handle this, when a
/// list page is scheduled, we only schedule the I/O for the offsets and then we immediately
/// launch a new tokio task.  This new task waits for the offsets, decodes them, and then
/// schedules the I/O for the items.  Keep in mind that list items can be lists themselves.  If
/// that is the case then this indirection will continue.  The decode task that is returned will
/// only finish `wait`ing when all of the I/O has completed.
///
/// Whenever we schedule follow-up I/O like this the priority is based on the top-level row
/// index.  This helps ensure that earlier rows get finished completely (including follow up
/// tasks) before we perform I/O for later rows.
#[derive(Debug)]
pub struct ListFieldScheduler {
    offsets_scheduler: Arc<dyn FieldScheduler>,
    items_scheduler: Arc<dyn FieldScheduler>,
    items_field: Arc<Field>,
    offset_type: DataType,
    list_type: DataType,
    offset_page_info: Vec<OffsetPageInfo>,
}

/// The offsets are stored in a uint64 encoded column.  For each page we
/// store some supplementary data that helps us understand the offsets.
/// This is needed to construct the scheduler
#[derive(Debug)]
pub struct OffsetPageInfo {
    pub offsets_in_page: u64,
    pub null_offset_adjustment: u64,
    pub num_items_referenced_by_page: u64,
}

impl ListFieldScheduler {
    // Create a new ListPageScheduler
    pub fn new(
        offsets_scheduler: Arc<dyn FieldScheduler>,
        items_scheduler: Arc<dyn FieldScheduler>,
        items_field: Arc<Field>,
        // Should be int32 or int64
        offset_type: DataType,
        offset_page_info: Vec<OffsetPageInfo>,
    ) -> Self {
        let list_type = match &offset_type {
            DataType::Int32 => DataType::List(items_field.clone()),
            DataType::Int64 => DataType::LargeList(items_field.clone()),
            _ => panic!("Unexpected offset type {}", offset_type),
        };
        Self {
            offsets_scheduler,
            items_scheduler,
            items_field,
            offset_type,
            offset_page_info,
            list_type,
        }
    }
}

impl FieldScheduler for ListFieldScheduler {
    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Box<dyn SchedulingJob + 'a>> {
        Ok(Box::new(ListFieldSchedulingJob::try_new(
            self, ranges, filter,
        )?))
    }

    fn num_rows(&self) -> u64 {
        self.offsets_scheduler.num_rows()
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

/// As soon as the first call to decode comes in we wait for all indirect I/O to
/// complete.
///
/// Once the indirect I/O is finished we pull items out of `unawaited`, wait them
/// (this wait should return immediately) and then push them into `item_decoders`.
///
/// We then drain from `item_decoders`, popping item pages off as we finish with
/// them.
///
/// TODO: Test the case where a single list page has multiple items pages
#[derive(Debug)]
struct ListPageDecoder {
    unloaded: Option<JoinHandle<Result<IndirectlyLoaded>>>,
    // offsets and validity will have already been decoded as part of the indirect I/O
    offsets: Arc<[u64]>,
    validity: BooleanBuffer,
    item_decoder: Option<SimpleStructDecoder>,
    num_rows: u64,
    rows_drained: u64,
    rows_loaded: u64,
    items_field: Arc<Field>,
    offset_type: DataType,
    data_type: DataType,
}

struct ListDecodeTask {
    offsets: Vec<u64>,
    validity: BooleanBuffer,
    // Will be None if there are no items (all empty / null lists)
    items: Option<Box<dyn DecodeArrayTask>>,
    items_field: Arc<Field>,
    offset_type: DataType,
}

impl DecodeArrayTask for ListDecodeTask {
    fn decode(self: Box<Self>) -> Result<(ArrayRef, u64)> {
        let items = self
            .items
            .map(|items| {
                // When we run the indirect I/O we wrap things in a struct array with a single field
                // named "item".  We can unwrap that now.
                let (wrapped_items, _) = items.decode()?;
                Result::Ok(wrapped_items.as_struct().column(0).clone())
            })
            .unwrap_or_else(|| Ok(new_empty_array(self.items_field.data_type())))?;

        // The offsets are already decoded but they need to be shifted back to 0 and cast
        // to the appropriate type
        //
        // Although, in some cases, the shift IS strictly required since the unshifted offsets
        // may cross i32::MAX even though the shifted offsets do not
        let offsets = UInt64Array::from(self.offsets);
        let validity = NullBuffer::new(self.validity);
        let validity = if validity.null_count() == 0 {
            None
        } else {
            Some(validity)
        };
        let min_offset = UInt64Array::new_scalar(offsets.value(0));
        let offsets = arrow_arith::numeric::sub(&offsets, &min_offset)?;
        let array: ArrayRef = match &self.offset_type {
            DataType::Int32 => {
                let offsets = arrow_cast::cast(&offsets, &DataType::Int32)?;
                let offsets_i32 = offsets.as_primitive::<Int32Type>();
                let offsets = OffsetBuffer::new(offsets_i32.values().clone());

                Arc::new(ListArray::try_new(
                    self.items_field.clone(),
                    offsets,
                    items,
                    validity,
                )?)
            }
            DataType::Int64 => {
                let offsets = arrow_cast::cast(&offsets, &DataType::Int64)?;
                let offsets_i64 = offsets.as_primitive::<Int64Type>();
                let offsets = OffsetBuffer::new(offsets_i64.values().clone());

                Arc::new(LargeListArray::try_new(
                    self.items_field.clone(),
                    offsets,
                    items,
                    validity,
                )?)
            }
            _ => panic!("ListDecodeTask with data type that is not i32 or i64"),
        };
        // data_size is only tracked in the v2.1 structural decode path; the v2.0 array
        // v2.0 path does not need it so we return 0.
        Ok((array, 0))
    }
}

// Helper method that performs binary search.  However, once the
// target is found it walks past any duplicates.  E.g. if the
// input list is [0, 3, 5, 5, 5, 7] then this will only return
// 0, 1, 4, or 5.
fn binary_search_to_end(to_search: &[u64], target: u64) -> u64 {
    let mut result = match to_search.binary_search(&target) {
        Ok(idx) => idx,
        Err(idx) => idx - 1,
    };
    while result < (to_search.len() - 1) && to_search[result + 1] == target {
        result += 1;
    }
    result as u64
}

impl LogicalPageDecoder for ListPageDecoder {
    fn wait_for_loaded(&mut self, loaded_need: u64) -> BoxFuture<'_, Result<()>> {
        async move {
            // wait for the indirect I/O to finish, run the scheduler for the indirect
            // I/O and then wait for enough items to arrive
            if self.unloaded.is_some() {
                trace!("List scheduler needs to wait for indirect I/O to complete");
                let indirectly_loaded = self.unloaded.take().unwrap().await;
                if let Err(err) = indirectly_loaded {
                    match err.try_into_panic() {
                        Ok(err) => std::panic::resume_unwind(err),
                        Err(err) => panic!("{:?}", err),
                    };
                }
                let indirectly_loaded = indirectly_loaded.unwrap()?;

                self.offsets = indirectly_loaded.offsets;
                self.validity = indirectly_loaded.validity;
                self.item_decoder = indirectly_loaded.root_decoder;
            }
            if self.rows_loaded > loaded_need {
                return Ok(());
            }

            let boundary = loaded_need as usize;
            debug_assert!(boundary < self.num_rows as usize);
            // We need more than X lists which means we need at least X+1 lists which means
            // we need at least offsets[X+1] items which means we need more than offsets[X+1]-1 items.
            let items_needed = self.offsets[boundary + 1].saturating_sub(1);
            trace!(
                "List decoder is waiting for more than {} rows to be loaded and {}/{} are already loaded.  To satisfy this we need more than {} loaded items",
                loaded_need,
                self.rows_loaded,
                self.num_rows,
                items_needed,
            );

            let items_loaded = if let Some(item_decoder) = self.item_decoder.as_mut() {
                item_decoder.wait_for_loaded(items_needed).await?;
                item_decoder.rows_loaded()
            } else {
                0
            };

            self.rows_loaded = binary_search_to_end(&self.offsets, items_loaded);
            trace!("List decoder now has {} loaded rows", self.rows_loaded);

            Ok(())
        }
        .boxed()
    }

    fn drain(&mut self, num_rows: u64) -> Result<NextDecodeTask> {
        // We already have the offsets but need to drain the item pages
        let mut actual_num_rows = num_rows;
        let item_start = self.offsets[self.rows_drained as usize];
        if self.offset_type != DataType::Int64 {
            // We might not be able to drain `num_rows` because that request might contain more than 2^31 items
            // so we need to figure out how many rows we can actually drain.
            while actual_num_rows > 0 {
                let num_items =
                    self.offsets[(self.rows_drained + actual_num_rows) as usize] - item_start;
                if num_items <= i32::MAX as u64 {
                    break;
                }
                // TODO: This could be slow.  Maybe faster to start from zero or do binary search.  Investigate when
                // actually adding support for smaller than requested batches
                actual_num_rows -= 1;
            }
        }
        if actual_num_rows < num_rows {
            // TODO: We should be able to automatically
            // shrink the read batch size if we detect the batches are going to be huge (maybe
            // even achieve this with a read_batch_bytes parameter, though some estimation may
            // still be required)
            return Err(Error::not_supported_source(format!("loading a batch of {} lists would require creating an array with over i32::MAX items and we don't yet support returning smaller than requested batches", num_rows).into()));
        }
        let offsets = self.offsets
            [self.rows_drained as usize..(self.rows_drained + actual_num_rows + 1) as usize]
            .to_vec();
        let validity = self
            .validity
            .slice(self.rows_drained as usize, actual_num_rows as usize);
        let start = offsets[0];
        let end = offsets[offsets.len() - 1];
        let num_items_to_drain = end - start;

        let item_decode = if num_items_to_drain == 0 {
            None
        } else {
            self.item_decoder
                .as_mut()
                .map(|item_decoder| Result::Ok(item_decoder.drain(num_items_to_drain)?.task))
                .transpose()?
        };

        self.rows_drained += num_rows;
        Ok(NextDecodeTask {
            num_rows,
            task: Box::new(ListDecodeTask {
                offsets,
                validity,
                items_field: self.items_field.clone(),
                items: item_decode,
                offset_type: self.offset_type.clone(),
            }) as Box<dyn DecodeArrayTask>,
        })
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }

    fn rows_loaded(&self) -> u64 {
        self.rows_loaded
    }

    fn rows_drained(&self) -> u64 {
        self.rows_drained
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

struct IndirectlyLoaded {
    offsets: Arc<[u64]>,
    validity: BooleanBuffer,
    root_decoder: Option<SimpleStructDecoder>,
}

impl std::fmt::Debug for IndirectlyLoaded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("IndirectlyLoaded")
            .field("offsets", &self.offsets)
            .field("validity", &self.validity)
            .finish()
    }
}

/// An encoder for list offsets that "stitches" offsets and encodes nulls into the offsets
///
/// If we need to encode several list arrays into a single page then we need to "stitch" the offsets
/// For example, imagine we have list arrays [[0, 1], [2]] and [[3, 4, 5]].
///
/// We will have offset arrays [0, 2, 3] and [0, 3].  We don't want to encode [0, 2, 3, 0, 3].  What
/// we want is [0, 2, 3, 6]
///
/// This encoder also handles validity by converting a null value into an oversized offset.  For example,
/// if we have four lists with offsets [0, 20, 20, 20, 30] and the list at index 2 is null (note that
/// the list at index 1 is empty) then we turn this into offsets [0, 20, 20, 51, 30].  We replace a null
/// offset with previous_offset + max_offset + 1.  This makes it possible to load a single item from the
/// list array.
///
/// These offsets are always stored on disk as a u64 array.  First, this is because its simply much more
/// likely than one expects that this is needed, even if our lists are not massive.  This is because we
/// only write an offsets page when we have enough data.  This means we will probably accumulate a million
/// offsets or more before we bother to write a page. If our lists have a few thousand items a piece then
/// we end up passing the u32::MAX boundary.
///
/// The second reason is that list offsets are very easily compacted with delta + bit packing and so those
/// u64 offsets should easily be shrunk down before being put on disk.
///
/// This encoder can encode both lists and large lists.  It can decode the resulting column into either type
/// as well. (TODO: Test and enable large lists)
///
/// You can even write as a large list and decode as a regular list (as long as no single list has more than
/// 2^31 items) or vice versa.  You could even encode a mixed stream of list and large list (but unclear that
/// would ever be useful)
#[derive(Debug)]
struct ListOffsetsEncoder {
    // An accumulation queue, we insert both offset arrays and validity arrays into this queue
    accumulation_queue: AccumulationQueue,
    // The inner encoder of offset values
    inner_encoder: Arc<dyn ArrayEncoder>,
    column_index: u32,
}

impl ListOffsetsEncoder {
    fn new(
        cache_bytes: u64,
        keep_original_array: bool,
        column_index: u32,
        inner_encoder: Arc<dyn ArrayEncoder>,
    ) -> Self {
        Self {
            accumulation_queue: AccumulationQueue::new(
                cache_bytes,
                column_index,
                keep_original_array,
            ),
            inner_encoder,
            column_index,
        }
    }

    /// Given a list array, return the offsets as a standalone ArrayRef (either an Int32Array or Int64Array)
    fn extract_offsets(list_arr: &dyn Array) -> ArrayRef {
        match list_arr.data_type() {
            DataType::List(_) => {
                let offsets = list_arr.as_list::<i32>().offsets().clone();
                Arc::new(Int32Array::new(offsets.into_inner(), None))
            }
            DataType::LargeList(_) => {
                let offsets = list_arr.as_list::<i64>().offsets().clone();
                Arc::new(Int64Array::new(offsets.into_inner(), None))
            }
            _ => panic!(),
        }
    }

    /// Converts the validity of a list array into a boolean array.  If there is no validity information
    /// then this is an empty boolean array.
    fn extract_validity(list_arr: &dyn Array) -> ArrayRef {
        if let Some(validity) = list_arr.nulls() {
            Arc::new(BooleanArray::new(validity.inner().clone(), None))
        } else {
            // We convert None validity into an empty array because the accumulation queue can't
            // handle Option<ArrayRef>
            new_empty_array(&DataType::Boolean)
        }
    }

    fn make_encode_task(&self, arrays: Vec<ArrayRef>) -> EncodeTask {
        let inner_encoder = self.inner_encoder.clone();
        let column_idx = self.column_index;
        // At this point we should have 2*N arrays where the even-indexed arrays are integer offsets
        // and the odd-indexed arrays are boolean validity bitmaps
        let offset_arrays = arrays.iter().step_by(2).cloned().collect::<Vec<_>>();
        let validity_arrays = arrays.into_iter().skip(1).step_by(2).collect::<Vec<_>>();

        tokio::task::spawn(async move {
            let num_rows =
                offset_arrays.iter().map(|arr| arr.len()).sum::<usize>() - offset_arrays.len();
            let num_rows = num_rows as u64;
            let mut buffer_index = 0;
            let array = Self::do_encode(
                offset_arrays,
                validity_arrays,
                &mut buffer_index,
                num_rows,
                inner_encoder,
            )?;
            let (data, description) = array.into_buffers();
            Ok(EncodedPage {
                data,
                description: PageEncoding::Legacy(description),
                num_rows,
                column_idx,
                row_number: 0, // V2.0 encoders do not use
            })
        })
        .map(|res_res| res_res.unwrap())
        .boxed()
    }

    fn maybe_encode_offsets_and_validity(&mut self, list_arr: &dyn Array) -> Option<EncodeTask> {
        let offsets = Self::extract_offsets(list_arr);
        let validity = Self::extract_validity(list_arr);
        let num_rows = offsets.len() as u64;
        // Either inserting the offsets OR inserting the validity could cause the
        // accumulation queue to fill up
        if let Some(mut arrays) = self
            .accumulation_queue
            .insert(offsets, /*row_number=*/ 0, num_rows)
        {
            arrays.0.push(validity);
            Some(self.make_encode_task(arrays.0))
        } else if let Some(arrays) = self
            .accumulation_queue
            .insert(validity, /*row_number=*/ 0, num_rows)
        {
            Some(self.make_encode_task(arrays.0))
        } else {
            None
        }
    }

    fn flush(&mut self) -> Option<EncodeTask> {
        if let Some(arrays) = self.accumulation_queue.flush() {
            Some(self.make_encode_task(arrays.0))
        } else {
            None
        }
    }

    // Get's the total number of items covered by an array of offsets (keeping in
    // mind that the first offset may not be zero)
    fn get_offset_span(array: &dyn Array) -> u64 {
        match array.data_type() {
            DataType::Int32 => {
                let arr_i32 = array.as_primitive::<Int32Type>();
                (arr_i32.value(arr_i32.len() - 1) - arr_i32.value(0)) as u64
            }
            DataType::Int64 => {
                let arr_i64 = array.as_primitive::<Int64Type>();
                (arr_i64.value(arr_i64.len() - 1) - arr_i64.value(0)) as u64
            }
            _ => panic!(),
        }
    }

    // This is where we do the work to actually shift the offsets and encode nulls
    // Note that the output is u64 and the input could be i32 OR i64.
    fn extend_offsets_vec_u64(
        dest: &mut Vec<u64>,
        offsets: &dyn Array,
        validity: Option<&BooleanArray>,
        // The offset of this list into the destination
        base: u64,
        null_offset_adjustment: u64,
    ) {
        match offsets.data_type() {
            DataType::Int32 => {
                let offsets_i32 = offsets.as_primitive::<Int32Type>();
                let start = offsets_i32.value(0) as u64;
                // If we want to take a list from start..X and change it into
                // a list from end..X then we need to add (base - start) to all elements
                // Note that `modifier` may be negative but (item + modifier) will always be >= 0
                let modifier = base as i64 - start as i64;
                if let Some(validity) = validity {
                    dest.extend(
                        offsets_i32
                            .values()
                            .iter()
                            .skip(1)
                            .zip(validity.values().iter())
                            .map(|(&off, valid)| {
                                (off as i64 + modifier) as u64
                                    + (!valid as u64 * null_offset_adjustment)
                            }),
                    );
                } else {
                    dest.extend(
                        offsets_i32
                            .values()
                            .iter()
                            .skip(1)
                            // Subtract by `start` so offsets start at 0
                            .map(|&v| (v as i64 + modifier) as u64),
                    );
                }
            }
            DataType::Int64 => {
                let offsets_i64 = offsets.as_primitive::<Int64Type>();
                let start = offsets_i64.value(0) as u64;
                // If we want to take a list from start..X and change it into
                // a list from end..X then we need to add (base - start) to all elements
                // Note that `modifier` may be negative but (item + modifier) will always be >= 0
                let modifier = base as i64 - start as i64;
                if let Some(validity) = validity {
                    dest.extend(
                        offsets_i64
                            .values()
                            .iter()
                            .skip(1)
                            .zip(validity.values().iter())
                            .map(|(&off, valid)| {
                                (off + modifier) as u64 + (!valid as u64 * null_offset_adjustment)
                            }),
                    )
                } else {
                    dest.extend(
                        offsets_i64
                            .values()
                            .iter()
                            .skip(1)
                            .map(|&v| (v + modifier) as u64),
                    );
                }
            }
            _ => panic!("Invalid list offsets data type {:?}", offsets.data_type()),
        }
    }

    fn do_encode_u64(
        offset_arrays: Vec<ArrayRef>,
        validity: Vec<Option<&BooleanArray>>,
        num_offsets: u64,
        null_offset_adjustment: u64,
        buffer_index: &mut u32,
        inner_encoder: Arc<dyn ArrayEncoder>,
    ) -> Result<EncodedArray> {
        let mut offsets = Vec::with_capacity(num_offsets as usize);
        for (offsets_arr, validity_arr) in offset_arrays.iter().zip(validity) {
            let last_prev_offset = offsets.last().copied().unwrap_or(0) % null_offset_adjustment;
            Self::extend_offsets_vec_u64(
                &mut offsets,
                &offsets_arr,
                validity_arr,
                last_prev_offset,
                null_offset_adjustment,
            );
        }
        let offsets_data = DataBlock::FixedWidth(FixedWidthDataBlock {
            bits_per_value: 64,
            data: LanceBuffer::reinterpret_vec(offsets),
            num_values: num_offsets,
            block_info: BlockInfo::new(),
        });
        inner_encoder.encode(offsets_data, &DataType::UInt64, buffer_index)
    }

    fn do_encode(
        offset_arrays: Vec<ArrayRef>,
        validity_arrays: Vec<ArrayRef>,
        buffer_index: &mut u32,
        num_offsets: u64,
        inner_encoder: Arc<dyn ArrayEncoder>,
    ) -> Result<EncodedArray> {
        let validity_arrays = validity_arrays
            .iter()
            .map(|v| {
                if v.is_empty() {
                    None
                } else {
                    Some(v.as_boolean())
                }
            })
            .collect::<Vec<_>>();
        debug_assert_eq!(offset_arrays.len(), validity_arrays.len());
        let total_span = offset_arrays
            .iter()
            .map(|arr| Self::get_offset_span(arr.as_ref()))
            .sum::<u64>();
        // See encodings.proto for reasoning behind this value
        let null_offset_adjustment = total_span + 1;
        let encoded_offsets = Self::do_encode_u64(
            offset_arrays,
            validity_arrays,
            num_offsets,
            null_offset_adjustment,
            buffer_index,
            inner_encoder,
        )?;
        Ok(EncodedArray {
            data: encoded_offsets.data,
            encoding: pb::ArrayEncoding {
                array_encoding: Some(pb::array_encoding::ArrayEncoding::List(Box::new(
                    pb::List {
                        offsets: Some(Box::new(encoded_offsets.encoding)),
                        null_offset_adjustment,
                        num_items: total_span,
                    },
                ))),
            },
        })
    }
}

pub struct ListFieldEncoder {
    offsets_encoder: ListOffsetsEncoder,
    items_encoder: Box<dyn FieldEncoder>,
}

impl ListFieldEncoder {
    pub fn new(
        items_encoder: Box<dyn FieldEncoder>,
        inner_offsets_encoder: Arc<dyn ArrayEncoder>,
        cache_bytes_per_columns: u64,
        keep_original_array: bool,
        column_index: u32,
    ) -> Self {
        Self {
            offsets_encoder: ListOffsetsEncoder::new(
                cache_bytes_per_columns,
                keep_original_array,
                column_index,
                inner_offsets_encoder,
            ),
            items_encoder,
        }
    }

    fn combine_tasks(
        offsets_tasks: Vec<EncodeTask>,
        item_tasks: Vec<EncodeTask>,
    ) -> Result<Vec<EncodeTask>> {
        let mut all_tasks = offsets_tasks;
        let item_tasks = item_tasks;
        all_tasks.extend(item_tasks);
        Ok(all_tasks)
    }
}

impl FieldEncoder for ListFieldEncoder {
    fn maybe_encode(
        &mut self,
        array: ArrayRef,
        external_buffers: &mut OutOfLineBuffers,
        repdef: RepDefBuilder,
        row_number: u64,
        num_rows: u64,
    ) -> Result<Vec<EncodeTask>> {
        // The list may have an offset / shorter length which means the underlying
        // values array could be longer than what we need to encode and so we need
        // to slice down to the region of interest.
        let items = match array.data_type() {
            DataType::List(_) => {
                let list_arr = array.as_list::<i32>();
                let items_start = list_arr.value_offsets()[list_arr.offset()] as usize;
                let items_end =
                    list_arr.value_offsets()[list_arr.offset() + list_arr.len()] as usize;
                list_arr
                    .values()
                    .slice(items_start, items_end - items_start)
            }
            DataType::LargeList(_) => {
                let list_arr = array.as_list::<i64>();
                let items_start = list_arr.value_offsets()[list_arr.offset()] as usize;
                let items_end =
                    list_arr.value_offsets()[list_arr.offset() + list_arr.len()] as usize;
                list_arr
                    .values()
                    .slice(items_start, items_end - items_start)
            }
            _ => panic!(),
        };
        let offsets_tasks = self
            .offsets_encoder
            .maybe_encode_offsets_and_validity(array.as_ref())
            .map(|task| vec![task])
            .unwrap_or_default();
        let mut item_tasks = self.items_encoder.maybe_encode(
            items,
            external_buffers,
            repdef,
            row_number,
            num_rows,
        )?;
        if !offsets_tasks.is_empty() && item_tasks.is_empty() {
            // An items page cannot currently be shared by two different offsets pages.  This is
            // a limitation in the current scheduler and could be addressed in the future.  As a result
            // we always need to encode the items page if we encode the offsets page.
            //
            // In practice this isn't usually too bad unless we are targeting very small pages.
            item_tasks = self.items_encoder.flush(external_buffers)?;
        }
        Self::combine_tasks(offsets_tasks, item_tasks)
    }

    fn flush(&mut self, external_buffers: &mut OutOfLineBuffers) -> Result<Vec<EncodeTask>> {
        let offsets_tasks = self
            .offsets_encoder
            .flush()
            .map(|task| vec![task])
            .unwrap_or_default();
        let item_tasks = self.items_encoder.flush(external_buffers)?;
        Self::combine_tasks(offsets_tasks, item_tasks)
    }

    fn num_columns(&self) -> u32 {
        self.items_encoder.num_columns() + 1
    }

    fn finish(
        &mut self,
        external_buffers: &mut OutOfLineBuffers,
    ) -> BoxFuture<'_, Result<Vec<EncodedColumn>>> {
        let inner_columns = self.items_encoder.finish(external_buffers);
        async move {
            let mut columns = vec![EncodedColumn::default()];
            let inner_columns = inner_columns.await?;
            columns.extend(inner_columns);
            Ok(columns)
        }
        .boxed()
    }
}
