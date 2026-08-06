// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use std::{
    collections::{BinaryHeap, VecDeque},
    ops::Range,
    sync::Arc,
};

use crate::{
    decoder::{
        DecodeArrayTask, FilterExpression, MessageType, NextDecodeTask, PriorityRange,
        ScheduledScanLine, SchedulerContext,
    },
    decoder::{DecoderReady, FieldScheduler, LogicalPageDecoder, SchedulingJob},
};
use arrow_array::{ArrayRef, StructArray};
use arrow_schema::{DataType, Field, Fields};
use futures::{FutureExt, StreamExt, TryStreamExt, future::BoxFuture, stream::FuturesUnordered};
use lance_core::{Error, Result};
use log::trace;

#[derive(Debug)]
struct SchedulingJobWithStatus<'a> {
    col_idx: u32,
    col_name: &'a str,
    job: Box<dyn SchedulingJob + 'a>,
    rows_scheduled: u64,
    rows_remaining: u64,
}

impl PartialEq for SchedulingJobWithStatus<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.col_idx == other.col_idx
    }
}

impl Eq for SchedulingJobWithStatus<'_> {}

impl PartialOrd for SchedulingJobWithStatus<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SchedulingJobWithStatus<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note this is reversed to make it min-heap
        other.rows_scheduled.cmp(&self.rows_scheduled)
    }
}

#[derive(Debug)]
struct EmptyStructDecodeTask {
    num_rows: u64,
}

impl DecodeArrayTask for EmptyStructDecodeTask {
    fn decode(self: Box<Self>) -> Result<(ArrayRef, u64)> {
        // data_size is only tracked in the v2.1 structural decode path; the v2.0 array
        // v2.0 path does not need it so we return 0.
        Ok((
            Arc::new(StructArray::new_empty_fields(self.num_rows as usize, None)),
            0,
        ))
    }
}

#[derive(Debug)]
struct EmptyStructDecoder {
    num_rows: u64,
    rows_drained: u64,
    data_type: DataType,
}

impl EmptyStructDecoder {
    fn new(num_rows: u64) -> Self {
        Self {
            num_rows,
            rows_drained: 0,
            data_type: DataType::Struct(Fields::from(Vec::<Field>::default())),
        }
    }
}

impl LogicalPageDecoder for EmptyStructDecoder {
    fn wait_for_loaded(&mut self, _loaded_need: u64) -> BoxFuture<'_, Result<()>> {
        Box::pin(std::future::ready(Ok(())))
    }
    fn rows_loaded(&self) -> u64 {
        self.num_rows
    }
    fn rows_unloaded(&self) -> u64 {
        0
    }
    fn num_rows(&self) -> u64 {
        self.num_rows
    }
    fn rows_drained(&self) -> u64 {
        self.rows_drained
    }
    fn drain(&mut self, num_rows: u64) -> Result<NextDecodeTask> {
        self.rows_drained += num_rows;
        Ok(NextDecodeTask {
            num_rows,
            task: Box::new(EmptyStructDecodeTask { num_rows }),
        })
    }
    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

#[derive(Debug)]
struct EmptyStructSchedulerJob {
    num_rows: u64,
}

impl SchedulingJob for EmptyStructSchedulerJob {
    fn schedule_next(
        &mut self,
        context: &mut SchedulerContext,
        _priority: &dyn PriorityRange,
    ) -> Result<ScheduledScanLine> {
        let empty_decoder = Box::new(EmptyStructDecoder::new(self.num_rows));
        #[allow(deprecated)]
        let struct_decoder = context.locate_decoder(empty_decoder);
        Ok(ScheduledScanLine {
            decoders: vec![MessageType::DecoderReady(struct_decoder)],
            rows_scheduled: self.num_rows,
        })
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

/// Scheduling job for struct data
///
/// The order in which we schedule the children is important.  We want to schedule the child
/// with the least amount of data first.
///
/// This allows us to decode entire rows as quickly as possible
#[derive(Debug)]
struct SimpleStructSchedulerJob<'a> {
    scheduler: &'a SimpleStructScheduler,
    /// A min-heap whose key is the # of rows currently scheduled
    children: BinaryHeap<SchedulingJobWithStatus<'a>>,
    rows_scheduled: u64,
    num_rows: u64,
    initialized: bool,
}

impl<'a> SimpleStructSchedulerJob<'a> {
    fn new(
        scheduler: &'a SimpleStructScheduler,
        children: Vec<Box<dyn SchedulingJob + 'a>>,
        num_rows: u64,
    ) -> Self {
        let children = children
            .into_iter()
            .enumerate()
            .map(|(idx, job)| SchedulingJobWithStatus {
                col_idx: idx as u32,
                col_name: scheduler.child_fields[idx].name(),
                job,
                rows_scheduled: 0,
                rows_remaining: num_rows,
            })
            .collect::<BinaryHeap<_>>();
        Self {
            scheduler,
            children,
            rows_scheduled: 0,
            num_rows,
            initialized: false,
        }
    }
}

impl SchedulingJob for SimpleStructSchedulerJob<'_> {
    fn schedule_next(
        &mut self,
        mut context: &mut SchedulerContext,
        priority: &dyn PriorityRange,
    ) -> Result<ScheduledScanLine> {
        let mut decoders = Vec::new();
        if !self.initialized {
            // Send info to the decoder thread so it knows a struct is here.  In the future we will also
            // send validity info here.
            let struct_decoder = Box::new(SimpleStructDecoder::new(
                self.scheduler.child_fields.clone(),
                self.num_rows,
            ));
            #[allow(deprecated)]
            let struct_decoder = context.locate_decoder(struct_decoder);
            decoders.push(MessageType::DecoderReady(struct_decoder));
            self.initialized = true;
        }
        let old_rows_scheduled = self.rows_scheduled;
        // Schedule as many children as we need to until we have scheduled at least one
        // complete row
        while old_rows_scheduled == self.rows_scheduled {
            let mut next_child = self.children.pop().unwrap();
            trace!("Scheduling more rows for child {}", next_child.col_idx);
            let scoped = context.push(next_child.col_name, next_child.col_idx);
            let child_scan = next_child.job.schedule_next(scoped.context, priority)?;
            trace!(
                "Scheduled {} rows for child {}",
                child_scan.rows_scheduled, next_child.col_idx
            );
            next_child.rows_scheduled += child_scan.rows_scheduled;
            next_child.rows_remaining -= child_scan.rows_scheduled;
            decoders.extend(child_scan.decoders);
            self.children.push(next_child);
            self.rows_scheduled = self.children.peek().unwrap().rows_scheduled;
            context = scoped.pop();
        }
        let struct_rows_scheduled = self.rows_scheduled - old_rows_scheduled;
        Ok(ScheduledScanLine {
            decoders,
            rows_scheduled: struct_rows_scheduled,
        })
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }
}

/// A scheduler for structs
///
/// The implementation is actually a bit more tricky than one might initially think.  We can't just
/// go through and schedule each column one after the other.  This would mean our decode can't start
/// until nearly all the data has arrived (since we need data from each column)
///
/// Instead, we schedule in row-major fashion
///
/// Note: this scheduler is the starting point for all decoding.  This is because we treat the top-level
/// record batch as a non-nullable struct.
#[derive(Debug)]
pub struct SimpleStructScheduler {
    children: Vec<Arc<dyn FieldScheduler>>,
    child_fields: Fields,
    num_rows: u64,
}

impl SimpleStructScheduler {
    pub fn new(
        children: Vec<Arc<dyn FieldScheduler>>,
        child_fields: Fields,
        num_rows: u64,
    ) -> Self {
        let num_rows = children
            .first()
            .map(|child| child.num_rows())
            .unwrap_or(num_rows);
        debug_assert!(children.iter().all(|child| child.num_rows() == num_rows));
        Self {
            children,
            child_fields,
            num_rows,
        }
    }
}

impl FieldScheduler for SimpleStructScheduler {
    fn schedule_ranges<'a>(
        &'a self,
        ranges: &[Range<u64>],
        filter: &FilterExpression,
    ) -> Result<Box<dyn SchedulingJob + 'a>> {
        if self.children.is_empty() {
            return Ok(Box::new(EmptyStructSchedulerJob {
                num_rows: ranges.iter().map(|r| r.end - r.start).sum(),
            }));
        }
        let child_schedulers = self
            .children
            .iter()
            .map(|child| child.schedule_ranges(ranges, filter))
            .collect::<Result<Vec<_>>>()?;
        let num_rows = child_schedulers[0].num_rows();
        Ok(Box::new(SimpleStructSchedulerJob::new(
            self,
            child_schedulers,
            num_rows,
        )))
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }

    fn initialize<'a>(
        &'a self,
        _filter: &'a FilterExpression,
        _context: &'a SchedulerContext,
    ) -> BoxFuture<'a, Result<()>> {
        let futures = self
            .children
            .iter()
            .map(|child| child.initialize(_filter, _context))
            .collect::<FuturesUnordered<_>>();
        async move {
            futures
                .map(|res| res.map(|_| ()))
                .try_collect::<Vec<_>>()
                .await?;
            Ok(())
        }
        .boxed()
    }
}

#[derive(Debug)]
struct ChildState {
    // As child decoders are scheduled they are added to this queue
    // Once the decoder is fully drained it is popped from this queue
    //
    // TODO: It may be a minor perf optimization, in some rare cases, if we have a separate
    // "fully awaited but not yet drained" queue so we don't loop through fully awaited pages
    // during each call to wait.
    //
    // Note: This queue may have more than one page in it if the batch size is very large
    // or pages are very small
    // TODO: Test this case
    scheduled: VecDeque<Box<dyn LogicalPageDecoder>>,
    // Rows that have been awaited
    rows_loaded: u64,
    // Rows that have drained
    rows_drained: u64,
    // Rows that have been popped (the decoder has been completely drained and removed from `scheduled`)
    rows_popped: u64,
    // Total number of rows in the struct
    num_rows: u64,
    // The field index in the struct (used for debugging / logging)
    field_index: u32,
}

impl ChildState {
    fn new(num_rows: u64, field_index: u32) -> Self {
        Self {
            scheduled: VecDeque::new(),
            rows_loaded: 0,
            rows_drained: 0,
            rows_popped: 0,
            num_rows,
            field_index,
        }
    }

    // Wait for the next set of rows to arrive
    //
    // Wait until we have at least `loaded_need` loaded and stop as soon as we
    // go above that limit.
    async fn wait_for_loaded(&mut self, loaded_need: u64) -> Result<()> {
        trace!(
            "Struct child {} waiting for more than {} rows to be loaded and {} are fully loaded already",
            self.field_index, loaded_need, self.rows_loaded,
        );
        let mut fully_loaded = self.rows_popped;
        for (page_idx, next_decoder) in self.scheduled.iter_mut().enumerate() {
            if next_decoder.rows_unloaded() > 0 {
                let mut current_need = loaded_need;
                current_need -= fully_loaded;
                let rows_in_page = next_decoder.num_rows();
                let need_for_page = (rows_in_page - 1).min(current_need);
                trace!(
                    "Struct child {} page {} will wait until more than {} rows loaded from page with {} rows",
                    self.field_index, page_idx, need_for_page, rows_in_page,
                );
                // We might only await part of a page.  This is important for things
                // like the struct<struct<...>> case where we have one outer page, one
                // middle page, and then a bunch of inner pages.  If we await the entire
                // middle page then we will have to wait for all the inner pages to arrive
                // before we can start decoding.
                next_decoder.wait_for_loaded(need_for_page).await?;
                let now_loaded = next_decoder.rows_loaded();
                fully_loaded += now_loaded;
                trace!(
                    "Struct child {} page {} await and now has {} loaded rows and we have {} fully loaded",
                    self.field_index, page_idx, now_loaded, fully_loaded
                );
            } else {
                fully_loaded += next_decoder.num_rows();
            }
            if fully_loaded > loaded_need {
                break;
            }
        }
        self.rows_loaded = fully_loaded;
        trace!(
            "Struct child {} loaded {} new rows and now {} are loaded",
            self.field_index, fully_loaded, self.rows_loaded
        );
        Ok(())
    }

    fn drain(&mut self, num_rows: u64) -> Result<CompositeDecodeTask> {
        trace!("Struct draining {} rows", num_rows);

        trace!(
            "Draining {} rows from struct page with {} rows already drained",
            num_rows, self.rows_drained
        );
        let mut remaining = num_rows;
        let mut composite = CompositeDecodeTask {
            tasks: Vec::new(),
            num_rows: 0,
            has_more: true,
        };
        while remaining > 0 {
            let next = self.scheduled.front_mut().unwrap();
            let rows_to_take = remaining.min(next.rows_left());
            let next_task = next.drain(rows_to_take)?;
            if next.rows_left() == 0 {
                trace!("Completely drained page");
                self.rows_popped += next.num_rows();
                self.scheduled.pop_front();
            }
            remaining -= rows_to_take;
            composite.tasks.push(next_task.task);
            composite.num_rows += next_task.num_rows;
        }
        self.rows_drained += num_rows;
        composite.has_more = self.rows_drained != self.num_rows;
        Ok(composite)
    }
}

// Wrapper around ChildState that orders using rows_unawaited
struct WaitOrder<'a>(&'a mut ChildState);

impl Eq for WaitOrder<'_> {}
impl PartialEq for WaitOrder<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.0.rows_loaded == other.0.rows_loaded
    }
}
impl Ord for WaitOrder<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Note: this is inverted so we have a min-heap
        other.0.rows_loaded.cmp(&self.0.rows_loaded)
    }
}
impl PartialOrd for WaitOrder<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct SimpleStructDecoder {
    children: Vec<ChildState>,
    child_fields: Fields,
    data_type: DataType,
    num_rows: u64,
}

impl SimpleStructDecoder {
    pub fn new(child_fields: Fields, num_rows: u64) -> Self {
        let data_type = DataType::Struct(child_fields.clone());
        Self {
            children: child_fields
                .iter()
                .enumerate()
                .map(|(idx, _)| ChildState::new(num_rows, idx as u32))
                .collect(),
            child_fields,
            data_type,
            num_rows,
        }
    }

    async fn do_wait_for_loaded(&mut self, loaded_need: u64) -> Result<()> {
        let mut wait_orders = self
            .children
            .iter_mut()
            .filter_map(|child| {
                if child.rows_loaded <= loaded_need {
                    Some(WaitOrder(child))
                } else {
                    None
                }
            })
            .collect::<BinaryHeap<_>>();
        while !wait_orders.is_empty() {
            let next_waiter = wait_orders.pop().unwrap();
            let next_highest = wait_orders
                .peek()
                .map(|w| w.0.rows_loaded)
                .unwrap_or(u64::MAX);
            // Wait until you have the number of rows needed, or at least more than the
            // next highest waiter
            let limit = loaded_need.min(next_highest);
            next_waiter.0.wait_for_loaded(limit).await?;
            log::trace!(
                "Struct child {} finished await pass and now {} are loaded",
                next_waiter.0.field_index,
                next_waiter.0.rows_loaded
            );
            if next_waiter.0.rows_loaded <= loaded_need {
                wait_orders.push(next_waiter);
            }
        }
        Ok(())
    }
}

impl LogicalPageDecoder for SimpleStructDecoder {
    fn accept_child(&mut self, mut child: DecoderReady) -> Result<()> {
        // children with empty path should not be delivered to this method
        let child_idx = child.path.pop_front().unwrap();
        if child.path.is_empty() {
            // This decoder is intended for us
            self.children[child_idx as usize]
                .scheduled
                .push_back(child.decoder);
        } else {
            // This decoder is intended for one of our children
            let intended = self.children[child_idx as usize].scheduled.back_mut().ok_or_else(|| Error::internal(format!("Decoder scheduled for child at index {} but we don't have any child at that index yet", child_idx)))?;
            intended.accept_child(child)?;
        }
        Ok(())
    }

    fn wait_for_loaded(&mut self, loaded_need: u64) -> BoxFuture<'_, Result<()>> {
        self.do_wait_for_loaded(loaded_need).boxed()
    }

    fn drain(&mut self, num_rows: u64) -> Result<NextDecodeTask> {
        let child_tasks = self
            .children
            .iter_mut()
            .map(|child| child.drain(num_rows))
            .collect::<Result<Vec<_>>>()?;
        let num_rows = child_tasks[0].num_rows;
        debug_assert!(child_tasks.iter().all(|task| task.num_rows == num_rows));
        Ok(NextDecodeTask {
            task: Box::new(SimpleStructDecodeTask {
                children: child_tasks,
                child_fields: self.child_fields.clone(),
            }),
            num_rows,
        })
    }

    fn rows_loaded(&self) -> u64 {
        self.children.iter().map(|c| c.rows_loaded).min().unwrap()
    }

    fn rows_drained(&self) -> u64 {
        // All children should have the same number of rows drained
        debug_assert!(
            self.children
                .iter()
                .all(|c| c.rows_drained == self.children[0].rows_drained)
        );
        self.children[0].rows_drained
    }

    fn num_rows(&self) -> u64 {
        self.num_rows
    }

    fn data_type(&self) -> &DataType {
        &self.data_type
    }
}

struct CompositeDecodeTask {
    // One per child
    tasks: Vec<Box<dyn DecodeArrayTask>>,
    num_rows: u64,
    has_more: bool,
}

impl CompositeDecodeTask {
    fn decode(self) -> Result<ArrayRef> {
        let arrays = self
            .tasks
            .into_iter()
            .map(|task| task.decode().map(|(arr, _)| arr))
            .collect::<Result<Vec<_>>>()?;
        let array_refs = arrays.iter().map(|arr| arr.as_ref()).collect::<Vec<_>>();
        // TODO: If this is a primitive column we should be able to avoid this
        // allocation + copy with "page bridging" which could save us a few CPU
        // cycles.
        //
        // This optimization is probably most important for super fast storage like NVME
        // where the page size can be smaller.
        Ok(arrow_select::concat::concat(&array_refs)?)
    }
}

struct SimpleStructDecodeTask {
    children: Vec<CompositeDecodeTask>,
    child_fields: Fields,
}

impl DecodeArrayTask for SimpleStructDecodeTask {
    fn decode(self: Box<Self>) -> Result<(ArrayRef, u64)> {
        let child_arrays = self
            .children
            .into_iter()
            .map(|child| child.decode())
            .collect::<Result<Vec<_>>>()?;
        // data_size is only tracked in the v2.1 structural decode path; the v2.0 array
        // v2.0 path does not need it so we return 0.
        Ok((
            Arc::new(StructArray::try_new(self.child_fields, child_arrays, None)?),
            0,
        ))
    }
}
