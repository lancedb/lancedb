// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use bytes::Bytes;
use futures::channel::oneshot;
use futures::{FutureExt, TryFutureExt};
use object_store::path::Path;
use std::collections::BinaryHeap;
use std::fmt::Debug;
use std::future::Future;
use std::num::NonZero;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;
use tokio::sync::Notify;

use lance_core::utils::io_stats::IoStatsRecorder;
use lance_core::utils::parse::str_is_truthy;
use lance_core::{Error, Result};

use crate::object_store::ObjectStore;
use crate::traits::Reader;
use crate::utils::CachedFileSize;

mod lite;

// Don't log backpressure warnings until at least this many seconds have passed
const BACKPRESSURE_MIN: u64 = 5;
// Don't log backpressure warnings more than once / minute
const BACKPRESSURE_DEBOUNCE: u64 = 60;
const SCHEDULER_STATE_EVENT_TARGET: &str = "lance_io::scheduler::state";

// Global counter of how many IOPS we have issued
static IOPS_COUNTER: AtomicU64 = AtomicU64::new(0);
// Global counter of how many bytes were read by the scheduler
static BYTES_READ_COUNTER: AtomicU64 = AtomicU64::new(0);

pub fn iops_counter() -> u64 {
    IOPS_COUNTER.load(Ordering::Acquire)
}

pub fn bytes_read_counter() -> u64 {
    BYTES_READ_COUNTER.load(Ordering::Acquire)
}

// We want to allow requests that have a lower priority than any
// currently in-flight request.  This helps avoid potential deadlocks
// related to backpressure.  Unfortunately, it is quite expensive to
// keep track of which priorities are in-flight.
//
// TODO: At some point it would be nice if we can optimize this away but
// in_flight should remain relatively small (generally less than 256 items)
// and has not shown itself to be a bottleneck yet.
struct PrioritiesInFlight {
    in_flight: Vec<u128>,
}

impl PrioritiesInFlight {
    fn new(capacity: u32) -> Self {
        Self {
            in_flight: Vec::with_capacity(capacity as usize * 2),
        }
    }

    fn min_in_flight(&self) -> u128 {
        self.in_flight.first().copied().unwrap_or(u128::MAX)
    }

    fn contains(&self, prio: u128) -> bool {
        self.in_flight.binary_search(&prio).is_ok()
    }

    fn push(&mut self, prio: u128) {
        let pos = match self.in_flight.binary_search(&prio) {
            Ok(pos) => pos,
            Err(pos) => pos,
        };
        self.in_flight.insert(pos, prio);
    }

    fn remove(&mut self, prio: u128) {
        if let Ok(pos) = self.in_flight.binary_search(&prio) {
            self.in_flight.remove(pos);
        }
    }

    fn len(&self) -> usize {
        self.in_flight.len()
    }

    fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }
}

struct IoQueueState {
    // The configured number of IOPS that can be issued concurrently.
    io_capacity: u32,
    // Number of IOPS we can issue concurrently before pausing I/O
    iops_avail: u32,
    // The configured byte budget for unread I/O.
    io_buffer_size: u64,
    // Number of bytes we are allowed to buffer in memory before pausing I/O
    //
    // This can dip below 0 due to I/O prioritization
    bytes_avail: i64,
    // Pending I/O requests
    pending_requests: BinaryHeap<IoTask>,
    // Priorities of in-flight requests
    priorities_in_flight: PrioritiesInFlight,
    // Set when the scheduler is finished to notify the I/O loop to shut down
    // once all outstanding requests have been completed.
    done_scheduling: bool,
    // Time when the scheduler started
    start: Instant,
    // Last time we warned about backpressure
    last_warn: AtomicU64,
    // When true, skip all byte-based backpressure checks (set when io_buffer_size == 0)
    no_backpressure: bool,
}

impl IoQueueState {
    fn new(io_capacity: u32, io_buffer_size: u64) -> Self {
        Self {
            io_capacity,
            iops_avail: io_capacity,
            io_buffer_size,
            bytes_avail: io_buffer_size as i64,
            pending_requests: BinaryHeap::new(),
            priorities_in_flight: PrioritiesInFlight::new(io_capacity),
            done_scheduling: false,
            start: Instant::now(),
            last_warn: AtomicU64::from(0),
            no_backpressure: io_buffer_size == 0,
        }
    }

    fn scheduler_state_event(&self) -> Option<SchedulerStateEvent> {
        if !tracing::enabled!(target: SCHEDULER_STATE_EVENT_TARGET, tracing::Level::TRACE) {
            return None;
        }

        let pending_bytes = self
            .pending_requests
            .iter()
            .map(IoTask::num_bytes)
            .sum::<u64>();
        let head_task = self.pending_requests.peek();
        let min_in_flight_priority = if self.priorities_in_flight.is_empty() {
            None
        } else {
            Some(self.priorities_in_flight.min_in_flight())
        };
        let head_task_priority_bypass = head_task.map(|task| {
            self.no_backpressure
                || task.bypass_backpressure
                || task.priority <= self.priorities_in_flight.min_in_flight()
        });
        let head_task_blocked_by_iops = head_task.map(|_| self.iops_avail == 0);
        let head_task_blocked_by_bytes = head_task.map(|task| {
            let bypasses_bytes = self.no_backpressure
                || task.bypass_backpressure
                || task.priority <= self.priorities_in_flight.min_in_flight();
            !bypasses_bytes && task.num_bytes() as i64 > self.bytes_avail
        });
        let head_task_can_deliver = head_task.map(|task| self.can_deliver_without_warning(task));
        let head_task_bytes = head_task.map(IoTask::num_bytes);
        let (head_task_priority_high, head_task_priority_low) =
            split_priority(head_task.map(|task| task.priority));
        let (min_in_flight_priority_high, min_in_flight_priority_low) =
            split_priority(min_in_flight_priority);

        Some(SchedulerStateEvent {
            queue_kind: "standard",
            io_capacity: u64::from(self.io_capacity),
            iops_available: u64::from(self.iops_avail),
            active_iops: u64::from(self.io_capacity.saturating_sub(self.iops_avail)),
            pending_iops: self.pending_requests.len() as u64,
            pending_bytes,
            bytes_available: self.bytes_avail,
            bytes_reserved: self.io_buffer_size as i64 - self.bytes_avail,
            io_buffer_size_bytes: self.io_buffer_size,
            priorities_in_flight: self.priorities_in_flight.len() as u64,
            no_backpressure: self.no_backpressure,
            head_task_bytes,
            head_task_priority_high,
            head_task_priority_low,
            min_in_flight_priority_high,
            min_in_flight_priority_low,
            head_task_can_deliver,
            head_task_priority_bypass,
            head_task_blocked_by_iops,
            head_task_blocked_by_bytes,
        })
    }

    fn warn_if_needed(&self) {
        let seconds_elapsed = self.start.elapsed().as_secs();
        let last_warn = self.last_warn.load(Ordering::Acquire);
        let since_last_warn = seconds_elapsed - last_warn;
        if (last_warn == 0
            && seconds_elapsed > BACKPRESSURE_MIN
            && seconds_elapsed < BACKPRESSURE_DEBOUNCE)
            || since_last_warn > BACKPRESSURE_DEBOUNCE
        {
            tracing::event!(tracing::Level::DEBUG, "Backpressure throttle exceeded");
            log::debug!(
                "Backpressure throttle is full, I/O will pause until buffer is drained.  Max I/O bandwidth will not be achieved because CPU is falling behind"
            );
            self.last_warn
                .store(seconds_elapsed.max(1), Ordering::Release);
        }
    }

    fn can_deliver(&self, task: &IoTask) -> bool {
        let can_deliver = self.can_deliver_without_warning(task);
        if !can_deliver
            && self.iops_avail > 0
            && !(self.no_backpressure
                || task.bypass_backpressure
                || task.priority <= self.priorities_in_flight.min_in_flight())
            && task.num_bytes() as i64 > self.bytes_avail
        {
            self.warn_if_needed();
        }
        can_deliver
    }

    fn can_deliver_without_warning(&self, task: &IoTask) -> bool {
        if self.iops_avail == 0 {
            false
        } else if self.no_backpressure
            || task.bypass_backpressure
            || task.priority <= self.priorities_in_flight.min_in_flight()
            // Chunks from an admitted logical request must keep moving.  A
            // higher-priority request may be scheduled later and remain
            // unconsumed while the caller awaits this request.
            || self.priorities_in_flight.contains(task.priority)
        {
            true
        } else {
            task.num_bytes() as i64 <= self.bytes_avail
        }
    }

    fn next_task(&mut self) -> Option<IoTask> {
        let task = self.pending_requests.peek()?;
        if self.can_deliver(task) {
            let skip_bytes_accounting = self.no_backpressure || task.bypass_backpressure;
            self.priorities_in_flight.push(task.priority);
            self.iops_avail -= 1;
            if !skip_bytes_accounting {
                self.bytes_avail -= task.num_bytes() as i64;
                if self.bytes_avail < 0 {
                    // This can happen when we admit special priority requests
                    log::debug!(
                        "Backpressure throttle temporarily exceeded by {} bytes due to priority I/O",
                        -self.bytes_avail
                    );
                }
            }
            Some(self.pending_requests.pop().unwrap())
        } else {
            None
        }
    }
}

// This is modeled after the MPSC queue described here: https://docs.rs/tokio/latest/tokio/sync/struct.Notify.html
//
// However, it only needs to be SPSC since there is only one "scheduler thread"
// and one I/O loop.
struct IoQueue {
    // Queue state
    state: Mutex<IoQueueState>,
    // Used to signal new I/O requests have arrived that might potentially be runnable
    notify: Notify,
    stats: IoStats,
}

impl IoQueue {
    fn new(io_capacity: u32, io_buffer_size: u64, stats: IoStats) -> Self {
        Self {
            state: Mutex::new(IoQueueState::new(io_capacity, io_buffer_size)),
            notify: Notify::new(),
            stats,
        }
    }

    fn push(&self, task: IoTask) {
        log::trace!(
            "Inserting I/O request for {} bytes with priority ({},{}) into I/O queue",
            task.num_bytes(),
            task.priority >> 64,
            task.priority & 0xFFFFFFFFFFFFFFFF
        );
        let event = {
            let mut state = self.state.lock().unwrap();
            state.pending_requests.push(task);
            state.scheduler_state_event()
        };
        emit_scheduler_state_event(event, &self.stats);

        self.notify.notify_one();
    }

    async fn pop(&self) -> Option<IoTask> {
        loop {
            {
                let mut state = self.state.lock().unwrap();
                if let Some(task) = state.next_task() {
                    let event = state.scheduler_state_event();
                    drop(state);
                    emit_scheduler_state_event(event, &self.stats);
                    return Some(task);
                }

                if state.done_scheduling {
                    return None;
                }
            }

            self.notify.notified().await;
        }
    }

    fn on_iop_complete(&self) {
        let event = {
            let mut state = self.state.lock().unwrap();
            state.iops_avail += 1;
            state.scheduler_state_event()
        };
        emit_scheduler_state_event(event, &self.stats);

        self.notify.notify_one();
    }

    fn on_bytes_consumed(&self, bytes: u64, priority: u128, num_reqs: usize) {
        let event = {
            let mut state = self.state.lock().unwrap();
            state.bytes_avail += bytes as i64;
            for _ in 0..num_reqs {
                state.priorities_in_flight.remove(priority);
            }
            state.scheduler_state_event()
        };
        emit_scheduler_state_event(event, &self.stats);

        self.notify.notify_one();
    }

    fn close(&self) {
        let (pending_requests, event) = {
            let mut state = self.state.lock().unwrap();
            state.done_scheduling = true;
            let pending_requests = std::mem::take(&mut state.pending_requests);
            let event = state.scheduler_state_event();
            (pending_requests, event)
        };
        emit_scheduler_state_event(event, &self.stats);
        for request in pending_requests {
            request.cancel();
        }

        self.notify.notify_one();
    }
}

// There is one instance of MutableBatch shared by all the I/O operations
// that make up a single request.  When all the I/O operations complete
// then the MutableBatch goes out of scope and the batch request is considered
// complete
struct MutableBatch<F: FnOnce(Response) + Send> {
    when_done: Option<F>,
    data_buffers: Vec<Bytes>,
    num_bytes: u64,
    priority: u128,
    num_reqs: usize,
    num_delivered: usize,
    err: Option<Error>,
    // When true, report 0 bytes consumed so the backpressure budget is unaffected
    bypass_backpressure: bool,
    // Queue the batch's backpressure reservation is refunded to once its response
    // is delivered or discarded (see `Response`'s `Drop`).
    io_queue: Arc<IoQueue>,
}

impl<F: FnOnce(Response) + Send> MutableBatch<F> {
    fn new(
        when_done: F,
        num_data_buffers: u32,
        priority: u128,
        num_reqs: usize,
        bypass_backpressure: bool,
        io_queue: Arc<IoQueue>,
    ) -> Self {
        Self {
            when_done: Some(when_done),
            data_buffers: vec![Bytes::default(); num_data_buffers as usize],
            num_bytes: 0,
            priority,
            num_reqs,
            num_delivered: 0,
            err: None,
            bypass_backpressure,
            io_queue,
        }
    }
}

// Rather than keep track of when all the I/O requests are finished so that we
// can deliver the batch of data we let Rust do that for us.  When all I/O's are
// done then the MutableBatch will go out of scope and we know we have all the
// data.
impl<F: FnOnce(Response) + Send> Drop for MutableBatch<F> {
    fn drop(&mut self) {
        // If we have an error, return that. Otherwise return the data, as long as the I/O requests have been processed.
        let result = if let Some(err) = self.err.take() {
            Err(err)
        } else if self.num_delivered < self.data_buffers.len() {
            // This usually happens on tokio runtime shutdown
            Err(Error::io(format!(
                "I/O request was dropped before completion ({} of {} reads delivered)",
                self.num_delivered,
                self.data_buffers.len()
            )))
        } else {
            let mut data = Vec::new();
            std::mem::swap(&mut data, &mut self.data_buffers);
            Ok(data)
        };
        // We don't really care if no one is around to receive it, just let
        // the result go out of scope and get cleaned up
        let response = Response {
            data: Some(result),
            io_queue: self.io_queue.clone(),
            // Report 0 bytes for bypass tasks so the backpressure budget is unaffected
            num_bytes: if self.bypass_backpressure {
                0
            } else {
                self.num_bytes
            },
            priority: self.priority,
            num_reqs: self.num_reqs,
        };
        (self.when_done.take().unwrap())(response);
    }
}

struct DataChunk {
    task_idx: usize,
    num_bytes: u64,
    data: Result<Bytes>,
}

trait DataSink: Send {
    fn deliver_data(&mut self, data: DataChunk);
}

impl<F: FnOnce(Response) + Send> DataSink for MutableBatch<F> {
    // Called by worker tasks to add data to the MutableBatch
    fn deliver_data(&mut self, data: DataChunk) {
        self.num_bytes += data.num_bytes;
        self.num_delivered += 1;
        match data.data {
            Ok(data_bytes) => {
                self.data_buffers[data.task_idx] = data_bytes;
            }
            Err(err) => {
                // This keeps the original error, if present
                self.err.get_or_insert(err);
            }
        }
    }
}

struct IoTask {
    reader: Arc<dyn Reader>,
    to_read: Range<u64>,
    when_done: Box<dyn FnOnce(Result<Bytes>) + Send>,
    priority: u128,
    bypass_backpressure: bool,
}

fn validate_read_length(
    file_path: &Path,
    requested_range: &Range<u64>,
    bytes: Bytes,
) -> Result<Bytes> {
    let expected_len = requested_range.end - requested_range.start;
    if bytes.len() as u64 != expected_len {
        return Err(Error::io(format!(
            "I/O request for file {file_path} and range {}..{} returned {} bytes, expected {expected_len} bytes",
            requested_range.start,
            requested_range.end,
            bytes.len()
        )));
    }
    Ok(bytes)
}

impl Eq for IoTask {}

impl PartialEq for IoTask {
    fn eq(&self, other: &Self) -> bool {
        self.bypass_backpressure == other.bypass_backpressure && self.priority == other.priority
    }
}

impl PartialOrd for IoTask {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for IoTask {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Bypass tasks are always delivered before normal tasks.
        // Within the same bypass class, this is a min-heap on priority.
        self.bypass_backpressure
            .cmp(&other.bypass_backpressure)
            .then(other.priority.cmp(&self.priority))
    }
}

impl IoTask {
    fn num_bytes(&self) -> u64 {
        self.to_read.end - self.to_read.start
    }
    fn cancel(self) {
        (self.when_done)(Err(Error::internal(
            "Scheduler closed before I/O was completed".to_string(),
        )));
    }

    async fn run(self) {
        let file_path = self.reader.path().as_ref();
        let num_bytes = self.num_bytes();
        let bytes = if self.to_read.start == self.to_read.end {
            Ok(Bytes::new())
        } else {
            let bytes_fut = self
                .reader
                .get_range(self.to_read.start as usize..self.to_read.end as usize);
            IOPS_COUNTER.fetch_add(1, Ordering::Release);
            let num_bytes = self.num_bytes();
            bytes_fut
                .inspect(move |_| {
                    BYTES_READ_COUNTER.fetch_add(num_bytes, Ordering::Release);
                })
                .await
                .map_err(Error::from)
                .and_then(|bytes| validate_read_length(self.reader.path(), &self.to_read, bytes))
        };
        // Emit per-file I/O trace event only when tracing is enabled
        tracing::trace!(
            file = file_path,
            bytes_read = num_bytes,
            requests = 1,
            range_start = self.to_read.start,
            range_end = self.to_read.end,
            "File I/O completed"
        );
        (self.when_done)(bytes);
    }
}

// Every time a scheduler starts up it launches a task to run the I/O loop.  This loop
// repeats endlessly until the scheduler is destroyed.
async fn run_io_loop(tasks: Arc<IoQueue>) {
    // Pop the first finished task off the queue and submit another until
    // we are done
    loop {
        let next_task = tasks.pop().await;
        match next_task {
            Some(task) => {
                tokio::spawn(task.run());
            }
            None => {
                // The sender has been dropped, we are done
                return;
            }
        }
    }
}

#[derive(Debug)]
struct StatsCollector {
    iops: AtomicU64,
    requests: AtomicU64,
    bytes_read: AtomicU64,
}

impl StatsCollector {
    fn new() -> Self {
        Self {
            iops: AtomicU64::new(0),
            requests: AtomicU64::new(0),
            bytes_read: AtomicU64::new(0),
        }
    }

    fn iops(&self) -> u64 {
        self.iops.load(Ordering::Relaxed)
    }

    fn bytes_read(&self) -> u64 {
        self.bytes_read.load(Ordering::Relaxed)
    }

    fn requests(&self) -> u64 {
        self.requests.load(Ordering::Relaxed)
    }

    fn record_request(&self, request: &[Range<u64>]) {
        self.requests.fetch_add(1, Ordering::Relaxed);
        self.iops.fetch_add(request.len() as u64, Ordering::Relaxed);
        self.bytes_read.fetch_add(
            request.iter().map(|r| r.end - r.start).sum::<u64>(),
            Ordering::Relaxed,
        );
    }

    /// Add already-aggregated counts (e.g. a snapshot captured from another
    /// scheduler) into these counters.
    fn add(&self, iops: u64, requests: u64, bytes_read: u64) {
        self.iops.fetch_add(iops, Ordering::Relaxed);
        self.requests.fetch_add(requests, Ordering::Relaxed);
        self.bytes_read.fetch_add(bytes_read, Ordering::Relaxed);
    }
}

impl IoStatsRecorder for StatsCollector {
    fn record_request(&self, request: &[Range<u64>]) {
        // Inherent methods take precedence in resolution, so this delegates to
        // the inherent `record_request` above rather than recursing.
        Self::record_request(self, request)
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct ScanStats {
    pub iops: u64,
    pub requests: u64,
    pub bytes_read: u64,
}

impl ScanStats {
    fn new(stats: &StatsCollector) -> Self {
        Self {
            iops: stats.iops(),
            requests: stats.requests(),
            bytes_read: stats.bytes_read(),
        }
    }
}

fn split_priority(priority: Option<u128>) -> (Option<u64>, Option<u64>) {
    priority
        .map(|priority| ((priority >> 64) as u64, priority as u64))
        .unzip()
}

#[derive(Debug, Clone, Copy)]
pub(super) struct SchedulerStateEvent {
    pub(super) queue_kind: &'static str,
    pub(super) io_capacity: u64,
    pub(super) iops_available: u64,
    pub(super) active_iops: u64,
    pub(super) pending_iops: u64,
    pub(super) pending_bytes: u64,
    pub(super) bytes_available: i64,
    pub(super) bytes_reserved: i64,
    pub(super) io_buffer_size_bytes: u64,
    pub(super) priorities_in_flight: u64,
    pub(super) no_backpressure: bool,
    pub(super) head_task_bytes: Option<u64>,
    pub(super) head_task_priority_high: Option<u64>,
    pub(super) head_task_priority_low: Option<u64>,
    pub(super) min_in_flight_priority_high: Option<u64>,
    pub(super) min_in_flight_priority_low: Option<u64>,
    pub(super) head_task_can_deliver: Option<bool>,
    pub(super) head_task_priority_bypass: Option<bool>,
    pub(super) head_task_blocked_by_iops: Option<bool>,
    pub(super) head_task_blocked_by_bytes: Option<bool>,
}

impl SchedulerStateEvent {
    fn trace(self, stats: ScanStats) {
        tracing::event!(
            target: SCHEDULER_STATE_EVENT_TARGET,
            tracing::Level::TRACE,
            queue_kind = self.queue_kind,
            scheduler_iops = stats.iops,
            scheduler_requests = stats.requests,
            scheduler_bytes_read = stats.bytes_read,
            io_capacity = self.io_capacity,
            iops_available = self.iops_available,
            active_iops = self.active_iops,
            pending_iops = self.pending_iops,
            pending_bytes = self.pending_bytes,
            bytes_available = self.bytes_available,
            bytes_reserved = self.bytes_reserved,
            io_buffer_size_bytes = self.io_buffer_size_bytes,
            priorities_in_flight = self.priorities_in_flight,
            no_backpressure = self.no_backpressure,
            head_task_bytes_present = self.head_task_bytes.is_some(),
            head_task_bytes = self.head_task_bytes.unwrap_or_default(),
            head_task_priority_high_present = self.head_task_priority_high.is_some(),
            head_task_priority_high = self.head_task_priority_high.unwrap_or_default(),
            head_task_priority_low_present = self.head_task_priority_low.is_some(),
            head_task_priority_low = self.head_task_priority_low.unwrap_or_default(),
            min_in_flight_priority_high_present = self.min_in_flight_priority_high.is_some(),
            min_in_flight_priority_high = self.min_in_flight_priority_high.unwrap_or_default(),
            min_in_flight_priority_low_present = self.min_in_flight_priority_low.is_some(),
            min_in_flight_priority_low = self.min_in_flight_priority_low.unwrap_or_default(),
            head_task_can_deliver_present = self.head_task_can_deliver.is_some(),
            head_task_can_deliver = self.head_task_can_deliver.unwrap_or(false),
            head_task_priority_bypass_present = self.head_task_priority_bypass.is_some(),
            head_task_priority_bypass = self.head_task_priority_bypass.unwrap_or(false),
            head_task_blocked_by_iops_present = self.head_task_blocked_by_iops.is_some(),
            head_task_blocked_by_iops = self.head_task_blocked_by_iops.unwrap_or(false),
            head_task_blocked_by_bytes_present = self.head_task_blocked_by_bytes.is_some(),
            head_task_blocked_by_bytes = self.head_task_blocked_by_bytes.unwrap_or(false),
            "Scheduler state"
        );
    }
}

pub(super) fn emit_scheduler_state_event(event: Option<SchedulerStateEvent>, stats: &IoStats) {
    if let Some(event) = event {
        event.trace(stats.snapshot());
    }
}

/// A shareable, cloneable handle to a set of cumulative I/O counters.
///
/// All clones share the same underlying counters.  This serves two purposes:
///
/// 1. It backs each [`ScanScheduler`]'s own running totals.
/// 2. It can be attached to an individual [`FileScheduler`] (via
///    [`FileScheduler::with_io_stats`]) as a *secondary* sink, so a caller can
///    measure the exact bytes/IOPS performed through that file handle for a
///    bounded scope (e.g. a single query) without disturbing the scheduler's
///    global totals.  Read the result back with [`IoStats::snapshot`].
#[derive(Debug, Clone)]
pub struct IoStats(Arc<StatsCollector>);

impl IoStats {
    pub fn new() -> Self {
        Self(Arc::new(StatsCollector::new()))
    }

    /// Record a single completed request.  `request` holds the byte ranges as
    /// actually submitted to storage (post coalescing/splitting), so the counts
    /// reflect physical I/O.
    pub fn record_request(&self, request: &[Range<u64>]) {
        self.0.record_request(request);
    }

    /// Take an immutable snapshot of the current cumulative counters.
    pub fn snapshot(&self) -> ScanStats {
        ScanStats::new(self.0.as_ref())
    }

    /// Return this handle as a type-erased [`IoStatsRecorder`], suitable for
    /// attaching to a file reader (e.g. `FileReader::with_io_stats`).  The
    /// returned recorder shares the same underlying counters as `self`.
    pub fn recorder(&self) -> Arc<dyn IoStatsRecorder> {
        self.0.clone()
    }

    /// Add a snapshot of already-aggregated statistics into this sink.  Used to
    /// fold in I/O measured on a separate scheduler (e.g. the one-time reads
    /// performed while opening an index).
    pub fn add_scan_stats(&self, stats: &ScanStats) {
        self.0.add(stats.iops, stats.requests, stats.bytes_read);
    }
}

impl Default for IoStats {
    fn default() -> Self {
        Self::new()
    }
}

enum IoQueueType {
    Standard(Arc<IoQueue>),
    Lite(Arc<lite::IoQueue>),
}

/// An I/O scheduler which wraps an ObjectStore and throttles the amount of
/// parallel I/O that can be run.
///
/// The ScanScheduler will cancel any outstanding I/O requests when it is dropped.
/// For this reason it should be kept alive until all I/O has finished.
///
/// Note: The 2.X file readers already do this so this is only a concern if you are
/// using the ScanScheduler directly.
pub struct ScanScheduler {
    object_store: Arc<ObjectStore>,
    io_queue: IoQueueType,
    stats: IoStats,
}

impl Debug for ScanScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScanScheduler")
            .field("object_store", &self.object_store)
            .finish()
    }
}

struct Response {
    // `Option` so the caller can take the data out while the response (and its
    // backpressure refund on drop) stays intact.
    data: Option<Result<Vec<Bytes>>>,
    io_queue: Arc<IoQueue>,
    priority: u128,
    num_reqs: usize,
    num_bytes: u64,
}

// Refund the batch's backpressure reservation when the response is dropped, be
// that on delivery or when a cancelled request's undelivered response is
// discarded.  This releases the budget even if the caller drops the future early.
impl Drop for Response {
    fn drop(&mut self) {
        self.io_queue
            .on_bytes_consumed(self.num_bytes, self.priority, self.num_reqs);
    }
}

#[derive(Debug, Clone, Copy)]
pub struct SchedulerConfig {
    /// the # of bytes that can be buffered but not yet requested.
    /// This controls back pressure.  If data is not processed quickly enough then this
    /// buffer will fill up and the I/O loop will pause until the buffer is drained.
    pub io_buffer_size_bytes: u64,
    /// Whether to use the lite scheduler.
    ///
    /// - `Some(true)` forces the lite scheduler (e.g. from env var or programmatic).
    /// - `Some(false)` forces the standard scheduler.
    /// - `None` defers to the object store's preference (see [`ObjectStore::prefers_lite_scheduler`]).
    pub use_lite_scheduler: Option<bool>,
}

impl SchedulerConfig {
    pub fn new(io_buffer_size_bytes: u64) -> Self {
        Self {
            io_buffer_size_bytes,
            use_lite_scheduler: std::env::var("LANCE_USE_LITE_SCHEDULER")
                .ok()
                .map(|v| str_is_truthy(v.trim())),
        }
    }

    /// Big enough for unit testing
    pub fn default_for_testing() -> Self {
        Self {
            io_buffer_size_bytes: 256 * 1024 * 1024,
            use_lite_scheduler: None,
        }
    }

    /// Configuration that should generally maximize bandwidth (not trying to save RAM
    /// at all).  We assume a max page size of 32MiB and then allow 32MiB per I/O thread
    pub fn max_bandwidth(store: &ObjectStore) -> Self {
        Self::new(32 * 1024 * 1024 * store.io_parallelism() as u64)
    }

    pub fn with_lite_scheduler(self) -> Self {
        Self {
            use_lite_scheduler: Some(true),
            ..self
        }
    }
}

impl ScanScheduler {
    /// Create a new scheduler with the given I/O capacity
    ///
    /// # Arguments
    ///
    /// * object_store - the store to wrap
    /// * config - configuration settings for the scheduler
    pub fn new(object_store: Arc<ObjectStore>, config: SchedulerConfig) -> Arc<Self> {
        let io_capacity = object_store.io_parallelism();
        let stats = IoStats::new();
        let use_lite = config
            .use_lite_scheduler
            .unwrap_or_else(|| object_store.prefers_lite_scheduler());
        let io_queue = if use_lite {
            let io_queue = Arc::new(lite::IoQueue::new(
                io_capacity as u64,
                config.io_buffer_size_bytes,
                stats.clone(),
            ));
            IoQueueType::Lite(io_queue)
        } else {
            let io_queue = Arc::new(IoQueue::new(
                io_capacity as u32,
                config.io_buffer_size_bytes,
                stats.clone(),
            ));
            let io_queue_clone = io_queue.clone();
            // Best we can do here is fire and forget.  If the I/O loop is still running when the scheduler is
            // dropped we can't wait for it to finish or we'd block a tokio thread.  We could spawn a blocking task
            // to wait for it to finish but that doesn't seem helpful.
            tokio::task::spawn(async move { run_io_loop(io_queue_clone).await });
            IoQueueType::Standard(io_queue)
        };
        Arc::new(Self {
            object_store,
            io_queue,
            stats,
        })
    }

    /// Open a file for reading
    ///
    /// # Arguments
    ///
    /// * path - the path to the file to open
    /// * base_priority - the base priority for I/O requests submitted to this file scheduler
    ///   this will determine the upper 64 bits of priority (the lower 64 bits
    ///   come from `submit_request` and `submit_single`)
    pub async fn open_file_with_priority(
        self: &Arc<Self>,
        path: &Path,
        base_priority: u64,
        file_size_bytes: &CachedFileSize,
    ) -> Result<FileScheduler> {
        let file_size_bytes = if let Some(size) = file_size_bytes.get() {
            u64::from(size)
        } else {
            let size = self.object_store.size(path).await?;
            if let Some(size) = NonZero::new(size) {
                file_size_bytes.set(size);
            }
            size
        };
        let reader = self
            .object_store
            .open_with_size(path, file_size_bytes as usize)
            .await?;
        let block_size = self.object_store.block_size() as u64;
        let max_iop_size = self.object_store.max_iop_size();
        Ok(FileScheduler {
            reader: reader.into(),
            block_size,
            root: self.clone(),
            base_priority,
            max_iop_size,
            bypass_backpressure: false,
            extra_stats: None,
        })
    }

    /// Open a file with a default priority of 0
    ///
    /// See [`Self::open_file_with_priority`] for more information on the priority
    pub async fn open_file(
        self: &Arc<Self>,
        path: &Path,
        file_size_bytes: &CachedFileSize,
    ) -> Result<FileScheduler> {
        self.open_file_with_priority(path, 0, file_size_bytes).await
    }

    /// Open a [`FileScheduler`] over an already-open [`Reader`].
    ///
    /// Unlike [`Self::open_file`], this skips the path lookup and size probe and
    /// schedules I/O against `reader` directly. This is useful when the reader
    /// was produced outside the scheduler's object store (e.g. a spill file
    /// opened via [`crate::spill::Spill::reader`]), since a bare `Reader`
    /// cannot otherwise drive a v2 `FileReader` (which needs a scheduler).
    ///
    /// Uses a base priority of 0; chain [`FileScheduler::with_priority`] to set
    /// a different one.
    pub fn open_reader(self: &Arc<Self>, reader: Arc<dyn Reader>) -> FileScheduler {
        FileScheduler {
            reader,
            block_size: self.object_store.block_size() as u64,
            root: self.clone(),
            base_priority: 0,
            max_iop_size: self.object_store.max_iop_size(),
            bypass_backpressure: false,
            extra_stats: None,
        }
    }

    fn do_submit_request(
        &self,
        reader: Arc<dyn Reader>,
        request: Vec<Range<u64>>,
        tx: oneshot::Sender<Response>,
        priority: u128,
        io_queue: &Arc<IoQueue>,
        bypass_backpressure: bool,
    ) {
        let num_iops = request.len() as u32;

        let when_all_io_done = move |bytes_and_permits| {
            // We don't care if the receiver has given up so discard the result
            let _ = tx.send(bytes_and_permits);
        };

        let dest = Arc::new(Mutex::new(Box::new(MutableBatch::new(
            when_all_io_done,
            num_iops,
            priority,
            request.len(),
            bypass_backpressure,
            io_queue.clone(),
        ))));

        for (task_idx, iop) in request.into_iter().enumerate() {
            let dest = dest.clone();
            let io_queue_clone = io_queue.clone();
            let num_bytes = iop.end - iop.start;
            let task = IoTask {
                reader: reader.clone(),
                to_read: iop,
                priority,
                bypass_backpressure,
                when_done: Box::new(move |data| {
                    io_queue_clone.on_iop_complete();
                    let mut dest = dest.lock().unwrap();
                    let chunk = DataChunk {
                        data,
                        task_idx,
                        num_bytes,
                    };
                    dest.deliver_data(chunk);
                }),
            };
            io_queue.push(task);
        }
    }

    fn submit_request_standard(
        &self,
        reader: Arc<dyn Reader>,
        request: Vec<Range<u64>>,
        priority: u128,
        io_queue: &Arc<IoQueue>,
        bypass_backpressure: bool,
    ) -> impl Future<Output = Result<Vec<Bytes>>> + Send + use<> {
        let (tx, rx) = oneshot::channel::<Response>();

        self.do_submit_request(reader, request, tx, priority, io_queue, bypass_backpressure);

        rx.map(|wrapped_rsp| {
            // A cancel error can't occur: the sender always sends before dropping.
            // The reservation is refunded on `Response` drop, so just take the data.
            let mut rsp = wrapped_rsp.unwrap();
            rsp.data.take().unwrap()
        })
    }

    fn submit_request_lite(
        &self,
        reader: Arc<dyn Reader>,
        request: Vec<Range<u64>>,
        priority: u128,
        io_queue: &Arc<lite::IoQueue>,
        bypass_backpressure: bool,
    ) -> impl Future<Output = Result<Vec<Bytes>>> + Send + use<> {
        // It's important that we submit all requests _before_ we await anything
        let maybe_tasks = request
            .into_iter()
            .map(|task| {
                let reader = reader.clone();
                let queue = io_queue.clone();
                let requested_range = task.clone();
                let run_fn = Box::new(move || {
                    let bytes_fut = reader
                        .get_range(requested_range.start as usize..requested_range.end as usize);
                    async move {
                        let bytes = bytes_fut.await.map_err(Error::from)?;
                        validate_read_length(reader.path(), &requested_range, bytes)
                    }
                    .boxed()
                });
                queue.submit(task, priority, run_fn, bypass_backpressure)
            })
            .collect::<Result<Vec<_>>>();
        match maybe_tasks {
            Ok(tasks) => async move {
                let mut results = Vec::with_capacity(tasks.len());
                for task in tasks {
                    results.push(task.await?);
                }
                Ok(results)
            }
            .boxed(),
            Err(e) => async move { Err(e) }.boxed(),
        }
    }

    pub fn submit_request(
        &self,
        reader: Arc<dyn Reader>,
        request: Vec<Range<u64>>,
        priority: u128,
        bypass_backpressure: bool,
    ) -> impl Future<Output = Result<Vec<Bytes>>> + Send + use<> {
        match &self.io_queue {
            IoQueueType::Standard(io_queue) => {
                futures::future::Either::Left(self.submit_request_standard(
                    reader,
                    request,
                    priority,
                    io_queue,
                    bypass_backpressure,
                ))
            }
            IoQueueType::Lite(io_queue) => futures::future::Either::Right(
                self.submit_request_lite(reader, request, priority, io_queue, bypass_backpressure),
            ),
        }
    }

    pub fn stats(&self) -> ScanStats {
        self.stats.snapshot()
    }

    #[cfg(test)]
    fn uses_lite_scheduler(&self) -> bool {
        matches!(self.io_queue, IoQueueType::Lite(_))
    }
}

impl Drop for ScanScheduler {
    fn drop(&mut self) {
        // If the user is dropping the ScanScheduler then they _should_ be done with I/O.  This can happen
        // even when I/O is in progress if, for example, the user is dropping a scan mid-read because they found
        // the data they wanted (limit after filter or some other example).
        //
        // Closing the I/O queue will cancel any requests that have not yet been sent to the I/O loop.  However,
        // it will not terminate the I/O loop itself.  This is to help prevent deadlock and ensure that all I/O
        // requests that are submitted will terminate.
        //
        // In theory, this isn't strictly necessary, as callers should drop any task expecting I/O before they
        // drop the scheduler.  In practice, this can be difficult to do, and it is better to spend a little bit
        // of time letting the I/O loop drain so that we can avoid any potential deadlocks.
        match &self.io_queue {
            IoQueueType::Standard(io_queue) => io_queue.close(),
            IoQueueType::Lite(io_queue) => io_queue.close(),
        }
    }
}

/// A throttled file reader
#[derive(Clone, Debug)]
pub struct FileScheduler {
    reader: Arc<dyn Reader>,
    root: Arc<ScanScheduler>,
    block_size: u64,
    base_priority: u64,
    max_iop_size: u64,
    bypass_backpressure: bool,
    /// Optional secondary statistics sink.  When set, every request submitted
    /// through this handle is also recorded here, in addition to the
    /// scheduler's global totals.  Used to measure per-scope I/O.
    extra_stats: Option<Arc<dyn IoStatsRecorder>>,
}

fn is_close_together(range1: &Range<u64>, range2: &Range<u64>, block_size: u64) -> bool {
    // Note that range1.end <= range2.start is possible (e.g. when decoding string arrays)
    range2.start <= (range1.end + block_size)
}

fn is_overlapping(range1: &Range<u64>, range2: &Range<u64>) -> bool {
    range1.start < range2.end && range2.start < range1.end
}

impl FileScheduler {
    /// Submit a batch of I/O requests to the reader
    ///
    /// The requests will be queued in a FIFO manner and, when all requests
    /// have been fulfilled, the returned future will be completed.
    ///
    /// Each request has a given priority.  If the I/O loop is full then requests
    /// will be buffered and requests with the *lowest* priority will be released
    /// from the buffer first.
    ///
    /// Each request has a backpressure ID which controls which backpressure throttle
    /// is applied to the request.  Requests made to the same backpressure throttle
    /// will be throttled together.
    pub fn submit_request(
        &self,
        request: Vec<Range<u64>>,
        priority: u64,
    ) -> impl Future<Output = Result<Vec<Bytes>>> + Send + use<> {
        // The final priority is a combination of the row offset and the file number
        let priority = ((self.base_priority as u128) << 64) + priority as u128;

        let mut merged_requests = Vec::with_capacity(request.len());

        if !request.is_empty() {
            let mut curr_interval = request[0].clone();

            for req in request.iter().skip(1) {
                if is_close_together(&curr_interval, req, self.block_size) {
                    curr_interval.end = curr_interval.end.max(req.end);
                } else {
                    merged_requests.push(curr_interval);
                    curr_interval = req.clone();
                }
            }

            merged_requests.push(curr_interval);
        }

        let mut updated_requests = Vec::with_capacity(merged_requests.len());
        for req in merged_requests {
            if req.is_empty() {
                updated_requests.push(req);
            } else {
                let num_requests = (req.end - req.start).div_ceil(self.max_iop_size);
                let bytes_per_request = (req.end - req.start) / num_requests;
                for i in 0..num_requests {
                    let start = req.start + i * bytes_per_request;
                    let end = if i == num_requests - 1 {
                        // Last request is a bit bigger due to rounding
                        req.end
                    } else {
                        start + bytes_per_request
                    };
                    updated_requests.push(start..end);
                }
            }
        }

        self.root.stats.record_request(&updated_requests);
        if let Some(extra_stats) = &self.extra_stats {
            extra_stats.record_request(&updated_requests);
        }

        let bytes_vec_fut = self.root.submit_request(
            self.reader.clone(),
            updated_requests.clone(),
            priority,
            self.bypass_backpressure,
        );

        let mut updated_index = 0;
        let mut final_bytes = Vec::with_capacity(request.len());

        async move {
            let bytes_vec = bytes_vec_fut.await?;

            let mut orig_index = 0;
            while (updated_index < updated_requests.len()) && (orig_index < request.len()) {
                let updated_range = &updated_requests[updated_index];
                let orig_range = &request[orig_index];
                let byte_offset = updated_range.start as usize;

                if is_overlapping(updated_range, orig_range) {
                    // We need to undo the coalescing and splitting done earlier
                    let start = orig_range.start as usize - byte_offset;
                    if orig_range.end <= updated_range.end {
                        // The original range is fully contained in the updated range, can do
                        // zero-copy slice
                        let end = orig_range.end as usize - byte_offset;
                        final_bytes.push(bytes_vec[updated_index].slice(start..end));
                    } else {
                        // The original read was split into multiple requests, need to copy
                        // back into a single buffer
                        let orig_size = orig_range.end - orig_range.start;
                        let mut merged_bytes = Vec::with_capacity(orig_size as usize);
                        merged_bytes.extend_from_slice(&bytes_vec[updated_index].slice(start..));
                        let mut copy_offset = merged_bytes.len() as u64;
                        while copy_offset < orig_size {
                            updated_index += 1;
                            let next_range = &updated_requests[updated_index];
                            let bytes_to_take =
                                (orig_size - copy_offset).min(next_range.end - next_range.start);
                            merged_bytes.extend_from_slice(
                                &bytes_vec[updated_index].slice(0..bytes_to_take as usize),
                            );
                            copy_offset += bytes_to_take;
                        }
                        final_bytes.push(Bytes::from(merged_bytes));
                    }
                    orig_index += 1;
                } else {
                    updated_index += 1;
                }
            }

            Ok(final_bytes)
        }
    }

    pub fn with_priority(&self, priority: u64) -> Self {
        Self {
            reader: self.reader.clone(),
            root: self.root.clone(),
            block_size: self.block_size,
            max_iop_size: self.max_iop_size,
            base_priority: priority,
            bypass_backpressure: self.bypass_backpressure,
            extra_stats: self.extra_stats.clone(),
        }
    }

    /// Returns a copy of this scheduler that additionally records the I/O it
    /// performs into `stats`, on top of the scheduler's global statistics.
    ///
    /// This is the mechanism for measuring exact per-scope (e.g. per-query) I/O:
    /// attach a recorder here (e.g. via [`IoStats::recorder`]), perform the reads
    /// through the returned handle, then read the totals back with
    /// [`IoStats::snapshot`].  The returned handle is cheap to create (a few
    /// `Arc` clones) and reuses the same underlying reader, so it does not
    /// re-open the file.
    pub fn with_io_stats(&self, stats: Arc<dyn IoStatsRecorder>) -> Self {
        Self {
            extra_stats: Some(stats),
            ..self.clone()
        }
    }

    /// Returns a copy of this scheduler that bypasses backpressure for all requests.
    ///
    /// This should be used for indirect I/O (e.g. fetching items after decoding offsets) where
    /// blocking on backpressure could cause a deadlock or excessive latency.
    pub fn with_bypass_backpressure(&self) -> Self {
        Self {
            bypass_backpressure: true,
            ..self.clone()
        }
    }

    /// Submit a single IOP to the reader
    ///
    /// If you have multiple IOPS to perform then [`Self::submit_request`] is going
    /// to be more efficient.
    ///
    /// See [`Self::submit_request`] for more information on the priority and backpressure.
    pub fn submit_single(
        &self,
        range: Range<u64>,
        priority: u64,
    ) -> impl Future<Output = Result<Bytes>> + Send {
        self.submit_request(vec![range], priority)
            .map_ok(|vec_bytes| vec_bytes.into_iter().next().unwrap())
    }

    /// Provides access to the underlying reader
    ///
    /// Do not use this for reading data as it will bypass any I/O scheduling!
    /// This is mainly exposed to allow metadata operations (e.g size, block_size,)
    /// which either aren't IOPS or we don't throttle
    pub fn reader(&self) -> &Arc<dyn Reader> {
        &self.reader
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::VecDeque, time::Duration};

    use futures::poll;
    use lance_core::utils::tempfile::TempObjFile;
    use rand::RngCore;
    use rstest::rstest;

    use object_store::{GetRange, ObjectStore as OSObjectStore, ObjectStoreExt, memory::InMemory};
    use tokio::{runtime::Handle, time::timeout};
    use url::Url;

    use crate::{
        object_store::{DEFAULT_DOWNLOAD_RETRY_COUNT, DEFAULT_MAX_IOP_SIZE},
        testing::MockObjectStore,
    };

    use super::*;

    fn make_task(priority: u128, bypass_backpressure: bool) -> IoTask {
        IoTask {
            reader: Arc::new(TrackingReader {
                get_range_count: Arc::new(AtomicU64::new(0)),
                path: Path::parse("test").unwrap(),
            }),
            to_read: 0..1,
            when_done: Box::new(|_| {}),
            priority,
            bypass_backpressure,
        }
    }

    #[test]
    fn test_scheduler_state_event_fields() {
        use tracing_mock::{expect, subscriber};

        let event = expect::event()
            .with_target(SCHEDULER_STATE_EVENT_TARGET)
            .at_level(tracing::Level::TRACE)
            .with_fields(
                expect::field("queue_kind")
                    .with_value(&"standard")
                    .and(expect::field("scheduler_iops").with_value(&7u64))
                    .and(expect::field("scheduler_requests").with_value(&3u64))
                    .and(expect::field("scheduler_bytes_read").with_value(&4096u64))
                    .and(expect::field("io_capacity").with_value(&4u64))
                    .and(expect::field("pending_iops").with_value(&1u64))
                    .and(expect::field("bytes_available").with_value(&128i64))
                    .and(expect::field("head_task_bytes_present").with_value(&true))
                    .and(expect::field("head_task_bytes").with_value(&1u64))
                    .and(expect::field("head_task_can_deliver_present").with_value(&true))
                    .and(expect::field("head_task_can_deliver").with_value(&true)),
            );
        let (subscriber, handle) = subscriber::mock().event(event).run_with_handle();

        let stats = IoStats::new();
        stats.add_scan_stats(&ScanStats {
            iops: 7,
            requests: 3,
            bytes_read: 4096,
        });
        let mut state = IoQueueState::new(4, 192);
        state.iops_avail = 2;
        state.bytes_avail = 128;
        state.pending_requests.push(make_task(1, false));

        tracing::subscriber::with_default(subscriber, || {
            emit_scheduler_state_event(state.scheduler_state_event(), &stats);
        });

        handle.assert_finished();
    }

    #[test]
    fn test_iotask_ordering() {
        // Bypass tasks must come out of the heap before non-bypass tasks.
        // Within each group, lower priority number (= higher priority) comes first.
        let mut heap = BinaryHeap::new();
        heap.push(make_task(10, false)); // non-bypass, low priority
        heap.push(make_task(1, false)); // non-bypass, high priority
        heap.push(make_task(20, true)); // bypass, low priority
        heap.push(make_task(5, true)); // bypass, high priority

        let order: Vec<(u128, bool)> = std::iter::from_fn(|| heap.pop())
            .map(|t| (t.priority, t.bypass_backpressure))
            .collect();

        assert_eq!(order, vec![(5, true), (20, true), (1, false), (10, false)]);
    }

    #[test]
    fn test_batch_with_undelivered_slot_is_error() {
        let response = Arc::new(Mutex::new(None));
        let response_clone = response.clone();
        let io_queue = Arc::new(IoQueue::new(1, 1024, IoStats::new()));
        let batch = MutableBatch::new(
            move |rsp| *response_clone.lock().unwrap() = Some(rsp),
            2, // num_data_buffers
            0, // priority
            2, // num_reqs
            false,
            io_queue,
        );
        drop(batch);

        let mut rsp = response.lock().unwrap().take().unwrap();
        let data = rsp.data.take().unwrap();
        assert!(
            data.is_err(),
            "undelivered slot must yield an error, got {data:?}",
        );
    }

    #[tokio::test]
    async fn test_full_seq_read() {
        let tmp_file = TempObjFile::default();

        let obj_store = Arc::new(ObjectStore::local());

        // Write 1MiB of data
        const DATA_SIZE: u64 = 1024 * 1024;
        let mut some_data = vec![0; DATA_SIZE as usize];
        rand::rng().fill_bytes(&mut some_data);
        obj_store.put(&tmp_file, &some_data).await.unwrap();

        let config = SchedulerConfig::default_for_testing();

        let scheduler = ScanScheduler::new(obj_store, config);

        let file_scheduler = scheduler
            .open_file(&tmp_file, &CachedFileSize::unknown())
            .await
            .unwrap();

        // Read it back 4KiB at a time
        const READ_SIZE: u64 = 4 * 1024;
        let mut reqs = VecDeque::new();
        let mut offset = 0;
        while offset < DATA_SIZE {
            reqs.push_back(
                #[allow(clippy::single_range_in_vec_init)]
                file_scheduler
                    .submit_request(vec![offset..offset + READ_SIZE], 0)
                    .await
                    .unwrap(),
            );
            offset += READ_SIZE;
        }

        offset = 0;
        // Note: we should get parallel I/O even though we are consuming serially
        while offset < DATA_SIZE {
            let data = reqs.pop_front().unwrap();
            let actual = &data[0];
            let expected = &some_data[offset as usize..(offset + READ_SIZE) as usize];
            assert_eq!(expected, actual);
            offset += READ_SIZE;
        }
    }

    #[tokio::test]
    async fn test_open_reader_bridge() {
        let tmp_file = TempObjFile::default();

        let obj_store = Arc::new(ObjectStore::local());

        const DATA_SIZE: u64 = 64 * 1024;
        let mut some_data = vec![0; DATA_SIZE as usize];
        rand::rng().fill_bytes(&mut some_data);
        obj_store.put(&tmp_file, &some_data).await.unwrap();

        let config = SchedulerConfig::default_for_testing();
        let scheduler = ScanScheduler::new(obj_store.clone(), config);

        // Open a bare Reader ourselves, then bridge it into a FileScheduler.
        let reader: Arc<dyn Reader> = obj_store.open(&tmp_file).await.unwrap().into();
        let file_scheduler = scheduler.open_reader(reader);

        let bytes = file_scheduler
            .submit_request(vec![0..DATA_SIZE], 0)
            .await
            .unwrap();
        assert_eq!(bytes[0], some_data);
    }

    #[derive(Debug)]
    struct ShortReader {
        path: Path,
    }

    impl lance_core::deepsize::DeepSizeOf for ShortReader {
        fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
            0
        }
    }

    impl Reader for ShortReader {
        fn path(&self) -> &Path {
            &self.path
        }

        fn block_size(&self) -> usize {
            4096
        }

        fn io_parallelism(&self) -> usize {
            1
        }

        fn size(&self) -> futures::future::BoxFuture<'_, object_store::Result<usize>> {
            Box::pin(async { Ok(0) })
        }

        fn get_range(
            &self,
            _range: Range<usize>,
        ) -> futures::future::BoxFuture<'static, object_store::Result<Bytes>> {
            Box::pin(async { Ok(Bytes::new()) })
        }

        fn get_all(&self) -> futures::future::BoxFuture<'_, object_store::Result<Bytes>> {
            Box::pin(async { Ok(Bytes::new()) })
        }
    }

    #[rstest]
    #[case::standard(false)]
    #[case::lite(true)]
    #[tokio::test]
    async fn test_short_read_returns_io_error(#[case] use_lite_scheduler: bool) {
        let config = SchedulerConfig {
            use_lite_scheduler: Some(use_lite_scheduler),
            ..SchedulerConfig::default_for_testing()
        };
        let scheduler = ScanScheduler::new(Arc::new(ObjectStore::memory()), config);
        let reader = Arc::new(ShortReader {
            path: Path::parse("short-file").unwrap(),
        });
        let file_scheduler = scheduler.open_reader(reader);

        let error = file_scheduler
            .submit_request(vec![0..8], 0)
            .await
            .unwrap_err();

        assert!(matches!(error, Error::IO { .. }), "{error:?}");
        assert!(
            error.to_string().contains(
                "I/O request for file short-file and range 0..8 returned 0 bytes, expected 8 bytes"
            ),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_split_coalesce() {
        let tmp_file = TempObjFile::default();

        let obj_store = Arc::new(ObjectStore::local());

        // Write 75MiB of data
        const DATA_SIZE: u64 = 75 * 1024 * 1024;
        let mut some_data = vec![0; DATA_SIZE as usize];
        rand::rng().fill_bytes(&mut some_data);
        obj_store.put(&tmp_file, &some_data).await.unwrap();

        let config = SchedulerConfig::default_for_testing();

        let scheduler = ScanScheduler::new(obj_store, config);

        let file_scheduler = scheduler
            .open_file(&tmp_file, &CachedFileSize::unknown())
            .await
            .unwrap();

        // These 3 requests should be coalesced into a single I/O because they are within 4KiB
        // of each other
        let req =
            file_scheduler.submit_request(vec![50_000..51_000, 52_000..53_000, 54_000..55_000], 0);

        let bytes = req.await.unwrap();

        assert_eq!(bytes[0], &some_data[50_000..51_000]);
        assert_eq!(bytes[1], &some_data[52_000..53_000]);
        assert_eq!(bytes[2], &some_data[54_000..55_000]);

        assert_eq!(1, scheduler.stats().iops);

        // This should be split into 5 requests because it is so large
        let req = file_scheduler.submit_request(vec![0..DATA_SIZE], 0);
        let bytes = req.await.unwrap();
        assert!(bytes[0] == some_data, "data is not the same");

        assert_eq!(6, scheduler.stats().iops);

        // None of these requests are bigger than the max IOP size but they will be coalesced into
        // one IOP that is bigger and then split back into 2 requests that don't quite align with the original
        // ranges.
        let chunk_size = *DEFAULT_MAX_IOP_SIZE;
        let req = file_scheduler.submit_request(
            vec![
                10..chunk_size,
                chunk_size + 10..(chunk_size * 2) - 20,
                chunk_size * 2..(chunk_size * 2) + 10,
            ],
            0,
        );

        let bytes = req.await.unwrap();
        let chunk_size = chunk_size as usize;
        assert!(
            bytes[0] == some_data[10..chunk_size],
            "data is not the same"
        );
        assert!(
            bytes[1] == some_data[chunk_size + 10..(chunk_size * 2) - 20],
            "data is not the same"
        );
        assert!(
            bytes[2] == some_data[chunk_size * 2..(chunk_size * 2) + 10],
            "data is not the same"
        );
        assert_eq!(8, scheduler.stats().iops);

        let reads = (0..44)
            .map(|i| i * 1_000_000..(i + 1) * 1_000_000)
            .collect::<Vec<_>>();
        let req = file_scheduler.submit_request(reads, 0);
        let bytes = req.await.unwrap();
        for (i, bytes) in bytes.iter().enumerate() {
            assert!(
                bytes == &some_data[i * 1_000_000..(i + 1) * 1_000_000],
                "data is not the same"
            );
        }
        assert_eq!(11, scheduler.stats().iops);
    }

    #[tokio::test]
    async fn test_io_stats_sink() {
        let tmp_file = TempObjFile::default();
        let obj_store = Arc::new(ObjectStore::local());

        const DATA_SIZE: u64 = 1024 * 1024;
        let mut some_data = vec![0; DATA_SIZE as usize];
        rand::rng().fill_bytes(&mut some_data);
        obj_store.put(&tmp_file, &some_data).await.unwrap();

        let scheduler = ScanScheduler::new(obj_store, SchedulerConfig::default_for_testing());

        // Attach a per-scope sink to one file handle.
        let sink = IoStats::new();
        let file_scheduler = scheduler
            .open_file(&tmp_file, &CachedFileSize::unknown())
            .await
            .unwrap()
            .with_io_stats(sink.recorder());

        // Three reads within 4KiB coalesce into a single physical IOP.  The sink
        // and the scheduler's global totals must agree exactly, because both are
        // recorded from the same post-coalescing request.
        file_scheduler
            .submit_request(vec![50_000..51_000, 52_000..53_000, 54_000..55_000], 0)
            .await
            .unwrap();

        let global = scheduler.stats();
        let scoped = sink.snapshot();
        assert_eq!(1, scoped.iops);
        assert_eq!(1, scoped.requests);
        // Coalesced range 50_000..55_000 => 5000 physical bytes.
        assert_eq!(5000, scoped.bytes_read);
        assert_eq!(global.iops, scoped.iops);
        assert_eq!(global.requests, scoped.requests);
        assert_eq!(global.bytes_read, scoped.bytes_read);

        // A sibling handle without the sink: the global totals advance but the
        // sink stays put, proving per-scope isolation.
        let other = scheduler
            .open_file(&tmp_file, &CachedFileSize::unknown())
            .await
            .unwrap();
        other.submit_request(vec![0..1000], 0).await.unwrap();

        let global_after = scheduler.stats();
        let scoped_after = sink.snapshot();
        assert_eq!(global.bytes_read + 1000, global_after.bytes_read);
        assert_eq!(scoped.bytes_read, scoped_after.bytes_read);
        assert_eq!(scoped.iops, scoped_after.iops);
    }

    #[tokio::test]
    async fn test_priority() {
        let some_path = Path::parse("foo").unwrap();
        let base_store = Arc::new(InMemory::new());
        base_store
            .put(&some_path, vec![0; 1000].into())
            .await
            .unwrap();

        let semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        let mut obj_store = MockObjectStore::default();
        let semaphore_copy = semaphore.clone();
        obj_store
            .expect_get_opts()
            .returning(move |location, options| {
                let semaphore = semaphore.clone();
                let base_store = base_store.clone();
                let location = location.clone();
                async move {
                    semaphore.acquire().await.unwrap().forget();
                    base_store.get_opts(&location, options).await
                }
                .boxed()
            });
        let obj_store = Arc::new(ObjectStore::new(
            Arc::new(obj_store),
            Url::parse("mem://").unwrap(),
            Some(500),
            None,
            false,
            false,
            1,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ));

        let config = SchedulerConfig {
            io_buffer_size_bytes: 1024 * 1024,
            use_lite_scheduler: None,
        };

        let scan_scheduler = ScanScheduler::new(obj_store, config);

        let file_scheduler = scan_scheduler
            .open_file(&Path::parse("foo").unwrap(), &CachedFileSize::new(1000))
            .await
            .unwrap();

        // Issue a request, priority doesn't matter, it will be submitted
        // immediately (it will go pending)
        // Note: the timeout is to prevent a deadlock if the test fails.
        let first_fut = timeout(
            Duration::from_secs(10),
            file_scheduler.submit_single(0..10, 0),
        )
        .boxed();

        // Issue another low priority request (it will go in queue)
        let mut second_fut = timeout(
            Duration::from_secs(10),
            file_scheduler.submit_single(0..20, 100),
        )
        .boxed();

        // Issue a high priority request (it will go in queue and should bump
        // the other queued request down)
        let mut third_fut = timeout(
            Duration::from_secs(10),
            file_scheduler.submit_single(0..30, 0),
        )
        .boxed();

        // Finish one file, should be the in-flight first request
        semaphore_copy.add_permits(1);
        assert!(first_fut.await.unwrap().unwrap().len() == 10);
        // Other requests should not be finished
        assert!(poll!(&mut second_fut).is_pending());
        assert!(poll!(&mut third_fut).is_pending());

        // Next should be high priority request
        semaphore_copy.add_permits(1);
        assert!(third_fut.await.unwrap().unwrap().len() == 30);
        assert!(poll!(&mut second_fut).is_pending());

        // Finally, the low priority request
        semaphore_copy.add_permits(1);
        assert!(second_fut.await.unwrap().unwrap().len() == 20);
    }

    #[tokio::test]
    async fn test_standard_scheduler_state_tracks_queue_state() {
        let some_path = Path::parse("foo").unwrap();
        let base_store = Arc::new(InMemory::new());
        base_store
            .put(&some_path, vec![0; 1000].into())
            .await
            .unwrap();

        let semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        let mut obj_store = MockObjectStore::default();
        let semaphore_copy = semaphore.clone();
        obj_store
            .expect_get_opts()
            .returning(move |location, options| {
                let semaphore = semaphore.clone();
                let base_store = base_store.clone();
                let location = location.clone();
                async move {
                    semaphore.acquire().await.unwrap().forget();
                    base_store.get_opts(&location, options).await
                }
                .boxed()
            });
        let obj_store = Arc::new(ObjectStore::new(
            Arc::new(obj_store),
            Url::parse("mem://").unwrap(),
            Some(500),
            None,
            false,
            false,
            1,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ));

        let scheduler = ScanScheduler::new(
            obj_store,
            SchedulerConfig {
                io_buffer_size_bytes: 1024 * 1024,
                use_lite_scheduler: Some(false),
            },
        );
        let file_scheduler = scheduler
            .open_file(&Path::parse("foo").unwrap(), &CachedFileSize::new(1000))
            .await
            .unwrap();

        let first_fut = timeout(
            Duration::from_secs(10),
            file_scheduler.submit_single(0..10, 0),
        )
        .boxed();
        let second_fut = timeout(
            Duration::from_secs(10),
            file_scheduler.submit_single(0..20, 100),
        )
        .boxed();
        let third_fut = timeout(
            Duration::from_secs(10),
            file_scheduler.submit_single(0..30, 0),
        )
        .boxed();

        let io_queue = match &scheduler.io_queue {
            IoQueueType::Standard(io_queue) => io_queue.clone(),
            IoQueueType::Lite(_) => unreachable!("test forces the standard scheduler"),
        };
        let (
            io_capacity,
            iops_available,
            pending_bytes,
            bytes_reserved,
            priorities_in_flight,
            head_task_bytes,
            head_task_blocked_by_iops,
            head_task_blocked_by_bytes,
        ) = timeout(Duration::from_secs(5), async {
            loop {
                let observed = {
                    let state = io_queue.state.lock().unwrap();
                    let active_iops = state.io_capacity.saturating_sub(state.iops_avail);
                    if active_iops == 1 && state.pending_requests.len() == 2 {
                        let pending_bytes = state
                            .pending_requests
                            .iter()
                            .map(IoTask::num_bytes)
                            .sum::<u64>();
                        let head_task = state.pending_requests.peek().unwrap();
                        let bypasses_bytes = state.no_backpressure
                            || head_task.bypass_backpressure
                            || head_task.priority <= state.priorities_in_flight.min_in_flight();
                        Some((
                            state.io_capacity,
                            state.iops_avail,
                            pending_bytes,
                            state.io_buffer_size as i64 - state.bytes_avail,
                            state.priorities_in_flight.len(),
                            head_task.num_bytes(),
                            state.iops_avail == 0,
                            !bypasses_bytes && head_task.num_bytes() as i64 > state.bytes_avail,
                        ))
                    } else {
                        None
                    }
                };
                if let Some(observed) = observed {
                    break observed;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();

        assert_eq!(io_capacity, 1);
        assert_eq!(iops_available, 0);
        assert_eq!(pending_bytes, 50);
        assert_eq!(bytes_reserved, 10);
        assert_eq!(priorities_in_flight, 1);
        assert_eq!(head_task_bytes, 30);
        assert!(head_task_blocked_by_iops);
        assert!(!head_task_blocked_by_bytes);

        semaphore_copy.add_permits(3);
        assert_eq!(first_fut.await.unwrap().unwrap().len(), 10);
        assert_eq!(third_fut.await.unwrap().unwrap().len(), 30);
        assert_eq!(second_fut.await.unwrap().unwrap().len(), 20);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_backpressure() {
        let some_path = Path::parse("foo").unwrap();
        let base_store = Arc::new(InMemory::new());
        base_store
            .put(&some_path, vec![0; 100000].into())
            .await
            .unwrap();

        let bytes_read = Arc::new(AtomicU64::from(0));
        let mut obj_store = MockObjectStore::default();
        let bytes_read_copy = bytes_read.clone();
        // Wraps the obj_store to keep track of how many bytes have been read
        obj_store
            .expect_get_opts()
            .returning(move |location, options| {
                let range = options.range.as_ref().unwrap();
                let num_bytes = match range {
                    GetRange::Bounded(bounded) => bounded.end - bounded.start,
                    _ => panic!(),
                };
                bytes_read_copy.fetch_add(num_bytes, Ordering::Release);
                let location = location.clone();
                let base_store = base_store.clone();
                async move { base_store.get_opts(&location, options).await }.boxed()
            });
        let obj_store = Arc::new(ObjectStore::new(
            Arc::new(obj_store),
            Url::parse("mem://").unwrap(),
            Some(500),
            None,
            false,
            false,
            1,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ));

        let config = SchedulerConfig {
            io_buffer_size_bytes: 10,
            use_lite_scheduler: None,
        };

        let scan_scheduler = ScanScheduler::new(obj_store.clone(), config);

        let file_scheduler = scan_scheduler
            .open_file(&Path::parse("foo").unwrap(), &CachedFileSize::new(100000))
            .await
            .unwrap();

        let wait_for_idle = || async move {
            let handle = Handle::current();
            while handle.metrics().num_alive_tasks() != 1 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        };
        let wait_for_bytes_read_and_idle = |target_bytes: u64| {
            // We need to move `target` but don't want to move `bytes_read`
            let bytes_read = &bytes_read;
            async move {
                let bytes_read_copy = bytes_read.clone();
                while bytes_read_copy.load(Ordering::Acquire) < target_bytes {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
                wait_for_idle().await;
            }
        };

        // This read will begin immediately
        let first_fut = file_scheduler.submit_single(0..5, 0);
        // This read should also begin immediately
        let second_fut = file_scheduler.submit_single(0..5, 0);
        // This read will be throttled
        let third_fut = file_scheduler.submit_single(0..3, 0);
        // Two tasks (third_fut and unit test)
        wait_for_bytes_read_and_idle(10).await;

        assert_eq!(first_fut.await.unwrap().len(), 5);
        // One task (unit test)
        wait_for_bytes_read_and_idle(13).await;

        // 2 bytes are ready but 5 bytes requested, read will be blocked
        let fourth_fut = file_scheduler.submit_single(0..5, 0);
        wait_for_bytes_read_and_idle(13).await;

        // Out of order completion is ok, will unblock backpressure
        assert_eq!(third_fut.await.unwrap().len(), 3);
        wait_for_bytes_read_and_idle(18).await;

        assert_eq!(second_fut.await.unwrap().len(), 5);
        // At this point there are 5 bytes available in backpressure queue
        // Now we issue multi-read that can be partially fulfilled, it will read some bytes but
        // not all of them. (using large range gap to ensure request not coalesced)
        //
        // I'm actually not sure this behavior is great.  It's possible that we should just
        // block until we can fulfill the entire request.
        let fifth_fut = file_scheduler.submit_request(vec![0..3, 90000..90007], 0);
        wait_for_bytes_read_and_idle(21).await;

        // Fifth future should eventually finish due to deadlock prevention
        let fifth_bytes = tokio::time::timeout(Duration::from_secs(10), fifth_fut)
            .await
            .unwrap();
        assert_eq!(
            fifth_bytes.unwrap().iter().map(|b| b.len()).sum::<usize>(),
            10
        );

        // And now let's just make sure that we can read the rest of the data
        assert_eq!(fourth_fut.await.unwrap().len(), 5);
        wait_for_bytes_read_and_idle(28).await;

        // Ensure deadlock prevention timeout can be disabled
        let config = SchedulerConfig {
            io_buffer_size_bytes: 10,
            use_lite_scheduler: None,
        };

        let scan_scheduler = ScanScheduler::new(obj_store, config);
        let file_scheduler = scan_scheduler
            .open_file(&Path::parse("foo").unwrap(), &CachedFileSize::new(100000))
            .await
            .unwrap();

        let first_fut = file_scheduler.submit_single(0..10, 0);
        let second_fut = file_scheduler.submit_single(0..10, 0);

        std::thread::sleep(Duration::from_millis(100));
        assert_eq!(first_fut.await.unwrap().len(), 10);
        assert_eq!(second_fut.await.unwrap().len(), 10);
    }

    #[derive(Debug)]
    struct BlockingReader {
        semaphore: Arc<tokio::sync::Semaphore>,
        get_range_count: Arc<AtomicU64>,
        path: Path,
    }

    impl lance_core::deepsize::DeepSizeOf for BlockingReader {
        fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
            0
        }
    }

    impl Reader for BlockingReader {
        fn path(&self) -> &Path {
            &self.path
        }

        fn block_size(&self) -> usize {
            4096
        }

        fn io_parallelism(&self) -> usize {
            1
        }

        fn size(&self) -> futures::future::BoxFuture<'_, object_store::Result<usize>> {
            Box::pin(async { Ok(1_000_000) })
        }

        fn get_range(
            &self,
            range: Range<usize>,
        ) -> futures::future::BoxFuture<'static, object_store::Result<Bytes>> {
            self.get_range_count.fetch_add(1, Ordering::Release);
            let semaphore = self.semaphore.clone();
            let num_bytes = range.end - range.start;
            Box::pin(async move {
                semaphore.acquire().await.unwrap().forget();
                Ok(Bytes::from(vec![0u8; num_bytes]))
            })
        }

        fn get_all(&self) -> futures::future::BoxFuture<'_, object_store::Result<Bytes>> {
            Box::pin(async { Ok(Bytes::from(vec![0u8; 1_000_000])) })
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_same_priority_chunks_continue_after_higher_priority_request() {
        let obj_store = Arc::new(ObjectStore::new(
            Arc::new(InMemory::new()),
            Url::parse("mem://").unwrap(),
            Some(4096),
            None,
            false,
            false,
            1,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ));
        let scheduler = ScanScheduler::new(
            obj_store,
            SchedulerConfig {
                io_buffer_size_bytes: 10,
                use_lite_scheduler: Some(false),
            },
        );
        let semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        let reader: Arc<dyn Reader> = Arc::new(BlockingReader {
            semaphore: semaphore.clone(),
            get_range_count: Arc::new(AtomicU64::new(0)),
            path: Path::parse("test").unwrap(),
        });

        let low_priority =
            scheduler.submit_request(reader.clone(), vec![0..6, 100..106], 10, false);
        let high_priority = scheduler.submit_request(reader, vec![200..204], 0, false);

        semaphore.add_permits(3);
        let low_priority = timeout(Duration::from_secs(5), low_priority)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            low_priority.iter().map(|bytes| bytes.len()).sum::<usize>(),
            12
        );

        let high_priority = timeout(Duration::from_secs(5), high_priority)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(high_priority[0].len(), 4);
    }

    /// A Reader that tracks how many times get_range has been called.
    #[derive(Debug)]
    struct TrackingReader {
        get_range_count: Arc<AtomicU64>,
        path: Path,
    }

    impl lance_core::deepsize::DeepSizeOf for TrackingReader {
        fn deep_size_of_children(&self, _context: &mut lance_core::deepsize::Context) -> usize {
            0
        }
    }

    impl Reader for TrackingReader {
        fn path(&self) -> &Path {
            &self.path
        }

        fn block_size(&self) -> usize {
            4096
        }

        fn io_parallelism(&self) -> usize {
            1
        }

        fn size(&self) -> futures::future::BoxFuture<'_, object_store::Result<usize>> {
            Box::pin(async { Ok(1_000_000) })
        }

        fn get_range(
            &self,
            range: Range<usize>,
        ) -> futures::future::BoxFuture<'static, object_store::Result<Bytes>> {
            self.get_range_count.fetch_add(1, Ordering::Release);
            let num_bytes = range.end - range.start;
            Box::pin(async move { Ok(Bytes::from(vec![0u8; num_bytes])) })
        }

        fn get_all(&self) -> futures::future::BoxFuture<'_, object_store::Result<Bytes>> {
            Box::pin(async { Ok(Bytes::from(vec![0u8; 1_000_000])) })
        }
    }

    #[tokio::test]
    async fn test_lite_scheduler_submits_eagerly() {
        let obj_store = Arc::new(ObjectStore::memory());
        let config = SchedulerConfig::default_for_testing().with_lite_scheduler();
        let scheduler = ScanScheduler::new(obj_store, config);

        let get_range_count = Arc::new(AtomicU64::new(0));
        let reader: Arc<dyn Reader> = Arc::new(TrackingReader {
            get_range_count: get_range_count.clone(),
            path: Path::parse("test").unwrap(),
        });

        // Submit several requests. The lite scheduler should call get_range
        // eagerly during submit (before the returned future is polled).
        let fut1 = scheduler.submit_request(reader.clone(), vec![0..100], 0, false);
        let fut2 = scheduler.submit_request(reader.clone(), vec![100..200], 10, false);
        let fut3 = scheduler.submit_request(reader.clone(), vec![200..300], 20, false);

        // get_range must have been called for all 3 requests already.
        assert_eq!(get_range_count.load(Ordering::Acquire), 3);

        // The futures should still resolve with the correct data.
        assert_eq!(fut1.await.unwrap()[0].len(), 100);
        assert_eq!(fut2.await.unwrap()[0].len(), 100);
        assert_eq!(fut3.await.unwrap()[0].len(), 100);
    }

    #[tokio::test]
    async fn test_object_store_selects_scheduler() {
        // A memory:// store should use the standard scheduler when config is None
        let memory_store = Arc::new(ObjectStore::memory());
        assert!(!memory_store.prefers_lite_scheduler());
        let config = SchedulerConfig {
            io_buffer_size_bytes: 256 * 1024 * 1024,
            use_lite_scheduler: None,
        };
        let scheduler = ScanScheduler::new(memory_store.clone(), config);
        assert!(!scheduler.uses_lite_scheduler());

        // A file+uring:// store should use the lite scheduler when config is None
        let uring_store = Arc::new(ObjectStore::new(
            Arc::new(InMemory::new()),
            Url::parse("file+uring:///tmp").unwrap(),
            None,
            None,
            false,
            false,
            8,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ));
        assert!(uring_store.prefers_lite_scheduler());
        let config = SchedulerConfig {
            io_buffer_size_bytes: 256 * 1024 * 1024,
            use_lite_scheduler: None,
        };
        let scheduler = ScanScheduler::new(uring_store.clone(), config);
        assert!(scheduler.uses_lite_scheduler());

        // Explicit Some(false) overrides a file+uring:// store's preference
        let config = SchedulerConfig {
            io_buffer_size_bytes: 256 * 1024 * 1024,
            use_lite_scheduler: Some(false),
        };
        let scheduler = ScanScheduler::new(uring_store, config);
        assert!(!scheduler.uses_lite_scheduler());

        // Explicit Some(true) overrides a memory:// store's preference
        let config = SchedulerConfig {
            io_buffer_size_bytes: 256 * 1024 * 1024,
            use_lite_scheduler: Some(true),
        };
        let scheduler = ScanScheduler::new(memory_store, config);
        assert!(scheduler.uses_lite_scheduler());
    }

    #[test_log::test(tokio::test(flavor = "multi_thread"))]
    async fn stress_backpressure() {
        // This test ensures that the backpressure mechanism works correctly with
        // regards to priority.  In other words, as long as all requests are consumed
        // in priority order then the backpressure mechanism should not deadlock
        let some_path = Path::parse("foo").unwrap();
        let obj_store = Arc::new(ObjectStore::memory());
        obj_store
            .put(&some_path, vec![0; 100000].as_slice())
            .await
            .unwrap();

        // Only one request will be allowed in
        let config = SchedulerConfig {
            io_buffer_size_bytes: 1,
            use_lite_scheduler: None,
        };
        let scan_scheduler = ScanScheduler::new(obj_store.clone(), config);
        let file_scheduler = scan_scheduler
            .open_file(&some_path, &CachedFileSize::unknown())
            .await
            .unwrap();

        let mut futs = Vec::with_capacity(10000);
        for idx in 0..10000 {
            futs.push(file_scheduler.submit_single(idx..idx + 1, idx));
        }

        for fut in futs {
            fut.await.unwrap();
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_zero_buffer_size_no_backpressure() {
        // With io_buffer_size_bytes=0 (no_backpressure=true), reads at any priority go
        // through without blocking, even though a zero budget would normally halt all I/O.
        let obj_store = Arc::new(ObjectStore::memory());
        let config = SchedulerConfig {
            io_buffer_size_bytes: 0,
            use_lite_scheduler: Some(false),
        };
        let scheduler = ScanScheduler::new(obj_store, config);

        let get_range_count = Arc::new(AtomicU64::new(0));
        let reader: Arc<dyn Reader> = Arc::new(TrackingReader {
            get_range_count: get_range_count.clone(),
            path: Path::parse("test").unwrap(),
        });

        // Submit three reads at increasing priorities without awaiting any first.
        // Priority 1 and 2 would deadlock under a real 0-byte budget without no_backpressure.
        let fut1 = scheduler.submit_request(reader.clone(), vec![0..1000], 0, false);
        let fut2 = scheduler.submit_request(reader.clone(), vec![1000..2000], 1, false);
        let fut3 = scheduler.submit_request(reader.clone(), vec![2000..3000], 2, false);

        let bytes1 = timeout(Duration::from_secs(5), fut1)
            .await
            .unwrap()
            .unwrap();
        let bytes2 = timeout(Duration::from_secs(5), fut2)
            .await
            .unwrap()
            .unwrap();
        let bytes3 = timeout(Duration::from_secs(5), fut3)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes1[0].len(), 1000);
        assert_eq!(bytes2[0].len(), 1000);
        assert_eq!(bytes3[0].len(), 1000);
        assert_eq!(get_range_count.load(Ordering::Acquire), 3);
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_file_scheduler_bypass_backpressure() {
        // A FileScheduler obtained via with_bypass_backpressure() submits reads that bypass
        // the byte budget, allowing them to proceed even when the budget is exhausted.
        let some_path = Path::parse("foo").unwrap();
        let base_store = Arc::new(InMemory::new());
        base_store
            .put(&some_path, vec![0u8; 1000].into())
            .await
            .unwrap();

        let bytes_dispatched = Arc::new(AtomicU64::from(0));
        let mut obj_store = MockObjectStore::default();
        let bytes_dispatched_copy = bytes_dispatched.clone();
        obj_store
            .expect_get_opts()
            .returning(move |location, options| {
                let range = options.range.as_ref().unwrap();
                let num_bytes = match range {
                    GetRange::Bounded(bounded) => bounded.end - bounded.start,
                    _ => panic!(),
                };
                bytes_dispatched_copy.fetch_add(num_bytes, Ordering::Release);
                let location = location.clone();
                let base_store = base_store.clone();
                async move { base_store.get_opts(&location, options).await }.boxed()
            });
        let obj_store = Arc::new(ObjectStore::new(
            Arc::new(obj_store),
            Url::parse("mem://").unwrap(),
            Some(500),
            None,
            false,
            false,
            1,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ));

        // Budget = 10 bytes.
        let config = SchedulerConfig {
            io_buffer_size_bytes: 10,
            use_lite_scheduler: Some(false),
        };
        let scan_scheduler = ScanScheduler::new(obj_store, config);
        let file_scheduler = scan_scheduler
            .open_file(&Path::parse("foo").unwrap(), &CachedFileSize::new(1000))
            .await
            .unwrap();
        let bypass_scheduler = file_scheduler.with_bypass_backpressure();

        // Fill the 10-byte budget with a priority-0 read.
        let blocker_fut = file_scheduler.submit_single(0..10, 0);
        while bytes_dispatched.load(Ordering::Acquire) < 10 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // A normal read at priority 2 is blocked: budget = 0, priority 2 > min-in-flight 0.
        // A bypass read at priority 1 (higher priority in the queue) bypasses the budget check.
        let normal_fut = file_scheduler.submit_single(0..10, 2);
        let bypass_fut = bypass_scheduler.submit_single(0..10, 1);

        // Bypass read is dispatched; normal read is still blocked.
        while bytes_dispatched.load(Ordering::Acquire) < 20 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(
            bytes_dispatched.load(Ordering::Acquire),
            20,
            "normal read should still be blocked while budget is exhausted"
        );

        // Consuming the blocker releases its 10-byte budget → normal read can proceed.
        timeout(Duration::from_secs(5), blocker_fut)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(5), bypass_fut)
            .await
            .unwrap()
            .unwrap();
        timeout(Duration::from_secs(5), normal_fut)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(bytes_dispatched.load(Ordering::Acquire), 30);
    }

    // Against a 100-byte budget: submit fut1 (50 bytes, priority 0), drop it while
    // its read is still blocked in get_range, then submit fut2 (60 bytes, priority 1).
    // fut2's priority can't win the priority-bypass, so it needs 60 of the budget --
    // available only if fut1's dropped reservation was refunded. Returns whether fut2
    // completed within 2s (false = the reservation leaked and fut2 deadlocked).
    async fn run_caller_drop_scenario(use_lite_scheduler: bool) -> (bool, Duration) {
        let obj_store = Arc::new(ObjectStore::new(
            Arc::new(InMemory::new()),
            Url::parse("mem://").unwrap(),
            Some(4096),
            None,
            false,
            false,
            1,
            DEFAULT_DOWNLOAD_RETRY_COUNT,
            None,
        ));
        let scheduler = ScanScheduler::new(
            obj_store,
            SchedulerConfig {
                io_buffer_size_bytes: 100,
                use_lite_scheduler: Some(use_lite_scheduler),
            },
        );

        let semaphore = Arc::new(tokio::sync::Semaphore::new(0));
        let get_range_count = Arc::new(AtomicU64::new(0));
        let reader: Arc<dyn Reader> = Arc::new(BlockingReader {
            semaphore: semaphore.clone(),
            get_range_count: get_range_count.clone(),
            path: Path::parse("test").unwrap(),
        });

        // Step 1: reserve 50 of the 100 budget bytes with a read we never consume.
        // Spawn it so we can cancel the caller-side future while it is still parked
        // waiting for the (blocked) read to finish.
        let fut1 = scheduler.submit_request(reader.clone(), vec![0..50], 0, false);
        let handle = tokio::spawn(async move {
            let _ = fut1.await;
        });

        // Wait until the read is genuinely in flight (blocked on the semaphore).
        // This guarantees the 50-byte reservation has been taken before we drop
        // the caller, closing the race between the I/O loop and the abort.
        while get_range_count.load(Ordering::Acquire) == 0 {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }

        // Step 2: drop the caller-side future while its `rx` is still pending.
        handle.abort();
        let _ = handle.await;

        // Step 3: let the in-flight read finish. The reservation should be refunded
        // now that the request is done, whether or not the caller is still around.
        semaphore.add_permits(1);
        // Give the read time to run to completion so the refund would already have
        // happened.
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Step 4: submit the follow-up. Add a permit up front so that, if it *is*
        // admitted, its own read can complete rather than block on the semaphore.
        semaphore.add_permits(1);
        let fut2 = scheduler.submit_request(reader, vec![100..160], 1, false);

        let start = std::time::Instant::now();
        let outcome = timeout(Duration::from_secs(2), fut2).await;
        let elapsed = start.elapsed();
        match outcome {
            Ok(res) => {
                assert_eq!(res.unwrap().iter().map(|b| b.len()).sum::<usize>(), 60);
                (true, elapsed)
            }
            Err(_) => (false, elapsed),
        }
    }

    /// Dropping a standard-scheduler request future while its read is in flight must
    /// still refund the backpressure reservation, so a later request that needs the
    /// budget does not deadlock.
    #[tokio::test(flavor = "multi_thread")]
    async fn standard_scheduler_refunds_reservation_on_caller_drop() {
        let (completed, elapsed) = run_caller_drop_scenario(false).await;
        assert!(
            completed,
            "standard scheduler deadlocked the follow-up request (elapsed {elapsed:?}); \
             the dropped request's reservation was not refunded"
        );
    }

    /// Same guarantee for the lite scheduler: dropping a request future mid-read
    /// releases its reservation via the `TaskHandle` drop path.
    #[tokio::test(flavor = "multi_thread")]
    async fn lite_scheduler_refunds_reservation_on_caller_drop() {
        let (completed, elapsed) = run_caller_drop_scenario(true).await;
        assert!(
            completed,
            "lite scheduler deadlocked the follow-up request (elapsed {elapsed:?}); \
             the dropped request's reservation was not refunded"
        );
    }
}
