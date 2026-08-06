// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

//! A lightweight I/O scheduler primarily intended for use with I/O uring.
//!
//! This scheduler attempts to avoid any kind of task switching whenever possible
//! to minimize context switching overhead.
//!
//! There are a few limitations compared to the standard scheduler:
//!
//! * There is no concurrency limit.  The scheduler will allow as many IOPS to run
//!   as possible as long as the backpressure throttle is not exceeded.
//! * There is no "babysitting" of IOPS.  An I/O task will only be polled when its
//!   future is polled.  The standard scheduler will `spawn` I/O tasks and so they
//!   are always polled by tokio's runtime.  This is important for operations like
//!   cloud requests where intermittent polling is required to clear out network
//!   buffers and keep the TCP connection moving.

use std::{
    collections::{BinaryHeap, HashMap},
    fmt::Debug,
    future::Future,
    ops::Range,
    pin::Pin,
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Instant,
};

use bytes::Bytes;
use lance_core::{Error, Result};

use super::{
    BACKPRESSURE_DEBOUNCE, BACKPRESSURE_MIN, IoStats, SCHEDULER_STATE_EVENT_TARGET,
    SchedulerStateEvent, emit_scheduler_state_event,
};

type RunFn = Box<dyn FnOnce() -> Pin<Box<dyn Future<Output = Result<Bytes>> + Send>> + Send>;

/// The state of an I/O task
///
/// The state machine is as follows:
///
/// * `Broken` - The task is in an error state and cannot be run, should never happen
/// * `Initial` - The task has been submitted but does not have a backpressure reservation
/// * `Reserved` - The task has a backpressure reservation
/// * `Running` - The task is running and has a future to poll
/// * `Finished` - The task has finished and has a result
enum TaskState {
    Broken,
    Initial {
        idle_waker: Option<Waker>,
        run_fn: RunFn,
    },
    Reserved {
        idle_waker: Option<Waker>,
        backpressure_reservation: BackpressureReservation,
        run_fn: RunFn,
    },
    Running {
        backpressure_reservation: BackpressureReservation,
        inner: Pin<Box<dyn Future<Output = Result<Bytes>> + Send>>,
    },
    Finished {
        backpressure_reservation: BackpressureReservation,
        data: Result<Bytes>,
    },
}

impl TaskState {
    fn backpressure_reservation(&self) -> Option<BackpressureReservation> {
        match self {
            Self::Reserved {
                backpressure_reservation,
                ..
            }
            | Self::Running {
                backpressure_reservation,
                ..
            }
            | Self::Finished {
                backpressure_reservation,
                ..
            } => Some(*backpressure_reservation),
            Self::Initial { .. } | Self::Broken => None,
        }
    }
}

/// A custom error type that might have a backpressure reservation
///
/// This is used instead of Lance's standard error type so we can ensure
/// we release the reservation before returning the error.
struct BrokenTaskError {
    message: String,
    backpressure_reservation: Option<BackpressureReservation>,
}

/// The result type corresponding to BrokenTaskError
type TaskResult = std::result::Result<(), BrokenTaskError>;

impl BrokenTaskError {
    // Create a BrokenTaskError from a task state
    //
    // This will capture any backpressure reservation the task has and put it into the
    // error so we make sure to release it when returning the error.
    fn new(task_state: TaskState, message: String) -> Self {
        match task_state.backpressure_reservation() {
            None => Self {
                message,
                backpressure_reservation: None,
            },
            Some(reservation) => Self {
                message,
                backpressure_reservation: Some(reservation),
            },
        }
    }
}

/// An I/O task represents a single read operation
struct IoTask {
    /// The unique identifier of the task (only used for debugging)
    id: u64,
    /// The number of bytes to read
    num_bytes: u64,
    /// The priority of the task, lower values are higher priority
    priority: u128,
    /// The current state of the task
    state: TaskState,
    /// When true, the task bypasses backpressure
    bypass_backpressure: bool,
}

impl IoTask {
    fn is_reserved(&self) -> bool {
        !matches!(self.state, TaskState::Initial { .. })
    }

    fn cancel(&mut self) -> bool {
        let was_running = matches!(self.state, TaskState::Running { .. });
        self.state = TaskState::Finished {
            backpressure_reservation: BackpressureReservation {
                num_bytes: 0,
                priority: 0,
            },
            data: Err(Error::io_source(Box::new(Error::io_source(
                "I/O Task cancelled".to_string().into(),
            )))),
        };
        was_running
    }

    fn reserve(&mut self, backpressure_reservation: BackpressureReservation) -> TaskResult {
        let state = std::mem::replace(&mut self.state, TaskState::Broken);
        let TaskState::Initial { idle_waker, run_fn } = state else {
            return Err(BrokenTaskError::new(
                state,
                format!("Task with id {} not in initial state", self.id),
            ));
        };
        self.state = TaskState::Reserved {
            idle_waker,
            backpressure_reservation,
            run_fn,
        };
        Ok(())
    }

    fn start(&mut self) -> TaskResult {
        let state = std::mem::replace(&mut self.state, TaskState::Broken);
        let TaskState::Reserved {
            backpressure_reservation,
            idle_waker,
            run_fn,
        } = state
        else {
            return Err(BrokenTaskError::new(
                state,
                format!("Task with id {} not in reserved state", self.id),
            ));
        };
        let inner = run_fn();
        self.state = TaskState::Running {
            backpressure_reservation,
            inner,
        };

        // If someone is already waiting for this task let them know it is now running
        // so they can poll it
        if let Some(idle_waker) = idle_waker {
            idle_waker.wake();
        }
        Ok(())
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        match &mut self.state {
            TaskState::Broken => Poll::Ready(()),
            TaskState::Initial { idle_waker, .. } | TaskState::Reserved { idle_waker, .. } => {
                idle_waker.replace(cx.waker().clone());
                Poll::Pending
            }
            TaskState::Running {
                inner,
                backpressure_reservation,
            } => match inner.as_mut().poll(cx) {
                Poll::Ready(data) => {
                    self.state = TaskState::Finished {
                        data,
                        backpressure_reservation: *backpressure_reservation,
                    };
                    Poll::Ready(())
                }
                Poll::Pending => Poll::Pending,
            },
            TaskState::Finished { .. } => Poll::Ready(()),
        }
    }

    fn consume(self) -> Result<(Result<Bytes>, BackpressureReservation)> {
        let TaskState::Finished {
            data,
            backpressure_reservation,
        } = self.state
        else {
            return Err(Error::internal(format!(
                "Task with id {} not in finished state",
                self.id
            )));
        };
        Ok((data, backpressure_reservation))
    }
}

#[derive(Debug, Clone, Copy)]
struct BackpressureReservation {
    num_bytes: u64,
    priority: u128,
}

/// A throttle to control how many bytes can be read before we pause to let compute catch up
trait BackpressureThrottle: Send {
    fn try_acquire(&mut self, num_bytes: u64, priority: u128) -> Option<BackpressureReservation>;
    fn release(&mut self, reservation: BackpressureReservation);
    /// Unconditionally acquire a zero-cost reservation, tracking only the priority.
    /// Used for bypass tasks that must never be blocked by backpressure.
    fn force_acquire(&mut self, priority: u128) -> BackpressureReservation;
    fn state(&self) -> BackpressureState;
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
    fn new(capacity: u64) -> Self {
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
}

#[derive(Debug, Clone, Copy)]
struct BackpressureState {
    max_bytes: u64,
    bytes_available: i64,
    priorities_in_flight: u64,
    no_backpressure: bool,
}

struct SimpleBackpressureThrottle {
    max_bytes: u64,
    start: Instant,
    last_warn: AtomicU64,
    bytes_available: i64,
    priorities_in_flight: PrioritiesInFlight,
    // When true, skip all byte-based backpressure checks (set when max_bytes == 0)
    no_backpressure: bool,
}

impl SimpleBackpressureThrottle {
    fn new(max_bytes: u64, max_concurrency: u64) -> Self {
        if max_bytes > i64::MAX as u64 {
            // This is unlikely to ever be an issue
            panic!("Max bytes must be less than {}", i64::MAX);
        }
        Self {
            max_bytes,
            start: Instant::now(),
            last_warn: AtomicU64::new(0),
            bytes_available: max_bytes as i64,
            priorities_in_flight: PrioritiesInFlight::new(max_concurrency),
            no_backpressure: max_bytes == 0,
        }
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
}

impl BackpressureThrottle for SimpleBackpressureThrottle {
    fn try_acquire(&mut self, num_bytes: u64, priority: u128) -> Option<BackpressureReservation> {
        if self.no_backpressure
            || self.bytes_available >= num_bytes as i64
            || self.priorities_in_flight.min_in_flight() >= priority
            // Chunks from an admitted logical request must keep moving.  A
            // higher-priority request may be scheduled later and remain
            // unconsumed while the caller awaits this request.
            || self.priorities_in_flight.contains(priority)
        {
            self.bytes_available -= num_bytes as i64;
            self.priorities_in_flight.push(priority);
            Some(BackpressureReservation {
                num_bytes,
                priority,
            })
        } else {
            self.warn_if_needed();
            None
        }
    }

    fn release(&mut self, reservation: BackpressureReservation) {
        self.bytes_available += reservation.num_bytes as i64;
        self.priorities_in_flight.remove(reservation.priority);
    }

    fn force_acquire(&mut self, priority: u128) -> BackpressureReservation {
        self.priorities_in_flight.push(priority);
        BackpressureReservation {
            num_bytes: 0,
            priority,
        }
    }

    fn state(&self) -> BackpressureState {
        BackpressureState {
            max_bytes: self.max_bytes,
            bytes_available: self.bytes_available,
            priorities_in_flight: self.priorities_in_flight.len() as u64,
            no_backpressure: self.no_backpressure,
        }
    }
}

struct TaskEntry {
    task_id: u64,
    priority: u128,
    reserved: bool,
}

impl Ord for TaskEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Prefer reserved tasks over unreserved tasks and then highest priority tasks over lowest
        // priority tasks.
        //
        // This is a max-heap so we sort by reserved in normal order (true > false) and priority
        // in reverse order (lowest priority first)
        self.reserved
            .cmp(&other.reserved)
            .then(other.priority.cmp(&self.priority))
    }
}

impl PartialOrd for TaskEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for TaskEntry {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for TaskEntry {}

struct IoQueueState {
    backpressure_throttle: Box<dyn BackpressureThrottle>,
    pending_tasks: BinaryHeap<TaskEntry>,
    tasks: HashMap<u64, IoTask>,
    next_task_id: u64,
}

impl IoQueueState {
    fn new(max_concurrency: u64, max_bytes: u64) -> Self {
        Self {
            backpressure_throttle: Box::new(SimpleBackpressureThrottle::new(
                max_bytes,
                max_concurrency,
            )),
            pending_tasks: BinaryHeap::new(),
            tasks: HashMap::new(),
            next_task_id: 0,
        }
    }

    // If a task is in an unexpected state then we need to release any reservations that were made
    // before we return an error.
    //
    // Note: this is perhaps a bit paranoid as a task should never be in an unexpected state.
    fn handle_result(&mut self, result: TaskResult) -> Result<()> {
        if let Err(error) = result {
            if let Some(reservation) = error.backpressure_reservation {
                self.backpressure_throttle.release(reservation);
            }
            Err(Error::internal(error.message))
        } else {
            Ok(())
        }
    }

    fn scheduler_state_event(&self) -> Option<SchedulerStateEvent> {
        if !tracing::enabled!(target: SCHEDULER_STATE_EVENT_TARGET, tracing::Level::TRACE) {
            return None;
        }

        let backpressure = self.backpressure_throttle.state();
        let pending_bytes = self
            .pending_tasks
            .iter()
            .filter_map(|entry| self.tasks.get(&entry.task_id))
            .map(|task| task.num_bytes)
            .sum::<u64>();
        let active_iops = self
            .tasks
            .values()
            .filter(|task| matches!(task.state, TaskState::Running { .. }))
            .count() as u64;

        Some(SchedulerStateEvent {
            queue_kind: "lite",
            io_capacity: 0,
            iops_available: 0,
            active_iops,
            pending_iops: self.pending_tasks.len() as u64,
            pending_bytes,
            bytes_available: backpressure.bytes_available,
            bytes_reserved: backpressure.max_bytes as i64 - backpressure.bytes_available,
            io_buffer_size_bytes: backpressure.max_bytes,
            priorities_in_flight: backpressure.priorities_in_flight,
            no_backpressure: backpressure.no_backpressure,
            head_task_bytes: None,
            head_task_priority_high: None,
            head_task_priority_low: None,
            min_in_flight_priority_high: None,
            min_in_flight_priority_low: None,
            head_task_can_deliver: None,
            head_task_priority_bypass: None,
            head_task_blocked_by_iops: None,
            head_task_blocked_by_bytes: None,
        })
    }
}

/// A queue of I/O tasks to be shared between the I/O scheduler and the I/O decoder.
///
/// The queue is protected by two different throttles.  The first controls memory backpressure, and
/// will only allow a certain number of bytes to be allocated for reads.  This throttle is released
/// as soon as the decoder consumes the bytes (not when the bytes have been fully processed).  This
/// throttle is currently scoped to the scheduler and not shared across the process.  This will likely
/// change in the future.
///
/// The second throttle controls how many IOPS can be issued concurrently.  This throttle is released
/// as soon as the IOP is finished.  This throttle has both a local per-scheduler limit and also a
/// process-wide limit.
///
/// Note: unlike the standard scheduler, there is no dedicated I/O loop thread.  If the decoder is not
/// polling the I/O tasks then nothing else will.  This scheduler is currently intended for use with I/O
/// uring where I/O tasks are bunched together and polling one task advances all outstanding I/O.  It
/// would not be suitable for cloud storage where each task is an independent HTTP request and needs to
/// be polled individually (though presumably one could use I/O uring for networked cloud storage some
/// day as well)
pub(super) struct IoQueue {
    state: Arc<Mutex<IoQueueState>>,
    stats: IoStats,
}

impl IoQueue {
    pub fn new(max_concurrency: u64, max_bytes: u64, stats: IoStats) -> Self {
        Self {
            state: Arc::new(Mutex::new(IoQueueState::new(max_concurrency, max_bytes))),
            stats,
        }
    }

    fn push(&self, mut task: IoTask, mut state: MutexGuard<IoQueueState>) -> Result<()> {
        let task_id = task.id;
        let maybe_reservation = if task.bypass_backpressure {
            Some(state.backpressure_throttle.force_acquire(task.priority))
        } else {
            state
                .backpressure_throttle
                .try_acquire(task.num_bytes, task.priority)
        };
        if let Some(reservation) = maybe_reservation {
            state.handle_result(task.reserve(reservation))?;
            state.handle_result(task.start())?;
            state.tasks.insert(task_id, task);
            let event = state.scheduler_state_event();
            drop(state);
            emit_scheduler_state_event(event, &self.stats);
            return Ok(());
        }

        state.pending_tasks.push(TaskEntry {
            task_id,
            priority: task.priority,
            reserved: task.is_reserved(),
        });
        state.tasks.insert(task_id, task);
        let event = state.scheduler_state_event();
        drop(state);
        emit_scheduler_state_event(event, &self.stats);
        Ok(())
    }

    pub(super) fn submit(
        self: Arc<Self>,
        range: Range<u64>,
        priority: u128,
        run_fn: RunFn,
        bypass_backpressure: bool,
    ) -> Result<TaskHandle> {
        log::trace!(
            "Submitting I/O task with range {:?}, priority {:?}",
            range,
            priority
        );
        let mut state = self.state.lock().unwrap();
        let task_id = state.next_task_id;
        state.next_task_id += 1;

        let task = IoTask {
            id: task_id,
            num_bytes: range.end - range.start,
            priority,
            bypass_backpressure,
            state: TaskState::Initial {
                idle_waker: None,
                run_fn,
            },
        };
        self.push(task, state)?;
        Ok(TaskHandle {
            task_id,
            queue: self,
        })
    }

    // When a task completes we should check to see if any other tasks are now runnable
    fn on_task_complete(&self, mut state: MutexGuard<IoQueueState>) -> Result<()> {
        let result = {
            let state_ref = &mut *state;
            let mut task_result = TaskResult::Ok(());
            while !state_ref.pending_tasks.is_empty() {
                // Unwrap safe here since we just checked the queue is not empty
                let task_id = state_ref.pending_tasks.peek().unwrap().task_id;
                let Some(task) = state_ref.tasks.get_mut(&task_id) else {
                    // The caller dropped this task's handle (see `abandon`); discard the
                    // stale queue entry instead of spinning on it.
                    state_ref.pending_tasks.pop();
                    continue;
                };
                if !task.is_reserved() {
                    let Some(reservation) = state_ref
                        .backpressure_throttle
                        .try_acquire(task.num_bytes, task.priority)
                    else {
                        break;
                    };
                    if let Err(e) = task.reserve(reservation) {
                        task_result = Err(e);
                        break;
                    }
                }
                state_ref.pending_tasks.pop();
                if let Err(e) = task.start() {
                    task_result = Err(e);
                    break;
                }
            }
            state_ref.handle_result(task_result)
        };
        let event = state.scheduler_state_event();
        drop(state);
        emit_scheduler_state_event(event, &self.stats);
        result
    }

    fn poll(&self, task_id: u64, cx: &mut Context<'_>) -> Poll<Result<Bytes>> {
        let mut state = self.state.lock().unwrap();
        let Some(task) = state.tasks.get_mut(&task_id) else {
            // This should never happen and indicates a bug
            return Poll::Ready(Err(Error::internal(format!(
                "Task with id {} was lost",
                task_id
            ))));
        };
        match task.poll(cx) {
            Poll::Ready(_) => {
                let task = state.tasks.remove(&task_id).unwrap();
                let (bytes, reservation) = task.consume()?;
                state.backpressure_throttle.release(reservation);
                // We run on_task_complete even if not newly finished because we released the backpressure reservation
                match self.on_task_complete(state) {
                    Ok(_) => Poll::Ready(bytes),
                    Err(e) => Poll::Ready(Err(e)),
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }

    pub(super) fn close(&self) {
        let event = {
            let mut state = self.state.lock().unwrap();
            for task in std::mem::take(&mut state.tasks).values_mut() {
                task.cancel();
            }
            state.scheduler_state_event()
        };
        emit_scheduler_state_event(event, &self.stats);
    }

    // Called when a caller drops a task's handle before the task finishes.  Removes
    // the task and returns any backpressure reservation it holds to the budget, then
    // re-checks the queue so newly-affordable tasks can start.  Unlike the standard
    // release path (`poll`), this runs without the task being polled to completion,
    // so a cancelled read does not leak its reservation.
    fn abandon(&self, task_id: u64) {
        let mut state = self.state.lock().unwrap();
        let Some(task) = state.tasks.remove(&task_id) else {
            // Already consumed by `poll`; nothing to release.
            return;
        };

        if let Some(reservation) = task.state.backpressure_reservation() {
            state.backpressure_throttle.release(reservation);
        }
        // Freed budget may make queued tasks runnable; there is no caller to surface
        // an error to here.
        let _ = self.on_task_complete(state);
    }
}

pub(super) struct TaskHandle {
    task_id: u64,
    queue: Arc<IoQueue>,
}

impl Future for TaskHandle {
    type Output = Result<Bytes>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.queue.poll(self.task_id, cx)
    }
}

impl Drop for TaskHandle {
    fn drop(&mut self) {
        self.queue.abandon(self.task_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn test_priority_ordering() {
        // Backpressure budget of 10 bytes: only one 10-byte task runs at a time.
        let queue = Arc::new(IoQueue::new(128, 10, IoStats::default()));

        // Records the priority of each task when its run_fn is invoked (i.e. when
        // the task transitions to Running).
        let start_order: Arc<Mutex<Vec<u128>>> = Arc::new(Mutex::new(Vec::new()));

        // Helper: builds a RunFn that records `prio` in start_order and then
        // waits on the oneshot receiver for its result bytes.
        let make_run_fn =
            |prio: u128, rx: oneshot::Receiver<Bytes>, order: Arc<Mutex<Vec<u128>>>| -> RunFn {
                Box::new(move || {
                    order.lock().unwrap().push(prio);
                    Box::pin(async move { Ok(rx.await.unwrap()) })
                })
            };

        // Submit a blocker task (priority 0, 10 bytes).
        // It starts immediately because there is enough backpressure budget.
        let (blocker_tx, blocker_rx) = oneshot::channel();
        let blocker = queue
            .clone()
            .submit(
                0..10,
                0,
                make_run_fn(0, blocker_rx, start_order.clone()),
                false,
            )
            .unwrap();

        // Submit four tasks with out-of-order priorities.
        // All are queued because the blocker consumed the full budget.
        let (tx_30, rx_30) = oneshot::channel();
        let h30 = queue
            .clone()
            .submit(
                0..10,
                30,
                make_run_fn(30, rx_30, start_order.clone()),
                false,
            )
            .unwrap();

        let (tx_10, rx_10) = oneshot::channel();
        let h10 = queue
            .clone()
            .submit(
                0..10,
                10,
                make_run_fn(10, rx_10, start_order.clone()),
                false,
            )
            .unwrap();

        let (tx_50, rx_50) = oneshot::channel();
        let h50 = queue
            .clone()
            .submit(
                0..10,
                50,
                make_run_fn(50, rx_50, start_order.clone()),
                false,
            )
            .unwrap();

        let (tx_20, rx_20) = oneshot::channel();
        let h20 = queue
            .clone()
            .submit(
                0..10,
                20,
                make_run_fn(20, rx_20, start_order.clone()),
                false,
            )
            .unwrap();

        // Only the blocker has started so far.
        assert_eq!(*start_order.lock().unwrap(), vec![0]);

        // Complete the blocker -> frees budget -> starts priority 10 (lowest value = highest priority).
        blocker_tx.send(Bytes::from_static(b"x")).unwrap();
        blocker.await.unwrap();
        assert_eq!(*start_order.lock().unwrap(), vec![0, 10]);

        // Complete priority 10 -> starts priority 20.
        tx_10.send(Bytes::from_static(b"x")).unwrap();
        h10.await.unwrap();
        assert_eq!(*start_order.lock().unwrap(), vec![0, 10, 20]);

        // Complete priority 20 -> starts priority 30.
        tx_20.send(Bytes::from_static(b"x")).unwrap();
        h20.await.unwrap();
        assert_eq!(*start_order.lock().unwrap(), vec![0, 10, 20, 30]);

        // Complete priority 30 -> starts priority 50.
        tx_30.send(Bytes::from_static(b"x")).unwrap();
        h30.await.unwrap();
        assert_eq!(*start_order.lock().unwrap(), vec![0, 10, 20, 30, 50]);

        // Complete priority 50 -> no more pending tasks.
        tx_50.send(Bytes::from_static(b"x")).unwrap();
        h50.await.unwrap();
        assert_eq!(*start_order.lock().unwrap(), vec![0, 10, 20, 30, 50]);
    }

    #[tokio::test]
    async fn test_zero_buffer_bypasses_backpressure() {
        // Budget = 0 sets no_backpressure = true, so all tasks start immediately
        // regardless of how many bytes are "outstanding".
        let queue = Arc::new(IoQueue::new(128, 0, IoStats::default()));
        let start_order: Arc<Mutex<Vec<u128>>> = Arc::new(Mutex::new(Vec::new()));

        let make_run_fn =
            |prio: u128, rx: oneshot::Receiver<Bytes>, order: Arc<Mutex<Vec<u128>>>| -> RunFn {
                Box::new(move || {
                    order.lock().unwrap().push(prio);
                    Box::pin(async move { Ok(rx.await.unwrap()) })
                })
            };

        let (tx0, rx0) = oneshot::channel();
        let h0 = queue
            .clone()
            .submit(0..10, 0, make_run_fn(0, rx0, start_order.clone()), false)
            .unwrap();
        let (tx1, rx1) = oneshot::channel();
        let h1 = queue
            .clone()
            .submit(0..10, 1, make_run_fn(1, rx1, start_order.clone()), false)
            .unwrap();
        let (tx2, rx2) = oneshot::channel();
        let h2 = queue
            .clone()
            .submit(0..10, 2, make_run_fn(2, rx2, start_order.clone()), false)
            .unwrap();

        // All three tasks start immediately — no backpressure budget check when max_bytes=0.
        assert_eq!(*start_order.lock().unwrap(), vec![0, 1, 2]);

        tx0.send(Bytes::from_static(b"done")).unwrap();
        tx1.send(Bytes::from_static(b"done")).unwrap();
        tx2.send(Bytes::from_static(b"done")).unwrap();
        h0.await.unwrap();
        h1.await.unwrap();
        h2.await.unwrap();
    }

    #[tokio::test]
    async fn test_bypass_flag_proceeds_past_exhausted_budget() {
        // Budget of 10 bytes. A blocker task fills it. A task with bypass=true starts
        // immediately despite the exhausted budget; a normal task stays queued.
        let queue = Arc::new(IoQueue::new(128, 10, IoStats::default()));
        let start_order: Arc<Mutex<Vec<u128>>> = Arc::new(Mutex::new(Vec::new()));

        let make_run_fn =
            |prio: u128, rx: oneshot::Receiver<Bytes>, order: Arc<Mutex<Vec<u128>>>| -> RunFn {
                Box::new(move || {
                    order.lock().unwrap().push(prio);
                    Box::pin(async move { Ok(rx.await.unwrap()) })
                })
            };

        // Blocker (priority 0, 10 bytes): fills the budget.
        let (blocker_tx, blocker_rx) = oneshot::channel();
        let blocker = queue
            .clone()
            .submit(
                0..10,
                0,
                make_run_fn(0, blocker_rx, start_order.clone()),
                false,
            )
            .unwrap();

        // Normal (priority 1, 10 bytes): blocked — budget exhausted, no priority bypass.
        let (normal_tx, normal_rx) = oneshot::channel();
        let normal = queue
            .clone()
            .submit(
                0..10,
                1,
                make_run_fn(1, normal_rx, start_order.clone()),
                false,
            )
            .unwrap();

        // Bypass (priority 2, 10 bytes): starts immediately via force_acquire.
        let (bypass_tx, bypass_rx) = oneshot::channel();
        let bypass = queue
            .clone()
            .submit(
                0..10,
                2,
                make_run_fn(2, bypass_rx, start_order.clone()),
                true,
            )
            .unwrap();

        // Blocker (0) and bypass (2) have started; normal (1) is still queued.
        assert_eq!(*start_order.lock().unwrap(), vec![0, 2]);

        // Completing the blocker frees the budget and unblocks the normal task.
        blocker_tx.send(Bytes::from_static(b"done")).unwrap();
        blocker.await.unwrap();
        assert_eq!(*start_order.lock().unwrap(), vec![0, 2, 1]);

        bypass_tx.send(Bytes::from_static(b"done")).unwrap();
        bypass.await.unwrap();
        normal_tx.send(Bytes::from_static(b"done")).unwrap();
        normal.await.unwrap();
    }

    #[test]
    fn test_same_priority_reservation_continues_after_higher_priority() {
        let mut throttle = SimpleBackpressureThrottle::new(10, 128);

        let low_priority_first = throttle.try_acquire(6, 10).unwrap();
        let high_priority = throttle.try_acquire(4, 0).unwrap();
        let low_priority_next = throttle.try_acquire(6, 10);

        assert!(
            low_priority_next.is_some(),
            "chunks from an already admitted logical request should continue"
        );

        throttle.release(low_priority_first);
        throttle.release(high_priority);
        throttle.release(low_priority_next.unwrap());
    }
}
