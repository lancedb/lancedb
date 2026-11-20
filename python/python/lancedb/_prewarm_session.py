# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Drives a prewarm run for one StreamingDataset iteration.

A rank's main process owns a [Session][lancedb._prewarm_session.Session]: it
opens the run, joins it, reports the wave the rank is reading, and on rank 0
announces upcoming waves. Whichever processes read the rows (DataLoader
workers, or the main process itself) each use a
[WaveReader][lancedb._prewarm_session.WaveReader], which waits for a wave to
be warm before reading it and re-warms rows a cache-only read finds missing.

The two sides share only a picklable
[ReaderState][lancedb._prewarm_session.ReaderState] and an array with one
slot per split, in which readers record the wave each split has reached.
"""

import asyncio
import concurrent.futures
import hashlib
import json
import logging
import os
import threading
import time
import uuid
from typing import Any, Callable, Mapping, NamedTuple, Optional, Sequence, TypeVar

import pyarrow as pa

from ._prewarm_waves import WavePlan
from .background_loop import LOOP
from .exceptions import NotResidentError, PrewarmRunError

logger = logging.getLogger(__name__)

T = TypeVar("T")

# How long a reader waits for one wave to warm before it fails the read. A
# placeholder until the time a replaced supervisor takes to resume is measured.
DEFAULT_WAIT_READY_TIMEOUT_S = 600.0

# How often the session reports the rank's wave and checks whether rank 0 may
# announce more.
_POLL_INTERVAL_S = 0.5

# A wait for a wave is logged once it has taken this long.
_SLOW_WAIT_LOG_S = 5.0

# How long closing a session waits for an announce call in flight.
_ANNOUNCER_JOIN_TIMEOUT_S = 30.0

# How often a waiting read checks whether its iteration has ended.
_CLOSE_POLL_S = 0.2

# Slot value of a split with nothing left to fetch.
SPLIT_DONE = 2**62

_BASE_VERSION_KEY = b"base_version"

_BEYOND_HORIZON = "beyond_horizon"
_CACHE_ONLY_UNSUPPORTED = "cache_only_unsupported"
# Client-side: rank 0 stopped announcing, so later waves will never warm.
ANNOUNCER_STOPPED = "announcer_stopped"


class ReaderState(NamedTuple):
    """What a reading process needs to take part in a run another process
    opened."""

    run_id: str
    version: int
    wave_positions: int
    wait_ready_timeout_s: float


class _RemoteBackend:
    """The run operations of a LanceDB Enterprise table."""

    def __init__(self, table):
        self._table = table

    def open_run(
        self,
        run_key: str,
        *,
        version: int,
        world_size: int,
        columns: Optional[Sequence[str]],
        row_addresses: bool,
        num_splits: int,
    ):
        return self._table.open_prewarm_run(
            run_key,
            version=version,
            world_size=world_size,
            columns=list(columns) if columns is not None else None,
            row_addresses=row_addresses,
            num_splits=num_splits,
        )

    def reader(self, run_id: str, version: int, rank: int):
        """An async reader for ``rank`` of a run another process opened."""
        return self._table._table.attach_prewarm_run(run_id, version).reader(rank)

    def cache_only_table(self, version: int):
        """A cache-only handle at ``version``, leaving the caller's table as
        it was."""
        from .remote.table import RemoteTable

        pinned = RemoteTable.__new__(RemoteTable)
        pinned.__setstate__(self._table.__getstate__())
        pinned.checkout(version)
        return pinned.cache_only()


def backend_for(table) -> Optional[_RemoteBackend]:
    """The run operations of ``table``, or ``None`` for a table that has none."""
    from .remote.table import RemoteTable

    if isinstance(table, RemoteTable) and table._connection_state is not None:
        return _RemoteBackend(table)
    return None


def unavailable(reason: str, *, level: int = logging.WARNING) -> None:
    """Report that an iteration reads without a prewarm run, and why."""
    logger.log(level, "StreamingDataset is reading without a prewarm run: %s", reason)


def base_version(permutation: pa.Table) -> Optional[int]:
    """The base table version the permutation was built against."""
    raw = (permutation.schema.metadata or {}).get(_BASE_VERSION_KEY)
    return int(raw) if raw is not None else None


def _attempt_id(rank: int, world_size: int) -> Optional[str]:
    """An id every rank of this training attempt agrees on and no other
    attempt shares, or ``None`` when there is no way to get one."""
    import torch.distributed as dist

    if (
        dist.is_available()
        and dist.is_initialized()
        and dist.get_world_size() == world_size
        and dist.get_rank() == rank
    ):
        # A collective: every rank starts an epoch's iteration, so every rank
        # reaches this call.
        box = [uuid.uuid4().hex if rank == 0 else None]
        dist.broadcast_object_list(box, src=0)
        return box[0]
    # torchrun names every launch "none" unless it is given a run id.
    torchrun_id = os.environ.get("TORCHELASTIC_RUN_ID")
    if torchrun_id and torchrun_id != "none":
        restarts = os.environ.get("TORCHELASTIC_RESTART_COUNT", "0")
        return f"{torchrun_id}-{restarts}"
    if world_size == 1:
        return uuid.uuid4().hex
    return None


def _run_key(attempt_id: str, parts: Sequence[Any]) -> str:
    payload = json.dumps([attempt_id, *parts], sort_keys=True, default=str)
    return hashlib.sha256(payload.encode()).hexdigest()


class Session:
    """A rank's membership in a run, for one iteration of the dataset."""

    def __init__(
        self,
        rank_handle,
        plan: WavePlan,
        state: ReaderState,
        slots,
        rank_splits: Sequence[int],
        *,
        announce: bool,
        announcer_stopped,
    ):
        self.state = state
        self._rank = rank_handle
        self._plan = plan
        self._slots = slots
        self._announcer_stopped = announcer_stopped
        self._announcer_stopped.value = False
        self._rank_splits = list(rank_splits)
        self._stop = threading.Event()
        self._reporter = threading.Thread(
            target=self._report_loop, name="lancedb-prewarm-report", daemon=True
        )
        self._announcer = (
            threading.Thread(
                target=self._announce_loop, name="lancedb-prewarm-announce", daemon=True
            )
            if announce
            else None
        )
        self._reporter.start()
        if self._announcer is not None:
            self._announcer.start()

    def _report_loop(self) -> None:
        while not self._stop.wait(_POLL_INTERVAL_S):
            wave = min(self._slots[split] for split in self._rank_splits)
            if wave < SPLIT_DONE:
                self._rank.report_wave(wave)

    def _announce_loop(self) -> None:
        wave = self._plan.first_wave
        try:
            while wave < self._plan.num_waves and not self._stop.is_set():
                if wave > self._rank.view().announce_limit:
                    self._stop.wait(_POLL_INTERVAL_S)
                    continue
                try:
                    # One wave per call: a wave of a row-by-row shuffle is
                    # already megabytes of ranges.
                    self._rank.announce({wave: self._plan.ranges(wave)})
                except PrewarmRunError as error:
                    if error.code != _BEYOND_HORIZON:
                        raise
                    # The limit moved down since the last view, when a rank
                    # joined at a lower wave. The next view allows it again.
                    self._stop.wait(_POLL_INTERVAL_S)
                    continue
                wave += 1
        except Exception:
            logger.exception(
                "prewarm run %s: rank 0 stopped announcing at wave %d",
                self.state.run_id,
                wave,
            )
            # Without this, this rank's readers would wait out their whole
            # timeout on a wave nobody will announce. The flag is shared
            # memory because they may be other processes. It reaches only
            # this rank: the run has no call that fails it for the others,
            # which end when the launcher tears the job down or their own
            # wait times out.
            if not self._stop.is_set():
                self._announcer_stopped.value = True

    def close(self) -> None:
        """Leave the run. Never raises, so it cannot hide a training error."""
        self._stop.set()
        self._reporter.join()
        if self._announcer is not None:
            # A call made after `close` would join the rank again.
            self._announcer.join(_ANNOUNCER_JOIN_TIMEOUT_S)
        try:
            self._rank.close()
        except Exception:
            logger.warning(
                "prewarm run %s: closing rank %d failed",
                self.state.run_id,
                self._rank.rank,
                exc_info=True,
            )
        logger.info(
            "prewarm run %s: rank %d finished", self.state.run_id, self._rank.rank
        )


def open_session(
    *,
    table,
    permutation: pa.Table,
    num_splits: int,
    rank: int,
    world_size: int,
    rank_splits: Sequence[int],
    columns: Optional[Sequence[str]],
    start_positions: Mapping[int, int],
    slots,
    key_parts: Sequence[Any],
    wait_ready_timeout_s: float,
    announcer_stopped,
) -> Optional[Session]:
    """Open and join the run for one iteration.

    Returns ``None``, after reporting why, when the iteration has to read
    without a run. Raises when the run exists but cannot be used, such as
    after it failed.

    ``start_positions`` holds the position every split resumes from. Other
    ranks' splits may be lower bounds.

    ``announcer_stopped`` is a shared flag the session sets when this rank
    was announcing and had to stop; this rank's readers watch it.
    """
    backend = backend_for(table)
    if backend is None:
        # Not worth a warning: prewarm runs are a LanceDB Enterprise feature.
        unavailable("the table is not a LanceDB Enterprise table", level=logging.DEBUG)
        return None
    version = base_version(permutation)
    if version is None:
        unavailable("the permutation does not record the table version it read")
        return None
    attempt_id = _attempt_id(rank, world_size)
    if attempt_id is None:
        unavailable(
            "the ranks have no shared id for this training attempt; initialize "
            "torch.distributed or launch with `torchrun --rdzv-id`"
        )
        return None
    run_key = _run_key(attempt_id, [table.name, version, *key_parts])
    try:
        run = backend.open_run(
            run_key,
            version=version,
            world_size=world_size,
            columns=columns,
            row_addresses=WavePlan.has_row_addresses(permutation),
            num_splits=num_splits,
        )
    except NotImplementedError:
        unavailable("the server does not support prewarm runs")
        return None
    except PrewarmRunError as error:
        if error.code != _CACHE_ONLY_UNSUPPORTED:
            raise
        unavailable(f"the table cannot be read cache-only ({error})")
        return None

    plan = WavePlan(permutation, num_splits, run.wave_positions, start_positions)
    waves = [
        plan.wave_of(start_positions.get(split, 0))
        if start_positions.get(split, 0) < plan.split_rows(split)
        else SPLIT_DONE
        for split in range(num_splits)
    ]
    for split in rank_splits:
        slots[split] = waves[split]
    first_wave = min(waves[split] for split in rank_splits)
    if first_wave == SPLIT_DONE:
        first_wave = plan.num_waves
    rank_handle = run.join(rank, first_wave)
    view = rank_handle.view()
    logger.info(
        "prewarm run %s: rank %d of %d reading table %s at version %d from wave %d "
        "of %d (%d positions per split in a wave, %d waves warmed ahead)",
        run.run_id,
        rank,
        world_size,
        table.name,
        run.version,
        first_wave,
        plan.num_waves,
        plan.wave_positions,
        view.horizon,
    )
    state = ReaderState(
        run.run_id, run.version, plan.wave_positions, wait_ready_timeout_s
    )
    return Session(
        rank_handle,
        plan,
        state,
        slots,
        rank_splits,
        announce=rank == 0,
        announcer_stopped=announcer_stopped,
    )


class ReaderClosed(RuntimeError):
    """A read was abandoned because its iteration ended."""


class WaveReader:
    """One process's reads in a run: waits on waves and re-warms misses."""

    def __init__(
        self,
        state: ReaderState,
        *,
        table,
        permutation: pa.Table,
        num_splits: int,
        rank: int,
        slots,
        announcer_stopped,
    ):
        backend = backend_for(table)
        if backend is None:
            raise RuntimeError(
                f"prewarm run {state.run_id}: the table has no run support in "
                "this process"
            )
        self.table = backend.cache_only_table(state.version)
        """The table to read through: its queries are cache-only."""
        self._run_id = state.run_id
        self._reader = backend.reader(state.run_id, state.version, rank)
        self._closed = threading.Event()
        self._plan = WavePlan(permutation, num_splits, state.wave_positions)
        self._timeout = state.wait_ready_timeout_s
        self._slots = slots
        self._announcer_stopped = announcer_stopped
        self._lock = threading.Lock()
        self._wave_locks: dict[int, threading.Lock] = {}
        self._ready: set[int] = set()
        self._read_any = False

    def fetch_end(self, split: int, position: int, limit: int) -> int:
        """Where a fetch of up to ``limit`` positions from ``position`` must
        end, so that it stays inside one wave."""
        wave_end = self._plan.wave_end(split, self._plan.wave_of(position))
        return min(position + limit, wave_end)

    def note_position(self, split: int, position: Optional[int]) -> None:
        """Record the oldest position ``split`` still has to fetch, or
        ``None`` once it has fetched everything."""
        self._slots[split] = (
            SPLIT_DONE if position is None else self._plan.wave_of(position)
        )

    def read(self, split: int, start: int, end: int, fetch: Callable[[], T]) -> T:
        """Run ``fetch``, which reads positions ``[start, end)`` of ``split``,
        once its wave is warm; re-warm and retry while rows are missing.

        There is no retry limit here: the run fails itself when re-warming
        stops making progress, and that failure raises from the wait.
        """
        wave = self._plan.wave_of(start)
        self._wait(wave)
        while True:
            hit, result = self._attempt(fetch)
            if hit:
                return result
            logger.warning(
                "prewarm run %s: rows of wave %d were not in the cache; "
                "warming them again",
                self._run_id,
                wave,
            )
            self._call(
                self._reader.rewarm(wave, self._plan.fetch_ranges(split, start, end))
            )
            self._call(self._reader.wait_ready(wave, self._timeout))

    def close(self) -> None:
        """End the iteration's reads. A fetch can be waiting on a wave that
        will never warm, because the iteration that would have read up to it
        stopped early; this makes such a wait raise instead of running out
        its timeout."""
        self._closed.set()

    def _call(self, call) -> None:
        """Run a run call to its end, or until this reader is closed."""
        future = asyncio.run_coroutine_threadsafe(call, LOOP.loop)
        while True:
            if self._closed.is_set():
                future.cancel()
                raise ReaderClosed(f"prewarm run {self._run_id}: the iteration ended")
            if self._announcer_stopped.value:
                future.cancel()
                raise PrewarmRunError(
                    f"prewarm run {self._run_id}: rank 0 stopped announcing "
                    "waves, so the rows still to read will not be warmed; its "
                    "log has the cause",
                    self._run_id,
                    ANNOUNCER_STOPPED,
                )
            done, _ = concurrent.futures.wait([future], timeout=_CLOSE_POLL_S)
            if done:
                return future.result()

    @staticmethod
    def _attempt(fetch: Callable[[], T]) -> tuple[bool, Optional[T]]:
        """Run ``fetch``; ``(False, None)`` when the cache lacked its rows."""
        try:
            return True, fetch()
        except NotResidentError:
            return False, None

    def _wait(self, wave: int) -> None:
        # Every split's fetches reach a wave together; one of them polls and
        # the rest wait on it.
        with self._lock:
            wave_lock = self._wave_locks.setdefault(wave, threading.Lock())
            first_read = not self._read_any
            self._read_any = True
        with wave_lock:
            if wave in self._ready:
                return
            if first_read:
                logger.info(
                    "prewarm run %s: waiting for wave %d to warm before reading",
                    self._run_id,
                    wave,
                )
            started = time.monotonic()
            self._call(self._reader.wait_ready(wave, self._timeout))
            waited = time.monotonic() - started
            if first_read:
                logger.info(
                    "prewarm run %s: wave %d is warm after %.1fs; reading while "
                    "later waves warm",
                    self._run_id,
                    wave,
                    waited,
                )
            elif waited >= _SLOW_WAIT_LOG_S:
                logger.info(
                    "prewarm run %s: waited %.1fs for wave %d to warm",
                    self._run_id,
                    waited,
                    wave,
                )
            self._ready.add(wave)
