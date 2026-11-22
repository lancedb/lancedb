# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""StreamingDataset driving a prewarm run, against a scripted run."""

import asyncio
import logging
import threading
import time
from types import SimpleNamespace

import lancedb
import pyarrow as pa
import pytest
from lancedb import _prewarm_session
from lancedb._prewarm_waves import WavePlan
from lancedb.exceptions import (
    NotResidentError,
    PlanExecutorUnavailableError,
    PrewarmRunError,
)

torch = pytest.importorskip("torch")
streaming = pytest.importorskip("lancedb.streaming")
StreamingDataset = streaming.StreamingDataset
StreamingDataLoader = streaming.StreamingDataLoader

NUM_ROWS = 160
WAVE_POSITIONS = 8
HORIZON = 2


class FakeRun:
    """A run held in memory: a wave is warm as soon as it is announced, and
    the view follows the ranks' reports without waiting for a heartbeat."""

    run_id = "run-1"

    def __init__(self, version, *, strict_waits=True):
        self.version = version
        self.wave_positions = WAVE_POSITIONS
        # Readers in a forked worker see a copy that announcements never reach.
        self.strict_waits = strict_waits
        self.lock = threading.Lock()
        self.joins = []
        self.reports = {}
        self.announced = {}
        self.beyond_horizon = 0
        self.rewarms = []
        self.closed = []
        self.stale_views = 0
        self.failure = None
        self.too_large_from = None
        # When set, a rank's view is the one its last refresh brought, as it
        # is between the heartbeats of a real run.
        self.views_need_refresh = False
        self.refreshes = 0

    def reading_wave(self):
        with self.lock:
            return min(self.reports.values(), default=0)

    def join(self, rank, first_wave):
        with self.lock:
            self.joins.append((rank, first_wave))
            self.reports[rank] = first_wave
        return FakeRank(self, rank)


class FakeRank:
    def __init__(self, run, rank):
        self.run = run
        self.rank = rank
        self.limit = run.reading_wave() + HORIZON

    def refresh_view(self):
        with self.run.lock:
            self.run.refreshes += 1
        self.limit = self.run.reading_wave() + HORIZON
        return SimpleNamespace(announce_limit=self.limit, horizon=HORIZON)

    def view(self):
        if self.run.views_need_refresh:
            return SimpleNamespace(announce_limit=self.limit, horizon=HORIZON)
        limit = self.run.reading_wave() + HORIZON
        with self.run.lock:
            if self.run.stale_views:
                self.run.stale_views -= 1
                limit += 1
        return SimpleNamespace(announce_limit=limit, horizon=HORIZON)

    def announce(self, waves):
        limit = self.run.reading_wave() + HORIZON
        with self.run.lock:
            if any(wave > limit for wave in waves):
                self.run.beyond_horizon += 1
                raise PrewarmRunError(
                    "too far ahead", self.run.run_id, "beyond_horizon"
                )
            too_large = self.run.too_large_from
            if too_large is not None and any(wave >= too_large for wave in waves):
                raise PrewarmRunError(
                    "the call is too large", self.run.run_id, "call_too_large"
                )
            self.run.announced.update(waves)

    def report_wave(self, wave):
        with self.run.lock:
            self.run.reports[self.rank] = max(self.run.reports[self.rank], wave)

    def close(self):
        with self.run.lock:
            self.run.closed.append(self.rank)
            # A rank that left no longer holds the others back.
            del self.run.reports[self.rank]


class FakeReader:
    def __init__(self, run):
        self.run = run

    async def wait_ready(self, wave, timeout):
        deadline = time.monotonic() + timeout
        while self.run.strict_waits:
            with self.run.lock:
                if self.run.failure is not None:
                    raise PrewarmRunError(
                        "the run failed", self.run.run_id, self.run.failure
                    )
                if wave in self.run.announced:
                    return
            if time.monotonic() > deadline:
                raise RuntimeError(f"wave {wave} was not warm in time")
            await asyncio.sleep(0.002)

    async def rewarm(self, wave, ranges):
        with self.run.lock:
            self.run.rewarms.append((wave, list(ranges)))


class FakeBackend:
    """Run operations over a local table, which stands in for the cache."""

    def __init__(self, table, run):
        self.table = table
        self.run = run
        self.keys = []

    def open_run(
        self, run_key, *, version, world_size, columns, row_addresses, num_splits
    ):
        assert version == self.run.version
        self.keys.append(run_key)
        self.row_addresses = row_addresses
        self.num_splits = num_splits
        return self.run

    def reader(self, run_id, version, rank):
        assert (run_id, version) == (self.run.run_id, self.run.version)
        return FakeReader(self.run)

    def cache_only_table(self, version):
        return self.table


@pytest.fixture
def table(tmp_path):
    db = lancedb.connect(tmp_path)
    table = db.create_table("data", pa.table({"id": list(range(40))}))
    for start in range(40, NUM_ROWS, 40):
        table.add(pa.table({"id": list(range(start, start + 40))}))
    return table


@pytest.fixture
def run(table, monkeypatch):
    """Give every table a run, and make the session's polling fast."""
    run = FakeRun(table.version)
    backend = FakeBackend(table, run)
    run.backend = backend
    monkeypatch.setattr(_prewarm_session, "backend_for", lambda _table: backend)
    monkeypatch.setattr(_prewarm_session, "_POLL_INTERVAL_S", 0.005)
    monkeypatch.setattr(_prewarm_session, "_REPORT_INTERVAL_S", 0.005)
    monkeypatch.setattr(_prewarm_session, "_VIEW_CHECK_INTERVAL_S", 0.002)
    monkeypatch.setattr(_prewarm_session, "_UNAVAILABLE_BACKOFF_S", 0.005)
    monkeypatch.setattr(_prewarm_session, "_UNAVAILABLE_BACKOFF_MAX_S", 0.02)
    return run


def dataset(table, **kwargs):
    kwargs.setdefault("num_splits", 4)
    kwargs.setdefault("shuffle_seed", 7)
    kwargs.setdefault("read_batch_size", 5)
    kwargs.setdefault("_wait_ready_timeout_s", 20)
    return StreamingDataset(table, **kwargs)


def ids(samples):
    return [int(sample["id"]) for sample in samples]


def plan_of(ds, start_positions=None):
    return WavePlan(
        ds._perm_table.to_arrow(), ds._num_splits, WAVE_POSITIONS, start_positions
    )


def test_a_run_changes_no_sample_and_announces_every_wave(table, run):
    expected = ids(dataset(table, _prewarm=False))
    ds = dataset(table)
    assert ids(ds) == expected
    assert sorted(expected) == list(range(NUM_ROWS))

    plan = plan_of(ds)
    assert plan.num_waves == 5
    assert run.joins == [(0, 0)]
    assert run.backend.num_splits == 4
    assert run.announced == {wave: plan.ranges(wave) for wave in range(5)}
    assert run.beyond_horizon == 0
    assert run.rewarms == []
    assert run.closed == [0]
    assert ds._prewarm_session is None and ds._prewarm_state is None


def test_a_table_with_stable_row_ids_is_warmed_by_row_address(tmp_path, monkeypatch):
    """With stable row ids a row id is not where the row is stored, so the
    dataset records row addresses and cuts every wave from them."""
    db = lancedb.connect(tmp_path)
    stable = {"new_table_enable_stable_row_ids": "true"}
    table = db.create_table(
        "data", pa.table({"id": list(range(40))}), storage_options=stable
    )
    for start in range(40, NUM_ROWS, 40):
        table.add(pa.table({"id": list(range(start, start + 40))}))
    # Compaction moves every row; the row ids stay.
    table.optimize()
    rows = table.search().select(["_rowid", "_rowaddr"]).to_arrow()
    address_of = dict(zip(rows["_rowid"].to_pylist(), rows["_rowaddr"].to_pylist()))
    assert any(row_id != addr for row_id, addr in address_of.items())

    run = FakeRun(table.version)
    backend = FakeBackend(table, run)
    monkeypatch.setattr(_prewarm_session, "backend_for", lambda _table: backend)
    monkeypatch.setattr(_prewarm_session, "_POLL_INTERVAL_S", 0.005)
    monkeypatch.setattr(_prewarm_session, "_REPORT_INTERVAL_S", 0.005)
    monkeypatch.setattr(_prewarm_session, "_VIEW_CHECK_INTERVAL_S", 0.002)
    monkeypatch.setattr(_prewarm_session, "_UNAVAILABLE_BACKOFF_S", 0.005)
    monkeypatch.setattr(_prewarm_session, "_UNAVAILABLE_BACKOFF_MAX_S", 0.02)

    ds = dataset(table)
    perm = ds._perm_table.to_arrow()
    assert perm.column_names == ["row_id", "row_addr", "split_id"]
    assert perm["row_addr"].to_pylist() == [
        address_of[row_id] for row_id in perm["row_id"].to_pylist()
    ]

    assert sorted(ids(ds)) == list(range(NUM_ROWS))
    assert backend.row_addresses is True
    announced = {
        (r.fragment_id, offset)
        for ranges in run.announced.values()
        for r in ranges
        for offset in range(r.start, r.end)
    }
    assert announced == {
        (addr >> 32, addr & 0xFFFFFFFF) for addr in address_of.values()
    }


def test_a_dataset_without_a_run_records_no_row_addresses(table):
    assert dataset(table)._perm_table.to_arrow().column_names == ["row_id", "split_id"]


def test_a_fetch_never_crosses_a_wave(table, run, monkeypatch):
    fetches = []
    read = _prewarm_session.WaveReader.read

    def recording_read(self, split, start, end, fetch):
        fetches.append((start, end))
        return read(self, split, start, end, fetch)

    monkeypatch.setattr(_prewarm_session.WaveReader, "read", recording_read)
    ids(dataset(table))
    assert fetches
    # read_batch_size is 5 and a wave holds 8 positions, so fetches get cut.
    assert all(
        start // WAVE_POSITIONS == (end - 1) // WAVE_POSITIONS for start, end in fetches
    )
    assert any(end - start < 5 for start, end in fetches)


def test_a_missed_read_is_rewarmed_and_retried(table, run, monkeypatch):
    expected = ids(dataset(table, _prewarm=False))
    getitems = streaming.Permutation.__getitems__
    missed = set()

    def flaky_getitems(self, indices):
        # Every fetch misses once: the first attempt finds the cache cold.
        key = (id(self), tuple(indices))
        if key not in missed:
            missed.add(key)
            raise NotResidentError("not in the cache", "request-1")
        return getitems(self, indices)

    ds = dataset(table)
    monkeypatch.setattr(streaming.Permutation, "__getitems__", flaky_getitems)
    assert ids(ds) == expected

    plan = plan_of(ds)
    assert len(run.rewarms) == len(missed)
    for wave, ranges in run.rewarms:
        # A re-warm asks only for rows of its own wave.
        wave_rows = {
            (r.fragment_id, offset)
            for r in plan.ranges(wave)
            for offset in range(r.start, r.end)
        }
        rows = {(r[0], offset) for r in ranges for offset in range(r[1], r[2])}
        assert rows and rows <= wave_rows


def unavailable(message="no plan executor can serve this read now"):
    return PlanExecutorUnavailableError(message, "request-1")


def test_a_read_no_plan_executor_can_serve_is_retried_without_a_rewarm(
    table, run, monkeypatch
):
    expected = ids(dataset(table, _prewarm=False))
    getitems = streaming.Permutation.__getitems__
    refusals = {}

    def restarting_getitems(self, indices):
        # Every fetch is refused three times before an executor serves it.
        key = (id(self), tuple(indices))
        refusals[key] = refusals.get(key, 0) + 1
        if refusals[key] <= 3:
            raise unavailable()
        return getitems(self, indices)

    ds = dataset(table)
    monkeypatch.setattr(streaming.Permutation, "__getitems__", restarting_getitems)
    assert ids(ds) == expected
    assert refusals and all(count == 4 for count in refusals.values())
    assert run.rewarms == []


def test_a_read_no_plan_executor_ever_serves_fails_at_the_timeout(
    table, run, monkeypatch
):
    def down_getitems(self, indices):
        raise unavailable()

    ds = dataset(table, _wait_ready_timeout_s=0.3)
    monkeypatch.setattr(streaming.Permutation, "__getitems__", down_getitems)
    started = time.monotonic()
    with pytest.raises(PlanExecutorUnavailableError):
        ids(ds)
    assert 0.3 <= time.monotonic() - started < 10
    assert run.rewarms == []
    assert run.closed == [0]


def test_closing_the_iteration_ends_a_read_waiting_for_a_plan_executor(
    table, run, monkeypatch
):
    getitems = streaming.Permutation.__getitems__
    served = set()
    refused = threading.Event()

    def getitems_once(self, indices):
        # Each split's first fetch is served; the executor is down after it.
        if id(self) in served:
            refused.set()
            raise unavailable()
        served.add(id(self))
        return getitems(self, indices)

    ds = dataset(table, _wait_ready_timeout_s=60)
    monkeypatch.setattr(streaming.Permutation, "__getitems__", getitems_once)
    iterator = iter(ds)
    next(iterator)
    assert refused.wait(5)
    started = time.monotonic()
    iterator.close()
    assert time.monotonic() - started < 5
    assert run.rewarms == []
    assert run.closed == [0]


def test_a_resumed_iteration_starts_at_its_wave(table, run):
    first = dataset(table)
    iterator = iter(first)
    head = ids(next(iterator) for _ in range(4 * 11))
    state = first.state_dict()
    # Fetches waiting on waves past the horizon must not hold the close up.
    started = time.monotonic()
    iterator.close()
    assert time.monotonic() - started < 5
    assert run.closed == [0]
    run.closed.clear()
    run.joins.clear()
    run.announced.clear()
    run.reports.clear()

    resumed = dataset(table)
    resumed.load_state_dict(state)
    tail = ids(resumed)
    assert sorted(head + tail) == list(range(NUM_ROWS))

    # Every split has read 11 positions, so reading resumes in wave 1.
    plan = plan_of(resumed, {split: 11 for split in range(4)})
    assert run.joins == [(0, 1)]
    assert sorted(run.announced) == [1, 2, 3, 4]
    assert run.announced[1] == plan.ranges(1)
    assert len(plan.ranges(1)) < len(plan_of(resumed).ranges(1))


def test_the_announcer_survives_a_limit_that_moved_down(table, run):
    # Every view allows one wave more than the server does.
    run.stale_views = 10**9
    expected = ids(dataset(table, _prewarm=False))
    assert ids(dataset(table)) == expected
    assert run.beyond_horizon > 0
    assert sorted(run.announced) == [0, 1, 2, 3, 4]


def test_a_held_back_announcer_asks_for_the_view_instead_of_waiting_for_it(table, run):
    """No heartbeat brings a view here, so every wave past the first limit
    is announced only because the announcer refreshed its view."""
    run.views_need_refresh = True
    ds = dataset(table)
    assert sorted(ids(ds)) == list(range(NUM_ROWS))
    assert sorted(run.announced) == list(range(plan_of(ds).num_waves))
    assert run.refreshes > 0
    assert run.beyond_horizon == 0


def test_ranks_share_a_run_and_stay_within_the_horizon(table, run, monkeypatch):
    monkeypatch.setattr(_prewarm_session, "_attempt_id", lambda rank, size: "attempt")
    results = {}

    def read(rank):
        results[rank] = ids(dataset(table, rank=rank, world_size=2))

    threads = [threading.Thread(target=read, args=(rank,)) for rank in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(60)
    assert sorted(results[0] + results[1]) == list(range(NUM_ROWS))
    assert len(results[0]) == len(results[1]) == NUM_ROWS // 2
    assert sorted(run.joins) == [(0, 0), (1, 0)]
    assert sorted(run.closed) == [0, 1]
    assert sorted(run.announced) == [0, 1, 2, 3, 4]
    assert run.beyond_horizon == 0
    # Both ranks opened the same run.
    assert len(set(run.backend.keys)) == 1


def test_a_failed_run_fails_the_iteration(table, run):
    run.failure = "run_failed"
    with pytest.raises(PrewarmRunError) as error:
        ids(dataset(table))
    assert error.value.code == "run_failed"
    assert run.closed == [0]


def test_a_stopped_announcer_fails_the_read_without_waiting_out_the_timeout(table, run):
    run.too_large_from = 2
    started = time.monotonic()
    with pytest.raises(PrewarmRunError) as raised:
        list(dataset(table, _wait_ready_timeout_s=60))
    assert raised.value.code == "announcer_stopped"
    assert time.monotonic() - started < 10
    assert sorted(run.announced) == [0, 1]
    assert run.closed == [0]


def test_workers_read_while_the_main_process_owns_the_run(table, run):
    run.strict_waits = False
    expected = ids(dataset(table, _prewarm=False))
    ds = dataset(table)
    loader = StreamingDataLoader(
        ds, batch_size=4, num_workers=2, multiprocessing_context="fork"
    )
    got = [int(value) for batch in loader for value in batch["id"]]
    assert sorted(got) == sorted(expected)
    assert run.joins == [(0, 0)]
    # The workers' positions reached this process through the shared slots.
    assert sorted(run.announced) == [0, 1, 2, 3, 4]
    assert run.closed == [0]
    assert ds._prewarm_session is None


def test_iterating_again_opens_another_run(table, run):
    ds = dataset(table)
    ids(ds)
    ds.load_state_dict(
        {
            **ds.state_dict(),
            "samples_consumed_per_split": [0] * 4,
            "positions_consumed_per_split": [0] * 4,
        }
    )
    ids(ds)
    assert len(run.backend.keys) == len(set(run.backend.keys)) == 2
    assert len(dataset(table, epoch=1)._perm_table.to_arrow()) == NUM_ROWS


@pytest.mark.parametrize(
    "error, reason",
    [
        (NotImplementedError("no route"), "does not support prewarm runs"),
        (
            PrewarmRunError("additional bases", "", "cache_only_unsupported"),
            "cannot be read cache-only",
        ),
    ],
)
def test_no_run_support_reads_as_before_with_a_warning(
    table, run, monkeypatch, caplog, error, reason
):
    def refuse(*args, **kwargs):
        raise error

    monkeypatch.setattr(run.backend, "open_run", refuse)
    with caplog.at_level(logging.WARNING, logger=_prewarm_session.__name__):
        got = ids(dataset(table))
    assert sorted(got) == list(range(NUM_ROWS))
    assert run.joins == []
    assert reason in caplog.text


def test_a_run_that_cannot_open_for_another_reason_raises(table, run, monkeypatch):
    def refuse(*args, **kwargs):
        raise PrewarmRunError("the run has ended", "", "run_released")

    monkeypatch.setattr(run.backend, "open_run", refuse)
    with pytest.raises(PrewarmRunError):
        ids(dataset(table))


def test_prewarm_off_reads_without_a_run_and_says_so(table, run, caplog):
    with caplog.at_level(logging.WARNING, logger=_prewarm_session.__name__):
        ids(dataset(table, _prewarm=False))
    assert run.joins == []
    assert "_prewarm=False" in caplog.text


def test_a_local_table_reads_without_a_run_quietly(table, caplog):
    with caplog.at_level(logging.WARNING, logger=_prewarm_session.__name__):
        got = ids(dataset(table))
    assert sorted(got) == list(range(NUM_ROWS))
    assert caplog.text == ""


def test_ranks_without_a_shared_attempt_id_read_without_a_run(
    table, run, monkeypatch, caplog
):
    monkeypatch.delenv("TORCHELASTIC_RUN_ID", raising=False)
    with caplog.at_level(logging.WARNING, logger=_prewarm_session.__name__):
        ids(dataset(table, rank=0, world_size=2))
    assert run.joins == []
    assert "no shared id" in caplog.text


def test_attempt_id_comes_from_torchrun_only_when_it_names_the_launch(monkeypatch):
    monkeypatch.setenv("TORCHELASTIC_RUN_ID", "none")
    assert _prewarm_session._attempt_id(0, 2) is None
    monkeypatch.setenv("TORCHELASTIC_RUN_ID", "job-7")
    monkeypatch.setenv("TORCHELASTIC_RESTART_COUNT", "2")
    assert _prewarm_session._attempt_id(0, 2) == "job-7-2"
    assert _prewarm_session._attempt_id(1, 2) == "job-7-2"
    monkeypatch.delenv("TORCHELASTIC_RUN_ID")
    # One rank needs no agreement: any id unique to this iteration will do.
    assert _prewarm_session._attempt_id(0, 1) != _prewarm_session._attempt_id(0, 1)
