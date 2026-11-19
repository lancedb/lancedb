# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Prewarm runs: warm a training job's data a wave at a time, ahead of reads.

A training job opens one run per table. Each rank joins it, rank 0 announces
the rows of upcoming waves, and every rank waits for a wave to be warm before
reading it. While a rank is joined, heartbeats sent in the background keep it
live and tell LanceDB Enterprise which wave it is reading.

Runs are only supported by LanceDB Enterprise.
"""

from dataclasses import dataclass
from datetime import timedelta
import json
from typing import Any, Iterable, Mapping, NamedTuple, Optional, Sequence, Union

from lancedb.background_loop import LOOP


class RowRange(NamedTuple):
    """Rows ``[start, end)`` of one fragment."""

    fragment_id: int
    start: int
    end: int


@dataclass(frozen=True)
class RunView:
    """The run as the server last reported it to this rank."""

    status: str
    """``open``, ``released`` or ``failed``."""
    reading_wave: int
    """The lowest wave any live rank is reading."""
    highest_announced: Optional[int]
    live_ranks: int
    horizon: int
    """How many waves past ``reading_wave`` rank 0 may announce."""
    wave_positions: int
    """Permutation positions of each split that one wave holds."""
    heartbeat_interval_ms: int

    @property
    def announce_limit(self) -> int:
        """The highest wave rank 0 may announce now."""
        return self.reading_wave + self.horizon

    @classmethod
    def _from_json(cls, raw: str) -> "RunView":
        return cls(**json.loads(raw))


Timeout = Union[timedelta, float]
Waves = Mapping[int, Iterable[Sequence[int]]]


def _seconds(timeout: Timeout) -> float:
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return float(timeout)


def _ranges(ranges: Iterable[Sequence[int]]) -> list:
    return [tuple(RowRange(*r)) for r in ranges]


class AsyncPrewarmRunRank:
    """One rank's membership in a prewarm run.

    Heartbeats stop when this object is closed or garbage collected; call
    [close][lancedb.prewarm_run.AsyncPrewarmRunRank.close] when the rank is
    done reading.
    """

    def __init__(self, inner: Any):
        self._inner = inner

    @property
    def rank(self) -> int:
        return self._inner.rank

    def view(self) -> RunView:
        """The run as the server last reported it."""
        return RunView._from_json(self._inner.view())

    async def announce(self, waves: Waves) -> Optional[int]:
        """Announce upcoming waves, mapping each wave to its row ranges.

        Only rank 0 may announce. Waves may come in any order, and the server
        warms the lowest one first; they must stay within
        `RunView.announce_limit`. Repeating an announced wave with the same
        rows is harmless, and a wave every rank has passed is ignored.
        Returns the highest wave announced so far.
        """
        return await self._inner.announce(
            [(wave, _ranges(ranges)) for wave, ranges in sorted(waves.items())]
        )

    async def wait_ready(self, wave: int, timeout: Timeout) -> None:
        """Wait until ``wave`` is warm.

        Marks ``wave`` as the one this rank is reading, so the server lets
        earlier waves go. Raises ``RuntimeError`` if ``timeout`` (seconds or a
        ``timedelta``) passes first, and
        [PrewarmRunError][lancedb.exceptions.PrewarmRunError] if the run
        failed or ended.
        """
        await self._inner.wait_ready(wave, _seconds(timeout))

    def report_wave(self, wave: int) -> None:
        """Report that this rank is reading ``wave``, for readers in other
        processes. Heartbeats carry the highest wave reported."""
        self._inner.report_wave(wave)

    async def rewarm(self, wave: int, ranges: Iterable[Sequence[int]]) -> None:
        """Warm ``ranges`` of ``wave`` again after a read found them missing.

        Follow with [wait_ready][lancedb.prewarm_run.AsyncPrewarmRunRank.wait_ready].
        """
        await self._inner.rewarm(wave, _ranges(ranges))

    async def close(self) -> None:
        """Stop heartbeats and leave the run. A run that ended counts as closed."""
        await self._inner.close()


class AsyncPrewarmRunReader:
    """Reads for a rank another process joined: waits on waves and re-warms
    misses, without heartbeats. For DataLoader workers; the rank's main
    process reports the waves they read with
    [report_wave][lancedb.prewarm_run.AsyncPrewarmRunRank.report_wave].
    """

    def __init__(self, inner: Any):
        self._inner = inner

    @property
    def rank(self) -> int:
        return self._inner.rank

    async def wait_ready(self, wave: int, timeout: Timeout) -> None:
        """Wait until ``wave`` is warm. Does not report the wave as being read."""
        await self._inner.wait_ready(wave, _seconds(timeout))

    async def rewarm(self, wave: int, ranges: Iterable[Sequence[int]]) -> None:
        """Warm ``ranges`` of ``wave`` again after a read found them missing."""
        await self._inner.rewarm(wave, _ranges(ranges))


class AsyncPrewarmRun:
    """An open prewarm run. Each rank joins it with
    [join][lancedb.prewarm_run.AsyncPrewarmRun.join]."""

    def __init__(self, inner: Any):
        self._inner = inner

    @property
    def run_id(self) -> str:
        return self._inner.run_id

    @property
    def version(self) -> int:
        """The table version the run reads."""
        return self._inner.version

    def reader(self, rank: int) -> AsyncPrewarmRunReader:
        """A reader for ``rank``, which another process has joined."""
        return AsyncPrewarmRunReader(self._inner.reader(rank))

    async def join(self, rank: int, first_wave: int = 0) -> AsyncPrewarmRunRank:
        """Join as ``rank`` and start sending heartbeats. Joining again is
        harmless.

        ``first_wave`` is the wave the rank reads first: 0 unless training
        resumes part way through the data."""
        return AsyncPrewarmRunRank(await self._inner.join(rank, first_wave))


class PrewarmRunRank:
    """Synchronous counterpart of `AsyncPrewarmRunRank`."""

    def __init__(self, inner: AsyncPrewarmRunRank):
        self._inner = inner

    @property
    def rank(self) -> int:
        return self._inner.rank

    def view(self) -> RunView:
        """See `AsyncPrewarmRunRank.view`."""
        return self._inner.view()

    def announce(self, waves: Waves) -> Optional[int]:
        """See `AsyncPrewarmRunRank.announce`."""
        return LOOP.run(self._inner.announce(waves))

    def wait_ready(self, wave: int, timeout: Timeout) -> None:
        """See `AsyncPrewarmRunRank.wait_ready`."""
        LOOP.run(self._inner.wait_ready(wave, timeout))

    def report_wave(self, wave: int) -> None:
        """See `AsyncPrewarmRunRank.report_wave`."""
        self._inner.report_wave(wave)

    def rewarm(self, wave: int, ranges: Iterable[Sequence[int]]) -> None:
        """See `AsyncPrewarmRunRank.rewarm`."""
        LOOP.run(self._inner.rewarm(wave, ranges))

    def close(self) -> None:
        """See `AsyncPrewarmRunRank.close`."""
        LOOP.run(self._inner.close())


class PrewarmRunReader:
    """Synchronous counterpart of `AsyncPrewarmRunReader`."""

    def __init__(self, inner: AsyncPrewarmRunReader):
        self._inner = inner

    @property
    def rank(self) -> int:
        return self._inner.rank

    def wait_ready(self, wave: int, timeout: Timeout) -> None:
        """See `AsyncPrewarmRunReader.wait_ready`."""
        LOOP.run(self._inner.wait_ready(wave, timeout))

    def rewarm(self, wave: int, ranges: Iterable[Sequence[int]]) -> None:
        """See `AsyncPrewarmRunReader.rewarm`."""
        LOOP.run(self._inner.rewarm(wave, ranges))


class PrewarmRun:
    """Synchronous counterpart of `AsyncPrewarmRun`."""

    def __init__(self, inner: AsyncPrewarmRun):
        self._inner = inner

    @property
    def run_id(self) -> str:
        return self._inner.run_id

    @property
    def version(self) -> int:
        return self._inner.version

    def reader(self, rank: int) -> PrewarmRunReader:
        """See `AsyncPrewarmRun.reader`."""
        return PrewarmRunReader(self._inner.reader(rank))

    def join(self, rank: int, first_wave: int = 0) -> PrewarmRunRank:
        """See `AsyncPrewarmRun.join`."""
        return PrewarmRunRank(LOOP.run(self._inner.join(rank, first_wave)))
