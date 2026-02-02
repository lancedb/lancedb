# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Progress reporting utilities for LanceDB operations."""

from typing import Callable, Optional, Protocol, TypedDict, Union


class WriteProgress(TypedDict):
    """Progress information for write operations.

    Attributes
    ----------
    rows_written : int
        Number of rows written so far.
    bytes_written : int
        Approximate bytes written so far (based on Arrow batch memory size).
    elapsed_secs : float
        Elapsed time in seconds since the operation started.
    """

    rows_written: int
    bytes_written: int
    elapsed_secs: float


class ProgressReporter(Protocol):
    """Protocol for progress reporters (e.g., tqdm).

    Any object with an `update(n)` method can be used as a progress reporter.
    """

    def update(self, n: int = 1) -> None:
        """Update progress by n units."""
        ...


def _format_bytes(num_bytes: float) -> str:
    """Format bytes into human-readable string."""
    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if abs(num_bytes) < 1024.0:
            return f"{num_bytes:.1f}{unit}"
        num_bytes /= 1024.0
    return f"{num_bytes:.1f}PB"


class TqdmProgressAdapter:
    """Adapter that converts WriteProgress updates to tqdm updates.

    This adapter takes a tqdm-like progress bar and updates it with progress
    information from write operations. It shows rows written, bytes, and
    throughput in the postfix.

    Parameters
    ----------
    pbar : ProgressReporter
        A progress bar instance (e.g., tqdm.tqdm()).
    unit : str, default "rows"
        The unit to display. Either "rows" or "bytes".
    show_bytes : bool, default True
        Whether to show bytes written in the postfix.
    show_rate : bool, default True
        Whether to show write rate in the postfix.

    Examples
    --------
    >>> from tqdm import tqdm
    >>> from lancedb.progress import TqdmProgressAdapter
    >>>
    >>> with tqdm(unit="rows") as pbar:
    ...     adapter = TqdmProgressAdapter(pbar)
    ...     table.add(data, progress=adapter)
    """

    def __init__(
        self,
        pbar: ProgressReporter,
        unit: str = "rows",
        show_bytes: bool = True,
        show_rate: bool = True,
    ):
        self.pbar = pbar
        self.unit = unit
        self.show_bytes = show_bytes
        self.show_rate = show_rate
        self._last_rows = 0
        self._last_bytes = 0

    def __call__(self, progress: WriteProgress) -> None:
        """Handle a progress update."""
        if self.unit == "rows":
            delta = progress["rows_written"] - self._last_rows
            self._last_rows = progress["rows_written"]
        else:
            delta = progress["bytes_written"] - self._last_bytes
            self._last_bytes = progress["bytes_written"]

        if delta > 0:
            self.pbar.update(delta)

        postfix = {}
        if self.show_bytes:
            postfix["bytes"] = _format_bytes(progress["bytes_written"])
        if self.show_rate and progress["elapsed_secs"] > 0:
            bytes_per_sec = progress["bytes_written"] / progress["elapsed_secs"]
            postfix["throughput"] = _format_bytes(int(bytes_per_sec)) + "/s"

        if postfix and hasattr(self.pbar, "set_postfix"):
            self.pbar.set_postfix(**postfix)


def make_progress_callback(
    reporter: Union[ProgressReporter, Callable[[WriteProgress], None], None],
) -> Optional[Callable[[WriteProgress], None]]:
    """Convert a progress reporter to a callback function.

    This function normalizes different types of progress reporters into a
    callable that can be passed to the underlying Rust implementation.

    Parameters
    ----------
    reporter : ProgressReporter, Callable, or None
        Either a tqdm-like object with an `update` method, a raw callback
        function, or None.

    Returns
    -------
    Optional[Callable[[WriteProgress], None]]
        A callback function or None.
    """
    if reporter is None:
        return None

    if callable(reporter) and not hasattr(reporter, "update"):
        return reporter  # type: ignore[return-value]

    # It's a tqdm-like object with an update method
    return TqdmProgressAdapter(reporter)  # type: ignore[arg-type]
