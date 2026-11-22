# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Custom exception handling"""

from typing import Optional


class MissingValueError(ValueError):
    """Exception raised when a required value is missing."""

    pass


class MissingColumnError(KeyError):
    """
    Exception raised when a column name specified is not in
    the  DataFrame object
    """

    def __init__(self, column_name):
        self.column_name = column_name

    def __str__(self):
        return (
            f"Error: Column '{self.column_name}' does not exist in the DataFrame object"
        )


class ColumnNotFoundError(ValueError):
    """A requested table column does not exist.

    Remote HTTP and SQL errors inherit this type and carry request metadata.
    Locally rejected queries have no request or job identifiers.
    """

    def __init__(self, message: str):
        super().__init__(message)
        self.request_id: Optional[str] = None
        self.job_id: Optional[str] = None
        self.job_request_id: Optional[str] = None


class ReferenceNotFoundError(ValueError):
    """A requested branch or table reference does not exist."""

    request_id: Optional[str] = None
    job_id: Optional[str] = None
    job_request_id: Optional[str] = None


class SchemaMismatchError(ValueError):
    """Input data does not match the table's schema."""

    request_id: Optional[str] = None
    job_id: Optional[str] = None
    job_request_id: Optional[str] = None


class JobFailedError(RuntimeError):
    """Exception raised when an asynchronous job reaches the failed state.

    ``job_id`` identifies the failed job when available. ``sql_job_id`` preserves
    the SQL execution identity reported by the server, which may differ from
    the outer job's identity. ``request_id`` and ``job_request_id`` identify the
    current request and isolated worker request. A typed SQL cause is available
    through ``__cause__`` when the server reports one.
    """

    def __init__(
        self,
        message: str,
        job_id: Optional[str] = None,
        sql_job_id: Optional[str] = None,
        request_id: Optional[str] = None,
        job_request_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.job_id = job_id
        self.sql_job_id = sql_job_id
        self.request_id = request_id
        self.job_request_id = job_request_id


class JobCancelledError(RuntimeError):
    """Exception raised when an asynchronous job was cancelled."""

    pass


class JobNotFoundError(ValueError):
    """Exception raised when opening a job the server does not have."""

    pass


class PrewarmRunError(RuntimeError):
    """Exception raised when a prewarm run call is refused or the run ended.

    ``run_id`` identifies the run, or is empty when opening it failed.
    ``code`` is the server's error code, such as ``run_failed`` when the run
    could not keep upcoming waves in the cache, ``run_released`` after the run
    ended, or ``wave_consumed`` when this rank fell behind the others.
    """

    def __init__(self, message: str, run_id: str, code: str):
        super().__init__(message)
        self.run_id = run_id
        self.code = code


class NotResidentError(RuntimeError):
    """Exception raised when a cache-only query needs data the cache does not hold.

    Raised by queries on a handle from ``Table.cache_only()``. In a prewarm
    run, re-warm the rows the query asked for, wait for them, and retry.
    """

    def __init__(self, message: str, request_id: str):
        super().__init__(message)
        self.request_id = request_id


class PlanExecutorUnavailableError(NotResidentError):
    """Exception raised when no plan executor can serve a cache-only query now.

    This happens while the plan executor that holds the data restarts, and
    for a few seconds after it is back. Wait and retry the query. Do not
    re-warm: no plan executor can serve the rows right now, and warming
    cannot change that.

    It subclasses ``NotResidentError`` because the rows could not be served
    from the cache either way.
    """
