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
