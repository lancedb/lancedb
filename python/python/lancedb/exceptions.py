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


class JobFailedError(RuntimeError):
    """Exception raised when an asynchronous job reaches the failed state."""

    def __init__(
        self,
        message: str,
        job_id: Optional[str] = None,
        sql_job_id: Optional[str] = None,
    ):
        super().__init__(message)
        self.job_id = job_id
        self.sql_job_id = sql_job_id


class JobCancelledError(RuntimeError):
    """Exception raised when an asynchronous job was cancelled."""

    pass


class JobNotFoundError(ValueError):
    """Exception raised when opening a job the server does not have."""

    pass
