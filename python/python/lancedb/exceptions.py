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
    """Exception raised when an asynchronous job reaches the failed state.

    ``error_code`` is the optional exact category string projected from the
    native job failure when the backend supplied one. The RuntimeError
    message remains the existing diagnostic text and must not be used to
    recover or override the code.
    """

    __slots__ = ("_error_code",)

    def __init__(self, message: str, error_code: Optional[str] = None) -> None:
        super().__init__(message)
        self._error_code = error_code

    @property
    def error_code(self) -> Optional[str]:
        """Exact job failure error category string, when supplied."""
        return self._error_code


class JobCancelledError(RuntimeError):
    """Exception raised when an asynchronous job was cancelled."""

    pass


class FunctionError(RuntimeError):
    """Exception raised when a first-class Function operation fails.

    ``code`` is the stable semantic category from the native error. The
    message is a sanitized client diagnostic and must not be used to recover
    or override the code.
    """

    __slots__ = ("_code",)

    def __init__(self, message: str, code: str) -> None:
        super().__init__(message)
        self._code = code

    @property
    def code(self) -> str:
        """Stable Function error category string."""
        return self._code
