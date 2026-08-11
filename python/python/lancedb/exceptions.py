# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Custom exception handling"""


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

    pass


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
