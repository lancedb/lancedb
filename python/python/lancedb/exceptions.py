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
