#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
from __future__ import annotations

import deprecation

from . import __version__
from .exceptions import MissingColumnError, MissingValueError
from .util import safe_import_pandas

pd = safe_import_pandas()


def contextualize(raw_df: "pd.DataFrame") -> Contextualizer:
    """Create a Contextualizer object for the given DataFrame.

    Used to create context windows. Context windows are rolling subsets of text
    data.

    The input text column should already be separated into rows that will be the
    unit of the window. So to create a context window over tokens, start with
    a DataFrame with one token per row. To create a context window over sentences,
    start with a DataFrame with one sentence per row.

    Examples
    --------
    >>> from lancedb.context import contextualize
    >>> import pandas as pd
    >>> data = pd.DataFrame({
    ...    'token': ['The', 'quick', 'brown', 'fox', 'jumped', 'over',
    ...              'the', 'lazy', 'dog', 'I', 'love', 'sandwiches'],
    ...    'document_id': [1, 1, 1, 1, 1, 1, 1, 1, 1, 2, 2, 2]
    ... })

    ``window`` determines how many rows to include in each window. In our case
    this how many tokens, but depending on the input data, it could be sentences,
    paragraphs, messages, etc.

    >>> contextualize(data).window(3).stride(1).text_col('token').to_pandas()
                    token  document_id
    0     The quick brown            1
    1     quick brown fox            1
    2    brown fox jumped            1
    3     fox jumped over            1
    4     jumped over the            1
    5       over the lazy            1
    6        the lazy dog            1
    7          lazy dog I            1
    8          dog I love            1
    9   I love sandwiches            2
    10    love sandwiches            2
    >>> (contextualize(data).window(7).stride(1).min_window_size(7)
    ...   .text_col('token').to_pandas())
                                      token  document_id
    0   The quick brown fox jumped over the            1
    1  quick brown fox jumped over the lazy            1
    2    brown fox jumped over the lazy dog            1
    3        fox jumped over the lazy dog I            1
    4       jumped over the lazy dog I love            1
    5   over the lazy dog I love sandwiches            1

    ``stride`` determines how many rows to skip between each window start. This can
    be used to reduce the total number of windows generated.

    >>> contextualize(data).window(4).stride(2).text_col('token').to_pandas()
                        token  document_id
    0     The quick brown fox            1
    2   brown fox jumped over            1
    4    jumped over the lazy            1
    6          the lazy dog I            1
    8   dog I love sandwiches            1
    10        love sandwiches            2

    ``groupby`` determines how to group the rows. For example, we would like to have
    context windows that don't cross document boundaries. In this case, we can
    pass ``document_id`` as the group by.

    >>> (contextualize(data)
    ...     .window(4).stride(2).text_col('token').groupby('document_id')
    ...     .to_pandas())
                       token  document_id
    0    The quick brown fox            1
    2  brown fox jumped over            1
    4   jumped over the lazy            1
    6           the lazy dog            1
    9      I love sandwiches            2

    ``min_window_size`` determines the minimum size of the context windows
    that are generated.This can be used to trim the last few context windows
    which have size less than ``min_window_size``.
    By default context windows of size 1 are skipped.

    >>> (contextualize(data)
    ...     .window(6).stride(3).text_col('token').groupby('document_id')
    ...     .to_pandas())
                                 token  document_id
    0  The quick brown fox jumped over            1
    3     fox jumped over the lazy dog            1
    6                     the lazy dog            1
    9                I love sandwiches            2

    >>> (contextualize(data)
    ...     .window(6).stride(3).min_window_size(4).text_col('token')
    ...     .groupby('document_id')
    ...     .to_pandas())
                                 token  document_id
    0  The quick brown fox jumped over            1
    3     fox jumped over the lazy dog            1

    """
    return Contextualizer(raw_df)


class Contextualizer:
    """Create context windows from a DataFrame.
    See [lancedb.context.contextualize][].
    """

    def __init__(self, raw_df):
        self._text_col = None
        self._groupby = None
        self._stride = None
        self._window = None
        self._min_window_size = 2
        self._raw_df = raw_df

    def window(self, window: int) -> Contextualizer:
        """Set the window size. i.e., how many rows to include in each window.

        Parameters
        ----------
        window: int
            The window size.
        """
        self._window = window
        return self

    def stride(self, stride: int) -> Contextualizer:
        """Set the stride. i.e., how many rows to skip between each window.

        Parameters
        ----------
        stride: int
            The stride.
        """
        self._stride = stride
        return self

    def groupby(self, groupby: str) -> Contextualizer:
        """Set the groupby column. i.e., how to group the rows.
        Windows don't cross groups

        Parameters
        ----------
        groupby: str
            The groupby column.
        """
        self._groupby = groupby
        return self

    def text_col(self, text_col: str) -> Contextualizer:
        """Set the text column used to make the context window.

        Parameters
        ----------
        text_col: str
            The text column.
        """
        self._text_col = text_col
        return self

    def min_window_size(self, min_window_size: int) -> Contextualizer:
        """Set the (optional) min_window_size size for the context window.

        Parameters
        ----------
        min_window_size: int
            The min_window_size.
        """
        self._min_window_size = min_window_size
        return self

    @deprecation.deprecated(
        deprecated_in="0.3.1",
        removed_in="0.4.0",
        current_version=__version__,
        details="Use to_pandas() instead",
    )
    def to_df(self) -> "pd.DataFrame":
        return self.to_pandas()

    def to_pandas(self) -> "pd.DataFrame":
        """Create the context windows and return a DataFrame."""
        if pd is None:
            raise ImportError(
                "pandas is required to create context windows using lancedb"
            )

        if self._text_col not in self._raw_df.columns.tolist():
            raise MissingColumnError(self._text_col)

        if self._window is None or self._window < 1:
            raise MissingValueError(
                "The value of window is None or less than 1. Specify the "
                "window size (number of rows to include in each window)"
            )

        if self._stride is None or self._stride < 1:
            raise MissingValueError(
                "The value of stride is None or less than 1. Specify the "
                "stride (number of rows to skip between each window)"
            )

        def process_group(grp):
            # For each group, create the text rolling window
            # with values of size >= min_window_size
            text = grp[self._text_col].values
            contexts = grp.iloc[:: self._stride, :].copy()
            windows = [
                " ".join(text[start_i : min(start_i + self._window, len(grp))])
                for start_i in range(0, len(grp), self._stride)
                if start_i + self._window <= len(grp)
                or len(grp) - start_i >= self._min_window_size
            ]
            # if last few rows dropped
            if len(windows) < len(contexts):
                contexts = contexts.iloc[: len(windows)]
            contexts[self._text_col] = windows
            return contexts

        if self._groupby is None:
            return process_group(self._raw_df)
        # concat result from all groups
        return pd.concat(
            [process_group(grp) for _, grp in self._raw_df.groupby(self._groupby)]
        )
