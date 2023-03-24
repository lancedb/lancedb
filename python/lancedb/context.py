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

import pandas as pd


def contextualize(raw_df: pd.DataFrame) -> Contextualizer:
    """Create a Contextualizer object for the given DataFrame.
    Used to create context windows.
    """
    return Contextualizer(raw_df)


class Contextualizer:
    def __init__(self, raw_df):
        self._text_col = None
        self._groupby = None
        self._stride = None
        self._window = None
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

    def to_df(self) -> pd.DataFrame:
        """Create the context windows and return a DataFrame."""

        def process_group(grp):
            # For each group, create the text rolling window
            text = grp[self._text_col].values
            contexts = grp.iloc[: -self._window : self._stride, :].copy()
            contexts[self._text_col] = [
                " ".join(text[start_i : start_i + self._window])
                for start_i in range(0, len(grp) - self._window, self._stride)
            ]
            return contexts

        if self._groupby is None:
            return process_group(self._raw_df)
        # concat result from all groups
        return pd.concat(
            [process_group(grp) for _, grp in self._raw_df.groupby(self._groupby)]
        )
