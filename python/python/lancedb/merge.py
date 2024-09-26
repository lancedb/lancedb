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

from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from .common import DATA


class LanceMergeInsertBuilder(object):
    """Builder for a LanceDB merge insert operation

    See [`merge_insert`][lancedb.table.Table.merge_insert] for
    more context
    """

    def __init__(self, table: "Table", on: List[str]):  # noqa: F821
        # Do not put a docstring here.  This method should be hidden
        # from API docs.  Users should use merge_insert to create
        # this object.
        self._table = table
        self._on = on
        self._when_matched_update_all = False
        self._when_matched_update_all_condition = None
        self._when_not_matched_insert_all = False
        self._when_not_matched_by_source_delete = False
        self._when_not_matched_by_source_condition = None

    def when_matched_update_all(
        self, *, where: Optional[str] = None
    ) -> LanceMergeInsertBuilder:
        """
        Rows that exist in both the source table (new data) and
        the target table (old data) will be updated, replacing
        the old row with the corresponding matching row.

        If there are multiple matches then the behavior is undefined.
        Currently this causes multiple copies of the row to be created
        but that behavior is subject to change.
        """
        self._when_matched_update_all = True
        self._when_matched_update_all_condition = where
        return self

    def when_not_matched_insert_all(self) -> LanceMergeInsertBuilder:
        """
        Rows that exist only in the source table (new data) should
        be inserted into the target table.
        """
        self._when_not_matched_insert_all = True
        return self

    def when_not_matched_by_source_delete(
        self, condition: Optional[str] = None
    ) -> LanceMergeInsertBuilder:
        """
        Rows that exist only in the target table (old data) will be
        deleted.  An optional condition can be provided to limit what
        data is deleted.

        Parameters
        ----------
        condition: Optional[str], default None
            If None then all such rows will be deleted.  Otherwise the
            condition will be used as an SQL filter to limit what rows
            are deleted.
        """
        self._when_not_matched_by_source_delete = True
        if condition is not None:
            self._when_not_matched_by_source_condition = condition
        return self

    def execute(
        self,
        new_data: DATA,
        on_bad_vectors: str = "error",
        fill_value: float = 0.0,
    ):
        """
        Executes the merge insert operation

        Nothing is returned but the [`Table`][lancedb.table.Table] is updated

        Parameters
        ----------
        new_data: DATA
            New records which will be matched against the existing records
            to potentially insert or update into the table.  This parameter
            can be anything you use for [`add`][lancedb.table.Table.add]
        on_bad_vectors: str, default "error"
            What to do if any of the vectors are not the same size or contains NaNs.
            One of "error", "drop", "fill".
        fill_value: float, default 0.
            The value to use when filling vectors. Only used if on_bad_vectors="fill".
        """
        return self._table._do_merge(self, new_data, on_bad_vectors, fill_value)
