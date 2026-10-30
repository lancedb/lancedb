# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from typing import assert_type

import lancedb
from lancedb import AsyncConnection, DBConnection


def check_connect_type() -> None:
    assert_type(lancedb.connect("memory://"), DBConnection)


async def check_connect_async_type() -> None:
    assert_type(await lancedb.connect_async("memory://"), AsyncConnection)
