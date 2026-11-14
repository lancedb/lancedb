# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from typing import assert_type

import lancedb
from lancedb import AsyncCatalog, AsyncConnection, Catalog, DBConnection


def check_connect_type() -> None:
    assert_type(lancedb.connect("memory://"), DBConnection)
    assert_type(
        lancedb.connect_catalog(
            uri="db://analytics",
            api_key="secret",
            region="us-west-2",
            host_override="https://my-server.example",
        ),
        Catalog,
    )


async def check_connect_async_type() -> None:
    assert_type(await lancedb.connect_async("memory://"), AsyncConnection)
    assert_type(
        await lancedb.connect_catalog_async(
            uri="db://analytics",
            api_key="secret",
            region="us-west-2",
            host_override="https://my-server.example",
        ),
        AsyncCatalog,
    )
