# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import numpy as np
import pytest
from lancedb import LanceDBConnection

# TODO: setup integ test mark and script


@pytest.mark.skip(reason="Need to set up a local server")
def test_against_local_server():
    conn = LanceDBConnection("lancedb+http://localhost:10024")
    table = conn.open_table("sift1m_ivf1024_pq16")
    df = table.search(np.random.rand(128)).to_pandas()
    assert len(df) == 10
