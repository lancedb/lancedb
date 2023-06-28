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

import numpy as np
import pytest

from lancedb import LanceDBConnection

# TODO: setup integ test mark and script


@pytest.mark.skip(reason="Need to set up a local server")
def test_against_local_server():
    conn = LanceDBConnection("lancedb+http://localhost:10024")
    table = conn.open_table("sift1m_ivf1024_pq16")
    df = table.search(np.random.rand(128)).to_df()
    assert len(df) == 10
