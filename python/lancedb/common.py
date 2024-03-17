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
from pathlib import Path
from typing import Iterable, List, Union

import numpy as np
import pyarrow as pa

from .util import safe_import_pandas

pd = safe_import_pandas()

DATA = Union[List[dict], dict, "pd.DataFrame", pa.Table, Iterable[pa.RecordBatch]]
VEC = Union[list, np.ndarray, pa.Array, pa.ChunkedArray]
URI = Union[str, Path]
VECTOR_COLUMN_NAME = "vector"


class Credential(str):
    """Credential field"""

    def __repr__(self) -> str:
        return "********"

    def __str__(self) -> str:
        return "********"
