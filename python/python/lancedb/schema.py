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

"""Schema related utilities."""

import pyarrow as pa


def vector(dimension: int, value_type: pa.DataType = pa.float32()) -> pa.DataType:
    """A help function to create a vector type.

    Parameters
    ----------
    dimension: The dimension of the vector.
    value_type: pa.DataType, optional
        The type of the value in the vector.

    Returns
    -------
    A PyArrow DataType for vectors.

    Examples
    --------

    >>> import pyarrow as pa
    >>> import lancedb
    >>> schema = pa.schema([
    ...     pa.field("id", pa.int64()),
    ...     pa.field("vector", lancedb.vector(756)),
    ... ])
    """
    return pa.list_(value_type, dimension)
