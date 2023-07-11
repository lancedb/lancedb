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

import pyarrow as pa

import lancedb
from lancedb.schema import dict_to_schema, schema_to_dict


def test_schema_to_dict():
    schema = pa.schema(
        [
            pa.field("id", pa.int64()),
            pa.field("vector", lancedb.vector(512), nullable=False),
            pa.field(
                "struct",
                pa.struct(
                    [
                        pa.field("a", pa.utf8()),
                        pa.field("b", pa.float32()),
                    ]
                ),
                True,
            ),
            pa.field("d", pa.dictionary(pa.int64(), pa.utf8()), False),
        ],
        metadata={"key": "value"},
    )

    json_schema = schema_to_dict(schema)
    assert json_schema == {
        "fields": [
            {"name": "id", "type": {"type": "int64"}, "nullable": True},
            {
                "name": "vector",
                "type": {
                    "type": "fixed_size_list",
                    "value_type": {"type": "float32"},
                    "width": 512,
                },
                "nullable": False,
            },
            {
                "name": "struct",
                "type": {
                    "type": "struct",
                    "fields": [
                        {"name": "a", "type": {"type": "string"}, "nullable": True},
                        {"name": "b", "type": {"type": "float32"}, "nullable": True},
                    ],
                },
                "nullable": True,
            },
            {
                "name": "d",
                "type": {
                    "type": "dictionary",
                    "index_type": {"type": "int64"},
                    "value_type": {"type": "string"},
                },
                "nullable": False,
            },
        ],
        "metadata": {"key": "value"},
    }

    actual_schema = dict_to_schema(json_schema)
    assert actual_schema == schema
