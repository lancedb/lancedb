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

import os
import pathlib

import pytest
import lancedb
from lancedb.util import get_uri_scheme, join_uri, value_to_sql


def test_normalize_uri():
    uris = [
        "relative/path",
        "/absolute/path",
        "file:///absolute/path",
        "s3://bucket/path",
        "gs://bucket/path",
        "c:\\windows\\path",
    ]
    schemes = ["file", "file", "file", "s3", "gs", "file"]

    for uri, expected_scheme in zip(uris, schemes):
        parsed_scheme = get_uri_scheme(uri)
        assert parsed_scheme == expected_scheme


def test_join_uri_remote():
    schemes = ["s3", "az", "gs"]
    for scheme in schemes:
        expected = f"{scheme}://bucket/path/to/table.lance"
        base_uri = f"{scheme}://bucket/path/to/"
        parts = ["table.lance"]
        assert join_uri(base_uri, *parts) == expected

        base_uri = f"{scheme}://bucket"
        parts = ["path", "to", "table.lance"]
        assert join_uri(base_uri, *parts) == expected


# skip this test if on windows
@pytest.mark.skipif(os.name == "nt", reason="Windows paths are not POSIX")
def test_join_uri_posix():
    for base in [
        # relative path
        "relative/path",
        "relative/path/",
        # an absolute path
        "/absolute/path",
        "/absolute/path/",
        # a file URI
        "file:///absolute/path",
        "file:///absolute/path/",
    ]:
        joined = join_uri(base, "table.lance")
        assert joined == str(pathlib.Path(base) / "table.lance")
        joined = join_uri(pathlib.Path(base), "table.lance")
        assert joined == pathlib.Path(base) / "table.lance"


# skip this test if not on windows
@pytest.mark.skipif(os.name != "nt", reason="Windows paths are not POSIX")
def test_local_join_uri_windows():
    # https://learn.microsoft.com/en-us/dotnet/standard/io/file-path-formats
    for base in [
        # windows relative path
        "relative\\path",
        "relative\\path\\",
        # windows absolute path from current drive
        "c:\\absolute\\path",
        # relative path from root of current drive
        "\\relative\\path",
    ]:
        joined = join_uri(base, "table.lance")
        assert joined == str(pathlib.Path(base) / "table.lance")
        joined = join_uri(pathlib.Path(base), "table.lance")
        assert joined == pathlib.Path(base) / "table.lance"


def test_value_to_sql_string(tmp_path):
    # Make sure we can convert Python string literals to SQL strings, even if
    # they contain characters meaningful in SQL, such as ' and \.
    values = ["anthony's", 'a "test" string', "anthony's \"favorite color\" wasn't red"]
    expected_values = [
        "'anthony''s'",
        "'a \"test\" string'",
        "'anthony''s \"favorite color\" wasn''t red'",
    ]

    for value, expected in zip(values, expected_values):
        assert value_to_sql(value) == expected

    # Also test we can roundtrip those strings through update.
    # This validates the query parser understands the strings we
    # are creating.
    db = lancedb.connect(tmp_path)
    table = db.create_table(
        "test",
        [{"search": value, "replace": "something"} for value in values],
    )
    for value in values:
        table.update(where=f"search = {value_to_sql(value)}", values={"replace": value})
        assert table.to_pandas().query("search == @value")["replace"].item() == value
