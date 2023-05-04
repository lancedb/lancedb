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

from lancedb.util import get_uri_scheme


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
