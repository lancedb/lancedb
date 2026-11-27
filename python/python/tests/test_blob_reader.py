# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import io

import pytest

from lancedb import BlobReader


def test_reader_is_lazy_and_seekable():
    requests = []
    size = 64 * 1024 * 1024

    def read_range(offset, length):
        requests.append((offset, length))
        return bytes((index % 251 for index in range(offset, offset + length)))

    with BlobReader(read_range, size) as reader:
        assert requests == []
        assert reader.size() == size
        reader.seek(-4, io.SEEK_END)
        assert reader.read(10) == bytes(index % 251 for index in range(size - 4, size))
        assert reader.read(1) == b""
        assert requests == [(size - 4, 4)]
        assert reader.read_range(1, 3) == b"\x01\x02\x03"
        assert reader.tell() == size
        reader.seek(2)
        target = bytearray(3)
        assert reader.readinto(target) == 3
        assert target == b"\x02\x03\x04"
        reader.seek(-1, io.SEEK_CUR)
        assert reader.tell() == 4
    with pytest.raises(ValueError, match="closed"):
        reader.read(1)


def test_reader_supports_buffered_io_and_bounds_range_requests():
    requests = []
    payload = b"x" * (3 * 1024 * 1024 + 7)

    def read_range(offset, length):
        requests.append((offset, length))
        return payload[offset : offset + length]

    with io.BufferedReader(BlobReader(read_range, len(payload))) as reader:
        assert reader.read() == payload
    assert requests
    assert all(length <= 1024 * 1024 for _, length in requests)


def test_reader_rejects_invalid_ranges_and_short_reads():
    reader = BlobReader(lambda offset, length: b"", 10)
    with pytest.raises(ValueError):
        reader.seek(-1)
    with pytest.raises(ValueError):
        reader.read_range(-1, 1)
    with pytest.raises(OSError, match="payload length"):
        reader.read(1)
