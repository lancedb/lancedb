# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
Tests for S3 bucket names containing dots.

Related issue: https://github.com/lancedb/lancedb/issues/1898

These tests validate the early error checking for S3 bucket names with dots.
No actual S3 connection is made - validation happens before connection.
"""

import pytest
import lancedb

# Test URIs
BUCKET_WITH_DOTS = "s3://my.bucket.name/path"
BUCKET_WITH_DOTS_AND_REGION = ("s3://my.bucket.name", {"region": "us-east-1"})
BUCKET_WITH_DOTS_AND_AWS_REGION = ("s3://my.bucket.name", {"aws_region": "us-east-1"})
BUCKET_WITHOUT_DOTS = "s3://my-bucket/path"


class TestS3BucketWithDotsSync:
    """Tests for connect()."""

    def test_bucket_with_dots_requires_region(self):
        with pytest.raises(ValueError, match="contains dots"):
            lancedb.connect(BUCKET_WITH_DOTS)

    def test_bucket_with_dots_and_region_passes(self):
        uri, opts = BUCKET_WITH_DOTS_AND_REGION
        db = lancedb.connect(uri, storage_options=opts)
        assert db is not None

    def test_bucket_with_dots_and_aws_region_passes(self):
        uri, opts = BUCKET_WITH_DOTS_AND_AWS_REGION
        db = lancedb.connect(uri, storage_options=opts)
        assert db is not None

    def test_bucket_without_dots_passes(self):
        db = lancedb.connect(BUCKET_WITHOUT_DOTS)
        assert db is not None


class TestS3BucketWithDotsAsync:
    """Tests for connect_async()."""

    @pytest.mark.asyncio
    async def test_bucket_with_dots_requires_region(self):
        with pytest.raises(ValueError, match="contains dots"):
            await lancedb.connect_async(BUCKET_WITH_DOTS)

    @pytest.mark.asyncio
    async def test_bucket_with_dots_and_region_passes(self):
        uri, opts = BUCKET_WITH_DOTS_AND_REGION
        db = await lancedb.connect_async(uri, storage_options=opts)
        assert db is not None

    @pytest.mark.asyncio
    async def test_bucket_with_dots_and_aws_region_passes(self):
        uri, opts = BUCKET_WITH_DOTS_AND_AWS_REGION
        db = await lancedb.connect_async(uri, storage_options=opts)
        assert db is not None

    @pytest.mark.asyncio
    async def test_bucket_without_dots_passes(self):
        db = await lancedb.connect_async(BUCKET_WITHOUT_DOTS)
        assert db is not None
