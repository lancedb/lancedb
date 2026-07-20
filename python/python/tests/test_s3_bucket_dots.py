# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""
Tests for S3 bucket names containing dots.

Related issue: https://github.com/lancedb/lancedb/issues/1898

These tests validate the early error checking for S3 bucket names with dots.
When validation succeeds, eager namespace initialization may still fail later
because these tests intentionally do not provide real S3 credentials.
"""

import pytest
import lancedb

# Test URIs
BUCKET_WITH_DOTS = "s3://my.bucket.name/path"
BUCKET_WITH_DOTS_AND_REGION = ("s3://my.bucket.name", {"region": "us-east-1"})
BUCKET_WITH_DOTS_AND_AWS_REGION = ("s3://my.bucket.name", {"aws_region": "us-east-1"})
BUCKET_WITHOUT_DOTS = "s3://my-bucket/path"


def assert_not_rejected_for_bucket_dots(connect):
    try:
        connect()
    except ValueError as err:
        assert "contains dots" not in str(err)


class TestS3BucketWithDotsSync:
    """Tests for connect()."""

    def test_bucket_with_dots_requires_region(self):
        with pytest.raises(ValueError, match="contains dots"):
            lancedb.connect(BUCKET_WITH_DOTS)

    def test_bucket_with_dots_and_region_is_not_rejected(self):
        uri, opts = BUCKET_WITH_DOTS_AND_REGION
        assert_not_rejected_for_bucket_dots(
            lambda: lancedb.connect(uri, storage_options=opts)
        )

    def test_bucket_with_dots_and_aws_region_is_not_rejected(self):
        uri, opts = BUCKET_WITH_DOTS_AND_AWS_REGION
        assert_not_rejected_for_bucket_dots(
            lambda: lancedb.connect(uri, storage_options=opts)
        )

    def test_bucket_without_dots_is_not_rejected(self):
        assert_not_rejected_for_bucket_dots(
            lambda: lancedb.connect(BUCKET_WITHOUT_DOTS)
        )


class TestS3BucketWithDotsAsync:
    """Tests for connect_async()."""

    @pytest.mark.asyncio
    async def test_bucket_with_dots_requires_region(self):
        with pytest.raises(ValueError, match="contains dots"):
            await lancedb.connect_async(BUCKET_WITH_DOTS)

    @pytest.mark.asyncio
    async def test_bucket_with_dots_and_region_is_not_rejected(self):
        uri, opts = BUCKET_WITH_DOTS_AND_REGION
        try:
            await lancedb.connect_async(uri, storage_options=opts)
        except ValueError as err:
            assert "contains dots" not in str(err)

    @pytest.mark.asyncio
    async def test_bucket_with_dots_and_aws_region_is_not_rejected(self):
        uri, opts = BUCKET_WITH_DOTS_AND_AWS_REGION
        try:
            await lancedb.connect_async(uri, storage_options=opts)
        except ValueError as err:
            assert "contains dots" not in str(err)

    @pytest.mark.asyncio
    async def test_bucket_without_dots_is_not_rejected(self):
        try:
            await lancedb.connect_async(BUCKET_WITHOUT_DOTS)
        except ValueError as err:
            assert "contains dots" not in str(err)
