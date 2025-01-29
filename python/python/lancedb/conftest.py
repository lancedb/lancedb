# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os
import time

import numpy as np
import pytest

from .embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction

# import lancedb so we don't have to in every example


@pytest.fixture(autouse=True)
def doctest_setup(monkeypatch, tmpdir):
    # disable color for doctests so we don't have to include
    # escape codes in docstrings
    monkeypatch.setitem(os.environ, "NO_COLOR", "1")
    # Explicitly set the column width
    monkeypatch.setitem(os.environ, "COLUMNS", "80")
    # Work in a temporary directory
    monkeypatch.chdir(tmpdir)


registry = EmbeddingFunctionRegistry.get_instance()


@registry.register("test")
class MockTextEmbeddingFunction(TextEmbeddingFunction):
    """
    Return the hash of the first 10 characters (normalized)
    """

    def generate_embeddings(self, texts):
        return [self._compute_one_embedding(row) for row in texts]

    def _compute_one_embedding(self, row):
        emb = np.array([float(hash(c)) for c in row[:10]])
        emb /= np.linalg.norm(emb)
        return emb if len(emb) == 10 else [0] * 10

    def ndims(self):
        return 10


@registry.register("nonnorm")
class MockNonNormTextEmbeddingFunction(TextEmbeddingFunction):
    """
    Return the ord of the first 10 characters (not normalized)
    """

    def generate_embeddings(self, texts):
        return [self._compute_one_embedding(row) for row in texts]

    def _compute_one_embedding(self, row):
        emb = np.array([float(ord(c)) for c in row[:10]])
        return emb if len(emb) == 10 else [0] * 10

    def ndims(self):
        return 10


class RateLimitedAPI:
    rate_limit = 0.1  # 1 request per 0.1 second
    last_request_time = 0

    @staticmethod
    def make_request():
        current_time = time.time()

        if current_time - RateLimitedAPI.last_request_time < RateLimitedAPI.rate_limit:
            raise Exception("Rate limit exceeded. Please try again later.")

        # Simulate a successful request
        RateLimitedAPI.last_request_time = current_time
        return "Request successful"


@registry.register("test-rate-limited")
class MockRateLimitedEmbeddingFunction(MockTextEmbeddingFunction):
    def generate_embeddings(self, texts):
        RateLimitedAPI.make_request()
        return [self._compute_one_embedding(row) for row in texts]
