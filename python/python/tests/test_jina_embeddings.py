# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import pytest

from lancedb.embeddings import get_registry


@pytest.mark.parametrize(
    "name,dims",
    [
        ("jina-clip-v1", 768),
        ("jina-clip-v2", 1024),
        ("jina-embeddings-v2-small-en", 512),
        ("jina-embeddings-v2-base-en", 768),
        ("jina-embeddings-v3", 1024),
        ("jina-embeddings-v4", 2048),
    ],
)
def test_jina_ndims_per_model(name, dims):
    func = get_registry().get("jina").create(name=name)
    assert func.ndims() == dims


def test_jina_ndims_unknown_model():
    func = get_registry().get("jina").create(name="not-a-jina-model")
    with pytest.raises(ValueError, match="Unknown Jina model"):
        func.ndims()
