# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import lancedb

# --8<-- [start:imports]
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

# --8<-- [end:imports]
import pytest


@pytest.mark.slow
def test_embeddings_openai():
    uri = "memory://"
    # --8<-- [start:openai_embeddings]
    registry = get_registry()
    registry.set_var("open_api_key", "sk-...")
    func = registry.get("openai").create(
        name="text-embedding-ada-002",
        api_key="$var:open_api_key",
    )

    class Words(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    db = lancedb.connect(uri)
    table = db.create_table("words", schema=Words, mode="overwrite")
    table.add([{"text": "hello world"}, {"text": "goodbye world"}])

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    print(actual.text)
    # --8<-- [end:openai_embeddings]
