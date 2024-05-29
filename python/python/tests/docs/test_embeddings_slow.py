import lancedb

# --8<-- [start:imports]
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

# --8<-- [end:imports]
import pytest


@pytest.mark.slow
def test_embeddings_openai():
    # --8<-- [start:openai_embeddings]
    db = lancedb.connect("/tmp/db")
    func = get_registry().get("openai").create(name="text-embedding-ada-002")

    class Words(LanceModel):
        text: str = func.SourceField()
        vector: Vector(func.ndims()) = func.VectorField()

    table = db.create_table("words", schema=Words, mode="overwrite")
    table.add([{"text": "hello world"}, {"text": "goodbye world"}])

    query = "greetings"
    actual = table.search(query).limit(1).to_pydantic(Words)[0]
    print(actual.text)
    # --8<-- [end:openai_embeddings]
