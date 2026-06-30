import lancedb
from lancedb.embeddings import get_registry
from lancedb.pydantic import LanceModel, Vector
import os

def test_ollama_serialization():
    db = lancedb.connect("./test_db")
    model = get_registry().get("ollama").create(name="nomic-embed-text")
    
    class MyModel(LanceModel):
        text: str = model.SourceField()
        vector: Vector(model.ndims()) = model.VectorField()

    try:
        db.create_table("test_table", schema=MyModel, mode="overwrite")
        print("Success: Table created without serialization error")
    except TypeError as e:
        print(f"Failure: Serialization error still exists: {e}")

if __name__ == "__main__":
    test_ollama_serialization()