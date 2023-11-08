from typing import List

import pytest

import lancedb

try:
    import langchain
    from langchain.schema.embeddings import Embeddings
    from langchain.vectorstores import LanceDB
except ImportError:
    langchain = None


@pytest.mark.skipif(langchain is None, reason="langchain not installed")
class TestLangchain:
    def test_lancedb(self) -> None:
        embeddings = self._fake_embeddings()
        db = lancedb.connect("/tmp/lancedb")
        texts = ["text 1", "text 2", "item 3"]
        vectors = embeddings.embed_documents(texts)
        table = db.create_table(
            "my_table",
            data=[
                {"vector": vectors[idx], "id": text, "text": text}
                for idx, text in enumerate(texts)
            ],
            mode="overwrite",
        )
        store = LanceDB(table, embeddings)
        result = store.similarity_search("text 1")
        result_texts = [doc.page_content for doc in result]
        assert "text 1" in result_texts

    def test_lancedb_add_texts(self) -> None:
        embeddings = self._fake_embeddings()
        db = lancedb.connect("/tmp/lancedb")
        texts = ["text 1"]
        vectors = embeddings.embed_documents(texts)
        table = db.create_table(
            "my_table",
            data=[
                {"vector": vectors[idx], "id": text, "text": text}
                for idx, text in enumerate(texts)
            ],
            mode="overwrite",
        )
        store = LanceDB(table, embeddings)
        store.add_texts(["text 2"])
        result = store.similarity_search("text 2")
        result_texts = [doc.page_content for doc in result]
        assert "text 2" in result_texts

    @staticmethod
    def _fake_embeddings():
        class FakeEmbeddings(Embeddings):
            """Fake embeddings functionality for testing."""

            def embed_documents(self, texts: List[str]) -> List[List[float]]:
                """Return simple embeddings.
                Embeddings encode each text as its index."""
                return [[float(1.0)] * 9 + [float(i)] for i in range(len(texts))]

            async def aembed_documents(self, texts: List[str]) -> List[List[float]]:
                return self.embed_documents(texts)

            def embed_query(self, text: str) -> List[float]:
                """Return constant query embeddings.
                Embeddings are identical to embed_documents(texts)[0].
                Distance to each text will be that text's index,
                as it was passed to embed_documents."""
                return [float(1.0)] * 9 + [float(0.0)]

            async def aembed_query(self, text: str) -> List[float]:
                return self.embed_query(text)

        return FakeEmbeddings()
