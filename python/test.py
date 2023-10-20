import lancedb
import time
import numpy as np
from lancedb.embeddings import get_registry, TextEmbeddingFunction
from lancedb.embeddings.utils import RateLimitHandler
from lancedb.pydantic import LanceModel, Vector
from pydantic import PrivateAttr


class MockEmbeddingAPI:
    """
    Dummy class representing an embedding API that is rate limited
    """

    def __init__(self, rate_limit=0, time_unit=60):
        self.rate_limit = rate_limit
        self.time_unit = time_unit
        self.request_count = 0
        self.window_start_time = time.time()

    def embed(self, texts):
        if not self.rate_limit:  # no rate limit
            return self._process_api_request(texts)

        current_time = time.time()

        if current_time - self.window_start_time > self.time_unit:
            self.window_start_time = current_time
            self.request_count = 0

        if self.request_count < self.rate_limit:
            self.request_count += 1
            return self._process_api_request(texts)
        else:
            raise Exception("429")  # too many requests

    def _process_api_request(self, texts):
        return [self._compute_one_embedding(row) for row in texts]

    def _compute_one_embedding(self, row):
        emb = np.random.rand(10)
        emb /= np.linalg.norm(emb)
        return emb


@get_registry().register("test_rate")
class RateLimitedTextEmbeddingFunction(TextEmbeddingFunction):
    """
    Return the hash of the first 10 characters
    """

    # 1 request allowed per 0.1 sec
    _model: MockEmbeddingAPI = PrivateAttr(
        default=MockEmbeddingAPI(rate_limit=1, time_unit=0.1)
    )

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    def generate_embeddings(self, texts):
        rs = self._model.embed(texts)
        return rs

    def ndims(self):
        return 10


def test_rate_limited_embedding_function(tmp_path):
    def _get_schema(model):
        class Schema(LanceModel):
            text: str = model.SourceField()
            vector: Vector(model.ndims()) = model.VectorField()

        return Schema

    db = lancedb.connect(tmp_path)
    model_builder = get_registry().get("test_rate")
    """
    model = model_builder.create(rate_limit=1, time_unit=0.1) # without rate limiting
    table = db.create_table("test_without_limit", schema=_get_schema(model))

    table.add([{"text": "hello world"}]) # 
    assert len(table) == 1
    time.sleep(0.1)

    # Hits the rate limit
    with pytest.raises(Exception) as e:
        table.add([{"text": "hello world"}, {"text": "goodbye world"}])
    e.value == "429"
    """

    model = model_builder.create(rate_limit=1, time_unit=60)  # with rate limiting
    table = db.create_table(
        "test_with_limit", schema=_get_schema(model), mode="overwrite"
    )

    table.add([{"text": "hello"}])
    table.add([{"text": "hello"}])
    table.add([{"text": "hello"}])
    assert len(table) == 3


if __name__ == "__main__":
    test_rate_limited_embedding_function("/tmp/lancedb")
