import os
from enum import Enum
from typing import List, Optional

from ibm_watsonx_ai import Credentials
from ibm_watsonx_ai.foundation_models import Embeddings
from lancedb.embeddings import EmbeddingFunctionRegistry, TextEmbeddingFunction

registry = EmbeddingFunctionRegistry.get_instance()


class WatsonxEnv(str, Enum):
    API_KEY = "WATSONX_API_KEY"
    PROJECT_ID = "WATSONX_PROJECT_ID"
    URL = "WATSONX_URL"
    EMBEDDINGS_MODEL = "WATSONX_EMBEDDINGS_MODEL"
    NUM_DIMS = "WATSONX_EMBEDDINGS_MODEL_NUM_DIMS"

    def __str__(self):
        return self.value


@registry.register("watsonx")
class WatsonxEmbeddings(TextEmbeddingFunction):
    _client: Optional[Embeddings] = None
    _ndims: Optional[int] = None

    url: str
    model: str

    def __init__(
        self,
        url: Optional[str] = None,
        model: Optional[str] = None,
        ndims: Optional[int] = None,
        **kwargs,
    ):
        api_key = os.getenv(WatsonxEnv.API_KEY, None)
        if api_key is None:
            raise ValueError(f"{WatsonxEnv.API_KEY.value} must be set")

        project_id = os.getenv(WatsonxEnv.PROJECT_ID, None)
        if project_id is None:
            raise ValueError(f"{WatsonxEnv.PROJECT_ID.value} must be set")

        url = os.getenv(WatsonxEnv.URL, None)
        if url is None:
            raise ValueError(f"{WatsonxEnv.URL.value} must be set")

        model = os.getenv(WatsonxEnv.EMBEDDINGS_MODEL, None)
        if model is None:
            raise ValueError(f"{WatsonxEnv.EMBEDDINGS_MODEL.value} must be set")

        super().__init__(
            model=model,
            url=url,
            **kwargs,
        )
        self._client = Embeddings(
            model_id=model,
            params={},
            credentials=Credentials(
                api_key=api_key,
                url=url,
            ),
            project_id=project_id,
        )

        if ndims is None:
            ndims = os.getenv(WatsonxEnv.NUM_DIMS, None)
        if ndims is not None:
            self._ndims = self.ndims

    def generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        assert self._client is not None
        return self._client.embed_documents(texts=texts)

    @property
    def ndims(self) -> int:
        if self._ndims is None:
            vector = self.generate_embeddings(["foo"])[0]
            self._ndims = len(vector)
        return self._ndims
