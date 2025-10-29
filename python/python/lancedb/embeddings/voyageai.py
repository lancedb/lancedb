# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
import base64
import os
from typing import ClassVar, TYPE_CHECKING, List, Union, Any, Generator

from pathlib import Path
from urllib.parse import urlparse
from io import BytesIO

import numpy as np
import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import api_key_not_found_help, IMAGES, TEXT

if TYPE_CHECKING:
    import PIL

# Token limits for different VoyageAI models
VOYAGE_TOTAL_TOKEN_LIMITS = {
    "voyage-context-3": 32_000,
    "voyage-3.5-lite": 1_000_000,
    "voyage-3.5": 320_000,
    "voyage-3-lite": 120_000,
    "voyage-3": 120_000,
    "voyage-multimodal-3": 120_000,
    "voyage-finance-2": 120_000,
    "voyage-multilingual-2": 120_000,
    "voyage-law-2": 120_000,
    "voyage-code-2": 120_000,
}

# Batch size for embedding requests (max number of items per batch)
BATCH_SIZE = 1000


def is_valid_url(text):
    try:
        parsed = urlparse(text)
        return bool(parsed.scheme) and bool(parsed.netloc)
    except Exception:
        return False


def transform_input(input_data: Union[str, bytes, Path]):
    PIL = attempt_import_or_raise("PIL", "pillow")
    if isinstance(input_data, str):
        if is_valid_url(input_data):
            content = {"type": "image_url", "image_url": input_data}
        else:
            content = {"type": "text", "text": input_data}
    elif isinstance(input_data, PIL.Image.Image):
        buffered = BytesIO()
        input_data.save(buffered, format="JPEG")
        img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
        content = {
            "type": "image_base64",
            "image_base64": "data:image/jpeg;base64," + img_str,
        }
    elif isinstance(input_data, bytes):
        img = PIL.Image.open(BytesIO(input_data))
        buffered = BytesIO()
        img.save(buffered, format="JPEG")
        img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
        content = {
            "type": "image_base64",
            "image_base64": "data:image/jpeg;base64," + img_str,
        }
    elif isinstance(input_data, Path):
        img = PIL.Image.open(input_data)
        buffered = BytesIO()
        img.save(buffered, format="JPEG")
        img_str = base64.b64encode(buffered.getvalue()).decode("utf-8")
        content = {
            "type": "image_base64",
            "image_base64": "data:image/jpeg;base64," + img_str,
        }
    else:
        raise ValueError("Each input should be either str, bytes, Path or Image.")

    return {"content": [content]}


def sanitize_multimodal_input(inputs: Union[TEXT, IMAGES]) -> List[Any]:
    """
    Sanitize the input to the embedding function.
    """
    PIL = attempt_import_or_raise("PIL", "pillow")
    if isinstance(inputs, (str, bytes, Path, PIL.Image.Image)):
        inputs = [inputs]
    elif isinstance(inputs, pa.Array):
        inputs = inputs.to_pylist()
    elif isinstance(inputs, pa.ChunkedArray):
        inputs = inputs.combine_chunks().to_pylist()
    else:
        raise ValueError(
            f"Input type {type(inputs)} not allowed with multimodal model."
        )

    if not all(isinstance(x, (str, bytes, Path, PIL.Image.Image)) for x in inputs):
        raise ValueError("Each input should be either str, bytes, Path or Image.")

    return [transform_input(i) for i in inputs]


def sanitize_text_input(inputs: TEXT) -> List[str]:
    """
    Sanitize the input to the embedding function.
    """
    if isinstance(inputs, str):
        inputs = [inputs]
    elif isinstance(inputs, pa.Array):
        inputs = inputs.to_pylist()
    elif isinstance(inputs, pa.ChunkedArray):
        inputs = inputs.combine_chunks().to_pylist()
    else:
        raise ValueError(f"Input type {type(inputs)} not allowed with text model.")

    if not all(isinstance(x, str) for x in inputs):
        raise ValueError("Each input should be str.")

    return inputs


@register("voyageai")
class VoyageAIEmbeddingFunction(EmbeddingFunction):
    """
    An embedding function that uses the VoyageAI API

    https://docs.voyageai.com/docs/embeddings

    Parameters
    ----------
    name: str
        The name of the model to use. List of acceptable models:

            * voyage-context-3
            * voyage-3.5
            * voyage-3.5-lite
            * voyage-3
            * voyage-3-lite
            * voyage-multimodal-3
            * voyage-finance-2
            * voyage-multilingual-2
            * voyage-law-2
            * voyage-code-2


    Examples
    --------
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import EmbeddingFunctionRegistry

    voyageai = EmbeddingFunctionRegistry
        .get_instance()
        .get("voyageai")
        .create(name="voyage-3")

    class TextModel(LanceModel):
        text: str = voyageai.SourceField()
        vector: Vector(voyageai.ndims()) =  voyageai.VectorField()

    data = [ { "text": "hello world" },
            { "text": "goodbye world" }]

    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("test", schema=TextModel, mode="overwrite")

    tbl.add(data)

    """

    name: str
    client: ClassVar = None
    text_embedding_models: list = [
        "voyage-3.5",
        "voyage-3.5-lite",
        "voyage-3",
        "voyage-3-lite",
        "voyage-finance-2",
        "voyage-multilingual-2",
        "voyage-law-2",
        "voyage-code-2",
    ]
    multimodal_embedding_models: list = ["voyage-multimodal-3"]
    contextual_embedding_models: list = ["voyage-context-3"]

    def _is_multimodal_model(self, model_name: str):
        return (
            model_name in self.multimodal_embedding_models or "multimodal" in model_name
        )

    def _is_contextual_model(self, model_name: str):
        return model_name in self.contextual_embedding_models or "context" in model_name

    def ndims(self):
        if self.name == "voyage-3-lite":
            return 512
        elif self.name == "voyage-code-2":
            return 1536
        elif self.name in [
            "voyage-context-3",
            "voyage-3.5",
            "voyage-3.5-lite",
            "voyage-3",
            "voyage-multimodal-3",
            "voyage-finance-2",
            "voyage-multilingual-2",
            "voyage-law-2",
            "voyage-multimodal-3",
        ]:
            return 1024
        else:
            raise ValueError(f"Model {self.name} not supported")

    def compute_query_embeddings(
        self, query: Union[str, "PIL.Image.Image"], *args, **kwargs
    ) -> List[np.ndarray]:
        """
        Compute the embeddings for a given user query

        Parameters
        ----------
        query : Union[str, PIL.Image.Image]
            The query to embed. A query can be either text or an image.

        Returns
        -------
            List[np.array]: the list of embeddings
        """
        client = VoyageAIEmbeddingFunction._get_client()
        if self._is_multimodal_model(self.name):
            result = client.multimodal_embed(
                inputs=[[query]], model=self.name, input_type="query", **kwargs
            )
        elif self._is_contextual_model(self.name):
            result = client.contextualized_embed(
                inputs=[[query]], model=self.name, input_type="query", **kwargs
            )
            result = result.results[0]
        else:
            result = client.embed(
                texts=[query], model=self.name, input_type="query", **kwargs
            )

        return [result.embeddings[0]]

    def compute_source_embeddings(
        self, inputs: Union[TEXT, IMAGES], *args, **kwargs
    ) -> List[np.array]:
        """
        Compute the embeddings for the inputs

        Parameters
        ----------
        inputs : Union[TEXT, IMAGES]
            The inputs to embed. The input can be either str, bytes, Path (to an image),
            PIL.Image or list of these.

        Returns
        -------
            List[np.array]: the list of embeddings
        """
        client = VoyageAIEmbeddingFunction._get_client()

        # For multimodal models, check if inputs contain images
        if self._is_multimodal_model(self.name):
            sanitized = sanitize_multimodal_input(inputs)
            has_images = any(
                inp["content"][0].get("type") != "text" for inp in sanitized
            )
            if has_images:
                # Use non-batched API for images
                result = client.multimodal_embed(
                    inputs=sanitized, model=self.name, input_type="document", **kwargs
                )
                return result.embeddings
            # Extract texts for batching
            inputs = [inp["content"][0]["text"] for inp in sanitized]
        else:
            inputs = sanitize_text_input(inputs)

        # Use batching for all text inputs
        return self._embed_with_batching(
            client, inputs, input_type="document", **kwargs
        )

    def _build_batches(
        self, client, texts: List[str]
    ) -> Generator[List[str], None, None]:
        """
        Generate batches of texts based on token limits using a generator.

        Parameters
        ----------
        client : voyageai.Client
            The VoyageAI client instance.
        texts : List[str]
            List of texts to batch.

        Yields
        ------
            List[str]: Batches of texts.
        """
        if not texts:
            return

        max_tokens_per_batch = VOYAGE_TOTAL_TOKEN_LIMITS.get(self.name, 120_000)
        current_batch: List[str] = []
        current_batch_tokens = 0

        # Tokenize all texts in one API call
        token_lists = client.tokenize(texts, model=self.name)
        token_counts = [len(token_list) for token_list in token_lists]

        for i, text in enumerate(texts):
            n_tokens = token_counts[i]

            # Check if adding this text would exceed limits
            if current_batch and (
                len(current_batch) >= BATCH_SIZE
                or (current_batch_tokens + n_tokens > max_tokens_per_batch)
            ):
                # Yield the current batch and start a new one
                yield current_batch
                current_batch = []
                current_batch_tokens = 0

            current_batch.append(text)
            current_batch_tokens += n_tokens

        # Yield the last batch (always has at least one text)
        if current_batch:
            yield current_batch

    def _get_embed_function(
        self, client, input_type: str = "document", **kwargs
    ) -> callable:
        """
        Get the appropriate embedding function based on model type.

        Parameters
        ----------
        client : voyageai.Client
            The VoyageAI client instance.
        input_type : str
            Either "query" or "document"
        **kwargs
            Additional arguments to pass to the embedding API

        Returns
        -------
            callable: A function that takes a batch of texts and returns embeddings.
        """
        if self._is_multimodal_model(self.name):

            def embed_batch(batch: List[str]) -> List[np.array]:
                batch_inputs = sanitize_multimodal_input(batch)
                result = client.multimodal_embed(
                    inputs=batch_inputs,
                    model=self.name,
                    input_type=input_type,
                    **kwargs,
                )
                return result.embeddings

            return embed_batch

        elif self._is_contextual_model(self.name):

            def embed_batch(batch: List[str]) -> List[np.array]:
                result = client.contextualized_embed(
                    inputs=[batch], model=self.name, input_type=input_type, **kwargs
                )
                return result.results[0].embeddings

            return embed_batch

        else:

            def embed_batch(batch: List[str]) -> List[np.array]:
                result = client.embed(
                    texts=batch, model=self.name, input_type=input_type, **kwargs
                )
                return result.embeddings

            return embed_batch

    def _embed_with_batching(
        self, client, texts: List[str], input_type: str = "document", **kwargs
    ) -> List[np.array]:
        """
        Embed texts with automatic batching based on token limits.

        Parameters
        ----------
        client : voyageai.Client
            The VoyageAI client instance.
        texts : List[str]
            List of texts to embed.
        input_type : str
            Either "query" or "document"
        **kwargs
            Additional arguments to pass to the embedding API

        Returns
        -------
            List[np.array]: List of embeddings.
        """
        if not texts:
            return []

        # Get the appropriate embedding function for this model type
        embed_fn = self._get_embed_function(client, input_type=input_type, **kwargs)

        # Process each batch
        all_embeddings = []
        for batch in self._build_batches(client, texts):
            batch_embeddings = embed_fn(batch)
            all_embeddings.extend(batch_embeddings)

        return all_embeddings

    @staticmethod
    def _get_client():
        if VoyageAIEmbeddingFunction.client is None:
            voyageai = attempt_import_or_raise("voyageai")
            if os.environ.get("VOYAGE_API_KEY") is None:
                api_key_not_found_help("voyageai")
            VoyageAIEmbeddingFunction.client = voyageai.Client(
                os.environ["VOYAGE_API_KEY"]
            )
        return VoyageAIEmbeddingFunction.client
