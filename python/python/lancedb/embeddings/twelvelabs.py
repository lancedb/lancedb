# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors
import io
import os
from pathlib import Path
from typing import ClassVar, List, TYPE_CHECKING, Union
from urllib.parse import urlparse

import numpy as np
import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import IMAGES, TEXT, api_key_not_found_help

if TYPE_CHECKING:
    import PIL

# Marengo embedding models and their output dimensions.
TWELVELABS_MODEL_DIMS = {
    "marengo3.0": 512,
    "Marengo-retrieval-2.7": 1024,
}


def _is_url(text: str) -> bool:
    try:
        parsed = urlparse(text)
        return bool(parsed.scheme) and bool(parsed.netloc)
    except Exception:
        return False


@register("twelvelabs")
class TwelveLabsEmbeddingFunction(EmbeddingFunction):
    """
    An embedding function that uses the TwelveLabs Marengo multimodal model.

    Marengo produces embeddings that live in a single space shared across text,
    image, audio, and video, so a text query can be matched directly against
    image or audio source columns. See https://docs.twelvelabs.io/ for details.

    The TwelveLabs API key is read from the ``TWELVELABS_API_KEY`` environment
    variable. You can grab a free key at https://twelvelabs.io.

    Parameters
    ----------
    name: str, default "marengo3.0"
        The name of the Marengo model to use. Supported models:

            * marengo3.0 (512 dims)
            * Marengo-retrieval-2.7 (1024 dims)

    Examples
    --------
    import lancedb
    from lancedb.pydantic import LanceModel, Vector
    from lancedb.embeddings import get_registry

    tl = get_registry().get("twelvelabs").create(name="marengo3.0")

    class Media(LanceModel):
        url: str = tl.SourceField()
        vector: Vector(tl.ndims()) = tl.VectorField()

    db = lancedb.connect("~/.lancedb")
    tbl = db.create_table("media", schema=Media, mode="overwrite")
    tbl.add([{"url": "https://example.com/cat.jpg"}])

    # Search images using a text query (shared embedding space)
    tbl.search("a cat sitting on a couch").limit(1).to_pandas()
    """

    name: str = "marengo3.0"
    client: ClassVar = None

    def ndims(self) -> int:
        if self.name not in TWELVELABS_MODEL_DIMS:
            raise ValueError(
                f"Model {self.name} not supported. "
                f"Supported models: {list(TWELVELABS_MODEL_DIMS)}"
            )
        return TWELVELABS_MODEL_DIMS[self.name]

    @staticmethod
    def sensitive_keys() -> List[str]:
        return ["api_key"]

    def compute_query_embeddings(
        self, query: Union[str, "PIL.Image.Image", bytes, Path], *args, **kwargs
    ) -> List[np.ndarray]:
        """Compute the embedding for a single text or image query."""
        return [self._embed_one(query)]

    def compute_source_embeddings(
        self, inputs: Union[TEXT, IMAGES], *args, **kwargs
    ) -> List[np.ndarray]:
        """Compute embeddings for a column of text and/or image inputs."""
        return [self._embed_one(i) for i in self._sanitize(inputs)]

    def _sanitize(self, inputs: Union[TEXT, IMAGES]) -> List:
        PIL_Image = attempt_import_or_raise("PIL.Image", "pillow")
        if isinstance(inputs, (str, bytes, Path, PIL_Image.Image)):
            return [inputs]
        if isinstance(inputs, pa.Array):
            return inputs.to_pylist()
        if isinstance(inputs, pa.ChunkedArray):
            return inputs.combine_chunks().to_pylist()
        if isinstance(inputs, (list, np.ndarray)):
            return list(inputs)
        raise ValueError(f"Input type {type(inputs)} not allowed.")

    def _embed_one(
        self, item: Union[str, bytes, Path, "PIL.Image.Image"]
    ) -> np.ndarray:
        client = self._get_client()
        PIL_Image = attempt_import_or_raise("PIL.Image", "pillow")

        if isinstance(item, str) and not _is_url(item):
            resp = client.embed.create(model_name=self.name, text=item)
            result = resp.text_embedding
        elif isinstance(item, str):
            resp = client.embed.create(model_name=self.name, image_url=item)
            result = resp.image_embedding
        else:
            # Local image: Path, raw bytes, or a PIL image.
            if isinstance(item, Path):
                image_file = item.read_bytes()
            elif isinstance(item, bytes):
                image_file = item
            elif isinstance(item, PIL_Image.Image):
                buf = io.BytesIO()
                item.save(buf, format="JPEG")
                image_file = buf.getvalue()
            else:
                raise ValueError("Each input should be str, bytes, Path or PIL.Image.")
            resp = client.embed.create(model_name=self.name, image_file=image_file)
            result = resp.image_embedding

        if result is None or not result.segments:
            raise ValueError(
                f"TwelveLabs returned no embedding for input "
                f"(error: {getattr(result, 'error_message', None)})"
            )
        return np.array(result.segments[0].float_)

    @classmethod
    def _get_client(cls):
        if cls.client is None:
            twelvelabs = attempt_import_or_raise("twelvelabs")
            if os.environ.get("TWELVELABS_API_KEY") is None:
                api_key_not_found_help("twelvelabs")
            cls.client = twelvelabs.TwelveLabs(api_key=os.environ["TWELVELABS_API_KEY"])
        return cls.client
