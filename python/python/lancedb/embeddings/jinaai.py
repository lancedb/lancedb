#  Copyright (c) 2023. LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

import os
import io
import requests
import base64
import urllib.parse as urlparse
from typing import ClassVar, List, Union, Optional, TYPE_CHECKING

import numpy as np
import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import api_key_not_found_help, TEXT, IMAGES, url_retrieve

if TYPE_CHECKING:
    import PIL

API_URL = "https://api.jina.ai/v1/embeddings"


@register("jina")
class JinaEmbeddings(EmbeddingFunction):
    """
    An embedding function that uses the Jina API

    https://jina.ai/embeddings/

    Parameters
    ----------
    name: str, default "jina-clip-v1". Note that some models support both image
        and text embeddings and some just text embedding

    api_key: str, default None
        The api key to access Jina API. If you pass None, you can set JINA_API_KEY
        environment variable

    """

    name: str = "jina-clip-v1"
    api_key: Optional[str] = None
    _session: ClassVar = None

    def ndims(self):
        # TODO: fix hardcoding
        return 768

    def sanitize_input(self, inputs: IMAGES) -> Union[List[bytes], np.ndarray]:
        """
        Sanitize the input to the embedding function.
        """
        if isinstance(inputs, (str, bytes)):
            inputs = [inputs]
        elif isinstance(inputs, pa.Array):
            inputs = inputs.to_pylist()
        elif isinstance(inputs, pa.ChunkedArray):
            inputs = inputs.combine_chunks().to_pylist()
        return inputs

    def compute_query_embeddings(
        self, query: Union[str, "PIL.Image.Image"], *args, **kwargs
    ) -> List[np.ndarray]:
        """
        Compute the embeddings for a given user query

        Parameters
        ----------
        query : Union[str, PIL.Image.Image]
            The query to embed. A query can be either text or an image.
        """
        if isinstance(query, str):
            return self.generate_text_embeddings([query])
        else:
            PIL = attempt_import_or_raise("PIL", "pillow")
            if isinstance(query, PIL.Image.Image):
                return [self.generate_image_embedding(query)]
            else:
                raise TypeError(
                    "JinaEmbeddingFunction supports str or PIL Image as query"
                )

    def compute_source_embeddings(self, texts: TEXT, *args, **kwargs) -> List[np.array]:
        texts = self.sanitize_input(texts)
        return self.generate_text_embeddings(texts)

    def generate_image_embedding(
        self, image: Union[str, bytes, "PIL.Image.Image"]
    ) -> np.ndarray:
        """
        Generate the embedding for a single image

        Parameters
        ----------
        image : Union[str, bytes, PIL.Image.Image]
            The image to embed. If the image is a str, it is treated as a uri.
            If the image is bytes, it is treated as the raw image bytes.
        """
        PIL = attempt_import_or_raise("PIL", "pillow")
        if isinstance(image, bytes):
            image = {"image": base64.b64encode(image).decode("utf-8")}
        if isinstance(image, PIL.Image.Image):
            buffered = io.BytesIO()
            image.save(buffered, format="PNG")
            image_bytes = buffered.getvalue()
            image = {"image": base64.b64encode(image_bytes).decode("utf-8")}
        elif isinstance(image, str):
            parsed = urlparse.urlparse(image)
            # TODO handle drive letter on windows.
            if parsed.scheme == "file":
                pil_image = PIL.Image.open(parsed.path)
            elif parsed.scheme == "":
                pil_image = PIL.Image.open(image if os.name == "nt" else parsed.path)
            elif parsed.scheme.startswith("http"):
                pil_image = PIL.Image.open(io.BytesIO(url_retrieve(image)))
            else:
                raise NotImplementedError("Only local and http(s) urls are supported")
            buffered = io.BytesIO()
            pil_image.save(buffered, format="PNG")
            image_bytes = buffered.getvalue()
            image = {"image": base64.b64encode(image_bytes).decode("utf-8")}
        return self._generate_embeddings(input=[image])[0]

    def generate_text_embeddings(
        self, texts: Union[List[str], np.ndarray], *args, **kwargs
    ) -> List[np.array]:
        return self._generate_embeddings(input=texts)

    def _generate_embeddings(self, input: List, *args, **kwargs) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
        self._init_client()
        resp = JinaEmbeddings._session.post(  # type: ignore
            API_URL, json={"input": input, "model": self.name}
        ).json()
        if "data" not in resp:
            raise RuntimeError(resp["detail"])

        embeddings = resp["data"]

        # Sort resulting embeddings by index
        sorted_embeddings = sorted(embeddings, key=lambda e: e["index"])  # type: ignore

        return [result["embedding"] for result in sorted_embeddings]

    def _init_client(self):
        if JinaEmbeddings._session is None:
            if self.api_key is None and os.environ.get("JINA_API_KEY") is None:
                api_key_not_found_help("jina")
            api_key = self.api_key or os.environ.get("JINA_API_KEY")
            JinaEmbeddings._session = requests.Session()
            JinaEmbeddings._session.headers.update(
                {"Authorization": f"Bearer {api_key}", "Accept-Encoding": "identity"}
            )
