# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import os
import io
import base64
from urllib.parse import urlparse
from pathlib import Path
from typing import TYPE_CHECKING, ClassVar, List, Union, Optional, Any, Dict

import numpy as np
import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import api_key_not_found_help, TEXT, IMAGES, url_retrieve

if TYPE_CHECKING:
    import PIL

API_URL = "https://api.jina.ai/v1/embeddings"


def is_valid_url(text):
    try:
        parsed = urlparse(text)
        return bool(parsed.scheme) and bool(parsed.netloc)
    except Exception:
        return False


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

    @staticmethod
    def sensitive_keys() -> List[str]:
        return ["api_key"]

    def sanitize_input(
        self, inputs: Union[TEXT, IMAGES]
    ) -> Union[List[Any], np.ndarray]:
        """
        Sanitize the input to the embedding function.
        """
        if isinstance(inputs, (str, bytes, Path)):
            inputs = [inputs]
        elif isinstance(inputs, pa.Array):
            inputs = inputs.to_pylist()
        elif isinstance(inputs, pa.ChunkedArray):
            inputs = inputs.combine_chunks().to_pylist()
        else:
            if isinstance(inputs, list):
                inputs = inputs
            else:
                PIL = attempt_import_or_raise("PIL", "pillow")
                if isinstance(inputs, PIL.Image.Image):
                    inputs = [inputs]
        return inputs

    @staticmethod
    def _generate_image_input_dict(image: Union[str, bytes, "PIL.Image.Image"]) -> Dict:
        if isinstance(image, bytes):
            image_dict = {"image": base64.b64encode(image).decode("utf-8")}
        elif isinstance(image, (str, Path)):
            parsed = urlparse.urlparse(image)
            # TODO handle drive letter on windows.
            PIL = attempt_import_or_raise("PIL", "pillow")
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
            image_dict = {"image": base64.b64encode(image_bytes).decode("utf-8")}
        else:
            PIL = attempt_import_or_raise("PIL", "pillow")

            if isinstance(image, PIL.Image.Image):
                buffered = io.BytesIO()
                image.save(buffered, format="PNG")
                image_bytes = buffered.getvalue()
                image_dict = {"image": base64.b64encode(image_bytes).decode("utf-8")}
            else:
                raise TypeError(
                    f"JinaEmbeddingFunction supports str, Path, bytes or PIL Image"
                    f" as query, but {type(image)} is given"
                )
        return image_dict

    def compute_query_embeddings(
        self, query: Union[str, bytes, "Path", "PIL.Image.Image"], *args, **kwargs
    ) -> List[np.ndarray]:
        """
        Compute the embeddings for a given user query

        Parameters
        ----------
        query : Union[str, PIL.Image.Image]
            The query to embed. A query can be either text or an image.
        """
        if isinstance(query, str):
            if not is_valid_url(query):
                return self.generate_text_embeddings([query])
            else:
                return [self.generate_image_embedding(query)]
        elif isinstance(query, (Path, bytes)):
            return [self.generate_image_embedding(query)]
        else:
            PIL = attempt_import_or_raise("PIL", "pillow")

            if isinstance(query, PIL.Image.Image):
                return [self.generate_image_embedding(query)]
            else:
                raise TypeError(
                    f"JinaEmbeddingFunction supports str, Path, bytes or PIL Image"
                    f" as query, but {type(query)} is given"
                )

    def compute_source_embeddings(
        self, inputs: Union[TEXT, IMAGES], *args, **kwargs
    ) -> List[np.array]:
        inputs = self.sanitize_input(inputs)
        model_inputs = []
        image_inputs = 0

        def process_input(input, model_inputs, image_inputs):
            if isinstance(input, str):
                if not is_valid_url(input):
                    model_inputs.append({"text": input})
                else:
                    image_inputs += 1
                    model_inputs.append(self._generate_image_input_dict(input))
            elif isinstance(input, list):
                for _input in input:
                    image_inputs = process_input(_input, model_inputs, image_inputs)
            else:
                image_inputs += 1
                model_inputs.append(self._generate_image_input_dict(input))
            return image_inputs

        for input in inputs:
            image_inputs = process_input(input, model_inputs, image_inputs)

        if image_inputs > 0:
            return self._generate_embeddings(model_inputs)
        else:
            return self.generate_text_embeddings(inputs)

    def generate_image_embedding(
        self, image: Union[str, bytes, Path, "PIL.Image.Image"]
    ) -> np.ndarray:
        """
        Generate the embedding for a single image

        Parameters
        ----------
        image : Union[str, bytes, PIL.Image.Image]
            The image to embed. If the image is a str, it is treated as a uri.
            If the image is bytes, it is treated as the raw image bytes.
        """
        image_dict = self._generate_image_input_dict(image)
        return self._generate_embeddings(input=[image_dict])[0]

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
        import requests

        if JinaEmbeddings._session is None:
            if self.api_key is None and os.environ.get("JINA_API_KEY") is None:
                api_key_not_found_help("jina")
            api_key = self.api_key or os.environ.get("JINA_API_KEY")
            JinaEmbeddings._session = requests.Session()
            JinaEmbeddings._session.headers.update(
                {"Authorization": f"Bearer {api_key}", "Accept-Encoding": "identity"}
            )
