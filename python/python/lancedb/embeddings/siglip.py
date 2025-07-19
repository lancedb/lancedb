# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import concurrent.futures
import io
import os
from typing import TYPE_CHECKING, List, Union
import urllib.parse as urlparse

import numpy as np
import pyarrow as pa
from tqdm import tqdm
from pydantic import PrivateAttr

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import IMAGES, url_retrieve

if TYPE_CHECKING:
    import PIL
    import torch


@register("siglip")
class SigLipEmbeddings(EmbeddingFunction):
    model_name: str = "google/siglip-base-patch16-224"
    device: str = "cpu"
    batch_size: int = 64
    normalize: bool = True

    _model = PrivateAttr()
    _processor = PrivateAttr()
    _tokenizer = PrivateAttr()
    _torch = PrivateAttr()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        transformers = attempt_import_or_raise("transformers")
        self._torch = attempt_import_or_raise("torch")

        self._processor = transformers.AutoProcessor.from_pretrained(self.model_name)
        self._model = transformers.SiglipModel.from_pretrained(self.model_name)
        self._model.to(self.device)
        self._model.eval()
        self._ndims = None

    def ndims(self):
        if self._ndims is None:
            self._ndims = self.generate_text_embeddings("foo").shape[0]
        return self._ndims

    def compute_query_embeddings(
        self, query: Union[str, "PIL.Image.Image"], *args, **kwargs
    ) -> List[np.ndarray]:
        if isinstance(query, str):
            return [self.generate_text_embeddings(query)]
        else:
            PIL = attempt_import_or_raise("PIL", "pillow")
            if isinstance(query, PIL.Image.Image):
                return [self.generate_image_embedding(query)]
            else:
                raise TypeError("SigLIP supports str or PIL Image as query")

    def generate_text_embeddings(self, text: str) -> np.ndarray:
        torch = self._torch
        text_inputs = self._processor(
            text=text,
            return_tensors="pt",
            padding="max_length",
            truncation=True,
            max_length=64,
        ).to(self.device)

        with torch.no_grad():
            text_features = self._model.get_text_features(**text_inputs)
            if self.normalize:
                text_features = text_features / text_features.norm(dim=-1, keepdim=True)
            return text_features.cpu().detach().numpy().squeeze()

    def sanitize_input(self, images: IMAGES) -> Union[List[bytes], np.ndarray]:
        if isinstance(images, (str, bytes)):
            images = [images]
        elif isinstance(images, pa.Array):
            images = images.to_pylist()
        elif isinstance(images, pa.ChunkedArray):
            images = images.combine_chunks().to_pylist()
        return images

    def compute_source_embeddings(
        self, images: IMAGES, *args, **kwargs
    ) -> List[np.ndarray]:
        images = self.sanitize_input(images)
        embeddings = []

        for i in range(0, len(images), self.batch_size):
            j = min(i + self.batch_size, len(images))
            batch = images[i:j]
            embeddings.extend(self._parallel_get(batch))
        return embeddings

    def _parallel_get(self, images: Union[List[str], List[bytes]]) -> List[np.ndarray]:
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [
                executor.submit(self.generate_image_embedding, image)
                for image in images
            ]
            return [f.result() for f in tqdm(futures, desc="SigLIP Embedding")]

    def generate_image_embedding(
        self, image: Union[str, bytes, "PIL.Image.Image"]
    ) -> np.ndarray:
        image = self._to_pil(image)
        image = self._processor(images=image, return_tensors="pt")["pixel_values"]
        return self._encode_and_normalize_image(image)

    def _encode_and_normalize_image(self, image_tensor: "torch.Tensor") -> np.ndarray:
        torch = self._torch
        with torch.no_grad():
            image_features = self._model.get_image_features(
                image_tensor.to(self.device)
            )
            if self.normalize:
                image_features = image_features / image_features.norm(
                    dim=-1, keepdim=True
                )
            return image_features.cpu().detach().numpy().squeeze()

    def _to_pil(self, image: Union[str, bytes, "PIL.Image.Image"]):
        PIL = attempt_import_or_raise("PIL", "pillow")
        if isinstance(image, PIL.Image.Image):
            return image.convert("RGB") if image.mode != "RGB" else image
        elif isinstance(image, bytes):
            return PIL.Image.open(io.BytesIO(image)).convert("RGB")
        elif isinstance(image, str):
            parsed = urlparse.urlparse(image)
            if parsed.scheme == "file":
                return PIL.Image.open(parsed.path).convert("RGB")
            elif parsed.scheme == "":
                path = image if os.name == "nt" else parsed.path
                return PIL.Image.open(path).convert("RGB")
            elif parsed.scheme.startswith("http"):
                image_bytes = url_retrieve(image)
                return PIL.Image.open(io.BytesIO(image_bytes)).convert("RGB")
            else:
                raise NotImplementedError("Only local and http(s) urls are supported")
        else:
            raise ValueError(f"Unsupported image type: {type(image)}")
