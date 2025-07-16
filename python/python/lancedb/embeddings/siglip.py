# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


import io
import os
from typing import List, Union
from urllib import parse as urlparse
import numpy as np
import PIL
import pyarrow as pa
import torch
from tqdm import tqdm
import time
from pydantic import PrivateAttr
from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import IMAGES, url_retrieve


@register("siglip")
class SigLipEmbeddings(EmbeddingFunction):
    model_name: str = "google/siglip-base-patch16-224"
    device: str = "cpu"
    batch_size: int = 64
    normalize: bool = True
    _model = PrivateAttr()  # Use the full model instead of separate text/vision
    _processor = PrivateAttr()

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        transformers = attempt_import_or_raise("transformers")

        # Load the full SigLIP model (not separate text/vision models)
        self._processor = transformers.AutoProcessor.from_pretrained(self.model_name)
        self._model = transformers.SiglipModel.from_pretrained(self.model_name)

        # Move model to device
        self._model.to(self.device)
        self._model.eval()
        self._ndims = None

    def ndims(self):
        if self._ndims is None:
            result = self.generate_text_embeddings("test query")
            if result is None:
                raise ValueError("generate_text_embeddings returned None")
            self._ndims = result.shape[0]
        return self._ndims

    def compute_query_embeddings(
        self, query: Union[str, "PIL.Image.Image"], *args, **kwargs
    ) -> List[np.ndarray]:
        if isinstance(query, str):
            # Preprocess text to be more descriptive if needed

            return [self.generate_text_embeddings(query)]
        else:
            PIL = attempt_import_or_raise("PIL", "pillow")
            if isinstance(query, PIL.Image.Image):
                return [self.generate_image_embedding(query)]
            else:
                raise TypeError("SigLIP supports str or PIL Image as query")

    def generate_text_embeddings(self, text: str) -> np.ndarray:
        inputs = self._processor(
            text=text,
            return_tensors="pt",
            padding="max_length",
            truncation=True,
            max_length=64,
        ).to(self.device)

        with torch.no_grad():
            text_features = self._model.get_text_features(**inputs)

            if self.normalize:
                text_features = text_features / text_features.norm(dim=-1, keepdim=True)

            return text_features.cpu().numpy().squeeze()

    def sanitize_input(self, images: IMAGES) -> Union[List[bytes], np.ndarray]:
        if isinstance(images, (str, bytes)):
            images = [images]
        elif isinstance(images, pa.Array):
            images = images.to_pylist()
        elif isinstance(images, pa.ChunkedArray):
            images = images.combine_chunks().to_pylist()
        return np.array(images)

    def _batch_generate_embeddings(
            self, images: List[Union[str, bytes, "PIL.Image.Image"]]
            ) -> List[np.ndarray]:
        """Process multiple images in a single forward pass"""
        # Convert all images to PIL at once
        pil_images = [self._to_pil(image) for image in images]

        # Process entire batch together
        inputs = self._processor(images=pil_images, return_tensors="pt").to(self.device)

        with torch.no_grad():
            # Single forward pass for entire batch
            image_features = self._model.get_image_features(**inputs)

            if self.normalize:
               image_features = image_features / image_features.norm(
                dim=-1,
                keepdim=True
            )


            # Convert to list of individual embeddings
            embeddings = image_features.cpu().numpy()
            return [emb for emb in embeddings]

    def compute_source_embeddings(
            self, images: IMAGES, *args, **kwargs
            ) -> List[np.ndarray]:
        images = self.sanitize_input(images)
        embeddings = []

        total_images = len(images)
        total_batches = (total_images + self.batch_size - 1) // self.batch_size

        print(
        f"Processing {total_images} images in {total_batches} batches "
        f"(batch size: {self.batch_size})"
    )


        start_time = time.time()
        progress_bar = tqdm(total=total_images, desc="SigLIP Embedding")

        for i in range(0, total_images, self.batch_size):
            j = min(i + self.batch_size, total_images)
            batch = images[i:j]

            # Process entire batch at once instead of individual images
            batch_embeddings = self._batch_generate_embeddings(batch)
            embeddings.extend(batch_embeddings)

            progress_bar.update(len(batch))

        progress_bar.close()
        total_time = time.time() - start_time
        print(f"✓ Completed processing all {total_images} images in {total_time:.1f}s")

        return embeddings

    def generate_image_embedding(
        self, image: Union[str, bytes, "PIL.Image.Image"]
    ) -> np.ndarray:
        pil_image = self._to_pil(image)
        inputs = self._processor(images=pil_image, return_tensors="pt").to(self.device)

        with torch.no_grad():
            # Use the full model's get_image_features method
            image_features = self._model.get_image_features(**inputs)

            if self.normalize:
                image_features = image_features / image_features.norm(
                    dim=-1, keepdim=True
                )

            return image_features.cpu().numpy().squeeze()

    def _to_pil(self, image: Union[str, bytes, "PIL.Image.Image"]):
        PIL = attempt_import_or_raise("PIL", "pillow")

        # Return PIL images directly if already in correct format
        if isinstance(image, PIL.Image.Image):
            return image.convert('RGB') if image.mode != 'RGB' else image

        # Handle bytes with faster I/O
        elif isinstance(image, bytes):
            return PIL.Image.open(io.BytesIO(image)).convert('RGB')

        # Handle string paths/URLs
        elif isinstance(image, str):
            parsed = urlparse.urlparse(image)

            if parsed.scheme == "file":
                return PIL.Image.open(parsed.path).convert('RGB')
            elif parsed.scheme == "":
                path = image if os.name == "nt" else parsed.path
                return PIL.Image.open(path).convert('RGB')
            elif parsed.scheme.startswith("http"):
                # Cache HTTP requests to avoid re-downloading
                image_bytes = url_retrieve(image)
                return PIL.Image.open(io.BytesIO(image_bytes)).convert('RGB')
            else:
                raise NotImplementedError("Only local and http(s) urls are supported")

        else:
            raise ValueError(f"Unsupported image type: {type(image)}")
