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

from functools import cached_property
from typing import List, Union

import numpy as np
import pyarrow as pa

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import AUDIO, IMAGES, TEXT

from lancedb.pydantic import PYDANTIC_VERSION


@register("imagebind")
class ImageBindEmbeddings(EmbeddingFunction):
    """
    An embedding function that uses the ImageBind API
    For generating multi-modal embeddings across
    six different modalities: images, text, audio, depth, thermal, and IMU data

    to download package, run :
        `pip install imagebind-packaged==0.1.2`
    """

    name: str = "imagebind_huge"
    device: str = "cpu"
    normalize: bool = False

    if PYDANTIC_VERSION.major < 2:  # Pydantic 1.x compat

        class Config:
            keep_untouched = (cached_property,)
    else:
        model_config = dict()
        model_config["ignored_types"] = (cached_property,)

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._ndims = 1024
        self._audio_extensions = (".mp3", ".wav", ".flac", ".ogg", ".aac")
        self._image_extensions = (".jpg", ".jpeg", ".png", ".gif", ".bmp")

    @cached_property
    def embedding_model(self):
        """
        Get the embedding model. This is cached so that the model is only loaded
        once per process.
        """
        return self.get_embedding_model()

    @cached_property
    def _data(self):
        """
        Get the data module from imagebind
        """
        data = attempt_import_or_raise("imagebind.data", "imagebind")
        return data

    @cached_property
    def _ModalityType(self):
        """
        Get the ModalityType from imagebind
        """
        imagebind = attempt_import_or_raise("imagebind", "imagebind")
        return imagebind.imagebind_model.ModalityType

    def ndims(self):
        return self._ndims

    def compute_query_embeddings(
        self, query: Union[str], *args, **kwargs
    ) -> List[np.ndarray]:
        """
        Compute the embeddings for a given user query

        Parameters
        ----------
        query : Union[str]
            The query to embed. A query can be either text, image paths or audio paths.
        """
        query = self.sanitize_input(query)
        if query[0].endswith(self._audio_extensions):
            return [self.generate_audio_embeddings(query)]
        elif query[0].endswith(self._image_extensions):
            return [self.generate_image_embeddings(query)]
        else:
            return [self.generate_text_embeddings(query)]

    def generate_image_embeddings(self, image: IMAGES) -> np.ndarray:
        torch = attempt_import_or_raise("torch")
        inputs = {
            self._ModalityType.VISION: self._data.load_and_transform_vision_data(
                image, self.device
            )
        }
        with torch.no_grad():
            image_features = self.embedding_model(inputs)[self._ModalityType.VISION]
            if self.normalize:
                image_features /= image_features.norm(dim=-1, keepdim=True)
            return image_features.cpu().numpy().squeeze()

    def generate_audio_embeddings(self, audio: AUDIO) -> np.ndarray:
        torch = attempt_import_or_raise("torch")
        inputs = {
            self._ModalityType.AUDIO: self._data.load_and_transform_audio_data(
                audio, self.device
            )
        }
        with torch.no_grad():
            audio_features = self.embedding_model(inputs)[self._ModalityType.AUDIO]
            if self.normalize:
                audio_features /= audio_features.norm(dim=-1, keepdim=True)
            return audio_features.cpu().numpy().squeeze()

    def generate_text_embeddings(self, text: TEXT) -> np.ndarray:
        torch = attempt_import_or_raise("torch")
        inputs = {
            self._ModalityType.TEXT: self._data.load_and_transform_text(
                text, self.device
            )
        }
        with torch.no_grad():
            text_features = self.embedding_model(inputs)[self._ModalityType.TEXT]
            if self.normalize:
                text_features /= text_features.norm(dim=-1, keepdim=True)
            return text_features.cpu().numpy().squeeze()

    def compute_source_embeddings(
        self, source: Union[IMAGES, AUDIO], *args, **kwargs
    ) -> List[np.array]:
        """
        Get the embeddings for the given sourcefield column in the pydantic model.
        """
        source = self.sanitize_input(source)
        embeddings = []
        if source[0].endswith(self._audio_extensions):
            embeddings.extend(self.generate_audio_embeddings(source))
            return embeddings
        elif source[0].endswith(self._image_extensions):
            embeddings.extend(self.generate_image_embeddings(source))
            return embeddings
        else:
            embeddings.extend(self.generate_text_embeddings(source))
            return embeddings

    def sanitize_input(
        self, input: Union[IMAGES, AUDIO]
    ) -> Union[List[bytes], np.ndarray]:
        """
        Sanitize the input to the embedding function.
        """
        if isinstance(input, (str, bytes)):
            input = [input]
        elif isinstance(input, pa.Array):
            input = input.to_pylist()
        elif isinstance(input, pa.ChunkedArray):
            input = input.combine_chunks().to_pylist()
        return input

    def get_embedding_model(self):
        """
        fetches the imagebind embedding model
        """
        imagebind = attempt_import_or_raise("imagebind", "imagebind")
        model = imagebind.imagebind_model.imagebind_huge(pretrained=True)
        model.eval()
        model.to(self.device)
        return model
