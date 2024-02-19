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

from typing import TYPE_CHECKING, List, Union

import numpy as np
import pyarrow as pa
from pydantic import PrivateAttr
from tqdm import tqdm

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import AUDIO, IMAGES, TEXT

if TYPE_CHECKING:
    import PIL
    import torch


@register("imagebind")
class ImageBindEmbeddings(EmbeddingFunction):
    """
    An embedding function that uses the ImageBind API
    For generating multi-modal embeddings across
    six different modalities: images, text, audio, depth, thermal, and IMU data

    download module in current working directry: https://github.com/facebookresearch/ImageBind/
    """

    name: str = "imagebind_huge"
    device: str = "cpu"
    normalize: bool = False
    _model = PrivateAttr()
    _data = PrivateAttr()
    _ModalityType = PrivateAttr

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        imagebind_model = attempt_import_or_raise(
            "imagebind.models.imagebind_model",
            "https://github.com/facebookresearch/ImageBind/",
            pkg=".ImageBind",
        )
        self._data = attempt_import_or_raise(
            "imagebind.data",
            "https://github.com/facebookresearch/ImageBind/",
            pkg=".ImageBind",
        )
        self._ModalityType = imagebind_model.ModalityType
        model = imagebind_model.imagebind_huge(pretrained=True)
        model.eval()
        model.to(self.device)
        self._model = model
        self._ndims = None

    def ndims(self):
        if self._ndims is None:
            self._ndims = self.generate_text_embeddings(["foo"]).shape[0]
        return self._ndims

    def compute_query_embeddings(
        self, query: Union[str], *args, **kwargs
    ) -> List[np.ndarray]:
        """
        Compute the embeddings for a given user query

        Parameters
        ----------
        query : Union[str, PIL.Image.Image]
            The query to embed. A query can be either text, image or audio.
        """
        query = self.sanitize_input(query)
        if query[0].endswith((".mp3", ".wav", ".flac", ".ogg", ".aac")):
            return [self.generate_audio_embeddings([query])]
        elif query[0].endswith((".jpg", ".jpeg", ".png", ".gif", ".bmp")):
            return [self.generate_image_embeddings([query])]
        else :
            return [self.generate_text_embeddings([query])]

    def generate_image_embeddings(self, image: IMAGES) -> np.ndarray:
        torch = attempt_import_or_raise("torch")
        inputs = {
            self._ModalityType.VISION: self._data.load_and_transform_vision_data(
                image, self.device
            )
        }
        with torch.no_grad():
            image_features = self._model(inputs)[self._ModalityType.VISION]
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
            audio_features = self._model(inputs)[self._ModalityType.AUDIO]
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
            text_features = self._model(inputs)[self._ModalityType.TEXT]
            if self.normalize:
                text_features /= text_features.norm(dim=-1, keepdim=True)
            return text_features.cpu().numpy().squeeze()

    # def sanitize_input(self, images: IMAGES) -> Union[List[bytes], np.ndarray]:
    #     """
    #     Sanitize the input to the embedding function.
    #     """
    #     if isinstance(images, (str, bytes)):
    #         images = [images]
    #     elif isinstance(images, pa.Array):
    #         images = images.to_pylist()
    #     elif isinstance(images, pa.ChunkedArray):
    #         images = images.combine_chunks().to_pylist()
    #     return images

    def compute_source_embeddings(
        self, source: Union[IMAGES, AUDIO], *args, **kwargs
    ) -> List[np.array]:
        """
        Get the embeddings for the given images
        """
        source = self.sanitize_input(source)
        embeddings = []
        if source[0].endswith((".mp3", ".wav", ".flac", ".ogg", ".aac")):
            for i in self.generate_audio_embeddings(source):
                embeddings.append(i)
            return embeddings
        elif source[0].endswith((".jpg", ".jpeg", ".png", ".gif", ".bmp")):
            for i in self.generate_image_embeddings(source):
                embeddings.append(i)
            return embeddings
        else :
            for i in self.generate_text_embeddings(source):
                embeddings.append(i)
            return embeddings

    def sanitize_input(self, input: Union[IMAGES, AUDIO]) -> Union[List[bytes], np.ndarray]:
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

    # def _parallel_get(self, images: Union[List[str], List[bytes]]) -> List[np.ndarray]:
    #     """
    #     Issue concurrent requests to retrieve the image data
    #     """
    #     with concurrent.futures.ThreadPoolExecutor() as executor:
    #         futures = [
    #             executor.submit(self.generate_image_embedding, image)
    #             for image in images
    #         ]
    #         return [future.result() for future in tqdm(futures)]

    # def _to_pil(self, image: Union[str, bytes]):
    #     PIL = attempt_import_or_raise("PIL", "pillow")
    #     if isinstance(image, bytes):
    #         return PIL.Image.open(io.BytesIO(image))
    #     if isinstance(image, PIL.Image.Image):
    #         return image
    #     elif isinstance(image, str):
    #         parsed = urlparse.urlparse(image)
    #         # TODO handle drive letter on windows.
    #         if parsed.scheme == "file":
    #             return PIL.Image.open(parsed.path)
    #         elif parsed.scheme == "":
    #             return PIL.Image.open(image if os.name == "nt" else parsed.path)
    #         elif parsed.scheme.startswith("http"):
    #             return PIL.Image.open(io.BytesIO(url_retrieve(image)))
    # else:
    #     raise NotImplementedError("Only local and http(s) urls are supported")

    # def _encode_and_normalize_image(self, image_tensor: "torch.Tensor"):
    #     """
    #     encode a single image tensor and optionally normalize the output
    #     """
    #     image_features = self._model.encode_image(image_tensor.to(self.device))
    #     if self.normalize:
    #         image_features /= image_features.norm(dim=-1, keepdim=True)
    #     return image_features.cpu().numpy().squeeze()
