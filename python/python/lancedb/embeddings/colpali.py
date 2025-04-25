# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from functools import lru_cache
from typing import List, Union, Optional, Any
import numpy as np
import io

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import TEXT, IMAGES, is_flash_attn_2_available


@register("colpali")
class ColPaliEmbeddings(EmbeddingFunction):
    """
    An embedding function that uses the ColPali engine for
    multimodal multi-vector embeddings.

    This embedding function supports ColQwen2.5 models, producing multivector outputs
    for both text and image inputs. The output embeddings are lists of vectors, each
    vector being 128-dimensional by default, represented as List[List[float]].

    Parameters
    ----------
    model_name : str
        The name of the model to use (e.g., "Metric-AI/ColQwen2.5-3b-multilingual-v1.0")
    device : str
        The device for inference (default "cuda:0").
    dtype : str
        Data type for model weights (default "bfloat16").
    use_token_pooling : bool
        Whether to use token pooling to reduce embedding size (default True).
    pool_factor : int
        Factor to reduce sequence length if token pooling is enabled (default 2).
    quantization_config : Optional[BitsAndBytesConfig]
        Quantization configuration for the model. (default None, bitsandbytes needed)
    batch_size : int
        Batch size for processing inputs (default 2).
    """

    model_name: str = "Metric-AI/ColQwen2.5-3b-multilingual-v1.0"
    device: str = "auto"
    dtype: str = "bfloat16"
    use_token_pooling: bool = True
    pool_factor: int = 2
    quantization_config: Optional[Any] = None
    batch_size: int = 2

    _model = None
    _processor = None
    _token_pooler = None
    _vector_dim = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        (
            self._model,
            self._processor,
            self._token_pooler,
        ) = self._load_model(
            self.model_name,
            self.dtype,
            self.device,
            self.use_token_pooling,
            self.quantization_config,
        )

    @staticmethod
    @lru_cache(maxsize=1)
    def _load_model(
        model_name: str,
        dtype: str,
        device: str,
        use_token_pooling: bool,
        quantization_config: Optional[Any],
    ):
        """
        Initialize and cache the ColPali model, processor, and token pooler.
        """
        torch = attempt_import_or_raise("torch", "torch")
        transformers = attempt_import_or_raise("transformers", "transformers")
        colpali_engine = attempt_import_or_raise("colpali_engine", "colpali_engine")
        from colpali_engine.compression.token_pooling import HierarchicalTokenPooler

        if quantization_config is not None:
            if not isinstance(quantization_config, transformers.BitsAndBytesConfig):
                raise ValueError("quantization_config must be a BitsAndBytesConfig")

        if dtype == "bfloat16":
            torch_dtype = torch.bfloat16
        elif dtype == "float16":
            torch_dtype = torch.float16
        elif dtype == "float64":
            torch_dtype = torch.float64
        else:
            torch_dtype = torch.float32

        model = colpali_engine.models.ColQwen2_5.from_pretrained(
            model_name,
            torch_dtype=torch_dtype,
            device_map=device,
            quantization_config=quantization_config
            if quantization_config is not None
            else None,
            attn_implementation="flash_attention_2"
            if is_flash_attn_2_available()
            else None,
        ).eval()
        processor = colpali_engine.models.ColQwen2_5_Processor.from_pretrained(
            model_name
        )
        token_pooler = HierarchicalTokenPooler() if use_token_pooling else None
        return model, processor, token_pooler

    def ndims(self):
        """
        Return the dimension of a vector in the multivector output (e.g., 128).
        """
        torch = attempt_import_or_raise("torch", "torch")
        if self._vector_dim is None:
            dummy_query = "test"
            batch_queries = self._processor.process_queries([dummy_query]).to(
                self._model.device
            )
            with torch.no_grad():
                query_embeddings = self._model(**batch_queries)

            if self.use_token_pooling and self._token_pooler is not None:
                query_embeddings = self._token_pooler.pool_embeddings(
                    query_embeddings,
                    pool_factor=self.pool_factor,
                    padding=True,
                    padding_side=self._processor.tokenizer.padding_side,
                )

            self._vector_dim = query_embeddings[0].shape[-1]
        return self._vector_dim

    def _process_embeddings(self, embeddings):
        """
        Format model embeddings into List[List[float]].
        Use token pooling if enabled.
        """
        torch = attempt_import_or_raise("torch", "torch")
        if self.use_token_pooling and self._token_pooler is not None:
            embeddings = self._token_pooler.pool_embeddings(
                embeddings,
                pool_factor=self.pool_factor,
                padding=True,
                padding_side=self._processor.tokenizer.padding_side,
            )

        if isinstance(embeddings, torch.Tensor):
            tensors = embeddings.detach().cpu()
            if tensors.dtype == torch.bfloat16:
                tensors = tensors.to(torch.float32)
            return (
                tensors.numpy()
                .astype(np.float64 if self.dtype == "float64" else np.float32)
                .tolist()
            )
        return []

    def generate_text_embeddings(self, text: TEXT) -> List[List[List[float]]]:
        """
        Generate embeddings for text input.
        """
        torch = attempt_import_or_raise("torch", "torch")
        text = self.sanitize_input(text)
        all_embeddings = []

        for i in range(0, len(text), self.batch_size):
            batch_text = text[i : i + self.batch_size]
            batch_queries = self._processor.process_queries(batch_text).to(
                self._model.device
            )
            with torch.no_grad():
                query_embeddings = self._model(**batch_queries)
            all_embeddings.extend(self._process_embeddings(query_embeddings))
        return all_embeddings

    def _prepare_images(self, images: IMAGES) -> List:
        """
        Convert image inputs to PIL Images.
        """
        PIL = attempt_import_or_raise("PIL", "pillow")
        requests = attempt_import_or_raise("requests", "requests")
        images = self.sanitize_input(images)
        pil_images = []
        try:
            for image in images:
                if isinstance(image, str):
                    if image.startswith(("http://", "https://")):
                        response = requests.get(image, timeout=10)
                        response.raise_for_status()
                        pil_images.append(PIL.Image.open(io.BytesIO(response.content)))
                    else:
                        with PIL.Image.open(image) as im:
                            pil_images.append(im.copy())
                elif isinstance(image, bytes):
                    pil_images.append(PIL.Image.open(io.BytesIO(image)))
                else:
                    # Assume it's a PIL Image; will raise if invalid
                    pil_images.append(image)
        except Exception as e:
            raise ValueError(f"Failed to process image: {e}")

        return pil_images

    def generate_image_embeddings(self, images: IMAGES) -> List[List[List[float]]]:
        """
        Generate embeddings for a batch of images.
        """
        torch = attempt_import_or_raise("torch", "torch")
        pil_images = self._prepare_images(images)
        all_embeddings = []

        for i in range(0, len(pil_images), self.batch_size):
            batch_images = pil_images[i : i + self.batch_size]
            batch_images = self._processor.process_images(batch_images).to(
                self._model.device
            )
            with torch.no_grad():
                image_embeddings = self._model(**batch_images)
            all_embeddings.extend(self._process_embeddings(image_embeddings))
        return all_embeddings

    def compute_query_embeddings(
        self, query: Union[str, IMAGES], *args, **kwargs
    ) -> List[List[List[float]]]:
        """
        Compute embeddings for a single user query (text only).
        """
        if not isinstance(query, str):
            raise ValueError(
                "Query must be a string, image to image search is not supported"
            )
        return self.generate_text_embeddings([query])

    def compute_source_embeddings(
        self, images: IMAGES, *args, **kwargs
    ) -> List[List[List[float]]]:
        """
        Compute embeddings for a batch of source images.

        Parameters
        ----------
        images : Union[str, bytes, List, pa.Array, pa.ChunkedArray, np.ndarray]
            Batch of images (paths, URLs, bytes, or PIL Images).
        """
        images = self.sanitize_input(images)
        return self.generate_image_embeddings(images)
