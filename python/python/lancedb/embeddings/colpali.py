# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors


from functools import lru_cache
from logging import warning
from typing import List, Union, Optional, Any, Callable
import numpy as np
import io
import warnings

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import TEXT, IMAGES, is_flash_attn_2_available


@register("colpali")
class ColPaliEmbeddings(EmbeddingFunction):
    """
    An embedding function that uses the ColPali engine for
    multimodal multi-vector embeddings.

    This embedding function supports ColPali models, producing multivector outputs
    for both text and image inputs.

    Parameters
    ----------
    model_name : str
        The name of the model to use (e.g., "Metric-AI/ColQwen2.5-3b-multilingual-v1.0")
        Supports models based on these engines:
        - ColPali: "vidore/colpali-v1.3" and others
        - ColQwen2.5: "Metric-AI/ColQwen2.5-3b-multilingual-v1.0" and others
        - ColQwen2: "vidore/colqwen2-v1.0" and others
        - ColSmol: "vidore/colSmol-256M" and others

    device : str
        The device for inference (default "auto").
    dtype : str
        Data type for model weights (default "bfloat16").
    use_token_pooling : bool
        DEPRECATED. Whether to use token pooling. Use `pooling_strategy` instead.
    pooling_strategy : str, optional
        The token pooling strategy to use, by default "hierarchical".
        - "hierarchical": Progressively pools tokens to reduce sequence length.
        - "lambda": A simpler pooling that uses a custom `pooling_func`.
    pooling_func: typing.Callable, optional
        A function to use for pooling when `pooling_strategy` is "lambda".
    pool_factor : int
        Factor to reduce sequence length if token pooling is enabled (default 2).
    quantization_config : Optional[BitsAndBytesConfig]
        Quantization configuration for the model. (default None, bitsandbytes needed)
    batch_size : int
        Batch size for processing inputs (default 2).
    offload_folder: str, optional
        Folder to offload model weights if using CPU offloading (default None). This is
        useful for large models that do not fit in memory.
    """

    model_name: str = "Metric-AI/ColQwen2.5-3b-multilingual-v1.0"
    device: str = "auto"
    dtype: str = "bfloat16"
    use_token_pooling: bool = True
    pooling_strategy: Optional[str] = "hierarchical"
    pooling_func: Optional[Any] = None
    pool_factor: int = 2
    quantization_config: Optional[Any] = None
    batch_size: int = 2
    offload_folder: Optional[str] = None

    _model = None
    _processor = None
    _token_pooler = None
    _vector_dim = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        torch = attempt_import_or_raise("torch", "torch")

        if not self.use_token_pooling:
            warnings.warn(
                "use_token_pooling is deprecated, use pooling_strategy=None instead",
                DeprecationWarning,
            )
            self.pooling_strategy = None

        if self.pooling_strategy == "lambda" and self.pooling_func is None:
            raise ValueError(
                "pooling_func must be provided when pooling_strategy is 'lambda'"
            )

        device = self.device
        if device == "auto":
            if torch.cuda.is_available():
                device = "cuda"
            elif torch.backends.mps.is_available():
                device = "mps"
            else:
                device = "cpu"

        dtype = self.dtype
        if device == "mps" and dtype == "bfloat16":
            dtype = "float32"  # Avoid NaNs on MPS

        (
            self._model,
            self._processor,
            self._token_pooler,
        ) = self._load_model(
            self.model_name,
            dtype,
            device,
            self.pooling_strategy,
            self.pooling_func,
            self.quantization_config,
        )

    @staticmethod
    @lru_cache(maxsize=1)
    def _load_model(
        model_name: str,
        dtype: str,
        device: str,
        pooling_strategy: Optional[str],
        pooling_func: Optional[Callable],
        quantization_config: Optional[Any],
    ):
        """
        Initialize and cache the ColPali model, processor, and token pooler.
        """
        if device.startswith("mps"):
            # warn some torch ops in late interaction architecture result in nans on mps
            warning(
                "MPS device detected. Some operations may result in NaNs. "
                "If you encounter issues, consider using 'cpu' or 'cuda' devices."
            )
        torch = attempt_import_or_raise("torch", "torch")
        transformers = attempt_import_or_raise("transformers", "transformers")
        colpali_engine = attempt_import_or_raise("colpali_engine", "colpali_engine")
        from colpali_engine.compression.token_pooling import (
            HierarchicalTokenPooler,
            LambdaTokenPooler,
        )

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

        model_class, processor_class = None, None
        model_name_lower = model_name.lower()
        if "colqwen2.5" in model_name_lower:
            model_class = colpali_engine.models.ColQwen2_5
            processor_class = colpali_engine.models.ColQwen2_5_Processor
        elif "colsmol" in model_name_lower or "colidefics3" in model_name_lower:
            model_class = colpali_engine.models.ColIdefics3
            processor_class = colpali_engine.models.ColIdefics3Processor
        elif "colqwen" in model_name_lower:
            model_class = colpali_engine.models.ColQwen2
            processor_class = colpali_engine.models.ColQwen2Processor
        elif "colpali" in model_name_lower:
            model_class = colpali_engine.models.ColPali
            processor_class = colpali_engine.models.ColPaliProcessor

        if model_class is None:
            raise ValueError(f"Unsupported model: {model_name}")

        model = model_class.from_pretrained(
            model_name,
            torch_dtype=torch_dtype,
            quantization_config=quantization_config
            if quantization_config is not None
            else None,
            attn_implementation="flash_attention_2"
            if is_flash_attn_2_available()
            else None,
            low_cpu_mem_usage=True,
        ).eval()
        model = model.to(device)
        model = model.to(torch_dtype)  # Force cast after moving to device
        processor = processor_class.from_pretrained(model_name)

        token_pooler = None
        if pooling_strategy == "hierarchical":
            token_pooler = HierarchicalTokenPooler()
        elif pooling_strategy == "lambda":
            token_pooler = LambdaTokenPooler(pool_func=pooling_func)

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

            if self.pooling_strategy and self._token_pooler is not None:
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
        if self.pooling_strategy and self._token_pooler is not None:
            if self.pooling_strategy == "hierarchical":
                embeddings = self._token_pooler.pool_embeddings(
                    embeddings,
                    pool_factor=self.pool_factor,
                    padding=True,
                    padding_side=self._processor.tokenizer.padding_side,
                )
            elif self.pooling_strategy == "lambda":
                embeddings = self._token_pooler.pool_embeddings(
                    embeddings,
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
            query_embeddings = torch.nan_to_num(query_embeddings)
            all_embeddings.extend(self._process_embeddings(query_embeddings))
        return all_embeddings

    def _prepare_images(self, images: IMAGES) -> List:
        """
        Convert image inputs to PIL Images.
        """
        PIL_Image = attempt_import_or_raise("PIL.Image", "pillow")
        requests = attempt_import_or_raise("requests", "requests")
        images = self.sanitize_input(images)
        pil_images = []
        try:
            for image in images:
                if isinstance(image, str):
                    if image.startswith(("http://", "https://")):
                        response = requests.get(image, timeout=10)
                        response.raise_for_status()
                        pil_images.append(PIL_Image.open(io.BytesIO(response.content)))
                    else:
                        with PIL_Image.open(image) as im:
                            pil_images.append(im.copy())
                elif isinstance(image, bytes):
                    pil_images.append(PIL_Image.open(io.BytesIO(image)))
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
            image_embeddings = torch.nan_to_num(image_embeddings)
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
