# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Late-interaction embeddings powered by colpali-engine."""

from __future__ import annotations

import io
from typing import Any, Dict, List, Optional, Sequence

from ..util import attempt_import_or_raise
from .base import EmbeddingFunction
from .registry import register
from .utils import IMAGES, TEXT, is_flash_attn_2_available, weak_lru


_FAMILY_ALIASES = {
    "colsmol": {"colsmol", "colsmolvlm", "smol"},
    "colqwen2.5": {"colqwen2.5", "colqwen25", "colqwen-2.5"},
    "colqwen2": {"colqwen2", "colqwen-2"},
    "colpali": {"colpali", "paligemma"},
}

_FAMILY_CLASSES = {
    "colpali": ("ColPali", "ColPaliProcessor"),
    "colqwen2.5": ("ColQwen2_5", "ColQwen2_5_Processor"),
    "colqwen2": ("ColQwen2", "ColQwen2Processor"),
    "colsmol": ("ColIdefics3", "ColIdefics3Processor"),
}


def _torch() -> Any:
    return attempt_import_or_raise("torch", "torch")


def _torch_dtype(dtype: str) -> Any:
    torch = _torch()
    mapping = {
        "bfloat16": torch.bfloat16,
        "float16": torch.float16,
        "float32": torch.float32,
        "float64": torch.float64,
    }
    if dtype not in mapping:
        raise ValueError(
            "Unsupported dtype '{}'. Expected one of {}".format(
                dtype, ", ".join(sorted(mapping))
            )
        )
    return mapping[dtype]


def _load_pooler(use_pooler: bool) -> Optional[Any]:
    if not use_pooler:
        return None
    token_pooling = attempt_import_or_raise(
        "colpali_engine.compression.token_pooling", "colpali-engine"
    )
    pooler_cls = getattr(token_pooling, "HierarchicalTokenPooler", None)
    if pooler_cls is None:
        raise ImportError(
            "colpali_engine HierarchicalTokenPooler not available; update colpali-engine"
        )
    return pooler_cls()


def _move_to_device(batch: Any, device: Any) -> Any:
    if device is None:
        return batch
    torch = _torch()
    if isinstance(device, str):
        device_obj = torch.device(device)
    else:
        device_obj = device
    if isinstance(batch, dict):
        return {k: _move_to_device(v, device_obj) for k, v in batch.items()}
    if hasattr(batch, "to"):
        return batch.to(device_obj)
    return batch


@register("multimodal-late-interaction")
class MultimodalLateInteractionEmbeddings(EmbeddingFunction):
    """Late-interaction embeddings for ViDoRe models."""

    model_name: str = "vidore/colSmol-256M"
    model_family: Optional[str] = None
    device: str = "auto"
    dtype: str = "bfloat16"
    use_token_pooling: bool = True
    pool_factor: int = 2
    batch_size: int = 4
    quantization_config: Optional[Any] = None

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._family = self._resolve_family(self.model_name, self.model_family)
        self._vector_dim: Optional[int] = None

    @property
    def model(self) -> Any:
        """The cached model."""
        return self._get_models()[0]

    @property
    def processor(self) -> Any:
        """The cached processor."""
        return self._get_models()[1]

    @property
    def pooler(self) -> Optional[Any]:
        """The cached pooler."""
        return self._get_models()[2]

    @property
    def target_device(self) -> Optional[Any]:
        """The cached target device."""
        return self._get_models()[3]

    # ------------------------------------------------------------------
    # Family detection
    # ------------------------------------------------------------------
    @classmethod
    def _resolve_family(cls, model_name: str, explicit: Optional[str]) -> str:
        if explicit:
            family = explicit.lower()
            if family not in _FAMILY_CLASSES:
                raise ValueError(
                    "Unknown model_family '{}'. Expected one of {}".format(
                        explicit, ", ".join(sorted(_FAMILY_CLASSES))
                    )
                )
            return family

        lowered = model_name.lower()
        for family, aliases in _FAMILY_ALIASES.items():
            if any(alias in lowered for alias in aliases):
                return family
        return "colpali"

    # ------------------------------------------------------------------
    # Model loading
    # ------------------------------------------------------------------
    @weak_lru(maxsize=1)
    def _get_models(self) -> tuple[Any, Optional[Any], Optional[Any], Optional[Any]]:
        colpali_engine = attempt_import_or_raise("colpali_engine", "colpali-engine")
        transformers = attempt_import_or_raise("transformers", "transformers")

        if (
            self.quantization_config is not None
            and not isinstance(
                self.quantization_config, transformers.BitsAndBytesConfig
            )
        ):
            raise ValueError(
                "quantization_config must be a transformers.BitsAndBytesConfig instance"
            )

        model_cls_name, processor_cls_name = _FAMILY_CLASSES[self._family]
        model_cls = getattr(colpali_engine.models, model_cls_name)
        processor_cls = getattr(colpali_engine.models, processor_cls_name)

        torch = _torch()
        device_map = self.device
        target_device: Optional[Any] = None
        if device_map == "auto":
            if torch.cuda.is_available():
                device_map = "cuda:0"
                target_device = torch.device("cuda:0")
            elif (
                getattr(torch.backends, "mps", None)
                and torch.backends.mps.is_available()
            ):
                device_map = "mps"
                target_device = torch.device("mps")
            else:
                device_map = "cpu"
                target_device = torch.device("cpu")
        else:
            try:
                target_device = torch.device(device_map)
            except (TypeError, ValueError):  # pragma: no cover - device map dicts
                target_device = None

        torch_dtype = _torch_dtype(self.dtype)
        if isinstance(device_map, str) and device_map == "cpu" and torch_dtype in {
            torch.bfloat16,
            torch.float16,
        }:
            torch_dtype = torch.float32

        load_kwargs: Dict[str, Any] = {
            "torch_dtype": torch_dtype,
            "device_map": device_map,
        }
        if self.quantization_config is not None:
            load_kwargs["quantization_config"] = self.quantization_config
        attn_impl = "flash_attention_2" if is_flash_attn_2_available() else None
        if attn_impl is not None:
            load_kwargs["attn_implementation"] = attn_impl

        model = model_cls.from_pretrained(self.model_name, **load_kwargs)
        if hasattr(model, "eval"):
            model = model.eval()

        processor = processor_cls.from_pretrained(self.model_name)
        pooler = _load_pooler(self.use_token_pooling)
        if target_device is None and hasattr(model, "device"):
            target_device = getattr(model, "device")

        return model, processor, pooler, target_device

    # ------------------------------------------------------------------
    # Encoding helpers
    # ------------------------------------------------------------------
    def _pool_tensor(self, tensor: Any) -> Any:
        if self.pooler is None:
            return tensor
        torch = _torch()
        assert isinstance(tensor, torch.Tensor)
        expanded = False
        if tensor.ndim == 2:
            tensor = tensor.unsqueeze(0)
            expanded = True
        kwargs = {"pool_factor": self.pool_factor, "padding": True}
        tokenizer = getattr(
            getattr(self.processor, "tokenizer", None), "padding_side", None
        )
        if tokenizer is not None:
            kwargs["padding_side"] = tokenizer
        pooled = self.pooler.pool_embeddings(tensor, **kwargs)
        if expanded:
            pooled = pooled.squeeze(0)
        return pooled

    def _normalize_output(self, embeddings: Any) -> List[List[List[float]]]:
        torch = _torch()
        if hasattr(embeddings, "last_hidden_state"):
            return self._normalize_output(embeddings.last_hidden_state)
        if isinstance(embeddings, dict) and "last_hidden_state" in embeddings:
            return self._normalize_output(embeddings["last_hidden_state"])
        if isinstance(embeddings, torch.Tensor):
            pooled = self._pool_tensor(embeddings).detach().cpu()
            if pooled.ndim == 2:
                pooled = pooled.unsqueeze(0)
            target = torch.float64 if self.dtype == "float64" else torch.float32
            return pooled.to(target).numpy().tolist()
        if isinstance(embeddings, (list, tuple)):
            results: List[List[List[float]]] = []
            for item in embeddings:
                results.extend(self._normalize_output(item))
            return results
        raise TypeError(f"Unsupported embedding type {type(embeddings)}")

    # ------------------------------------------------------------------
    # Text encoding
    # ------------------------------------------------------------------
    def _encode_text(self, batch: Sequence[str]) -> List[List[List[float]]]:
        if not self.processor or not hasattr(self.processor, "process_queries"):
            raise RuntimeError("Processor for text queries is not available for this model")
        payload = self.processor.process_queries(batch)
        payload = _move_to_device(payload, self.target_device)
        torch = _torch()
        with torch.no_grad():
            outputs = self.model(**payload)
        return self._normalize_output(outputs)

    # ------------------------------------------------------------------
    # Image encoding
    # ------------------------------------------------------------------
    def _prepare_images(self, images: IMAGES) -> List[Any]:
        PIL = attempt_import_or_raise("PIL", "pillow")
        requests = attempt_import_or_raise("requests", "requests")
        prepared: List[Any] = []
        for image in self.sanitize_input(images):
            if isinstance(image, str) and image.startswith(("http://", "https://")):
                response = requests.get(image, timeout=10)
                response.raise_for_status()
                prepared.append(PIL.Image.open(io.BytesIO(response.content)))
            elif isinstance(image, str):
                with PIL.Image.open(image) as img:
                    prepared.append(img.copy())
            elif isinstance(image, bytes):
                prepared.append(PIL.Image.open(io.BytesIO(image)))
            else:
                prepared.append(image)
        return prepared

    def _encode_images(self, images: Sequence[Any]) -> List[List[List[float]]]:
        if not self.processor or not hasattr(self.processor, "process_images"):
            raise RuntimeError("Processor for images is not available for this model")
        payload = self.processor.process_images(images)
        payload = _move_to_device(payload, self.target_device)
        torch = _torch()
        with torch.no_grad():
            outputs = self.model(**payload)
        return self._normalize_output(outputs)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def _batched(self, values: Sequence[Any], encoder) -> List[List[List[float]]]:
        results: List[List[List[float]]] = []
        for start in range(0, len(values), self.batch_size):
            chunk = values[start : start + self.batch_size]
            results.extend(encoder(chunk))
        return results

    def generate_text_embeddings(self, text: TEXT) -> List[List[List[float]]]:
        text = self.sanitize_input(text)
        if len(text) == 0:
            return []
        return self._batched(text, self._encode_text)

    def generate_image_embeddings(self, images: IMAGES) -> List[List[List[float]]]:
        prepared = self._prepare_images(images)
        if len(prepared) == 0:
            return []
        return self._batched(prepared, self._encode_images)

    def compute_query_embeddings(
        self, query: str, *args: Any, **kwargs: Any
    ) -> List[List[List[float]]]:
        if not isinstance(query, str):
            raise ValueError("Late interaction queries must be text")
        return self.generate_text_embeddings([query])

    def compute_source_embeddings(
        self, images: IMAGES, *args: Any, **kwargs: Any
    ) -> List[List[List[float]]]:
        return self.generate_image_embeddings(images)

    def ndims(self) -> int:
        if self._vector_dim is None:
            probe = self.generate_text_embeddings(["probe"])
            if not probe or not probe[0]:
                raise RuntimeError("Failed to determine embedding dimension")
            self._vector_dim = len(probe[0][0])
        return self._vector_dim


# Backwards compatibility: keep the historical "colpali" key
register("colpali")(MultimodalLateInteractionEmbeddings)


# Legacy class name kept for backwards compatibility in imports
ColPaliEmbeddings = MultimodalLateInteractionEmbeddings
