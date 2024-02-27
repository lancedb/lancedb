from pydantic import BaseModel
from abc import ABC, abstractmethod

class BaseEmbeddingTuner(ABC):
    """Base Embedding finetuning engine."""

    @abstractmethod
    def finetune(self) -> None:
        """Goes off and does stuff."""

    def helper(self) -> None:
        """A helper method."""
        pass