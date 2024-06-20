from abc import ABC, abstractmethod


class BaseEmbeddingTuner(ABC):
    """Base Embedding finetuning engine."""

    @abstractmethod
    def finetune(self) -> None:
        """
        Finetune the embedding model.
        """
        pass

    def helper(self) -> None:
        """
        A helper method called after finetuning. This is meant to provide
        usage instructions or other helpful information.
        """
        pass
