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
from typing import Any, List, Optional, Union

import numpy as np

from lancedb.embeddings.fine_tuner import QADataset
from lancedb.utils.general import LOGGER

from ..util import attempt_import_or_raise
from .base import TextEmbeddingFunction
from .fine_tuner.basetuner import BaseEmbeddingTuner
from .registry import register
from .utils import weak_lru


@register("sentence-transformers")
class SentenceTransformerEmbeddings(TextEmbeddingFunction):
    """
    An embedding function that uses the sentence-transformers library

    https://huggingface.co/sentence-transformers
    """

    name: str = "all-MiniLM-L6-v2"
    device: str = "cpu"
    normalize: bool = True

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self._ndims = None

    @property
    def embedding_model(self):
        """
        Get the sentence-transformers embedding model specified by the
        name and device. This is cached so that the model is only loaded
        once per process.
        """
        return self.get_embedding_model()

    def ndims(self):
        if self._ndims is None:
            self._ndims = len(self.generate_embeddings("foo")[0])
        return self._ndims

    def generate_embeddings(
        self, texts: Union[List[str], np.ndarray]
    ) -> List[np.array]:
        """
        Get the embeddings for the given texts

        Parameters
        ----------
        texts: list[str] or np.ndarray (of str)
            The texts to embed
        """
        return self.embedding_model.encode(
            list(texts),
            convert_to_numpy=True,
            normalize_embeddings=self.normalize,
        ).tolist()

    @weak_lru(maxsize=1)
    def get_embedding_model(self):
        """
        Get the sentence-transformers embedding model specified by the
        name and device. This is cached so that the model is only loaded
        once per process.

        TODO: use lru_cache instead with a reasonable/configurable maxsize
        """
        sentence_transformers = attempt_import_or_raise(
            "sentence_transformers", "sentence-transformers"
        )
        return sentence_transformers.SentenceTransformer(self.name, device=self.device)

    def finetune(self, trainset: QADataset, *args, **kwargs):
        """
        Finetune the Sentence Transformers model

        Parameters
        ----------
        dataset: QADataset
            The dataset to use for finetuning
        """
        tuner = SentenceTransformersTuner(
            model=self.embedding_model,
            trainset=trainset,
            **kwargs,
        )
        tuner.finetune()


class SentenceTransformersTuner(BaseEmbeddingTuner):
    """Sentence Transformers Embedding Finetuning Engine."""

    def __init__(
        self,
        model: Any,
        trainset: QADataset,
        valset: Optional[QADataset] = None,
        path: Optional[str] = "~/.lancedb/embeddings/models",
        batch_size: int = 8,
        epochs: int = 1,
        show_progress: bool = True,
        eval_steps: int = 50,
        max_input_per_doc: int = -1,
        loss: Optional[Any] = None,
        evaluator: Optional[Any] = None,
        run_name: Optional[str] = None,
        log_wandb: bool = False,
    ) -> None:
        """
        Parameters
        ----------
        model: str
            The model to use for finetuning.
        trainset: QADataset
            The training dataset.
        valset: Optional[QADataset]
            The validation dataset.
        path: Optional[str]
            The path to save the model.
        batch_size: int, default=8
            The batch size.
        epochs: int, default=1
            The number of epochs.
        show_progress: bool, default=True
            Whether to show progress.
        eval_steps: int, default=50
            The number of steps to evaluate.
        max_input_per_doc: int, default=-1
            The number of input per document.
            if -1, use all documents.
        """
        from sentence_transformers import InputExample, losses
        from sentence_transformers.evaluation import InformationRetrievalEvaluator
        from torch.utils.data import DataLoader

        self.model = model
        self.trainset = trainset
        self.valset = valset
        self.path = path
        self.batch_size = batch_size
        self.epochs = epochs
        self.show_progress = show_progress
        self.eval_steps = eval_steps
        self.max_input_per_doc = max_input_per_doc
        self.evaluator = None
        self.epochs = epochs
        self.show_progress = show_progress
        self.eval_steps = eval_steps
        self.run_name = run_name
        self.log_wandb = log_wandb

        if self.max_input_per_doc < -1:
            raise ValueError("max_input_per_doc must be -1 or greater than 0.")

        examples: Any = []
        for query_id, query in self.trainset.queries.items():
            if max_input_per_doc == -1:
                for node_id in self.trainset.relevant_docs[query_id]:
                    text = self.trainset.corpus[node_id]
                    example = InputExample(texts=[query, text])
                    examples.append(example)
            else:
                node_id = self.trainset.relevant_docs[query_id][
                    min(max_input_per_doc, len(self.trainset.relevant_docs[query_id]))
                ]
                text = self.trainset.corpus[node_id]
                example = InputExample(texts=[query, text])
                examples.append(example)

        self.examples = examples

        self.loader: DataLoader = DataLoader(examples, batch_size=batch_size)

        if self.valset is not None:
            eval_engine = evaluator or InformationRetrievalEvaluator
            self.evaluator = eval_engine(
                valset.queries, valset.corpus, valset.relevant_docs
            )
        self.evaluator = evaluator

        # define loss
        self.loss = loss or losses.MultipleNegativesRankingLoss(self.model)
        self.warmup_steps = int(len(self.loader) * epochs * 0.1)

    def finetune(self) -> None:
        """Finetune the Sentence Transformers model."""
        self.model.fit(
            train_objectives=[(self.loader, self.loss)],
            epochs=self.epochs,
            warmup_steps=self.warmup_steps,
            output_path=self.path,
            show_progress_bar=self.show_progress,
            evaluator=self.evaluator,
            evaluation_steps=self.eval_steps,
            callback=self._wandb_callback if self.log_wandb else None,
        )

        self.helper()

    def helper(self) -> None:
        """A helper method."""
        LOGGER.info("Finetuning complete.")
        LOGGER.info(f"Model saved to {self.path}.")
        LOGGER.info("You can now use the model as follows:")
        LOGGER.info(
            f"model = get_registry().get('sentence-transformers').create(name='./{self.path}')"
        )

    def _wandb_callback(self, score, epoch, steps):
        try:
            import wandb
            from wandb import (
                __version__,
            )
        except ImportError:
            raise ImportError(
                "wandb is not installed. Please install it using `pip install wandb`"
            )
        run = wandb.run or wandb.init(
            project="sbert_lancedb_finetune", name=self.run_name
        )
        run.log({"epoch": epoch, "steps": steps, "score": score})
