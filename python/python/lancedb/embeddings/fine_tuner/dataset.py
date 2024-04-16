import re
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

import lance
import pyarrow as pa
from pydantic import BaseModel
from tqdm import tqdm

from .llm import BaseLLM

DEFAULT_PROMPT_TMPL = """\
Context information is below.

---------------------
{context_str}
---------------------

Given the context information and no prior knowledge.
generate only questions based on the below query.

You are a Teacher/ Professor. Your task is to setup \
{num_questions_per_chunk} questions for an upcoming \
quiz/examination. The questions should be diverse in nature \
across the document. Restrict the questions to the \
context information provided."
"""


class QADataset(BaseModel):
    """Embedding QA Finetuning Dataset.

    Args:
        queries (Dict[str, str]): Dict id -> query.
        corpus (Dict[str, str]): Dict id -> string.
        relevant_docs (Dict[str, List[str]]): Dict query id -> list of doc ids.

    """

    queries: Dict[str, str]  # id -> query
    corpus: Dict[str, str]  # id -> text
    relevant_docs: Dict[str, List[str]]  # query id -> list of retrieved doc ids
    mode: str = "text"

    @property
    def query_docid_pairs(self) -> List[Tuple[str, List[str]]]:
        """Get query, relevant doc ids."""
        return [
            (query, self.relevant_docs[query_id])
            for query_id, query in self.queries.items()
        ]

    def save(self, path: str, mode: str = "overwrite") -> None:
        """
        Save the current dataset to a directory as .lance files.

        Parameters
        ----------
        path : str
            The path to save the dataset.
        mode : str, optional
            The mode to save the dataset, by default "overwrite". Accepts
            lance modes.
        """
        save_dir = Path(path)
        save_dir.mkdir(parents=True, exist_ok=True)

        # convert to pydict {"id": []}
        queries = {
            "id": list(self.queries.keys()),
            "query": list(self.queries.values()),
        }
        corpus = {
            "id": list(self.corpus.keys()),
            "text": [
                val or " " for val in self.corpus.values()
            ],  # lance saves empty strings as null
        }
        relevant_docs = {
            "query_id": list(self.relevant_docs.keys()),
            "doc_id": list(self.relevant_docs.values()),
        }

        # write to lance
        lance.write_dataset(
            pa.Table.from_pydict(queries), save_dir / "queries.lance", mode=mode
        )
        lance.write_dataset(
            pa.Table.from_pydict(corpus), save_dir / "corpus.lance", mode=mode
        )
        lance.write_dataset(
            pa.Table.from_pydict(relevant_docs),
            save_dir / "relevant_docs.lance",
            mode=mode,
        )

    @classmethod
    def load(cls, path: str) -> "QADataset":
        """
        Load QADataset from a directory.

        Parameters
        ----------
        path : str
            The path to load the dataset from.
        
        Returns
        -------
        QADataset
            The loaded QADataset.
        
        """
        load_dir = Path(path)
        queries = lance.dataset(load_dir / "queries.lance").to_table().to_pydict()
        corpus = lance.dataset(load_dir / "corpus.lance").to_table().to_pydict()
        relevant_docs = (
            lance.dataset(load_dir / "relevant_docs.lance").to_table().to_pydict()
        )
        return cls(
            queries=dict(zip(queries["id"], queries["query"])),
            corpus=dict(zip(corpus["id"], corpus["text"])),
            relevant_docs=dict(zip(relevant_docs["query_id"], relevant_docs["doc_id"])),
        )

    # generate queries as a convenience function
    @classmethod
    def from_llm(
        cls,
        nodes: "List[TextChunk]",
        llm: BaseLLM,
        qa_generate_prompt_tmpl: str = DEFAULT_PROMPT_TMPL,
        num_questions_per_chunk: int = 2,
    ) -> "QADataset":
        """
        Generate a QADataset from a list of TextChunks. 
        
        Parameters
        ----------
        nodes : List[TextChunk]
            The list of text chunks.
        llm : BaseLLM
            The language model to generate questions.
        qa_generate_prompt_tmpl : str, optional
            The template for generating questions, by default DEFAULT_PROMPT_TMPL.
        num_questions_per_chunk : int, optional
            The number of questions to generate per chunk, by default 2.

        Returns
        -------
        QADataset
            The generated QADataset.
        """
        node_dict = {node.id: node.text for node in nodes}

        queries = {}
        relevant_docs = {}
        for node_id, text in tqdm(node_dict.items()):
            query = qa_generate_prompt_tmpl.format(
                context_str=text, num_questions_per_chunk=num_questions_per_chunk
            )
            response = llm.chat_completion(query)

            result = str(response).strip().split("\n")
            questions = [
                re.sub(r"^\d+[\).\s]", "", question).strip() for question in result
            ]
            questions = [question for question in questions if len(question) > 0]
            for question in questions:
                question_id = str(uuid.uuid4())
                queries[question_id] = question
                relevant_docs[question_id] = [node_id]

        return cls(queries=queries, corpus=node_dict, relevant_docs=relevant_docs)

    @classmethod
    def from_responses(
        cls,
        nodes: List["TextChunk"],
        queries: Dict[str, str],
        relevant_docs: Dict[str, List[str]],
    ) -> "QADataset":
        """
        Create a QADataset from a list of TextChunks and a list of questions, queries, and relevant docs.

        Parameters
        ----------
        nodes : List[TextChunk]
            The list of text chunks.
        queries : Dict[str, str]
            The queries. query id -> query.
        relevant_docs : Dict[str, List[str]]
            The relevant docs. Dict query id -> list of doc ids.
        
        Returns
        -------
        QADataset
            The QADataset.
        """
        node_dict = {node.id: node.text for node in nodes}
        return cls(queries=queries, corpus=node_dict, relevant_docs=relevant_docs)


class TextChunk(BaseModel):
    """
    Simple text chunk for storing text nodes. Acts as a wrapper around text.
    Allow interoperability between different text processing libraries.

    Args:
        text (str): The text of the chunk.
        id (str): The id of the chunk.
        metadata (Dict[str, Any], optional): The metadata of the chunk. Defaults to {}.
    """

    text: str
    id: str
    metadata: Dict[str, Any] = {}

    @classmethod
    def from_chunk(cls, chunk: str, metadata: dict = {}) -> "TextChunk":
        """
        Create a SimpleTextChunk from a chunk.

        Parameters
        ----------
        chunk : str
            The text chunk.
        metadata : dict, optional
            The metadata, by default {}.

        Returns
        -------
        TextChunk
            The text chunk.
        
        """
        # generate a unique id
        return cls(text=chunk, id=str(uuid.uuid4()), metadata=metadata)

    @classmethod
    def from_llama_index_node(cls, node):
        """
        Generate a TextChunk from a llama index node.

        Parameters
        ----------
        node : llama_index.core.TextNode
            The llama index node.
        
        """
        return cls(text=node.text, id=node.node_id, metadata=node.metadata)

    @classmethod
    def from_langchain_node(cls, node):
        """
        Generate a TextChunk from a langchain node.

        Parameters
        ----------
        node : langchain.core.TextNode
            The langchain node.
        
        """
        raise NotImplementedError("Not implemented yet.")

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a dictionary.
        
        Returns
        -------
        Dict[str, Any]
            The dictionary.
        """
        return self.dict()

    def __str__(self) -> str:
        return self.text

    def __repr__(self) -> str:
        return f"SimpleTextChunk(text={self.text}, id={self.id}, \
        metadata={self.metadata})"
