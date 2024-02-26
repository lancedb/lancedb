import json
import uuid
import re
from tqdm import tqdm
from pydantic import BaseModel
from typing import Dict, List, Tuple

from .llm import Openai, BaseLLM

DEFAULT_PROMPT_TMPL = """\
Context information is below.

---------------------
{context_str}
---------------------

Given the context information and not prior knowledge.
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

    queries: Dict[str, str]  # dict id -> query
    corpus: Dict[str, str]  # dict id -> string
    relevant_docs: Dict[str, List[str]]  # query id -> list of doc ids
    mode: str = "text"

    @property
    def query_docid_pairs(self) -> List[Tuple[str, List[str]]]:
        """Get query, relevant doc ids."""
        return [
            (query, self.relevant_docs[query_id])
            for query_id, query in self.queries.items()
        ]

    def save(self, path: str) -> None:
        """Save to lance dataset"""
        pass

    @classmethod
    def load(cls, path: str) -> "QADataset":
        """Load from .lance data"""
        pass
    
    # generate queries as a convenience function
    @classmethod
    def generate_qa_embedding_pairs(
        cls,
        nodes: 'List[TextNode]', # TODO: implement TextNode, currently using llama-index TextNode
        llm: BaseLLM,
        qa_generate_prompt_tmpl: str = DEFAULT_PROMPT_TMPL,
        num_questions_per_chunk: int = 2,
    ) -> "QADataset":
        """Generate examples given a set of nodes."""
        node_dict = {
            node.node_id: node.text
            for node in nodes
        }

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

        return QADataset(
            queries=queries, corpus=node_dict, relevant_docs=relevant_docs
        )
    

