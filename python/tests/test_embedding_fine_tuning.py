import pytest
import uuid
from tqdm import tqdm
from lancedb.embeddings.fine_tuner import QADataset, TextChunk
from lancedb.embeddings import get_registry


def test_finetuning_sentence_transformers(tmp_path):
    queries = {}
    relevant_docs = {}
    chunks = [
        "This is a chunk related to legal docs",
        "This is another chunk related financial docs",
        "This is a chunk related to sports docs",
        "This is another chunk related to fashion docs",
    ]
    text_chunks = [TextChunk.from_chunk(chunk) for chunk in chunks]
    for chunk in tqdm(text_chunks):
        text = chunk.text
        questions = [
            "What is this chunk about?",
            "What is the main topic of this chunk?",
        ]
        for question in questions:
            question_id = str(uuid.uuid4())
            queries[question_id] = question
            relevant_docs[question_id] = [chunk.id]
    ds = QADataset.from_responses(text_chunks, queries, relevant_docs)

    assert len(ds.queries) == 8
    assert len(ds.corpus) == 4

    model = get_registry().get("sentence-transformers").create()
    model.finetune(trainset=ds, valset=ds, path=str(tmp_path / "model"), epochs=1)
    model = (
        get_registry().get("sentence-transformers").create(name=str(tmp_path / "model"))
    )
    res = model.evaluate(ds)


def test_text_chunk():
    # TODO
    pass
