import pytest
import lancedb
from lancedb.embeddings.fine_tuner.llm import Openai
from lancedb.embeddings.fine_tuner.dataset import QADataset


def download_test_files():
    import os
    import requests

    url1 = "https://raw.githubusercontent.com/run-llama/llama_index/main/docs/examples/data/10k/uber_2021.pdf"

    # download to cwd
    files = []
    for url in [url1]:
        filename = os.path.basename(url)
        if not os.path.exists(filename):
            print(f"Downloading {url} to {filename}")
            r = requests.get(url)
            with open(filename, "wb") as f:
                f.write(r.content)
        files.append(filename)
    return files


@pytest.mark.slow
def test_qa_dataset(tmp_path):
    import json
    from llama_index.core import SimpleDirectoryReader
    from llama_index.core.node_parser import SentenceSplitter
    from llama_index.core.schema import MetadataMode

    reader = SimpleDirectoryReader(input_files=download_test_files())
    docs = reader.load_data()

    parser = SentenceSplitter()
    nodes = parser.get_nodes_from_documents(docs)

    llm = Openai()
    ds = QADataset.generate_qa_embedding_pairs(nodes, llm)

    assert len(ds.queries) > 0
    assert len(ds.relevant_docs) == len(ds.queries)

    ds.save(tmp_path / "qa_dataset")
    ds2 = QADataset.load(tmp_path / "qa_dataset")

    assert ds.queries == ds2.queries
    assert ds.corpus == ds2.corpus
    assert ds.relevant_docs == ds2.relevant_docs
