import pytest
import lancedb
from lancedb.embeddings.fine_tuner.llm import Openai
from lancedb.embeddings.fine_tuner.dataset import QADataset

def download_test_files():
    import os
    import requests
    
    url1 =  'https://raw.githubusercontent.com/run-llama/llama_index/main/docs/examples/data/10k/uber_2021.pdf'

    if not os.path.exists('data/10k/uber_2021.pdf'):
        os.makedirs('data/10k', exist_ok=True)
        with open('data/10k/uber_2021.pdf', 'wb') as f:
            f.write(requests.get(url1).content)

    return 'data/10k/uber_2021.pdf'

@pytest.mark.slow
def test_qa_dataset():
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
