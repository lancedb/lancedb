import os
import json
import lancedb
import pandas as pd

from lancedb.embeddings.fine_tuner.llm import Openai
from lancedb.embeddings.fine_tuner.dataset import QADataset, TextChunk
from lancedb.pydantic import LanceModel, Vector
from llama_index.core import SimpleDirectoryReader
from llama_index.core.node_parser import SentenceSplitter
from llama_index.core.schema import MetadataMode
from lancedb.embeddings import get_registry


test_url = 'https://raw.githubusercontent.com/run-llama/llama_index/main/docs/examples/data/10k/lyft_2021.pdf'
train_url = 'https://raw.githubusercontent.com/run-llama/llama_index/main/docs/examples/data/10k/uber_2021.pdf'
def download_test_files(url):
    import os
    import requests
    
    # download to cwd
    files = []
    filename = os.path.basename(url)
    if not os.path.exists(filename):
        print(f"Downloading {url} to {filename}")
        r = requests.get(url)
        with open(filename, 'wb') as f:
            f.write(r.content)
    files.append(filename)
    return files

def get_dataset(url, name):
    reader = SimpleDirectoryReader(input_files=download_test_files(url))
    docs = reader.load_data()

    parser = SentenceSplitter()
    nodes = parser.get_nodes_from_documents(docs)

    if os.path.exists(name):
        ds = QADataset.load(name)
    else:
        llm = Openai()
        
        # convert Llama-index TextNode to TextChunk
        chunks = [TextChunk.from_llama_index_node(node) for node in nodes]

        ds = QADataset.from_llm(chunks, llm)
        ds.save(name)
    return ds



trainset = get_dataset(test_url, "qa_dataset_1")
valset = get_dataset(train_url, "valset")

#model = get_registry().get("sentence-transformers").create()
#model.finetune(trainset=trainset, valset=valset, path="model_finetuned_1", epochs=4)

base = get_registry().get("sentence-transformers").create()
tuned = get_registry().get("sentence-transformers").create(name="./model_finetuned_1")
#openai = get_registry().get("openai").create(name="text-embedding-3-large")


rs1 = base.evaluate(valset, path="val_res")
rs2 = tuned.evaluate(valset, path="val_res")
#rs3 = openai.evaluate(valset)

#print("openai-embedding-v3 hit-rate  - ", pd.DataFrame(rs3)["is_hit"].mean())
print("fine-tuned hit-rate  - ", pd.DataFrame(rs2)["is_hit"].mean())
print("Base model hite-rate - ", pd.DataFrame(rs1)["is_hit"].mean())

