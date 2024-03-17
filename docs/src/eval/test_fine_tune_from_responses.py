import os
import re
import json
import uuid
import lancedb
import pandas as pd

from tqdm import tqdm
from lancedb.embeddings.fine_tuner.llm import Openai
from lancedb.embeddings.fine_tuner.dataset import QADataset, TextChunk, DEFAULT_PROMPT_TMPL
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


def get_node(url):
    reader = SimpleDirectoryReader(input_files=download_test_files(url))
    docs = reader.load_data()

    parser = SentenceSplitter()
    nodes = parser.get_nodes_from_documents(docs)

    return nodes
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

nodes = get_node(train_url)

db = lancedb.connect("~/lancedb/fine-tuning")
model = get_registry().get("openai").create()
class Schema(LanceModel):
    id: str
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

retriever = db.create_table("fine-tuning", schema=Schema, mode="overwrite")
pylist = [{"id": str(node.node_id), "text": node.text} for node in nodes]
retriever.add(pylist)



ds_name = "response_data"
if os.path.exists(ds_name):
    ds = QADataset.load(ds_name)
else:
    # Generate questions
    llm = Openai()
    text_chunks = [TextChunk.from_llama_index_node(node) for node in nodes]

    queries = {}
    relevant_docs = {}
    for chunk in tqdm(text_chunks):
        text = chunk.text
        questions = llm.get_questions(DEFAULT_PROMPT_TMPL.format(context_str=text, num_questions_per_chunk=2))

        for question in questions:
            question_id = str(uuid.uuid4())
            queries[question_id] = question
            relevant_docs[question_id] = [retriever.search(question).to_pandas()["id"].tolist()[0]]
    ds = QADataset.from_responses(text_chunks, queries, relevant_docs)
    ds.save(ds_name)


# Fine-tune model
valset = get_dataset(train_url, "valset")

model = get_registry().get("sentence-transformers").create()
res_base = model.evaluate(valset)

model.finetune(trainset=ds, path="model_finetuned", epochs=4, log_wandb=True)
tuned = get_registry().get("sentence-transformers").create(name="./model_finetuned")
res_tuned = tuned.evaluate(valset)

openai_model = get_registry().get("openai").create()
#res_openai = openai_model.evaluate(valset)

#print(f"openai model results: {pd.DataFrame(res_openai)['is_hit'].mean()}")
print(f"base model results: {pd.DataFrame(res_base)['is_hit'].mean()}")
print(f"tuned model results: {pd.DataFrame(res_tuned)['is_hit'].mean()}")


