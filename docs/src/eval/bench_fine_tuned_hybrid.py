import json
from tqdm import tqdm
import pandas as pd
import os
import requests
from llama_index.core import ServiceContext, VectorStoreIndex, StorageContext
from llama_index.core.schema import TextNode
from llama_index.vector_stores.lancedb import LanceDBVectorStore
from lancedb.rerankers import CrossEncoderReranker, ColbertReranker, CohereReranker, LinearCombinationReranker
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.embeddings.openai import OpenAIEmbedding
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry
from lancedb.embeddings.fine_tuner.dataset import QADataset, TextChunk, DEFAULT_PROMPT_TMPL
from lancedb.pydantic import LanceModel, Vector
from llama_index.core import SimpleDirectoryReader
from llama_index.core.node_parser import SentenceSplitter
from lancedb.embeddings.fine_tuner.llm import Openai

import time
import lancedb
import wandb
from pydantic import BaseModel, root_validator
from typing import Optional

TRAIN_DATASET_FPATH = './data/train_dataset.json'
VAL_DATASET_FPATH = './data/val_dataset.json'

with open(TRAIN_DATASET_FPATH, 'r+') as f:
    train_dataset = json.load(f)

with open(VAL_DATASET_FPATH, 'r+') as f:
    val_dataset = json.load(f)

def train_embedding_model(epoch):
    def download_test_files(url):
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

            ds = QADataset.from_llm(chunks, llm, num_questions_per_chunk=2)
            ds.save(name)
        return ds
    train_url = 'https://raw.githubusercontent.com/run-llama/llama_index/main/docs/examples/data/10k/uber_2021.pdf'
    ds = get_dataset(train_url, "qa_dataset_uber")


    model = get_registry().get("sentence-transformers").create(name="BAAI/bge-small-en-v1.5")
    model.finetune(trainset=ds, valset=None, path="model_airbnb", epochs=epoch, log_wandb=True, run_name="lyft_finetune")


def evaluate(
    dataset,
    embed_model,
    reranker=None,
    top_k=5,
    verbose=False,
):
    corpus = dataset['corpus']
    queries = dataset['queries']
    relevant_docs = dataset['relevant_docs']

    vector_store = LanceDBVectorStore(uri="/tmp/lancedb")
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    service_context = ServiceContext.from_defaults(embed_model=embed_model)
    nodes = [TextNode(id_=id_, text=text) for id_, text in corpus.items()] 
    index = VectorStoreIndex(
        nodes, 
        service_context=service_context, 
        show_progress=True,
        storage_context=storage_context,
    )
    tbl = vector_store.connection.open_table(vector_store.table_name)
    tbl.create_fts_index("text", replace=True)

    eval_results = []
    for query_id, query in tqdm(queries.items()):
        query_vector = embed_model.get_query_embedding(query)
        try:
            if reranker is None:
                rs = tbl.search(query_vector).limit(top_k).to_pandas()
            else:
                rs = tbl.search((query_vector, query)).rerank(reranker=reranker).limit(top_k).to_pandas()
        except Exception as e:
            print(f'Error with query: {query_id} {e}')
            continue
        retrieved_ids = rs['id'].tolist()[:top_k]
        expected_id = relevant_docs[query_id][0]
        is_hit = expected_id in retrieved_ids  # assume 1 relevant doc
        if len(eval_results) == 0:
            print(f"Query: {query}")
            print(f"Expected: {expected_id}")
            print(f"Retrieved: {retrieved_ids}")
        eval_result = {
            'is_hit': is_hit,
            'retrieved': retrieved_ids,
            'expected': expected_id,
            'query': query_id,
        }
        eval_results.append(eval_result)
    return eval_results

if __name__ == '__main__':
    train_embedding_model(4)
    #embed_model = OpenAIEmbedding() # model="text-embedding-3-small"
    rerankers = {
        "Vector Search": None,
        "Cohere": CohereReranker(),
        "Cross Encoder": CrossEncoderReranker(),
        "Colbert": ColbertReranker(),
        "linear": LinearCombinationReranker(),
    }
    top_ks = [3]
    for top_k in top_ks:
        #for epoch in epochs:
        for name, reranker in rerankers.items():
            #embed_model = HuggingFaceEmbedding("./model_airbnb")
            embed_model = OpenAIEmbedding()
            wandb.init(project=f"Reranker-based", name=name)
            val_eval_results = evaluate(val_dataset, embed_model, reranker=reranker, top_k=top_k)
            df = pd.DataFrame(val_eval_results)

            hit_rate = df['is_hit'].mean()
            print(f'Hit rate: {hit_rate:.2f}')
            wandb.log({f"openai_base_hit_rate_@{top_k}": hit_rate})
            wandb.finish()


