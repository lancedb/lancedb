import json
from tqdm import tqdm
import pandas as pd
import time
from llama_index.core import ServiceContext, VectorStoreIndex, StorageContext
from llama_index.core.schema import TextNode
from llama_index.embeddings.openai import OpenAIEmbedding
from llama_index.vector_stores.lancedb import LanceDBVectorStore
from lancedb.rerankers import Reranker, CohereReranker
from llama_index.embeddings.huggingface import HuggingFaceEmbedding

import wandb


TRAIN_DATASET_FPATH = './finetune-embedding/data/train_dataset.json'
VAL_DATASET_FPATH = './finetune-embedding/data/val_dataset.json'

with open(TRAIN_DATASET_FPATH, 'r+') as f:
    train_dataset = json.load(f)

with open(VAL_DATASET_FPATH, 'r+') as f:
    val_dataset = json.load(f)

def run_query(tbl, query, vector_query, fts_query=None, reranker=None, top_k=5):
    if reranker is None:
        return tbl.search(vector_query).limit(2*top_k)
    elif fts_query is None:
        results = tbl.search(vector_query).rerank(reranker=reranker, query_string=query).limit(2*top_k)
    else:
        results = tbl.search((vector_query, fts_query)).rerank(reranker=reranker).limit(2*top_k)
    return results
       
    
def evaluate(
    dataset,
    embed_model,
    top_k=5,
    verbose=False,
    reranker: Reranker=None,
    query_type="vector"
):
    """
    Evaluate the retrieval performance of the given dataset using the given embedding model.

    Args:
    - dataset (dict): The dataset to evaluate. It should have the following keys:
        - corpus (dict): A dictionary of document IDs and their corresponding text.
        - queries (dict): A dictionary of query IDs and their corresponding text.
        - relevant_docs (dict): A dictionary of query IDs and their corresponding relevant document IDs.
    - embed_model (str): The embedding model to use.
    - top_k (int): The number of documents to retrieve.
    - verbose (bool): Whether to print the evaluation results.
    - reranker (Reranker): The reranker to use.
    - query_type (str): The type of query to use. It should be either "vector" or "hybrid".
           
    """
    corpus = dataset['corpus']
    queries = dataset['queries']
    relevant_docs = dataset['relevant_docs']

    service_context = ServiceContext.from_defaults(embed_model=embed_model)
    nodes = [TextNode(id_=id_, text=text) for id_, text in corpus.items()] 

    vector_store = LanceDBVectorStore(uri=f"/tmp/lancedb_hybrid_benc-{time.time()}")
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    index = VectorStoreIndex(
        nodes, 
        service_context=service_context, 
        storage_context=storage_context,
        show_progress=True
    )
    tbl = vector_store._connection.open_table(vector_store.table_name)
    # id: string
    # doc_id: null
    # vector: fixed_size_list<item: float>[1536]
    # child 0, item: float
    # text: string
    tbl.create_fts_index("text", replace=True)
    eval_results = []
    for query_id, query in tqdm(queries.items()):
        vector_query = embed_model.get_text_embedding(query)
        if query_type == "vector":
            rs = run_query(tbl, query, vector_query, reranker=reranker, top_k=top_k)
        elif query_type == "hybrid":
            fts_query = query
            rs = run_query(tbl, query, vector_query, fts_query, reranker=reranker, top_k=top_k)
        else:
            raise ValueError(f"Invalid query_type: {query_type}")
        try:
            retrieved_ids = rs.to_pandas()["id"].tolist()[:top_k]
        except Exception as e:
            print(f"Error: {e}")
            continue
        expected_id = relevant_docs[query_id][0]
        is_hit = expected_id in retrieved_ids  # assume 1 relevant doc
        
        eval_result = {
            'is_hit': is_hit,
            'retrieved': retrieved_ids,
            'expected': expected_id,
            'query': query_id,
        }
        eval_results.append(eval_result)
    return eval_results

rerankers = [None, CohereReranker(), CohereReranker(model_name="rerank-english-v3.0")]
query_type = ["vector", "hybrid"]
top_ks = [3]

for top_k in top_ks:
    for qt in query_type:
        for reranker in rerankers:
            wandb.init(project="cohere-v3-hf-embed", name=f"{reranker}_{qt}_top@{top_k}")
            embed = HuggingFaceEmbedding("sentence-transformers/all-MiniLM-L6-v2") #OpenAIEmbedding()
            results = evaluate(val_dataset, embed, reranker=reranker, query_type=qt)
            df = pd.DataFrame(results)
            hit_rate = df['is_hit'].mean()
            print(f"Reranker: {reranker}, Query Type: {qt}, Hit Rate: {hit_rate}")
            wandb.log({"hit_rate": hit_rate})
            wandb.finish()

