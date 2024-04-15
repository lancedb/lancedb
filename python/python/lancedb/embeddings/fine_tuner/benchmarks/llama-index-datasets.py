import os
from llama_index.core import SimpleDirectoryReader
from llama_index.core.llama_dataset import LabelledRagDataset
from llama_index.core.node_parser import SentenceSplitter
from lancedb.embeddings.fine_tuner.dataset import QADataset, TextChunk
from lancedb.embeddings.fine_tuner.llm import Openai
from lancedb.embeddings import get_registry
from llama_index.core import VectorStoreIndex
from llama_index.vector_stores.lancedb import LanceDBVectorStore
from llama_index.core import ServiceContext, VectorStoreIndex, StorageContext
from llama_index.embeddings.huggingface import HuggingFaceEmbedding
from llama_index.core.llama_pack import download_llama_pack

import time
import wandb



import pandas as pd

def get_paths_from_dataset(dataset: str, split=True):
    """
    Returns paths of:
    - downloaded dataset, lance train dataset, lance test dataset, finetuned model
    """
    if split:
        return f"./data/{dataset}", f"./data/{dataset}_lance_train", f"./data/{dataset}_lance_test", f"./data/tuned_{dataset}"
    return f"./data/{dataset}", f"./data/{dataset}_lance", f"./data/tuned_{dataset}"

def get_llama_dataset(dataset: str):
    """
    returns:
    - nodes, documents, rag_dataset
    """
    if not os.path.exists(f"./data/{dataset}"):
        os.system(f"llamaindex-cli download-llamadataset {dataset} --download-dir ./data/{dataset}")
    rag_dataset = LabelledRagDataset.from_json(f"./data/{dataset}/rag_dataset.json")
    docs = SimpleDirectoryReader(input_dir=f"./data/{dataset}/source_files").load_data()

    parser = SentenceSplitter()
    nodes = parser.get_nodes_from_documents(docs)

    return nodes, docs, rag_dataset

def lance_dataset_from_llama_nodes(nodes: list, name: str, split=True):
    llm = Openai()
    chunks = [TextChunk.from_llama_index_node(node) for node in nodes]
    # train test split 75-35
    if not split:
        if os.path.exists(f"./data/{name}_lance"):
            ds = QADataset.load(f"./data/{name}_lance")
            return ds
        ds = QADataset.from_llm(chunks, llm)
        ds.save(f"./data/{name}_lance")
        return ds
    
    if os.path.exists(f"./data/{name}_lance_train") and os.path.exists(f"./data/{name}_lance_test"):
        train_ds = QADataset.load(f"./data/{name}_lance_train")
        test_ds = QADataset.load(f"./data/{name}_lance_test")
        return train_ds, test_ds
    # split chunks random 
    train_size = int(len(chunks) * 0.65)
    train_chunks = chunks[:train_size]
    test_chunks = chunks[train_size:]
    train_ds = QADataset.from_llm(train_chunks, llm)
    test_ds = QADataset.from_llm(test_chunks, llm)
    train_ds.save(f"./data/{name}_lance_train")
    test_ds.save(f"./data/{name}_lance_test")
    return train_ds, test_ds



    


def finetune(trainset: str, model: str, epochs: int, path: str, valset: str = None, top_k=5):
    print(f"Finetuning {model} for {epochs} epochs")
    print(f"trainset query instances: {len(trainset.queries)}")
    print(f"valset query instances: {len(valset.queries)}")

    valset = valset if valset is not None else trainset
    model = get_registry().get("sentence-transformers").create(name=model)
    base_result = model.evaluate(valset, path=f"./data/eva/", top_k=top_k)
    base_hit_rate = pd.DataFrame(base_result)["is_hit"].mean()

    model.finetune(trainset=trainset, valset=valset, path=path, epochs=epochs)
    tuned = get_registry().get("sentence-transformers").create(name=path)
    tuned_result = tuned.evaluate(valset, path=f"./data/eva/{str(time.time())}", top_k=top_k)
    tuned_hit_rate = pd.DataFrame(tuned_result)["is_hit"].mean()

    return base_hit_rate, tuned_hit_rate


def eval_rag(dataset: str, model: str):
    # Requires - pip install llama-index-vector-stores-lancedb
    # Requires - pip install llama-index-embeddings-huggingface
    nodes, docs, rag_dataset = get_llama_dataset(dataset)

    embed_model = HuggingFaceEmbedding(model)
    vector_store = LanceDBVectorStore(uri="/tmp/lancedb")
    storage_context = StorageContext.from_defaults(vector_store=vector_store)
    service_context = ServiceContext.from_defaults(embed_model=embed_model)
    index = VectorStoreIndex(
        nodes, 
        service_context=service_context, 
        show_progress=True,
        storage_context=storage_context,
    )

    # build basic RAG system
    index = VectorStoreIndex.from_documents(documents=docs)
    query_engine = index.as_query_engine()

    # evaluate using the RagEvaluatorPack
    RagEvaluatorPack = download_llama_pack(
        "RagEvaluatorPack", "./rag_evaluator_pack"
    )
    rag_evaluator_pack = RagEvaluatorPack(
        rag_dataset=rag_dataset, query_engine=query_engine
    )

    metrics = rag_evaluator_pack.run(
        batch_size=20,  # batches the number of openai api calls to make
        sleep_time_in_seconds=1,  # seconds to sleep before making an api call
    )

    return metrics

def main(dataset, model, epochs, top_k=5, eval_rag=False, project: str = "lancedb_finetune"):
    nodes, _, _ = get_llama_dataset(dataset)
    trainset, valset = lance_dataset_from_llama_nodes(nodes, dataset)
    data_path, lance_train_path, lance_test_path, tuned_path = get_paths_from_dataset(dataset, split=True)
    
    base_hit_rate, tuned_hit_rate = finetune(trainset, model, epochs, tuned_path, valset, top_k=top_k)

    # Base model model metrics
    metrics = eval_rag(dataset, model)

    # Tuned model metrics
    metrics_tuned = eval_rag(dataset, tuned_path)
    
    wandb.init(project="lancedb_finetune",  name=f"{dataset}_{model}_{epochs}")
    wandb.log({
        "hit_rate": tuned_hit_rate,
    })
    wandb.log(metrics_tuned)
    wandb.finish()

    wandb.init(project="lancedb_finetune",  name=f"{dataset}_{model}_base")
    wandb.log({
        "hit_rate": base_hit_rate,
    })
    wandb.log(metrics)
    wandb.finish()


def banchmark_all():
    datasets = ["Uber10KDataset2021", "MiniTruthfulQADataset", "MiniSquadV2Dataset", "MiniEsgBenchDataset", "MiniCovidQaDataset", "Llama2PaperDataset", "HistoryOfAlexnetDataset", "PatronusAIFinanceBenchDataset"]
    models = ["BAAI/bge-small-en-v1.5"]
    top_ks = [5]
    for top_k in top_ks:
        for model in models:
            for dataset in datasets:
                main(dataset, model, 5, top_k=top_k)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--dataset", type=str, default="BraintrustCodaHelpDeskDataset")
    parser.add_argument("--model", type=str, default="BAAI/bge-small-en-v1.5")
    parser.add_argument("--epochs", type=int, default=4)
    parser.add_argument("--project", type=str, default="lancedb_finetune")
    parser.add_argument("--top_k", type=int, default=5)
    parser.add_argument("--eval-rag", action="store_true")
    parser.add_argument("--benchmark-all", action="store_true")
    args = parser.parse_args()

    if args.benchmark_all:
        banchmark_all()
    else:
        main(args.dataset, args.model, args.epochs, args.top_k, args.eval_rag, args.project)
