## Finetuning the Embedding Model
Try it yourself - <a href="https://colab.research.google.com/github/lancedb/lancedb/blob/main/docs/src/notebooks/embedding_tuner.ipynb"><img src="https://colab.research.google.com/assets/colab-badge.svg" alt="Open In Colab"></a><br/>

Another way to improve retriever performance is to fine-tune the embedding model itself. Fine-tuning the embedding model can help in learning better representations for the documents and queries in the dataset. This can be particularly useful when the dataset is very different from the pre-trained data used to train the embedding model.

We'll use the same dataset as in the previous sections. Start off by splitting the dataset into training and validation sets:
```python
from sklearn.model_selection import train_test_split

train_df, validation_df = train_test_split("data_qa.csv", test_size=0.2, random_state=42)

train_df.to_csv("data_train.csv", index=False)
validation_df.to_csv("data_val.csv", index=False)
```

You can use any tuning API to fine-tune embedding models. In this example, we'll utilise Llama-index as it also comes with utilities for synthetic data generation and training the model. 


Then parse the dataset as llama-index text nodes and generate synthetic QA pairs from each node.
```python
from llama_index.core.node_parser import SentenceSplitter
from llama_index.readers.file import PagedCSVReader
from llama_index.finetuning import generate_qa_embedding_pairs
from llama_index.core.evaluation import EmbeddingQAFinetuneDataset

def load_corpus(file):
    loader = PagedCSVReader(encoding="utf-8")
    docs = loader.load_data(file=Path(file))

    parser = SentenceSplitter()
    nodes = parser.get_nodes_from_documents(docs)

    return nodes

from llama_index.llms.openai import OpenAI


train_dataset = generate_qa_embedding_pairs(
    llm=OpenAI(model="gpt-3.5-turbo"), nodes=train_nodes, verbose=False
)
val_dataset = generate_qa_embedding_pairs(
    llm=OpenAI(model="gpt-3.5-turbo"), nodes=val_nodes, verbose=False
)
```

Now we'll use `SentenceTransformersFinetuneEngine` engine to fine-tune the model. You can also use `sentence-transformers` or `transformers` library to fine-tune the model. 

```python
from llama_index.finetuning import SentenceTransformersFinetuneEngine

finetune_engine = SentenceTransformersFinetuneEngine(
    train_dataset,
    model_id="BAAI/bge-small-en-v1.5",
    model_output_path="tuned_model",
    val_dataset=val_dataset,
)
finetune_engine.finetune()
embed_model = finetune_engine.get_finetuned_model()
```
This saves the fine tuned embedding model in `tuned_model` folder. This al

# Evaluation results
In order to eval the retriever, you can either use this model to ingest the data into LanceDB directly or llama-index's LanceDB integration to create a `VectorStoreIndex` and use it as a retriever. 
On performing the same hit-rate evaluation as before, we see a significant improvement in the hit-rate across all query types.

### Baseline
| Query Type | Hit-rate@5 |
| --- | --- |
| Vector Search | 0.640 |
| Full-text Search | 0.595 |
| Reranked Vector Search | 0.677 |
| Reranked Full-text Search | 0.672 |
| Hybrid Search (w/ CohereReranker) | 0.759|

### Fine-tuned model ( 2 iterations )
| Query Type | Hit-rate@5 |
| --- | --- |
| Vector Search | 0.672 |
| Full-text Search | 0.595 |
| Reranked Vector Search | 0.754 |
| Reranked Full-text Search | 0.672|
| Hybrid Search (w/ CohereReranker) | 0.768 |
