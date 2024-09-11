**Graph RAG 📊**
====================================================================
Graph RAG uses knowledge graphs together with large language models (LLMs) to improve how information is retrieved and generated. It overcomes the limits of traditional search methods by using knowledge graphs, which organize data as connected entities and relationships.

One of the main benefits of Graph RAG is its ability to capture and represent complex relationships between entities, something that traditional text-based retrieval systems struggle with. By using this structured knowledge, LLMs can better grasp the context and details of a query, resulting in more accurate and insightful answers.

**[Official Paper](https://arxiv.org/pdf/2404.16130)**

**[Offical Implementation](https://github.com/microsoft/graphrag)**

[Microsoft Research Blog](https://www.microsoft.com/en-us/research/blog/graphrag-unlocking-llm-discovery-on-narrative-private-data/)

!!! note "Default VectorDB"

    Graph RAG uses LanceDB as the default vector database for performing vector search to retrieve relevant entities.

Working with Graph RAG is quite straightforward

- **Installation and API KEY as env variable**

Set `OPENAI_API_KEY` as `GRAPHRAG_API_KEY`

```bash
pip install graphrag
export GRAPHRAG_API_KEY="sk-..."
```

- **Initial structure for indexing dataset**

```bash
python3 -m graphrag.index --init --root dataset-dir
```

- **Index Dataset**

```bash
python3 -m graphrag.index --root dataset-dir
```

- **Execute Query**

Global Query Execution gives a broad overview of dataset

```bash
python3 -m graphrag.query --root dataset-dir --method global "query-question"
```

Local Query Execution gives a detailed and specific answers based on the context of the entities

```bash
python3 -m graphrag.query --root  dataset-dir --method local "query-question"
```

[![Open In Colab](../assets/colab.svg)](https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/Graphrag/main.ipynb)