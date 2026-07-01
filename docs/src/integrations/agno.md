---
title: Agno Integration with LanceDB | AI Agent Framework Guide
description: Learn how to build AI agents with LanceDB using the Agno framework. Includes knowledge base setup, vector search configuration, and best practices for building context-aware AI applications.
---

# Agno

**[Agno](https://docs.agno.com)** (formerly Phidata) is an open-source framework for building **multi-modal AI agents** with memory, knowledge, and tools. It is model-agnostic and designed to make it easy to create agents that can reason, search, and take actions.

Agno agents work by combining three capabilities:

- **Memory**: Stores chat history in a database so agents can hold long-term conversations.
- **Knowledge**: Stores information in a **vector database** so agents can search for relevant context before responding. (This is where LanceDB comes in.)
- **Tools**: Give agents the ability to take actions — search the web, query APIs, run code, and more.

LanceDB acts as the **knowledge vector store** for Agno agents. When a user asks a question, the agent searches LanceDB for the most relevant content and passes it to the LLM as context — enabling accurate, grounded responses without hallucination.

## Installation

```bash
pip install agno lancedb openai
```

Set your OpenAI API key as an environment variable:

```bash
export OPENAI_API_KEY="sk-..."
```

## Quick Start

The example below creates an Agno agent that answers questions about Thai recipes, using a PDF loaded into a LanceDB knowledge base.

```python
from agno.agent import Agent
from agno.knowledge.pdf_url import PDFUrlKnowledgeBase
from agno.models.openai import OpenAIChat
from agno.vectordb.lancedb import LanceDb, SearchType

# Initialize LanceDB as the vector store
vector_db = LanceDb(
    table_name="recipes",
    uri="tmp/lancedb",            # local directory where LanceDB stores data
    search_type=SearchType.vector,
)

# Create a knowledge base from a PDF URL
knowledge_base = PDFUrlKnowledgeBase(
    urls=["https://agno-public.s3.amazonaws.com/recipes/ThaiRecipes.pdf"],
    vector_db=vector_db,
)

# Load data into LanceDB (only needs to run once)
knowledge_base.load(recreate=False)

# Create an agent with access to the knowledge base
agent = Agent(
    model=OpenAIChat(id="gpt-4o-mini"),
    knowledge=knowledge_base,
    search_knowledge=True,   # lets the agent search LanceDB before responding
    show_tool_calls=True,
    markdown=True,
)

# Ask the agent a question
agent.print_response("How do I make Pad Thai?", stream=True)
```

When you call `knowledge_base.load()`, Agno:

1. Fetches and parses the source document.
2. Splits it into smaller chunks.
3. Creates vector embeddings for each chunk.
4. Stores the embeddings in the LanceDB table.

On each query, the agent searches LanceDB for the most relevant chunks and includes them in the prompt sent to the LLM.

## LanceDb Parameters

| Name | Type | Purpose | Default |
|------|------|---------|---------|
| `uri` | `str` | Path to the local LanceDB directory (or LanceDB Cloud URI). | `"/tmp/lancedb"` |
| `table_name` | `str` | Name of the table inside the database. | `"agno"` |
| `search_type` | `SearchType` | Search strategy: `SearchType.vector`, `SearchType.keyword`, or `SearchType.hybrid`. | `SearchType.vector` |
| `embedder` | `Embedder` | Embedding model to use. | `OpenAIEmbedder` |
| `distance` | `Distance` | Distance metric for vector search: `cosine`, `l2`, or `dot`. | `Distance.cosine` |
| `nprobes` | `int` | Number of partitions to search during ANN search. Higher values improve recall at the cost of speed. | `20` |

## Loading Different Knowledge Sources

Agno supports loading knowledge from multiple source types into LanceDB:

=== "PDF from URL"

    ```python
    from agno.knowledge.pdf_url import PDFUrlKnowledgeBase
    from agno.vectordb.lancedb import LanceDb

    knowledge_base = PDFUrlKnowledgeBase(
        urls=["https://example.com/document.pdf"],
        vector_db=LanceDb(table_name="pdf_docs", uri="tmp/lancedb"),
    )
    knowledge_base.load(recreate=False)
    ```

=== "Local text files"

    ```python
    from agno.knowledge.text import TextKnowledgeBase
    from agno.vectordb.lancedb import LanceDb

    knowledge_base = TextKnowledgeBase(
        path="data/",   # path to a text file or a directory of text files
        vector_db=LanceDb(table_name="text_docs", uri="tmp/lancedb"),
    )
    knowledge_base.load(recreate=False)
    ```

=== "Website content"

    ```python
    from agno.knowledge.website import WebsiteKnowledgeBase
    from agno.vectordb.lancedb import LanceDb

    knowledge_base = WebsiteKnowledgeBase(
        urls=["https://docs.agno.com"],
        max_links=10,
        vector_db=LanceDb(table_name="web_docs", uri="tmp/lancedb"),
    )
    knowledge_base.load(recreate=False)
    ```

## Async Support

For high-throughput applications, Agno supports async loading and querying with LanceDB:

```python
import asyncio
from agno.agent import Agent
from agno.knowledge.pdf_url import PDFUrlKnowledgeBase
from agno.models.openai import OpenAIChat
from agno.vectordb.lancedb import LanceDb, SearchType


async def main():
    vector_db = LanceDb(
        table_name="recipes",
        uri="tmp/lancedb",
        search_type=SearchType.vector,
    )
    knowledge_base = PDFUrlKnowledgeBase(
        urls=["https://agno-public.s3.amazonaws.com/recipes/ThaiRecipes.pdf"],
        vector_db=vector_db,
    )

    # Load data asynchronously
    await knowledge_base.aload(recreate=False)

    agent = Agent(
        model=OpenAIChat(id="gpt-4o-mini"),
        knowledge=knowledge_base,
        search_knowledge=True,
    )

    # Query asynchronously
    await agent.aprint_response("List the ingredients for Tom Yum soup", stream=True)


asyncio.run(main())
```

## Further Resources

- [Agno documentation](https://docs.agno.com)
- [LanceDB x Agno on Agno docs](https://docs.agno.com/knowledge/vector-stores/lancedb/overview)
- [Agno cookbook — LanceDB examples](https://github.com/agno-agi/agno/tree/main/cookbook/knowledge/vector_db/lance_db)
- [LanceDB Python SDK reference](../python/python.md)
