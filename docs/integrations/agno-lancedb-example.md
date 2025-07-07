# Agno + LanceDB Integration

This guide shows  you how to integrate Agno (https://docs.agno.com/)  -- a simple framework for building LLM-powered agents with LanceDB (https://lancedb.com/)-- a fast, open-source vector database.

Together, they can build powerful AI agents that can search and retrieve vector data intelligently.

##  Requirements

Make sure you have the following installed:

- Python 3.8+
- pip install agno lancedb openai pandas


## What This Example Does

This integration demonstrates how to use **LanceDB** as a vector store inside an **Agno** application. The flow is:

- Take a question/query from the user
- Use OpenAI to generate embeddings
- **LanceDB** contains store/query embeddings
- Let Agno handle the LLM agent orchestration


## Setup

1. Clone the Agno repository:

```bash
git clone https://github.com/agnos-ai/agno.git
