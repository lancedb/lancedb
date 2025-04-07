# MCP server with LanceDB

The Model Context Protocol (MCP) is an open protocol that enables seamless integration between LLM applications and external data sources and tools. Whether you're building an AI-powered IDE, enhancing a chat interface, or creating custom AI workflows, MCP provides a standardized way to connect LLMs with the context they need.

With LanceDB, your MCP can be embedded in your application. Let's implement 2 simple MCP tools using LanceDB
1. Add data - add data to LanceDB
2. Retreive data - retrieve data from LanceDB

You need to install `mcp[cli]` python package.

First, let's define some configs:
```python
# mcp_server.py

LANCEDB_URI = "~/lancedb"
TABLE_NAME =  "mcp_data"
EMBEDDING_FUNCTION = "sentence-transformers"
MODEL_NAME = "all-MiniLM-L6-v2" 
```

Then initialize the table that we'll use to store and retreive data:
```python
import lancedb
from lancedb.pydantic import LanceModel, Vector
from lancedb.embeddings import get_registry

model = get_registry().get(EMBEDDING_FUNCTION).create(model_name=MODEL_NAME)

class Schema(LanceModel):
    text: str = model.SourceField()
    vector: Vector(model.ndims()) = model.VectorField()

db = lancedb.connect(LANCEDB_URI)
if TABLE_NAME not in db.table_names():
    db.create_table(TABLE_NAME, schema=Schema)
```

!!! Note "Using LanceDB cloud"
    If you want to use LanceDB cloud, you'll need to set the uri to your remote table
    instance and also provide a token. Every other functionality will remain the same

## Defining the tools
Tools let LLMs take actions through your server. There are other components like `resources` that allow you to expose certain data sources to LLMs. For our use case, we need to define tools that LLMs can call in order to inget or retrieve data
We'll use `FastMCP` interface of the MCP package. The FastMCP server is your core interface to the MCP protocol. It handles connection management, protocol compliance, and message routing.

```python
from mcp.server.fastmcp import FastMCP

mcp = FastMCP("lancedb-example")
```
### Add data ingestion tool
This function takes a string as input and adds it to the LanceDB table.

```python
@mcp.tool()
async def ingest_data(content: str) -> str:
    """
    Add a new memory to the vector database
    Args:
        content: Content of the memory
    """
    tbl = db[TABLE_NAME]
    tbl.add([
        {"text": content}
        ])
    return f"Added memory: {content}"
```
### Retreive data tool

This function takes a string and limit as input and searches the LanceDB table for the most relevant memories.

```python
@mcp.tool()
async def retrieve_data(query: str, limit: int = 5) -> str:
    """
    Search db using vector search
    Args:
        query: The search query
        limit: Maximum number of results to return
    """
    tbl = data[TABLE_NAME]
    rs = tbl.search(query).limit(limit).to_list()
    data = [
        r["text"] for r in rs
    ]
    if not data:
        return "No relevant data found."

    return "\n\n".join(data)
```

## Install it on Claude desktop

To install this MCP, you can simply run this command and it'll be registered on you Claude desktop
```
mcp install mcp_server.py
```
You'll see logs similar to this:
```
[04/07/25 20:18:08] INFO     Load pretrained SentenceTransformer: BAAI/bge-small-en-v1.5       SentenceTransformer.py:218
Batches: 100%|█████████████████████████████████████████████████████████████████████████████| 1/1 [00:00<00:00,  4.06it/s]
[04/07/25 20:18:11] INFO     Added server 'lancedb' to Claude config                                        claude.py:129
                    INFO     Successfully installed lancedb in Claude app                                      cli.py:467
```

Now simply fire up claude desktop and you can start using it.

## Community examples

- Find a minimal LanceDB mcp server similar to this [here](https://github.com/kyryl-opens-ml/mcp-server-lancedb/blob/main/src/mcp_lance_db/server.py)

- You can find an implementation of a more complex MCP server that uses LanceDB to implement an advanced CodeQA feature [here](https://github.com/lancedb/MCPExample).

