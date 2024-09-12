**FLARE 💥**
====================================================================
FLARE, stands for Forward-Looking Active REtrieval augmented generation is a generic retrieval-augmented generation method that actively decides when and what to retrieve using a prediction of the upcoming sentence to anticipate future content and utilize it as the query to retrieve relevant documents if it contains low-confidence tokens.

**[Official Paper](https://arxiv.org/abs/2305.06983)**

<figure markdown="span">
  ![flare](https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/rag/flare.gif)
  <figcaption>FLARE: <a href="https://github.com/jzbjyb/FLARE">Source</a></figcaption>
</figure>

[![Open In Colab](../../assets/colab.svg)](https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/better-rag-FLAIR/main.ipynb)

Here’s a code snippet for using FLARE with Langchain

```python
from langchain.vectorstores import LanceDB
from langchain.document_loaders import ArxivLoader
from langchain.chains import FlareChain
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain
from langchain.llms import OpenAI

llm = OpenAI()

# load dataset

# LanceDB retriever
vector_store = LanceDB.from_documents(doc_chunks, embeddings, connection=table)
retriever = vector_store.as_retriever()

# define flare chain
flare = FlareChain.from_llm(llm=llm,retriever=vector_store_retriever,max_generation_len=300,min_prob=0.45)

result = flare.run(input_text)
```

[![Open In Colab](../../assets/colab.svg)](https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/better-rag-FLAIR/main.ipynb)