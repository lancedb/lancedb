**HyDE: Hypothetical Document Embeddings 🤹‍♂️**
====================================================================
HyDE, stands for Hypothetical Document Embeddings is an approach used for precise zero-shot dense retrieval without relevance labels. It focuses on augmenting and improving similarity searches, often intertwined with vector stores in information retrieval. The method generates a hypothetical document for an incoming query, which is then embedded and used to look up real documents that are similar to the hypothetical document.

**[Official Paper](https://arxiv.org/pdf/2212.10496)**

<figure markdown="span">
  ![hyde](https://raw.githubusercontent.com/lancedb/assets/main/docs/assets/rag/hyde.png)
  <figcaption>HyDE: <a href="https://arxiv.org/pdf/2212.10496">Source</a></figcaption>
</figure>

[![Open In Colab](../../assets/colab.svg)](https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/Advance-RAG-with-HyDE/main.ipynb)

Here’s a code snippet for using HyDE with Langchain

```python
from langchain.llms import OpenAI
from langchain.embeddings import OpenAIEmbeddings
from langchain.prompts import PromptTemplate
from langchain.chains import LLMChain, HypotheticalDocumentEmbedder
from langchain.vectorstores import LanceDB

# set OPENAI_API_KEY as env variable before this step
# initialize LLM and embedding function
llm = OpenAI()
emebeddings = OpenAIEmbeddings()

# HyDE embedding
embeddings = HypotheticalDocumentEmbedder(llm_chain=llm_chain,base_embeddings=embeddings)

# load dataset

# LanceDB retriever
retriever = LanceDB.from_documents(documents, embeddings, connection=table)

# prompt template
prompt_template = """
As a knowledgeable and helpful research assistant, your task is to provide informative answers based on the given context. Use your extensive knowledge base to offer clear, concise, and accurate responses to the user's inquiries.
if quetion is not related to documents simply say you dont know
Question: {question}

Answer:
"""

prompt = PromptTemplate(input_variables=["question"], template=prompt_template)

# LLM Chain
llm_chain = LLMChain(llm=llm, prompt=prompt)

# vector search
retriever.similarity_search(query)
llm_chain.run(query)
```

[![Open In Colab](../../assets/colab.svg)](https://colab.research.google.com/github/lancedb/vectordb-recipes/blob/main/examples/Advance-RAG-with-HyDE/main.ipynb)
