import pickle
import re
import sys
import zipfile
from pathlib import Path

import requests
from langchain.chains import RetrievalQA
from langchain.document_loaders import UnstructuredHTMLLoader
from langchain.embeddings import OpenAIEmbeddings
from langchain.llms import OpenAI
from langchain.text_splitter import RecursiveCharacterTextSplitter
from langchain.vectorstores import LanceDB
from modal import Image, Secret, Stub, web_endpoint

import lancedb

lancedb_image = Image.debian_slim().pip_install(
    "lancedb", "langchain", "openai", "pandas", "tiktoken", "unstructured", "tabulate"
)

stub = Stub(
    name="example-langchain-lancedb",
    image=lancedb_image,
    secrets=[Secret.from_name("my-openai-secret")],
)

docsearch = None
docs_path = Path("docs.pkl")
db_path = Path("lancedb")


def get_document_title(document):
    m = str(document.metadata["source"])
    title = re.findall("pandas.documentation(.*).html", m)
    if title[0] is not None:
        return title[0]
    return ""


def download_docs():
    pandas_docs = requests.get(
        "https://eto-public.s3.us-west-2.amazonaws.com/datasets/pandas_docs/pandas.documentation.zip"
    )
    with open(Path("pandas.documentation.zip"), "wb") as f:
        f.write(pandas_docs.content)

    file = zipfile.ZipFile(Path("pandas.documentation.zip"))
    file.extractall(path=Path("pandas_docs"))


def store_docs():
    docs = []

    if not docs_path.exists():
        for p in Path("pandas_docs/pandas.documentation").rglob("*.html"):
            if p.is_dir():
                continue
            loader = UnstructuredHTMLLoader(p)
            raw_document = loader.load()

            m = {}
            m["title"] = get_document_title(raw_document[0])
            m["version"] = "2.0rc0"
            raw_document[0].metadata = raw_document[0].metadata | m
            raw_document[0].metadata["source"] = str(raw_document[0].metadata["source"])
            docs = docs + raw_document

        with docs_path.open("wb") as fh:
            pickle.dump(docs, fh)
    else:
        with docs_path.open("rb") as fh:
            docs = pickle.load(fh)

    return docs


def qanda_langchain(query):
    download_docs()
    docs = store_docs()

    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=1000,
        chunk_overlap=200,
    )
    documents = text_splitter.split_documents(docs)
    embeddings = OpenAIEmbeddings()

    db = lancedb.connect(db_path)
    table = db.create_table(
        "pandas_docs",
        data=[
            {
                "vector": embeddings.embed_query("Hello World"),
                "text": "Hello World",
                "id": "1",
            }
        ],
        mode="overwrite",
    )
    docsearch = LanceDB.from_documents(documents, embeddings, connection=table)
    qa = RetrievalQA.from_chain_type(
        llm=OpenAI(), chain_type="stuff", retriever=docsearch.as_retriever()
    )
    return qa.run(query)


@stub.function()
@web_endpoint(method="GET")
def web(query: str):
    answer = qanda_langchain(query)
    return {
        "answer": answer,
    }


@stub.function()
def cli(query: str):
    answer = qanda_langchain(query)
    print(answer)
