#  Copyright 2023 LanceDB Developers
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Full text search index using tantivy-py"""
import os
from typing import List, Tuple

import pyarrow as pa

try:
    import tantivy
except ImportError:
    raise ImportError(
        "Please install tantivy-py `pip install tantivy@git+https://github.com/quickwit-oss/tantivy-py#164adc87e1a033117001cf70e38c82a53014d985` to use the full text search feature."
    )

from .table import LanceTable


def create_index(index_path: str, text_fields: List[str]) -> tantivy.Index:
    """
    Create a new Index (not populated)

    Parameters
    ----------
    index_path : str
        Path to the index directory
    text_fields : List[str]
        List of text fields to index

    Returns
    -------
    index : tantivy.Index
        The index object (not yet populated)
    """
    # Declaring our schema.
    schema_builder = tantivy.SchemaBuilder()
    # special field that we'll populate with row_id
    schema_builder.add_integer_field("doc_id", stored=True)
    # data fields
    for name in text_fields:
        schema_builder.add_text_field(name, stored=True)
    schema = schema_builder.build()
    os.makedirs(index_path, exist_ok=True)
    index = tantivy.Index(schema, path=index_path)
    return index


def populate_index(index: tantivy.Index, table: LanceTable, fields: List[str]) -> int:
    """
    Populate an index with data from a LanceTable

    Parameters
    ----------
    index : tantivy.Index
        The index object
    table : LanceTable
        The table to index
    fields : List[str]
        List of fields to index

    Returns
    -------
    int
        The number of rows indexed
    """
    # first check the fields exist and are string or large string type
    for name in fields:
        f = table.schema.field(name)  # raises KeyError if not found
        if not pa.types.is_string(f.type) and not pa.types.is_large_string(f.type):
            raise TypeError(f"Field {name} is not a string type")

    # create a tantivy writer
    writer = index.writer()
    # write data into index
    dataset = table.to_lance()
    row_id = 0
    for b in dataset.to_batches(columns=fields):
        for i in range(b.num_rows):
            doc = tantivy.Document()
            doc.add_integer("doc_id", row_id)
            for name in fields:
                doc.add_text(name, b[name][i].as_py())
            writer.add_document(doc)
            row_id += 1
    # commit changes
    writer.commit()
    return row_id


def search_index(
    index: tantivy.Index, query: str, limit: int = 10
) -> Tuple[Tuple[int], Tuple[float]]:
    """
    Search an index for a query

    Parameters
    ----------
    index : tantivy.Index
        The index object
    query : str
        The query string
    limit : int
        The maximum number of results to return

    Returns
    -------
    ids_and_score: list[tuple[int], tuple[float]]
        A tuple of two tuples, the first containing the document ids
        and the second containing the scores
    """
    searcher = index.searcher()
    query = index.parse_query(query)
    # get top results
    results = searcher.search(query, limit)
    if results.count == 0:
        return tuple(), tuple()
    return tuple(
        zip(
            *[
                (searcher.doc(doc_address)["doc_id"][0], score)
                for score, doc_address in results.hits
            ]
        )
    )
