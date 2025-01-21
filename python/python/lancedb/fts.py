# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Full text search index using tantivy-py"""

import os
from typing import List, Tuple, Optional

import pyarrow as pa

try:
    import tantivy
except ImportError:
    raise ImportError(
        "Please install tantivy-py `pip install tantivy` to use the full text search feature."  # noqa: E501
    )

from .table import LanceTable


def create_index(
    index_path: str,
    text_fields: List[str],
    ordering_fields: Optional[List[str]] = None,
    tokenizer_name: str = "default",
) -> tantivy.Index:
    """
    Create a new Index (not populated)

    Parameters
    ----------
    index_path : str
        Path to the index directory
    text_fields : List[str]
        List of text fields to index
    ordering_fields: List[str]
        List of unsigned type fields to order by at search time
    tokenizer_name : str, default "default"
        The tokenizer to use

    Returns
    -------
    index : tantivy.Index
        The index object (not yet populated)
    """
    if ordering_fields is None:
        ordering_fields = []
    # Declaring our schema.
    schema_builder = tantivy.SchemaBuilder()
    # special field that we'll populate with row_id
    schema_builder.add_integer_field("doc_id", stored=True)
    # data fields
    for name in text_fields:
        schema_builder.add_text_field(name, stored=True, tokenizer_name=tokenizer_name)
    if ordering_fields:
        for name in ordering_fields:
            schema_builder.add_unsigned_field(name, fast=True)
    schema = schema_builder.build()
    os.makedirs(index_path, exist_ok=True)
    index = tantivy.Index(schema, path=index_path)
    return index


def populate_index(
    index: tantivy.Index,
    table: LanceTable,
    fields: List[str],
    writer_heap_size: Optional[int] = None,
    ordering_fields: Optional[List[str]] = None,
) -> int:
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
    writer_heap_size : int
        The writer heap size in bytes, defaults to 1GB

    Returns
    -------
    int
        The number of rows indexed
    """
    if ordering_fields is None:
        ordering_fields = []
    writer_heap_size = writer_heap_size or 1024 * 1024 * 1024
    # first check the fields exist and are string or large string type
    nested = []

    for name in fields:
        try:
            f = table.schema.field(name)  # raises KeyError if not found
        except KeyError:
            f = resolve_path(table.schema, name)
            nested.append(name)

        if not pa.types.is_string(f.type) and not pa.types.is_large_string(f.type):
            raise TypeError(f"Field {name} is not a string type")

    # create a tantivy writer
    writer = index.writer(heap_size=writer_heap_size)
    # write data into index
    dataset = table.to_lance()
    row_id = 0

    max_nested_level = 0
    if len(nested) > 0:
        max_nested_level = max([len(name.split(".")) for name in nested])

    for b in dataset.to_batches(columns=fields + ordering_fields):
        if max_nested_level > 0:
            b = pa.Table.from_batches([b])
            for _ in range(max_nested_level - 1):
                b = b.flatten()
        for i in range(b.num_rows):
            doc = tantivy.Document()
            for name in fields:
                value = b[name][i].as_py()
                if value is not None:
                    doc.add_text(name, value)
            for name in ordering_fields:
                value = b[name][i].as_py()
                if value is not None:
                    doc.add_unsigned(name, value)
            if not doc.is_empty:
                doc.add_integer("doc_id", row_id)
                writer.add_document(doc)
            row_id += 1
    # commit changes
    writer.commit()
    return row_id


def resolve_path(schema, field_name: str) -> pa.Field:
    """
    Resolve a nested field path to a list of field names

    Parameters
    ----------
    field_name : str
        The field name to resolve

    Returns
    -------
    List[str]
        The resolved path
    """
    path = field_name.split(".")
    field = schema.field(path.pop(0))
    for segment in path:
        if pa.types.is_struct(field.type):
            field = field.type.field(segment)
        else:
            raise KeyError(f"field {field_name} not found in schema {schema}")
    return field


def search_index(
    index: tantivy.Index, query: str, limit: int = 10, ordering_field=None
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
    if ordering_field:
        results = searcher.search(query, limit, order_by_field=ordering_field)
    else:
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
