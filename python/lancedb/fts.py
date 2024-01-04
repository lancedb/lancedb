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

from lance import LanceFragment
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
    schema_builder.add_integer_field("_rowid", stored=True)
    schema_builder.add_integer_field("_version", stored=True)
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
    # version
    version = table.version
    # first check the fields exist and are string or large string type
    max_nested_level = _get_max_nested_level(table.schema, fields)

    # create a tantivy writer
    writer = open_writer(index)
    # write data into index
    dataset = table.to_lance()

    for b in dataset.to_batches(columns=fields, with_row_id=True):
        _add_batch(b, writer, fields, version, max_nested_level)
    # commit changes
    writer.commit()
    return len(table)


def open_writer(index: tantivy.Index) -> "IndexWriter":
    writer_heap_size = os.environ.get("LANCEDB_FTS_HEAP_SIZE")
    if writer_heap_size is not None:
        writer_heap_size = int(writer_heap_size)
        writer = index.writer(heap_size=writer_heap_size)
    else:
        writer = index.writer(heap_size=1024 * 1024 * 1024)
    return writer


def _get_max_nested_level(schema: pa.Schema, fields: List[str]) -> int:
    """
    pyarrow table can't resolve nested references automatically
    so we need to check whether there are nested references and
    then flatten the table
    """
    nested = []
    for name in fields:
        try:
            f = schema.field(name)  # raises KeyError if not found
        except KeyError:
            f = resolve_path(schema, name)
            nested.append(name)

        if not pa.types.is_string(f.type) and not pa.types.is_large_string(f.type):
            raise TypeError(f"Field {name} is not a string type")

    max_nested_level = 0
    if len(nested) > 0:
        max_nested_level = max([len(name.split(".")) for name in nested])
    return max_nested_level


def _add_batch(
    b: pa.RecordBatch,
    writer: "IndexWriter",
    fields: List[str],
    version: int,
    max_nested_level: int,
):
    if max_nested_level > 0:
        b = pa.Table.from_batches([b])
        for _ in range(max_nested_level - 1):
            b = b.flatten()
    for i in range(b.num_rows):
        doc = tantivy.Document()
        rowid = b["_rowid"][i].as_py()
        doc.add_integer("_rowid", rowid)
        doc.add_integer("_version", version)
        for name in fields:
            doc.add_text(name, b[name][i].as_py())
        writer.add_document(doc)


def update_index(index_path: str, fragments: List[LanceFragment], version: int) -> int:
    """
    Update an existing index with new data
    """
    if len(fragments) == 0:
        return 0
    # Open the index
    index = tantivy.Index.open(index_path)
    # Filter the schema and exclude _rowid and _version fields
    # tantivy doesn't expose the schema field names
    # fields = [f for f in schema.field_names() if f not in ["_rowid", "_version"]]
    searcher = index.searcher()
    query = index.parse_query("*")
    batch = searcher.search(query, limit=1)
    fields = []
    for _, doc_address in batch.hits:
        doc = searcher.doc(doc_address)
        fields = [f for f in doc.to_dict().keys() if f not in ["_rowid", "_version"]]
    max_nested_level = _get_max_nested_level(fragments[0].schema, fields)

    # Read each batch from each fragment and add to the index
    writer = open_writer(index)
    count = 0
    for fragment in fragments:
        for b in fragment.to_batches(columns=fields, with_row_id=True):
            _add_batch(b, writer, fields, version, max_nested_level)
            count += b.num_rows
    # commit changes
    writer.commit()
    return count


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
    index: tantivy.Index, query: str, limit: int = 10, version: int = None
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
    batch = searcher.search(query, limit=limit)
    if batch.count == 0:
        return ((), ())
    rowids, scores = [], []
    for score, doc_address in batch.hits:
        rowids.append(searcher.doc(doc_address)["_rowid"][0])
        scores.append(score)
    return (tuple(rowids), tuple(scores))
