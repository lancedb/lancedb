# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

import os

import pytest


def require_env(var_name: str) -> str:
    """Skip the test unless the required environment variable is provided."""
    value = os.environ.get(var_name)
    if not value:
        pytest.skip(f"Set {var_name} to run this integration snippet")
    return value


def test_firecrawl_crawl_and_search() -> None:
    require_env("FIRECRAWL_API_KEY")
    pytest.importorskip("firecrawl")
    pytest.importorskip("sentence_transformers")

    # --8<-- [start:firecrawl_crawl]
    import os

    from firecrawl import Firecrawl

    firecrawl = Firecrawl(api_key=os.environ["FIRECRAWL_API_KEY"])

    crawl = firecrawl.crawl(
        "https://docs.lancedb.com/enterprise/v1-14-0/integrations",
        limit=10,
        scrape_options={"formats": ["markdown"], "only_main_content": True},
        integration="_lancedb",
    )
    pages = [page for page in crawl.data if page.markdown]
    print(f"Crawled {len(pages)} pages")
    # --8<-- [end:firecrawl_crawl]

    assert pages

    # --8<-- [start:firecrawl_chunk]
    def chunk_markdown(markdown: str, max_chars: int = 1000) -> list[str]:
        chunks, current = [], ""
        for paragraph in markdown.split("\n\n"):
            if current and len(current) + len(paragraph) > max_chars:
                chunks.append(current.strip())
                current = ""
            current += paragraph + "\n\n"
        if current.strip():
            chunks.append(current.strip())
        return chunks

    # --8<-- [end:firecrawl_chunk]

    # --8<-- [start:firecrawl_table]
    import tempfile

    import lancedb
    from lancedb.embeddings import get_registry
    from lancedb.pydantic import LanceModel, Vector

    embedder = (
        get_registry().get("sentence-transformers").create(name="all-MiniLM-L6-v2")
    )

    class WebChunk(LanceModel):
        text: str = embedder.SourceField()
        vector: Vector(embedder.ndims()) = embedder.VectorField()
        source_url: str
        title: str

    rows = [
        {
            "text": chunk,
            "source_url": page.metadata.source_url,
            "title": page.metadata.title or "",
        }
        for page in pages
        for chunk in chunk_markdown(page.markdown)
    ]

    db = lancedb.connect(tempfile.mkdtemp(prefix="lancedb-firecrawl-"))
    table = db.create_table("firecrawl_docs", schema=WebChunk)
    table.add(rows)
    print(f"Added {table.count_rows()} chunks")
    # --8<-- [end:firecrawl_table]

    assert table.count_rows() == len(rows) > 0

    # --8<-- [start:firecrawl_query]
    results = (
        table.search("How do I query a LanceDB table with SQL?")
        .select(["title", "source_url", "text", "_distance"])
        .limit(3)
        .to_list()
    )
    for row in results:
        print(f"{row['_distance']:.3f}", row["source_url"])
    # --8<-- [end:firecrawl_query]

    assert len(results) == 3
    assert all(row["source_url"].startswith("https://") for row in results)

    # --8<-- [start:firecrawl_web_search]
    search = firecrawl.search(
        "lancedb hybrid search reranking",
        limit=5,
        scrape_options={"formats": ["markdown"], "only_main_content": True},
        integration="_lancedb",
    )
    table.add(
        [
            {
                "text": chunk,
                "source_url": result.metadata.source_url,
                "title": result.metadata.title or "",
            }
            for result in search.web or []
            if getattr(result, "markdown", None)
            for chunk in chunk_markdown(result.markdown)
        ]
    )
    print(f"Table now holds {table.count_rows()} chunks")
    # --8<-- [end:firecrawl_web_search]

    assert table.count_rows() > len(rows)


def test_firecrawl_search_with_mixed_results(monkeypatch: pytest.MonkeyPatch) -> None:
    """Run the examples on canned responses where search mixes plain and scraped results."""
    firecrawl = pytest.importorskip("firecrawl")
    pytest.importorskip("sentence_transformers")
    from firecrawl.v2.types import (
        CrawlJob,
        Document,
        DocumentMetadata,
        SearchData,
        SearchResultWeb,
    )

    def page(url: str, title: str, markdown: str) -> Document:
        return Document(
            markdown=markdown,
            metadata=DocumentMetadata(source_url=url, title=title),
        )

    class FakeFirecrawl:
        def __init__(self, api_key: str) -> None:
            pass

        def crawl(self, url: str, **kwargs) -> CrawlJob:
            return CrawlJob(
                status="completed",
                total=3,
                completed=3,
                data=[
                    page(
                        f"https://docs.lancedb.com/{slug}",
                        slug.title(),
                        f"# {slug.title()}\n\nLanceDB {slug} guide.",
                    )
                    for slug in ("sql", "search", "indexing")
                ],
            )

        def search(self, query: str, **kwargs) -> SearchData:
            return SearchData(
                web=[
                    SearchResultWeb(
                        url="https://example.com/unscraped",
                        title="Unscraped result",
                    ),
                    page(
                        "https://example.com/hybrid-search",
                        "Hybrid search",
                        "# Hybrid search\n\nReranking LanceDB results.",
                    ),
                ]
            )

    import lancedb

    connections = []
    connect = lancedb.connect

    def recording_connect(*args, **kwargs):
        connections.append(connect(*args, **kwargs))
        return connections[-1]

    monkeypatch.setenv("FIRECRAWL_API_KEY", "fc-test")
    monkeypatch.setattr(firecrawl, "Firecrawl", FakeFirecrawl)
    monkeypatch.setattr(lancedb, "connect", recording_connect)

    test_firecrawl_crawl_and_search()

    source_urls = (
        connections[0].open_table("firecrawl_docs").to_pandas()["source_url"].tolist()
    )
    assert len(source_urls) == 4
    assert source_urls.count("https://example.com/hybrid-search") == 1
    assert "https://example.com/unscraped" not in source_urls
