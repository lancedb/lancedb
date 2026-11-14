# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from ci.extract_sdk_doc_links import extract_doc_links, main


class TestExtractSdkDocLinks(unittest.TestCase):
    def test_extracts_all_links_from_repo_docs_rs(self) -> None:
        urls = extract_doc_links()
        self.assertIn(
            "https://docs.lancedb.com/maintenance/compaction#when",
            urls,
        )
        self.assertIn(
            "https://docs.lancedb.com/indexing/reindexing#incremental-reindexing",
            urls,
        )
        self.assertIn(
            "https://docs.lancedb.com/search/sql",
            urls,
        )
        self.assertIn(
            "https://docs.lancedb.com/basics/embedded-vs-enterprise",
            urls,
        )

    def test_cli_writes_markdown(self) -> None:
        with TemporaryDirectory() as tmp:
            out = Path(tmp) / "sdk-links.md"
            rc = main(["--output", str(out)])
            self.assertEqual(rc, 0)
            content = out.read_text(encoding="utf-8")
            self.assertIn(
                "<https://docs.lancedb.com/maintenance/compaction#when>",
                content,
            )


if __name__ == "__main__":
    unittest.main()
