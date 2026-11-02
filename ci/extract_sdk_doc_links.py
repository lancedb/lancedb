#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
# SPDX-FileCopyrightText: Copyright The LanceDB Authors

"""Extract documentation links defined in rust/lancedb/src/docs.rs for link checking."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DOCS_RS = REPO_ROOT / "rust" / "lancedb" / "src" / "docs.rs"


def _parse_match_arm_map(match_arms: str) -> dict[str, str | None]:
    mapping: dict[str, str | None] = {}
    for raw_line in match_arms.splitlines():
        line = raw_line.strip().rstrip(",")
        if not line or line.startswith("//") or "=>" not in line:
            continue
        lhs, rhs = [part.strip() for part in line.split("=>", 1)]
        variants = [
            part.strip().removeprefix("Self::")
            for part in lhs.split("|")
            if part.strip()
        ]
        if rhs == "None":
            value: str | None = None
        else:
            str_match = re.search(r'"([^"]+)"', rhs)
            if not str_match:
                raise ValueError(f"unsupported match arm value: {rhs}")
            value = str_match.group(1)
        for variant in variants:
            mapping[variant] = value
    return mapping


def extract_doc_links(docs_rs_path: Path = DEFAULT_DOCS_RS) -> list[str]:
    source = docs_rs_path.read_text(encoding="utf-8")

    base_match = re.search(
        r'pub\s+const\s+DOCS_BASE_URL:\s*&str\s*=\s*"([^"]+)";', source
    )
    if not base_match:
        raise ValueError("DOCS_BASE_URL not found in docs.rs")
    base_url = base_match.group(1)

    all_match = re.search(
        r"pub\s+const\s+ALL:\s*&'static\s*\[Self\]\s*=\s*&\[(.*?)\];",
        source,
        re.DOTALL,
    )
    if not all_match:
        raise ValueError("DocLink::ALL not found in docs.rs")
    variants = re.findall(r"Self::([A-Za-z0-9_]+)", all_match.group(1))
    if not variants:
        raise ValueError("no variants found in DocLink::ALL")

    path_fn = re.search(
        r"pub\s+const\s+fn\s+path\(self\)\s*->\s*&'static\s+str\s*\{\s*match\s+self\s*\{(.*?)\}\s*\}",
        source,
        re.DOTALL,
    )
    anchor_fn = re.search(
        r"pub\s+const\s+fn\s+anchor\(self\)\s*->\s*Option<&'static\s+str>\s*\{\s*match\s+self\s*\{(.*?)\}\s*\}",
        source,
        re.DOTALL,
    )
    if not path_fn or not anchor_fn:
        raise ValueError("DocLink::path or DocLink::anchor not found in docs.rs")

    paths = _parse_match_arm_map(path_fn.group(1))
    anchors = _parse_match_arm_map(anchor_fn.group(1))

    urls: list[str] = []
    for variant in variants:
        path = paths.get(variant)
        if not path:
            raise ValueError(f"missing path mapping for DocLink::{variant}")
        anchor = anchors.get(variant)
        url = f"{base_url}{path}#{anchor}" if anchor else f"{base_url}{path}"
        if url not in urls:
            urls.append(url)
    return urls


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--docs-rs",
        type=Path,
        default=DEFAULT_DOCS_RS,
        help="Path to rust/lancedb/src/docs.rs",
    )
    parser.add_argument(
        "--output",
        type=Path,
        required=True,
        help="Markdown file to write extracted links to",
    )
    args = parser.parse_args(argv)

    urls = extract_doc_links(args.docs_rs)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    lines = ["# SDK documentation links", ""] + [f"- <{url}>" for url in urls] + [""]
    args.output.write_text("\n".join(lines), encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(main())
