#!/usr/bin/env python3
"""Scan Python and TypeScript for likely unsafe LanceDB materialization."""

from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass
from pathlib import Path


PY_FULL_TABLE = re.compile(r"\b\w+\.(to_pandas|to_arrow|to_polars)\s*\(")
TS_TABLE_TO_ARROW = re.compile(r"\b\w+\.toArrow\s*\(")
TS_QUERY_COLLECTOR = re.compile(r"\.query\s*\(\s*\)[\s\S]*?\.to(Array|Arrow)\s*\(")


@dataclass
class Finding:
    path: Path
    line: int
    message: str
    text: str


def iter_files(paths: list[Path]) -> list[Path]:
    files: list[Path] = []
    for path in paths:
        if path.is_dir():
            files.extend(
                p
                for p in path.rglob("*")
                if p.suffix in {".py", ".ts", ".tsx"} and "node_modules" not in p.parts
            )
        elif path.suffix in {".py", ".ts", ".tsx"}:
            files.append(path)
    return sorted(set(files))


def line_number(text: str, offset: int) -> int:
    return text.count("\n", 0, offset) + 1


def scan_python(path: Path, text: str) -> list[Finding]:
    findings: list[Finding] = []
    for match in PY_FULL_TABLE.finditer(text):
        line_start = text.rfind("\n", 0, match.start()) + 1
        line_end = text.find("\n", match.start())
        if line_end == -1:
            line_end = len(text)
        line = text[line_start:line_end].strip()
        if ".search(" in line or ".query(" in line:
            continue
        findings.append(
            Finding(
                path,
                line_number(text, match.start()),
                f"Review Python `{match.group(1)}()` call; table-level materialization is not portable to remote tables.",
                line,
            )
        )
    return findings


def statement_around(text: str, start: int, end: int) -> str:
    before = max(text.rfind(";", 0, start), text.rfind("\n\n", 0, start))
    after_candidates = [
        pos for pos in (text.find(";", end), text.find("\n\n", end)) if pos != -1
    ]
    after = min(after_candidates) if after_candidates else len(text)
    return text[before + 1 : after].strip()


def scan_typescript(path: Path, text: str) -> list[Finding]:
    findings: list[Finding] = []
    for match in TS_TABLE_TO_ARROW.finditer(text):
        stmt = statement_around(text, match.start(), match.end())
        if ".query(" in stmt or ".search(" in stmt:
            continue
        findings.append(
            Finding(
                path,
                line_number(text, match.start()),
                "Review TypeScript `table.toArrow()`-style call; table-level materialization is not portable for large/remote tables.",
                stmt.splitlines()[0].strip(),
            )
        )

    for match in TS_QUERY_COLLECTOR.finditer(text):
        stmt = statement_around(text, match.start(), match.end())
        if ".limit(" in stmt:
            continue
        findings.append(
            Finding(
                path,
                line_number(text, match.start()),
                "Review unbounded TypeScript query collection; add `limit()` or stream batches.",
                stmt.splitlines()[0].strip(),
            )
        )
    return findings


def scan_file(path: Path) -> list[Finding]:
    text = path.read_text(encoding="utf-8", errors="replace")
    if path.suffix == ".py":
        return scan_python(path, text)
    if path.suffix in {".ts", ".tsx"}:
        return scan_typescript(path, text)
    return []


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("paths", nargs="+", type=Path)
    parser.add_argument(
        "--no-fail", action="store_true", help="Always exit 0 after reporting findings."
    )
    args = parser.parse_args()

    findings: list[Finding] = []
    for path in iter_files(args.paths):
        findings.extend(scan_file(path))

    for finding in findings:
        print(f"{finding.path}:{finding.line}: {finding.message}")
        print(f"  {finding.text}")

    if findings:
        print(
            f"\n{len(findings)} finding(s). Review manually; bounded query result conversion may be OK."
        )
    return 0 if args.no_fail or not findings else 1


if __name__ == "__main__":
    sys.exit(main())
