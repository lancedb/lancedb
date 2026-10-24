"""
Generate MDX-ready snippet modules that export raw string constants.

The generator groups snippets by the originating test file name rather than by
language. For a source file like `tests/py/test_basic_usage.py`, the snippets are
written under `docs/snippets/basic.mdx`. Each exported constant is prefixed with
a language identifier (Py|Ts|Rs) followed by the snippet name converted to
TitleCase so that docs authors can selectively assemble `<CodeGroup>` blocks.
"""

from __future__ import annotations

import argparse
import json
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, Iterator, List, Mapping, MutableMapping, Tuple

# Supported languages and relevant metadata.
LANG_ORDER = ("py", "ts", "rs")
LANG_PREFIX = {"py": "Py", "ts": "Ts", "rs": "Rs"}
LANG_EXTENSIONS = {
    ".py": "py",
    ".ts": "ts",
    ".tsx": "ts",
    ".rs": "rs",
}
LANG_MARKERS = {"py": ["#"], "ts": ["//"], "rs": ["//"]}
# Match each export block; we rely on the blank line we emit after each export to
# avoid accidentally truncating on semicolons inside the JSON string literal.
EXPORT_RE = re.compile(r"export const (\w+)\s*=\s*(.+?);\s*\n(?:\s*\n|$)", re.DOTALL)

DEFAULT_SOURCE_DIRS = (
    Path("tests") / "py",
    Path("tests") / "ts",
    Path("tests") / "rs",
)


@dataclass(frozen=True)
class SnippetRecord:
    lang: str
    snippet_name: str
    export_name: str
    text: str
    source_rel: str


def build_markers(
    comment_markers: Iterable[str],
) -> Tuple[re.Pattern[str], re.Pattern[str]]:
    escaped = "|".join(re.escape(c) for c in comment_markers)
    start_re = re.compile(rf"^\s*(?:{escaped})\s*--8<--\s*\[start:([^\]]+)\]")
    end_re = re.compile(rf"^\s*(?:{escaped})\s*--8<--\s*\[end:([^\]]+)\]")
    return start_re, end_re


def parse_snippets(
    lines: List[str], start_re: re.Pattern[str], end_re: re.Pattern[str]
) -> Dict[str, List[str]]:
    snippets: Dict[str, List[str]] = {}
    stack: List[Tuple[str, int]] = []

    for idx, line in enumerate(lines):
        m_start = start_re.match(line)
        if m_start:
            name = m_start.group(1).strip()
            stack.append((name, idx))
            continue

        m_end = end_re.match(line)
        if m_end:
            name = m_end.group(1).strip()
            if not stack:
                raise ValueError(
                    f"End marker for '{name}' found at line {idx + 1} without matching start"
                )
            start_name, start_idx = stack.pop()
            if start_name != name:
                raise ValueError(
                    f"Mismatched markers: start '{start_name}' at line {start_idx + 1}, "
                    f"end '{name}' at line {idx + 1}"
                )
            snippets[name] = lines[start_idx + 1 : idx]
            continue

    if stack:
        open_names = ", ".join(f"{n}@{i + 1}" for n, i in stack)
        raise ValueError(f"Unclosed snippet markers for: {open_names}")

    return snippets


def dedent(lines: List[str]) -> List[str]:
    min_indent = None
    for ln in lines:
        if ln.strip() == "":
            continue
        expanded = ln.expandtabs(4)
        indent = len(expanded) - len(expanded.lstrip(" "))
        if min_indent is None or indent < min_indent:
            min_indent = indent

    if not lines:
        return []
    if min_indent in (None, 0):
        return [ln.rstrip("\n") for ln in lines]

    dedented: List[str] = []
    for ln in lines:
        expanded = ln.expandtabs(4)
        if expanded.strip() == "":
            dedented.append(expanded.rstrip("\n"))
        else:
            dedented.append(expanded[min_indent:].rstrip("\n"))
    return dedented


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def detect_language(file_path: Path) -> str:
    lang = LANG_EXTENSIONS.get(file_path.suffix.lower())
    if not lang:
        raise ValueError(f"Unsupported file extension for {file_path}")
    return lang


def markers_for_lang(lang: str) -> Tuple[re.Pattern[str], re.Pattern[str]]:
    markers = LANG_MARKERS.get(lang)
    if not markers:
        raise ValueError(f"Unsupported language for markers: {lang}")
    return build_markers(markers)


def normalize_target_name(file_path: Path) -> str:
    stem = file_path.stem
    if stem.startswith("test_"):
        stem = stem[len("test_") :]
    if stem.endswith("_test"):
        stem = stem[: -len("_test")]
    if stem.endswith(".test"):
        stem = stem[: -len(".test")]
    if stem.endswith(".spec"):
        stem = stem[: -len(".spec")]
    if stem == "":
        stem = file_path.stem
    return stem.replace(" ", "_")


def to_title_case(name: str) -> str:
    parts = re.split(r"[^0-9a-zA-Z]+", name)
    filtered = [p for p in parts if p]
    return "".join(p[:1].upper() + p[1:].lower() for p in filtered) or "Snippet"


def format_export_name(lang: str, snippet_name: str) -> str:
    prefix = LANG_PREFIX[lang]
    return f"{prefix}{to_title_case(snippet_name)}"


def snippet_text(lines: List[str]) -> str:
    if not lines:
        return ""
    joined = "\n".join(lines).rstrip("\n")
    return (joined + "\n") if joined else ""


def to_js_literal(text: str) -> str:
    return json.dumps(text, ensure_ascii=False)


def split_export_prefix(export_name: str) -> Tuple[str | None, str]:
    for lang, prefix in LANG_PREFIX.items():
        if export_name.startswith(prefix):
            suffix = export_name[len(prefix) :]
            return lang, suffix or export_name
    return None, export_name


def parse_existing_module(path: Path) -> Mapping[str, Mapping[str, SnippetRecord]]:
    try:
        text = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}

    existing: Dict[str, Dict[str, SnippetRecord]] = {}
    for match in EXPORT_RE.finditer(text):
        export_name, literal = match.group(1), match.group(2).strip()
        lang, snippet_suffix = split_export_prefix(export_name)
        if not lang:
            continue
        try:
            decoded = json.loads(literal)
        except Exception:
            decoded = literal.strip().strip(";")

        record = SnippetRecord(
            lang=lang,
            snippet_name=snippet_suffix or export_name,
            export_name=export_name,
            text=decoded,
            source_rel="(existing module)",
        )
        existing.setdefault(lang, {})[record.snippet_name] = record

    return existing


def iter_source_files(source_dirs: Iterable[Path]) -> Iterator[Path]:
    for root in source_dirs:
        for path in root.rglob("*"):
            if path.is_file() and path.suffix.lower() in LANG_EXTENSIONS:
                yield path


def write_if_changed(path: Path, content: str) -> bool:
    if path.exists():
        try:
            current = path.read_text(encoding="utf-8")
            if current == content:
                return False
        except Exception:
            pass
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")
    return True


def collect_snippets(
    source_dirs: Iterable[Path],
) -> Mapping[str, Mapping[str, Mapping[str, SnippetRecord]]]:
    result: Dict[str, Dict[str, Dict[str, SnippetRecord]]] = {}
    for file_path in iter_source_files(source_dirs):
        lang = detect_language(file_path)
        start_re, end_re = markers_for_lang(lang)
        try:
            text = file_path.read_text(encoding="utf-8")
        except Exception as exc:  # pragma: no cover
            raise RuntimeError(f"Failed to read {file_path}: {exc}") from exc
        lines = text.splitlines(keepends=True)
        snippets = parse_snippets(lines, start_re, end_re)
        if not snippets:
            continue

        target = normalize_target_name(file_path)
        lang_map = result.setdefault(target, {}).setdefault(lang, {})
        for snippet_name, content_lines in snippets.items():
            if snippet_name in lang_map:
                raise ValueError(
                    f"Duplicate snippet '{snippet_name}' in {file_path} and {lang_map[snippet_name].source_rel}"
                )
            dedented = dedent(content_lines)
            export_name = format_export_name(lang, snippet_name)
            source_rel = os.path.relpath(file_path, Path.cwd())
            lang_map[snippet_name] = SnippetRecord(
                lang=lang,
                snippet_name=snippet_name,
                export_name=export_name,
                text=snippet_text(dedented),
                source_rel=source_rel,
            )
    return result


def render_module(
    target: str, lang_map: Mapping[str, Mapping[str, SnippetRecord]]
) -> str:
    parts: List[str] = []
    parts.append(
        "{/* Auto-generated by scripts/mdx_snippets_gen.py. Do not edit manually. */}\n\n"
    )
    for lang in LANG_ORDER:
        snippets = lang_map.get(lang)
        if not snippets:
            continue
        for snippet_name in sorted(snippets.keys()):
            record = snippets[snippet_name]
            literal = to_js_literal(record.text)
            parts.append(f"export const {record.export_name} = {literal};\n\n")
    return "".join(parts)


def generate_modules(
    snippets_by_target: Mapping[str, Mapping[str, Mapping[str, SnippetRecord]]],
    output_root: Path,
) -> None:
    modules_written = 0
    total_exports = 0

    for target in sorted(snippets_by_target.keys()):
        module_path = output_root / f"{target}.mdx"
        lang_map = snippets_by_target[target]
        existing_langs = parse_existing_module(module_path)
        for lang, snippets in existing_langs.items():
            if lang in lang_map:
                continue
            print(module_path, " -- ", lang)
            merged_lang = lang_map.setdefault(lang, {})
            existing_export_names = {rec.export_name for rec in merged_lang.values()}
            for snippet_name, record in snippets.items():
                # If an export with the same name already exists (e.g., regenerated),
                # prefer the newly generated snippet and skip the old one to avoid duplicates.
                if record.export_name in existing_export_names:
                    continue
                merged_lang.setdefault(snippet_name, record)

        total_exports += sum(len(snippets) for snippets in lang_map.values())
        module_content = render_module(target, lang_map)
        if write_if_changed(module_path, module_content):
            modules_written += 1
    print(
        f"\nGenerated {total_exports} snippets\n---"
    )


def resolve_source_dirs(args_dirs: List[str] | None) -> List[Path]:
    if args_dirs:
        dirs = [Path(p) for p in args_dirs]
    else:
        dirs = list(DEFAULT_SOURCE_DIRS)

    missing = [str(p) for p in dirs if not p.exists()]
    if missing:
        raise FileNotFoundError(f"Source directories not found: {', '.join(missing)}")
    return dirs


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate MDX snippet modules grouped by test filename."
    )
    parser.add_argument(
        "-s",
        "--source-dir",
        action="append",
        help="Path(s) to language test directories (default: tests/py, ts, rs)",
    )
    parser.add_argument(
        "-o",
        "--output-dir",
        default=str(Path("docs") / "snippets"),
        help="Directory where snippet modules are written (default: docs/snippets)",
    )
    args = parser.parse_args()

    source_dirs = resolve_source_dirs(args.source_dir)
    snippets = collect_snippets(source_dirs)
    if not snippets:
        print("No snippets found.")
        return 0

    output_root = Path(args.output_dir)
    generate_modules(snippets, output_root)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
