#!/usr/bin/env python3
"""Check that the ENT-3561 examples appear in rendered documentation text."""

import html
import pathlib
import re
import sys


def main() -> None:
    page = pathlib.Path(sys.argv[1]).read_text(encoding="utf-8")
    text = html.unescape(re.sub(r"<[^>]+>", "", page))
    required = [
        'normalize(" Hello ")',
        "read_range(6, 4)",
        "FunctionApplication",
    ]
    missing = [item for item in required if item not in text]
    if missing:
        raise SystemExit(f"rendered documentation is missing: {missing!r}")
    print("docs_render=checked")


if __name__ == "__main__":
    main()
