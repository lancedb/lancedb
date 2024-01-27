#!/usr/bin/env python3
#
# Generating tests from docs/

"""Generating tests from docs Markdown files.

It considers a block starts with three backticks (`).

After the three backticks, you can speicfy languages, "py","typescript","rust".
Additionally, you can offer options:

  - ignore -- ignore this in test


For example

``` { .typescript .ignore}
// this block will not be tested, but rendered correctly in doc site.
```
"""

import argparse
import glob
from typing import Generator
import re

INCLUDES = ["../src/**/*.md", "../src/*.md"]
EXCLUDES = [
    "../src/fts.md",
    "../src/javascript/**/*.md",
    "../src/embedding.md",
    "../src/examples/*.md",
    "../src/guides/tables.md",
    "../src/embeddings/*.md",
]


def generate_py_tests():
    pass


def yield_code_blocks(file_path, types) -> Generator[str, None, None]:
    in_code_block = False
    ignored = False
    with open(file_path, "r") as f:
        for line in f:
            if re.match(r"\s*```.*", line) and any([t in line for t in types]):
                # print("Begin line: ", line)
                in_code_block = True
                ignored = ".ignore" in line
            elif in_code_block and re.match(r"\S*```.*", line):
                in_code_block = False
                ignored = False
            elif in_code_block and not ignored:
                yield line


def generate_typescript_tests(args):
    markdown_files = args.file or find_markdown_files()
    for pattern in markdown_files:
        files = glob.glob(pattern, recursive=True)
        for md_file in files:
            code_block = list(
                yield_code_blocks(md_file, ["typescript", "javascript", "ts"])
            )
            # print(md_file, " has block: ", len(code_block) > 0)
            if not code_block:
                continue
            print(md_file)
            # print("".join(code_block))


def find_markdown_files():
    included = set()
    for pattern in INCLUDES:
        included |= set(glob.glob(pattern, recursive=True))
    excluded = set()
    for pattern in EXCLUDES:
        excluded |= set(glob.glob(pattern, recursive=True))
    return included - excluded


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-t",
        "--type",
        help="language type",
        choices=["py", "typescript", "rust"],
        default="py",
    )
    parser.add_argument("file", default=None, nargs="*")
    args = parser.parse_args()

    match args.type:
        case "py":
            generate_py_tests()
        case "typescript":
            generate_typescript_tests(args)
        case "rust":
            pass
        case _:
            raise ValueError("Invalid file type")


if __name__ == "__main__":
    main()
