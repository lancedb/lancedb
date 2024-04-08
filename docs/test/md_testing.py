import glob
from typing import Iterator
from pathlib import Path

glob_string = "../src/**/*.md"
excluded_globs = [
    "../src/fts.md",
    "../src/embedding.md",
    "../src/examples/*.md",
    "../src/integrations/voxel51.md",
    "../src/guides/tables.md",
    "../src/python/duckdb.md",
    "../src/embeddings/*.md",
    "../src/concepts/*.md",
    "../src/ann_indexes.md",
    "../src/basic.md",
    "../src/hybrid_search/hybrid_search.md",
    "../src/reranking/*.md",
]

python_prefix = "py"
python_file = ".py"
python_folder = "python"

files = glob.glob(glob_string, recursive=True)
excluded_files = [
    f
    for excluded_glob in excluded_globs
    for f in glob.glob(excluded_glob, recursive=True)
]


def yield_lines(lines: Iterator[str], prefix: str, suffix: str):
    in_code_block = False
    # Python code has strict indentation
    strip_length = 0
    skip_test = False
    for line in lines:
        if "skip-test" in line:
            skip_test = True
        if line.strip().startswith(prefix + python_prefix):
            in_code_block = True
            strip_length = len(line) - len(line.lstrip())
        elif in_code_block and line.strip().startswith(suffix):
            in_code_block = False
            if not skip_test:
                yield "\n"
            skip_test = False
        elif in_code_block:
            if not skip_test:
                yield line[strip_length:]


for file in filter(lambda file: file not in excluded_files, files):
    with open(file, "r") as f:
        lines = list(yield_lines(iter(f), "```", "```"))

    if len(lines) > 0:
        print(lines)
        out_path = (
            Path(python_folder)
            / Path(file).name.strip(".md")
            / (Path(file).name.strip(".md") + python_file)
        )
        print(out_path)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        with open(out_path, "w") as out:
            out.writelines(lines)
