import glob
from typing import Iterator
from pathlib import Path

excluded_files = [
    "../src/fts.md",
    "../src/embedding.md",
    "../src/examples/serverless_lancedb_with_s3_and_lambda.md",
    "../src/examples/serverless_qa_bot_with_modal_and_langchain.md",
    "../src/examples/youtube_transcript_bot_with_nodejs.md"
]

python_prefix = "py"
python_file = ".py"
python_folder = "python"
glob_string = "../src/**/*.md"

def yield_lines(lines: Iterator[str], prefix: str, suffix: str):
    in_code_block = False
    for line in lines:
        if line.strip().startswith(prefix + python_prefix):
            in_code_block = True
        elif in_code_block and line.strip().startswith(suffix):
            in_code_block = False
            yield "\n"
        elif in_code_block:
            yield line

def create_code_files(prefix: str, suffix: str, file_ending: str = ""):
    for file in filter(lambda file: file not in excluded_files, glob.glob(glob_string, recursive=True)):
        with open(file, "r") as f:
            lines = list(yield_lines(iter(f), prefix, suffix))

        if len(lines) > 0:
            out_path = Path(python_folder) / Path(file).name.strip(".md") / (Path(file).name.strip(".md") + file_ending + python_file)
            print(out_path)
            out_path.parent.mkdir(exist_ok=True, parents=True)
            with open(out_path, "w") as out:
                out.writelines(lines)

# Setup doc code
create_code_files("<!--", "-->", "-setup")

# Actual doc code
create_code_files("```", "```")
