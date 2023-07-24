import glob
from typing import Iterator
from pathlib import Path

excluded_files = [
    "../src/fts.md",
    "../src/embedding.md",
    "../src/examples/serverless_lancedb_with_s3_and_lambda.md",
    "../src/examples/serverless_qa_bot_with_modal_and_langchain.md",
    "../src/examples/youtube_transcript_bot_with_nodejs.md"
    "../src/integrations/voxel51.md",
]

python_prefix = "py"
python_file = ".py"
python_folder = "python"
glob_string = "../src/**/*.md"

def yield_lines(lines: Iterator[str], prefix: str, suffix: str):
    in_code_block = False
    # Python code has strict indentation
    strip_length = 0
    for line in lines:
        if line.strip().startswith(prefix + python_prefix):
            in_code_block = True
            strip_length = len(line) - len(line.lstrip())
        elif in_code_block and line.strip().startswith(suffix):
            in_code_block = False
            yield "\n"
        elif in_code_block:
            yield line[strip_length:]

for file in filter(lambda file: file not in excluded_files, glob.glob(glob_string, recursive=True)):
    with open(file, "r") as f:
        lines = list(yield_lines(iter(f), "```", "```"))

    if len(lines) > 0:
        out_path = Path(python_folder) / Path(file).name.strip(".md") / (Path(file).name.strip(".md") + python_file)
        print(out_path)
        out_path.parent.mkdir(exist_ok=True, parents=True)
        with open(out_path, "w") as out:
            out.writelines(lines)
