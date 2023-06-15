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
languages = ["py", "javascript"]
glob_string = "../src/**/*.md"

def yield_lines(lines: Iterator[str], prefix: str, suffix: str, languages: list):
    current_language = {language: False for language in languages}
    for line in lines:
        for language in languages:
            if line.strip().startswith(prefix + language):
                current_language[language] = True
            elif current_language[language] and line.strip().startswith(suffix):
                current_language[language] = False
                yield ("\n", language)
            elif current_language[language]:
                yield (line, language)

def create_code_files(prefix: str, suffix: str, file_ending: str = ""):
    for file in filter(lambda file: file not in excluded_files, glob.glob(glob_string, recursive=True)):
        with open(file, "r") as f:
            lines = list(yield_lines(iter(f), prefix, suffix, languages))
            python_lines = [line[0] for line in lines if line[1] == "py"]
            node_lines = [line[0] for line in lines if line[1] == "javascript"]

        if len(python_lines) > 0:
            python_out_path = Path("python") / Path(file).name.strip(".md") / (Path(file).name.strip(".md") + file_ending + ".py")
            python_out_path.parent.mkdir(exist_ok=True, parents=True)
            with open(python_out_path, "w") as python_out:
                python_out.writelines(python_lines)

        if len(node_lines) > 0:
            node_out_path = Path("node") / Path(file).name.strip(".md") / (Path(file).name.strip(".md") + file_ending + ".js")
            node_out_path.parent.mkdir(exist_ok=True, parents=True)
            with open(node_out_path, "w") as node_out:
                node_out.write("(async () => {\n")
                node_out.writelines(node_lines)
                node_out.write("})();")

# Setup doc code
create_code_files("<!--", "-->", "-setup")

# Actual doc code
create_code_files("```", "```")
