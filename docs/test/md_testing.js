const glob = require("glob");
const fs = require("fs");
const path = require("path");

const excludedFiles = [
  "../src/fts.md",
  "../src/embedding.md",
  "../src/examples/serverless_lancedb_with_s3_and_lambda.md",
  "../src/examples/serverless_qa_bot_with_modal_and_langchain.md",
  "../src/examples/youtube_transcript_bot_with_nodejs.md",
];
const nodePrefix = "javascript";
const nodeFile = ".js";
const nodeFolder = "node";
const globString = "../src/**/*.md";
const asyncPrefix = "(async () => {\n";
const asyncSuffix = "})();";

function* yieldLines(lines, prefix, suffix) {
  let inCodeBlock = false;
  for (const line of lines) {
    if (line.trim().startsWith(prefix + nodePrefix)) {
      inCodeBlock = true;
    } else if (inCodeBlock && line.trim().startsWith(suffix)) {
      inCodeBlock = false;
      yield "\n";
    } else if (inCodeBlock) {
      yield line;
    }
  }
}

function createCodeFiles(prefix, suffix, fileEnding = "") {
  const files = glob.sync(globString, { recursive: true });

  for (const file of files.filter((file) => !excludedFiles.includes(file))) {
    const lines = [];
    const data = fs.readFileSync(file, "utf-8");
    const fileLines = data.split("\n");

    for (const line of yieldLines(fileLines, prefix, suffix)) {
      lines.push(line);
    }

    if (lines.length > 0) {
      const fileName = path.basename(file, ".md");
      const fileExtension = fileEnding === "" ? nodeFile : fileEnding + nodeFile;
      const outPath = path.join(nodeFolder, fileName, `${fileName}${fileExtension}`);
      console.log(outPath)
      fs.mkdirSync(path.dirname(outPath), { recursive: true });
      fs.writeFileSync(outPath, asyncPrefix + "\n" + lines.join("\n") + asyncSuffix);
    }
  }
}

// Setup doc code
// Some documentation pages have certain assumptions such as a created database or a table with vectors.
// We can create code files with <!--[language] --> tag to set up the actual documentation:
createCodeFiles("<!--", "-->", "-setup");

// Actual doc code
createCodeFiles("```", "```");
