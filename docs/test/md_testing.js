const glob = require("glob");
const fs = require("fs");
const path = require("path");

const globString = "../src/**/*.md";

const excludedGlobs = [
  "../src/fts.md",
  "../src/embedding.md",
  "../src/examples/*.md",
  "../src/guides/tables.md",
  "../src/guides/storage.md",
  "../src/embeddings/*.md",
];

const nodePrefix = "javascript";
const nodeFile = ".js";
const nodeFolder = "node";
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

const files = glob.sync(globString, { recursive: true });
const excludedFiles = glob.sync(excludedGlobs, { recursive: true });

for (const file of files.filter((file) => !excludedFiles.includes(file))) {
  const lines = [];
  const data = fs.readFileSync(file, "utf-8");
  const fileLines = data.split("\n");

  for (const line of yieldLines(fileLines, "```", "```")) {
    lines.push(line);
  }

  if (lines.length > 0) {
    const fileName = path.basename(file, ".md");
    const outPath = path.join(nodeFolder, fileName, `${fileName}${nodeFile}`);
    console.log(outPath)
    fs.mkdirSync(path.dirname(outPath), { recursive: true });
    fs.writeFileSync(outPath, asyncPrefix + "\n" + lines.join("\n") + asyncSuffix);
  }
}