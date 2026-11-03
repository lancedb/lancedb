// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:vscode_imports]
import * as path from "node:path";
import * as lancedb from "@lancedb/lancedb";
import "@lancedb/lancedb/embedding/transformers";
import { Int32, Utf8 } from "apache-arrow";
// --8<-- [end:vscode_imports]

// --8<-- [start:vscode_chunk]
export type Chunk = { path: string; line: number; text: string };

// Split a Markdown file into paragraphs, keeping the line each one starts on
// so a search result can jump straight to it.
export function chunkMarkdown(filePath: string, content: string): Chunk[] {
  const lines = content.split(/\r?\n/);
  const chunks: Chunk[] = [];
  let start = 0;
  for (let i = 0; i <= lines.length; i++) {
    if (i === lines.length || lines[i].trim() === "") {
      const text = lines.slice(start, i).join("\n").trim();
      if (text) {
        chunks.push({ path: filePath, line: start, text });
      }
      start = i + 1;
    }
  }
  return chunks;
}
// --8<-- [end:vscode_chunk]

// --8<-- [start:vscode_connect]
// Pass an absolute directory, such as `context.storageUri.fsPath`. A relative
// path resolves against the extension host's working directory, which is not
// your workspace and may be read-only (on macOS it is usually `/`).
export async function openDatabase(storageDir: string) {
  return lancedb.connect(path.join(storageDir, "lancedb"));
}
// --8<-- [end:vscode_connect]

// --8<-- [start:vscode_index]
export async function indexChunks(db: lancedb.Connection, chunks: Chunk[]) {
  // Runs all-MiniLM-L6-v2 locally through Transformers.js: no API key needed.
  const embedder = (await lancedb.embedding
    .getRegistry()
    .get("huggingface")
    ?.create()) as lancedb.embedding.EmbeddingFunction;

  const schema = lancedb.embedding.LanceSchema({
    path: new Utf8(),
    line: new Int32(),
    text: embedder.sourceField(new Utf8()),
    vector: embedder.vectorField(),
  });

  // LanceDB fills in the `vector` column from `text` as the rows are written.
  return db.createTable("chunks", chunks, { schema, mode: "overwrite" });
}
// --8<-- [end:vscode_index]

// --8<-- [start:vscode_search]
export async function searchChunks(
  db: lancedb.Connection,
  query: string,
  limit = 5,
) {
  // The table stores its embedding function in its metadata, so a text query
  // is embedded with the same model that embedded the rows.
  const table = await db.openTable("chunks");
  return table
    .search(query)
    .select(["path", "line", "text", "_distance"])
    .limit(limit)
    .toArray();
}
// --8<-- [end:vscode_search]
