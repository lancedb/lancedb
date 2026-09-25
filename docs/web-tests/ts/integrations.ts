// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// --8<-- [start:frameworks_genkit_usage]
import { lancedbIndexerRef, lancedb, lancedbRetrieverRef, WriteMode } from "genkitx-lancedb";
import { textEmbedding004, vertexAI } from "@genkit-ai/vertexai";
import { gemini } from "@genkit-ai/vertexai";
import { z, genkit } from "genkit";
import { Document } from "genkit/retriever";
import { chunk } from "llm-chunk";
import { readFile } from "fs/promises";
import path from "path";
import pdf from "pdf-parse/lib/pdf-parse";

const ai = genkit({
  plugins: [
    // vertexAI provides the textEmbedding004 embedder
    vertexAI(),

    // the local vector store requires an embedder to translate from text to vector
    lancedb([
      {
        dbUri: ".db", // optional lancedb uri, default to .db
        tableName: "table", // optional table name, default to table
        embedder: textEmbedding004,
      },
    ]),
  ],
});
// --8<-- [end:frameworks_genkit_usage]

// --8<-- [start:frameworks_genkit_custom_indexer]
export const menuPdfIndexer = lancedbIndexerRef({
  // Using all defaults, for dbUri, tableName, and embedder, etc
});

const chunkingConfig = {
  minLength: 1000,
  maxLength: 2000,
  splitter: "sentence",
  overlap: 100,
  delimiters: "",
} as any;

async function extractTextFromPdf(filePath: string) {
  const pdfFile = path.resolve(filePath);
  const dataBuffer = await readFile(pdfFile);
  const data = await pdf(dataBuffer);
  return data.text;
}

export const indexMenu = ai.defineFlow(
  {
    name: "indexMenu",
    inputSchema: z.string().describe("PDF file path"),
    outputSchema: z.void(),
  },
  async (filePath: string) => {
    filePath = path.resolve(filePath);

    // Read the pdf.
    const pdfTxt = await ai.run("extract-text", () => extractTextFromPdf(filePath));

    // Divide the pdf text into segments.
    const chunks = await ai.run("chunk-it", async () => chunk(pdfTxt, chunkingConfig));

    // Convert chunks of text into documents to store in the index.
    const documents = chunks.map((text) => {
      return Document.fromText(text, { filePath });
    });

    // Add documents to the index.
    await ai.index({
      indexer: menuPdfIndexer,
      documents,
      options: {
        writeMode: WriteMode.Overwrite,
      } as any,
    });
  },
);
// --8<-- [end:frameworks_genkit_custom_indexer]

// --8<-- [start:frameworks_genkit_custom_retriever]
export const menuRetriever = lancedbRetrieverRef({
  tableName: "table", // Use the same table name as the indexer.
  displayName: "Menu", // Use a custom display name.
});

export const menuQAFlow = ai.defineFlow(
  { name: "Menu", inputSchema: z.string(), outputSchema: z.string() },
  async (input: string) => {
    // retrieve relevant documents
    const docs = await ai.retrieve({
      retriever: menuRetriever,
      query: input,
      options: {
        k: 3,
      },
    });

    const extractedContent = docs.map((doc) => {
      if (doc.content && Array.isArray(doc.content) && doc.content.length > 0) {
        if (doc.content[0].media && doc.content[0].media.url) {
          return doc.content[0].media.url;
        }
      }
      return "No content found";
    });

    console.log("Extracted content:", extractedContent);

    const { text } = await ai.generate({
      model: gemini("gemini-2.0-flash"),
      prompt: `
You are acting as a helpful AI assistant that can answer 
questions about the food available on the menu at Genkit Grub Pub.

Use only the context provided to answer the question.
If you don't know, do not make up an answer.
Do not add or change items on the menu.

Context:
${extractedContent.join("\n\n")}

Question: ${input}`,
      docs,
    });

    return text;
  },
);
// --8<-- [end:frameworks_genkit_custom_retriever]

