### genkitx-lancedb
This is a lancedb plugin for genkit framework. It allows you to use LanceDB for ingesting and rereiving data using genkit framework.

![integration-banner-genkit](https://github.com/user-attachments/assets/a6cc28af-98e9-4425-b87c-7ab139bd7893)

### Installation
```bash
pnpm install genkitx-lancedb
```

### Usage

Adding LanceDB plugin to your genkit instance.

```ts
import { lancedbIndexerRef, lancedb, lancedbRetrieverRef, WriteMode } from 'genkitx-lancedb';
import { textEmbedding004, vertexAI } from '@genkit-ai/vertexai';
import { gemini } from '@genkit-ai/vertexai';
import { z, genkit } from 'genkit';
import { Document } from 'genkit/retriever';
import { chunk } from 'llm-chunk';
import { readFile } from 'fs/promises';
import path from 'path';
import pdf from 'pdf-parse/lib/pdf-parse';

const ai = genkit({
  plugins: [
    // vertexAI provides the textEmbedding004 embedder
    vertexAI(),

    // the local vector store requires an embedder to translate from text to vector
    lancedb([
      {
        dbUri: '.db', // optional lancedb uri, default to .db
        tableName: 'table', // optional table name, default to table
        embedder: textEmbedding004,
      },
    ]),
  ],
});
```

You can run this app with the following command:
```bash
genkit start -- tsx --watch src/index.ts
```

This'll add LanceDB as a retriever and indexer to the genkit instance. You can see it in the GUI view
<img width="1710" alt="Screenshot 2025-05-11 at 7 21 05 PM" src="https://github.com/user-attachments/assets/e752f7f4-785b-4797-a11e-72ab06a531b7" />

**Testing retrieval on a sample table**
Let's see the raw retrieval results

<img width="1710" alt="Screenshot 2025-05-11 at 7 21 05 PM" src="https://github.com/user-attachments/assets/b8d356ed-8421-4790-8fc0-d6af563b9657" />
On running this query, you'll 5 results fetched from the lancedb table, where each result looks something like this:
<img width="1417" alt="Screenshot 2025-05-11 at 7 21 18 PM" src="https://github.com/user-attachments/assets/77429525-36e2-4da6-a694-e58c1cf9eb83" />



## Creating a custom RAG flow

Now that we've seen how you can use LanceDB for in a genkit pipeline, let's refine the flow and create a RAG. A RAG flow will consist of an index and a retreiver with its outputs postprocessed an fed into an LLM for final response

### Creating custom indexer flows
You can also create custom indexer flows, utilizing more options and features provided by LanceDB.

```ts
export const menuPdfIndexer = lancedbIndexerRef({
   // Using all defaults, for dbUri, tableName, and embedder, etc
});

const chunkingConfig = {
  minLength: 1000,
  maxLength: 2000,
  splitter: 'sentence',
  overlap: 100,
  delimiters: '',
} as any;


async function extractTextFromPdf(filePath: string) {
  const pdfFile = path.resolve(filePath);
  const dataBuffer = await readFile(pdfFile);
  const data = await pdf(dataBuffer);
  return data.text;
}

export const indexMenu = ai.defineFlow(
  {
    name: 'indexMenu',
    inputSchema: z.string().describe('PDF file path'),
    outputSchema: z.void(),
  },
  async (filePath: string) => {
    filePath = path.resolve(filePath);

    // Read the pdf.
    const pdfTxt = await ai.run('extract-text', () =>
      extractTextFromPdf(filePath)
    );

    // Divide the pdf text into segments.
    const chunks = await ai.run('chunk-it', async () =>
      chunk(pdfTxt, chunkingConfig)
    );

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
      } as any
    });
  }
);
```

<img width="1316" alt="Screenshot 2025-05-11 at 8 35 56 PM" src="https://github.com/user-attachments/assets/e2a20ce4-d1d0-4fa2-9a84-f2cc26e3a29f" />

In your console, you can see the logs

<img width="511" alt="Screenshot 2025-05-11 at 7 19 14 PM" src="https://github.com/user-attachments/assets/243f26c5-ed38-40b6-b661-002f40f0423a" />

### Creating custom retriever flows
You can also create custom retriever flows, utilizing more options and features provided by LanceDB.
```ts
export const menuRetriever = lancedbRetrieverRef({
  tableName: "table", // Use the same table name as the indexer.
  displayName: "Menu", // Use a custom display name.

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

    const extractedContent = docs.map(doc => {
      if (doc.content && Array.isArray(doc.content) && doc.content.length > 0) {
        if (doc.content[0].media && doc.content[0].media.url) {
          return doc.content[0].media.url;
        }
      }
      return "No content found";
    });

    console.log("Extracted content:", extractedContent);

    const { text } = await ai.generate({
      model: gemini('gemini-2.0-flash'),
      prompt: `
You are acting as a helpful AI assistant that can answer 
questions about the food available on the menu at Genkit Grub Pub.

Use only the context provided to answer the question.
If you don't know, do not make up an answer.
Do not add or change items on the menu.

Context:
${extractedContent.join('\n\n')}

Question: ${input}`,
      docs,
    });
    
    return text;
  }
);
```
Now using our retrieval flow, we can ask question about the ingsted PDF
<img width="1306" alt="Screenshot 2025-05-11 at 7 18 45 PM" src="https://github.com/user-attachments/assets/86c66b13-7c12-4d5f-9d81-ae36bfb1c346" />

