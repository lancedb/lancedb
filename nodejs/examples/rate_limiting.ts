// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

import * as lancedb from "../lancedb";
import { OpenAIEmbeddingFunction } from "../lancedb/embedding";

/**
 * This example demonstrates how to use the rate limiting and retry functionality 
 * with OpenAI embeddings to handle rate limits gracefully.
 */
async function main() {
  console.log("Creating embedding function with rate limiting and retry...");
  
  // Create an OpenAI embedding function with custom rate limiting and retry settings
  const embedFunction = new OpenAIEmbeddingFunction({ 
    model: "text-embedding-3-small",
    // API key will be taken from OPENAI_API_KEY environment variable
    maxRequestsPerMinute: 60, // 60 requests per minute (1 per second)
    maxRetries: 5            // 5 retries with exponential backoff
  });

  // Sample text data to embed
  const textData = [
    "The quick brown fox jumps over the lazy dog",
    "LanceDB is a serverless vector database",
    "Vector search enables semantic understanding of data",
    "Machine learning models can be used for generating embeddings",
    "Natural language processing has advanced significantly with transformers"
  ];

  console.log("Computing embeddings with automatic rate limiting and retry...");
  try {
    // Using the retry-enabled method to handle rate limits automatically
    const embeddings = await embedFunction.computeSourceEmbeddingsWithRetry(textData);
    console.log(`Successfully generated ${embeddings.length} embeddings`);
    console.log(`Each embedding has ${embeddings[0].length} dimensions`);
    
    // Create an in-memory database
    const db = await lancedb.connect("./.lancedb");
    
    // Create a table with the embeddings
    const data = textData.map((text, i) => ({
      id: i,
      text,
      vector: embeddings[i]
    }));
    
    const table = await db.createTable("embeddings_example", data);
    console.log("Table created successfully!");
    
    // Perform a search
    console.log("Performing vector search...");
    const query = "semantic search technology";
    const queryEmbedding = await embedFunction.computeQueryEmbeddingsWithRetry(query);
    
    const results = await table.search(queryEmbedding).limit(2).execute();
    console.log("Search results for query: " + query);
    console.log(results);
  } catch (error) {
    console.error("Error occurred:", error);
  }
}

main().catch(console.error); 