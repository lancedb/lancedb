# Rate Limiting and Retry Support in LanceDB Node.js SDK

LanceDB Node.js SDK supports automatic rate limiting and exponential backoff retry for embedding functions to handle API rate limits gracefully, similar to the Python SDK.

## Basic Usage

Embedding functions now include rate limiting and retry functionality built-in:

```typescript
import { OpenAIEmbeddingFunction } from "lancedb/embedding";

// Create with default rate limiting (0.9 requests per second) and retries (7 attempts)
const embed = new OpenAIEmbeddingFunction({ 
  model: "text-embedding-3-small",
  apiKey: "your-api-key" 
});

// Use with retry and rate limiting
const embeddings = await embed.computeSourceEmbeddingsWithRetry(["text1", "text2"]);

// For query embeddings
const queryEmbedding = await embed.computeQueryEmbeddingsWithRetry("search query");
```

## Customizing Rate Limiting and Retry

You can customize the rate limiting and retry behavior by:

1. Passing parameters during embedding function creation (OpenAI example):

```typescript
const embed = new OpenAIEmbeddingFunction({ 
  model: "text-embedding-3-small",
  apiKey: "your-api-key",
  maxRequestsPerMinute: 60, // 60 requests per minute
  maxRetries: 5            // 5 retries with exponential backoff
});
```

2. Configuring after creation (works with any embedding function):

```typescript
import { EmbeddingFunction } from "lancedb/embedding";

const myEmbedFunc = new SomeEmbeddingFunction();

// Configure rate limiting (0.9 requests per second by default)
myEmbedFunc.rateLimit(
  maxCalls = 1.5,  // 1.5 calls per second
  period = 1.0     // per 1 second
);

// Configure retry with exponential backoff
myEmbedFunc.retry(
  initialDelay = 1,      // Start with 1 second delay
  exponentialBase = 2,   // Double the delay after each failed attempt
  jitter = true,         // Add random jitter to avoid thundering herd
  maxRetries = 7         // Maximum number of retries
);
```

## How It Works

When you call `computeSourceEmbeddingsWithRetry()` or `computeQueryEmbeddingsWithRetry()`, the SDK:

1. Enforces rate limits according to your configuration
2. Automatically retries requests with increasing backoff periods when rate limits are hit
3. Provides meaningful error messages in case all retries are exhausted

This helps you handle rate-limited APIs (like OpenAI, Cohere, etc.) more gracefully without manual implementation.

## Example

See the complete example at [examples/rate_limiting.ts](../examples/rate_limiting.ts). 