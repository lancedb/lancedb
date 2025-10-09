# Mastra

Mastra is an open-source TypeScript agent framework designed to help you build AI applications and features quickly. It provides a set of primitives for creating AI agents with memory, workflows, and retrieval-augmented generation (RAG) capabilities.

Key features of Mastra include:

- **Agent memory and tool calling** - Create agents that can call functions and maintain memory
- **Workflow graphs** - Define deterministic, graph-based workflows for LLM calls
- **RAG capabilities** - Process documents, create embeddings, and store them in vector databases
- **Development environment** - Chat with your agents locally and see their state and memory
- **Deployment tools** - Bundle your agents within existing applications or as standalone endpoints
- **Evaluation metrics** - Assess LLM outputs with model-graded, rule-based, and statistical methods

## Using LanceDB with Mastra

Mastra provides an official integration with LanceDB through the `@mastra/lance` package. This integration allows you to use LanceDB as both a storage backend for persistent application data and as a vector database for embedding storage and similarity search.

### Installation

To use LanceDB with Mastra, install the required packages:

```bash
pnpm add @mastra/lance
```

### Setup & Configuration

#### Basic Setup

```typescript
import { LanceStorage } from "@mastra/lance";
import { Mastra } from "@mastra/core/mastra";

// Initialize LanceStorage
const storage = await LanceStorage.create(
  "myApp", // Name for your storage instance
  "path/to/db" // Path to database directory
);

// Configure Mastra with LanceStorage
const mastra = new Mastra({
  storage: storage,
});
```

#### Connection Options

LanceStorage supports multiple connection configurations:

```typescript
// Local database
const localStore = await LanceStorage.create("myApp", "/path/to/db");

// LanceDB Cloud
const cloudStore = await LanceStorage.create("myApp", "db://host:port");

// S3 bucket
const s3Store = await LanceStorage.create("myApp", "s3://bucket/db", {
  storageOptions: { timeout: "60s" },
});
```

### Basic Storage Operations

#### Creating Tables

```typescript
import { TABLE_MESSAGES } from "@mastra/core/storage";
import type { StorageColumn } from "@mastra/core/storage";

// Define schema with appropriate types
const schema: Record<string, StorageColumn> = {
  id: { type: "uuid", nullable: false },
  threadId: { type: "uuid", nullable: false },
  content: { type: "text", nullable: true },
  createdAt: { type: "timestamp", nullable: false },
  metadata: { type: "jsonb", nullable: true },
};

// Create table
await storage.createTable({
  tableName: TABLE_MESSAGES,
  schema,
});
```

#### Inserting Records

```typescript
// Insert a single record
await storage.insert({
  tableName: TABLE_MESSAGES,
  record: {
    id: "123e4567-e89b-12d3-a456-426614174000",
    threadId: "123e4567-e89b-12d3-a456-426614174001",
    content: "Hello, world!",
    createdAt: new Date(),
    metadata: { tags: ["important", "greeting"] },
  },
});

// Batch insert multiple records
await storage.batchInsert({
  tableName: TABLE_MESSAGES,
  records: [
    {
      id: "123e4567-e89b-12d3-a456-426614174002",
      threadId: "123e4567-e89b-12d3-a456-426614174001",
      content: "First message",
      createdAt: new Date(),
      metadata: { priority: "high" },
    },
    {
      id: "123e4567-e89b-12d3-a456-426614174003",
      threadId: "123e4567-e89b-12d3-a456-426614174001",
      content: "Second message",
      createdAt: new Date(),
      metadata: { priority: "low" },
    },
  ],
});
```

#### Querying Data

```typescript
// Load a record by id
const message = await storage.load({
  tableName: TABLE_MESSAGES,
  keys: { id: "123e4567-e89b-12d3-a456-426614174000" },
});

// Load messages from a thread
const messages = await storage.getMessages({
  threadId: "123e4567-e89b-12d3-a456-426614174001",
});
```

### Working with Threads & Messages

#### Creating Threads

```typescript
import type { StorageThreadType } from "@mastra/core";

// Create a new thread
const thread: StorageThreadType = {
  id: "123e4567-e89b-12d3-a456-426614174010",
  resourceId: "resource-123",
  title: "New Discussion",
  createdAt: new Date(),
  metadata: { topic: "technical-support" },
};

// Save the thread
const savedThread = await storage.saveThread({ thread });
```

#### Working with Messages

```typescript
import type { MessageType } from "@mastra/core";

// Create messages
const messages: MessageType[] = [
  {
    id: "msg-001",
    threadId: "123e4567-e89b-12d3-a456-426614174010",
    resourceId: "resource-123",
    role: "user",
    content: "How can I use LanceDB with Mastra?",
    createdAt: new Date(),
    type: "text",
    toolCallIds: [],
    toolCallArgs: [],
    toolNames: [],
  },
  {
    id: "msg-002",
    threadId: "123e4567-e89b-12d3-a456-426614174010",
    resourceId: "resource-123",
    role: "assistant",
    content:
      "You can use LanceDB with Mastra by installing @mastra/lance package.",
    createdAt: new Date(),
    type: "text",
    toolCallIds: [],
    toolCallArgs: [],
    toolNames: [],
  },
];

// Save messages
await storage.saveMessages({ messages });

// Retrieve messages with context
const retrievedMessages = await storage.getMessages({
  threadId: "123e4567-e89b-12d3-a456-426614174010",
  selectBy: [
    {
      id: "msg-001",
      withPreviousMessages: 5, // Include up to 5 messages before this one
      withNextMessages: 5, // Include up to 5 messages after this one
    },
  ],
});
```

### Working with Workflows

Mastra's workflow system uses LanceDB to persist workflow state for continuity across runs:

```typescript
import type { WorkflowRunState } from "@mastra/core";

// Persist a workflow snapshot
await storage.persistWorkflowSnapshot({
  workflowName: "documentProcessing",
  runId: "run-123",
  snapshot: {
    context: {
      steps: {
        step1: { status: "success", payload: { data: "processed" } },
        step2: { status: "waiting" },
      },
      triggerData: { documentId: "doc-123" },
      attempts: { step1: 1, step2: 0 },
    },
  } as WorkflowRunState,
});

// Load a workflow snapshot
const workflowState = await storage.loadWorkflowSnapshot({
  workflowName: "documentProcessing",
  runId: "run-123",
});
```

### Using LanceDB for Vector Storage

The LanceDB integration in Mastra can be used for both traditional storage and vector search:

```typescript
// Create a schema with vector field
const vectorSchema: Record<string, StorageColumn> = {
  id: { type: "uuid", nullable: false },
  content: { type: "text", nullable: true },
  embedding: { type: "binary", nullable: false }, // Vector embedding
  metadata: { type: "jsonb", nullable: true },
};

// Create a vector table
await storage.createTable({
  tableName: "vector_store",
  schema: vectorSchema,
});

// Insert a vector with content and metadata
await storage.insert({
  tableName: "vector_store",
  record: {
    id: "vec-001",
    content: "This is a document about LanceDB and Mastra integration",
    embedding: new Float32Array([0.1, 0.2, 0.3, 0.4]), // Your embedding vector
    metadata: { source: "documentation", category: "integration" },
  },
});
```

### Data Management

```typescript
// Drop a table
await storage.dropTable(TABLE_MESSAGES);

// Clear all records from a table
await storage.clearTable({ tableName: TABLE_MESSAGES });

// Get table schema
const schema = await storage.getTableSchema(TABLE_MESSAGES);
```

## Benefits of Using LanceDB with Mastra

- **Serverless & Embeddable**: LanceDB can be embedded directly in your application with no separate server needed
- **Fast & Efficient**: Built on Apache Arrow and the Lance format for high-performance vector operations
- **Persistent Storage**: Reliably store agent knowledge and context with automatic persistence
- **Seamless Integration**: Works well with Mastra's RAG workflow
- **Schema Flexibility**: Store diverse metadata alongside your vectors

## Additional Resources

- [Mastra Documentation](https://mastra.ai/docs)
