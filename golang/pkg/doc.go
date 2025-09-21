// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

/*
Package lancedb provides Go bindings for LanceDB, an open-source vector database.

LanceDB is designed for AI applications that need to store, manage, and query
high-dimensional vector embeddings alongside traditional data types. This Go SDK
provides a comprehensive interface to all LanceDB features through CGO bindings
to the Rust core library.

# Key Features

• Vector Search: High-performance similarity search with multiple distance metrics (L2, cosine, dot product)
• Multi-modal Data: Store vectors, metadata, text, images, and more in a single database
• SQL Queries: Query your data using familiar SQL syntax via DataFusion integration
• Multiple Backends: Local filesystem, S3, Google Cloud Storage, and LanceDB Cloud support
• Scalable Indexing: Support for IVF-PQ, HNSW, and other vector index types
• ACID Transactions: Full transactional support with automatic versioning
• Zero-Copy Operations: Efficient memory usage through Apache Arrow integration

# Basic Usage

Connect to a database and perform basic operations:

	db, err := lancedb.ConnectLocal("./my_database")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// Create schema
	schema, err := lancedb.NewSchemaBuilder().
		AddInt32Field("id", false).
		AddVectorField("embedding", 128, lancedb.VectorDataTypeFloat32, false).
		AddStringField("text", true).
		Build()
	if err != nil {
		log.Fatal(err)
	}

	// Create table
	table, err := db.CreateTable("documents", schema)
	if err != nil {
		log.Fatal(err)
	}
	defer table.Close()

# Vector Search

Perform similarity search on vector embeddings:

	queryVector := []float32{0.1, 0.2, 0.3}
	results, err := table.VectorSearch(queryVector).
		Limit(10).
		Filter("text IS NOT NULL").
		DistanceType(lancedb.DistanceTypeCosine).
		Execute()

# Connection Types

Local database:

	db, err := lancedb.ConnectLocal("/path/to/database")

S3-based database:

	db, err := lancedb.ConnectS3("my-bucket", "db-prefix", map[string]string{
		"aws_access_key_id":     "your-key",
		"aws_secret_access_key": "your-secret",
	})

LanceDB Cloud:

	db, err := lancedb.ConnectCloud("your-db-name", "your-api-key", nil)

# Schema Building

Build schemas with a fluent interface:

	schema, err := lancedb.NewSchemaBuilder().
		AddInt32Field("id", false).                                    // Required integer
		AddVectorField("embedding", 384, lancedb.VectorDataTypeFloat32, false). // 384-dim vector
		AddStringField("text", true).                                  // Optional string
		AddFloat32Field("score", true).                               // Optional float
		AddTimestampField("created_at", arrow.Microsecond, true).     // Optional timestamp
		Build()

# Error Handling

The SDK provides structured error handling:

	if err != nil {
		if lancedb.IsNotFoundError(err) {
			// Handle not found
		} else if lancedb.IsAlreadyExistsError(err) {
			// Handle already exists
		} else {
			// Handle other errors
		}
	}

# Performance Considerations

• Use batch operations when inserting large amounts of data
• Create appropriate indexes for your query patterns
• Consider using async operations for long-running queries
• Leverage Arrow's zero-copy operations when possible

# Thread Safety

Connection and Table objects are thread-safe and can be used concurrently
from multiple goroutines. However, individual query builders are not thread-safe
and should not be shared between goroutines.

For more detailed examples and advanced usage, see the examples directory
and the full documentation at https://lancedb.github.io/lancedb/
*/
package lancedb
