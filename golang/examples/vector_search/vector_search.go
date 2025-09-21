// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// Vector Search Example
//
// This example demonstrates comprehensive vector similarity search capabilities
// with LanceDB using the Go SDK. It covers:
// - Creating vector embeddings and storing them
// - Basic vector similarity search
// - Vector search with different K values
// - Vector search with metadata filtering
// - Configuring search parameters
// - Performance optimization techniques

package main

import (
	"context"
	"fmt"
	lancedb "github.com/lancedb/lancedb/pkg"
	"log"
	"math"
	"math/rand"
	"os"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

const (
	EmbeddingDimensions = 128
	NumDocuments        = 100
)

type Document struct {
	ID       int32
	Title    string
	Content  string
	Category string
	Tags     string
	Vector   []float32
}

func main() {
	fmt.Println("🔍 LanceDB Go SDK - Vector Search Example")
	fmt.Println("==================================================")

	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir, err := os.MkdirTemp("", "lancedb_vector_example_")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)
	fmt.Printf("📂 Using database directory: %s\n", tempDir)

	// Connect to database
	fmt.Println("\n📋 Step 1: Connecting to LanceDB...")
	conn, err := lancedb.Connect(ctx, tempDir, nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()
	fmt.Println("✅ Connected successfully")

	// Create table with vector field
	fmt.Println("\n📋 Step 2: Creating vector table...")
	table, schema, err := createVectorTable(conn, ctx)
	if err != nil {
		log.Fatalf("Failed to create vector table: %v", err)
	}
	defer table.Close()
	fmt.Printf("✅ Created table with %d-dimensional vectors\n", EmbeddingDimensions)

	// Generate and insert sample documents
	fmt.Println("\n📋 Step 3: Generating sample documents with embeddings...")
	if err := insertSampleDocuments(table, schema); err != nil {
		log.Fatalf("Failed to insert sample documents: %v", err)
	}
	fmt.Printf("✅ Inserted %d documents with vector embeddings\n", NumDocuments)

	// Demonstrate basic vector search
	fmt.Println("\n📋 Step 4: Basic vector similarity search...")
	if err := basicVectorSearch(table); err != nil {
		log.Fatalf("Failed basic vector search: %v", err)
	}

	// Demonstrate search with different K values
	fmt.Println("\n📋 Step 5: Vector search with different K values...")
	if err := vectorSearchWithDifferentK(table); err != nil {
		log.Fatalf("Failed vector search with different K: %v", err)
	}

	// Demonstrate vector search with metadata filtering
	fmt.Println("\n📋 Step 6: Vector search with metadata filtering...")
	if err := vectorSearchWithFiltering(table); err != nil {
		log.Fatalf("Failed vector search with filtering: %v", err)
	}

	// Demonstrate advanced search configurations
	fmt.Println("\n📋 Step 7: Advanced search configurations...")
	if err := advancedSearchConfigurations(table); err != nil {
		log.Fatalf("Failed advanced search configurations: %v", err)
	}

	// Performance benchmarks
	fmt.Println("\n📋 Step 8: Performance benchmarks...")
	if err := performanceBenchmarks(table); err != nil {
		log.Fatalf("Failed performance benchmarks: %v", err)
	}

	fmt.Println("\n🎉 Vector search examples completed successfully!")
	fmt.Println("==================================================")
}

func createVectorTable(conn *lancedb.Connection, ctx context.Context) (*lancedb.Table, *arrow.Schema, error) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "title", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "content", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "vector", Type: arrow.FixedSizeListOf(EmbeddingDimensions, arrow.PrimitiveTypes.Float32), Nullable: false},
	}

	arrowSchema := arrow.NewSchema(fields, nil)
	schema, err := lancedb.NewSchema(arrowSchema)
	if err != nil {
		return nil, nil, err
	}

	table, err := conn.CreateTable(ctx, "documents", *schema)
	if err != nil {
		return nil, nil, err
	}

	return table, arrowSchema, nil
}

func insertSampleDocuments(table *lancedb.Table, schema *arrow.Schema) error {
	pool := memory.NewGoAllocator()
	rand.Seed(time.Now().UnixNano())

	// Generate sample documents
	documents := generateSampleDocuments()

	// Create Arrow arrays
	idBuilder := array.NewInt32Builder(pool)
	titleBuilder := array.NewStringBuilder(pool)
	contentBuilder := array.NewStringBuilder(pool)
	categoryBuilder := array.NewStringBuilder(pool)
	tagsBuilder := array.NewStringBuilder(pool)

	ids := make([]int32, len(documents))
	titles := make([]string, len(documents))
	contents := make([]string, len(documents))
	categories := make([]string, len(documents))
	tags := make([]string, len(documents))
	allVectors := make([]float32, len(documents)*EmbeddingDimensions)

	for i, doc := range documents {
		ids[i] = doc.ID
		titles[i] = doc.Title
		contents[i] = doc.Content
		categories[i] = doc.Category
		tags[i] = doc.Tags

		// Copy vector data
		copy(allVectors[i*EmbeddingDimensions:(i+1)*EmbeddingDimensions], doc.Vector)
	}

	idBuilder.AppendValues(ids, nil)
	titleBuilder.AppendValues(titles, nil)
	contentBuilder.AppendValues(contents, nil)
	categoryBuilder.AppendValues(categories, nil)
	tagsBuilder.AppendValues(tags, nil)

	idArray := idBuilder.NewArray()
	defer idArray.Release()
	titleArray := titleBuilder.NewArray()
	defer titleArray.Release()
	contentArray := contentBuilder.NewArray()
	defer contentArray.Release()
	categoryArray := categoryBuilder.NewArray()
	defer categoryArray.Release()
	tagsArray := tagsBuilder.NewArray()
	defer tagsArray.Release()

	// Create vector array
	vectorFloat32Builder := array.NewFloat32Builder(pool)
	vectorFloat32Builder.AppendValues(allVectors, nil)
	vectorFloat32Array := vectorFloat32Builder.NewArray()
	defer vectorFloat32Array.Release()

	vectorListType := arrow.FixedSizeListOf(EmbeddingDimensions, arrow.PrimitiveTypes.Float32)
	vectorArray := array.NewFixedSizeListData(
		array.NewData(vectorListType, len(documents), []*memory.Buffer{nil},
			[]arrow.ArrayData{vectorFloat32Array.Data()}, 0, 0),
	)
	defer vectorArray.Release()

	// Create record and insert
	columns := []arrow.Array{idArray, titleArray, contentArray, categoryArray, tagsArray, vectorArray}
	record := array.NewRecord(schema, columns, int64(len(documents)))
	defer record.Release()

	return table.Add(record, nil)
}

func generateSampleDocuments() []Document {
	categories := []string{"technology", "science", "business", "health", "entertainment", "sports"}
	titles := map[string][]string{
		"technology": {
			"Machine Learning Breakthroughs", "Cloud Computing Trends", "Artificial Intelligence Ethics",
			"Quantum Computing Progress", "Blockchain Applications", "Cybersecurity Advances",
		},
		"science": {
			"Climate Change Research", "Space Exploration", "Medical Discoveries",
			"Physics Innovations", "Biology Studies", "Chemistry Breakthroughs",
		},
		"business": {
			"Market Analysis", "Startup Strategies", "Investment Trends",
			"Economic Forecasts", "Corporate Leadership", "Industry Reports",
		},
		"health": {
			"Mental Health Awareness", "Nutrition Guidelines", "Exercise Benefits",
			"Medical Treatment", "Health Technology", "Preventive Care",
		},
		"entertainment": {
			"Movie Reviews", "Music Trends", "Gaming Industry",
			"Celebrity News", "TV Shows", "Entertainment Technology",
		},
		"sports": {
			"Football Analysis", "Basketball Updates", "Olympic Games",
			"Tennis Championships", "Soccer World Cup", "Sports Technology",
		},
	}

	documents := make([]Document, 0, NumDocuments)
	docID := int32(1)

	for len(documents) < NumDocuments {
		category := categories[rand.Intn(len(categories))]
		titleList := titles[category]
		title := titleList[rand.Intn(len(titleList))]

		content := fmt.Sprintf("This is a detailed article about %s in the %s domain. "+
			"It covers various aspects and provides insights into current trends and future developments.",
			title, category)

		tags := fmt.Sprintf("tag_%s,trending,featured", category)

		// Generate vector embedding (simulated)
		vector := generateEmbedding(title, content, category)

		documents = append(documents, Document{
			ID:       docID,
			Title:    title,
			Content:  content,
			Category: category,
			Tags:     tags,
			Vector:   vector,
		})
		docID++
	}

	return documents
}

func generateEmbedding(title, content, category string) []float32 {
	// Simulate realistic embedding generation based on content
	vector := make([]float32, EmbeddingDimensions)

	// Create a simple hash-based embedding for consistent results
	seed := hash(title + content + category)
	rand.Seed(int64(seed))

	for i := 0; i < EmbeddingDimensions; i++ {
		vector[i] = float32(rand.NormFloat64() * 0.1) // Small variance around 0
	}

	// Add category-specific bias to make similar content cluster
	categoryBias := hash(category) % 10
	for i := 0; i < EmbeddingDimensions/10; i++ {
		idx := i*10 + int(categoryBias)
		vector[idx] += 0.5
	}

	// Normalize vector
	normalizeVector(vector)

	return vector
}

func hash(s string) uint32 {
	h := uint32(2166136261)
	for _, c := range s {
		h ^= uint32(c)
		h *= 16777619
	}
	return h
}

func normalizeVector(vector []float32) {
	var norm float32
	for _, v := range vector {
		norm += v * v
	}
	norm = float32(math.Sqrt(float64(norm)))

	if norm > 0 {
		for i := range vector {
			vector[i] /= norm
		}
	}
}

func basicVectorSearch(table *lancedb.Table) error {
	fmt.Println("  🔍 Performing basic vector similarity search...")

	// Create a query vector (simulate search for "machine learning" content)
	queryVector := generateEmbedding("Machine Learning", "artificial intelligence deep learning", "technology")

	// Perform vector search
	results, err := table.VectorSearch("vector", queryVector, 5)
	if err != nil {
		return fmt.Errorf("vector search failed: %w", err)
	}

	fmt.Printf("  📊 Found %d similar documents:\n", len(results))
	for i, result := range results {
		fmt.Printf("    %d. Title: %v\n", i+1, result["title"])
		fmt.Printf("       Category: %v\n", result["category"])
		fmt.Printf("       Distance: %v\n", result["_distance"])
		fmt.Printf("       Content: %.100s...\n\n", result["content"])
	}

	return nil
}

func vectorSearchWithDifferentK(table *lancedb.Table) error {
	fmt.Println("  🔢 Testing vector search with different K values...")

	queryVector := generateEmbedding("Sports Analysis", "football basketball statistics", "sports")

	kValues := []int{1, 3, 5, 10}

	for _, k := range kValues {
		fmt.Printf("  🔍 K = %d:\n", k)

		start := time.Now()
		results, err := table.VectorSearch("vector", queryVector, k)
		elapsed := time.Since(start)

		if err != nil {
			return fmt.Errorf("vector search with K=%d failed: %w", k, err)
		}

		fmt.Printf("    📊 Found %d results in %v\n", len(results), elapsed)
		for i, result := range results[:min(3, len(results))] { // Show top 3
			fmt.Printf("      %d. %v (distance: %v)\n", i+1, result["title"], result["_distance"])
		}
		fmt.Println()
	}

	return nil
}

func vectorSearchWithFiltering(table *lancedb.Table) error {
	fmt.Println("  🎯 Vector search with metadata filtering...")

	queryVector := generateEmbedding("Health Research", "medical studies wellness", "health")

	// Search within specific category
	fmt.Println("  🔍 Searching within 'health' category:")
	results, err := table.VectorSearchWithFilter("vector", queryVector, 5, "category = 'health'")
	if err != nil {
		return fmt.Errorf("filtered vector search failed: %w", err)
	}

	fmt.Printf("  📊 Found %d health-related documents:\n", len(results))
	for i, result := range results {
		fmt.Printf("    %d. %v (distance: %v, category: %v)\n",
			i+1, result["title"], result["_distance"], result["category"])
	}

	// Search with multiple filters
	fmt.Println("\n  🔍 Searching with multiple filters:")
	results, err = table.VectorSearchWithFilter("vector", queryVector, 10,
		"category IN ('health', 'science') AND tags LIKE '%featured%'")
	if err != nil {
		return fmt.Errorf("multi-filtered vector search failed: %w", err)
	}

	fmt.Printf("  📊 Found %d filtered documents:\n", len(results))
	for i, result := range results {
		fmt.Printf("    %d. %v (category: %v, distance: %v)\n",
			i+1, result["title"], result["category"], result["_distance"])
	}

	return nil
}

func advancedSearchConfigurations(table *lancedb.Table) error {
	fmt.Println("  ⚙️ Advanced search configurations...")

	queryVector := generateEmbedding("Technology Innovation", "startup artificial intelligence", "technology")

	// Complex query with vector search, filtering, column selection, and limits
	fmt.Println("  🔍 Complex query configuration:")
	limit := 3
	config := lancedb.QueryConfig{
		Columns: []string{"id", "title", "category"},
		Where:   "category IN ('technology', 'business')",
		Limit:   &limit,
		VectorSearch: &lancedb.VectorSearch{
			Column: "vector",
			Vector: queryVector,
			K:      10, // Get top 10, then filter and limit to 3
		},
	}

	start := time.Now()
	results, err := table.Select(config)
	elapsed := time.Since(start)

	if err != nil {
		return fmt.Errorf("advanced search configuration failed: %w", err)
	}

	fmt.Printf("  📊 Advanced query returned %d results in %v:\n", len(results), elapsed)
	for i, result := range results {
		fmt.Printf("    %d. ID: %v, Title: %v, Category: %v, Distance: %v\n",
			i+1, result["id"], result["title"], result["category"], result["_distance"])
	}

	return nil
}

func performanceBenchmarks(table *lancedb.Table) error {
	fmt.Println("  ⚡ Performance benchmarks...")

	queryVectors := make([][]float32, 5)
	for i := range queryVectors {
		queryVectors[i] = generateEmbedding(
			fmt.Sprintf("Query %d", i),
			fmt.Sprintf("benchmark test query %d", i),
			"technology",
		)
	}

	// Benchmark different K values
	fmt.Println("  📈 Benchmarking different K values:")
	kValues := []int{1, 5, 10, 20}

	for _, k := range kValues {
		start := time.Now()
		for i, queryVector := range queryVectors {
			_, err := table.VectorSearch("vector", queryVector, k)
			if err != nil {
				return fmt.Errorf("benchmark search %d failed: %w", i, err)
			}
		}
		elapsed := time.Since(start)

		avgTime := elapsed / time.Duration(len(queryVectors))
		fmt.Printf("    K=%d: %v total, %v average per query\n", k, elapsed, avgTime)
	}

	// Benchmark with and without filtering
	fmt.Println("\n  📈 Benchmarking with/without filtering:")

	// Without filtering
	start := time.Now()
	for _, queryVector := range queryVectors {
		_, err := table.VectorSearch("vector", queryVector, 5)
		if err != nil {
			return fmt.Errorf("benchmark without filter failed: %w", err)
		}
	}
	elapsedNoFilter := time.Since(start)

	// With filtering
	start = time.Now()
	for _, queryVector := range queryVectors {
		_, err := table.VectorSearchWithFilter("vector", queryVector, 5, "category = 'technology'")
		if err != nil {
			return fmt.Errorf("benchmark with filter failed: %w", err)
		}
	}
	elapsedWithFilter := time.Since(start)

	fmt.Printf("    Without filter: %v (%v avg)\n", elapsedNoFilter, elapsedNoFilter/time.Duration(len(queryVectors)))
	fmt.Printf("    With filter: %v (%v avg)\n", elapsedWithFilter, elapsedWithFilter/time.Duration(len(queryVectors)))

	return nil
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
