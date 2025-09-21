// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// Index Management Example
//
// This example demonstrates comprehensive index management capabilities
// with LanceDB using the Go SDK. It covers:
// - Creating different types of indexes (vector, scalar, full-text)
// - Index performance comparison
// - Best practices for index selection
// - Managing indexes throughout application lifecycle
// - Query optimization with proper indexing

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
	VectorDimensions = 384 // Typical sentence transformer dimension
	NumRecords       = 1000
)

type IndexedDocument struct {
	ID       int32
	Title    string
	Content  string
	Category string
	Status   string
	Price    float64
	Rating   int32
	Tags     string
	Vector   []float32
}

func main() {
	fmt.Println("🔧 LanceDB Go SDK - Index Management Example")
	fmt.Println("==================================================")

	// Setup
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tempDir, err := os.MkdirTemp("", "lancedb_index_example_")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)
	fmt.Printf("📂 Using database directory: %s\n", tempDir)

	// Connect and create table
	fmt.Println("\n📋 Step 1: Setting up database with comprehensive schema...")
	conn, err := lancedb.Connect(ctx, tempDir, nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	table, schema, err := createComprehensiveTable(conn, ctx)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	defer table.Close()
	fmt.Println("✅ Created table with diverse data types for indexing")

	// Insert sample data
	fmt.Println("\n📋 Step 2: Loading sample dataset...")
	if err := insertIndexedData(table, schema); err != nil {
		log.Fatalf("Failed to insert data: %v", err)
	}
	fmt.Printf("✅ Loaded %d records for index testing\n", NumRecords)

	// Demonstrate different index types
	fmt.Println("\n📋 Step 3: Creating and testing vector indexes...")
	if err := demonstrateVectorIndexes(table); err != nil {
		log.Fatalf("Failed vector index demo: %v", err)
	}

	fmt.Println("\n📋 Step 4: Creating and testing scalar indexes...")
	if err := demonstrateScalarIndexes(table); err != nil {
		log.Fatalf("Failed scalar index demo: %v", err)
	}

	fmt.Println("\n📋 Step 5: Creating and testing full-text indexes...")
	if err := demonstrateFullTextIndexes(table); err != nil {
		log.Fatalf("Failed full-text index demo: %v", err)
	}

	fmt.Println("\n📋 Step 6: Index performance comparison...")
	if err := performanceComparison(table); err != nil {
		log.Fatalf("Failed performance comparison: %v", err)
	}

	fmt.Println("\n📋 Step 7: Index management operations...")
	if err := indexManagementOperations(table); err != nil {
		log.Fatalf("Failed index management: %v", err)
	}

	fmt.Println("\n📋 Step 8: Best practices demonstration...")
	if err := bestPracticesDemo(table); err != nil {
		log.Fatalf("Failed best practices demo: %v", err)
	}

	fmt.Println("\n🎉 Index management examples completed successfully!")
	fmt.Println("==================================================")
}

func createComprehensiveTable(conn *lancedb.Connection, ctx context.Context) (*lancedb.Table, *arrow.Schema, error) {
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},                                                // Primary key
		{Name: "title", Type: arrow.BinaryTypes.String, Nullable: false},                                               // Text search
		{Name: "content", Type: arrow.BinaryTypes.String, Nullable: false},                                             // Full-text search
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: false},                                            // Categorical (Bitmap)
		{Name: "status", Type: arrow.BinaryTypes.String, Nullable: false},                                              // Low-cardinality (Bitmap)
		{Name: "price", Type: arrow.PrimitiveTypes.Float64, Nullable: false},                                           // Range queries (BTree)
		{Name: "rating", Type: arrow.PrimitiveTypes.Int32, Nullable: false},                                            // Discrete values (BTree)
		{Name: "tags", Type: arrow.BinaryTypes.String, Nullable: true},                                                 // Tag strings
		{Name: "vector", Type: arrow.FixedSizeListOf(VectorDimensions, arrow.PrimitiveTypes.Float32), Nullable: false}, // Vector search
	}

	arrowSchema := arrow.NewSchema(fields, nil)
	schema, err := lancedb.NewSchema(arrowSchema)
	if err != nil {
		return nil, nil, err
	}

	table, err := conn.CreateTable(ctx, "indexed_docs", *schema)
	if err != nil {
		return nil, nil, err
	}
	return table, arrowSchema, nil
}

func insertIndexedData(table *lancedb.Table, schema *arrow.Schema) error {
	pool := memory.NewGoAllocator()
	rand.Seed(time.Now().UnixNano())

	documents := generateIndexedDocuments()

	// Prepare data arrays
	ids := make([]int32, len(documents))
	titles := make([]string, len(documents))
	contents := make([]string, len(documents))
	categories := make([]string, len(documents))
	statuses := make([]string, len(documents))
	prices := make([]float64, len(documents))
	ratings := make([]int32, len(documents))
	tags := make([]string, len(documents))
	allVectors := make([]float32, len(documents)*VectorDimensions)

	for i, doc := range documents {
		ids[i] = doc.ID
		titles[i] = doc.Title
		contents[i] = doc.Content
		categories[i] = doc.Category
		statuses[i] = doc.Status
		prices[i] = doc.Price
		ratings[i] = doc.Rating
		tags[i] = doc.Tags
		copy(allVectors[i*VectorDimensions:(i+1)*VectorDimensions], doc.Vector)
	}

	// Build Arrow arrays
	builders := []interface{}{
		array.NewInt32Builder(pool),                          // id
		array.NewStringBuilder(pool),                         // title
		array.NewStringBuilder(pool),                         // content
		array.NewStringBuilder(pool),                         // category
		array.NewStringBuilder(pool),                         // status
		array.NewFloat64Builder(pool),                        // price
		array.NewInt32Builder(pool),                          // rating
		array.NewStringBuilder(pool),                        // tags
	}

	// Append data
	builders[0].(*array.Int32Builder).AppendValues(ids, nil)
	builders[1].(*array.StringBuilder).AppendValues(titles, nil)
	builders[2].(*array.StringBuilder).AppendValues(contents, nil)
	builders[3].(*array.StringBuilder).AppendValues(categories, nil)
	builders[4].(*array.StringBuilder).AppendValues(statuses, nil)
	builders[5].(*array.Float64Builder).AppendValues(prices, nil)
	builders[6].(*array.Int32Builder).AppendValues(ratings, nil)

	builders[7].(*array.StringBuilder).AppendValues(tags, nil)

	// Create arrays
	arrays := make([]arrow.Array, 8)
	for i, builder := range builders {
		switch b := builder.(type) {
		case *array.Int32Builder:
			arrays[i] = b.NewArray()
		case *array.StringBuilder:
			arrays[i] = b.NewArray()
		case *array.Float64Builder:
			arrays[i] = b.NewArray()
		}
		defer arrays[i].Release()
	}

	// Vector array
	vectorBuilder := array.NewFloat32Builder(pool)
	vectorBuilder.AppendValues(allVectors, nil)
	vectorFloat32Array := vectorBuilder.NewArray()
	defer vectorFloat32Array.Release()

	vectorListType := arrow.FixedSizeListOf(VectorDimensions, arrow.PrimitiveTypes.Float32)
	vectorArray := array.NewFixedSizeListData(
		array.NewData(vectorListType, len(documents), []*memory.Buffer{nil},
			[]arrow.ArrayData{vectorFloat32Array.Data()}, 0, 0),
	)
	defer vectorArray.Release()

	// Combine all arrays
	allArrays := append(arrays, vectorArray)
	record := array.NewRecord(schema, allArrays, int64(len(documents)))
	defer record.Release()

	return table.Add(record, nil)
}

func generateIndexedDocuments() []IndexedDocument {
	categories := []string{"Technology", "Health", "Finance", "Education", "Entertainment"}
	statuses := []string{"published", "draft", "archived", "featured"}

	documents := make([]IndexedDocument, NumRecords)

	for i := 0; i < NumRecords; i++ {
		category := categories[rand.Intn(len(categories))]
		status := statuses[rand.Intn(len(statuses))]

		title := generateTitle(category, i)
		content := generateContent(category, title)
		tags := generateDocumentTags(category, status)

		documents[i] = IndexedDocument{
			ID:       int32(i + 1),
			Title:    title,
			Content:  content,
			Category: category,
			Status:   status,
			Price:    10.0 + rand.Float64()*990.0, // $10 - $1000
			Rating:   int32(1 + rand.Intn(5)),     // 1-5 stars
			Tags:     tags,
			Vector:   generateDocumentEmbedding(title, content, category),
		}
	}

	return documents
}

func generateTitle(category string, index int) string {
	titleTemplates := map[string][]string{
		"Technology": {
			"Advanced AI Systems in Modern Computing %d",
			"Cloud Infrastructure and Scalability %d",
			"Machine Learning Applications %d",
			"Cybersecurity Best Practices %d",
		},
		"Health": {
			"Medical Research Breakthrough %d",
			"Healthcare Technology Innovations %d",
			"Wellness and Preventive Care %d",
			"Mental Health Awareness %d",
		},
		"Finance": {
			"Investment Strategies for %d",
			"Market Analysis and Trends %d",
			"Financial Planning Guide %d",
			"Economic Forecast %d",
		},
		"Education": {
			"Learning Methodologies %d",
			"Educational Technology %d",
			"Student Success Strategies %d",
			"Curriculum Development %d",
		},
		"Entertainment": {
			"Media and Content Creation %d",
			"Gaming Industry Insights %d",
			"Entertainment Technology %d",
			"Cultural Impact Analysis %d",
		},
	}

	templates := titleTemplates[category]
	template := templates[rand.Intn(len(templates))]
	return fmt.Sprintf(template, index%100+1)
}

func generateContent(category, title string) string {
	return fmt.Sprintf("This comprehensive article about %s explores various aspects of %s. "+
		"It provides detailed analysis, practical insights, and future perspectives on the topic. "+
		"The content covers both theoretical foundations and real-world applications, making it "+
		"valuable for professionals and enthusiasts alike. Key findings include innovative approaches "+
		"and emerging trends that will shape the future of this field.",
		title, category)
}

func generateDocumentTags(category, status string) string {
	baseTags := []string{category, status}
	additionalTags := []string{"trending", "featured", "popular", "recommended", "premium"}

	numAdditional := 1 + rand.Intn(3) // 1-3 additional tags
	for i := 0; i < numAdditional; i++ {
		tag := additionalTags[rand.Intn(len(additionalTags))]
		baseTags = append(baseTags, tag)
	}

	// Join tags with commas
	result := ""
	for i, tag := range baseTags {
		if i > 0 {
			result += ","
		}
		result += tag
	}
	return result
}

func generateDocumentEmbedding(title, content, category string) []float32 {
	vector := make([]float32, VectorDimensions)

	// Create consistent embedding
	seed := hash(title + content + category)
	rand.Seed(int64(seed))

	for i := 0; i < VectorDimensions; i++ {
		vector[i] = float32(rand.NormFloat64() * 0.1)
	}

	// Category clustering - add influence to a 4-element segment
	categoryHash := hash(category) % uint32(VectorDimensions/4)
	startIdx := int(categoryHash) * 4
	for i := 0; i < 4; i++ {
		vector[startIdx+i] += 0.2
	}

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

func demonstrateVectorIndexes(table *lancedb.Table) error {
	fmt.Println("  🎯 Vector Index Types and Performance")

	// Test without index (baseline)
	fmt.Println("  📊 Baseline: Vector search without index")
	queryVector := generateDocumentEmbedding("AI technology", "machine learning artificial intelligence", "Technology")

	start := time.Now()
	results, err := table.VectorSearch("vector", queryVector, 5)
	baselineTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("baseline vector search failed: %w", err)
	}
	fmt.Printf("    ⏱️ Baseline time: %v (%d results)\n", baselineTime, len(results))

	// Create IVF-PQ index (good for large datasets)
	fmt.Println("\n  🔧 Creating IVF-PQ index for large-scale similarity search...")
	err = table.CreateIndexWithName([]string{"vector"}, lancedb.IndexTypeIvfPq, "vector_ivf_pq_idx")
	if err != nil {
		return fmt.Errorf("failed to create IVF-PQ index: %w", err)
	}
	fmt.Println("  ✅ IVF-PQ index created successfully")

	// Test with IVF-PQ index
	start = time.Now()
	results, err = table.VectorSearch("vector", queryVector, 5)
	ivfPqTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("IVF-PQ vector search failed: %w", err)
	}
	fmt.Printf("    ⏱️ IVF-PQ time: %v (%d results) - %.2fx %s\n",
		ivfPqTime, len(results),
		float64(baselineTime)/float64(ivfPqTime),
		map[bool]string{true: "faster", false: "slower"}[ivfPqTime < baselineTime])

	// Create IVF-Flat index (better accuracy, more memory)
	fmt.Println("\n  🔧 Creating IVF-Flat index for high-accuracy search...")
	err = table.CreateIndexWithName([]string{"vector"}, lancedb.IndexTypeIvfFlat, "vector_ivf_flat_idx")
	if err != nil {
		return fmt.Errorf("failed to create IVF-Flat index: %w", err)
	}
	fmt.Println("  ✅ IVF-Flat index created successfully")

	// Test with IVF-Flat index
	start = time.Now()
	results, err = table.VectorSearch("vector", queryVector, 5)
	ivfFlatTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("IVF-Flat vector search failed: %w", err)
	}
	fmt.Printf("    ⏱️ IVF-Flat time: %v (%d results) - %.2fx %s\n",
		ivfFlatTime, len(results),
		float64(baselineTime)/float64(ivfFlatTime),
		map[bool]string{true: "faster", false: "slower"}[ivfFlatTime < baselineTime])

	// Create HNSW-PQ index (very fast queries)
	fmt.Println("\n  🔧 Creating HNSW-PQ index for ultra-fast queries...")
	err = table.CreateIndexWithName([]string{"vector"}, lancedb.IndexTypeHnswPq, "vector_hnsw_pq_idx")
	if err != nil {
		return fmt.Errorf("failed to create HNSW-PQ index: %w", err)
	}
	fmt.Println("  ✅ HNSW-PQ index created successfully")

	// Test with HNSW-PQ index
	start = time.Now()
	results, err = table.VectorSearch("vector", queryVector, 5)
	hnswPqTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("HNSW-PQ vector search failed: %w", err)
	}
	fmt.Printf("    ⏱️ HNSW-PQ time: %v (%d results) - %.2fx %s\n",
		hnswPqTime, len(results),
		float64(baselineTime)/float64(hnswPqTime),
		map[bool]string{true: "faster", false: "slower"}[hnswPqTime < baselineTime])

	fmt.Println("\n  📋 Vector Index Recommendations:")
	fmt.Println("    • IVF-PQ: Best for large datasets (>1M vectors) where some accuracy trade-off is acceptable")
	fmt.Println("    • IVF-Flat: Good balance of accuracy and performance for medium datasets")
	fmt.Println("    • HNSW-PQ: Fastest queries, good for real-time applications with frequent searches")

	return nil
}

func demonstrateScalarIndexes(table *lancedb.Table) error {
	fmt.Println("  📊 Scalar Index Types for Structured Data")

	// BTree index for range queries
	fmt.Println("  🌳 Creating BTree indexes for range queries...")

	// Price index for range queries
	err := table.CreateIndexWithName([]string{"price"}, lancedb.IndexTypeBTree, "price_btree_idx")
	if err != nil {
		return fmt.Errorf("failed to create price BTree index: %w", err)
	}
	fmt.Println("  ✅ Price BTree index created")

	// Rating index for discrete values
	err = table.CreateIndexWithName([]string{"rating"}, lancedb.IndexTypeBTree, "rating_btree_idx")
	if err != nil {
		return fmt.Errorf("failed to create rating BTree index: %w", err)
	}
	fmt.Println("  ✅ Rating BTree index created")

	// Test range queries with BTree indexes
	fmt.Println("\n  🔍 Testing BTree index performance on range queries...")

	// Price range query
	start := time.Now()
	results, err := table.SelectWithFilter("price BETWEEN 100 AND 500")
	priceQueryTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("price range query failed: %w", err)
	}
	fmt.Printf("    💰 Price range (100-500): %v (%d results)\n", priceQueryTime, len(results))

	// Rating query
	start = time.Now()
	results, err = table.SelectWithFilter("rating >= 4")
	ratingQueryTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("rating query failed: %w", err)
	}
	fmt.Printf("    ⭐ High ratings (4+): %v (%d results)\n", ratingQueryTime, len(results))

	// Bitmap indexes for low-cardinality data
	fmt.Println("\n  🗂️ Creating Bitmap indexes for categorical data...")

	// Category bitmap index
	err = table.CreateIndexWithName([]string{"category"}, lancedb.IndexTypeBitmap, "category_bitmap_idx")
	if err != nil {
		return fmt.Errorf("failed to create category bitmap index: %w", err)
	}
	fmt.Println("  ✅ Category Bitmap index created")

	// Status bitmap index
	err = table.CreateIndexWithName([]string{"status"}, lancedb.IndexTypeBitmap, "status_bitmap_idx")
	if err != nil {
		return fmt.Errorf("failed to create status bitmap index: %w", err)
	}
	fmt.Println("  ✅ Status Bitmap index created")

	// Test categorical queries with bitmap indexes
	fmt.Println("\n  🔍 Testing Bitmap index performance on categorical queries...")

	start = time.Now()
	results, err = table.SelectWithFilter("category = 'Technology'")
	categoryQueryTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("category query failed: %w", err)
	}
	fmt.Printf("    💻 Technology category: %v (%d results)\n", categoryQueryTime, len(results))

	start = time.Now()
	results, err = table.SelectWithFilter("status IN ('published', 'featured')")
	statusQueryTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("status query failed: %w", err)
	}
	fmt.Printf("    📰 Published/Featured status: %v (%d results)\n", statusQueryTime, len(results))

	// Note: LabelList indexes are not currently supported in the Go SDK
	fmt.Println("\n  🏷️ Note: LabelList indexes for tag-based queries...")
	fmt.Println("  📝 LabelList indexes are not yet supported in the Go SDK")
	fmt.Println("      For tag queries, use string filters with LIKE patterns:")

	start = time.Now()
	results, err = table.SelectWithFilter("tags LIKE '%trending%'")
	tagsQueryTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("tags query failed: %w", err)
	}
	fmt.Printf("    🔥 Trending tags (string filter): %v (%d results)\n", tagsQueryTime, len(results))

	fmt.Println("\n  📋 Scalar Index Recommendations:")
	fmt.Println("    • BTree: Range queries, numerical data, ordered operations")
	fmt.Println("    • Bitmap: Low-cardinality categorical data, multiple equality filters")
	fmt.Println("    • Label List: Tag-based systems (not yet available in Go SDK - use string filters)")

	return nil
}

func demonstrateFullTextIndexes(table *lancedb.Table) error {
	fmt.Println("  📝 Full-Text Search Indexes")

	// Create FTS indexes on text fields
	fmt.Println("  🔧 Creating Full-Text Search indexes...")

	// Title FTS index
	err := table.CreateIndexWithName([]string{"title"}, lancedb.IndexTypeFts, "title_fts_idx")
	if err != nil {
		return fmt.Errorf("failed to create title FTS index: %w", err)
	}
	fmt.Println("  ✅ Title FTS index created")

	// Content FTS index
	err = table.CreateIndexWithName([]string{"content"}, lancedb.IndexTypeFts, "content_fts_idx")
	if err != nil {
		return fmt.Errorf("failed to create content FTS index: %w", err)
	}
	fmt.Println("  ✅ Content FTS index created")

	fmt.Println("\n  🔍 Testing Full-Text Search capabilities...")

	// Test text search queries
	textQueries := []string{
		"artificial intelligence",
		"machine learning",
		"healthcare technology",
		"financial planning",
	}

	for _, query := range textQueries {
		fmt.Printf("  🔎 Searching for: '%s'\n", query)

		// Search in titles
		start := time.Now()
		results, err := table.SelectWithFilter(fmt.Sprintf("title LIKE '%%%s%%'", query))
		titleSearchTime := time.Since(start)
		if err != nil {
			fmt.Printf("    ⚠️ Title search failed: %v\n", err)
			continue
		}

		// Search in content
		start = time.Now()
		contentResults, err := table.SelectWithFilter(fmt.Sprintf("content LIKE '%%%s%%'", query))
		contentSearchTime := time.Since(start)
		if err != nil {
			fmt.Printf("    ⚠️ Content search failed: %v\n", err)
			continue
		}

		fmt.Printf("    📄 Title matches: %d (%v)\n", len(results), titleSearchTime)
		fmt.Printf("    📝 Content matches: %d (%v)\n", len(contentResults), contentSearchTime)

		// Show sample results
		if len(results) > 0 {
			fmt.Printf("    📋 Sample title match: %s\n", results[0]["title"])
		}
		fmt.Println()
	}

	fmt.Println("  📋 Full-Text Search Recommendations:")
	fmt.Println("    • Use FTS indexes for text search, keyword matching, content discovery")
	fmt.Println("    • Combine with vector search for hybrid semantic + keyword search")
	fmt.Println("    • Consider field-specific indexes for title vs content searches")

	return nil
}

func performanceComparison(table *lancedb.Table) error {
	fmt.Println("  ⚡ Index Performance Comparison")

	queryVector := generateDocumentEmbedding("performance test", "benchmark query example", "Technology")

	// Vector search performance with different indexes
	fmt.Println("  🎯 Vector Search Performance:")

	vectorQueries := 5
	start := time.Now()
	for i := 0; i < vectorQueries; i++ {
		_, err := table.VectorSearch("vector", queryVector, 10)
		if err != nil {
			return fmt.Errorf("vector search benchmark failed: %w", err)
		}
	}
	avgVectorTime := time.Since(start) / time.Duration(vectorQueries)
	fmt.Printf("    🎯 Average vector search: %v\n", avgVectorTime)

	// Scalar query performance
	fmt.Println("\n  📊 Scalar Query Performance:")

	scalarQueries := []struct {
		name   string
		filter string
	}{
		{"Price Range", "price BETWEEN 200 AND 800"},
		{"Category Filter", "category = 'Technology'"},
		{"Rating Filter", "rating >= 4"},
		{"Status Filter", "status = 'published'"},
		{"Tag Search", "tags LIKE '%trending%'"},
	}

	for _, query := range scalarQueries {
		start = time.Now()
		results, err := table.SelectWithFilter(query.filter)
		queryTime := time.Since(start)
		if err != nil {
			fmt.Printf("    ⚠️ %s failed: %v\n", query.name, err)
			continue
		}
		fmt.Printf("    📊 %s: %v (%d results)\n", query.name, queryTime, len(results))
	}

	// Hybrid query performance
	fmt.Println("\n  🔀 Hybrid Query Performance:")
	limit := 5
	config := lancedb.QueryConfig{
		Columns: []string{"id", "title", "category", "price", "rating"},
		Where:   "category = 'Technology' AND price < 500 AND rating >= 4",
		Limit:   &limit,
		VectorSearch: &lancedb.VectorSearch{
			Column: "vector",
			Vector: queryVector,
			K:      20,
		},
	}

	start = time.Now()
	results, err := table.Select(config)
	hybridTime := time.Since(start)
	if err != nil {
		return fmt.Errorf("hybrid query failed: %w", err)
	}
	fmt.Printf("    🔀 Complex hybrid query: %v (%d results)\n", hybridTime, len(results))

	return nil
}

func indexManagementOperations(table *lancedb.Table) error {
	fmt.Println("  🛠️ Index Management Operations")

	// List all indexes
	fmt.Println("  📋 Current indexes on table:")
	indexes, err := table.GetAllIndexes()
	if err != nil {
		return fmt.Errorf("failed to get indexes: %w", err)
	}

	fmt.Printf("  📊 Total indexes: %d\n", len(indexes))
	for i, idx := range indexes {
		fmt.Printf("    %d. %s on %v (%s)\n", i+1, idx.Name, idx.Columns, idx.IndexType)
	}

	// Demonstrate index naming and organization
	fmt.Println("\n  🏷️ Index naming and organization best practices:")
	fmt.Println("    • Use descriptive names: 'price_btree_idx', 'category_bitmap_idx'")
	fmt.Println("    • Include index type in name for clarity")
	fmt.Println("    • Group related indexes with consistent naming conventions")
	fmt.Println("    • Document index purpose and query patterns they optimize")

	// Show example of checking if specific indexes exist
	fmt.Println("\n  🔍 Checking for specific indexes:")
	requiredIndexes := map[string][]string{
		"vector_ivf_pq_idx":   {"vector"},
		"price_btree_idx":     {"price"},
		"category_bitmap_idx": {"category"},
		"title_fts_idx":       {"title"},
	}

	indexMap := make(map[string]lancedb.IndexInfo)
	for _, idx := range indexes {
		indexMap[idx.Name] = idx
	}

	for indexName, columns := range requiredIndexes {
		if idx, exists := indexMap[indexName]; exists {
			fmt.Printf("    ✅ %s exists (columns: %v, type: %s)\n", indexName, idx.Columns, idx.IndexType)
		} else {
			fmt.Printf("    ❌ %s missing (would index: %v)\n", indexName, columns)
		}
	}

	return nil
}

func bestPracticesDemo(table *lancedb.Table) error {
	fmt.Println("  💡 Index Best Practices Demonstration")

	fmt.Println("  📋 Index Selection Guidelines:")
	fmt.Println()

	// Vector index guidelines
	fmt.Println("  🎯 Vector Indexes:")
	fmt.Println("    • Dataset < 100K: No index needed, linear search is fast")
	fmt.Println("    • Dataset 100K-1M: IVF-Flat for accuracy, IVF-PQ for speed/memory")
	fmt.Println("    • Dataset > 1M: IVF-PQ or HNSW-PQ for scalability")
	fmt.Println("    • Real-time queries: HNSW variants for sub-millisecond search")

	// Scalar index guidelines
	fmt.Println("\n  📊 Scalar Indexes:")
	fmt.Println("    • Range queries (price, date): BTree")
	fmt.Println("    • Equality filters on categories: Bitmap")
	fmt.Println("    • High cardinality unique values: BTree")
	fmt.Println("    • Low cardinality (<100 unique values): Bitmap")
	fmt.Println("    • Tag/label systems: Label List")

	// Text search guidelines
	fmt.Println("\n  📝 Text Search Indexes:")
	fmt.Println("    • Exact keyword matching: FTS")
	fmt.Println("    • Semantic search: Vector embeddings")
	fmt.Println("    • Hybrid text search: Both FTS + Vector")
	fmt.Println("    • Multi-language: Consider language-specific FTS config")

	// Performance optimization tips
	fmt.Println("\n  ⚡ Performance Optimization:")
	fmt.Println("    • Index frequently filtered columns")
	fmt.Println("    • Avoid over-indexing (storage + maintenance cost)")
	fmt.Println("    • Monitor query patterns and adjust indexes accordingly")
	fmt.Println("    • Use composite queries efficiently (vector + filter)")
	fmt.Println("    • Consider index maintenance during data updates")

	// Demonstrate query optimization
	fmt.Println("\n  🔧 Query Optimization Example:")

	// Inefficient query pattern
	fmt.Println("  ❌ Inefficient: Multiple separate queries")
	start := time.Now()

	// First get by category
	techDocs, err := table.SelectWithFilter("category = 'Technology'")
	if err != nil {
		return fmt.Errorf("category filter failed: %w", err)
	}

	// Then filter by price (simulated client-side filtering)
	expensiveTech := 0
	for _, doc := range techDocs {
		if price, ok := doc["price"].(float64); ok && price > 500 {
			expensiveTech++
		}
	}
	inefficientTime := time.Since(start)

	// Efficient query pattern
	fmt.Println("  ✅ Efficient: Combined filter query")
	start = time.Now()
	efficientResults, err := table.SelectWithFilter("category = 'Technology' AND price > 500")
	if err != nil {
		return fmt.Errorf("combined filter failed: %w", err)
	}
	efficientTime := time.Since(start)

	fmt.Printf("    Inefficient approach: %v (%d results)\n", inefficientTime, expensiveTech)
	fmt.Printf("    Efficient approach: %v (%d results)\n", efficientTime, len(efficientResults))
	fmt.Printf("    Performance improvement: %.2fx faster\n",
		float64(inefficientTime)/float64(efficientTime))

	// Index maintenance recommendations
	fmt.Println("\n  🛠️ Index Maintenance:")
	fmt.Println("    • Monitor index usage and performance metrics")
	fmt.Println("    • Remove unused indexes to save storage and update performance")
	fmt.Println("    • Rebuild indexes periodically for optimal performance")
	fmt.Println("    • Consider index fragmentation with frequent updates")
	fmt.Println("    • Plan index creation during low-traffic periods")

	return nil
}
