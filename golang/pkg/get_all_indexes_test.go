// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

package lancedb

import (
	"context"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func TestGetAllIndexes(t *testing.T) {
	// Setup test database
	tempDir, err := os.MkdirTemp("", "lancedb_test_indexes_")
	if err != nil {
		log.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Connect to database
	conn, err := Connect(context.Background(), tempDir, nil)
	if err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer conn.Close()

	// Create test schema with vector field
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "category", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "embedding", Type: arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32), Nullable: false},
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	schema, err := NewSchema(arrowSchema)
	if err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}

	// Create table
	table, err := conn.CreateTable(context.Background(), "test_indexes", *schema)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	defer table.Close()

	// Add some sample data
	fmt.Println("Adding sample data...")
	pool := memory.NewGoAllocator()

	// Create sample data
	idBuilder := array.NewInt32Builder(pool)
	idBuilder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	nameBuilder := array.NewStringBuilder(pool)
	nameBuilder.AppendValues([]string{"Alice", "Bob", "Charlie", "Diana", "Eve"}, nil)
	nameArray := nameBuilder.NewArray()
	defer nameArray.Release()

	categoryBuilder := array.NewStringBuilder(pool)
	categoryBuilder.AppendValues([]string{"A", "B", "A", "C", "B"}, nil)
	categoryArray := categoryBuilder.NewArray()
	defer categoryArray.Release()

	scoreBuilder := array.NewFloat64Builder(pool)
	scoreBuilder.AppendValues([]float64{95.5, 87.2, 92.8, 88.9, 94.1}, nil)
	scoreArray := scoreBuilder.NewArray()
	defer scoreArray.Release()

	// Create vector embeddings (128-dimensional vectors)
	embeddingValues := make([]float32, 5*128) // 5 records * 128 dimensions
	for i := 0; i < 5; i++ {
		for j := 0; j < 128; j++ {
			// Create unique vector patterns for each record
			embeddingValues[i*128+j] = float32(i)*0.1 + float32(j)*0.001
		}
	}

	// Create Float32Array for all embedding values
	embeddingFloat32Builder := array.NewFloat32Builder(pool)
	embeddingFloat32Builder.AppendValues(embeddingValues, nil)
	embeddingFloat32Array := embeddingFloat32Builder.NewArray()
	defer embeddingFloat32Array.Release()

	// Create FixedSizeListArray for embeddings
	embeddingListType := arrow.FixedSizeListOf(128, arrow.PrimitiveTypes.Float32)
	embeddingArray := array.NewFixedSizeListData(
		array.NewData(embeddingListType, 5, []*memory.Buffer{nil}, []arrow.ArrayData{embeddingFloat32Array.Data()}, 0, 0),
	)
	defer embeddingArray.Release()

	// Create Arrow Record
	columns := []arrow.Array{idArray, nameArray, categoryArray, scoreArray, embeddingArray}
	record := array.NewRecord(arrowSchema, columns, 5)
	defer record.Release()

	// Add data to table
	err = table.Add(record, nil)
	if err != nil {
		log.Fatalf("Failed to add data: %v", err)
	}
	fmt.Println("✅ Sample data added successfully")

	// Test GetAllIndexes on empty table (should return empty list)
	fmt.Println("\n📋 Testing GetAllIndexes on table with no indexes...")
	indexes, err := table.GetAllIndexes()
	if err != nil {
		log.Fatalf("Failed to get indexes: %v", err)
	}

	fmt.Printf("Found %d indexes (expected 0):\n", len(indexes))
	for i, idx := range indexes {
		fmt.Printf("  %d. Name: %s, Columns: %v, Type: %s\n", i+1, idx.Name, idx.Columns, idx.IndexType)
	}

	// Create some indexes
	fmt.Println("\n🔧 Creating various indexes...")

	indexesToCreate := []struct {
		name        string
		columns     []string
		indexType   IndexType
		customName  string
		description string
	}{
		{
			name:        "BTree Index",
			columns:     []string{"id"},
			indexType:   IndexTypeBTree,
			customName:  "id_btree_idx",
			description: "BTree index on ID field",
		},
		{
			name:        "Bitmap Index",
			columns:     []string{"category"},
			indexType:   IndexTypeBitmap,
			customName:  "category_bitmap_idx",
			description: "Bitmap index on category field",
		},
		{
			name:        "FTS Index",
			columns:     []string{"name"},
			indexType:   IndexTypeFts,
			customName:  "name_fts_idx",
			description: "Full-text search on name field",
		},
		{
			name:        "Vector Index (IVF_PQ)",
			columns:     []string{"embedding"},
			indexType:   IndexTypeIvfPq,
			customName:  "embedding_ivf_pq_idx",
			description: "IVF_PQ vector index on embedding field",
		},
		{
			name:        "Vector Index (IVF_Flat)",
			columns:     []string{"embedding"},
			indexType:   IndexTypeIvfFlat,
			customName:  "embedding_ivf_flat_idx",
			description: "IVF_Flat vector index for exact search",
		},
		{
			name:        "Vector Index (HNSW_PQ)",
			columns:     []string{"embedding"},
			indexType:   IndexTypeHnswPq,
			customName:  "embedding_hnsw_pq_idx",
			description: "HNSW_PQ vector index for high performance",
		},
	}

	// Create each index
	for _, indexSpec := range indexesToCreate {
		fmt.Printf("\nCreating %s...\n", indexSpec.description)
		fmt.Printf("  Columns: %v\n", indexSpec.columns)
		fmt.Printf("  Type: %v\n", indexSpec.indexType)
		fmt.Printf("  Custom Name: %s\n", indexSpec.customName)

		err = table.CreateIndexWithName(indexSpec.columns, indexSpec.indexType, indexSpec.customName)
		if err != nil {
			fmt.Printf("  ❌ Failed to create %s: %v\n", indexSpec.name, err)
			continue
		}
		fmt.Printf("  ✅ %s created successfully\n", indexSpec.name)

		// Test GetAllIndexes after each index creation
		fmt.Printf("  📋 Checking indexes after creating %s...\n", indexSpec.name)
		indexes, err := table.GetAllIndexes()
		if err != nil {
			fmt.Printf("  ❌ Failed to get indexes: %v\n", err)
			continue
		}
		fmt.Printf("  Found %d indexes:\n", len(indexes))
		for i, idx := range indexes {
			fmt.Printf("    %d. Name: %s, Columns: %v, Type: %s\n", i+1, idx.Name, idx.Columns, idx.IndexType)
		}
	}

	// Final check - get all indexes
	fmt.Println("\n📊 Final GetAllIndexes test...")
	finalIndexes, err := table.GetAllIndexes()
	if err != nil {
		log.Fatalf("Failed to get final indexes: %v", err)
	}

	fmt.Printf("🎯 Total indexes on table: %d\n", len(finalIndexes))
	if len(finalIndexes) > 0 {
		fmt.Println("Index details:")
		for i, idx := range finalIndexes {
			fmt.Printf("  %d. Name: %s\n", i+1, idx.Name)
			fmt.Printf("     Columns: %v\n", idx.Columns)
			fmt.Printf("     Type: %s\n", idx.IndexType)
			fmt.Println()
		}
	}

	// Test error cases
	fmt.Println("🧪 Testing error cases...")

	// Test GetAllIndexes on closed table
	table.Close()
	_, err = table.GetAllIndexes()
	if err != nil {
		fmt.Printf("✅ Correctly caught closed table error: %v\n", err)
	} else {
		fmt.Println("❌ Should have failed on closed table")
	}

	fmt.Println("\n🎉 GetAllIndexes functionality test completed successfully!")
}
