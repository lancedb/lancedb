// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

// Basic CRUD Operations Example
//
// This example demonstrates fundamental Create, Read, Update, Delete operations
// with LanceDB using the Go SDK. It covers:
// - Creating a database connection
// - Defining and creating a table schema
// - Inserting data (Create)
// - Querying data (Read)
// - Updating existing records (Update)
// - Deleting records (Delete)

package main

import (
	"context"
	"fmt"
	lancedb "github.com/lancedb/lancedb/pkg"
	"log"
	"os"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

func main() {
	fmt.Println("🚀 LanceDB Go SDK - Basic CRUD Operations Example")
	fmt.Println("==================================================")

	// Setup context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create temporary directory for this example
	tempDir, err := os.MkdirTemp("", "lancedb_crud_example_")
	if err != nil {
		log.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)
	fmt.Printf("📂 Using database directory: %s\n", tempDir)

	// Step 1: Connect to database
	fmt.Println("\n📋 Step 1: Connecting to LanceDB...")
	conn, err := lancedb.Connect(ctx, tempDir, nil)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer conn.Close()
	fmt.Println("✅ Connected to database successfully")

	// Step 2: Create table schema
	fmt.Println("\n📋 Step 2: Creating table schema...")
	fields := []arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		{Name: "score", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
		{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	}
	arrowSchema := arrow.NewSchema(fields, nil)
	schema, err := lancedb.NewSchema(arrowSchema)
	if err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}
	fmt.Println("✅ Schema created with fields: id, name, email, age, score, active")

	// Step 3: Create table
	fmt.Println("\n📋 Step 3: Creating table 'users'...")
	table, err := conn.CreateTable(ctx, "users", *schema)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	defer table.Close()
	fmt.Println("✅ Table 'users' created successfully")

	// Step 4: INSERT (Create) - Add initial data
	fmt.Println("\n📋 Step 4: INSERT - Adding initial data...")
	if err := insertInitialData(table, arrowSchema); err != nil {
		log.Fatalf("Failed to insert initial data: %v", err)
	}
	fmt.Println("✅ Initial data inserted successfully")

	// Step 5: SELECT (Read) - Query data
	fmt.Println("\n📋 Step 5: SELECT - Querying data...")
	if err := demonstrateRead(table); err != nil {
		log.Fatalf("Failed to demonstrate read operations: %v", err)
	}

	// Step 6: UPDATE - Modify existing data
	fmt.Println("\n📋 Step 6: UPDATE - Modifying existing data...")
	if err := demonstrateUpdate(table); err != nil {
		log.Fatalf("Failed to demonstrate update operations: %v", err)
	}

	// Step 7: DELETE - Remove data
	fmt.Println("\n📋 Step 7: DELETE - Removing data...")
	if err := demonstrateDelete(table); err != nil {
		log.Fatalf("Failed to demonstrate delete operations: %v", err)
	}

	// Step 8: Final verification
	fmt.Println("\n📋 Step 8: Final verification...")
	count, err := table.Count()
	if err != nil {
		log.Fatalf("Failed to get final count: %v", err)
	}
	fmt.Printf("📊 Final record count: %d\n", count)

	fmt.Println("\n🎉 Basic CRUD operations completed successfully!")
	fmt.Println("==================================================")
}

func insertInitialData(table *lancedb.Table, schema *arrow.Schema) error {
	pool := memory.NewGoAllocator()

	// Create sample data
	idBuilder := array.NewInt32Builder(pool)
	idBuilder.AppendValues([]int32{1, 2, 3, 4, 5}, nil)
	idArray := idBuilder.NewArray()
	defer idArray.Release()

	nameBuilder := array.NewStringBuilder(pool)
	nameBuilder.AppendValues([]string{"Alice Smith", "Bob Johnson", "Carol Davis", "David Wilson", "Eve Brown"}, nil)
	nameArray := nameBuilder.NewArray()
	defer nameArray.Release()

	emailBuilder := array.NewStringBuilder(pool)
	emailBuilder.AppendValues([]string{"alice@example.com", "bob@example.com", "carol@example.com", "david@example.com", "eve@example.com"}, nil)
	emailArray := emailBuilder.NewArray()
	defer emailArray.Release()

	ageBuilder := array.NewInt32Builder(pool)
	ageBuilder.AppendValues([]int32{28, 34, 29, 42, 31}, nil)
	ageArray := ageBuilder.NewArray()
	defer ageArray.Release()

	scoreBuilder := array.NewFloat64Builder(pool)
	scoreBuilder.AppendValues([]float64{85.5, 92.0, 88.3, 76.8, 94.2}, nil)
	scoreArray := scoreBuilder.NewArray()
	defer scoreArray.Release()

	activeBuilder := array.NewBooleanBuilder(pool)
	activeBuilder.AppendValues([]bool{true, true, false, true, true}, nil)
	activeArray := activeBuilder.NewArray()
	defer activeArray.Release()

	// Create record and insert
	columns := []arrow.Array{idArray, nameArray, emailArray, ageArray, scoreArray, activeArray}
	record := array.NewRecord(schema, columns, 5)
	defer record.Release()

	return table.Add(record, nil)
}

func demonstrateRead(table *lancedb.Table) error {
	fmt.Println("  📖 Reading all records...")
	results, err := table.Select(lancedb.QueryConfig{})
	if err != nil {
		return fmt.Errorf("failed to select all records: %w", err)
	}

	fmt.Printf("  📊 Found %d total records\n", len(results))
	for i, row := range results {
		fmt.Printf("    %d. ID: %v, Name: %v, Email: %v, Age: %v, Score: %v, Active: %v\n",
			i+1, row["id"], row["name"], row["email"], row["age"], row["score"], row["active"])
	}

	fmt.Println("\n  📖 Reading filtered records (score > 85)...")
	results, err = table.SelectWithFilter("score > 85")
	if err != nil {
		return fmt.Errorf("failed to select filtered records: %w", err)
	}

	fmt.Printf("  📊 Found %d records with score > 85\n", len(results))
	for i, row := range results {
		fmt.Printf("    %d. Name: %v, Score: %v\n", i+1, row["name"], row["score"])
	}

	fmt.Println("\n  📖 Reading specific columns...")
	results, err = table.SelectWithColumns([]string{"id", "name", "score"})
	if err != nil {
		return fmt.Errorf("failed to select specific columns: %w", err)
	}

	fmt.Printf("  📊 Retrieved %d records with selected columns\n", len(results))
	for i, row := range results {
		fmt.Printf("    %d. ID: %v, Name: %v, Score: %v\n", i+1, row["id"], row["name"], row["score"])
	}

	return nil
}

func demonstrateUpdate(table *lancedb.Table) error {
	fmt.Println("  ✏️ Updating score for user with ID = 1...")
	updates := map[string]interface{}{
		"score":  95.0,
		"active": true,
	}

	if err := table.Update("id = 1", updates); err != nil {
		return fmt.Errorf("failed to update record: %w", err)
	}
	fmt.Println("  ✅ Successfully updated user ID = 1")

	fmt.Println("\n  ✏️ Updating multiple records (increment age for active users)...")
	bulkUpdates := map[string]interface{}{
		"age": "age + 1", // This would need to be supported by the SQL engine
	}

	if err := table.Update("active = true", bulkUpdates); err != nil {
		// Note: This might not work depending on SQL expression support
		fmt.Printf("  ⚠️ Bulk update failed (expected): %v\n", err)
		fmt.Println("  💡 Individual updates would be needed for complex expressions")
	} else {
		fmt.Println("  ✅ Successfully updated multiple records")
	}

	// Verify update
	fmt.Println("\n  📖 Verifying updates...")
	results, err := table.SelectWithFilter("id = 1")
	if err != nil {
		return fmt.Errorf("failed to verify update: %w", err)
	}

	if len(results) > 0 {
		fmt.Printf("  ✅ Updated record: ID: %v, Score: %v, Active: %v\n",
			results[0]["id"], results[0]["score"], results[0]["active"])
	}

	return nil
}

func demonstrateDelete(table *lancedb.Table) error {
	fmt.Println("  🗑️ Deleting inactive users...")

	// First, check how many inactive users we have
	results, err := table.SelectWithFilter("active = false")
	if err != nil {
		return fmt.Errorf("failed to count inactive users: %w", err)
	}
	fmt.Printf("  📊 Found %d inactive users to delete\n", len(results))

	// Delete inactive users
	if err := table.Delete("active = false"); err != nil {
		return fmt.Errorf("failed to delete inactive users: %w", err)
	}
	fmt.Println("  ✅ Successfully deleted inactive users")

	fmt.Println("\n  🗑️ Deleting user with specific condition...")
	if err := table.Delete("score < 80"); err != nil {
		return fmt.Errorf("failed to delete low-score users: %w", err)
	}
	fmt.Println("  ✅ Successfully deleted users with score < 80")

	// Verify deletion
	count, err := table.Count()
	if err != nil {
		return fmt.Errorf("failed to get count after deletion: %w", err)
	}
	fmt.Printf("  📊 Records remaining after deletion: %d\n", count)

	// Show remaining records
	fmt.Println("\n  📖 Remaining records after deletion...")
	results, err = table.Select(lancedb.QueryConfig{})
	if err != nil {
		return fmt.Errorf("failed to select remaining records: %w", err)
	}

	for i, row := range results {
		fmt.Printf("    %d. ID: %v, Name: %v, Score: %v, Active: %v\n",
			i+1, row["id"], row["name"], row["score"], row["active"])
	}

	return nil
}
