package lancedb

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

// setupTestDB creates a temporary database for testing
func setupTestDB(t *testing.T) (*Connection, func()) {
	t.Helper()

	tmpDir, err := os.MkdirTemp("", "lancedb_table_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}

	dbPath := filepath.Join(tmpDir, "test.lance")
	conn, err := Connect(context.Background(), dbPath, nil)
	if err != nil {
		t.Fatalf("Failed to connect to database: %v", err)
	}

	cleanup := func() {
		conn.Close()
		os.RemoveAll(tmpDir)
	}

	return conn, cleanup
}

// createTestTable creates a table with a comprehensive schema for testing
func createTestTable(t *testing.T, conn *Connection, name string) *Table {
	t.Helper()

	schema, err := NewSchemaBuilder().
		AddInt32Field("id", false).
		AddStringField("name", true).
		AddFloat32Field("score", true).
		AddVectorField("embedding", 128, VectorDataTypeFloat32, false).
		AddBooleanField("active", true).
		Build()
	if err != nil {
		t.Fatalf("Failed to create schema: %v", err)
	}

	table, err := conn.CreateTable(context.Background(), name, *schema)
	if err != nil {
		t.Fatalf("Failed to create table: %v", err)
	}

	return table
}

func TestTableName(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	table := createTestTable(t, conn, "test_table_name")
	defer table.Close()

	if table.Name() != "test_table_name" {
		t.Errorf("Expected table name 'test_table_name', got '%s'", table.Name())
	}
}

func TestTableIsOpen(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	table := createTestTable(t, conn, "test_is_open")

	// Table should be open initially
	if !table.IsOpen() {
		t.Error("Table should be open after creation")
	}

	// Close the table
	err := table.Close()
	if err != nil {
		t.Fatalf("Failed to close table: %v", err)
	}

	// Table should be closed now
	if table.IsOpen() {
		t.Error("Table should be closed after calling Close()")
	}
}

func TestTableSchema(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	table := createTestTable(t, conn, "test_schema")
	defer table.Close()

	schema, err := table.Schema()
	if err != nil {
		t.Fatalf("Failed to get table schema: %v", err)
	}

	// Verify expected fields
	expectedFields := []string{"id", "name", "score", "embedding", "active"}
	if schema.NumFields() != len(expectedFields) {
		t.Fatalf("Expected %d fields, got %d", len(expectedFields), schema.NumFields())
	}

	for i, expectedName := range expectedFields {
		field := schema.Field(i)
		if field.Name != expectedName {
			t.Errorf("Expected field %d to be '%s', got '%s'", i, expectedName, field.Name)
		}
	}

	t.Logf("Successfully retrieved schema with %d fields", schema.NumFields())
}

func TestTableCount(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	table := createTestTable(t, conn, "test_count")
	defer table.Close()

	// Empty table should have 0 rows
	count, err := table.Count()
	if err != nil {
		t.Fatalf("Failed to count rows: %v", err)
	}

	if count != 0 {
		t.Errorf("Expected 0 rows in empty table, got %d", count)
	}

	t.Logf("Table row count: %d", count)
}

func TestTableVersion(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	table := createTestTable(t, conn, "test_version")
	defer table.Close()

	version, err := table.Version()
	if err != nil {
		t.Fatalf("Failed to get table version: %v", err)
	}

	if version < 0 {
		t.Errorf("Expected non-negative version, got %d", version)
	}

	t.Logf("Table version: %d", version)
}

func TestTableClose(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	table := createTestTable(t, conn, "test_close")

	// Close the table
	err := table.Close()
	if err != nil {
		t.Fatalf("Failed to close table: %v", err)
	}

	// Operations on closed table should fail
	_, err = table.Count()
	if err == nil {
		t.Error("Expected error when calling Count() on closed table")
	}

	_, err = table.Schema()
	if err == nil {
		t.Error("Expected error when calling Schema() on closed table")
	}

	_, err = table.Version()
	if err == nil {
		t.Error("Expected error when calling Version() on closed table")
	}
}

func TestOpenTable(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	// Create a table
	table1 := createTestTable(t, conn, "test_open_table")
	defer table1.Close()

	// Open the same table with a new handle
	table2, err := conn.OpenTable(context.Background(), "test_open_table")
	if err != nil {
		t.Fatalf("Failed to open existing table: %v", err)
	}
	defer table2.Close()

	// Both tables should have the same name
	if table1.Name() != table2.Name() {
		t.Errorf("Table names should match: '%s' vs '%s'", table1.Name(), table2.Name())
	}

	// Both tables should refer to the same data
	count1, err := table1.Count()
	if err != nil {
		t.Fatalf("Failed to count rows in table1: %v", err)
	}

	count2, err := table2.Count()
	if err != nil {
		t.Fatalf("Failed to count rows in table2: %v", err)
	}

	if count1 != count2 {
		t.Errorf("Row counts should match: %d vs %d", count1, count2)
	}

	// Both tables should have the same version
	version1, err := table1.Version()
	if err != nil {
		t.Fatalf("Failed to get version from table1: %v", err)
	}

	version2, err := table2.Version()
	if err != nil {
		t.Fatalf("Failed to get version from table2: %v", err)
	}

	if version1 != version2 {
		t.Errorf("Versions should match: %d vs %d", version1, version2)
	}
}

func TestTableLifecycle(t *testing.T) {
	conn, cleanup := setupTestDB(t)
	defer cleanup()

	// Create table
	table := createTestTable(t, conn, "test_lifecycle")

	// Test initial state
	if !table.IsOpen() {
		t.Error("Table should be open after creation")
	}

	if table.Name() != "test_lifecycle" {
		t.Errorf("Expected table name 'test_lifecycle', got '%s'", table.Name())
	}

	// Test operations work
	count, err := table.Count()
	if err != nil {
		t.Fatalf("Count should work on open table: %v", err)
	}

	version, err := table.Version()
	if err != nil {
		t.Fatalf("Version should work on open table: %v", err)
	}

	schema, err := table.Schema()
	if err != nil {
		t.Fatalf("Schema should work on open table: %v", err)
	}

	t.Logf("Table lifecycle test - Count: %d, Version: %d, Fields: %d",
		count, version, schema.NumFields())

	// Close table
	err = table.Close()
	if err != nil {
		t.Fatalf("Failed to close table: %v", err)
	}

	// Verify a closed state
	if table.IsOpen() {
		t.Error("Table should be closed after Close()")
	}

	// Verify operations fail on a closed table
	_, err = table.Count()
	if err == nil {
		t.Error("Operations should fail on closed table")
	}
}
