// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The LanceDB Authors

package lancedb

/*
#cgo CFLAGS: -I${SRCDIR}/../target/generated/include
#cgo darwin LDFLAGS: -L${SRCDIR}/../target/generated/lib -llancedb_go -framework Security -framework CoreFoundation
#cgo linux LDFLAGS: -L${SRCDIR}/../target/generated/lib -llancedb_go
#include "lancedb.h"
*/
import "C"

import (
	"encoding/json"
	"fmt"
	"runtime"
	"strings"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

// AddDataOptions configures how data is added to a Table
type AddDataOptions struct {
	Mode WriteMode
}

// WriteMode specifies how data should be written to a Table
type WriteMode int

const (
	WriteModeAppend WriteMode = iota
	WriteModeOverwrite
)

// Name returns the name of the Table
func (t *Table) Name() string {
	return t.name
}

// IsOpen returns true if the Table is still open
func (t *Table) IsOpen() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return !t.closed && !t.connection.closed
}

// Close closes the Table and releases resources
func (t *Table) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.closed || t.handle == nil {
		return nil
	}

	result := C.simple_lancedb_table_close(t.handle)
	defer C.simple_lancedb_result_free(result)

	t.handle = nil
	t.closed = true
	runtime.SetFinalizer(t, nil)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return fmt.Errorf("failed to close table: %s", errorMsg)
		}
		return fmt.Errorf("failed to close table: unknown error")
	}

	return nil
}

// Schema returns the schema of the Table
func (t *Table) Schema() (*arrow.Schema, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed || t.handle == nil {
		return nil, fmt.Errorf("table is closed")
	}

	var schemaJSON *C.char
	result := C.simple_lancedb_table_schema(t.handle, &schemaJSON)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return nil, fmt.Errorf("failed to get table schema: %s", errorMsg)
		}
		return nil, fmt.Errorf("failed to get table schema: unknown error")
	}

	if schemaJSON == nil {
		return nil, fmt.Errorf("received null schema")
	}

	jsonStr := C.GoString(schemaJSON)
	C.simple_lancedb_free_string(schemaJSON)

	// Parse JSON schema and convert to Arrow schema
	var schemaObj struct {
		Fields []struct {
			Name     string `json:"name"`
			Type     string `json:"type"`
			Nullable bool   `json:"nullable"`
		} `json:"fields"`
	}

	if err := json.Unmarshal([]byte(jsonStr), &schemaObj); err != nil {
		return nil, fmt.Errorf("failed to parse schema JSON: %w", err)
	}

	fields := make([]arrow.Field, 0, len(schemaObj.Fields))
	for _, field := range schemaObj.Fields {
		dataType, err := t.stringToArrowType(field.Type)
		if err != nil {
			return nil, fmt.Errorf("failed to convert field %s type %s: %w", field.Name, field.Type, err)
		}
		fields = append(fields, arrow.Field{
			Name:     field.Name,
			Type:     dataType,
			Nullable: field.Nullable,
		})
	}

	return arrow.NewSchema(fields, nil), nil
}

// Add inserts data into the Table
func (t *Table) Add(record arrow.Record, options *AddDataOptions) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed || t.handle == nil {
		return fmt.Errorf("table is closed")
	}

	if record == nil {
		return fmt.Errorf("record cannot be nil")
	}

	// Convert Arrow Record to JSON
	jsonData, err := t.recordToJSON(record)
	if err != nil {
		return fmt.Errorf("failed to convert record to JSON: %w", err)
	}

	// Call Rust binding
	cJsonData := C.CString(jsonData)
	defer C.free(unsafe.Pointer(cJsonData))

	var addedCount C.int64_t
	result := C.simple_lancedb_table_add_json(t.handle, cJsonData, &addedCount)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return fmt.Errorf("failed to add data: %s", errorMsg)
		}
		return fmt.Errorf("failed to add data: unknown error")
	}

	return nil
}

// AddRecords is a convenience method to add multiple records
func (t *Table) AddRecords(records []arrow.Record, options *AddDataOptions) error {
	for _, record := range records {
		if err := t.Add(record, options); err != nil {
			return err
		}
	}
	return nil
}

// Query creates a new query builder for this Table
func (t *Table) Query() *QueryBuilder {
	return &QueryBuilder{
		table:   t,
		filters: make([]string, 0),
		limit:   -1,
	}
}


// Count returns the number of rows in the Table
func (t *Table) Count() (int64, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed || t.handle == nil {
		return 0, fmt.Errorf("table is closed")
	}

	var count C.int64_t
	result := C.simple_lancedb_table_count_rows(t.handle, &count)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return 0, fmt.Errorf("failed to count rows: %s", errorMsg)
		}
		return 0, fmt.Errorf("failed to count rows: unknown error")
	}

	return int64(count), nil
}

// Version returns the current version of the Table
func (t *Table) Version() (int, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed || t.handle == nil {
		return 0, fmt.Errorf("table is closed")
	}

	var version C.int64_t
	result := C.simple_lancedb_table_version(t.handle, &version)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return 0, fmt.Errorf("failed to get table version: %s", errorMsg)
		}
		return 0, fmt.Errorf("failed to get table version: unknown error")
	}

	return int(version), nil
}

// Update updates records in the Table based on a filter
func (t *Table) Update(filter string, updates map[string]interface{}) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed || t.handle == nil {
		return fmt.Errorf("table is closed")
	}

	// Convert updates map to JSON
	updatesJSON, err := json.Marshal(updates)
	if err != nil {
		return fmt.Errorf("failed to marshal updates to JSON: %w", err)
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))

	cUpdatesJSON := C.CString(string(updatesJSON))
	defer C.free(unsafe.Pointer(cUpdatesJSON))

	result := C.simple_lancedb_table_update(t.handle, cFilter, cUpdatesJSON)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return fmt.Errorf("failed to update rows: %s", errorMsg)
		}
		return fmt.Errorf("failed to update rows: unknown error")
	}

	return nil
}

// Delete deletes records from the Table based on a filter
func (t *Table) Delete(filter string) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed || t.handle == nil {
		return fmt.Errorf("table is closed")
	}

	cFilter := C.CString(filter)
	defer C.free(unsafe.Pointer(cFilter))

	var deletedCount C.int64_t
	result := C.simple_lancedb_table_delete(t.handle, cFilter, &deletedCount)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return fmt.Errorf("failed to delete rows: %s", errorMsg)
		}
		return fmt.Errorf("failed to delete rows: unknown error")
	}

	// Note: deletedCount is set to -1 in the Rust implementation since LanceDB doesn't expose the count
	// We could return the count if needed, but for now we just ensure the operation succeeded
	return nil
}

// CreateIndex creates an index on the specified columns
func (t *Table) CreateIndex(columns []string, indexType IndexType) error {
	return t.CreateIndexWithName(columns, indexType, "")
}

// CreateIndexWithName creates an index on the specified columns with an optional name
func (t *Table) CreateIndexWithName(columns []string, indexType IndexType, name string) error {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed || t.handle == nil {
		return fmt.Errorf("table is closed")
	}

	if len(columns) == 0 {
		return fmt.Errorf("columns list cannot be empty")
	}

	// Convert columns to JSON
	columnsJSON, err := json.Marshal(columns)
	if err != nil {
		return fmt.Errorf("failed to marshal columns to JSON: %w", err)
	}

	// Convert index type to string
	indexTypeStr := t.indexTypeToString(indexType)

	cColumnsJSON := C.CString(string(columnsJSON))
	defer C.free(unsafe.Pointer(cColumnsJSON))

	cIndexType := C.CString(indexTypeStr)
	defer C.free(unsafe.Pointer(cIndexType))

	var cIndexName *C.char
	if name != "" {
		cIndexName = C.CString(name)
		defer C.free(unsafe.Pointer(cIndexName))
	}

	result := C.simple_lancedb_table_create_index(t.handle, cColumnsJSON, cIndexType, cIndexName)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return fmt.Errorf("failed to create index: %s", errorMsg)
		}
		return fmt.Errorf("failed to create index: unknown error")
	}

	return nil
}

// GetAllIndexes returns information about all indexes created on this table
func (t *Table) GetAllIndexes() ([]IndexInfo, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed || t.handle == nil {
		return nil, fmt.Errorf("table is closed")
	}

	var indexesJSON *C.char
	result := C.simple_lancedb_table_get_indexes(t.handle, &indexesJSON)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return nil, fmt.Errorf("failed to get indexes: %s", errorMsg)
		}
		return nil, fmt.Errorf("failed to get indexes: unknown error")
	}

	if indexesJSON == nil {
		return []IndexInfo{}, nil // Return empty slice if no indexes
	}

	jsonStr := C.GoString(indexesJSON)
	C.simple_lancedb_free_string(indexesJSON)

	// Parse JSON response
	var indexes []IndexInfo
	if err := json.Unmarshal([]byte(jsonStr), &indexes); err != nil {
		return nil, fmt.Errorf("failed to parse indexes JSON: %w", err)
	}

	return indexes, nil
}

// Select executes a select query with various predicates (vector search, filters, etc.)
func (t *Table) Select(config QueryConfig) ([]map[string]interface{}, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.closed || t.handle == nil {
		return nil, fmt.Errorf("table is closed")
	}

	// Convert QueryConfig to JSON
	configJSON, err := json.Marshal(config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal query config to JSON: %w", err)
	}

	cConfigJSON := C.CString(string(configJSON))
	defer C.free(unsafe.Pointer(cConfigJSON))

	var resultJSON *C.char
	result := C.simple_lancedb_table_select_query(t.handle, cConfigJSON, &resultJSON)
	defer C.simple_lancedb_result_free(result)

	if !result.SUCCESS {
		if result.ERROR_MESSAGE != nil {
			errorMsg := C.GoString(result.ERROR_MESSAGE)
			return nil, fmt.Errorf("failed to execute select query: %s", errorMsg)
		}
		return nil, fmt.Errorf("failed to execute select query: unknown error")
	}

	if resultJSON == nil {
		return []map[string]interface{}{}, nil // Return empty slice if no results
	}

	jsonStr := C.GoString(resultJSON)
	C.simple_lancedb_free_string(resultJSON)

	// Parse JSON response
	var rows []map[string]interface{}
	if err := json.Unmarshal([]byte(jsonStr), &rows); err != nil {
		return nil, fmt.Errorf("failed to parse query results JSON: %w", err)
	}

	return rows, nil
}

// SelectWithColumns is a convenience method for selecting specific columns
func (t *Table) SelectWithColumns(columns []string) ([]map[string]interface{}, error) {
	return t.Select(QueryConfig{
		Columns: columns,
	})
}

// SelectWithFilter is a convenience method for selecting with a WHERE filter
func (t *Table) SelectWithFilter(filter string) ([]map[string]interface{}, error) {
	return t.Select(QueryConfig{
		Where: filter,
	})
}

// VectorSearch is a convenience method for vector similarity search
func (t *Table) VectorSearch(column string, vector []float32, k int) ([]map[string]interface{}, error) {
	return t.Select(QueryConfig{
		VectorSearch: &VectorSearch{
			Column: column,
			Vector: vector,
			K:      k,
		},
	})
}

// VectorSearchWithFilter combines vector search with additional filtering
func (t *Table) VectorSearchWithFilter(column string, vector []float32, k int, filter string) ([]map[string]interface{}, error) {
	return t.Select(QueryConfig{
		VectorSearch: &VectorSearch{
			Column: column,
			Vector: vector,
			K:      k,
		},
		Where: filter,
	})
}

// FullTextSearch is a convenience method for full-text search
func (t *Table) FullTextSearch(column string, query string) ([]map[string]interface{}, error) {
	return t.Select(QueryConfig{
		FTSSearch: &FTSSearch{
			Column: column,
			Query:  query,
		},
	})
}

// FullTextSearchWithFilter combines full-text search with additional filtering
func (t *Table) FullTextSearchWithFilter(column string, query string, filter string) ([]map[string]interface{}, error) {
	return t.Select(QueryConfig{
		FTSSearch: &FTSSearch{
			Column: column,
			Query:  query,
		},
		Where: filter,
	})
}

// SelectWithLimit is a convenience method for selecting with limit and offset
func (t *Table) SelectWithLimit(limit int, offset int) ([]map[string]interface{}, error) {
	return t.Select(QueryConfig{
		Limit:  &limit,
		Offset: &offset,
	})
}

// IndexType represents the type of index to create
type IndexType int

const (
	IndexTypeAuto IndexType = iota
	IndexTypeIvfPq
	IndexTypeIvfFlat
	IndexTypeHnswPq
	IndexTypeHnswSq
	IndexTypeBTree
	IndexTypeBitmap
	IndexTypeLabelList
	IndexTypeFts
)

// IndexInfo represents information about an index on a table
type IndexInfo struct {
	Name      string   `json:"name"`
	Columns   []string `json:"columns"`
	IndexType string   `json:"index_type"`
}

// QueryConfig represents the configuration for a select query
type QueryConfig struct {
	Columns      []string      `json:"columns,omitempty"`
	Where        string        `json:"where,omitempty"`
	Limit        *int          `json:"limit,omitempty"`
	Offset       *int          `json:"offset,omitempty"`
	VectorSearch *VectorSearch `json:"vector_search,omitempty"`
	FTSSearch    *FTSSearch    `json:"fts_search,omitempty"`
}

// VectorSearch represents vector similarity search parameters
type VectorSearch struct {
	Column string    `json:"column"`
	Vector []float32 `json:"vector"`
	K      int       `json:"k"`
}

// FTSSearch represents full-text search parameters
type FTSSearch struct {
	Column string `json:"column"`
	Query  string `json:"query"`
}

// QueryResult represents the result of a select query
type QueryResult struct {
	Rows []map[string]interface{} `json:"rows"`
}

// indexTypeToString converts IndexType enum to string representation
func (t *Table) indexTypeToString(indexType IndexType) string {
	switch indexType {
	case IndexTypeAuto:
		return "vector" // Default to vector index for auto
	case IndexTypeIvfPq:
		return "ivf_pq"
	case IndexTypeIvfFlat:
		return "ivf_flat"
	case IndexTypeHnswPq:
		return "hnsw_pq"
	case IndexTypeHnswSq:
		return "hnsw_sq"
	case IndexTypeBTree:
		return "btree"
	case IndexTypeBitmap:
		return "bitmap"
	case IndexTypeLabelList:
		return "label_list"
	case IndexTypeFts:
		return "fts"
	default:
		return "vector" // Default fallback
	}
}

// stringToArrowType converts string type representation to Arrow DataType
func (t *Table) stringToArrowType(typeStr string) (arrow.DataType, error) {
	switch typeStr {
	case "int32":
		return arrow.PrimitiveTypes.Int32, nil
	case "int64":
		return arrow.PrimitiveTypes.Int64, nil
	case "float32":
		return arrow.PrimitiveTypes.Float32, nil
	case "float64":
		return arrow.PrimitiveTypes.Float64, nil
	case "string":
		return arrow.BinaryTypes.String, nil
	case "binary":
		return arrow.BinaryTypes.Binary, nil
	case "boolean":
		return arrow.FixedWidthTypes.Boolean, nil
	default:
		// Check for vector type (fixed_size_list[float32;N])
		prefix := "fixed_size_list[float32;"
		if strings.HasPrefix(typeStr, prefix) && strings.HasSuffix(typeStr, "]") {
			// Extract dimension from "fixed_size_list[float32;128]"
			dimStr := typeStr[len(prefix) : len(typeStr)-1] // Remove prefix and "]"
			var dimension int32
			_, err := fmt.Sscanf(dimStr, "%d", &dimension)
			if err != nil {
				return nil, fmt.Errorf("invalid vector dimension: %s", dimStr)
			}
			return arrow.FixedSizeListOf(dimension, arrow.PrimitiveTypes.Float32), nil
		}
		return nil, fmt.Errorf("unsupported type: %s", typeStr)
	}
}

// recordToJSON converts an Arrow Record to JSON format
func (t *Table) recordToJSON(record arrow.Record) (string, error) {
	schema := record.Schema()
	rows := make([]map[string]interface{}, record.NumRows())

	// Initialize rows
	for i := range rows {
		rows[i] = make(map[string]interface{})
	}

	// Process each column
	for colIdx, field := range schema.Fields() {
		column := record.Column(colIdx)
		fieldName := field.Name

		if err := t.convertColumnToJSON(column, fieldName, field.Type, rows); err != nil {
			return "", fmt.Errorf("failed to convert column %s: %w", fieldName, err)
		}
	}

	// Convert to JSON
	jsonBytes, err := json.Marshal(rows)
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return string(jsonBytes), nil
}

// convertColumnToJSON converts an Arrow column to JSON values in the rows
func (t *Table) convertColumnToJSON(column arrow.Array, fieldName string, dataType arrow.DataType, rows []map[string]interface{}) error {
	switch dataType.ID() {
	case arrow.INT32:
		arr := column.(*array.Int32)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				rows[i][fieldName] = nil
			} else {
				rows[i][fieldName] = arr.Value(i)
			}
		}
	case arrow.INT64:
		arr := column.(*array.Int64)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				rows[i][fieldName] = nil
			} else {
				rows[i][fieldName] = arr.Value(i)
			}
		}
	case arrow.FLOAT32:
		arr := column.(*array.Float32)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				rows[i][fieldName] = nil
			} else {
				rows[i][fieldName] = arr.Value(i)
			}
		}
	case arrow.FLOAT64:
		arr := column.(*array.Float64)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				rows[i][fieldName] = nil
			} else {
				rows[i][fieldName] = arr.Value(i)
			}
		}
	case arrow.BOOL:
		arr := column.(*array.Boolean)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				rows[i][fieldName] = nil
			} else {
				rows[i][fieldName] = arr.Value(i)
			}
		}
	case arrow.STRING:
		arr := column.(*array.String)
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				rows[i][fieldName] = nil
			} else {
				rows[i][fieldName] = arr.Value(i)
			}
		}
	case arrow.FIXED_SIZE_LIST:
		arr := column.(*array.FixedSizeList)
		listType := dataType.(*arrow.FixedSizeListType)

		// Handle vector fields (FixedSizeList of Float32)
		if listType.Elem().ID() == arrow.FLOAT32 {
			for i := 0; i < arr.Len(); i++ {
				if arr.IsNull(i) {
					rows[i][fieldName] = nil
				} else {
					listStart := i * int(listType.Len())
					values := make([]float32, listType.Len())
					
					valueArray := arr.ListValues().(*array.Float32)
					for j := 0; j < int(listType.Len()); j++ {
						if listStart+j < valueArray.Len() {
							values[j] = valueArray.Value(listStart + j)
						}
					}
					rows[i][fieldName] = values
				}
			}
		} else {
			return fmt.Errorf("unsupported FixedSizeList element type: %s", listType.Elem())
		}
	default:
		return fmt.Errorf("unsupported Arrow type: %s", dataType)
	}

	return nil
}
